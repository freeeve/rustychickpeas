# In-Memory Graph API Architecture

## Overview

This document describes the architecture of RustyChickpeas, an in-memory graph API written in Rust. The API uses an immutable `GraphSnapshot` design with CSR (Compressed Sparse Row) adjacency and columnar property storage, optimized for read-heavy workloads.

## Core Design Principles

1. **Immutable Snapshots**: Graphs are built once and become immutable for fast, safe concurrent reads
2. **CSR Format**: Compressed Sparse Row format for efficient neighbor lookups
3. **Columnar Properties**: Properties stored in columnar format (dense or sparse) for cache efficiency
4. **External ID Mapping**: GraphSnapshotBuilder maps external u64 IDs to internal u32 NodeIds
5. **Version Management**: RustyChickpeas manager stores multiple snapshots by version
6. **Rust-First Core**: High-performance Rust implementation with zero-cost abstractions
7. **Python Interoperability**: PyO3 bindings for seamless Python integration

## Project Structure

```
rustychickpeas/
├── Cargo.toml
├── README.md
├── readme/ARCHITECTURE.md
├── rustychickpeas-core/          # Core Rust library
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── snapshot.rs            # Immutable GraphSnapshot
│       ├── builder.rs             # GraphSnapshotBuilder
│       ├── builder_parquet.rs    # Parquet loading
│       ├── bitmap.rs              # NodeSet (RoaringBitmap/BitVec)
│       ├── types.rs               # Core types (NodeId, Label, PropertyKey, etc.)
│       ├── interner.rs            # String interning
│       ├── error.rs               # Error types
│       └── rusty_chickpeas.rs     # Version manager
├── rustychickpeas-python/        # Python bindings
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/
│       └── lib.rs                # PyO3 bindings
└── examples/
    ├── rust_example.rs
    └── python_example.py
```

## Core Data Structures

### ID Management

```rust
// Core ID types
pub type NodeId = u32;              // Internal node ID (u32 for efficiency)
pub type InternedStringId = u32;   // Interned string ID
pub type PropertyKey = u32;         // Property key (interned)

// External IDs are u64 (mapped to internal u32 by GraphSnapshotBuilder)
```

### GraphSnapshot (Immutable)

```rust
pub struct GraphSnapshot {
    // Core shape (CSR format)
    pub n_nodes: u32,
    pub n_rels: u64,
    pub out_offsets: Vec<u32>,      // CSR offsets for outgoing edges
    pub out_nbrs: Vec<NodeId>,      // CSR neighbors (outgoing)
    pub in_offsets: Vec<u32>,       // CSR offsets for incoming edges
    pub in_nbrs: Vec<NodeId>,       // CSR neighbors (incoming)
    
    // Indexes (value -> NodeSet)
    pub label_index: HashMap<Label, NodeSet>,
    pub type_index: HashMap<RelationshipType, NodeSet>,
    pub prop_index: HashMap<(PropertyKey, ValueId), NodeSet>,
    
    // Version tracking
    pub version: Option<String>,
    
    // Columnar property storage
    pub columns: HashMap<PropertyKey, Column>,
    
    // String interner (flattened)
    pub atoms: Atoms,  // Vec<String> for label/type/key resolution
}
```

### Columnar Property Storage

```rust
pub enum Column {
    // Dense columns (one value per node, aligned by NodeId)
    DenseI64(Vec<i64>),
    DenseF64(Vec<f64>),
    DenseBool(BitVec),              // Compact boolean storage
    DenseStr(Vec<u32>),             // Interned string IDs
    
    // Sparse columns (only nodes with values, sorted by NodeId)
    SparseI64(Vec<(NodeId, i64)>),
    SparseF64(Vec<(NodeId, f64)>),
    SparseBool(Vec<(NodeId, bool)>),
    SparseStr(Vec<(NodeId, u32)>),
}
```

### GraphSnapshotBuilder (Mutable Staging)

```rust
pub struct GraphBuilder {
    // External ID -> internal NodeId mapping
    ext2int: HashMap<u64, NodeId>,
    next_id: NodeId,
    
    // Adjacency assembly (counts first, then fill)
    deg_out: Vec<u32>,
    deg_in: Vec<u32>,
    rels: Vec<(NodeId, NodeId)>,     // Temporary storage
    
    // Labels/types during build
    node_labels: Vec<Vec<Label>>,
    rel_types: Vec<RelationshipType>,
    
    // Version tracking
    version: Option<String>,
    
    // Properties (staging)
    col_i64: HashMap<PropertyKey, Vec<(NodeId, i64)>>,
    col_f64: HashMap<PropertyKey, Vec<(NodeId, f64)>>,
    col_bool: HashMap<PropertyKey, Vec<(NodeId, bool)>>,
    col_str: HashMap<PropertyKey, Vec<(NodeId, u32)>>,
    
    // Inverted index (value -> NodeId buckets)
    inv: HashMap<(PropertyKey, ValueId), Vec<NodeId>>,
    
    // String interner
    interner: StringInterner,
}
```

### RustyChickpeas Manager

```rust
pub struct RustyChickpeas {
    snapshots: Arc<RwLock<HashMap<String, Arc<GraphSnapshot>>>>,
}
```

## Graph Building Workflow

### 1. Building Phase (GraphSnapshotBuilder)

```rust
let manager = RustyChickpeas::new();
let mut builder = manager.create_builder(1000, 1000);

// Add nodes with external IDs
builder.add_node(1001, &["Person"]);  // External ID: 1001
builder.add_node(1002, &["Person"]);  // External ID: 1002

// Add relationships
builder.add_rel(1001, 1002, "KNOWS");

// Set properties
builder.set_prop_str(1001, "name", "Alice");

// Set version
builder.set_version("v1.0");

// Finalize and add to manager
builder.finalize_into(&manager);
```

### 2. Query Phase (GraphSnapshot)

```rust
// Retrieve snapshot
let snapshot = manager.get_graph_snapshot("v1.0").unwrap();

// Fast neighbor lookup (CSR format)
let neighbors = snapshot.get_out_neighbors(0);  // Internal NodeId 0

// Label query (inverted index)
let person_nodes = snapshot.get_nodes_with_label(person_label);

// Property query (inverted index)
let alice_nodes = snapshot.get_nodes_with_property(name_key, ValueId::Str(alice_name_id));
```

## CSR (Compressed Sparse Row) Format

### Outgoing Neighbors

```
out_offsets: [0, 2, 4, 6, ...]  // Cumulative offsets
out_nbrs:    [1, 2, 0, 3, 0, 1, ...]  // Neighbor node IDs

Node 0: neighbors at out_nbrs[0..2] = [1, 2]
Node 1: neighbors at out_nbrs[2..4] = [0, 3]
Node 2: neighbors at out_nbrs[4..6] = [0, 1]
```

### Benefits

- **Cache-friendly**: Sequential memory access
- **Fast lookups**: O(1) to get neighbor slice
- **Memory efficient**: Only stores actual edges
- **SIMD-friendly**: Can use vectorized operations

## Columnar Property Storage

### Dense vs Sparse Selection

Properties are stored as:
- **Dense**: If >80% of nodes have the property (aligned by NodeId)
- **Sparse**: If <80% of nodes have the property (sorted pairs)

### Benefits

- **Cache efficiency**: Columnar layout improves cache locality
- **Memory efficient**: Sparse storage for rare properties
- **Fast lookups**: O(1) for dense, O(log n) for sparse (binary search)

## Inverted Indexes

### Label Index

```rust
label_index: HashMap<Label, NodeSet>
// "Person" -> NodeSet with all Person nodes
```

### Property Index

```rust
prop_index: HashMap<(PropertyKey, ValueId), NodeSet>
// ("name", "Alice") -> NodeSet with all nodes named Alice
```

### Benefits

- **Fast queries**: O(1) hash lookup + O(1) NodeSet operations
- **Efficient filtering**: Intersect multiple indexes
- **Memory efficient**: NodeSet uses RoaringBitmap or BitVec

## Python API

### Core Classes

```python
# Manager for version management
manager = rcp.RustyChickpeas()

# Builder (created from manager)
builder = manager.create_builder(1000, 1000)
builder.add_node(1, ["Person"])
builder.set_version("v1.0")
builder.finalize_into(manager)

# Snapshot (immutable)
snapshot = manager.get_graph_snapshot("v1.0")
neighbors = snapshot.get_neighbors(0, rcp.Direction.Outgoing)
```

## Performance Characteristics

### Build Phase (GraphSnapshotBuilder)

- **Node addition**: ~1µs per node
- **Relationship addition**: ~1µs per relationship
- **Finalization**: Linear with graph size, ~100ms for 100K nodes

### Query Phase (GraphSnapshot)

- **Neighbor lookup**: <10ns (CSR format)
- **Property lookup**: <50ns (columnar storage)
- **Label query**: <1µs for 100K nodes (inverted index)
- **Property index query**: <1µs (inverted index)

### Bulk Loading

- **From Parquet**: >100K entities/second
- **External ID mapping**: Handles u64 → u32 conversion automatically

## Memory Layout

### GraphSnapshot

- **CSR arrays**: ~8 bytes per relationship (offsets + neighbors)
- **Indexes**: ~4 bytes per indexed value (hash map overhead)
- **Properties**: Varies (dense: 8 bytes/node, sparse: 16 bytes/value)
- **Strings**: Interned once, referenced by ID

### Memory Efficiency

- **CSR format**: Minimal overhead for adjacency
- **Columnar properties**: Cache-friendly, efficient for queries
- **String interning**: Shared storage for labels/types/keys
- **NodeSet**: RoaringBitmap for sparse sets, BitVec for dense sets

## Key Dependencies

### Rust Core
```toml
[dependencies]
roaring = "0.10"           # RoaringBitmap for NodeSet
bitvec = "1.0"             # BitVec for dense boolean sets
lasso = "0.7"              # String interning
hashbrown = "0.16"         # Fast hash maps
parquet = "..."            # Parquet file support
arrow = "..."              # Arrow columnar format
```

### Python Bindings
```toml
[dependencies]
pyo3 = { version = "0.20", features = ["extension-module"] }
rustychickpeas-core = { path = "../rustychickpeas-core" }
```

## Migration from Old API

The old mutable `Graph` API has been removed. Migration path:

### Old API (Removed)
```rust
let mut graph = Graph::new();
graph.create_node(vec![Label::from("Person")])?;
```

### New API
```rust
let manager = RustyChickpeas::new();
let mut builder = manager.create_builder(1000, 1000);
builder.add_node(1, &["Person"]);
builder.finalize_into(&manager);
let snapshot = manager.get_graph_snapshot("latest").unwrap();
```

## Future Enhancements

1. **Query Language**: Cypher-like query interface
2. **Persistence**: Optional disk-backed storage
3. **Graph Algorithms**: Built-in algorithms (PageRank, community detection, etc.)
4. **Concurrent Queries**: Thread-safe snapshot access
5. **Incremental Updates**: Efficient snapshot diffing
