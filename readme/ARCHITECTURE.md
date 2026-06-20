# In-Memory Graph API Architecture

## Overview

This document describes the architecture of RustyChickpeas, an in-memory graph API written in Rust. The API uses an immutable `GraphSnapshot` design with CSR (Compressed Sparse Row) adjacency and columnar property storage, optimized for read-heavy workloads.

## Core Design Principles

1. **Immutable Snapshots**: Graphs are built once and become immutable for fast, safe concurrent reads
2. **CSR Format**: Compressed Sparse Row format for efficient neighbor lookups
3. **Columnar Properties**: Properties stored in columnar format (dense or sparse) for cache efficiency
4. **u32 Node IDs**: GraphBuilder takes u32 node IDs directly (optional — auto-generated sequentially when omitted); users map larger external IDs to u32 themselves
5. **Version Management**: RustyChickpeas manager stores multiple snapshots by version
6. **Rust-First Core**: High-performance Rust implementation with zero-cost abstractions
7. **Python Interoperability**: PyO3 bindings for seamless Python integration

## Project Structure

```
rustychickpeas/
├── Cargo.toml                    # Workspace root (members: core + python)
├── README.md
├── readme/ARCHITECTURE.md
├── rustychickpeas-core/          # Core Rust library
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs
│   │   ├── graph_snapshot.rs     # Immutable GraphSnapshot
│   │   ├── graph_builder.rs      # GraphBuilder
│   │   ├── graph_builder_csv.rs  # CSV/gzip bulk loading
│   │   ├── graph_builder_parquet.rs # Parquet/S3 bulk loading
│   │   ├── bitmap.rs              # NodeSet (RoaringBitmap/BitVec)
│   │   ├── types.rs               # Core types (NodeId, Label, PropertyKey, etc.)
│   │   ├── interner.rs            # String interning
│   │   ├── error.rs               # Error types
│   │   └── rusty_chickpeas.rs     # Version manager
│   ├── benches/                  # Criterion benchmarks
│   └── tests/                    # Integration tests
├── rustychickpeas-python/        # Python bindings
│   ├── Cargo.toml
│   ├── pyproject.toml
│   ├── src/
│   │   ├── lib.rs                # PyO3 module registration
│   │   ├── rusty_chickpeas.rs    # Python RustyChickpeas wrapper
│   │   ├── graph_snapshot.rs     # Python GraphSnapshot wrapper
│   │   ├── graph_snapshot_builder.rs # Python builder wrapper
│   │   ├── node.rs               # Python Node wrapper
│   │   ├── relationship.rs       # Python Relationship wrapper
│   │   ├── direction.rs          # Direction enum
│   │   └── utils.rs              # PyAny -> PropertyValue conversion
│   └── tests/                    # Python tests (pytest)
├── scripts/                      # Dev utility scripts (tests, coverage, benchmarks)
└── bench/                        # Benchmark collection, plotting, results
```

## Core Data Structures

### ID Management

```rust
// Core ID types
pub type NodeId = u32;              // Node ID (u32 for efficiency)
pub type InternedStringId = u32;   // Interned string ID
pub type PropertyKey = u32;         // Property key (interned)

// Node IDs are u32; users should map larger external IDs to u32 if needed
```

### GraphSnapshot (Immutable)

```rust
pub struct GraphSnapshot {
    // Core shape (CSR format)
    pub n_nodes: u32,  // Actual node count; access via node_count() in Python.
                       // CSR offset arrays are sized by max_node_id + 2, which
                       // can exceed n_nodes + 1 when node IDs are sparse.
    pub n_rels: u64,   // Access via relationship_count() in Python
    pub out_offsets: Vec<u32>,      // CSR offsets for outgoing rels
    pub out_nbrs: Vec<NodeId>,      // CSR neighbors (outgoing)
    pub out_types: Vec<RelationshipType>, // Relationship types, parallel to out_nbrs
    pub in_offsets: Vec<u32>,       // CSR offsets for incoming rels
    pub in_nbrs: Vec<NodeId>,       // CSR neighbors (incoming)
    pub in_types: Vec<RelationshipType>,  // Relationship types, parallel to in_nbrs

    // Indexes (value -> NodeSet)
    pub label_index: HashMap<Label, NodeSet>,
    pub type_index: HashMap<RelationshipType, NodeSet>,

    // Version tracking
    pub version: Option<String>,

    // Columnar property storage (nodes and relationships)
    pub columns: HashMap<PropertyKey, Column>,
    pub rel_columns: HashMap<PropertyKey, Column>,

    // Inverted property index, lazy-built on first access, scoped per label
    pub prop_index: Mutex<HashMap<(Label, PropertyKey), HashMap<ValueId, NodeSet>>>,

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

### GraphBuilder (Mutable Staging)

```rust
pub struct GraphBuilder {
    // Adjacency assembly (counts first, then fill)
    deg_out: Vec<u32>,
    deg_in: Vec<u32>,
    rels: Vec<(NodeId, NodeId)>,     // Temporary storage

    // Labels/types during build
    node_labels: Vec<Vec<Label>>,
    rel_types: Vec<RelationshipType>,

    // Version tracking
    version: Option<String>,

    // Node properties (staging; dense or sparse chosen at finalize)
    node_col_i64: HashMap<PropertyKey, Vec<(NodeId, i64)>>,
    node_col_f64: HashMap<PropertyKey, Vec<(NodeId, f64)>>,
    node_col_bool: HashMap<PropertyKey, Vec<(NodeId, bool)>>,
    node_col_str: HashMap<PropertyKey, Vec<(NodeId, u32)>>,

    // Relationship properties (staging; indexed by position in rels)
    rel_col_i64: HashMap<PropertyKey, Vec<(usize, i64)>>,
    rel_col_f64: HashMap<PropertyKey, Vec<(usize, f64)>>,
    rel_col_bool: HashMap<PropertyKey, Vec<(usize, bool)>>,
    rel_col_str: HashMap<PropertyKey, Vec<(usize, u32)>>,

    // String interner
    interner: StringInterner,

    // Deduplication (unique property values -> node_id)
    dedup_unique_properties: Option<Vec<PropertyKey>>,
    dedup_map: HashMap<DedupKey, NodeId>,

    // Auto-generated ID counter and known-node tracking
    next_node_id: NodeId,
    known_nodes: RoaringBitmap,
}
```

### RustyChickpeas Manager

```rust
pub struct RustyChickpeas {
    snapshots: Arc<RwLock<HashMap<String, Arc<GraphSnapshot>>>>,
}
```

## Graph Building Workflow

### 1. Building Phase (GraphBuilder)

```rust
let manager = RustyChickpeas::new();
let mut builder = manager.create_builder(Some("v1.0"), None, None);

// Add nodes with u32 IDs (or None to auto-generate)
builder.add_node(Some(1001), &["Person"]).unwrap();
builder.add_node(Some(1002), &["Person"]).unwrap();

// Add relationships (node IDs are required)
builder.add_relationship(1001, 1002, "KNOWS").unwrap();

// Set properties
builder.set_prop_str(1001, "name", "Alice").unwrap();

// Finalize and add to manager
let snapshot = builder.finalize(None);
manager.add_snapshot(snapshot);
```

### 2. Query Phase (GraphSnapshot)

```rust
// Retrieve snapshot
let snapshot = manager.graph_snapshot("v1.0").unwrap();

// Fast neighbor lookup (CSR format)
let neighbors = snapshot.neighbors(1001, Direction::Outgoing);

// Label query (inverted index)
let person_nodes = snapshot.nodes_with_label("Person");

// Property query (lazy inverted index, scoped by label)
let alice_nodes = snapshot.nodes_with_property("Person", "name", "Alice");
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
- **Memory efficient**: Only stores actual rels
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
prop_index: Mutex<HashMap<(Label, PropertyKey), HashMap<ValueId, NodeSet>>>
// ("Person", "name") -> { "Alice" -> NodeSet with all Person nodes named Alice }
// Lazy-built on first access, scoped per label
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
builder = manager.create_builder(version="v1.0")
builder.add_node(["Person"], node_id=1)
builder.finalize_into(manager)

# Snapshot (immutable)
snapshot = manager.graph_snapshot("v1.0")
neighbors = snapshot.neighbors(1, rcp.Direction.Outgoing)
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

- **From Parquet**: millions of entities/second (see README for measured rates)
- **Node IDs**: must fit in u32; unsupported property column types return a SchemaError

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
roaring = "0.11"           # RoaringBitmap for NodeSet
bitvec = "1.0"             # BitVec for dense boolean sets
lasso = "0.7"              # String interning
hashbrown = "0.16"         # Fast hash maps
parquet = "57.0"           # Parquet file support
arrow = "57.0"             # Arrow columnar format
object_store = "0.12"      # S3/GCP/Azure access
rayon = "1.8"              # Parallel finalization
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
let mut builder = manager.create_builder(None, None, None);
builder.add_node(Some(1), &["Person"]).unwrap();
manager.add_snapshot(builder.finalize(None));
let snapshot = manager.graph_snapshot("latest").unwrap();
```

## Future Enhancements

1. **Query Language**: Cypher-like query interface
2. **Persistence**: Optional disk-backed storage
3. **Graph Algorithms**: Built-in algorithms (PageRank, community detection, etc.)
4. **Concurrent Queries**: Thread-safe snapshot access
5. **Incremental Updates**: Efficient snapshot diffing
