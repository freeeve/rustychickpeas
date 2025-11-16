# RustyChickpeas

An in-memory graph API written in Rust, using RoaringBitmaps as a fundamental data structure. 

## Overview

RustyChickpeas provides a high-performance, in-memory graph database API that:

- **Uses RoaringBitmaps**: Efficient set operations for node/relationship IDs
- **Rust-First**: High-performance core implementation
- **Python-Friendly**: PyO3 bindings for seamless Python integration
- **In-Memory**: Optimized for fast graph operations

## Project Status

✅ **Implementation Complete** - Core functionality implemented with comprehensive benchmarks and Python bindings.

- ✅ Immutable GraphSnapshot with CSR adjacency and columnar properties
- ✅ GraphSnapshotBuilder for efficient bulk loading with external ID mapping
- ✅ Parallel finalization using rayon for improved performance
- ✅ Python bindings via PyO3
- ✅ Bulk loading from Parquet files

See [rustychickpeas-core/benches/README.md](./rustychickpeas-core/benches/README.md) for benchmark information.

## Key Features

- ✅ **Immutable GraphSnapshot** - Read-optimized graph with CSR adjacency and columnar properties
- ✅ **GraphSnapshotBuilder** - Efficient bulk loading with external ID mapping (u64 → u32)
- ✅ **RustyChickpeas Manager** - Version management for multiple graph snapshots
- ✅ **Parallel Finalization** - Uses rayon to parallelize index building during finalization
- ✅ Label and property support with inverted indexes
- ✅ Efficient traversal using CSR (Compressed Sparse Row) format
- ✅ Python bindings via PyO3
- ✅ Bulk loading from Parquet files

## Installation

```bash
pip install rustychickpeas
```

Or from source:

```bash
git clone https://github.com/freeeve/rustychickpeas.git
cd rustychickpeas/rustychickpeas-python
pip install maturin
maturin develop --release
```

## Quick Start

### Python

```python
import rustychickpeas as rcp

# Create a manager for version management
manager = rcp.RustyChickpeas()

# Create a builder (capacity is optional, auto-grows as needed)
builder = manager.create_builder(version="v1.0")

# Add nodes and relationships
builder.add_node(1, ["Person"])
builder.add_node(2, ["Person"])
builder.add_rel(1, 2, "KNOWS")

# Set properties
builder.set_prop(1, "name", "Alice")
builder.set_prop(1, "age", 30)

# Finalize and add to manager
builder.finalize_into(manager)

# Retrieve snapshot by version
graph = manager.get_graph_snapshot("v1.0")

# Query the graph - get_rels() returns Node objects (neighbor nodes)
neighbors = graph.get_rels(0, rcp.Direction.Outgoing)  # Returns list of Node objects
print(f"Node 0 has {len(neighbors)} outgoing neighbors")
for neighbor in neighbors:
    print(f"  Neighbor ID: {neighbor.id()}")

# Or get a Node object and use its methods
node = graph.get_node(0)
neighbor_ids = node.get_rel_ids(rcp.Direction.Outgoing)  # Returns list of node IDs
print(f"Neighbor IDs: {neighbor_ids}")

# Get relationships as Relationship objects (includes type, start/end nodes)
relationships = node.get_rels(rcp.Direction.Outgoing)  # Returns list of Relationship objects
for rel in relationships:
    print(f"  Relationship: {rel.get_type()} from {rel.get_start_node().id()} to {rel.get_end_node().id()}")

# Load from Parquet files (recommended for bulk loading)
graph = rcp.GraphSnapshot.read_from_parquet(
    nodes_path="nodes.parquet",
    relationships_path="relationships.parquet",
    node_id_column="id",
    label_columns=["label"],
    start_node_column="from",
    end_node_column="to",
    rel_type_column="type"
)
```

### Rust

```rust
use rustychickpeas_core::RustyChickpeas;

// Create a manager (handles multiple snapshots by version)
let manager = RustyChickpeas::new();

// Create a builder from the manager with version
let mut builder = manager.create_builder(Some("v1.0"), None, None);

// Add nodes and relationships
builder.add_node(1, &["Person"]);
builder.add_node(2, &["Person"]);
builder.add_rel(1, 2, "KNOWS");

// Finalize the builder
let snapshot = builder.finalize(None);

// Add to manager
manager.add_snapshot(snapshot);

// Retrieve the snapshot by version
let snapshot = manager.get_graph_snapshot("v1.0").unwrap();

// Query the snapshot
let neighbors = snapshot.get_out_neighbors(0);
println!("Node 0 neighbors: {:?}", neighbors);
```

## Version Management

RustyChickpeas supports version management at the snapshot level using the `RustyChickpeas` manager. Each snapshot can have a version string (e.g., "v1.0", "v2.0") that identifies it.

### Python

```python
import rustychickpeas as rcp

# Create a manager
manager = rcp.RustyChickpeas()

# Create and build version 1.0
builder1 = manager.create_builder(version="v1.0")
# ... add nodes/relationships ...
builder1.finalize_into(manager)

# Create and build version 2.0
builder2 = manager.create_builder(version="v2.0")
# ... add nodes/relationships ...
builder2.finalize_into(manager)

# Retrieve snapshots by version
v1_snapshot = manager.get_graph_snapshot("v1.0")
v2_snapshot = manager.get_graph_snapshot("v2.0")

# List all versions
versions = manager.versions()  # ["v1.0", "v2.0"]
```

### How Version Management Works

1. **Setting Version**: Pass `version` parameter to `create_builder()` when creating the builder, or use `GraphSnapshotBuilder.set_version(version_string)` to set it later.

2. **Storage**: After calling `builder.finalize()`, add the snapshot to the manager using `manager.add_snapshot(snapshot)`. The snapshot will be stored under its version (if set) or "latest" if no version is set.

3. **Retrieval**: Use `manager.get_graph_snapshot(version_string)` to retrieve a snapshot by version.

4. **Version Storage**: Versions are stored as strings and can be any identifier you choose (e.g., "v1.0", "2024-01-01", "production").

### Capacity and Auto-Growing

The `capacity_nodes` and `capacity_rels` parameters are **optional performance hints** for pre-allocation:

- **Defaults**: If not specified, starts with 2^20 (1,048,576) nodes/relationships capacity
- **Auto-Growing**: The builder automatically grows as needed (doubling capacity each time)
- **Maximum Limits**: 
  - Nodes: Up to 2^32 - 1 (4.3 billion) - enforced by `u32` NodeId
  - Relationships: Up to 2^64 - 1 (18.4 quintillion) - limited by memory
- **When to Specify Capacity**: Only specify if you know the approximate size upfront to avoid reallocations. For most use cases, the default auto-growing behavior is sufficient.

**Example**:
```python
# Uses default (2^20 = 1,048,576), auto-grows as needed
builder = manager.create_builder(version="v1.0")

# Large graph - specify capacity hint to avoid reallocations
builder = manager.create_builder(version="v1.0", capacity_nodes=10_000_000, capacity_rels=50_000_000)
```

## Performance

See [rustychickpeas-core/benches/README.md](./rustychickpeas-core/benches/README.md) for benchmark information.

For Python-specific performance tests, see [rustychickpeas-python/tests/benchmark_large_parquet.py](./rustychickpeas-python/tests/benchmark_large_parquet.py).

**Highlights**:
- Node existence: ~7ns (constant time)
- Property lookup: ~44ns (constant time)
- Label queries: ~56ns for 100 nodes, ~348ns for 100K nodes
- BFS traversal: ~130ns per node
- Bitmap operations: Sub-microsecond for millions of elements

## Limits and Scalability

### Hard Limits

- **Nodes**: Up to 2^32 - 1 (4,294,967,295 nodes) - Limited by `u32` NodeId used internally
- **External Node IDs**: `u64` (up to 2^64 - 1) - Automatically mapped to internal `u32` by GraphSnapshotBuilder
- **Relationships**: Up to 2^64 - 1 (18,446,744,073,709,551,615) - Limited by `u64` counter, but practically constrained by memory
- **Unique Strings** (labels, relationship types, property keys): Up to 2^32 - 1 (4,294,967,295) - Limited by `u32` InternedStringId
- **Properties per Node**: No hard limit, constrained by available memory
- **Property Values**: No hard limit, constrained by available memory

**Note**: While relationships can theoretically reach 2^64 - 1, practical limits are determined by available system memory. Each relationship requires storage in CSR arrays and indexes.

### Tested Scales

✅ **1 Million Nodes + 1 Million Relationships** - Tested and verified:
- Memory usage: ~2.7GB (with string interning) to ~3.4GB (without)
- Bulk load rate: ~4M entities/sec from Parquet files
- Direct builder rate: 21-31M nodes/sec, 12-19M rels/sec
- Query performance: Sub-millisecond for most operations
- See [rustychickpeas-python/tests/benchmark_large_parquet.py](./rustychickpeas-python/tests/benchmark_large_parquet.py) for large-scale benchmarks

### Practical Considerations

**Memory Usage**:
- **Base overhead**: ~3.5 bytes per node/relationship (structure + indexes)
- **Properties**: Additional memory depends on property count and size
- **String interning**: Reduces memory signficantly for graphs with high string duplication
- **Property value interning**: Optional feature saves 32-50% memory when property values have high duplication

**Performance Characteristics**:
- **Direct Builder Operations**: 21-31M nodes/sec, 12-19M rels/sec (exceeds 10M/sec target)
- **Bulk Loading**: ~4M entities/sec from Parquet files (sequential processing, I/O bound)
- **Finalization**: Parallelized using rayon for label/type indexes, property columns, and inverted indexes
- **Queries**: Constant-time for indexed operations (label/type lookups)
- **Traversal**: Linear with graph connectivity (BFS, shortest path)
- **Memory**: Linear growth with graph size

**Recommended Usage**:
- **Small graphs** (< 100K nodes): Excellent performance, minimal memory footprint
- **Medium graphs** (100K - 10M nodes): Good performance, manageable memory requirements
- **Large graphs** (10M+ nodes): Feasible but memory becomes the primary constraint; finalization benefits from parallelization
- **Very large graphs** (100M+ nodes): Possible with sufficient RAM (100GB+); parallel finalization helps reduce finalization time

**Memory Estimation**:
```
Base memory ≈ (nodes + relationships) × 3.5 bytes
+ Properties (varies by property count/size)
+ String interning overhead (minimal)
- String interning savings (~21.5% if enabled)
```

**Example**: 1M nodes + 1M relationships with properties:
- Without interning: ~3.4GB
- With basic interning: ~2.7GB
- With property value interning (50% reuse): ~1.4GB

## Testing

### Running Tests

**Rust Tests**:
```bash
cargo test --workspace
```

**Python Tests**:
```bash
cd rustychickpeas-python
pytest tests/
```

### Test Coverage

Test coverage is set up for both Rust and Python:

**Run Coverage**:
```bash
./scripts/coverage.sh  # Linux/macOS
.\scripts\coverage.ps1  # Windows
```

**Coverage Reports**:
- Rust: `coverage/rust/tarpaulin-report.html`
- Python: `coverage/python/htmlcov/index.html`

See [docs/COVERAGE.md](docs/COVERAGE.md) for detailed coverage documentation.

## License

Licensed under MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT).

