# RustyChickpeas Python Tests

This directory contains test files for the RustyChickpeas Python bindings.

## API Overview

The Python API provides:

- **`RustyChickpeas`**: Manager for multiple graph snapshots by version
  - Creates builders via `create_builder()`
  - Stores and retrieves snapshots by version string
  - Manages multiple versions of graphs

- **`GraphSnapshot`**: Immutable, read-optimized graph structure
  - Created via `GraphSnapshot.read_from_parquet()` or `GraphSnapshotBuilder.finalize()`
  - Supports fast queries: labels, properties, neighbors
  - Uses CSR (Compressed Sparse Row) format for efficient traversal
  
- **`GraphSnapshotBuilder`**: Mutable builder for constructing graphs
  - Supports bulk loading from Parquet files
  - Supports pre-finalization queries and property updates
  - Maps external u64 IDs to internal u32 NodeIds
  - Finalizes into an immutable `GraphSnapshot`
  - Can finalize into manager with `finalize_into(manager)`

- **`Direction`**: Enum for relationship traversal (Outgoing, Incoming, Both)

## Running Tests

### Prerequisites

1. Build the Python extension:
```bash
cd rustychickpeas-python
maturin develop
# or
pip install -e .
```

2. Install test dependencies:
```bash
pip install pytest
```

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
pytest tests/test_basic_operations.py
pytest tests/test_bulk_load_parquet.py
pytest tests/test_bulk_load_builder.py
```

### Run with Verbose Output

```bash
pytest -v
```

### Run with Coverage

```bash
pip install pytest-cov
pytest --cov=rustychickpeas
```

## Test Structure

- **test_basic_operations.py**: Tests for GraphSnapshotBuilder and GraphSnapshot operations
- **test_bulk_load_parquet.py**: Tests for bulk loading from Parquet files
- **test_bulk_load_builder.py**: Performance tests for GraphSnapshotBuilder
- **test_relationship_api.py**: Tests for Node and Relationship APIs
- **benchmark_large_parquet.py**: Large-scale performance benchmarks

## Test Coverage

The tests cover:
- GraphSnapshotBuilder operations (adding nodes, relationships, properties)
- GraphSnapshot queries (neighbors, properties, labels)
- Bulk loading from Parquet files
- Version management with RustyChickpeas manager
- Property index queries
- Graph traversal operations

