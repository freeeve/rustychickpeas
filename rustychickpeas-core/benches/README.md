# Benchmark Suites

This directory contains comprehensive benchmarks for RustyChickpeas using Criterion.rs.

## Quick Start

```bash
# Run all benchmarks
cargo bench

# Run specific suite
cargo bench --bench graph_builder
cargo bench --bench graph_snapshot
cargo bench --bench bulk_load

# Generate HTML reports
cargo bench -- --output-format html
```

## Benchmark Files

- **graph_builder.rs**: GraphBuilder operations (adding nodes, relationships, properties, finalization)
- **graph_snapshot.rs**: GraphSnapshot query operations (neighbors, properties, labels, traversal)
- **bulk_load.rs**: Bulk loading from Parquet files

All benchmarks test the new immutable `GraphSnapshot` and `GraphSnapshotBuilder` APIs.

