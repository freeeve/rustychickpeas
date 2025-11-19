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

## Tracking Performance Across Tags

Criterion is configured to track benchmark performance across git tags, allowing you to see performance improvements or regressions over time.

### Running Benchmarks for a Tag

To run benchmarks for a specific tag and save the results:

```bash
# Run benchmarks for a tag
./scripts/benchmark_tag.sh v0.4.0
```

This will:
1. Checkout the tag
2. Build and run all benchmarks
3. Save results with the tag name as the baseline
4. Return to your original branch

### Comparing Two Tags

To compare performance between two tags:

```bash
# Compare v0.3.0 (baseline) vs v0.4.0 (current)
./scripts/compare_benchmarks.sh v0.3.0 v0.4.0
```

This will:
1. Run benchmarks for the baseline tag
2. Run benchmarks for the current tag (comparing against baseline)
3. Generate comparison reports

### Manual Comparison

You can also manually compare against a baseline:

```bash
# Set baseline and run benchmarks
export BENCHMARK_BASELINE=v0.3.0
cargo bench --bench graph_builder
cargo bench --bench graph_snapshot
cargo bench --bench bulk_load
```

### Viewing Results

Benchmark results are saved in `target/criterion/`. Each benchmark has its own directory with HTML reports:

```bash
# Open a specific benchmark report
open target/criterion/builder_add_nodes/report/index.html
```

The HTML reports include:
- Performance statistics (mean, median, standard deviation)
- Comparison with baseline (if set)
- Performance graphs over time
- Detailed timing information

### Best Practices

1. **Run benchmarks on tags**: Use the `benchmark_tag.sh` script to run benchmarks for each release tag
2. **Compare before releases**: Before creating a new release, compare against the previous tag to catch regressions
3. **Keep baseline consistent**: Use the same baseline tag for multiple benchmark runs to ensure consistent comparisons
4. **Review HTML reports**: The HTML reports provide detailed visualizations of performance changes

