# Setting Up Allocation Tracking

## Overview

The benchmark tracking system can track both **time** (nanoseconds) and **allocations** (count). Allocation tracking requires using `dhat-rs`, a heap profiler.

## Quick Start

### 1. Add dhat dependency (already done)

The `dhat` dependency is already added to `rustychickpeas-core/Cargo.toml` as an optional dev dependency.

### 2. Run benchmarks with dhat feature

```bash
cargo bench --features dhat --bench graph_builder --bench graph_snapshot --bench bulk_load
```

### 3. Collect results

The collector will automatically extract allocation data from dhat's output files:

```bash
python3 bench/collect.py
```

### 4. Generate charts

Allocation charts will be generated automatically:

```bash
python3 bench/plot.py
```

## How It Works

1. **dhat** tracks allocations during benchmark execution and writes data to `dhat-heap.json` files
2. **collect.py** searches for these files and extracts allocation counts
3. **plot.py** generates separate sparklines and badges for allocations

## Output

When allocation tracking is enabled, you'll get:

- **CSV data**: Allocation counts in the `allocs` column
- **Charts**: 
  - `{bench}_allocs.svg` - Allocation trend sparkline
  - `{bench}_allocs_badge.svg` - Current allocation count + % change

## Example

```csv
commit,date,bench,metric,unit,value,ci_low,ci_high,allocs,allocs_ci_low,allocs_ci_high,rustc,machine
abc123,2024-12-15T10:30:00Z,builder_add_nodes/10000,mean,ns,294026,292100,296804,1024,,,rustc 1.75.0,github
```

## Without dhat

If you run benchmarks without the `dhat` feature, allocation columns will be empty:

```csv
...,allocs,allocs_ci_low,allocs_ci_high,...
...,,,,...
```

This is fine - time tracking still works perfectly.

## GitHub Actions

To enable allocation tracking in CI, update `.github/workflows/bench.yml`:

```yaml
- name: Run benchmarks
  run: |
    cargo bench --features dhat --bench graph_builder --bench graph_snapshot --bench bulk_load
```

## Troubleshooting

### No allocation data collected

- Make sure you ran benchmarks with `--features dhat`
- Check that `target/criterion/*/dhat-heap.json` files exist
- Verify dhat is working: `cargo bench --features dhat --bench graph_builder` should create dhat files

### dhat files not found

- dhat writes files to the benchmark directory
- The collector searches recursively for `dhat-heap.json` files
- Make sure the benchmark directory structure matches expectations

## Future Improvements

- Custom Criterion measurement that tracks allocations directly
- Integration with `criterion-perf-events` if available
- More detailed allocation metrics (bytes, peak memory, etc.)

