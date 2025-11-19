# Performance Benchmarks

This document tracks performance metrics across different versions of RustyChickpeas.

> **Note**: This file is auto-generated. To update it, run:
> ```bash
> ./scripts/generate_performance_readme.py
> ```

## How to Update

1. Run benchmarks for a specific tag:
   ```bash
   ./scripts/benchmark_tag.sh v0.4.0
   ```

2. Generate this README:
   ```bash
   ./scripts/generate_performance_readme.py
   ```

3. Commit the updated README.

---

### bulk_load_nodes

| Size | current |
|---|---|
| 100,000 | 23.41 ms |
| 1,000,000 | 356.21 ms |
