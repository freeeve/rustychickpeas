# Quick Start: Lightweight Benchmark Tracking

This is a zero-infrastructure benchmark tracking system that stores results in CSV and generates SVG charts.

## What It Does

1. **Collects** benchmark results from Criterion → CSV files
2. **Tracks** both time (nanoseconds) and allocations
3. **Generates** SVG sparklines and badges
4. **Commits** everything to git (no external services needed)

## Usage

### Local Development

```bash
# 1. Run benchmarks
cargo bench --bench graph_builder --bench graph_snapshot --bench bulk_load

# 2. Collect results
python3 bench/collect.py

# 3. Generate charts
python3 bench/plot.py
```

### Automated (GitHub Actions)

The `.github/workflows/bench.yml` workflow runs automatically on pushes and PRs.

## Output

### CSV Files
- `bench/results/{machine}.csv` - Append-only CSV with all historical data

### SVG Charts
- `bench/img/{bench}_time.svg` - Time performance sparkline
- `bench/img/{bench}_time_badge.svg` - Current time + % change badge
- `bench/img/{bench}_allocs.svg` - Allocation sparkline (if available)
- `bench/img/{bench}_allocs_badge.svg` - Current allocations + % change badge

## Embedding in README

```markdown
### Performance

**builder_add_nodes/10000**

Time: <img src="bench/img/builder_add_nodes_10000_time_badge.svg" alt="time" />
<br/>
<img src="bench/img/builder_add_nodes_10000_time.svg" alt="time trend" />

Allocations: <img src="bench/img/builder_add_nodes_10000_allocs_badge.svg" alt="allocs" />
<br/>
<img src="bench/img/builder_add_nodes_10000_allocs.svg" alt="allocs trend" />
```

## CSV Format

```csv
commit,date,bench,metric,unit,value,ci_low,ci_high,allocs,allocs_ci_low,allocs_ci_high,rustc,machine
abc123,2024-12-15T10:30:00Z,builder_add_nodes/10000,mean,ns,2450000,2400000,2500000,1024,1000,1050,rustc 1.75.0,github
```

- **value**: Time in nanoseconds
- **allocs**: Allocation count (if available)
- All fields include confidence intervals

## Badge Colors

- 🟢 Green: >5% improvement (faster/less allocations)
- 🟡 Yellow: ±5% change (neutral)
- 🔴 Red: >5% regression (slower/more allocations)

## Files

- `bench/collect.py` - Extracts Criterion results → CSV
- `bench/plot.py` - Generates SVG charts from CSV
- `bench/README.md` - Full documentation

## Benefits

✅ Zero infrastructure - everything in git  
✅ Deterministic - every commit tracked  
✅ Markdown-friendly - plain SVGs  
✅ Portable - works anywhere  
✅ Historical - complete CSV history  
✅ Visual - sparklines show trends  

