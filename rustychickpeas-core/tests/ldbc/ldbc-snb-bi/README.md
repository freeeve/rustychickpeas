# LDBC Social Network Benchmark - Business Intelligence (BI) Queries

This directory contains benchmark tests for the LDBC Social Network Benchmark's Business Intelligence (BI) workload.

## Overview

The LDBC SNB BI workload consists of complex analytical queries designed to test graph database performance on business intelligence workloads. These queries involve:
- Graph pattern searches
- Hierarchy traversals
- Aggregations
- Path finding
- Community detection

## Data Requirements

The tests expect LDBC SNB BI data in Parquet format. By default, the tests will try to use **SF10** (Scale Factor 10) data, and automatically fall back to **SF0.003** if SF10 is not found.

### Default Data Location

The tests look for data in:
```
../ldbc_data/social-network-sf{SF}-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot/
```

Where `{SF}` is the scale factor (default: `10`).

### Configuring Scale Factor

You can specify which scale factor to use with the `LDBC_SF` environment variable:

```bash
# Use SF10 (default, ~30M persons, ~1B relationships)
export LDBC_SF=10
cargo test --test ldbc_snb_bi_benchmark --release

# Use SF1 (~3M persons, ~100M relationships)
export LDBC_SF=1
cargo test --test ldbc_snb_bi_benchmark --release

# Use SF0.003 (~3K persons, ~100K relationships)
export LDBC_SF=0.003
cargo test --test ldbc_snb_bi_benchmark --release
```

### Custom Data Directory

You can override the data directory entirely by setting the `LDBC_DATA_DIR` environment variable:
```bash
export LDBC_DATA_DIR=/path/to/ldbc/data/initial_snapshot
cargo test --test ldbc_snb_bi_benchmark --release
```

### Scale Factor Sizes

- **SF0.003**: ~3K persons, ~10K posts, ~30K comments, ~100K relationships (~100MB)
  - Good for: Quick testing, development
- **SF1**: ~3M persons, ~10M posts, ~30M comments, ~100M relationships (~100GB uncompressed, ~10GB compressed)
  - Good for: Realistic benchmarking, performance testing
  - Recommended for most benchmark runs
- **SF10**: ~30M persons, ~100M posts, ~300M comments, ~1B relationships (~1TB uncompressed, ~20GB compressed)
  - Good for: Large-scale stress testing
  - Requires: 100GB+ RAM, significant disk space

**Note**: The tests automatically try SF10 → SF1 → SF0.003 in order until data is found.

### Downloading Datasets

To download SF1 or SF10:

1. Visit: https://ldbcouncil.org/data-sets-surf-repository/
2. Navigate to: SNB Business Intelligence (BI) section
3. Find the desired scale factor (SF1 or SF10)
4. Click "Request" if the dataset needs to be staged (takes a few minutes)
5. Copy the download URL
6. Use the download script:
   ```bash
   # For SF1
   ./scripts/download_ldbc_sf1.sh
   # Paste the URL when prompted
   
   # For SF10
   ./scripts/download_ldbc_sf10_direct.sh
   # Paste the URL when prompted
   ```

Or download manually:
```bash
cd ldbc_data
curl -L -o bi-sf1-composite-projected-fk.tar.zst <URL>
tar -xvf bi-sf1-composite-projected-fk.tar.zst --use-compress-program=zstd
```

## Running the Benchmarks

### Run All BI Query Benchmarks

```bash
cd rustychickpeas-core
cargo test --test ldbc_snb_bi_benchmark --release -- --nocapture
```

### Run a Specific Query

```bash
# Run BI1 (Tag Evolution)
cargo test --test ldbc_snb_bi_benchmark bi1 --release -- --nocapture

# Run BI2 (Tag Person Path)
cargo test --test ldbc_snb_bi_benchmark bi2 --release -- --nocapture

# Run all queries in sequence
cargo test --test ldbc_snb_bi_benchmark run_all_bi_queries --release -- --nocapture
```

### Run as Release Benchmarks

For performance testing, always use `--release`:

```bash
cargo test --test ldbc_snb_bi_benchmark --release -- --nocapture
```

## Implemented Queries

Currently implemented BI queries:

1. **BI1: Tag Evolution** - Find tags that are used together in posts/comments
2. **BI2: Tag Person Path** - Find paths between persons through shared tags
3. **BI3: Popular Topics** - Find the most popular tags (by number of posts/comments)
4. **BI4: Top Commenters** - Find persons who have made the most comments
5. **BI5: Active Users** - Find persons with the most posts
6. **BI6: Tag Co-occurrence** - Find tags that frequently appear together

## Data Schema

The tests load the following LDBC SNB entities:

### Node Types
- **Person**: Users in the social network
- **Forum**: Discussion forums
- **Post**: Forum posts
- **Comment**: Comments on posts
- **Tag**: Tags for categorization
- **TagClass**: Tag hierarchies
- **Place**: Geographic locations
- **Organisation**: Companies/organizations

### Relationship Types
- **knows**: Person knows Person
- **hasInterest**: Person has interest in Tag
- **likes**: Person likes Post/Comment
- **hasMember**: Forum has member Person
- **hasTag**: Post/Comment/Forum has Tag

## Performance Tracking

These benchmarks are designed to be run on each release to track performance regressions. The output includes:
- Query execution time
- Number of results found
- Graph loading time
- Total benchmark suite time

Example output:
```
Loading LDBC SNB BI graph from: /path/to/data
Loaded graph: 3000 nodes, 100000 relationships in 2.45s

--- Running BI Queries ---

BI1: Found 150 tag pairs in 45.123ms
BI1 completed in 45.123ms

BI2: Found 500 tag-person paths in 120.456ms
BI2 completed in 120.456ms

=== Total Benchmark Time: 3.123s ===
```

## Extending the Benchmarks

To add more BI queries:

1. Add a new test function following the pattern:
```rust
#[test]
fn bi7_your_query_name() {
    let graph = load_ldbc_graph();
    let start = Instant::now();
    
    // Your query implementation
    
    let elapsed = start.elapsed();
    println!("BI7: Found {} results in {:.3}ms", results.len(), elapsed.as_secs_f64() * 1000.0);
}
```

2. Add the query to the `run_all_bi_queries` test function

3. Update this README with the new query description

## Troubleshooting

### "No such file or directory" errors

Make sure the LDBC data directory exists and contains the expected Parquet files. Check the path:
```bash
ls -la ../../../../ldbc_data/social-network-sf0.003-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot/
```

### "Column not found" errors

The LDBC data schema may vary. You may need to adjust column names in `load_ldbc_graph()` to match your dataset. Common column name variations:
- `Person1id` / `Person2id` vs `from` / `to`
- `Personid` vs `PersonId`
- Relationship type columns may be named differently

### Out of memory errors

For larger datasets (SF1+), ensure you have sufficient RAM. The SF0.003 dataset requires approximately 2-4GB of RAM.

## References

- **LDBC SNB Specification**: https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf
- **LDBC SNB BI Repository**: https://github.com/ldbc/ldbc_snb_bi
- **LDBC Datagen**: https://github.com/ldbc/ldbc_snb_datagen_spark

