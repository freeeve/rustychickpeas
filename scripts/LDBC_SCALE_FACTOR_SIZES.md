# LDBC SNB Scale Factor Sizes and Memory Requirements

## Dataset Sizes (Parquet Format)

### SF1 (Scale Factor 1)
- **Persons**: ~3M (10,620 base persons × scale factor multiplier)
- **Posts**: ~10M
- **Comments**: ~30M
- **Relationships**: ~100M
- **Parquet Size**: ~10GB compressed
- **Uncompressed CSV**: ~100GB
- **Generation Time**: ~1-3 hours
- **Memory after load**: ~50-100GB (estimated)

### SF10 (Scale Factor 10)
- **Persons**: ~30M (70,800 base persons × scale factor multiplier)
- **Posts**: ~100M
- **Comments**: ~300M
- **Relationships**: ~1B
- **Parquet Size**: ~20GB compressed
- **Uncompressed CSV**: ~1TB
- **Generation Time**: ~3-6 hours
- **Memory after load**: ~500GB-1TB (estimated)

### SF30 (Scale Factor 30)
- **Persons**: ~90M (175,950 base persons × scale factor multiplier)
- **Posts**: ~300M
- **Comments**: ~900M
- **Relationships**: ~3B
- **Parquet Size**: ~60GB compressed (estimated: 3× SF10)
- **Uncompressed CSV**: ~3TB (estimated)
- **Generation Time**: ~9-18 hours
- **Memory after load**: ~1.5TB-3TB (estimated)

## Memory Estimation Details

Memory usage depends on:
- **Base overhead**: ~3.5 bytes per node/relationship (structure + indexes)
- **Properties**: Additional memory per property (varies by type and size)
- **String interning**: Can reduce memory by 32-50% for high-duplication properties
- **Index structures**: Label/type indexes, property indexes, inverted indexes
- **Graph connectivity**: Actual relationship patterns affect memory layout

### Rough Memory Formula
```
Memory ≈ (nodes × 3.5 bytes) + (relationships × 3.5 bytes) + (properties × avg_size) + (indexes)
```

For SF10 with properties:
- ~500M entities (nodes + relationships) × 3.5 bytes = ~1.75GB base
- Properties can add 10-100× this amount depending on property count and size
- Indexes add another 20-50% overhead
- **Total**: ~500GB-1TB is a reasonable estimate

## Generation Commands

### Generate SF10
```bash
cd ldbc_snb_datagen_spark
export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
export SPARK_HOME="${HOME}/spark-3.2.2-bin-hadoop3.2"
export PATH="${SPARK_HOME}/bin:${PATH}"
export LDBC_SNB_DATAGEN_JAR="$(pwd)/target/ldbc_snb_datagen_2.12_spark3.2-0.5.1+23-1d60a657-jar-with-dependencies.jar"
python3 tools/run.py -y -- --format parquet --scale-factor 10 --mode bi
```

### Generate SF30
```bash
# Same setup as above, then:
python3 tools/run.py -y -- --format parquet --scale-factor 30 --mode bi
```

### Or use the automated script
```bash
./scripts/generate_ldbc_sf10_sf30.sh
```

## Expected Output Locations

After generation, data will be in:
- SF10: `ldbc_data/social-network-sf10-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot/`
- SF30: `ldbc_data/social-network-sf30-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot/`

## Using with Benchmarks

```bash
# For SF10
export LDBC_SF=10
cd rustychickpeas-core
cargo test --test ldbc_snb_bi_benchmark --release

# For SF30
export LDBC_SF=30
cd rustychickpeas-core
cargo test --test ldbc_snb_bi_benchmark --release
```

## Notes

- Generation times are estimates and depend on hardware (CPU cores, disk I/O, memory)
- Memory estimates are rough and actual usage may vary significantly
- SF30 requires substantial resources - ensure you have:
  - 60GB+ free disk space for parquet files
  - 1.5TB+ RAM for loading into memory
  - Several hours for generation

