# RustyChickpeas

High-performance in-memory graph database written in Rust with Python bindings via PyO3. Uses RoaringBitmaps and CSR (Compressed Sparse Row) adjacency for efficient graph operations.

- **License:** MIT
- **Version:** 0.6.0
- **Repository:** https://github.com/freeeve/rustychickpeas
- **PyPI:** `pip install rustychickpeas`

## Architecture

Three-layer design:

1. **Builder Layer** (`GraphBuilder`) — Mutable staging area for constructing graphs. Supports direct add_node/add_rel calls and bulk loading from CSV/Parquet/S3.
2. **Snapshot Layer** (`GraphSnapshot`) — Immutable, read-optimized graph. CSR adjacency, columnar properties, lazy-built inverted indexes.
3. **Manager Layer** (`RustyChickpeas`) — Thread-safe version management for multiple snapshots via `Arc<RwLock<HashMap>>`.

## Workspace Layout

```
rustychickpeas/
├── Cargo.toml                    # Workspace root (members: core + python)
├── rustychickpeas-core/          # Rust core library
│   ├── src/
│   │   ├── lib.rs                # Module exports
│   │   ├── rusty_chickpeas.rs    # Manager (multi-version snapshots)
│   │   ├── graph_snapshot.rs     # Immutable graph (CSR, columnar props) ~1400 lines
│   │   ├── graph_builder.rs      # Mutable builder ~1400 lines
│   │   ├── graph_builder_csv.rs  # CSV/gzip bulk loading ~1000 lines
│   │   ├── graph_builder_parquet.rs # Parquet/S3 bulk loading ~600 lines
│   │   ├── interner.rs           # Thread-safe string interning (lasso)
│   │   ├── bitmap.rs             # Adaptive NodeSet (RoaringBitmap or BitVec)
│   │   ├── types.rs              # Core types (NodeId=u32, Direction, PropertyValue, etc.)
│   │   └── error.rs              # GraphError enum
│   ├── benches/                  # Criterion benchmarks
│   │   ├── graph_builder.rs      # Builder operation benchmarks
│   │   ├── graph_snapshot.rs     # Query/traversal benchmarks
│   │   ├── graph_builder_parquet.rs
│   │   └── ldbc_snb_bi.rs        # LDBC Social Network BI workload
│   └── tests/                    # Integration tests
│       ├── ldbc_snb_bi_benchmark.rs
│       ├── python_api_coverage_test.rs
│       └── s3_integration_test.rs
├── rustychickpeas-python/        # PyO3 Python bindings
│   ├── Cargo.toml
│   ├── pyproject.toml            # Maturin build config, pytest config
│   ├── src/
│   │   ├── lib.rs                # PyO3 module registration
│   │   ├── rusty_chickpeas.rs    # Python RustyChickpeas wrapper
│   │   ├── graph_snapshot.rs     # Python GraphSnapshot wrapper ~935 lines
│   │   ├── graph_snapshot_builder.rs # Python builder wrapper ~647 lines
│   │   ├── node.rs               # Python Node wrapper
│   │   ├── relationship.rs       # Python Relationship wrapper
│   │   ├── direction.rs          # Direction enum
│   │   └── utils.rs              # PyAny -> PropertyValue conversion
│   └── tests/                    # Python tests (pytest)
│       ├── test_graph_snapshot.py
│       ├── test_graph_snapshot_builder.py
│       ├── test_node.py, test_relationship.py, test_direction.py
│       ├── test_rusty_chickpeas.py
│       ├── test_bulk_load_parquet.py
│       ├── test_parquet_deduplication.py
│       └── benchmark_*.py        # Performance benchmarks
├── .github/workflows/
│   ├── ci.yml                    # Multi-platform CI (Linux/macOS/Windows, Python 3.10-3.14)
│   ├── bench.yml                 # Benchmark runner with chart generation
│   └── publish.yml               # PyPI wheel publishing pipeline
├── scripts/                      # Dev utility scripts
│   ├── run_all_tests.sh          # Master test runner (Rust + Python)
│   ├── coverage.sh               # Rust coverage (tarpaulin)
│   ├── coverage_python.sh        # Python test coverage
│   ├── coverage_python_api.sh    # Rust coverage from Python tests
│   ├── run_benchmark_workflow.sh  # Full benchmark pipeline
│   ├── compare_benchmarks.sh     # Compare two git tags
│   └── create_release_tag.sh     # GPG-signed release tagging
├── bench/                        # Benchmark collection, plotting, results
│   ├── collect.py                # Collect Criterion results
│   ├── plot.py                   # Generate SVG charts
│   └── results/                  # Historical benchmark data
└── run_benchmarks.sh             # Top-level benchmark entry point
```

## Key Data Structures

- **NodeId** = `u32` (max ~4.3B nodes)
- **NodeSet** — Adaptive: `RoaringBitmap` for large sets, `BitVec` for sets <= 256 nodes
- **CSR adjacency** — `out_neighbors`/`in_neighbors` arrays with parallel type arrays
- **Column** — Dense or sparse columnar property storage (>80% fill = dense)
- **StringInterner** — Thread-safe string interning via `lasso::Rodeo` wrapped in `Arc<RwLock>`
- **ValueId** — Tagged union (Str, I64, F64, Bool) for property values

## Key Dependencies

- `roaring` 0.11 — RoaringBitmap
- `lasso` 0.7 — String interning
- `parquet` / `arrow` 57.0 — Parquet I/O
- `object_store` 0.12 — S3/GCP/Azure access
- `rayon` 1.8 — Parallel finalization
- `pyo3` 0.20 — Python bindings
- `bitvec` 1.0 — Compact bitsets
- `hashbrown` 0.16 — Fast hash maps

## Development Commands

```bash
# Rust tests
cargo test --workspace
cargo test --package rustychickpeas-core --release

# Python development (build extension + test)
cd rustychickpeas-python
maturin develop --release
pytest tests/

# For Python 3.13+ compatibility
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

# Benchmarks
cargo bench                                    # All benchmarks
cargo bench --bench graph_builder              # Specific suite
./run_benchmarks.sh                            # Via script

# Coverage
./scripts/coverage.sh                          # Rust coverage
./scripts/coverage_python.sh                   # Python coverage

# Full test suite (Rust + Python)
./scripts/run_all_tests.sh
```

## Conventions

- Rust 2021 edition, workspace-level version management
- Semantic commit messages with module scope: `feat(core): ...`, `fix(python): ...`
- Python API follows Pythonic naming (snake_case methods)
- Builder pattern: mutable `GraphBuilder` -> immutable `GraphSnapshot` via `finalize()`
- Thread safety: `Arc<RwLock>` for shared state, `Mutex` for lazy indexes
- Dense/sparse column threshold: 80% fill rate
- Auto-growing capacity: starts at 2^20, doubles as needed
