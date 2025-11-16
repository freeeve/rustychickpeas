# Setup Instructions

## Prerequisites

### Installing Rust

If Rust/Cargo is not installed, install it using rustup:

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add to PATH (or restart terminal)
source ~/.cargo/env

# Verify installation
cargo --version
rustc --version
```

## Running Benchmarks

Once Rust is installed, you can run benchmarks:

### Quick Start

```bash
# Run all benchmarks
cargo bench

# Or use the provided script
./run_benchmarks.sh
```

### Specific Benchmark Suites

```bash
# Basic operations (create/delete nodes, relationships)
cargo bench --bench basic_operations

# Query operations (read operations, lookups)
cargo bench --bench query_operations

# Traversal operations (BFS, shortest path)
cargo bench --bench traversal_operations

# Bitmap operations (set operations)
cargo bench --bench bitmap_operations
```

### Generate HTML Reports

```bash
cargo bench -- --output-format html
```

Reports will be in `target/criterion/`.

### First Time Setup

On first run, Criterion will compile the benchmarks. This may take a few minutes.

## Troubleshooting

### Cargo not found

If you see "cargo: command not found":
1. Install Rust using rustup (see above)
2. Make sure `~/.cargo/bin` is in your PATH
3. Run `source ~/.cargo/env` or restart your terminal

### Compilation Errors

If you see compilation errors:
1. Make sure you're using Rust 1.70+ (check with `rustc --version`)
2. Update Rust: `rustup update`
3. Clean and rebuild: `cargo clean && cargo bench`

### Benchmark Not Found

If you see "benchmark not found":
1. Make sure you're in the project root (`rustychickpeas/`)
2. Check that `rustychickpeas-core/benches/` directory exists
3. Verify `Cargo.toml` has the `[[bench]]` sections

