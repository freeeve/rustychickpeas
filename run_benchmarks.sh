#!/bin/bash
# Script to run RustyChickpeas benchmarks

set -e

echo "RustyChickpeas Benchmark Runner"
echo "================================"
echo ""

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    echo "Error: Cargo is not installed or not in PATH"
    echo ""
    echo "To install Rust and Cargo:"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    echo "  source ~/.cargo/env"
    echo ""
    exit 1
fi

echo "Cargo version: $(cargo --version)"
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo "Error: Cargo.toml not found. Please run from project root."
    exit 1
fi

echo "Running all benchmarks..."
echo ""

# Run all benchmarks
cargo bench

echo ""
echo "Benchmarks complete!"
echo ""
echo "To run specific benchmark suites:"
echo "  cargo bench --bench basic_operations"
echo "  cargo bench --bench query_operations"
echo "  cargo bench --bench traversal_operations"
echo "  cargo bench --bench bitmap_operations"
echo ""
echo "To generate HTML reports:"
echo "  cargo bench -- --output-format html"
echo ""

