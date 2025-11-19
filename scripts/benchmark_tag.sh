#!/bin/bash
# Script to run benchmarks for a specific git tag and save results
# Usage: ./scripts/benchmark_tag.sh <tag_name>
# Example: ./scripts/benchmark_tag.sh v0.4.0

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <tag_name>"
    echo "Example: $0 v0.4.0"
    exit 1
fi

TAG=$1

echo "Running benchmarks for tag: $TAG"

# Check if tag exists
if ! git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "Error: Tag $TAG does not exist"
    exit 1
fi

# Get current branch/commit
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_COMMIT=$(git rev-parse HEAD)

# Checkout the tag
echo "Checking out tag $TAG..."
git checkout "$TAG" 2>/dev/null || git checkout -b "benchmark-$TAG" "$TAG"

# Build the benchmarks
echo "Building benchmarks..."
cargo build --release --benches

# Run benchmarks with the tag name as baseline
echo "Running benchmarks..."
export BENCHMARK_BASELINE="$TAG"
cargo bench --bench graph_builder --bench graph_snapshot --bench bulk_load

# Return to original branch/commit
echo "Returning to original branch..."
git checkout "$CURRENT_BRANCH" 2>/dev/null || git checkout "$CURRENT_COMMIT"

echo "Benchmarks completed for tag: $TAG"
echo "Results saved in target/criterion/"
echo ""
echo "To compare with another tag, run:"
echo "  BENCHMARK_BASELINE=$TAG cargo bench --bench <bench_name>"

