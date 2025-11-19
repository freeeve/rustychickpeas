#!/bin/bash
# Script to compare benchmarks between two tags
# Usage: ./scripts/compare_benchmarks.sh <baseline_tag> <current_tag>
# Example: ./scripts/compare_benchmarks.sh v0.3.0 v0.4.0

set -e

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <baseline_tag> <current_tag>"
    echo "Example: $0 v0.3.0 v0.4.0"
    exit 1
fi

BASELINE_TAG=$1
CURRENT_TAG=$2

echo "Comparing benchmarks: $BASELINE_TAG (baseline) vs $CURRENT_TAG (current)"

# Check if tags exist
if ! git rev-parse "$BASELINE_TAG" >/dev/null 2>&1; then
    echo "Error: Tag $BASELINE_TAG does not exist"
    exit 1
fi

if ! git rev-parse "$CURRENT_TAG" >/dev/null 2>&1; then
    echo "Error: Tag $CURRENT_TAG does not exist"
    exit 1
fi

# Get current branch/commit
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_COMMIT=$(git rev-parse HEAD)

# First, run benchmarks for baseline tag
echo "Running benchmarks for baseline: $BASELINE_TAG..."
git checkout "$BASELINE_TAG" 2>/dev/null || git checkout -b "benchmark-$BASELINE_TAG" "$BASELINE_TAG"
cargo build --release --benches
export BENCHMARK_BASELINE="$BASELINE_TAG"
cargo bench --bench graph_builder --bench graph_snapshot --bench bulk_load

# Then, run benchmarks for current tag (comparing against baseline)
echo "Running benchmarks for current: $CURRENT_TAG (comparing against $BASELINE_TAG)..."
git checkout "$CURRENT_TAG" 2>/dev/null || git checkout -b "benchmark-$CURRENT_TAG" "$CURRENT_TAG"
cargo build --release --benches
export BENCHMARK_BASELINE="$BASELINE_TAG"
cargo bench --bench graph_builder --bench graph_snapshot --bench bulk_load

# Return to original branch/commit
echo "Returning to original branch..."
git checkout "$CURRENT_BRANCH" 2>/dev/null || git checkout "$CURRENT_COMMIT"

echo ""
echo "Benchmark comparison completed!"
echo "Results saved in target/criterion/"
echo "Open target/criterion/<bench_name>/report/index.html to view comparison"

