#!/bin/bash
# Complete workflow for running benchmarks, saving results, and generating charts
# Usage: ./scripts/run_benchmark_workflow.sh [version_tag]

set -e

VERSION_TAG="${1:-}"

echo "RustyChickpeas Benchmark Workflow"
echo "=================================="
echo ""

# Step 1: Run benchmarks
echo "Step 1: Running benchmarks..."
if [ -n "$VERSION_TAG" ]; then
    echo "  Using version tag: $VERSION_TAG"
    export BENCHMARK_BASELINE="$VERSION_TAG"
fi

cargo bench --bench graph_builder --bench graph_snapshot --bench bulk_load

echo ""
echo "Step 2: Saving benchmark results to JSON..."
python3 scripts/save_benchmark_results.py ${VERSION_TAG:+--version "$VERSION_TAG"}

echo ""
echo "Step 3: Generating comparison charts and updating README..."
python3 scripts/generate_benchmark_charts.py --update-readme --compare

echo ""
echo "Benchmark workflow complete!"
echo ""
echo "Results:"
echo "  - JSON files: benchmarks/benchmarks_*.json"
echo "  - Charts: benchmarks/charts/*.png"
echo "  - README: rustychickpeas-core/benches/PERFORMANCE.md"
echo ""
echo "To view charts:"
echo "  open benchmarks/charts/"

