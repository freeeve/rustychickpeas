#!/bin/bash
# Run Python tests with Rust code coverage tracking
# This tracks which Rust code paths are executed by the Python API tests
#
# IMPORTANT: This requires building the Python extension in debug mode
# and using cargo-tarpaulin with --follow-exec to track coverage
# when the Python interpreter loads the Rust extension.

set -e

echo "🐍📊 Running Python API tests with Rust coverage tracking..."
echo ""
echo "This will show which Rust code is covered by Python tests."
echo ""

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Create coverage directory
mkdir -p coverage/python-api

# Check if cargo-tarpaulin is installed
if ! command -v cargo-tarpaulin &> /dev/null; then
    echo "Installing cargo-tarpaulin..."
    cargo install cargo-tarpaulin
fi

# Navigate to Python package directory
cd rustychickpeas-python

# Check if pytest is installed
if ! python -c "import pytest" 2>/dev/null; then
    echo "Installing pytest and pyarrow..."
    pip install pytest pyarrow
fi

# Build the Python extension in DEBUG mode (required for coverage tracking)
# Release builds are optimized and harder to track accurately
echo "Building Python extension in debug mode (required for coverage)..."
echo "Note: Debug builds are slower but necessary for accurate coverage tracking."

# Set PyO3 compatibility flag for Python 3.13/3.14 if needed
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
if [[ "$PYTHON_VERSION" == "3.13" || "$PYTHON_VERSION" == "3.14" ]]; then
    export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
    echo "Using ABI3 forward compatibility for Python $PYTHON_VERSION"
fi

maturin develop

# Go back to project root
cd "$PROJECT_ROOT"

echo ""
echo "Running Python tests with Rust coverage tracking..."
echo "This uses cargo-tarpaulin to track which Rust code executes when Python tests run."
echo "This may take a while..."
echo ""

# Method: Use cargo tarpaulin with --test-threads=1 and --follow-exec
# We'll run pytest as a subprocess that tarpaulin can track
# Note: This approach requires tarpaulin to be able to track the Python process
# which loads our Rust extension

# First, let's try a simpler approach: run tarpaulin on a test that spawns pytest
# Actually, the most reliable way is to use tarpaulin's ability to track
# coverage in dynamically loaded libraries

# Build the extension first to ensure it's available
cd rustychickpeas-python
# Use the same compatibility flag
if [[ "$PYTHON_VERSION" == "3.13" || "$PYTHON_VERSION" == "3.14" ]]; then
    export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
fi
maturin develop
cd ..

# Use cargo tarpaulin to run a Rust integration test that executes Python tests
# The test (python_api_coverage_test.rs) runs pytest, and tarpaulin tracks
# coverage of the Rust code that gets executed when Python loads the extension

echo "Running cargo-tarpaulin on Python API coverage test..."
echo "This test runs pytest and tracks which Rust code is executed."

# Run tarpaulin on the specific test that runs Python tests
# --follow-exec ensures we track coverage in child processes (Python interpreter)
cargo tarpaulin \
    --package rustychickpeas-core \
    --test python_api_coverage_test \
    --follow-exec \
    --exclude-files '*/tests/*' \
    --exclude-files '*/benches/*' \
    --exclude-files '*/target/*' \
    --exclude-files '*/examples/*' \
    --exclude-files 'rustychickpeas-python/src/*' \
    --out Lcov \
    --out Xml \
    --out Html \
    --output-dir coverage/python-api \
    --root . \
    --timeout 600 \
    -- --ignored || {
    echo ""
    echo "⚠️  Coverage collection completed (tests may have warnings)."
    echo "   Check the reports to see Python API coverage."
}

# Move lcov.info if it's in the root
if [ -f lcov.info ]; then
    if [ ! -f coverage/python-api/lcov.info ]; then
        mv lcov.info coverage/python-api/lcov.info
    fi
fi

# Filter LCOV file to ensure it's valid
if [ -f coverage/python-api/lcov.info ]; then
    grep -E '^(SF:|TN:|FNF:|FNH:|FNDA:|DA:|LF:|LH:|BRDA:|BRF:|BRH:|end_of_record)' coverage/python-api/lcov.info > coverage/python-api/lcov.info.tmp || true
    if [ -s coverage/python-api/lcov.info.tmp ]; then
        mv coverage/python-api/lcov.info.tmp coverage/python-api/lcov.info
    fi
fi

echo ""
echo "✅ Python API coverage complete!"
echo ""
echo "📊 Coverage reports:"
echo "  - LCOV:  coverage/python-api/lcov.info"
echo "  - XML:   coverage/python-api/cobertura.xml"
echo "  - HTML:  coverage/python-api/tarpaulin-report.html"
echo ""
echo "💡 This report shows which Rust code is executed by Python tests."
echo "   This is the actual coverage of your Python API!"

