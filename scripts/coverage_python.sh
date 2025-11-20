#!/bin/bash
# Run Python test coverage and generate LCOV format for Coveralls

set -e

echo "🐍 Running Python test coverage..."
echo ""

# Navigate to Python package directory
cd "$(dirname "$0")/../rustychickpeas-python"

# Create coverage directory
mkdir -p ../../coverage/python

# Check if pytest-cov is installed
if ! python -c "import pytest_cov" 2>/dev/null; then
    echo "Installing pytest-cov and coverage..."
    pip install pytest-cov coverage[toml]
fi

# Build the Python extension first
echo "Building Python extension..."
maturin develop --release

# Run pytest with coverage
echo ""
echo "Running tests with coverage..."
pytest \
    --cov=rustychickpeas \
    --cov-report=term \
    --cov-report=html:../../coverage/python/htmlcov \
    --cov-report=xml:../../coverage/python/coverage.xml \
    --cov-report=lcov:../../coverage/python/lcov.info \
    tests/ -v

echo ""
echo "✅ Python coverage complete!"
echo ""
echo "📊 Coverage reports:"
echo "  - LCOV:  ../../coverage/python/lcov.info"
echo "  - XML:   ../../coverage/python/coverage.xml"
echo "  - HTML:  ../../coverage/python/htmlcov/index.html"
echo ""
echo "💡 Note: Since rustychickpeas is a Rust extension module,"
echo "   coverage will show 0% for the extension code itself."
echo "   This report tracks which Python test code is executed."

