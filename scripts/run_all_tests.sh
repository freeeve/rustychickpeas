#!/bin/bash
# Run all standard tests for Rust and Python

set -e

echo "🧪 Running all tests (Rust + Python)..."
echo ""

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Track overall success
RUST_TESTS_PASSED=true
PYTHON_TESTS_PASSED=true

# ============================================================================
# Rust Tests
# ============================================================================
echo "🦀 Running Rust tests..."
echo ""
echo "Note: Testing only rustychickpeas-core (Python bindings require Python environment)"
echo ""

if cargo test --package rustychickpeas-core --release 2>&1; then
    echo ""
    echo "✅ Rust tests passed!"
else
    echo ""
    echo "❌ Rust tests failed!"
    RUST_TESTS_PASSED=false
fi

echo ""
echo "─────────────────────────────────────────────────────────"
echo ""

# ============================================================================
# Python Tests
# ============================================================================
echo "🐍 Running Python tests..."
echo ""

cd rustychickpeas-python

# Check if venv exists, create if not
if [[ ! -d ".venv" ]]; then
    echo "Creating Python virtual environment..."
    python -m venv .venv
fi

# Activate venv
if [[ -f ".venv/bin/activate" ]]; then
    source .venv/bin/activate
elif [[ -f ".venv/Scripts/activate" ]]; then
    source .venv/Scripts/activate
else
    echo "⚠️  Warning: Could not find venv activation script"
fi

# Install/upgrade dependencies
echo "Installing Python dependencies..."
python -m pip install --upgrade pip --quiet
python -m pip install maturin pytest pyarrow --quiet

# Build the extension
echo "Building Python extension..."

# Set PyO3 compatibility flag for Python 3.13/3.14 if needed
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
if [[ "$PYTHON_VERSION" == "3.13" || "$PYTHON_VERSION" == "3.14" ]]; then
    export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
    echo "Using ABI3 forward compatibility for Python $PYTHON_VERSION"
fi

if maturin develop --release 2>&1; then
    echo "✓ Extension built successfully"
else
    echo "❌ Extension build failed!"
    PYTHON_TESTS_PASSED=false
    cd "$PROJECT_ROOT"
    echo ""
    echo "─────────────────────────────────────────────────────────"
    echo ""
    echo "📊 Test Summary:"
    echo ""
    if [[ "$RUST_TESTS_PASSED" = true ]]; then
        echo "  ✅ Rust tests: PASSED"
    else
        echo "  ❌ Rust tests: FAILED"
    fi
    echo "  ❌ Python extension: BUILD FAILED"
    echo ""
    echo "⚠️  Cannot run Python tests - extension build failed."
    exit 1
fi

# Run Python tests
echo ""
echo "Running pytest..."
if python -m pytest tests/ -v; then
    echo ""
    echo "✅ Python tests passed!"
else
    echo ""
    echo "❌ Python tests failed!"
    PYTHON_TESTS_PASSED=false
fi

cd ..

echo ""
echo "─────────────────────────────────────────────────────────"
echo ""

# ============================================================================
# Summary
# ============================================================================
echo "📊 Test Summary:"
echo ""

if [[ "$RUST_TESTS_PASSED" = true ]]; then
    echo "  ✅ Rust tests: PASSED"
else
    echo "  ❌ Rust tests: FAILED"
fi

if [[ "$PYTHON_TESTS_PASSED" = true ]]; then
    echo "  ✅ Python tests: PASSED"
else
    echo "  ❌ Python tests: FAILED"
fi

echo ""

if [[ "$RUST_TESTS_PASSED" = true ]] && [[ "$PYTHON_TESTS_PASSED" = true ]]; then
    echo "🎉 All tests passed!"
    exit 0
else
    echo "⚠️  Some tests failed. See output above for details."
    exit 1
fi

