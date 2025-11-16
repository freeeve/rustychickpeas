#!/bin/bash
# Test coverage script for Rust and Python

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== RustyChickpeas Test Coverage ==="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if cargo-tarpaulin is installed
if ! command -v cargo-tarpaulin &> /dev/null; then
    echo -e "${YELLOW}Installing cargo-tarpaulin...${NC}"
    cargo install cargo-tarpaulin
fi

# Check if pytest-cov is installed (Python)
cd "$PROJECT_ROOT/rustychickpeas-python"
if ! python3 -c "import pytest_cov" 2>/dev/null; then
    echo -e "${YELLOW}Installing pytest-cov...${NC}"
    pip install pytest-cov
fi

echo ""
echo "=== Running Rust Test Coverage ==="
cd "$PROJECT_ROOT"

# Run Rust coverage
cargo tarpaulin \
    --workspace \
    --exclude-files '*/tests/*' \
    --exclude-files '*/benches/*' \
    --exclude-files '*/target/*' \
    --exclude-files '*/examples/*' \
    --out Xml \
    --out Html \
    --output-dir coverage/rust \
    --timeout 300 \
    --follow-exec \
    || {
    echo -e "${RED}Rust coverage failed${NC}"
    exit 1
}

echo ""
echo -e "${GREEN}✓ Rust coverage complete${NC}"
echo "  HTML report: coverage/rust/tarpaulin-report.html"
echo "  XML report: coverage/rust/cobertura.xml"

echo ""
echo "=== Running Python Test Coverage ==="
cd "$PROJECT_ROOT/rustychickpeas-python"

# Build the Python extension first
echo "Building Python extension..."
maturin develop --release || {
    echo -e "${RED}Failed to build Python extension${NC}"
    exit 1
}

# Run Python coverage
pytest \
    --cov=rustychickpeas \
    --cov-report=html:coverage/python/htmlcov \
    --cov-report=xml:coverage/python/coverage.xml \
    --cov-report=term \
    tests/ \
    || {
    echo -e "${RED}Python coverage failed${NC}"
    exit 1
}

echo ""
echo -e "${GREEN}✓ Python coverage complete${NC}"
echo "  HTML report: coverage/python/htmlcov/index.html"
echo "  XML report: coverage/python/coverage.xml"

echo ""
echo -e "${GREEN}=== Coverage Complete ===${NC}"
echo ""
echo "Rust coverage: coverage/rust/tarpaulin-report.html"
echo "Python coverage: coverage/python/htmlcov/index.html"

