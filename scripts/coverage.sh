#!/bin/bash
# Run Rust test coverage and generate reports

set -eo pipefail

echo "🔍 Running Rust test coverage..."
echo ""

# Create coverage directory
mkdir -p coverage/rust

# Run coverage once, producing terminal, LCOV, and XML reports in a single pass
TARPAULIN_LOG=$(mktemp)
trap 'rm -f "$TARPAULIN_LOG"' EXIT
cargo tarpaulin \
    --package rustychickpeas-core \
    --out Stdout \
    --out Lcov \
    --out Xml \
    --output-dir coverage/rust \
    --timeout 300 \
    --root . \
    --exclude-files 'rustychickpeas-python/*' \
    --exclude-files 'tests/*' 2>&1 | tee "$TARPAULIN_LOG"

# Move lcov.info to coverage/rust/ if it's in the root
if [[ -f lcov.info ]]; then
    mv lcov.info coverage/rust/lcov.info
fi

# Filter LCOV file to ensure it's valid
if [[ -f coverage/rust/lcov.info ]]; then
    grep -E '^(SF:|TN:|FNF:|FNH:|FNDA:|DA:|LF:|LH:|BRDA:|BRF:|BRH:|end_of_record)' coverage/rust/lcov.info > coverage/rust/lcov.info.tmp || true
    if [[ -s coverage/rust/lcov.info.tmp ]]; then
        mv coverage/rust/lcov.info.tmp coverage/rust/lcov.info
    fi
fi

echo ""
echo "✅ Coverage report generated!"
echo ""
echo "📊 Coverage Summary:"
grep -A 20 "Coverage Results:" "$TARPAULIN_LOG" || true

echo ""
echo "📁 Reports:"
echo "  - LCOV: coverage/rust/lcov.info"
echo "  - XML:  coverage/rust/cobertura.xml"
echo "  - HTML: coverage/rust/tarpaulin-report.html"
echo ""
echo "💡 To view HTML report:"
echo "   open coverage/rust/tarpaulin-report.html"
