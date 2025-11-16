# Test coverage script for Rust and Python (PowerShell)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

Write-Host "=== RustyChickpeas Test Coverage ===" -ForegroundColor Cyan
Write-Host ""

# Check if cargo-tarpaulin is installed
$tarpaulinInstalled = Get-Command cargo-tarpaulin -ErrorAction SilentlyContinue
if (-not $tarpaulinInstalled) {
    Write-Host "Installing cargo-tarpaulin..." -ForegroundColor Yellow
    cargo install cargo-tarpaulin
}

# Check if pytest-cov is installed (Python)
Push-Location "$ProjectRoot\rustychickpeas-python"
try {
    python -c "import pytest_cov" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Installing pytest-cov..." -ForegroundColor Yellow
        pip install pytest-cov
    }
} catch {
    Write-Host "Installing pytest-cov..." -ForegroundColor Yellow
    pip install pytest-cov
}
Pop-Location

Write-Host ""
Write-Host "=== Running Rust Test Coverage ===" -ForegroundColor Cyan
Push-Location $ProjectRoot

# Run Rust coverage
cargo tarpaulin `
    --workspace `
    --exclude-files '*/tests/*' `
    --exclude-files '*/benches/*' `
    --exclude-files '*/target/*' `
    --exclude-files '*/examples/*' `
    --out Xml `
    --out Html `
    --output-dir coverage/rust `
    --timeout 300 `
    --follow-exec

if ($LASTEXITCODE -ne 0) {
    Write-Host "Rust coverage failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✓ Rust coverage complete" -ForegroundColor Green
Write-Host "  HTML report: coverage/rust/tarpaulin-report.html"
Write-Host "  XML report: coverage/rust/cobertura.xml"

Write-Host ""
Write-Host "=== Running Python Test Coverage ===" -ForegroundColor Cyan
Push-Location "$ProjectRoot\rustychickpeas-python"

# Build the Python extension first
Write-Host "Building Python extension..."
maturin develop --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build Python extension" -ForegroundColor Red
    exit 1
}

# Run Python coverage
pytest `
    --cov=rustychickpeas `
    --cov-report=html:coverage/python/htmlcov `
    --cov-report=xml:coverage/python/coverage.xml `
    --cov-report=term `
    tests/

if ($LASTEXITCODE -ne 0) {
    Write-Host "Python coverage failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✓ Python coverage complete" -ForegroundColor Green
Write-Host "  HTML report: coverage/python/htmlcov/index.html"
Write-Host "  XML report: coverage/python/coverage.xml"

Write-Host ""
Write-Host "=== Coverage Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Rust coverage: coverage/rust/tarpaulin-report.html"
Write-Host "Python coverage: coverage/python/htmlcov/index.html"

Pop-Location

