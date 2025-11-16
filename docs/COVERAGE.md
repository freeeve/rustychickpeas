# Test Coverage

This document describes how to run and interpret test coverage for RustyChickpeas.

## Overview

RustyChickpeas uses separate coverage tools for Rust and Python:

- **Rust**: `cargo-tarpaulin` - Fast, accurate Rust code coverage
- **Python**: `pytest-cov` - Coverage.py integration with pytest

## Quick Start

### Run All Coverage

```bash
# From project root
./scripts/coverage.sh
```

Or on Windows:

```powershell
.\scripts\coverage.ps1
```

### Run Rust Coverage Only

```bash
# Install cargo-tarpaulin if needed
cargo install cargo-tarpaulin

# Run coverage
cargo tarpaulin --workspace --out Html --out Xml --output-dir coverage/rust
```

### Run Python Coverage Only

```bash
cd rustychickpeas-python

# Install pytest-cov if needed
pip install pytest-cov

# Build extension
maturin develop --release

# Run coverage
pytest --cov=rustychickpeas --cov-report=html --cov-report=term tests/
```

## Coverage Reports

After running coverage, reports are generated in:

- **Rust**: `coverage/rust/tarpaulin-report.html`
- **Python**: `coverage/python/htmlcov/index.html`

Open these HTML files in a browser to view detailed coverage information.

## Configuration

### Rust Coverage (cargo-tarpaulin)

Configuration is in `tarpaulin.toml`:

```toml
[trace]
exclude_files = [
    "*/tests/*",
    "*/benches/*",
    "*/target/*",
]
```

### Python Coverage (pytest-cov)

Configuration is in `rustychickpeas-python/pyproject.toml`:

```toml
[tool.coverage.run]
source = ["rustychickpeas"]
omit = [
    "*/tests/*",
    "*/venv/*",
]
```

## CI Integration

Coverage can be integrated into CI/CD pipelines:

### GitHub Actions Example

```yaml
- name: Install cargo-tarpaulin
  run: cargo install cargo-tarpaulin

- name: Run Rust coverage
  run: cargo tarpaulin --workspace --out Xml

- name: Upload Rust coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage/rust/cobertura.xml
    flags: rust

- name: Run Python coverage
  run: |
    cd rustychickpeas-python
    pip install pytest-cov
    pytest --cov=rustychickpeas --cov-report=xml

- name: Upload Python coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./rustychickpeas-python/coverage.xml
    flags: python
```

## Coverage Goals

Current coverage targets:

- **Rust Core**: Aim for 80%+ line coverage
- **Python Bindings**: Aim for 90%+ line coverage (thin wrapper layer)

## Exclusions

The following are excluded from coverage:

- Test files (`*/tests/*`, `*/test_*.py`)
- Benchmark files (`*/benches/*`)
- Build artifacts (`*/target/*`, `*/build/*`)
- Generated code
- Unreachable code (`unreachable!()`, `panic!()`)

## Troubleshooting

### cargo-tarpaulin fails to install

```bash
# Update Rust toolchain
rustup update

# Install with specific version
cargo install cargo-tarpaulin --version 0.20.0
```

### Python coverage shows 0%

Make sure the Python extension is built:

```bash
cd rustychickpeas-python
maturin develop --release
```

### Coverage reports not generated

Check that output directories exist:

```bash
mkdir -p coverage/rust coverage/python/htmlcov
```

## Viewing Reports

### HTML Reports

Open the HTML files in a browser:

```bash
# Rust
open coverage/rust/tarpaulin-report.html  # macOS
xdg-open coverage/rust/tarpaulin-report.html  # Linux
start coverage/rust/tarpaulin-report.html  # Windows

# Python
open coverage/python/htmlcov/index.html  # macOS
xdg-open coverage/python/htmlcov/index.html  # Linux
start coverage/python/htmlcov/index.html  # Windows
```

### Terminal Reports

Both tools provide terminal output:

- `cargo tarpaulin` shows coverage summary in terminal
- `pytest --cov-report=term` shows coverage summary in terminal

## Continuous Improvement

To improve coverage:

1. Identify uncovered lines in HTML reports
2. Add tests for uncovered code paths
3. Focus on critical paths first (core graph operations)
4. Consider edge cases and error handling

