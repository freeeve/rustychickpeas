# Building and Testing Python Bindings

## Prerequisites

1. **Rust and Cargo**: Must be installed and in PATH
   ```bash
   # Install Rust if needed
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   source ~/.cargo/env
   ```

2. **Python 3.8+**: Required for building and testing
   ```bash
   python3 --version
   ```

3. **maturin**: Python package for building Rust extensions
   ```bash
   pip3 install maturin
   ```

## Building the Python Module

### Development Build (Recommended for Testing)

```bash
cd rustychickpeas-python
maturin develop
# or
python3 -m maturin develop
```

This builds the extension in debug mode and installs it in the current Python environment.

### Release Build

```bash
cd rustychickpeas-python
maturin develop --release
```

### Install as Package

```bash
cd rustychickpeas-python
pip install -e .
```

## Running Tests

### Install Test Dependencies

```bash
pip3 install pytest
```

### Run All Tests

```bash
cd rustychickpeas-python
pytest tests/
```

### Run Specific Test Suite

```bash
# Basic operations
pytest tests/test_basic_operations.py

# Traversal operations
pytest tests/test_traversal.py

# Relationship operations
pytest tests/test_relationships.py
```

### Run with Verbose Output

```bash
pytest -v tests/
```

### Run with Coverage

```bash
pip3 install pytest-cov
pytest --cov=rustychickpeas tests/
```

## Troubleshooting

### "rustc not found" Error

Make sure Rust is installed and in your PATH:

```bash
# Check if Rust is installed
rustc --version
cargo --version

# If not found, install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

### "maturin not found" Error

Install maturin:

```bash
pip3 install maturin
# or
python3 -m pip install maturin
```

### Import Errors

If you get import errors after building:

1. Make sure you're in the correct Python environment
2. Try rebuilding: `maturin develop --release`
3. Check that the module was installed: `python3 -c "import rustychickpeas; print(rustychickpeas)"`

### Build Errors

If you encounter compilation errors:

1. Make sure you're using a compatible Rust version (1.70+)
2. Update Rust: `rustup update`
3. Clean and rebuild: `cargo clean && maturin develop`

## Quick Start

```bash
# From project root
cd rustychickpeas-python

# Build and install
maturin develop

# Install pytest
pip3 install pytest

# Run tests
pytest tests/ -v
```

