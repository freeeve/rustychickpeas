# Building and Testing Python Bindings

## Prerequisites

1. **Rust and Cargo**: Must be installed and in PATH
   ```bash
   # Install Rust if needed
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   source ~/.cargo/env
   ```

2. **Python 3.10+**: Required for building and testing
   ```bash
   python3 --version
   ```
   
   **Supported Python Versions**: 3.10, 3.11, 3.12, 3.13, 3.14
   
   **Note**: Python 3.8 and 3.9 are **not supported**. The project requires Python 3.10 or later.

3. **maturin**: Python package for building Rust extensions
   ```bash
   pip3 install maturin
   ```
   
   **Note**: You may also need `pyarrow` for some tests:
   ```bash
   pip3 install pyarrow
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
pip3 install pytest pyarrow
```

**Note**: `pyarrow` is required for Parquet-related tests.

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

### Python Version Errors

If you see errors about Python version:

1. **Python 3.8/3.9 not supported**: The project requires Python 3.10 or later
   ```bash
   python3 --version  # Should show 3.10 or higher
   ```
2. **Python 3.13/3.14**: These versions work but require the ABI3 forward compatibility flag:
   ```bash
   # For cargo check/build commands
   export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
   cargo check --package rustychickpeas-python
   
   # For maturin builds, this is automatically handled
   maturin develop --release
   ```
   
   **Note**: You can add this to your shell config (`.bashrc`, `.zshrc`, etc.) if you're using Python 3.13/3.14:
   ```bash
   export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
   ```
   
   If you encounter issues, try Python 3.10-3.12 first (these don't need the flag).
3. **Check your Python version**: Make sure `python3` points to a supported version
   ```bash
   which python3
   python3 --version
   ```

## Quick Start

```bash
# From project root
cd rustychickpeas-python

# Build and install
maturin develop

# Install test dependencies
pip3 install pytest pyarrow

# Run tests
pytest tests/ -v
```

