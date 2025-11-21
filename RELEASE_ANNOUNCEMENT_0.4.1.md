# RustyChickpeas 0.4.1 Release Announcement

We're excited to announce the release of RustyChickpeas 0.4.1! This release adds CSV file loading support and significantly enhances our Parquet loading capabilities with better type handling, deduplication, and S3 integration.

## 🎉 What's New

### CSV Loading Support

RustyChickpeas now supports loading graph data directly from CSV files! This makes it easier to import data from common formats without needing to convert to Parquet first.

**Features:**
- ✅ Load nodes and relationships from CSV files
- ✅ Support for gzip-compressed CSV files (`.csv.gz`)
- ✅ Automatic type detection (integers, floats, booleans, strings)
- ✅ Optional explicit type hints for precise control
- ✅ Node deduplication based on unique properties
- ✅ Auto-generated node IDs when ID column is not specified
- ✅ Full property and label support

**Example:**
```rust
let mut builder = GraphBuilder::new(None, None);

// Load nodes from CSV
let node_ids = builder.load_nodes_from_csv(
    "nodes.csv",
    Some("id"),                    // Node ID column
    Some(vec!["label"]),           // Label columns
    Some(vec!["name", "age"]),     // Property columns
    Some(vec!["email"]),          // Deduplicate by email
    None,                          // Auto-detect column types
)?;

// Load relationships from CSV
let rel_ids = builder.load_relationships_from_csv(
    "relationships.csv",
    "from",                        // Source node column
    "to",                          // Target node column
    Some("type"),                  // Relationship type column
    Some(vec!["weight", "since"]), // Property columns
    None,                          // No fixed type
    None,                          // No deduplication
    None,                          // Auto-detect column types
)?;
```

### Enhanced Parquet Loading

We've made significant improvements to Parquet loading with better type handling and new features:

**New Features:**
- ✅ `default_label` parameter - Apply a default label to all loaded nodes
- ✅ Better handling of empty batches (automatically skipped)
- ✅ Improved support for Int32 node IDs and relationship endpoints
- ✅ Support for Float32 property values
- ✅ Support for LargeUtf8 string columns
- ✅ Enhanced error handling for invalid column types

**Example:**
```rust
// Load nodes with a default label
let node_ids = builder.load_nodes_from_parquet(
    "nodes.parquet",
    Some("id"),
    Some(vec!["label"]),
    Some(vec!["name", "age"]),
    Some("Person"),  // Default label applied to all nodes
)?;
```

### Python API Improvements

The Python bindings now include the `default_label` parameter for easier bulk loading:

```python
builder = GraphSnapshotBuilder()

# Load nodes with default label
node_ids = builder.load_nodes_from_parquet(
    "nodes.parquet",
    node_id_column="id",
    label_columns=["label"],
    property_columns=["name", "age"],
    default_label="Person"  # New parameter!
)
```

### S3 Integration Enhancements

Our S3 integration tests have been significantly expanded to cover more scenarios:
- Loading nodes and relationships with properties from S3
- Deduplication when loading from S3
- Better error handling for invalid S3 paths

## 🔧 Improvements

- **Better Type Handling**: Improved support for various Arrow data types including Int32, Float32, and LargeUtf8
- **Enhanced Deduplication**: Better handling of null values in deduplication keys
- **Error Messages**: More descriptive error messages for bulk loading operations
- **Test Coverage**: Added 30+ new test cases covering edge cases and various data type combinations

## 📦 Installation

Install the latest version from PyPI:

```bash
pip install rustychickpeas==0.4.1
```

Or update an existing installation:

```bash
pip install --upgrade rustychickpeas
```

## 🐛 Bug Fixes

- Fixed handling of empty Parquet batches (now properly skipped)
- Improved error handling for negative node IDs
- Better handling of null relationship types
- Fixed deduplication logic for nodes with null property values

## 📚 Documentation

- Enhanced documentation for bulk loading methods
- Added examples for CSV loading
- Improved parameter descriptions

## 🔄 Migration Guide

This is a minor release with backward-compatible changes. No breaking changes were introduced.

**New Optional Parameters:**
- `default_label` parameter in `load_nodes_from_parquet()` - optional, defaults to `None`
- CSV loading methods are new additions and don't affect existing code

## 🙏 Thanks

Thank you to everyone who has contributed feedback, bug reports, and feature requests. Your input helps make RustyChickpeas better with each release!

## 📝 Full Changelog

For a complete list of changes, see the [commit history](https://github.com/freeeve/rustychickpeas/compare/v0.4.0...v0.4.1).

---

**Next Steps:**
- Try out the new CSV loading features
- Explore the enhanced Parquet loading capabilities
- Check out our [documentation](https://github.com/freeeve/rustychickpeas#readme) for more examples

Happy graph building! 🎉

