//! GraphSnapshotBuilder Python wrapper

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict};
use rustychickpeas_core::{GraphBuilder, ValueId};
use crate::graph_snapshot::GraphSnapshot;
use crate::rusty_chickpeas::RustyChickpeas;
use crate::utils::py_to_property_value;

/// Parse a Python object into a NodeReference
/// Accepts:
/// - String: NodeReference::Id(column_name)
/// - Dict with "column" and "property_key": NodeReference::Property
/// - Dict with "columns" and "property_keys": NodeReference::CompositeProperty
fn parse_node_reference(py_obj: &pyo3::types::PyAny) -> PyResult<rustychickpeas_core::types::NodeReference> {
    use rustychickpeas_core::types::NodeReference;

    // Try string first (simple ID column)
    if let Ok(col_name) = py_obj.extract::<String>() {
        return Ok(NodeReference::Id(col_name));
    }

    // Try dict (property lookup)
    if let Ok(dict) = py_obj.downcast::<PyDict>() {
        // Check for composite property (has "columns" key)
        if let Some(columns_any) = dict.get_item("columns")? {
            let columns: Vec<String> = columns_any.extract()?;
            let property_keys: Vec<String> = dict
                .get_item("property_keys")?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Dict with 'columns' must also have 'property_keys'"
                ))?
                .extract()?;
            let label: Option<String> = dict
                .get_item("label")?
                .map(|v| v.extract())
                .transpose()?;

            return Ok(NodeReference::CompositeProperty {
                columns,
                property_keys,
                label,
            });
        }

        // Check for single property (has "column" key)
        if let Some(column_any) = dict.get_item("column")? {
            let column: String = column_any.extract()?;
            let property_key: String = dict
                .get_item("property_key")?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Dict with 'column' must also have 'property_key'"
                ))?
                .extract()?;
            let label: Option<String> = dict
                .get_item("label")?
                .map(|v| v.extract())
                .transpose()?;

            return Ok(NodeReference::Property {
                column,
                property_key,
                label,
            });
        }

        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Dict must have either 'column' (single property) or 'columns' (composite property)"
        ));
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Node reference must be a string (column name) or dict (property lookup spec)"
    ))
}

/// Python wrapper for GraphBuilder
#[pyclass(name = "GraphSnapshotBuilder")]
pub struct GraphSnapshotBuilder {
    pub(crate) builder: GraphBuilder,
    pub(crate) version_str: Option<String>,
    pub(crate) finalized: bool,
}

impl GraphSnapshotBuilder {
    /// Check if the builder has been finalized and raise an error if so
    fn check_not_finalized(&self) -> PyResult<()> {
        if self.finalized {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "GraphSnapshotBuilder has already been finalized"
            ));
        }
        Ok(())
    }
}

#[pymethods]
impl GraphSnapshotBuilder {
    #[new]
    #[pyo3(signature = (version=None, capacity_nodes=None, capacity_rels=None))]
    fn new(version: Option<String>, capacity_nodes: Option<usize>, capacity_rels: Option<usize>) -> Self {
        let builder = if let Some(ref v) = version {
            GraphBuilder::with_version(v, capacity_nodes, capacity_rels)
        } else {
            GraphBuilder::new(capacity_nodes, capacity_rels)
        };
        Self { builder, version_str: version, finalized: false }
    }

    fn __repr__(&self) -> String {
        let version = self.version_str.as_deref().unwrap_or("None");
        format!("GraphSnapshotBuilder(version={})", version)
    }

    /// Add a node with labels
    /// 
    /// # Arguments
    /// * `labels` - List of label strings
    /// * `node_id` - Optional node ID. If None, auto-generates the next sequential ID.
    ///               If Some(id), uses that ID (must be u32)
    /// 
    /// # Returns
    /// The node ID that was used (either the provided ID or the auto-generated one)
    #[pyo3(signature = (labels, *, node_id = None))]
    fn add_node(&mut self, labels: Vec<String>, node_id: Option<u32>) -> PyResult<u32> {
        self.check_not_finalized()?;
        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
        self.builder.add_node(node_id, &label_refs)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Add a relationship
    /// 
    /// # Arguments
    /// * `u` - Start node ID (must be u32)
    /// * `v` - End node ID (must be u32)
    /// * `rel_type` - Relationship type string
    fn add_rel(&mut self, u: u32, v: u32, rel_type: String) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.add_rel(u, v, &rel_type)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    /// Set property with automatic type detection
    /// Automatically calls the correct type-specific method based on the value type
    /// 
    /// # Arguments
    /// * `node_id` - Node ID (must be u32)
    /// * `key` - Property key string
    /// * `value` - Property value (str, int, float, or bool)
    fn set_prop(&mut self, node_id: u32, key: String, value: &PyAny) -> PyResult<()> {
        self.check_not_finalized()?;
        let map_err = |e: rustychickpeas_core::GraphError| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string());
        // Check bool first, as True/False can be extracted as int
        if let Ok(b) = value.extract::<bool>() {
            self.builder.set_prop_bool(node_id, &key, b).map_err(map_err)?;
        } else if let Ok(s) = value.extract::<String>() {
            self.builder.set_prop_str(node_id, &key, &s).map_err(map_err)?;
        } else if let Ok(i) = value.extract::<i64>() {
            self.builder.set_prop_i64(node_id, &key, i).map_err(map_err)?;
        } else if let Ok(f) = value.extract::<f64>() {
            self.builder.set_prop_f64(node_id, &key, f).map_err(map_err)?;
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Property value must be str, int, float, or bool"
            ));
        }
        Ok(())
    }

    /// Set multiple node properties at once from a dictionary
    ///
    /// This is more efficient than calling set_prop() multiple times because
    /// it reduces FFI overhead by batching all property updates in a single call.
    ///
    /// # Arguments
    /// * `node_id` - Node ID (must be u32)
    /// * `properties` - Dictionary of property key-value pairs
    ///
    /// # Example
    /// ```python
    /// builder.set_node_props(1, {"name": "Alice", "age": 30, "active": True})
    /// ```
    fn set_node_props(&mut self, node_id: u32, properties: &PyDict) -> PyResult<()> {
        self.check_not_finalized()?;
        let map_err = |e: rustychickpeas_core::GraphError| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string());
        for (key_obj, value_obj) in properties {
            let key: String = key_obj.extract()?;
            let value: &PyAny = value_obj;

            // Check bool first, as True/False can be extracted as int
            if let Ok(b) = value.extract::<bool>() {
                self.builder.set_prop_bool(node_id, &key, b).map_err(map_err)?;
            } else if let Ok(s) = value.extract::<String>() {
                self.builder.set_prop_str(node_id, &key, &s).map_err(map_err)?;
            } else if let Ok(i) = value.extract::<i64>() {
                self.builder.set_prop_i64(node_id, &key, i).map_err(map_err)?;
            } else if let Ok(f) = value.extract::<f64>() {
                self.builder.set_prop_f64(node_id, &key, f).map_err(map_err)?;
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    format!("Property value for key '{}' must be str, int, float, or bool", key)
                ));
            }
        }
        Ok(())
    }

    /// Set string property
    fn set_prop_str(&mut self, node_id: u32, key: String, value: String) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_prop_str(node_id, &key, &value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Set i64 property
    fn set_prop_i64(&mut self, node_id: u32, key: String, value: i64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_prop_i64(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Set f64 property
    fn set_prop_f64(&mut self, node_id: u32, key: String, value: f64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_prop_f64(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Set boolean property
    fn set_prop_bool(&mut self, node_id: u32, key: String, value: bool) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_prop_bool(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Set string property on a relationship
    /// Finds the relationship by (u, v, rel_type) and sets the property
    fn set_rel_prop_str(&mut self, u: u32, v: u32, rel_type: String, key: String, value: String) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_rel_prop_str(u, v, &rel_type, &key, &value);
        Ok(())
    }

    /// Set i64 property on a relationship
    fn set_rel_prop_i64(&mut self, u: u32, v: u32, rel_type: String, key: String, value: i64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_rel_prop_i64(u, v, &rel_type, &key, value);
        Ok(())
    }

    /// Set f64 property on a relationship
    fn set_rel_prop_f64(&mut self, u: u32, v: u32, rel_type: String, key: String, value: f64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_rel_prop_f64(u, v, &rel_type, &key, value);
        Ok(())
    }

    /// Set boolean property on a relationship
    fn set_rel_prop_bool(&mut self, u: u32, v: u32, rel_type: String, key: String, value: bool) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_rel_prop_bool(u, v, &rel_type, &key, value);
        Ok(())
    }

    /// Set property on a relationship with automatic type detection
    fn set_rel_prop(&mut self, u: u32, v: u32, rel_type: String, key: String, value: &PyAny) -> PyResult<()> {
        self.check_not_finalized()?;
        // Check bool first, as True/False can be extracted as int
        if let Ok(b) = value.extract::<bool>() {
            self.builder.set_rel_prop_bool(u, v, &rel_type, &key, b);
        } else if let Ok(s) = value.extract::<String>() {
            self.builder.set_rel_prop_str(u, v, &rel_type, &key, &s);
        } else if let Ok(i) = value.extract::<i64>() {
            self.builder.set_rel_prop_i64(u, v, &rel_type, &key, i);
        } else if let Ok(f) = value.extract::<f64>() {
            self.builder.set_rel_prop_f64(u, v, &rel_type, &key, f);
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Property value must be str, int, float, or bool"
            ));
        }
        Ok(())
    }

    /// Set multiple properties on a single relationship
    ///
    /// More efficient than multiple set_rel_prop() calls for the same relationship.
    ///
    /// Args:
    ///     u: Start node ID (u32)
    ///     v: End node ID (u32)
    ///     rel_type: Relationship type string
    ///     properties: Dict of property key -> value (str, int, float, or bool)
    ///
    /// Example:
    ///     builder.set_rel_props(1, 2, "KNOWS", {"since": "2020", "weight": 5})
    fn set_rel_props(&mut self, u: u32, v: u32, rel_type: String, properties: &PyDict) -> PyResult<()> {
        self.check_not_finalized()?;
        for (key_obj, value_obj) in properties {
            let key: String = key_obj.extract()?;
            let value: &PyAny = value_obj;

            // Check bool first, as True/False can be extracted as int
            if let Ok(b) = value.extract::<bool>() {
                self.builder.set_rel_prop_bool(u, v, &rel_type, &key, b);
            } else if let Ok(s) = value.extract::<String>() {
                self.builder.set_rel_prop_str(u, v, &rel_type, &key, &s);
            } else if let Ok(i) = value.extract::<i64>() {
                self.builder.set_rel_prop_i64(u, v, &rel_type, &key, i);
            } else if let Ok(f) = value.extract::<f64>() {
                self.builder.set_rel_prop_f64(u, v, &rel_type, &key, f);
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    format!("Property value for key '{}' must be str, int, float, or bool", key)
                ));
            }
        }
        Ok(())
    }

    /// Bulk set properties on multiple relationships
    ///
    /// Much more efficient than individual calls when setting properties on many relationships.
    /// Builds an internal index once and uses it for all lookups.
    ///
    /// Args:
    ///     rel_props: List of (u, v, rel_type, properties) tuples where:
    ///         - u: Start node ID (u32)
    ///         - v: End node ID (u32)
    ///         - rel_type: Relationship type string
    ///         - properties: Dict of property key -> value (str, int, float, or bool)
    ///
    /// Returns:
    ///     Number of relationships that were found and had properties set
    ///
    /// Example:
    ///     builder.set_rel_props_bulk([
    ///         (1, 2, "KNOWS", {"since": "2020", "weight": 5}),
    ///         (2, 3, "FOLLOWS", {"since": "2021", "active": True}),
    ///     ])
    fn set_rel_props_bulk(&mut self, rel_props: Vec<(u32, u32, String, &PyDict)>) -> PyResult<usize> {
        self.check_not_finalized()?;
        use rustychickpeas_core::types::PropertyValue;

        // Convert Python data to Rust types
        let mut rust_rel_props: Vec<(u32, u32, String, Vec<(String, PropertyValue)>)> = Vec::with_capacity(rel_props.len());

        for (u, v, rel_type, props_dict) in rel_props {
            let mut props: Vec<(String, PropertyValue)> = Vec::with_capacity(props_dict.len());

            for (key, value) in props_dict.iter() {
                let key_str: String = key.extract()?;
                let prop_val = py_to_property_value(value)?;
                props.push((key_str, prop_val));
            }

            rust_rel_props.push((u, v, rel_type, props));
        }

        // Convert to the format expected by the Rust function
        let converted: Vec<(u32, u32, &str, Vec<(&str, PropertyValue)>)> = rust_rel_props
            .iter()
            .map(|(u, v, rel_type, props)| {
                let props_refs: Vec<(&str, PropertyValue)> = props
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();
                (*u, *v, rel_type.as_str(), props_refs)
            })
            .collect();

        Ok(self.builder.set_rel_props(&converted))
    }

    /// Load nodes from a Parquet file into the builder
    /// 
    /// # Arguments
    /// * `path` - Path to the Parquet file (local file or S3 URI)
    /// * `node_id_column` - Optional column name for node IDs. If None, auto-generates sequential IDs.
    /// * `label_columns` - Optional list of column names to use as labels
    /// * `property_columns` - Optional list of column names to load as properties. If None, loads all columns except ID and label columns.
    /// * `unique_properties` - Optional list of property column names to use for deduplication
    /// * `default_label` - Optional default label to apply to all loaded nodes (in addition to any labels from label_columns)
    fn load_nodes_from_parquet(
        &mut self,
        path: String,
        node_id_column: Option<String>,
        label_columns: Option<Vec<String>>,
        property_columns: Option<Vec<String>>,
        unique_properties: Option<Vec<String>>,
        default_label: Option<String>,
    ) -> PyResult<Vec<u32>> {
        self.check_not_finalized()?;
        let label_cols = label_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let prop_cols = property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let unique_props = unique_properties.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        
        self.builder
            .load_nodes_from_parquet(
                &path,
                node_id_column.as_deref(),
                label_cols,
                prop_cols,
                unique_props,
                default_label.as_deref(),
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Load relationships from a Parquet file into the builder
    ///
    /// Args:
    ///     path: Path to Parquet file (local or S3)
    ///     start_node_column: Column name for start node IDs
    ///     end_node_column: Column name for end node IDs
    ///     rel_type_column: Optional column name for relationship type
    ///     property_columns: Optional list of property columns to load
    ///     fixed_rel_type: Fixed relationship type (used if rel_type_column is None)
    ///     deduplication: Optional deduplication strategy ("CreateAll", "CreateUniqueByRelType", "CreateUniqueByRelTypeAndKeyProperties")
    ///     key_property_columns: Optional list of property columns to use as uniqueness key when
    ///         deduplication is "CreateUniqueByRelTypeAndKeyProperties". If None, uses all property_columns.
    #[pyo3(signature = (path, start_node_column, end_node_column, rel_type_column=None, property_columns=None, fixed_rel_type=None, deduplication=None, key_property_columns=None))]
    fn load_relationships_from_parquet(
        &mut self,
        path: String,
        start_node_column: String,
        end_node_column: String,
        rel_type_column: Option<String>,
        property_columns: Option<Vec<String>>,
        fixed_rel_type: Option<String>,
        deduplication: Option<String>,
        key_property_columns: Option<Vec<String>>,
    ) -> PyResult<Vec<(u32, u32)>> {
        self.check_not_finalized()?;
        let prop_cols = property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let key_cols = key_property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let dedup = match deduplication.as_ref() {
            Some(s) => {
                match s.as_str() {
                    "create_all" | "CreateAll" => Some(rustychickpeas_core::types::RelationshipDeduplication::CreateAll),
                    "unique_by_type" | "CreateUniqueByRelType" => Some(rustychickpeas_core::types::RelationshipDeduplication::CreateUniqueByRelType),
                    "unique_by_type_and_key_properties" | "CreateUniqueByRelTypeAndKeyProperties" => Some(rustychickpeas_core::types::RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
                    other => return Err(pyo3::exceptions::PyValueError::new_err(format!("Unknown deduplication strategy: '{}'. Valid options: 'create_all', 'unique_by_type', 'unique_by_type_and_key_properties'", other))),
                }
            }
            None => None,
        };

        self.builder
            .load_relationships_from_parquet(
                &path,
                &start_node_column,
                &end_node_column,
                rel_type_column.as_deref(),
                prop_cols,
                fixed_rel_type.as_deref(),
                dedup,
                key_cols,
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Load relationships from a Parquet file with flexible node reference support
    ///
    /// This version supports looking up nodes by:
    /// - Node ID column (string): use the column directly as node ID
    /// - Single property lookup (dict): {"column": "uuid_col", "property_key": "uuid", "label": "Person"}
    /// - Composite property lookup (dict): {"columns": ["name", "city"], "property_keys": ["name", "city"], "label": "Person"}
    ///
    /// Args:
    ///     path: Path to Parquet file (local or S3)
    ///     start_node: String (column name for node ID) or dict (property lookup spec)
    ///     end_node: String (column name for node ID) or dict (property lookup spec)
    ///     rel_type_column: Optional column name for relationship type
    ///     property_columns: Optional list of property columns to load
    ///     fixed_rel_type: Fixed relationship type (used if rel_type_column is None)
    ///     deduplication: Optional deduplication strategy
    ///     key_property_columns: Optional list of property columns for uniqueness
    #[pyo3(signature = (path, start_node, end_node, rel_type_column=None, property_columns=None, fixed_rel_type=None, deduplication=None, key_property_columns=None))]
    fn load_relationships_from_parquet_v2(
        &mut self,
        path: String,
        start_node: &pyo3::types::PyAny,
        end_node: &pyo3::types::PyAny,
        rel_type_column: Option<String>,
        property_columns: Option<Vec<String>>,
        fixed_rel_type: Option<String>,
        deduplication: Option<String>,
        key_property_columns: Option<Vec<String>>,
    ) -> PyResult<Vec<(u32, u32)>> {
        self.check_not_finalized()?;
        // Parse start_node reference
        let start_ref = parse_node_reference(start_node)?;
        let end_ref = parse_node_reference(end_node)?;

        let prop_cols = property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let key_cols = key_property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let dedup = match deduplication.as_ref() {
            Some(s) => {
                match s.as_str() {
                    "create_all" | "CreateAll" => Some(rustychickpeas_core::types::RelationshipDeduplication::CreateAll),
                    "unique_by_type" | "CreateUniqueByRelType" => Some(rustychickpeas_core::types::RelationshipDeduplication::CreateUniqueByRelType),
                    "unique_by_type_and_key_properties" | "CreateUniqueByRelTypeAndKeyProperties" => Some(rustychickpeas_core::types::RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
                    other => return Err(pyo3::exceptions::PyValueError::new_err(format!("Unknown deduplication strategy: '{}'. Valid options: 'create_all', 'unique_by_type', 'unique_by_type_and_key_properties'", other))),
                }
            }
            None => None,
        };

        self.builder
            .load_relationships_from_parquet_v2(
                &path,
                start_ref,
                end_ref,
                rel_type_column.as_deref(),
                prop_cols,
                fixed_rel_type.as_deref(),
                dedup,
                key_cols,
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get property value for a node (before finalization)
    /// Note: Uses get_property to avoid conflict with Python's property builtin
    fn get_property(&self, node_id: u32, key: String) -> PyResult<Option<PyObject>> {
        self.check_not_finalized()?;
        let value_id = self.builder.prop(node_id, &key);
        
        Python::with_gil(|py| {
            if let Some(vid) = value_id {
                match vid {
                    ValueId::Str(sid) => {
                        // Resolve string ID from builder's interner
                        let s = self.builder.resolve_string(sid);
                        Ok(Some(s.to_object(py)))
                    }
                    ValueId::I64(i) => Ok(Some(i.to_object(py))),
                    ValueId::F64(bits) => Ok(Some(f64::from_bits(bits).to_object(py))),
                    ValueId::Bool(b) => Ok(Some(PyBool::new(py, b).into_py(py))),
                }
            } else {
                Ok(None)
            }
        })
    }

    /// Update property with automatic type detection
    /// Automatically calls the correct type-specific method based on the value type
    fn update_prop(&mut self, node_id: u32, key: String, value: &PyAny) -> PyResult<()> {
        self.check_not_finalized()?;
        // Check if property exists first
        if self.builder.prop(node_id, &key).is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Property key '{}' not found", key)
            ));
        }

        let map_err = |e: rustychickpeas_core::GraphError| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string());
        // Check bool first, as True/False can be extracted as int
        if let Ok(b) = value.extract::<bool>() {
            self.builder.update_prop_bool(node_id, &key, b).map_err(map_err)?;
        } else if let Ok(s) = value.extract::<String>() {
            self.builder.update_prop_str(node_id, &key, &s).map_err(map_err)?;
        } else if let Ok(i) = value.extract::<i64>() {
            self.builder.update_prop_i64(node_id, &key, i).map_err(map_err)?;
        } else if let Ok(f) = value.extract::<f64>() {
            self.builder.update_prop_f64(node_id, &key, f).map_err(map_err)?;
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Property value must be str, int, float, or bool"
            ));
        }
        Ok(())
    }

    /// Update string property (removes old, sets new)
    fn update_prop_str(&mut self, node_id: u32, key: String, value: String) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.update_prop_str(node_id, &key, &value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Update i64 property
    fn update_prop_i64(&mut self, node_id: u32, key: String, value: i64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.update_prop_i64(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Update f64 property
    fn update_prop_f64(&mut self, node_id: u32, key: String, value: f64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.update_prop_f64(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Update boolean property
    fn update_prop_bool(&mut self, node_id: u32, key: String, value: bool) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.update_prop_bool(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get nodes with a specific property value, scoped by label (before finalization)
    /// 
    /// # Arguments
    /// * `label` - The label to scope the query to
    /// * `key` - The property key
    /// * `value` - The property value to search for
    #[pyo3(signature = (label, key, value))]
    fn nodes_with_property(&self, label: String, key: String, value: &PyAny) -> PyResult<Vec<u32>> {
        self.check_not_finalized()?;
        let prop_value = py_to_property_value(value)?;
        let value_id = match prop_value {
            rustychickpeas_core::PropertyValue::String(_s) => {
                // Need to intern the string to get ID
                // For now, we can't easily do this without exposing interner
                // TODO: Add helper to convert PropertyValue to ValueId
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "String property queries not yet supported in GraphBuilder"
                ));
            }
            rustychickpeas_core::PropertyValue::Integer(i) => ValueId::I64(i),
            rustychickpeas_core::PropertyValue::Float(f) => ValueId::from_f64(f),
            rustychickpeas_core::PropertyValue::Boolean(b) => ValueId::Bool(b),
            rustychickpeas_core::PropertyValue::InternedString(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "InternedString not supported in GraphBuilder queries"
                ));
            }
        };
        
        let node_ids = self.builder.nodes_with_property(&label, &key, value_id);
        Ok(node_ids)
    }

    /// Get node labels (before finalization)
    fn node_labels(&self, node_id: u32) -> PyResult<Vec<String>> {
        self.check_not_finalized()?;
        Ok(self.builder.node_labels(node_id))
    }

    /// Get neighbors of a node (before finalization)
    /// Returns (outgoing, incoming) as tuple of lists of node IDs
    fn neighbor_ids(&self, node_id: u32) -> PyResult<(Vec<u32>, Vec<u32>)> {
        self.check_not_finalized()?;
        let (out, inc) = self.builder.neighbor_ids(node_id);
        Ok((out, inc))
    }

    /// Set the version for this snapshot
    /// This version will be stored with the snapshot when finalized
    fn set_version(&mut self, version: String) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder.set_version(&version);
        self.version_str = Some(version);
        Ok(())
    }

    /// Finalize the builder into a GraphSnapshot
    /// 
    /// # Arguments
    /// * `index_properties` - Optional list of property key names to index during finalization.
    ///   If provided, these properties will be indexed upfront (faster queries, more memory).
    ///   If None, all properties will be indexed lazily on first access (saves memory).
    #[pyo3(signature = (index_properties=None))]
    fn finalize(&mut self, index_properties: Option<Vec<String>>) -> PyResult<GraphSnapshot> {
        self.check_not_finalized()?;
        self.finalized = true;
        let builder = std::mem::replace(&mut self.builder, GraphBuilder::new(None, None));
        
        // Convert Vec<String> to Vec<&str> for the Rust API
        let keys_to_index: Option<Vec<&str>> = index_properties.as_ref().map(|names| {
            names.iter().map(|s| s.as_str()).collect()
        });
        
        let snapshot = builder.finalize(keys_to_index.as_deref());
        Ok(GraphSnapshot::new(snapshot))
    }

    /// Finalize the builder into a GraphSnapshot and add it to the manager
    /// 
    /// This is a convenience method that finalizes the builder and automatically
    /// adds the snapshot to the manager. Equivalent to:
    /// ```python
    /// snapshot = builder.finalize()
    /// manager.add_snapshot(snapshot)
    /// ```
    /// 
    /// # Arguments
    /// * `index_properties` - Optional list of property key names to index during finalization.
    ///   If provided, these properties will be indexed upfront (faster queries, more memory).
    ///   If None, all properties will be indexed lazily on first access (saves memory).
    #[pyo3(signature = (manager, index_properties=None))]
    fn finalize_into(&mut self, manager: &RustyChickpeas, index_properties: Option<Vec<String>>) -> PyResult<()> {
        self.check_not_finalized()?;
        self.finalized = true;
        let builder = std::mem::replace(&mut self.builder, GraphBuilder::new(None, None));
        
        // Convert Vec<String> to Vec<&str> for the Rust API
        let keys_to_index: Option<Vec<&str>> = index_properties.as_ref().map(|names| {
            names.iter().map(|s| s.as_str()).collect()
        });
        
        let snapshot = builder.finalize(keys_to_index.as_deref());
        manager.manager.add_snapshot(snapshot);
        Ok(())
    }
}

