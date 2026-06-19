//! GraphSnapshotBuilder Python wrapper

use crate::graph_snapshot::GraphSnapshot;
use crate::rusty_chickpeas::RustyChickpeas;
use crate::utils::py_to_property_value;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;
use rustychickpeas_core::{GraphBuilder, ValueId};

/// Parse a Python object into a NodeReference
/// Accepts:
/// - String: NodeReference::Id(column_name)
/// - Dict with "column" and "property_key": NodeReference::Property
/// - Dict with "columns" and "property_keys": NodeReference::CompositeProperty
fn parse_node_reference(
    py_obj: &Bound<'_, PyAny>,
) -> PyResult<rustychickpeas_core::types::NodeReference> {
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
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Dict with 'columns' must also have 'property_keys'",
                    )
                })?
                .extract()?;
            let label: Option<String> = dict.get_item("label")?.map(|v| v.extract()).transpose()?;

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
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Dict with 'column' must also have 'property_key'",
                    )
                })?
                .extract()?;
            let label: Option<String> = dict.get_item("label")?.map(|v| v.extract()).transpose()?;

            return Ok(NodeReference::Property {
                column,
                property_key,
                label,
            });
        }

        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Dict must have either 'column' (single property) or 'columns' (composite property)",
        ));
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Node reference must be a string (column name) or dict (property lookup spec)",
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
                "GraphSnapshotBuilder has already been finalized",
            ));
        }
        Ok(())
    }
}

#[pymethods]
impl GraphSnapshotBuilder {
    #[new]
    #[pyo3(signature = (version=None, capacity_nodes=None, capacity_rels=None))]
    fn new(
        version: Option<String>,
        capacity_nodes: Option<usize>,
        capacity_rels: Option<usize>,
    ) -> Self {
        let builder = if let Some(ref v) = version {
            GraphBuilder::with_version(v, capacity_nodes, capacity_rels)
        } else {
            GraphBuilder::new(capacity_nodes, capacity_rels)
        };
        Self {
            builder,
            version_str: version,
            finalized: false,
        }
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
        self.builder
            .add_node(node_id, &label_refs)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Add a relationship
    ///
    /// # Arguments
    /// * `u` - Start node ID (must be u32)
    /// * `v` - End node ID (must be u32)
    /// * `rel_type` - Relationship type string
    fn add_relationship(&mut self, u: u32, v: u32, rel_type: String) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .add_relationship(u, v, &rel_type)
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
    fn set_prop(&mut self, node_id: u32, key: String, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.check_not_finalized()?;
        let map_err = |e: rustychickpeas_core::GraphError| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        };
        // Check bool first, as True/False can be extracted as int
        if let Ok(b) = value.extract::<bool>() {
            self.builder
                .set_prop_bool(node_id, &key, b)
                .map_err(map_err)?;
        } else if let Ok(s) = value.extract::<String>() {
            self.builder
                .set_prop_str(node_id, &key, &s)
                .map_err(map_err)?;
        } else if let Ok(i) = value.extract::<i64>() {
            self.builder
                .set_prop_i64(node_id, &key, i)
                .map_err(map_err)?;
        } else if let Ok(f) = value.extract::<f64>() {
            self.builder
                .set_prop_f64(node_id, &key, f)
                .map_err(map_err)?;
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Property value must be str, int, float, or bool",
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
    fn set_node_props(&mut self, node_id: u32, properties: &Bound<'_, PyDict>) -> PyResult<()> {
        self.check_not_finalized()?;
        let map_err = |e: rustychickpeas_core::GraphError| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        };
        for (key_obj, value_obj) in properties {
            let key: String = key_obj.extract()?;
            let value = &value_obj;

            // Check bool first, as True/False can be extracted as int
            if let Ok(b) = value.extract::<bool>() {
                self.builder
                    .set_prop_bool(node_id, &key, b)
                    .map_err(map_err)?;
            } else if let Ok(s) = value.extract::<String>() {
                self.builder
                    .set_prop_str(node_id, &key, &s)
                    .map_err(map_err)?;
            } else if let Ok(i) = value.extract::<i64>() {
                self.builder
                    .set_prop_i64(node_id, &key, i)
                    .map_err(map_err)?;
            } else if let Ok(f) = value.extract::<f64>() {
                self.builder
                    .set_prop_f64(node_id, &key, f)
                    .map_err(map_err)?;
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Property value for key '{}' must be str, int, float, or bool",
                    key
                )));
            }
        }
        Ok(())
    }

    /// Set string property
    fn set_prop_str(&mut self, node_id: u32, key: String, value: String) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .set_prop_str(node_id, &key, &value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Set i64 property
    fn set_prop_i64(&mut self, node_id: u32, key: String, value: i64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .set_prop_i64(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Set f64 property
    fn set_prop_f64(&mut self, node_id: u32, key: String, value: f64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .set_prop_f64(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Set boolean property
    fn set_prop_bool(&mut self, node_id: u32, key: String, value: bool) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .set_prop_bool(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Set string property on a relationship
    /// Finds the relationship by (u, v, rel_type) and sets the property
    fn set_relationship_prop_str(
        &mut self,
        u: u32,
        v: u32,
        rel_type: String,
        key: String,
        value: String,
    ) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .set_relationship_prop_str(u, v, &rel_type, &key, &value);
        Ok(())
    }

    /// Set i64 property on a relationship
    fn set_relationship_prop_i64(
        &mut self,
        u: u32,
        v: u32,
        rel_type: String,
        key: String,
        value: i64,
    ) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .set_relationship_prop_i64(u, v, &rel_type, &key, value);
        Ok(())
    }

    /// Set f64 property on a relationship
    fn set_relationship_prop_f64(
        &mut self,
        u: u32,
        v: u32,
        rel_type: String,
        key: String,
        value: f64,
    ) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .set_relationship_prop_f64(u, v, &rel_type, &key, value);
        Ok(())
    }

    /// Set boolean property on a relationship
    fn set_relationship_prop_bool(
        &mut self,
        u: u32,
        v: u32,
        rel_type: String,
        key: String,
        value: bool,
    ) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .set_relationship_prop_bool(u, v, &rel_type, &key, value);
        Ok(())
    }

    /// Set property on a relationship with automatic type detection
    fn set_relationship_prop(
        &mut self,
        u: u32,
        v: u32,
        rel_type: String,
        key: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.check_not_finalized()?;
        // Check bool first, as True/False can be extracted as int
        if let Ok(b) = value.extract::<bool>() {
            self.builder
                .set_relationship_prop_bool(u, v, &rel_type, &key, b);
        } else if let Ok(s) = value.extract::<String>() {
            self.builder
                .set_relationship_prop_str(u, v, &rel_type, &key, &s);
        } else if let Ok(i) = value.extract::<i64>() {
            self.builder
                .set_relationship_prop_i64(u, v, &rel_type, &key, i);
        } else if let Ok(f) = value.extract::<f64>() {
            self.builder
                .set_relationship_prop_f64(u, v, &rel_type, &key, f);
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Property value must be str, int, float, or bool",
            ));
        }
        Ok(())
    }

    /// Set multiple properties on a single relationship
    ///
    /// More efficient than multiple set_relationship_prop() calls for the same relationship.
    ///
    /// Args:
    ///     u: Start node ID (u32)
    ///     v: End node ID (u32)
    ///     rel_type: Relationship type string
    ///     properties: Dict of property key -> value (str, int, float, or bool)
    ///
    /// Example:
    ///     builder.set_relationship_props(1, 2, "KNOWS", {"since": "2020", "weight": 5})
    fn set_relationship_props(
        &mut self,
        u: u32,
        v: u32,
        rel_type: String,
        properties: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        self.check_not_finalized()?;
        for (key_obj, value_obj) in properties {
            let key: String = key_obj.extract()?;
            let value = &value_obj;

            // Check bool first, as True/False can be extracted as int
            if let Ok(b) = value.extract::<bool>() {
                self.builder
                    .set_relationship_prop_bool(u, v, &rel_type, &key, b);
            } else if let Ok(s) = value.extract::<String>() {
                self.builder
                    .set_relationship_prop_str(u, v, &rel_type, &key, &s);
            } else if let Ok(i) = value.extract::<i64>() {
                self.builder
                    .set_relationship_prop_i64(u, v, &rel_type, &key, i);
            } else if let Ok(f) = value.extract::<f64>() {
                self.builder
                    .set_relationship_prop_f64(u, v, &rel_type, &key, f);
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Property value for key '{}' must be str, int, float, or bool",
                    key
                )));
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
    ///     builder.set_relationship_props_bulk([
    ///         (1, 2, "KNOWS", {"since": "2020", "weight": 5}),
    ///         (2, 3, "FOLLOWS", {"since": "2021", "active": True}),
    ///     ])
    #[allow(clippy::type_complexity)]
    fn set_relationship_props_bulk(
        &mut self,
        rel_props: Vec<(u32, u32, String, Bound<'_, PyDict>)>,
    ) -> PyResult<usize> {
        self.check_not_finalized()?;
        use rustychickpeas_core::types::PropertyValue;

        // Convert Python data to Rust types
        let mut rust_rel_props: Vec<(u32, u32, String, Vec<(String, PropertyValue)>)> =
            Vec::with_capacity(rel_props.len());

        for (u, v, rel_type, props_dict) in rel_props {
            let mut props: Vec<(String, PropertyValue)> = Vec::with_capacity(props_dict.len());

            for (key, value) in props_dict.iter() {
                let key_str: String = key.extract()?;
                let prop_val = py_to_property_value(&value)?;
                props.push((key_str, prop_val));
            }

            rust_rel_props.push((u, v, rel_type, props));
        }

        // Convert to the format expected by the Rust function
        let converted: Vec<(u32, u32, &str, Vec<(&str, PropertyValue)>)> = rust_rel_props
            .iter()
            .map(|(u, v, rel_type, props)| {
                let props_refs: Vec<(&str, PropertyValue)> =
                    props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                (*u, *v, rel_type.as_str(), props_refs)
            })
            .collect();

        Ok(self.builder.set_relationship_props(&converted))
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
    #[pyo3(signature = (path, node_id_column=None, label_columns=None, property_columns=None, unique_properties=None, default_label=None))]
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
        let label_cols = label_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let prop_cols = property_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let unique_props = unique_properties
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());

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

    /// Load nodes from a CSV file into the builder (.csv or .csv.gz).
    ///
    /// Args:
    ///     path: Path to the CSV file
    ///     node_id_column: Column holding node IDs (auto-generated if None)
    ///     label_columns: Columns whose values are labels
    ///     property_columns: Columns to load as properties (all non-id/label if None)
    ///     unique_properties: Property columns to deduplicate nodes on
    ///     default_label: A fixed label applied to every row (e.g. the entity type
    ///         when the label is the file rather than a column) — the CSV analogue
    ///         of the Parquet loader's default_label.
    #[pyo3(signature = (path, node_id_column=None, label_columns=None, property_columns=None, unique_properties=None, default_label=None, delimiter=",".to_string()))]
    #[allow(clippy::too_many_arguments)]
    fn load_nodes_from_csv(
        &mut self,
        path: String,
        node_id_column: Option<String>,
        label_columns: Option<Vec<String>>,
        property_columns: Option<Vec<String>>,
        unique_properties: Option<Vec<String>>,
        default_label: Option<String>,
        delimiter: String,
    ) -> PyResult<Vec<u32>> {
        self.check_not_finalized()?;
        let delim = delimiter.as_bytes().first().copied().unwrap_or(b',');
        let label_cols = label_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let prop_cols = property_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let unique_props = unique_properties
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());

        self.builder
            .load_nodes_from_csv(
                &path,
                node_id_column.as_deref(),
                label_cols,
                prop_cols,
                unique_props,
                None, // column_types: Auto heuristic
                default_label.as_deref(),
                delim,
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Load relationships from a CSV file into the builder (.csv or .csv.gz).
    ///
    /// Endpoints accept the same forms as the Parquet loader:
    /// - String: a column holding the node id directly
    /// - Dict (single property): {"column": "ext_id", "property_key": "id", "label": "Person"}
    /// - Dict (composite property): {"columns": [...], "property_keys": [...], "label": "Person"}
    ///
    /// Property references resolve against already-loaded nodes, so load nodes before their
    /// relationships. Use them when external ids overflow u32 or collide across labels (LDBC).
    ///
    /// Args:
    ///     path: Path to the CSV file
    ///     start_node_column / end_node_column: String column name or property-lookup dict
    ///     rel_type_column: Column holding the relationship type (or use fixed_rel_type)
    ///     property_columns: Columns to load as edge properties
    ///     fixed_rel_type: A fixed relationship type for every row
    ///     deduplication: 'create_all' | 'unique_by_type' | 'unique_by_type_and_key_properties'
    #[pyo3(signature = (path, start_node_column, end_node_column, rel_type_column=None, property_columns=None, fixed_rel_type=None, deduplication=None, delimiter=",".to_string()))]
    #[allow(clippy::too_many_arguments)]
    fn load_relationships_from_csv(
        &mut self,
        path: String,
        start_node_column: &Bound<'_, PyAny>,
        end_node_column: &Bound<'_, PyAny>,
        rel_type_column: Option<String>,
        property_columns: Option<Vec<String>>,
        fixed_rel_type: Option<String>,
        deduplication: Option<String>,
        delimiter: String,
    ) -> PyResult<Vec<(u32, u32)>> {
        use rustychickpeas_core::types::RelationshipDeduplication as Dd;
        self.check_not_finalized()?;
        // A bare column name (str) or a property-lookup dict; see parse_node_reference.
        let start_ref = parse_node_reference(start_node_column)?;
        let end_ref = parse_node_reference(end_node_column)?;
        let delim = delimiter.as_bytes().first().copied().unwrap_or(b',');
        let prop_cols = property_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let dedup = match deduplication.as_deref() {
            Some("create_all") | Some("CreateAll") => Some(Dd::CreateAll),
            Some("unique_by_type") | Some("CreateUniqueByRelType") => {
                Some(Dd::CreateUniqueByRelType)
            }
            Some("unique_by_type_and_key_properties")
            | Some("CreateUniqueByRelTypeAndKeyProperties") => {
                Some(Dd::CreateUniqueByRelTypeAndKeyProperties)
            }
            Some(other) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unknown deduplication strategy: '{}'",
                    other
                )))
            }
            None => None,
        };

        self.builder
            .load_relationships_from_csv(
                &path,
                start_ref,
                end_ref,
                rel_type_column.as_deref(),
                prop_cols,
                fixed_rel_type.as_deref(),
                dedup,
                None, // column_types: Auto heuristic
                delim,
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Load several relationship types from one CSV file in a single pass.
    ///
    /// ``rels`` is a list of ``Rel(rel_type, start, end, props=...)`` where ``start`` /
    /// ``end`` are ``Ref(column, label=None, property_key="id")`` and ``props`` are
    /// ``Prop(name, column=None, type=None)``. The node index for each distinct
    /// ``(property_key, label)`` is built once and the file is read once — so the
    /// several rels that share a merged-FK file load far faster than one call each.
    /// Returns the relationships added per ``Rel``, in order.
    #[pyo3(signature = (path, rels, delimiter=",".to_string()))]
    fn load_relationships_from_csv_multi(
        &mut self,
        path: String,
        rels: Vec<Rel>,
        delimiter: String,
    ) -> PyResult<Vec<u64>> {
        self.check_not_finalized()?;
        let delim = delimiter.as_bytes().first().copied().unwrap_or(b',');
        let specs: Vec<rustychickpeas_core::RelLoadSpec> =
            rels.into_iter().map(|r| r.spec).collect();
        self.builder
            .load_relationships_from_csv_multi(&path, &specs, delim)
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
    #[pyo3(signature = (path, start_node_column, end_node_column, rel_type_column=None, property_columns=None, fixed_rel_type=None, deduplication=None, key_property_columns=None))]
    #[allow(clippy::too_many_arguments)]
    fn load_relationships_from_parquet(
        &mut self,
        path: String,
        start_node_column: &Bound<'_, PyAny>,
        end_node_column: &Bound<'_, PyAny>,
        rel_type_column: Option<String>,
        property_columns: Option<Vec<String>>,
        fixed_rel_type: Option<String>,
        deduplication: Option<String>,
        key_property_columns: Option<Vec<String>>,
    ) -> PyResult<Vec<(u32, u32)>> {
        self.check_not_finalized()?;
        // A bare column name (str) or a property-lookup dict; see parse_node_reference.
        let start_ref = parse_node_reference(start_node_column)?;
        let end_ref = parse_node_reference(end_node_column)?;

        let prop_cols = property_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let key_cols = key_property_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
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
                        Ok(Some(s.into_py_any(py)?))
                    }
                    ValueId::I64(i) => Ok(Some(i.into_py_any(py)?)),
                    ValueId::F64(bits) => Ok(Some(f64::from_bits(bits).into_py_any(py)?)),
                    ValueId::Bool(b) => Ok(Some(b.into_py_any(py)?)),
                }
            } else {
                Ok(None)
            }
        })
    }

    /// Update property with automatic type detection
    /// Automatically calls the correct type-specific method based on the value type
    fn update_prop(&mut self, node_id: u32, key: String, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.check_not_finalized()?;
        // Check if property exists first
        if self.builder.prop(node_id, &key).is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Property key '{}' not found",
                key
            )));
        }

        let map_err = |e: rustychickpeas_core::GraphError| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        };
        // Check bool first, as True/False can be extracted as int
        if let Ok(b) = value.extract::<bool>() {
            self.builder
                .update_prop_bool(node_id, &key, b)
                .map_err(map_err)?;
        } else if let Ok(s) = value.extract::<String>() {
            self.builder
                .update_prop_str(node_id, &key, &s)
                .map_err(map_err)?;
        } else if let Ok(i) = value.extract::<i64>() {
            self.builder
                .update_prop_i64(node_id, &key, i)
                .map_err(map_err)?;
        } else if let Ok(f) = value.extract::<f64>() {
            self.builder
                .update_prop_f64(node_id, &key, f)
                .map_err(map_err)?;
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Property value must be str, int, float, or bool",
            ));
        }
        Ok(())
    }

    /// Update string property (removes old, sets new)
    fn update_prop_str(&mut self, node_id: u32, key: String, value: String) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .update_prop_str(node_id, &key, &value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Update i64 property
    fn update_prop_i64(&mut self, node_id: u32, key: String, value: i64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .update_prop_i64(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Update f64 property
    fn update_prop_f64(&mut self, node_id: u32, key: String, value: f64) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .update_prop_f64(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Update boolean property
    fn update_prop_bool(&mut self, node_id: u32, key: String, value: bool) -> PyResult<()> {
        self.check_not_finalized()?;
        self.builder
            .update_prop_bool(node_id, &key, value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get nodes with a specific property value, scoped by label (before finalization)
    ///
    /// # Arguments
    /// * `label` - The label to scope the query to
    /// * `key` - The property key
    /// * `value` - The property value to search for
    #[pyo3(signature = (label, key, value))]
    fn nodes_with_property(
        &self,
        label: String,
        key: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<u32>> {
        self.check_not_finalized()?;
        let prop_value = py_to_property_value(value)?;
        let value_id = match prop_value {
            rustychickpeas_core::PropertyValue::String(_s) => {
                // Need to intern the string to get ID
                // For now, we can't easily do this without exposing interner
                // TODO: Add helper to convert PropertyValue to ValueId
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "String property queries not yet supported in GraphBuilder",
                ));
            }
            rustychickpeas_core::PropertyValue::Integer(i) => ValueId::I64(i),
            rustychickpeas_core::PropertyValue::Float(f) => ValueId::from_f64(f),
            rustychickpeas_core::PropertyValue::Boolean(b) => ValueId::Bool(b),
            rustychickpeas_core::PropertyValue::InternedString(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "InternedString not supported in GraphBuilder queries",
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
        let keys_to_index: Option<Vec<&str>> = index_properties
            .as_ref()
            .map(|names| names.iter().map(|s| s.as_str()).collect());

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
    fn finalize_into(
        &mut self,
        manager: &RustyChickpeas,
        index_properties: Option<Vec<String>>,
    ) -> PyResult<()> {
        self.check_not_finalized()?;
        self.finalized = true;
        let builder = std::mem::replace(&mut self.builder, GraphBuilder::new(None, None));

        // Convert Vec<String> to Vec<&str> for the Rust API
        let keys_to_index: Option<Vec<&str>> = index_properties
            .as_ref()
            .map(|names| names.iter().map(|s| s.as_str()).collect());

        let snapshot = builder.finalize(keys_to_index.as_deref());
        manager.manager.add_snapshot(snapshot);
        Ok(())
    }
}

/// A relationship endpoint for `load_relationships_from_csv_multi`: a CSV `column`
/// holding either the internal node id (no `label`) or a value resolved to a node of
/// `label` by its `property_key` (default `"id"`).
#[pyclass]
#[derive(Clone)]
pub struct Ref {
    node_ref: rustychickpeas_core::types::NodeReference,
}

#[pymethods]
impl Ref {
    #[new]
    #[pyo3(signature = (column, label=None, property_key="id".to_string()))]
    fn new(column: String, label: Option<String>, property_key: String) -> Ref {
        use rustychickpeas_core::types::NodeReference;
        let node_ref = match label {
            Some(label) => NodeReference::Property {
                column,
                property_key,
                label: Some(label),
            },
            None => NodeReference::Id(column),
        };
        Ref { node_ref }
    }

    fn __repr__(&self) -> String {
        format!("Ref({:?})", self.node_ref)
    }
}

/// A renamed/typed relationship property for `load_relationships_from_csv_multi`:
/// store CSV `column` (default = `name`) under property `name`, parsed as `type`
/// (`int` / `float` / `bool` / `str`, default auto-detect).
#[pyclass]
#[derive(Clone)]
pub struct Prop {
    spec: rustychickpeas_core::RelPropSpec,
}

#[pymethods]
impl Prop {
    #[new]
    #[pyo3(signature = (name, column=None, r#type=None))]
    fn new(
        py: Python<'_>,
        name: String,
        column: Option<String>,
        r#type: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Prop> {
        let column = column.unwrap_or_else(|| name.clone());
        let col_type = col_type_from_py(py, r#type.as_ref())?;
        Ok(Prop {
            spec: rustychickpeas_core::RelPropSpec {
                name,
                column,
                col_type,
            },
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Prop(name='{}', column='{}', type={:?})",
            self.spec.name, self.spec.column, self.spec.col_type
        )
    }
}

/// One relationship type for `load_relationships_from_csv_multi`.
#[pyclass]
#[derive(Clone)]
pub struct Rel {
    spec: rustychickpeas_core::RelLoadSpec,
}

#[pymethods]
impl Rel {
    #[new]
    #[pyo3(signature = (rel_type, start, end, props=None))]
    fn new(rel_type: String, start: Ref, end: Ref, props: Option<Vec<Prop>>) -> Rel {
        Rel {
            spec: rustychickpeas_core::RelLoadSpec {
                rel_type,
                start: start.node_ref,
                end: end.node_ref,
                properties: props
                    .unwrap_or_default()
                    .into_iter()
                    .map(|p| p.spec)
                    .collect(),
            },
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Rel(rel_type='{}', start={:?}, end={:?}, props={})",
            self.spec.rel_type,
            self.spec.start,
            self.spec.end,
            self.spec.properties.len()
        )
    }
}

/// Map a Python type object (`int` / `float` / `bool` / `str`) to a `CsvColumnType`;
/// `None` means auto-detect.
fn col_type_from_py(
    py: Python<'_>,
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<rustychickpeas_core::CsvColumnType> {
    use rustychickpeas_core::CsvColumnType;
    let Some(obj) = obj else {
        return Ok(CsvColumnType::Auto);
    };
    // bool is a subclass of int — check it first.
    if obj.is(&py.get_type::<pyo3::types::PyBool>()) {
        Ok(CsvColumnType::Bool)
    } else if obj.is(&py.get_type::<pyo3::types::PyInt>()) {
        Ok(CsvColumnType::Int64)
    } else if obj.is(&py.get_type::<pyo3::types::PyFloat>()) {
        Ok(CsvColumnType::Float64)
    } else if obj.is(&py.get_type::<pyo3::types::PyString>()) {
        Ok(CsvColumnType::String)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Prop type must be int, float, bool, or str",
        ))
    }
}
