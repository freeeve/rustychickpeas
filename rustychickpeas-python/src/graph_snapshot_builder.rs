//! GraphSnapshotBuilder Python wrapper

use pyo3::prelude::*;
use pyo3::types::PyBool;
use rustychickpeas_core::{GraphBuilder, ValueId};
use crate::graph_snapshot::GraphSnapshot;
use crate::rusty_chickpeas::RustyChickpeas;
use crate::utils::py_to_property_value;

/// Python wrapper for GraphBuilder
#[pyclass(name = "GraphSnapshotBuilder")]
pub struct GraphSnapshotBuilder {
    pub(crate) builder: GraphBuilder,
}

#[pymethods]
impl GraphSnapshotBuilder {
    #[new]
    #[pyo3(signature = (version=None, capacity_nodes=None, capacity_rels=None))]
    fn new(version: Option<String>, capacity_nodes: Option<usize>, capacity_rels: Option<usize>) -> Self {
        let builder = if let Some(v) = version {
            GraphBuilder::with_version(&v, capacity_nodes, capacity_rels)
        } else {
            GraphBuilder::new(capacity_nodes, capacity_rels)
        };
        Self { builder }
    }

    /// Add a node with labels
    fn add_node(&mut self, ext_id: u64, labels: Vec<String>) -> PyResult<()> {
        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
        self.builder.add_node(ext_id, &label_refs);
        Ok(())
    }

    /// Add a relationship
    fn add_rel(&mut self, ext_u: u64, ext_v: u64, rel_type: String) -> PyResult<()> {
        self.builder.add_rel(ext_u, ext_v, &rel_type);
        Ok(())
    }

    /// Set property with automatic type detection
    /// Automatically calls the correct type-specific method based on the value type
    fn set_prop(&mut self, ext_id: u64, key: String, value: &PyAny) -> PyResult<()> {
        // Check bool first, as True/False can be extracted as int
        if let Ok(b) = value.extract::<bool>() {
            self.builder.set_prop_bool(ext_id, &key, b);
        } else if let Ok(s) = value.extract::<String>() {
            self.builder.set_prop_str(ext_id, &key, &s);
        } else if let Ok(i) = value.extract::<i64>() {
            self.builder.set_prop_i64(ext_id, &key, i);
        } else if let Ok(f) = value.extract::<f64>() {
            self.builder.set_prop_f64(ext_id, &key, f);
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Property value must be str, int, float, or bool"
            ));
        }
        Ok(())
    }

    /// Set string property
    fn set_prop_str(&mut self, ext_id: u64, key: String, value: String) -> PyResult<()> {
        self.builder.set_prop_str(ext_id, &key, &value);
        Ok(())
    }

    /// Set i64 property
    fn set_prop_i64(&mut self, ext_id: u64, key: String, value: i64) -> PyResult<()> {
        self.builder.set_prop_i64(ext_id, &key, value);
        Ok(())
    }

    /// Set f64 property
    fn set_prop_f64(&mut self, ext_id: u64, key: String, value: f64) -> PyResult<()> {
        self.builder.set_prop_f64(ext_id, &key, value);
        Ok(())
    }

    /// Set boolean property
    fn set_prop_bool(&mut self, ext_id: u64, key: String, value: bool) -> PyResult<()> {
        self.builder.set_prop_bool(ext_id, &key, value);
        Ok(())
    }

    /// Load nodes from a Parquet file into the builder
    fn load_nodes_from_parquet(
        &mut self,
        path: String,
        node_id_column: Option<String>,
        label_columns: Option<Vec<String>>,
        property_columns: Option<Vec<String>>,
    ) -> PyResult<Vec<u64>> {
        let label_cols = label_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let prop_cols = property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        
        self.builder
            .load_nodes_from_parquet(
                &path,
                node_id_column.as_deref(),
                label_cols,
                prop_cols,
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Load relationships from a Parquet file into the builder
    fn load_relationships_from_parquet(
        &mut self,
        path: String,
        start_node_column: String,
        end_node_column: String,
        rel_type_column: Option<String>,
        property_columns: Option<Vec<String>>,
        fixed_rel_type: Option<String>,
    ) -> PyResult<Vec<(u64, u64)>> {
        let prop_cols = property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        
        self.builder
            .load_relationships_from_parquet(
                &path,
                &start_node_column,
                &end_node_column,
                rel_type_column.as_deref(),
                prop_cols,
                fixed_rel_type.as_deref(),
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get property value for a node (before finalization)
    fn get_property(&self, ext_id: u64, key: String) -> PyResult<Option<PyObject>> {
        let value_id = self.builder.get_prop(ext_id, &key);
        
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
    fn update_prop(&mut self, ext_id: u64, key: String, value: &PyAny) -> PyResult<()> {
        // Check bool first, as True/False can be extracted as int
        if let Ok(b) = value.extract::<bool>() {
            self.builder.update_prop_bool(ext_id, &key, b);
        } else if let Ok(s) = value.extract::<String>() {
            self.builder.update_prop_str(ext_id, &key, &s);
        } else if let Ok(i) = value.extract::<i64>() {
            self.builder.update_prop_i64(ext_id, &key, i);
        } else if let Ok(f) = value.extract::<f64>() {
            self.builder.update_prop_f64(ext_id, &key, f);
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Property value must be str, int, float, or bool"
            ));
        }
        Ok(())
    }

    /// Update string property (removes old, sets new)
    fn update_property_str(&mut self, ext_id: u64, key: String, value: String) -> PyResult<()> {
        self.builder.update_prop_str(ext_id, &key, &value);
        Ok(())
    }

    /// Update i64 property
    fn update_property_i64(&mut self, ext_id: u64, key: String, value: i64) -> PyResult<()> {
        self.builder.update_prop_i64(ext_id, &key, value);
        Ok(())
    }

    /// Update f64 property
    fn update_property_f64(&mut self, ext_id: u64, key: String, value: f64) -> PyResult<()> {
        self.builder.update_prop_f64(ext_id, &key, value);
        Ok(())
    }

    /// Update boolean property
    fn update_property_bool(&mut self, ext_id: u64, key: String, value: bool) -> PyResult<()> {
        self.builder.update_prop_bool(ext_id, &key, value);
        Ok(())
    }

    /// Get nodes with a specific property value (before finalization)
    fn get_nodes_with_property(&self, key: String, value: &PyAny) -> PyResult<Vec<u64>> {
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
        
        let node_ids = self.builder.get_nodes_with_property(&key, value_id);
        // Convert internal NodeIds back to external IDs
        // We need a reverse mapping - for now, return internal IDs
        // TODO: Add ext2int reverse mapping or return external IDs
        Ok(node_ids.iter().map(|&id| id as u64).collect())
    }

    /// Get node labels (before finalization)
    fn get_node_labels(&self, ext_id: u64) -> PyResult<Vec<String>> {
        Ok(self.builder.get_node_labels(ext_id))
    }

    /// Get neighbors of a node (before finalization)
    /// Returns (outgoing, incoming) as tuple of lists
    fn get_neighbors(&self, ext_id: u64) -> PyResult<(Vec<u64>, Vec<u64>)> {
        let (out, inc) = self.builder.get_neighbors(ext_id);
        // Convert to external IDs - for now return internal IDs
        // TODO: Add reverse mapping
        Ok((out.iter().map(|&id| id as u64).collect(),
            inc.iter().map(|&id| id as u64).collect()))
    }

    /// Set the version for this snapshot
    /// This version will be stored with the snapshot when finalized
    fn set_version(&mut self, version: String) -> PyResult<()> {
        self.builder.set_version(&version);
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
        // We need to take ownership to finalize, so we'll use replace
        let builder = std::mem::replace(&mut self.builder, GraphBuilder::new(None, None));
        
        // Convert property key names to PropertyKey IDs if provided
        let keys_to_index = if let Some(prop_names) = index_properties {
            let mut keys = Vec::new();
            for name in prop_names {
                if let Some(key_id) = builder.get_property_key_id(&name) {
                    keys.push(key_id);
                }
            }
            Some(keys)
        } else {
            None
        };
        
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
        let builder = std::mem::replace(&mut self.builder, GraphBuilder::new(None, None));
        
        // Convert property key names to PropertyKey IDs if provided
        let keys_to_index = if let Some(prop_names) = index_properties {
            let mut keys = Vec::new();
            for name in prop_names {
                if let Some(key_id) = builder.get_property_key_id(&name) {
                    keys.push(key_id);
                }
            }
            Some(keys)
        } else {
            None
        };
        
        let snapshot = builder.finalize(keys_to_index.as_deref());
        manager.manager.add_snapshot(snapshot);
        Ok(())
    }
}

