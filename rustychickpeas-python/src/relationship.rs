//! Relationship Python wrapper

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict};
use rustychickpeas_core::GraphSnapshot as CoreGraphSnapshot;
use crate::node::Node;

/// Python wrapper for a Relationship in a GraphSnapshot
/// Relationships are identified by their position in the CSR arrays
#[pyclass(name = "Relationship")]
pub struct Relationship {
    pub(crate) snapshot: std::sync::Arc<CoreGraphSnapshot>,
    /// Index in the CSR arrays (out_nbrs/out_types or in_nbrs/in_types)
    pub(crate) rel_index: u32,
    /// Whether this is an outgoing relationship (true) or incoming (false)
    pub(crate) is_outgoing: bool,
}

#[pymethods]
impl Relationship {
    /// Get the start node (source) of this relationship
    fn get_start_node(&self) -> PyResult<Node> {
        let start_id = if self.is_outgoing {
            // For outgoing relationships, find which node has this relationship
            // We need to find the node whose offset range contains rel_index
            self.find_node_for_outgoing_rel(self.rel_index)
        } else {
            // For incoming relationships, the start node is in in_nbrs
            if self.rel_index as usize >= self.snapshot.in_nbrs.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid relationship index"
                ));
            }
            self.snapshot.in_nbrs[self.rel_index as usize]
        };
        
        Ok(Node {
            snapshot: self.snapshot.clone(),
            node_id: start_id,
        })
    }

    /// Get the end node (destination) of this relationship
    fn get_end_node(&self) -> PyResult<Node> {
        let end_id = if self.is_outgoing {
            // For outgoing relationships, the end node is in out_nbrs
            if self.rel_index as usize >= self.snapshot.out_nbrs.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid relationship index"
                ));
            }
            self.snapshot.out_nbrs[self.rel_index as usize]
        } else {
            // For incoming relationships, find which node has this relationship
            self.find_node_for_incoming_rel(self.rel_index)
        };
        
        Ok(Node {
            snapshot: self.snapshot.clone(),
            node_id: end_id,
        })
    }

    /// Get the relationship type
    fn get_type(&self) -> PyResult<String> {
        let rel_type = if self.is_outgoing {
            if self.rel_index as usize >= self.snapshot.out_types.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid relationship index"
                ));
            }
            self.snapshot.out_types[self.rel_index as usize]
        } else {
            if self.rel_index as usize >= self.snapshot.in_types.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid relationship index"
                ));
            }
            self.snapshot.in_types[self.rel_index as usize]
        };
        
        if let Some(type_str) = self.snapshot.resolve_string(rel_type.id()) {
            Ok(type_str.to_string())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Could not resolve relationship type"
            ))
        }
    }

    /// Get the relationship index (internal ID)
    fn id(&self) -> u32 {
        self.rel_index
    }

    /// Get property value for this relationship
    fn get_property(&self, key: String) -> PyResult<Option<PyObject>> {
        // Find property key ID - return None if key doesn't exist
        let key_id = match self.snapshot.atoms.strings.iter()
            .position(|s| s == &key)
            .map(|idx| idx as u32) {
            Some(id) => id,
            None => return Ok(None), // Key doesn't exist, property doesn't exist
        };
        
        // Get property value using outgoing CSR position (rel_index)
        // Note: For incoming relationships, we still use rel_index as the CSR position
        // because relationship properties are indexed by their position in the outgoing CSR
        let value_id = self.snapshot.get_rel_property(self.rel_index, key_id);
        
        Python::with_gil(|py| {
            if let Some(vid) = value_id {
                match vid {
                    rustychickpeas_core::ValueId::Str(sid) => {
                        if let Some(s) = self.snapshot.resolve_string(sid) {
                            Ok(Some(s.to_object(py)))
                        } else {
                            Ok(None)
                        }
                    }
                    rustychickpeas_core::ValueId::I64(i) => Ok(Some(i.to_object(py))),
                    rustychickpeas_core::ValueId::F64(bits) => {
                        Ok(Some(f64::from_bits(bits).to_object(py)))
                    }
                    rustychickpeas_core::ValueId::Bool(b) => {
                        Ok(Some(PyBool::new(py, b).into_py(py)))
                    }
                }
            } else {
                Ok(None)
            }
        })
    }

    /// Convert relationship to a Python dict (zero-allocation where possible)
    /// Returns a dict with: id, type, start_node, end_node, properties (nested)
    fn to_dict(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            
            // Add ID
            dict.set_item("id", self.rel_index)?;
            
            // Add type
            let rel_type = self.get_type()?;
            dict.set_item("type", rel_type)?;
            
            // Add start and end nodes (as IDs)
            let start_node = self.get_start_node()?;
            let end_node = self.get_end_node()?;
            dict.set_item("start_node", start_node.id_internal())?;
            dict.set_item("end_node", end_node.id_internal())?;
            
            // Add properties (nested in properties dict)
            let properties = PyDict::new(py);
            // Get all relationship properties by iterating through rel_columns
            // This is a bit inefficient but works for now
            for (key_id, _column) in &self.snapshot.rel_columns {
                if let Some(key_str) = self.snapshot.resolve_string(*key_id) {
                    if let Ok(Some(value)) = self.get_property(key_str.to_string()) {
                        properties.set_item(key_str, value)?;
                    }
                }
            }
            dict.set_item("properties", properties)?;
            
            Ok(dict.into())
        })
    }

    /// Convert relationship to JSON string
    fn to_json(&self) -> PyResult<String> {
        Python::with_gil(|py| {
            let dict = self.to_dict()?;
            let json_module = py.import("json")?;
            let json_dumps = json_module.getattr("dumps")?;
            let json_str: String = json_dumps.call1((dict,))?.extract()?;
            Ok(json_str)
        })
    }
}

impl Relationship {
    /// Find which node has an outgoing relationship at the given index
    /// Uses binary search for O(log n) lookup
    fn find_node_for_outgoing_rel(&self, rel_index: u32) -> u32 {
        // Binary search through out_offsets
        let offsets = &self.snapshot.out_offsets;
        let mut left = 0;
        let mut right = offsets.len().saturating_sub(1);
        
        while left < right {
            let mid = (left + right + 1) / 2;
            if offsets[mid] <= rel_index {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        left as u32
    }

    /// Find which node has an incoming relationship at the given index
    /// Uses binary search for O(log n) lookup
    fn find_node_for_incoming_rel(&self, rel_index: u32) -> u32 {
        // Binary search through in_offsets
        let offsets = &self.snapshot.in_offsets;
        let mut left = 0;
        let mut right = offsets.len().saturating_sub(1);
        
        while left < right {
            let mid = (left + right + 1) / 2;
            if offsets[mid] <= rel_index {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        left as u32
    }
}

