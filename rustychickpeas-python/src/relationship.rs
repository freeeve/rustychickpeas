//! Relationship Python wrapper

use crate::node::Node;
use crate::utils::{stable_hash_u64, value_id_to_pyobject};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rustychickpeas_core::GraphSnapshot as CoreGraphSnapshot;

/// Python wrapper for a Relationship in a GraphSnapshot
/// Relationships are identified by their position in the CSR arrays
#[pyclass(name = "Relationship")]
pub struct Relationship {
    pub(crate) snapshot: std::sync::Arc<CoreGraphSnapshot>,
    /// Index in the CSR arrays (out_nbrs/out_types when outgoing, in_nbrs/in_types
    /// when incoming).
    pub(crate) rel_index: u32,
    /// Whether this is an outgoing relationship (true) or incoming (false)
    pub(crate) is_outgoing: bool,
}

#[pymethods]
impl Relationship {
    /// The source [`Node`] of this relationship (its `Outgoing` endpoint). Raises
    /// `ValueError` if the relationship index is out of range.
    fn start_node(&self) -> PyResult<Node> {
        let start_id = if self.is_outgoing {
            // For outgoing relationships, find which node has this relationship
            // We need to find the node whose offset range contains rel_index
            self.find_node_for_outgoing_rel(self.rel_index)
        } else {
            // For incoming relationships, the start node is in in_nbrs
            if self.rel_index as usize >= self.snapshot.in_nbrs.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid relationship index",
                ));
            }
            self.snapshot.in_nbrs[self.rel_index as usize]
        };

        Ok(Node {
            snapshot: self.snapshot.clone(),
            node_id: start_id,
        })
    }

    /// The destination [`Node`] of this relationship (its `Incoming` endpoint).
    /// Raises `ValueError` if the relationship index is out of range.
    fn end_node(&self) -> PyResult<Node> {
        let end_id = if self.is_outgoing {
            // For outgoing relationships, the end node is in out_nbrs
            if self.rel_index as usize >= self.snapshot.out_nbrs.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid relationship index",
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

    /// The relationship type as a string. Raises `ValueError` if the index is out
    /// of range or the type cannot be resolved.
    fn rel_type(&self) -> PyResult<String> {
        let rel_type = if self.is_outgoing {
            if self.rel_index as usize >= self.snapshot.out_types.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid relationship index",
                ));
            }
            self.snapshot.out_types[self.rel_index as usize]
        } else {
            if self.rel_index as usize >= self.snapshot.in_types.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid relationship index",
                ));
            }
            self.snapshot.in_types[self.rel_index as usize]
        };

        if let Some(type_str) = self.snapshot.resolve_string(rel_type.id()) {
            Ok(type_str.to_string())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Could not resolve relationship type",
            ))
        }
    }

    /// Deprecated alias for [`rel_type`](Self::rel_type), kept for backward
    /// compatibility; prefer `rel_type()`.
    fn reltype(&self) -> PyResult<String> {
        self.rel_type()
    }

    /// The internal index identifying this relationship within the snapshot.
    fn id(&self) -> u32 {
        self.rel_index
    }

    fn __repr__(&self) -> PyResult<String> {
        let rel_type = self.rel_type()?;
        let start = self.start_node()?;
        let end = self.end_node()?;
        Ok(format!(
            "Relationship(id={}, type={}, {}->{})",
            self.rel_index,
            rel_type,
            start.id_internal(),
            end.id_internal()
        ))
    }

    fn __eq__(&self, other: &Relationship) -> bool {
        self.rel_index == other.rel_index
    }

    /// Stable, deterministic hash of the relationship index
    /// Consistent with `__eq__`: equal relationships always have equal hashes.
    fn __hash__(&self) -> u64 {
        stable_hash_u64(self.rel_index as u64)
    }

    /// Get property value for this relationship
    fn get_property(&self, key: String) -> PyResult<Option<PyObject>> {
        // Relationship properties are stored by outgoing CSR position. For an
        // incoming relationship `rel_index` addresses the incoming CSR, so map
        // it to the corresponding outgoing position first.
        let prop_index = if self.is_outgoing {
            self.rel_index
        } else {
            self.snapshot
                .in_to_out
                .get(self.rel_index as usize)
                .copied()
                .unwrap_or(self.rel_index)
        };
        let value_id = self.snapshot.rel_prop(prop_index, &key).map(|p| p.value());

        Python::with_gil(|py| {
            Ok(value_id.and_then(|vid| value_id_to_pyobject(py, vid, &self.snapshot.atoms)))
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
            let rel_type = self.reltype()?;
            dict.set_item("type", rel_type)?;

            // Add start and end nodes (as IDs)
            let start_node = self.start_node()?;
            let end_node = self.end_node()?;
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
            let mid = (left + right).div_ceil(2);
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
            let mid = (left + right).div_ceil(2);
            if offsets[mid] <= rel_index {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        left as u32
    }
}
