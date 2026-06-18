//! Node Python wrapper

use crate::direction::Direction;
use crate::relationship::Relationship;
use crate::utils::{stable_hash_u64, value_id_to_pyobject};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rustychickpeas_core::{GraphSnapshot as CoreGraphSnapshot, RelationshipType};

/// Python wrapper for a Node in a GraphSnapshot
#[pyclass(name = "Node")]
pub struct Node {
    pub(crate) snapshot: std::sync::Arc<CoreGraphSnapshot>,
    pub(crate) node_id: u32,
}

#[pymethods]
impl Node {
    /// Get property value for this node
    fn get_property(&self, key: String) -> PyResult<Option<PyObject>> {
        let key_id = self.snapshot.atoms.get_id(&key);

        if key_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Property key '{}' not found",
                key
            )));
        }

        let value_id = self.snapshot.prop(self.node_id, &key).map(|p| p.value());

        Python::with_gil(|py| {
            Ok(value_id.and_then(|vid| value_id_to_pyobject(py, vid, &self.snapshot.atoms)))
        })
    }

    /// Get relationships of this node as Relationship objects
    ///
    /// # Arguments
    /// * `direction` - Direction of relationships (Outgoing, Incoming, Both)
    /// * `rel_types` - Optional list of relationship types to filter by
    #[pyo3(signature = (direction, rel_types=None))]
    fn relationships(
        &self,
        direction: Direction,
        rel_types: Option<Vec<String>>,
    ) -> PyResult<Vec<Relationship>> {
        // Convert string types to RelationshipType IDs via the public resolver.
        let rel_type_ids: Option<Vec<RelationshipType>> = rel_types.as_ref().and_then(|types| {
            let ids: Vec<RelationshipType> = types
                .iter()
                .filter_map(|s| self.snapshot.rel_type(s))
                .collect();
            if ids.is_empty() && !types.is_empty() {
                return None;
            }
            Some(ids)
        });

        let mut relationships = Vec::new();

        match direction {
            Direction::Outgoing => {
                let start = self.snapshot.out_offsets[self.node_id as usize] as usize;
                let end = self.snapshot.out_offsets[self.node_id as usize + 1] as usize;

                for idx in start..end {
                    let rel_type = self.snapshot.out_types[idx];

                    // Apply type filter if provided
                    if let Some(type_ids) = rel_type_ids.as_ref() {
                        if !type_ids.contains(&rel_type) {
                            continue;
                        }
                    } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                        // Filter was provided but no types found - skip
                        continue;
                    }

                    relationships.push(Relationship {
                        snapshot: self.snapshot.clone(),
                        rel_index: idx as u32,
                        is_outgoing: true,
                    });
                }
            }
            Direction::Incoming => {
                let start = self.snapshot.in_offsets[self.node_id as usize] as usize;
                let end = self.snapshot.in_offsets[self.node_id as usize + 1] as usize;

                for idx in start..end {
                    let rel_type = self.snapshot.in_types[idx];

                    // Apply type filter if provided
                    if let Some(type_ids) = rel_type_ids.as_ref() {
                        if !type_ids.contains(&rel_type) {
                            continue;
                        }
                    } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                        // Filter was provided but no types found - skip
                        continue;
                    }

                    // Canonical outgoing CSR index: O(1) via the in->out map when
                    // present (deserialized graphs), else scan the source's out-edges.
                    let rel_index = match self.snapshot.in_to_out.get(idx) {
                        Some(&out) => out,
                        None => {
                            let source = self.snapshot.in_nbrs[idx] as usize;
                            let s = self.snapshot.out_offsets[source] as usize;
                            let e = self.snapshot.out_offsets[source + 1] as usize;
                            (s..e)
                                .find(|&k| {
                                    self.snapshot.out_nbrs[k] == self.node_id
                                        && self.snapshot.out_types[k] == rel_type
                                })
                                .map(|k| k as u32)
                                .unwrap_or(idx as u32)
                        }
                    };
                    relationships.push(Relationship {
                        snapshot: self.snapshot.clone(),
                        rel_index,
                        is_outgoing: true,
                    });
                }
            }
            Direction::Both => {
                // Get outgoing relationships
                let start = self.snapshot.out_offsets[self.node_id as usize] as usize;
                let end = self.snapshot.out_offsets[self.node_id as usize + 1] as usize;

                for idx in start..end {
                    let rel_type = self.snapshot.out_types[idx];

                    // Apply type filter if provided
                    if let Some(type_ids) = rel_type_ids.as_ref() {
                        if !type_ids.contains(&rel_type) {
                            continue;
                        }
                    } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                        continue;
                    }

                    relationships.push(Relationship {
                        snapshot: self.snapshot.clone(),
                        rel_index: idx as u32,
                        is_outgoing: true,
                    });
                }

                // Get incoming relationships (map to outgoing indices)
                let in_start = self.snapshot.in_offsets[self.node_id as usize] as usize;
                let in_end = self.snapshot.in_offsets[self.node_id as usize + 1] as usize;

                for in_idx in in_start..in_end {
                    let rel_type = self.snapshot.in_types[in_idx];

                    // Apply type filter if provided
                    if let Some(type_ids) = rel_type_ids.as_ref() {
                        if !type_ids.contains(&rel_type) {
                            continue;
                        }
                    } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                        continue;
                    }

                    // Canonical outgoing CSR index: O(1) via the in->out map when
                    // present, else scan the source's out-edges.
                    let rel_index = match self.snapshot.in_to_out.get(in_idx) {
                        Some(&out) => out,
                        None => {
                            let source = self.snapshot.in_nbrs[in_idx] as usize;
                            let s = self.snapshot.out_offsets[source] as usize;
                            let e = self.snapshot.out_offsets[source + 1] as usize;
                            (s..e)
                                .find(|&k| {
                                    self.snapshot.out_nbrs[k] == self.node_id
                                        && self.snapshot.out_types[k] == rel_type
                                })
                                .map(|k| k as u32)
                                .unwrap_or(in_idx as u32)
                        }
                    };
                    relationships.push(Relationship {
                        snapshot: self.snapshot.clone(),
                        rel_index,
                        is_outgoing: true,
                    });
                }
            }
        }

        Ok(relationships)
    }

    /// Get neighbor node IDs for this node
    ///
    /// Returns the IDs of nodes connected to this node via relationships.
    /// This is a lighter-weight alternative to relationships() when you only need node IDs.
    ///
    /// # Arguments
    /// * `direction` - Direction of relationships (Outgoing, Incoming, Both)
    /// * `rel_types` - Optional list of relationship types to filter by
    #[pyo3(signature = (direction, rel_types=None))]
    fn neighbor_ids(
        &self,
        direction: Direction,
        rel_types: Option<Vec<String>>,
    ) -> PyResult<Vec<u32>> {
        let neighbor_ids: Vec<u32> = match rel_types {
            Some(types) => {
                let type_strs: Vec<&str> = types.iter().map(|s| s.as_str()).collect();
                self.snapshot
                    .neighbors_by_type(self.node_id, direction.into(), type_strs.as_slice())
                    .collect()
            }
            None => self
                .snapshot
                .neighbors(self.node_id, direction.into())
                .collect(),
        };

        Ok(neighbor_ids)
    }

    /// Get labels for this node
    fn labels(&self) -> PyResult<Vec<String>> {
        let mut labels = Vec::new();
        for (label, node_set) in &self.snapshot.label_index {
            if node_set.contains(self.node_id) {
                if let Some(label_str) = self.snapshot.resolve_string(label.id()) {
                    labels.push(label_str.to_string());
                }
            }
        }
        Ok(labels)
    }

    /// Degree of this node — O(1) from the CSR offsets when untyped; with
    /// `rel_type`, the count of neighbors reached via that type.
    #[pyo3(signature = (direction, rel_type=None))]
    fn degree(&self, direction: Direction, rel_type: Option<&str>) -> usize {
        match rel_type {
            Some(rt) => self
                .snapshot
                .neighbors_by_type(self.node_id, direction.into(), rt)
                .count(),
            None => crate::utils::csr_degree(&self.snapshot, self.node_id, direction.into()),
        }
    }

    /// Get the internal node ID
    fn id(&self) -> u32 {
        self.node_id
    }

    fn __repr__(&self) -> PyResult<String> {
        let labels = self.labels()?;
        Ok(format!(
            "Node(id={}, labels=[{}])",
            self.node_id,
            labels.join(", ")
        ))
    }

    fn __eq__(&self, other: &Node) -> bool {
        self.node_id == other.node_id
    }

    /// Stable, deterministic hash of the node ID
    /// Consistent with `__eq__`: equal nodes always have equal hashes.
    fn __hash__(&self) -> u64 {
        stable_hash_u64(self.node_id as u64)
    }

    /// Convert node to a Python dict (zero-allocation where possible)
    /// Returns a dict with: id, labels, and properties (nested)
    fn to_dict(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = PyDict::new(py);

            // Add ID
            dict.set_item("id", self.node_id)?;

            // Add labels
            let labels = self.labels()?;
            dict.set_item("labels", labels)?;

            // Add all properties (nested in properties dict)
            let properties = PyDict::new(py);
            for (key_id, _column) in &self.snapshot.columns {
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

    /// Convert node to JSON string
    /// Note: This requires allocation for the JSON string, but minimizes intermediate allocations
    fn to_json(&self) -> PyResult<String> {
        Python::with_gil(|py| {
            let dict = self.to_dict()?;
            // Use Python's json module to serialize (most efficient)
            let json_module = py.import("json")?;
            let json_dumps = json_module.getattr("dumps")?;
            let json_str: String = json_dumps.call1((dict,))?.extract()?;
            Ok(json_str)
        })
    }
}

impl Node {
    /// Get the internal node ID (for use by other Rust modules)
    pub(crate) fn id_internal(&self) -> u32 {
        self.node_id
    }
}
