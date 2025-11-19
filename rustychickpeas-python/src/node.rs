//! Node Python wrapper

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict};
use rustychickpeas_core::{GraphSnapshot as CoreGraphSnapshot, RelationshipType, ValueId};
use crate::direction::Direction;
use crate::relationship::Relationship;

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
        // Check if property key exists first (O(1) lookup using reverse index)
        let key_id = self.snapshot.atoms.get_id(&key);
        
        if key_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Property key '{}' not found", key)
            ));
        }
        
        let value_id = self.snapshot.prop(self.node_id, &key);
        
        Python::with_gil(|py| {
            if let Some(vid) = value_id {
                match vid {
                    ValueId::Str(sid) => {
                        if let Some(s) = self.snapshot.resolve_string(sid) {
                            Ok(Some(s.to_object(py)))
                        } else {
                            Ok(None)
                        }
                    }
                    ValueId::I64(i) => Ok(Some(i.to_object(py))),
                    ValueId::F64(bits) => {
                        Ok(Some(f64::from_bits(bits).to_object(py)))
                    }
                    ValueId::Bool(b) => {
                        Ok(Some(PyBool::new(py, b).into_py(py)))
                    }
                }
            } else {
                Ok(None)
            }
        })
    }

    /// Get relationships of this node as Relationship objects
    /// 
    /// # Arguments
    /// * `direction` - Direction of relationships (Outgoing, Incoming, Both)
    /// * `rel_types` - Optional list of relationship types to filter by
    #[pyo3(signature = (direction, rel_types=None))]
    fn relationships(&self, direction: Direction, rel_types: Option<Vec<String>>) -> PyResult<Vec<Relationship>> {
        // Convert string types to RelationshipType IDs
        let rel_type_ids: Option<Vec<RelationshipType>> = rel_types.as_ref().map(|types| {
            let ids: Vec<RelationshipType> = types.iter()
                .filter_map(|s| {
                    self.snapshot.atoms.strings.iter()
                        .position(|st| st == s)
                        .map(|idx| RelationshipType::new(idx as u32))
                })
                .collect();
            // If filter was provided but none found, return None to indicate no matches
            if ids.is_empty() && !types.is_empty() {
                return None; // Will be handled below
            }
            Some(ids)
        }).flatten();

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
                    let source_node = self.snapshot.in_nbrs[idx];
                    
                    // Apply type filter if provided
                    if let Some(type_ids) = rel_type_ids.as_ref() {
                        if !type_ids.contains(&rel_type) {
                            continue;
                        }
                    } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                        // Filter was provided but no types found - skip
                        continue;
                    }
                    
                    // Find the corresponding outgoing index for this relationship
                    // We need to search the source node's outgoing relationships
                    let source_start = self.snapshot.out_offsets[source_node as usize] as usize;
                    let source_end = self.snapshot.out_offsets[source_node as usize + 1] as usize;
                    
                    // Find the relationship in the source node's outgoing edges
                    for out_idx in source_start..source_end {
                        if self.snapshot.out_nbrs[out_idx] == self.node_id &&
                           self.snapshot.out_types[out_idx] == rel_type {
                            relationships.push(Relationship {
                                snapshot: self.snapshot.clone(),
                                rel_index: out_idx as u32,
                                is_outgoing: true, // Use canonical outgoing index
                            });
                            break;
                        }
                    }
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
                    let source_node = self.snapshot.in_nbrs[in_idx];
                    
                    // Apply type filter if provided
                    if let Some(type_ids) = rel_type_ids.as_ref() {
                        if !type_ids.contains(&rel_type) {
                            continue;
                        }
                    } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                        continue;
                    }
                    
                    // Find the corresponding outgoing index
                    let source_start = self.snapshot.out_offsets[source_node as usize] as usize;
                    let source_end = self.snapshot.out_offsets[source_node as usize + 1] as usize;
                    
                    for out_idx in source_start..source_end {
                        if self.snapshot.out_nbrs[out_idx] == self.node_id &&
                           self.snapshot.out_types[out_idx] == rel_type {
                            relationships.push(Relationship {
                                snapshot: self.snapshot.clone(),
                                rel_index: out_idx as u32,
                                is_outgoing: true,
                            });
                            break;
                        }
                    }
                }
            }
        }

        Ok(relationships)
    }

    /// Get relationship IDs (neighbor node IDs) for this node
    /// 
    /// Returns the IDs of nodes connected to this node via relationships.
    /// This is a lighter-weight alternative to relationships() when you only need node IDs.
    /// 
    /// # Arguments
    /// * `direction` - Direction of relationships (Outgoing, Incoming, Both)
    /// * `rel_types` - Optional list of relationship types to filter by
    #[pyo3(signature = (direction, rel_types=None))]
    fn relationship_ids(&self, direction: Direction, rel_types: Option<Vec<String>>) -> PyResult<Vec<u32>> {
        // Convert Vec<String> to Vec<&str> for the Rust API
        let rel_type_strs: Option<Vec<&str>> = rel_types.as_ref().map(|types| {
            types.iter().map(|s| s.as_str()).collect()
        });

        let neighbor_ids = match direction {
            Direction::Outgoing => {
                if let Some(type_strs) = rel_type_strs.as_ref() {
                    self.snapshot.out_neighbors_by_type(self.node_id, type_strs)
                } else {
                    self.snapshot.out_neighbors(self.node_id).to_vec()
                }
            }
            Direction::Incoming => {
                if let Some(type_strs) = rel_type_strs.as_ref() {
                    self.snapshot.in_neighbors_by_type(self.node_id, type_strs)
                } else {
                    self.snapshot.in_neighbors(self.node_id).to_vec()
                }
            }
            Direction::Both => {
                let mut neighbors = Vec::new();
                if let Some(type_strs) = rel_type_strs.as_ref() {
                    neighbors.extend(self.snapshot.out_neighbors_by_type(self.node_id, type_strs));
                    neighbors.extend(self.snapshot.in_neighbors_by_type(self.node_id, type_strs));
                } else {
                    neighbors.extend_from_slice(self.snapshot.out_neighbors(self.node_id));
                    neighbors.extend_from_slice(self.snapshot.in_neighbors(self.node_id));
                }
                neighbors
            }
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

    /// Get degree (number of relationships) for this node
    fn degree(&self, direction: Direction) -> PyResult<usize> {
        match direction {
            Direction::Outgoing => Ok(self.snapshot.out_neighbors(self.node_id).len()),
            Direction::Incoming => Ok(self.snapshot.in_neighbors(self.node_id).len()),
            Direction::Both => {
                Ok(self.snapshot.out_neighbors(self.node_id).len() + 
                   self.snapshot.in_neighbors(self.node_id).len())
            }
        }
    }

    /// Get the internal node ID
    fn id(&self) -> u32 {
        self.node_id
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

