//! GraphSnapshot Python wrapper

use pyo3::prelude::*;
use pyo3::types::PyBool;
use rustychickpeas_core::{
    GraphSnapshot as CoreGraphSnapshot, Label, RelationshipType, ValueId,
};
use rustychickpeas_core::types::PropertyKey;
use crate::direction::Direction;
use crate::node::Node;
use crate::relationship::Relationship;
use crate::utils::py_to_property_value;

/// Python wrapper for GraphSnapshot
#[pyclass(name = "GraphSnapshot")]
pub struct GraphSnapshot {
    pub(crate) snapshot: std::sync::Arc<CoreGraphSnapshot>,
}

impl GraphSnapshot {
    /// Get string ID from string
    /// Since atoms.strings is indexed by interned string ID, the index matches the ID
    fn get_string_id(&self, s: &str) -> Option<u32> {
        // For now, search linearly (can optimize with cached reverse map later)
        // The index in atoms.strings should match the interned string ID
        self.snapshot.atoms.strings.iter()
            .position(|st| st == s)
            .map(|idx| idx as u32)
    }

    /// Get label from string
    fn label_from_str(&self, s: &str) -> Option<Label> {
        self.get_string_id(s).map(Label::new)
    }

    /// Get relationship type from string
    fn rel_type_from_str(&self, s: &str) -> Option<RelationshipType> {
        self.get_string_id(s).map(RelationshipType::new)
    }

    /// Get property key from string
    fn property_key_from_str(&self, s: &str) -> Option<PropertyKey> {
        self.get_string_id(s)
    }
}

impl GraphSnapshot {
    /// Internal constructor (not exposed to Python)
    /// Takes ownership of a GraphSnapshot
    pub(crate) fn new(snapshot: CoreGraphSnapshot) -> Self {
        Self { snapshot: std::sync::Arc::new(snapshot) }
    }
    
    /// Internal constructor from Arc (for manager.get_graph_snapshot)
    pub(crate) fn from_arc(snapshot: std::sync::Arc<CoreGraphSnapshot>) -> Self {
        Self { snapshot }
    }
}

#[pymethods]
impl GraphSnapshot {

    /// Get number of nodes
    fn n_nodes(&self) -> u32 {
        self.snapshot.n_nodes
    }

    /// Get number of relationships
    fn n_rels(&self) -> u64 {
        self.snapshot.n_rels
    }

    /// Get node labels
    fn get_node_labels(&self, node_id: u32) -> PyResult<Vec<String>> {
        // GraphSnapshot doesn't store labels per node directly
        // We need to iterate through label_index to find which labels contain this node
        let mut labels = Vec::new();
        for (label, node_set) in &self.snapshot.label_index {
            if node_set.contains(node_id) {
                if let Some(label_str) = self.snapshot.resolve_string(label.id()) {
                    labels.push(label_str.to_string());
                }
            }
        }
        Ok(labels)
    }

    /// Get nodes with a specific label
    fn get_nodes_with_label(&self, label: String) -> PyResult<Vec<u32>> {
        let label_id = self.label_from_str(&label)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Label '{}' not found", label)
            ))?;
        
        if let Some(node_set) = self.snapshot.get_nodes_with_label(label_id) {
            Ok(node_set.iter().collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Get a Node object for the given node ID
    fn get_node(&self, node_id: u32) -> PyResult<Node> {
        if node_id >= self.snapshot.n_nodes {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Node ID {} out of range (max: {})", node_id, self.snapshot.n_nodes - 1)
            ));
        }
        Ok(Node {
            snapshot: self.snapshot.clone(),
            node_id,
        })
    }

    /// Get relationships (neighbors) of a node with optional type filtering
    /// 
    /// # Arguments
    /// * `node_id` - The node ID
    /// * `direction` - Direction of relationships (Outgoing, Incoming, Both)
    /// * `rel_types` - Optional list of relationship types to filter by
    #[pyo3(signature = (node_id, direction, rel_types=None))]
    fn get_rels(&self, node_id: u32, direction: Direction, rel_types: Option<Vec<String>>) -> PyResult<Vec<Node>> {
        if node_id >= self.snapshot.n_nodes {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Node ID {} out of range (max: {})", node_id, self.snapshot.n_nodes - 1)
            ));
        }

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

        let neighbor_ids = match direction {
            Direction::Outgoing => {
                if let Some(type_ids) = rel_type_ids.as_ref() {
                    self.snapshot.get_out_neighbors_by_type(node_id, type_ids)
                } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                    // Filter was provided but no types found - return empty
                    Vec::new()
                } else {
                    self.snapshot.get_out_neighbors(node_id).to_vec()
                }
            }
            Direction::Incoming => {
                if let Some(type_ids) = rel_type_ids.as_ref() {
                    self.snapshot.get_in_neighbors_by_type(node_id, type_ids)
                } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                    // Filter was provided but no types found - return empty
                    Vec::new()
                } else {
                    self.snapshot.get_in_neighbors(node_id).to_vec()
                }
            }
            Direction::Both => {
                let mut neighbors = Vec::new();
                if let Some(type_ids) = rel_type_ids.as_ref() {
                    neighbors.extend(self.snapshot.get_out_neighbors_by_type(node_id, type_ids));
                    neighbors.extend(self.snapshot.get_in_neighbors_by_type(node_id, type_ids));
                } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                    // Filter was provided but no types found - return empty
                    // neighbors is already empty
                } else {
                    neighbors.extend_from_slice(self.snapshot.get_out_neighbors(node_id));
                    neighbors.extend_from_slice(self.snapshot.get_in_neighbors(node_id));
                }
                neighbors
            }
        };

        Ok(neighbor_ids.into_iter()
            .map(|id| Node {
                snapshot: self.snapshot.clone(),
                node_id: id,
            })
            .collect())
    }

    /// Get neighbors of a node (deprecated - use get_rels instead)
    #[allow(deprecated)]
    #[deprecated(note = "Use get_rels instead")]
    fn get_neighbors(&self, node_id: u32, direction: Direction) -> PyResult<Vec<u32>> {
        match direction {
            Direction::Outgoing => {
                Ok(self.snapshot.get_out_neighbors(node_id).to_vec())
            }
            Direction::Incoming => {
                Ok(self.snapshot.get_in_neighbors(node_id).to_vec())
            }
            Direction::Both => {
                let mut neighbors = Vec::new();
                neighbors.extend_from_slice(self.snapshot.get_out_neighbors(node_id));
                neighbors.extend_from_slice(self.snapshot.get_in_neighbors(node_id));
                Ok(neighbors)
            }
        }
    }

    /// Get degree of a node
    fn get_degree(&self, node_id: u32, direction: Direction) -> PyResult<usize> {
        match direction {
            Direction::Outgoing => Ok(self.snapshot.get_out_neighbors(node_id).len()),
            Direction::Incoming => Ok(self.snapshot.get_in_neighbors(node_id).len()),
            Direction::Both => {
                Ok(self.snapshot.get_out_neighbors(node_id).len() + 
                   self.snapshot.get_in_neighbors(node_id).len())
            }
        }
    }

    /// Get relationships of a node (returns neighbor node IDs, not relationship IDs)
    /// Note: GraphSnapshot doesn't track relationship IDs, only node-to-node connections
    fn get_relationships(&self, node_id: u32, direction: Direction) -> PyResult<Vec<u32>> {
        // GraphSnapshot doesn't have relationship IDs, so we return neighbor nodes
        // This matches the Graph API behavior where relationships are accessed via neighbors
        // Use get_rels instead of deprecated get_neighbors
        let nodes = self.get_rels(node_id, direction, None)?;
        Ok(nodes.into_iter().map(|n| n.node_id).collect())
    }

    /// Get relationships by type
    /// Returns Relationship objects for all relationships of the specified type
    fn get_relationships_by_type(&self, rel_type: String) -> PyResult<Vec<Relationship>> {
        let rel_type_id = self.rel_type_from_str(&rel_type)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Relationship type '{}' not found", rel_type)
            ))?;
        
        let mut relationships = Vec::new();
        
        // Iterate through all outgoing relationships
        for (idx, &rel_type_at_idx) in self.snapshot.out_types.iter().enumerate() {
            if rel_type_at_idx == rel_type_id {
                relationships.push(Relationship {
                    snapshot: self.snapshot.clone(),
                    rel_index: idx as u32,
                    is_outgoing: true,
                });
            }
        }
        
        // Note: We only return outgoing relationships to avoid duplicates
        // (each relationship appears once in out_types)
        Ok(relationships)
    }

    /// Get all nodes (returns all node IDs from 0 to n_nodes-1)
    fn get_all_nodes(&self) -> PyResult<Vec<u32>> {
        Ok((0..self.snapshot.n_nodes).collect())
    }

    /// Get all relationships as Relationship objects
    /// Returns all relationships in the graph
    fn get_all_relationships(&self) -> PyResult<Vec<Relationship>> {
        let mut relationships = Vec::with_capacity(self.snapshot.out_nbrs.len());
        
        // Iterate through all outgoing relationships (each relationship appears once)
        for idx in 0..self.snapshot.out_nbrs.len() {
            relationships.push(Relationship {
                snapshot: self.snapshot.clone(),
                rel_index: idx as u32,
                is_outgoing: true,
            });
        }
        
        Ok(relationships)
    }

    /// Get a relationship by index
    /// 
    /// Uses the outgoing relationship index (canonical index in out_nbrs).
    /// Each relationship appears once in out_nbrs, so this is a unique identifier.
    /// 
    /// # Arguments
    /// * `rel_index` - The relationship index in out_nbrs (0 to n_rels-1)
    fn get_relationship(&self, rel_index: u32) -> PyResult<Relationship> {
        let max_index = self.snapshot.out_nbrs.len() as u32;
        
        if rel_index >= max_index {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Relationship index {} out of range (max: {})", rel_index, max_index - 1)
            ));
        }
        
        Ok(Relationship {
            snapshot: self.snapshot.clone(),
            rel_index,
            is_outgoing: true, // Always true since we use out_nbrs as canonical
        })
    }

    /// Get a relationship by node pair
    /// 
    /// Finds a relationship between two nodes. If multiple relationships exist
    /// between the same nodes, returns the first one found.
    /// 
    /// # Arguments
    /// * `start_node` - Source node ID
    /// * `end_node` - Destination node ID
    fn get_relationship_by_nodes(&self, start_node: u32, end_node: u32) -> PyResult<Option<Relationship>> {
        // Check if start_node is valid
        if start_node as usize >= self.snapshot.out_offsets.len().saturating_sub(1) {
            return Ok(None);
        }
        
        let start = self.snapshot.out_offsets[start_node as usize] as usize;
        let end = self.snapshot.out_offsets[start_node as usize + 1] as usize;
        
        // Search for end_node in the outgoing neighbors of start_node
        for (idx, &nbr) in self.snapshot.out_nbrs[start..end].iter().enumerate() {
            if nbr == end_node {
                return Ok(Some(Relationship {
                    snapshot: self.snapshot.clone(),
                    rel_index: (start + idx) as u32,
                    is_outgoing: true,
                }));
            }
        }
        
        Ok(None)
    }

    /// Get property value for a node
    fn get_node_property(&self, node_id: u32, key: String) -> PyResult<Option<PyObject>> {
        let key_id = self.property_key_from_str(&key)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Property key '{}' not found", key)
            ))?;
        
        let value_id = self.snapshot.get_property(node_id, key_id);
        
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

    /// Get nodes with a specific property value
    fn get_nodes_with_property(&self, key: String, value: &PyAny) -> PyResult<Vec<u32>> {
        let key_id = self.property_key_from_str(&key)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Property key '{}' not found", key)
            ))?;
        
        let prop_value = py_to_property_value(value)?;
        let value_id = match prop_value {
            rustychickpeas_core::PropertyValue::String(s) => {
                // Need to find string ID
                let sid = self.get_string_id(&s)
                    .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!("Property value string '{}' not found", s)
                    ))?;
                ValueId::Str(sid)
            }
            rustychickpeas_core::PropertyValue::Integer(i) => ValueId::I64(i),
            rustychickpeas_core::PropertyValue::Float(f) => ValueId::from_f64(f),
            rustychickpeas_core::PropertyValue::Boolean(b) => ValueId::Bool(b),
            rustychickpeas_core::PropertyValue::InternedString(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "InternedString not supported in GraphSnapshot queries"
                ));
            }
        };
        
        // get_nodes_with_property now returns Option<NodeSet> (cloned) instead of Option<&NodeSet>
        if let Some(node_set) = self.snapshot.get_nodes_with_property(key_id, value_id) {
            Ok(node_set.iter().collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Get the version of this snapshot
    fn version(&self) -> PyResult<Option<String>> {
        Ok(self.snapshot.version().map(|s| s.to_string()))
    }

    /// Create a GraphSnapshot from Parquet files using GraphBuilder
    #[staticmethod]
    fn read_from_parquet(
        nodes_path: Option<String>,
        relationships_path: Option<String>,
        node_id_column: Option<String>,
        label_columns: Option<Vec<String>>,
        node_property_columns: Option<Vec<String>>,
        start_node_column: Option<String>,
        end_node_column: Option<String>,
        rel_type_column: Option<String>,
        rel_property_columns: Option<Vec<String>>,
    ) -> PyResult<GraphSnapshot> {
        let label_cols = label_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let node_prop_cols = node_property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let rel_prop_cols = rel_property_columns.as_ref().map(|cols| cols.iter().map(|s| s.as_str()).collect());
        
        let snapshot = CoreGraphSnapshot::from_parquet(
            nodes_path.as_deref(),
            relationships_path.as_deref(),
            node_id_column.as_deref(),
            label_cols,
            node_prop_cols,
            start_node_column.as_deref(),
            end_node_column.as_deref(),
            rel_type_column.as_deref(),
            rel_prop_cols,
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        
        Ok(GraphSnapshot::new(snapshot))
    }
}

