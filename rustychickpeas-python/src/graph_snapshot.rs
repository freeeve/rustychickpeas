//! GraphSnapshot Python wrapper

use std::sync::{Arc, Mutex};
use pyo3::prelude::*;
use rustychickpeas_core::{
    GraphSnapshot as CoreGraphSnapshot, Label, RelationshipType, ValueId,
};
use rustychickpeas_core::types::PropertyKey;
use rustychickpeas_core::bitmap::NodeSet;
use roaring::RoaringBitmap;
use crate::direction::Direction;
use crate::node::Node;
use crate::relationship::Relationship;
use crate::utils::{py_to_property_value, value_id_to_pyobject};

/// Python wrapper for GraphSnapshot
#[pyclass(name = "GraphSnapshot")]
pub struct GraphSnapshot {
    pub(crate) snapshot: std::sync::Arc<CoreGraphSnapshot>,
}

impl GraphSnapshot {
    /// Get string ID from string
    /// Uses the reverse index in Atoms for O(1) lookup
    fn get_string_id(&self, s: &str) -> Option<u32> {
        self.snapshot.atoms.get_id(s)
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

    fn __repr__(&self) -> String {
        let version = self.snapshot.version()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "None".to_string());
        format!(
            "GraphSnapshot(nodes={}, rels={}, version={})",
            self.snapshot.n_nodes, self.snapshot.n_rels, version
        )
    }

    fn __len__(&self) -> usize {
        self.snapshot.n_nodes as usize
    }

    /// Get number of nodes
    fn node_count(&self) -> u32 {
        self.snapshot.n_nodes
    }

    /// Get number of relationships
    fn relationship_count(&self) -> u64 {
        self.snapshot.n_rels
    }

    /// Get node labels
    fn node_labels(&self, node_id: u32) -> PyResult<Vec<String>> {
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
    fn nodes_with_label(&self, label: String) -> PyResult<Vec<u32>> {
        // Call Rust function - it returns None if label doesn't exist
        // We need to distinguish between "label doesn't exist" vs "label exists but no nodes"
        // Since nodes_with_label_id returns Option<&NodeSet>, None means label not in index
        let label_id = self.label_from_str(&label);
        if label_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Label '{}' not found", label)
            ));
        }
        
        // Label exists, now get nodes (will return Some even if empty)
        if let Some(node_set) = self.snapshot.nodes_with_label(&label) {
            Ok(node_set.iter().collect())
        } else {
            // This shouldn't happen if label exists, but handle it
            Ok(Vec::new())
        }
    }

    /// Get a Node object for the given node ID
    fn node(&self, node_id: u32) -> PyResult<Node> {
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
    fn relationships(&self, node_id: u32, direction: Direction, rel_types: Option<Vec<String>>) -> PyResult<Vec<Relationship>> {
        use rustychickpeas_core::types::RelationshipType;
        
        if node_id >= self.snapshot.n_nodes {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Node ID {} out of range (max: {})", node_id, self.snapshot.n_nodes - 1)
            ));
        }

        // Convert string types to RelationshipType IDs using O(1) reverse index
        let rel_type_ids: Option<Vec<RelationshipType>> = rel_types.as_ref().map(|types| {
            let ids: Vec<RelationshipType> = types.iter()
                .filter_map(|s| {
                    self.snapshot.atoms.get_id(s)
                        .map(RelationshipType::new)
                })
                .collect();
            if ids.is_empty() && !types.is_empty() {
                return None;
            }
            Some(ids)
        }).flatten();

        let mut relationships = Vec::new();

        // Handle outgoing relationships
        if matches!(direction, Direction::Outgoing | Direction::Both) {
            let start = self.snapshot.out_offsets[node_id as usize] as usize;
            let end = self.snapshot.out_offsets[node_id as usize + 1] as usize;
            
            for (idx, (&_neighbor, &rel_type)) in self.snapshot.out_nbrs[start..end]
                .iter()
                .zip(self.snapshot.out_types[start..end].iter())
                .enumerate()
            {
                let rel_csr_index = start + idx;
                
                // Apply type filter if provided
                if let Some(ref type_ids) = rel_type_ids {
                    if !type_ids.contains(&rel_type) {
                        continue;
                    }
                } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                    // Filter was provided but no types found - skip
                    continue;
                }
                
                relationships.push(Relationship {
                    snapshot: self.snapshot.clone(),
                    rel_index: rel_csr_index as u32,
                    is_outgoing: true,
                });
            }
        }

        // Handle incoming relationships
        if matches!(direction, Direction::Incoming | Direction::Both) {
            let start = self.snapshot.in_offsets[node_id as usize] as usize;
            let end = self.snapshot.in_offsets[node_id as usize + 1] as usize;
            
            for (idx, (&_neighbor, &rel_type)) in self.snapshot.in_nbrs[start..end]
                .iter()
                .zip(self.snapshot.in_types[start..end].iter())
                .enumerate()
            {
                let rel_csr_index = start + idx;
                
                // Apply type filter if provided
                if let Some(ref type_ids) = rel_type_ids {
                    if !type_ids.contains(&rel_type) {
                        continue;
                    }
                } else if rel_types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
                    // Filter was provided but no types found - skip
                    continue;
                }
                
                relationships.push(Relationship {
                    snapshot: self.snapshot.clone(),
                    rel_index: rel_csr_index as u32,
                    is_outgoing: false,
                });
            }
        }

        Ok(relationships)
    }

    /// Get neighbor node IDs of a node
    /// Returns a list of node IDs for neighbors in the specified direction
    fn neighbor_ids(&self, node_id: u32, direction: Direction) -> PyResult<Vec<u32>> {
        match direction {
            Direction::Outgoing => {
                Ok(self.snapshot.out_neighbors(node_id).to_vec())
            }
            Direction::Incoming => {
                Ok(self.snapshot.in_neighbors(node_id).to_vec())
            }
            Direction::Both => {
                let mut neighbors = Vec::new();
                neighbors.extend_from_slice(self.snapshot.out_neighbors(node_id));
                neighbors.extend_from_slice(self.snapshot.in_neighbors(node_id));
                Ok(neighbors)
            }
        }
    }

    /// Get neighbors of a node as Node objects
    /// Returns a list of Node objects for neighbors in the specified direction
    fn neighbors(&self, node_id: u32, direction: Direction) -> PyResult<Vec<Node>> {
        // Get relationships and extract neighbor node IDs
        let rels = self.relationships(node_id, direction, None)?;
        let mut neighbor_ids = Vec::new();
        
        for rel in rels {
            let neighbor_id = if rel.is_outgoing {
                // For outgoing relationships, the end node is in out_nbrs
                let idx = rel.rel_index as usize;
                if idx < self.snapshot.out_nbrs.len() {
                    self.snapshot.out_nbrs[idx]
                } else {
                    continue; // Skip invalid relationship
                }
            } else {
                // For incoming relationships, the start node (neighbor) is in in_nbrs
                let idx = rel.rel_index as usize;
                if idx < self.snapshot.in_nbrs.len() {
                    self.snapshot.in_nbrs[idx]
                } else {
                    continue; // Skip invalid relationship
                }
            };
            neighbor_ids.push(neighbor_id);
        }
        
        Ok(neighbor_ids.into_iter()
            .map(|id| Node {
                snapshot: self.snapshot.clone(),
                node_id: id,
            })
            .collect())
    }

    /// Get degree of a node
    fn degree(&self, node_id: u32, direction: Direction) -> PyResult<usize> {
        match direction {
            Direction::Outgoing => Ok(self.snapshot.out_neighbors(node_id).len()),
            Direction::Incoming => Ok(self.snapshot.in_neighbors(node_id).len()),
            Direction::Both => {
                Ok(self.snapshot.out_neighbors(node_id).len() + 
                   self.snapshot.in_neighbors(node_id).len())
            }
        }
    }


    /// Get relationships by type using the type_index bitmap for O(1) lookup
    /// Returns Relationship objects for all relationships of the specified type
    fn relationships_by_type(&self, rel_type: String) -> PyResult<Vec<Relationship>> {
        let rel_type_id = self.rel_type_from_str(&rel_type)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Relationship type '{}' not found", rel_type)
            ))?;

        if let Some(bitmap) = self.snapshot.type_index.get(&rel_type_id) {
            let relationships: Vec<Relationship> = bitmap.iter()
                .map(|idx| Relationship {
                    snapshot: self.snapshot.clone(),
                    rel_index: idx,
                    is_outgoing: true,
                })
                .collect();
            Ok(relationships)
        } else {
            Ok(Vec::new())
        }
    }

    /// Get all nodes (returns all node IDs that have data: labels, edges, or properties)
    fn all_nodes(&self) -> PyResult<Vec<u32>> {
        use std::collections::HashSet;
        let mut nodes = HashSet::new();
        
        // Add nodes with labels
        for (_label, node_set) in &self.snapshot.label_index {
            for node_id in node_set.iter() {
                nodes.insert(node_id);
            }
        }
        
        // Add nodes with edges (check CSR arrays)
        // Nodes with outgoing edges
        for node_id in 0..self.snapshot.out_offsets.len().saturating_sub(1) {
            let start = self.snapshot.out_offsets[node_id] as usize;
            let end = self.snapshot.out_offsets[node_id + 1] as usize;
            if start < end {
                nodes.insert(node_id as u32);
            }
        }
        
        // Nodes with incoming edges
        for node_id in 0..self.snapshot.in_offsets.len().saturating_sub(1) {
            let start = self.snapshot.in_offsets[node_id] as usize;
            let end = self.snapshot.in_offsets[node_id + 1] as usize;
            if start < end {
                nodes.insert(node_id as u32);
            }
        }
        
        // Add nodes with properties
        for column in self.snapshot.columns.values() {
            match column {
                rustychickpeas_core::Column::DenseI64(_) | rustychickpeas_core::Column::DenseF64(_) | 
                rustychickpeas_core::Column::DenseBool(_) | rustychickpeas_core::Column::DenseStr(_) => {
                    // Dense columns: all nodes from 0 to n_nodes-1 have this property
                    // But we only want nodes that actually have data, so skip dense columns
                    // (they're dense because most nodes have the property, but we can't tell which ones)
                }
                rustychickpeas_core::Column::SparseI64(pairs) => {
                    for (node_id, _) in pairs {
                        nodes.insert(*node_id);
                    }
                }
                rustychickpeas_core::Column::SparseF64(pairs) => {
                    for (node_id, _) in pairs {
                        nodes.insert(*node_id);
                    }
                }
                rustychickpeas_core::Column::SparseBool(pairs) => {
                    for (node_id, _) in pairs {
                        nodes.insert(*node_id);
                    }
                }
                rustychickpeas_core::Column::SparseStr(pairs) => {
                    for (node_id, _) in pairs {
                        nodes.insert(*node_id);
                    }
                }
            }
        }
        
        let mut result: Vec<u32> = nodes.into_iter().collect();
        result.sort_unstable();
        Ok(result)
    }

    /// Get all relationships as Relationship objects
    /// Returns all relationships in the graph
    fn all_relationships(&self) -> PyResult<Vec<Relationship>> {
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
    fn relationship(&self, rel_index: u32) -> PyResult<Relationship> {
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
    fn relationship_by_nodes(&self, start_node: u32, end_node: u32) -> PyResult<Option<Relationship>> {
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
    fn node_property(&self, node_id: u32, key: String) -> PyResult<Option<PyObject>> {
        let key_id = self.property_key_from_str(&key);
        if key_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Property key '{}' not found", key)
            ));
        }

        let value_id = self.snapshot.prop(node_id, &key);

        Python::with_gil(|py| {
            Ok(value_id.and_then(|vid| value_id_to_pyobject(py, vid, &self.snapshot.atoms)))
        })
    }

    /// Get nodes with a specific property value, scoped by label
    /// 
    /// # Arguments
    /// * `label` - The label to scope the query to
    /// * `key` - The property key
    /// * `value` - The property value to search for
    #[pyo3(signature = (label, key, value))]
    fn nodes_with_property(&self, label: String, key: String, value: &PyAny) -> PyResult<Vec<u32>> {
        // Check if label exists - need to do this before calling nodes_with_property()
        // because it returns None for both "label doesn't exist" and "no matches"
        let label_id = self.label_from_str(&label);
        if label_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Label '{}' not found", label)
            ));
        }
        
        // Check if property key exists
        let key_id = self.property_key_from_str(&key);
        if key_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Property key '{}' not found", key)
            ));
        }
        
        // Both label and key exist, now convert value and query
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
        // None means no matches (valid), Some means matches found
        if let Some(node_set) = self.snapshot.nodes_with_property(&label, &key, value_id) {
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

    /// Bidirectional BFS to find paths between source and target node sets
    /// 
    /// Performs BFS from both source and target nodes simultaneously, meeting in the middle.
    /// Returns the intersection of nodes and relationships that lie on paths between the sets.
    /// 
    /// # Arguments
    /// * `source_nodes` - List of starting node IDs for forward traversal
    /// * `target_nodes` - List of starting node IDs for backward traversal
    /// * `direction` - Direction of traversal (Direction.Outgoing, Direction.Incoming, or Direction.Both)
    ///   - Outgoing: Forward search uses outgoing edges, backward uses incoming (default for finding paths from source to target)
    ///   - Incoming: Forward search uses incoming edges, backward uses outgoing (reverse direction)
    ///   - Both: Both searches use both directions (bidirectional traversal)
    /// * `rel_types` - Optional list of relationship type names to filter by
    /// * `node_filter` - Optional callable that takes (node_id: int) and returns bool.
    ///   Returns True to include/continue from a node.
    /// * `rel_filter` - Optional callable that takes (from_node: int, to_node: int, rel_type: str, csr_pos: int) and returns bool.
    ///   Returns True to follow a relationship.
    /// * `max_depth` - Optional maximum depth for each direction (default: no limit)
    /// 
    /// # Returns
    /// A tuple `(node_ids, rel_csr_positions)` where:
    /// - `node_ids`: List of node IDs on paths between source and target
    /// - `rel_csr_positions`: List of relationship CSR positions on paths between source and target
    /// 
    /// # Examples
    /// ```python
    /// from rustychickpeas import Direction
    /// 
    /// # Simple bidirectional search (default: Outgoing)
    /// nodes, rels = snapshot.bidirectional_bfs([0, 1], [10, 11], Direction.Outgoing)
    /// 
    /// # With relationship type filter
    /// nodes, rels = snapshot.bidirectional_bfs(
    ///     [0, 1], [10, 11], 
    ///     Direction.Outgoing,
    ///     rel_types=["KNOWS", "WORKS_WITH"]
    /// )
    /// 
    /// # Bidirectional traversal (both directions)
    /// nodes, rels = snapshot.bidirectional_bfs(
    ///     [0, 1], [10, 11],
    ///     Direction.Both
    /// )
    /// 
    /// # With node filter (only "Person" nodes)
    /// def node_filter(node_id):
    ///     return "Person" in snapshot.node_labels(node_id)
    /// 
    /// nodes, rels = snapshot.bidirectional_bfs(
    ///     [0, 1], [10, 11],
    ///     Direction.Outgoing,
    ///     node_filter=node_filter
    /// )
    /// ```
    #[pyo3(signature = (source_nodes, target_nodes, direction, *, rel_types=None, node_filter=None, rel_filter=None, max_depth=None))]
    fn bidirectional_bfs(
        &self,
        source_nodes: Vec<u32>,
        target_nodes: Vec<u32>,
        direction: Direction,
        rel_types: Option<Vec<String>>,
        node_filter: Option<PyObject>,
        rel_filter: Option<PyObject>,
        max_depth: Option<u32>,
    ) -> PyResult<(Vec<u32>, Vec<u32>)> {
        let source_set = NodeSet::new(RoaringBitmap::from_iter(source_nodes.iter().copied()));
        let target_set = NodeSet::new(RoaringBitmap::from_iter(target_nodes.iter().copied()));

        use rustychickpeas_core::types::Direction as CoreDirection;
        let rust_direction = match direction {
            Direction::Outgoing => CoreDirection::Outgoing,
            Direction::Incoming => CoreDirection::Incoming,
            Direction::Both => CoreDirection::Both,
        };

        let rel_types_str: Option<Vec<&str>> = rel_types.as_ref().map(|types| {
            types.iter().map(|s| s.as_str()).collect()
        });

        // Error cell to capture Python exceptions from filter callbacks
        let error_cell: Arc<Mutex<Option<PyErr>>> = Arc::new(Mutex::new(None));

        let (node_bitmap, rel_bitmap) = if let (Some(nf_obj), Some(rf_obj)) = (node_filter.as_ref(), rel_filter.as_ref()) {
            let nf_obj = nf_obj.clone();
            let rf_obj = rf_obj.clone();
            let nf_err = error_cell.clone();
            let rf_err = error_cell.clone();
            self.snapshot.bidirectional_bfs(
                &source_set,
                &target_set,
                rust_direction,
                rel_types_str.as_deref(),
                Some(move |node_id: u32, _snapshot: &CoreGraphSnapshot| -> bool {
                    if nf_err.lock().unwrap().is_some() { return false; }
                    Python::with_gil(|py| {
                        match nf_obj.call1(py, (node_id,)).and_then(|r| r.extract::<bool>(py)) {
                            Ok(v) => v,
                            Err(e) => { *nf_err.lock().unwrap() = Some(e); false }
                        }
                    })
                }),
                Some(move |from: u32, to: u32, rel_type: RelationshipType, csr_pos: u32, snapshot: &CoreGraphSnapshot| -> bool {
                    if rf_err.lock().unwrap().is_some() { return false; }
                    let rf_obj = rf_obj.clone();
                    Python::with_gil(|py| {
                        let rel_type_str = snapshot.resolve_string(rel_type.id())
                            .unwrap_or("").to_string();
                        match rf_obj.call1(py, (from, to, rel_type_str, csr_pos)).and_then(|r| r.extract::<bool>(py)) {
                            Ok(v) => v,
                            Err(e) => { *rf_err.lock().unwrap() = Some(e); false }
                        }
                    })
                }),
                max_depth,
            )
        } else if let Some(nf_obj) = node_filter.as_ref() {
            let nf_obj = nf_obj.clone();
            let nf_err = error_cell.clone();
            type RelFilter = fn(u32, u32, RelationshipType, u32, &CoreGraphSnapshot) -> bool;
            self.snapshot.bidirectional_bfs::<_, RelFilter>(
                &source_set,
                &target_set,
                rust_direction,
                rel_types_str.as_deref(),
                Some(move |node_id: u32, _snapshot: &CoreGraphSnapshot| -> bool {
                    if nf_err.lock().unwrap().is_some() { return false; }
                    Python::with_gil(|py| {
                        match nf_obj.call1(py, (node_id,)).and_then(|r| r.extract::<bool>(py)) {
                            Ok(v) => v,
                            Err(e) => { *nf_err.lock().unwrap() = Some(e); false }
                        }
                    })
                }),
                None,
                max_depth,
            )
        } else if let Some(rf_obj) = rel_filter.as_ref() {
            let rf_obj = rf_obj.clone();
            let rf_err = error_cell.clone();
            type NodeFilter = fn(u32, &CoreGraphSnapshot) -> bool;
            self.snapshot.bidirectional_bfs::<NodeFilter, _>(
                &source_set,
                &target_set,
                rust_direction,
                rel_types_str.as_deref(),
                None,
                Some(move |from: u32, to: u32, rel_type: RelationshipType, csr_pos: u32, snapshot: &CoreGraphSnapshot| -> bool {
                    if rf_err.lock().unwrap().is_some() { return false; }
                    let rf_obj = rf_obj.clone();
                    Python::with_gil(|py| {
                        let rel_type_str = snapshot.resolve_string(rel_type.id())
                            .unwrap_or("").to_string();
                        match rf_obj.call1(py, (from, to, rel_type_str, csr_pos)).and_then(|r| r.extract::<bool>(py)) {
                            Ok(v) => v,
                            Err(e) => { *rf_err.lock().unwrap() = Some(e); false }
                        }
                    })
                }),
                max_depth,
            )
        } else {
            type NodeFilter = fn(u32, &CoreGraphSnapshot) -> bool;
            type RelFilter = fn(u32, u32, RelationshipType, u32, &CoreGraphSnapshot) -> bool;
            self.snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
                &source_set,
                &target_set,
                rust_direction,
                rel_types_str.as_deref(),
                None,
                None,
                max_depth,
            )
        };

        // Propagate any Python exception captured during BFS
        if let Some(err) = error_cell.lock().unwrap().take() {
            return Err(err);
        }

        Ok((
            node_bitmap.iter().collect(),
            rel_bitmap.iter().collect(),
        ))
    }

    /// BFS traversal from a set of starting nodes
    /// 
    /// Performs BFS from the starting nodes, following edges in the specified direction.
    /// Returns all nodes and relationships visited during the traversal.
    /// 
    /// # Arguments
    /// * `start_nodes` - List of starting node IDs
    /// * `direction` - Direction of traversal (Direction.Outgoing, Direction.Incoming, or Direction.Both)
    ///   - Outgoing: Follow outgoing edges
    ///   - Incoming: Follow incoming edges
    ///   - Both: Follow both outgoing and incoming edges
    /// * `rel_types` - Optional list of relationship type names to filter by
    /// * `node_filter` - Optional callable that takes (node_id: int) and returns bool.
    ///   Returns True to include/continue from a node.
    /// * `rel_filter` - Optional callable that takes (from_node: int, to_node: int, rel_type: str, csr_pos: int) and returns bool.
    ///   Returns True to follow a relationship.
    /// * `max_depth` - Optional maximum depth (default: no limit)
    /// 
    /// # Returns
    /// A tuple `(node_ids, rel_csr_positions)` where:
    /// - `node_ids`: List of node IDs visited during traversal
    /// - `rel_csr_positions`: List of relationship CSR positions traversed
    /// 
    /// # Examples
    /// ```python
    /// from rustychickpeas import Direction
    /// 
    /// # Simple BFS from a single node
    /// nodes, rels = snapshot.bfs([0], Direction.Outgoing)
    /// 
    /// # BFS with relationship type filter
    /// nodes, rels = snapshot.bfs(
    ///     [0], Direction.Outgoing, 
    ///     rel_types=["KNOWS", "WORKS_WITH"]
    /// )
    /// 
    /// # BFS with max depth
    /// nodes, rels = snapshot.bfs(
    ///     [0], Direction.Outgoing, 
    ///     max_depth=3
    /// )
    /// 
    /// # BFS with node filter (only "Person" nodes)
    /// def node_filter(node_id):
    ///     return "Person" in snapshot.node_labels(node_id)
    /// 
    /// nodes, rels = snapshot.bfs(
    ///     [0], Direction.Outgoing,
    ///     node_filter=node_filter
    /// )
    /// ```
    #[pyo3(signature = (start_nodes, direction, *, rel_types=None, node_filter=None, rel_filter=None, max_depth=None))]
    fn bfs(
        &self,
        start_nodes: Vec<u32>,
        direction: Direction,
        rel_types: Option<Vec<String>>,
        node_filter: Option<PyObject>,
        rel_filter: Option<PyObject>,
        max_depth: Option<u32>,
    ) -> PyResult<(Vec<u32>, Vec<u32>)> {
        let start_set = NodeSet::new(RoaringBitmap::from_iter(start_nodes.iter().copied()));

        use rustychickpeas_core::types::Direction as CoreDirection;
        let rust_direction = match direction {
            Direction::Outgoing => CoreDirection::Outgoing,
            Direction::Incoming => CoreDirection::Incoming,
            Direction::Both => CoreDirection::Both,
        };

        let rel_types_str: Option<Vec<&str>> = rel_types.as_ref().map(|types| {
            types.iter().map(|s| s.as_str()).collect()
        });

        // Error cell to capture Python exceptions from filter callbacks
        let error_cell: Arc<Mutex<Option<PyErr>>> = Arc::new(Mutex::new(None));

        let (node_bitmap, rel_bitmap) = if let (Some(nf_obj), Some(rf_obj)) = (node_filter.as_ref(), rel_filter.as_ref()) {
            let nf_obj = nf_obj.clone();
            let rf_obj = rf_obj.clone();
            let nf_err = error_cell.clone();
            let rf_err = error_cell.clone();
            self.snapshot.bfs(
                &start_set,
                rust_direction,
                rel_types_str.as_deref(),
                Some(move |node_id: u32, _snapshot: &CoreGraphSnapshot| -> bool {
                    if nf_err.lock().unwrap().is_some() { return false; }
                    Python::with_gil(|py| {
                        match nf_obj.call1(py, (node_id,)).and_then(|r| r.extract::<bool>(py)) {
                            Ok(v) => v,
                            Err(e) => { *nf_err.lock().unwrap() = Some(e); false }
                        }
                    })
                }),
                Some(move |from: u32, to: u32, rel_type: RelationshipType, csr_pos: u32, snapshot: &CoreGraphSnapshot| -> bool {
                    if rf_err.lock().unwrap().is_some() { return false; }
                    let rf_obj = rf_obj.clone();
                    Python::with_gil(|py| {
                        let rel_type_str = snapshot.resolve_string(rel_type.id())
                            .unwrap_or("").to_string();
                        match rf_obj.call1(py, (from, to, rel_type_str, csr_pos)).and_then(|r| r.extract::<bool>(py)) {
                            Ok(v) => v,
                            Err(e) => { *rf_err.lock().unwrap() = Some(e); false }
                        }
                    })
                }),
                max_depth,
            )
        } else if let Some(nf_obj) = node_filter.as_ref() {
            let nf_obj = nf_obj.clone();
            let nf_err = error_cell.clone();
            type RelFilter = fn(u32, u32, RelationshipType, u32, &CoreGraphSnapshot) -> bool;
            self.snapshot.bfs::<_, RelFilter>(
                &start_set,
                rust_direction,
                rel_types_str.as_deref(),
                Some(move |node_id: u32, _snapshot: &CoreGraphSnapshot| -> bool {
                    if nf_err.lock().unwrap().is_some() { return false; }
                    Python::with_gil(|py| {
                        match nf_obj.call1(py, (node_id,)).and_then(|r| r.extract::<bool>(py)) {
                            Ok(v) => v,
                            Err(e) => { *nf_err.lock().unwrap() = Some(e); false }
                        }
                    })
                }),
                None,
                max_depth,
            )
        } else if let Some(rf_obj) = rel_filter.as_ref() {
            let rf_obj = rf_obj.clone();
            let rf_err = error_cell.clone();
            type NodeFilter = fn(u32, &CoreGraphSnapshot) -> bool;
            self.snapshot.bfs::<NodeFilter, _>(
                &start_set,
                rust_direction,
                rel_types_str.as_deref(),
                None,
                Some(move |from: u32, to: u32, rel_type: RelationshipType, csr_pos: u32, snapshot: &CoreGraphSnapshot| -> bool {
                    if rf_err.lock().unwrap().is_some() { return false; }
                    let rf_obj = rf_obj.clone();
                    Python::with_gil(|py| {
                        let rel_type_str = snapshot.resolve_string(rel_type.id())
                            .unwrap_or("").to_string();
                        match rf_obj.call1(py, (from, to, rel_type_str, csr_pos)).and_then(|r| r.extract::<bool>(py)) {
                            Ok(v) => v,
                            Err(e) => { *rf_err.lock().unwrap() = Some(e); false }
                        }
                    })
                }),
                max_depth,
            )
        } else {
            type NodeFilter = fn(u32, &CoreGraphSnapshot) -> bool;
            type RelFilter = fn(u32, u32, RelationshipType, u32, &CoreGraphSnapshot) -> bool;
            self.snapshot.bfs::<NodeFilter, RelFilter>(
                &start_set,
                rust_direction,
                rel_types_str.as_deref(),
                None,
                None,
                max_depth,
            )
        };

        // Propagate any Python exception captured during BFS
        if let Some(err) = error_cell.lock().unwrap().take() {
            return Err(err);
        }

        Ok((
            node_bitmap.iter().collect(),
            rel_bitmap.iter().collect(),
        ))
    }

    /// Check if a path exists between two nodes
    #[pyo3(signature = (from_node, to_node, direction, *, rel_types=None, max_depth=None))]
    fn can_reach(
        &self,
        from_node: u32,
        to_node: u32,
        direction: Direction,
        rel_types: Option<Vec<String>>,
        max_depth: Option<usize>,
    ) -> PyResult<bool> {
        use rustychickpeas_core::types::Direction as CoreDirection;
        let rust_direction = match direction {
            Direction::Outgoing => CoreDirection::Outgoing,
            Direction::Incoming => CoreDirection::Incoming,
            Direction::Both => CoreDirection::Both,
        };

        let rel_types_str: Option<Vec<&str>> = rel_types.as_ref().map(|types| {
            types.iter().map(|s| s.as_str()).collect()
        });

        // can_reach takes Option<u32> for max_depth
        let max_depth_u32 = max_depth.map(|d| d as u32);

        Ok(self.snapshot.can_reach(
            from_node,
            to_node,
            rust_direction,
            rel_types_str.as_deref(),
            max_depth_u32,
        ))
    }
}

