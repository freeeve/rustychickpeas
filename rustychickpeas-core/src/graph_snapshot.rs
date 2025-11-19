//! Immutable graph snapshot optimized for read-only queries
//!
//! GraphSnapshot uses CSR (Compressed Sparse Row) format for adjacency
//! and columnar storage for properties, providing maximum query performance.

use crate::bitmap::NodeSet;
use crate::types::{Label, NodeId, PropertyKey, RelationshipType};
use hashbrown::HashMap;
use roaring::RoaringBitmap;
use std::sync::Mutex;

/// Interned value ID for property indexes
/// All strings are interned for fast equality/hash operations
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValueId {
    /// Interned string ID
    Str(u32),
    /// Integer value
    I64(i64),
    /// Float value (bitcast to u64 for total ordering)
    F64(u64),
    /// Boolean value
    Bool(bool),
}

impl ValueId {
    /// Convert f64 to ValueId (bitcast for ordering)
    pub fn from_f64(val: f64) -> Self {
        ValueId::F64(val.to_bits())
    }

    /// Convert ValueId back to f64
    pub fn to_f64(self) -> Option<f64> {
        match self {
            ValueId::F64(bits) => Some(f64::from_bits(bits)),
            _ => None,
        }
    }
}

/// Columnar property storage
/// Dense columns use direct Vec access (O(1)), sparse columns use sorted Vec (O(log n))
#[derive(Debug, Clone)]
pub enum Column {
    /// Dense i64 column (Vec[node_id] = value)
    DenseI64(Vec<i64>),
    /// Dense f64 column
    DenseF64(Vec<f64>),
    /// Dense boolean column (bitvec for compact storage)
    DenseBool(bitvec::vec::BitVec),
    /// Dense string column (interned string IDs)
    DenseStr(Vec<u32>),
    /// Sparse i64 column (sorted by NodeId)
    SparseI64(Vec<(NodeId, i64)>),
    /// Sparse f64 column
    SparseF64(Vec<(NodeId, f64)>),
    /// Sparse boolean column
    SparseBool(Vec<(NodeId, bool)>),
    /// Sparse string column (interned string IDs)
    SparseStr(Vec<(NodeId, u32)>),
}

impl Column {
    /// Get property value for a node (if dense) or None
    pub fn get_dense(&self, node_id: NodeId) -> Option<ValueId> {
        match self {
            Column::DenseI64(col) => col.get(node_id as usize).map(|&v| ValueId::I64(v)),
            Column::DenseF64(col) => col.get(node_id as usize).map(|&v| ValueId::from_f64(v)),
            Column::DenseBool(col) => col.get(node_id as usize).map(|b| ValueId::Bool(*b)),
            Column::DenseStr(col) => col.get(node_id as usize).map(|&v| ValueId::Str(v)),
            _ => None,
        }
    }

    /// Get property value for a node (sparse lookup)
    pub fn get_sparse(&self, node_id: NodeId) -> Option<ValueId> {
        match self {
            Column::SparseI64(col) => col
                .binary_search_by_key(&node_id, |(id, _)| *id)
                .ok()
                .map(|idx| ValueId::I64(col[idx].1)),
            Column::SparseF64(col) => col
                .binary_search_by_key(&node_id, |(id, _)| *id)
                .ok()
                .map(|idx| ValueId::from_f64(col[idx].1)),
            Column::SparseBool(col) => col
                .binary_search_by_key(&node_id, |(id, _)| *id)
                .ok()
                .map(|idx| ValueId::Bool(col[idx].1)),
            Column::SparseStr(col) => col
                .binary_search_by_key(&node_id, |(id, _)| *id)
                .ok()
                .map(|idx| ValueId::Str(col[idx].1)),
            _ => None,
        }
    }

    /// Get property value for a node (tries dense first, then sparse)
    pub fn get(&self, node_id: NodeId) -> Option<ValueId> {
        self.get_dense(node_id).or_else(|| self.get_sparse(node_id))
    }
}

/// Flattened string table (interner flushed to Vec)
#[derive(Debug, Clone)]
pub struct Atoms {
    /// String table: id -> string (id 0 is "" by convention)
    pub strings: Vec<String>,
    /// Reverse index: string -> id (for fast lookups)
    /// Built on construction to avoid repeated linear searches
    reverse_index: HashMap<String, u32>,
}

impl Atoms {
    pub fn new(strings: Vec<String>) -> Self {
        // Build reverse index for O(1) string lookups
        let mut reverse_index = HashMap::with_capacity(strings.len());
        for (id, s) in strings.iter().enumerate() {
            reverse_index.insert(s.clone(), id as u32);
        }
        Atoms { 
            strings,
            reverse_index,
        }
    }

    /// Resolve an interned string ID to a string
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(|s| s.as_str())
    }

    /// Get the ID for a string (O(1) lookup using reverse index)
    pub fn get_id(&self, s: &str) -> Option<u32> {
        self.reverse_index.get(s).copied()
    }
}

/// Immutable graph snapshot optimized for read-only queries
#[derive(Debug)]
pub struct GraphSnapshot {
    // --- Core shape (CSR) ---
    /// Number of nodes
    pub n_nodes: u32,
    /// Number of relationships
    pub n_rels: u64,

    // CSR (outgoing relationships)
    /// Outgoing offsets: len = n_nodes + 1
    /// out_offsets[i] to out_offsets[i+1] gives the range in out_nbrs for node i
    pub out_offsets: Vec<u32>,
    /// Outgoing neighbors: len = n_rels
    /// Contains destination node IDs
    pub out_nbrs: Vec<NodeId>,
    /// Outgoing relationship types: len = n_rels
    /// Parallel to out_nbrs, contains relationship type for each edge
    pub out_types: Vec<RelationshipType>,
    // CSR (incoming relationships) - optional
    /// Incoming offsets: len = n_nodes + 1
    pub in_offsets: Vec<u32>,
    /// Incoming neighbors: len = n_rels
    /// Contains source node IDs
    pub in_nbrs: Vec<NodeId>,
    /// Incoming relationship types: len = n_rels
    /// Parallel to in_nbrs, contains relationship type for each edge
    pub in_types: Vec<RelationshipType>,

    // --- Label/type indexes (value -> nodeset) ---
    /// Label index: label -> nodes with that label
    pub label_index: HashMap<Label, NodeSet>,
    /// Relationship type index: type -> relationships with that type
    pub type_index: HashMap<RelationshipType, NodeSet>,
    
    // --- Version tracking ---
    /// Version identifier for this snapshot (e.g., "v0.1", "v1.0")
    pub version: Option<String>,

    // --- Properties ---
    /// Column registry: property key -> column storage (for nodes)
    pub columns: HashMap<PropertyKey, Column>,
    /// Column registry: property key -> column storage (for relationships)
    /// Relationships are indexed by their position in the outgoing CSR array
    pub rel_columns: HashMap<PropertyKey, Column>,

    // --- Inverted property index (lazy-initialized) ---
    /// Lazy-initialized inverted index: (label, property_key) -> (value_id -> nodes with that property value)
    /// Indexes are built on first access to avoid memory overhead for unused properties
    /// Indexes are scoped by label to allow the same property key to be indexed separately per label
    pub prop_index: Mutex<HashMap<(Label, PropertyKey), HashMap<ValueId, NodeSet>>>,

    // --- String tables ---
    /// Flattened string interner
    pub atoms: Atoms,
}

/// Trait for converting common types to ValueId
/// For strings, requires access to the snapshot's atoms for lookup
pub trait IntoValueId {
    fn into_value_id(self, snapshot: &GraphSnapshot) -> Option<ValueId>;
}

impl IntoValueId for ValueId {
    fn into_value_id(self, _snapshot: &GraphSnapshot) -> Option<ValueId> {
        Some(self)
    }
}

impl IntoValueId for i64 {
    fn into_value_id(self, _snapshot: &GraphSnapshot) -> Option<ValueId> {
        Some(ValueId::I64(self))
    }
}

impl IntoValueId for i32 {
    fn into_value_id(self, _snapshot: &GraphSnapshot) -> Option<ValueId> {
        Some(ValueId::I64(self as i64))
    }
}

impl IntoValueId for f64 {
    fn into_value_id(self, _snapshot: &GraphSnapshot) -> Option<ValueId> {
        Some(ValueId::from_f64(self))
    }
}

impl IntoValueId for bool {
    fn into_value_id(self, _snapshot: &GraphSnapshot) -> Option<ValueId> {
        Some(ValueId::Bool(self))
    }
}

impl IntoValueId for &str {
    fn into_value_id(self, snapshot: &GraphSnapshot) -> Option<ValueId> {
        snapshot.value_id_from_str(self)
    }
}

impl IntoValueId for String {
    fn into_value_id(self, snapshot: &GraphSnapshot) -> Option<ValueId> {
        snapshot.value_id_from_str(&self)
    }
}

impl GraphSnapshot {
    /// Build the property index for a specific key by scanning the column
    /// This is used in tests and as a convenience wrapper
    #[cfg(test)]
    fn build_property_index_for_key(column: &Column) -> HashMap<ValueId, NodeSet> {
        Self::build_property_index_for_key_and_label(column, None)
    }
    
    /// Build the property index for a specific key and label by scanning the column
    /// If label_nodes is Some, only nodes in that set are included in the index
    /// If label_nodes is None, all nodes are included (backward compatibility)
    fn build_property_index_for_key_and_label(column: &Column, label_nodes: Option<&NodeSet>) -> HashMap<ValueId, NodeSet> {
        use roaring::RoaringBitmap;
        let mut key_index: HashMap<ValueId, Vec<NodeId>> = HashMap::new();
        
        // Helper to check if a node should be included
        let should_include = |node_id: NodeId| -> bool {
            label_nodes.map_or(true, |nodes| nodes.contains(node_id))
        };
        
        // Scan the column and group nodes by value
        match column {
            Column::DenseI64(col) => {
                for (node_id, &val) in col.iter().enumerate() {
                    let node_id = node_id as u32;
                    if should_include(node_id) {
                        key_index.entry(ValueId::I64(val)).or_default().push(node_id);
                    }
                }
            }
            Column::DenseF64(col) => {
                for (node_id, &val) in col.iter().enumerate() {
                    let node_id = node_id as u32;
                    if should_include(node_id) {
                        key_index.entry(ValueId::from_f64(val)).or_default().push(node_id);
                    }
                }
            }
            Column::DenseBool(col) => {
                for (node_id, val) in col.iter().enumerate() {
                    let node_id = node_id as u32;
                    if should_include(node_id) {
                        key_index.entry(ValueId::Bool(*val)).or_default().push(node_id);
                    }
                }
            }
            Column::DenseStr(col) => {
                for (node_id, &str_id) in col.iter().enumerate() {
                    let node_id = node_id as u32;
                    if should_include(node_id) {
                        key_index.entry(ValueId::Str(str_id)).or_default().push(node_id);
                    }
                }
            }
            Column::SparseI64(col) => {
                for &(node_id, val) in col.iter() {
                    if should_include(node_id) {
                        key_index.entry(ValueId::I64(val)).or_default().push(node_id);
                    }
                }
            }
            Column::SparseF64(col) => {
                for &(node_id, val) in col.iter() {
                    if should_include(node_id) {
                        key_index.entry(ValueId::from_f64(val)).or_default().push(node_id);
                    }
                }
            }
            Column::SparseBool(col) => {
                for &(node_id, val) in col.iter() {
                    if should_include(node_id) {
                        key_index.entry(ValueId::Bool(val)).or_default().push(node_id);
                    }
                }
            }
            Column::SparseStr(col) => {
                for &(node_id, str_id) in col.iter() {
                    if should_include(node_id) {
                        key_index.entry(ValueId::Str(str_id)).or_default().push(node_id);
                    }
                }
            }
        }
        
        // Convert Vec<NodeId> to NodeSet (RoaringBitmap)
        let mut key_index_final: HashMap<ValueId, NodeSet> = HashMap::new();
        for (val_id, mut node_ids) in key_index {
            node_ids.sort_unstable();
            node_ids.dedup();
            let bitmap = RoaringBitmap::from_sorted_iter(node_ids.into_iter()).unwrap();
            key_index_final.insert(val_id, NodeSet::new(bitmap));
        }
        
        key_index_final
    }

    /// Create a new empty snapshot
    pub fn new() -> Self {
        GraphSnapshot {
            n_nodes: 0,
            n_rels: 0,
            out_offsets: vec![0],
            out_nbrs: Vec::new(),
            out_types: Vec::new(),
            in_offsets: vec![0],
            in_nbrs: Vec::new(),
            in_types: Vec::new(),
            label_index: HashMap::new(),
            type_index: HashMap::new(),
            version: None,
            columns: HashMap::new(),
            rel_columns: HashMap::new(),
            prop_index: Mutex::new(HashMap::new()),
            atoms: Atoms::new(vec!["".to_string()]),
        }
    }

    /// Get outgoing neighbors of a node (CSR format)
    pub fn out_neighbors(&self, node_id: NodeId) -> &[NodeId] {
        if node_id as usize >= self.out_offsets.len().saturating_sub(1) {
            return &[];
        }
        let start = self.out_offsets[node_id as usize] as usize;
        let end = self.out_offsets[node_id as usize + 1] as usize;
        &self.out_nbrs[start..end]
    }

    /// Get incoming neighbors of a node (CSR format)
    pub fn in_neighbors(&self, node_id: NodeId) -> &[NodeId] {
        if node_id as usize >= self.in_offsets.len().saturating_sub(1) {
            return &[];
        }
        let start = self.in_offsets[node_id as usize] as usize;
        let end = self.in_offsets[node_id as usize + 1] as usize;
        &self.in_nbrs[start..end]
    }

    /// Get outgoing neighbors filtered by relationship types
    /// Returns only neighbors connected via relationships of the specified types
    /// 
    /// # Arguments
    /// * `node_id` - The node ID
    /// * `rel_types` - Relationship type names (e.g., `&["KNOWS", "WORKS_WITH"]`)
    /// 
    /// # Examples
    /// ```
    /// // Get neighbors connected via "KNOWS" relationships
    /// let neighbors = snapshot.out_neighbors_by_type(0, &["KNOWS"]);
    /// 
    /// // Get neighbors connected via multiple relationship types
    /// let neighbors = snapshot.out_neighbors_by_type(0, &["KNOWS", "WORKS_WITH"]);
    /// ```
    pub fn out_neighbors_by_type(&self, node_id: NodeId, rel_types: &[&str]) -> Vec<NodeId> {
        let rel_type_ids: Vec<RelationshipType> = rel_types.iter()
            .filter_map(|s| self.relationship_type_from_str(s))
            .collect();
        self.out_neighbors_by_type_id(node_id, &rel_type_ids)
    }

    /// Get outgoing neighbors filtered by relationship types (internal ID-based version)
    fn out_neighbors_by_type_id(&self, node_id: NodeId, rel_types: &[RelationshipType]) -> Vec<NodeId> {
        if node_id as usize >= self.out_offsets.len().saturating_sub(1) {
            return Vec::new();
        }
        let start = self.out_offsets[node_id as usize] as usize;
        let end = self.out_offsets[node_id as usize + 1] as usize;
        
        if rel_types.is_empty() {
            // No filter, return all
            return self.out_nbrs[start..end].to_vec();
        }
        
        // Build a set of allowed types for fast lookup
        let allowed_types: std::collections::HashSet<RelationshipType> = rel_types.iter().copied().collect();
        
        self.out_nbrs[start..end]
            .iter()
            .zip(self.out_types[start..end].iter())
            .filter_map(|(&nbr, &rel_type)| {
                if allowed_types.contains(&rel_type) {
                    Some(nbr)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get incoming neighbors filtered by relationship types
    /// Returns only neighbors connected via relationships of the specified types
    /// 
    /// # Arguments
    /// * `node_id` - The node ID
    /// * `rel_types` - Relationship type names (e.g., `&["KNOWS", "WORKS_WITH"]`)
    /// 
    /// # Examples
    /// ```
    /// // Get neighbors connected via "KNOWS" relationships
    /// let neighbors = snapshot.in_neighbors_by_type(0, &["KNOWS"]);
    /// 
    /// // Get neighbors connected via multiple relationship types
    /// let neighbors = snapshot.in_neighbors_by_type(0, &["KNOWS", "WORKS_WITH"]);
    /// ```
    pub fn in_neighbors_by_type(&self, node_id: NodeId, rel_types: &[&str]) -> Vec<NodeId> {
        let rel_type_ids: Vec<RelationshipType> = rel_types.iter()
            .filter_map(|s| self.relationship_type_from_str(s))
            .collect();
        self.in_neighbors_by_type_id(node_id, &rel_type_ids)
    }

    /// Get incoming neighbors filtered by relationship types (internal ID-based version)
    fn in_neighbors_by_type_id(&self, node_id: NodeId, rel_types: &[RelationshipType]) -> Vec<NodeId> {
        if node_id as usize >= self.in_offsets.len().saturating_sub(1) {
            return Vec::new();
        }
        let start = self.in_offsets[node_id as usize] as usize;
        let end = self.in_offsets[node_id as usize + 1] as usize;
        
        if rel_types.is_empty() {
            // No filter, return all
            return self.in_nbrs[start..end].to_vec();
        }
        
        // Build a set of allowed types for fast lookup
        let allowed_types: std::collections::HashSet<RelationshipType> = rel_types.iter().copied().collect();
        
        self.in_nbrs[start..end]
            .iter()
            .zip(self.in_types[start..end].iter())
            .filter_map(|(&nbr, &rel_type)| {
                if allowed_types.contains(&rel_type) {
                    Some(nbr)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if one node can reach another via traversal
    /// 
    /// Uses breadth-first search (BFS) to efficiently determine reachability.
    /// 
    /// # Arguments
    /// * `from` - Starting node ID
    /// * `to` - Target node ID
    /// * `direction` - Direction of traversal (Outgoing, Incoming, or Both)
    /// * `rel_types` - Optional filter: only follow relationships of these types.
    ///   If `None` or empty, all relationship types are allowed.
    /// * `max_depth` - Optional maximum traversal depth. If `None`, no limit.
    /// 
    /// # Returns
    /// `true` if `to` is reachable from `from` under the given constraints, `false` otherwise.
    /// 
    /// # Examples
    /// ```
    /// // Check if node 5 can reach node 10 via outgoing relationships
    /// let reachable = snapshot.can_reach(5, 10, Direction::Outgoing, None, None);
    /// 
    /// // Check reachability only via "KNOWS" and "WORKS_WITH" relationships, max 3 hops
    /// let reachable = snapshot.can_reach(
    ///     5, 10, 
    ///     Direction::Outgoing, 
    ///     Some(&["KNOWS", "WORKS_WITH"]), 
    ///     Some(3)
    /// );
    /// 
    /// // Check bidirectional reachability with no type filter
    /// let reachable = snapshot.can_reach(5, 10, Direction::Both, None, None);
    /// ```
    pub fn can_reach(
        &self,
        from: NodeId,
        to: NodeId,
        direction: crate::types::Direction,
        rel_types: Option<&[&str]>,
        max_depth: Option<u32>,
    ) -> bool {
        // Early exit: same node
        if from == to {
            return true;
        }

        // Convert relationship type strings to IDs if provided
        let rel_type_ids: Option<Vec<RelationshipType>> = rel_types.and_then(|types| {
            let ids: Vec<RelationshipType> = types
                .iter()
                .filter_map(|s| self.relationship_type_from_str(s))
                .collect();
            if ids.is_empty() {
                None
            } else {
                Some(ids)
            }
        });

        // BFS: queue of (node, depth)
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((from, 0u32));
        
        // Track visited nodes to avoid cycles (using RoaringBitmap for efficiency)
        let mut visited = RoaringBitmap::new();
        visited.insert(from);

        while let Some((current, depth)) = queue.pop_front() {
            // Check depth limit
            if let Some(max) = max_depth {
                if depth >= max {
                    continue;
                }
            }

            // Get neighbors based on direction and relationship type filter
            let neighbors = match direction {
                crate::types::Direction::Outgoing => {
                    if let Some(ref type_ids) = rel_type_ids {
                        self.out_neighbors_by_type_id(current, type_ids)
                    } else {
                        self.out_neighbors(current).to_vec()
                    }
                }
                crate::types::Direction::Incoming => {
                    if let Some(ref type_ids) = rel_type_ids {
                        self.in_neighbors_by_type_id(current, type_ids)
                    } else {
                        self.in_neighbors(current).to_vec()
                    }
                }
                crate::types::Direction::Both => {
                    let mut both = Vec::new();
                    if let Some(ref type_ids) = rel_type_ids {
                        both.extend(self.out_neighbors_by_type_id(current, type_ids));
                        both.extend(self.in_neighbors_by_type_id(current, type_ids));
                    } else {
                        both.extend(self.out_neighbors(current));
                        both.extend(self.in_neighbors(current));
                    }
                    both
                }
            };

            // Check each neighbor
            for neighbor in neighbors {
                if neighbor == to {
                    return true; // Found target!
                }

                // Add to queue if not visited
                if visited.insert(neighbor) {
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        false // Target not reachable
    }

    /// Bidirectional BFS to find paths between source and target node sets
    /// 
    /// Performs BFS from both source and target nodes simultaneously, meeting in the middle.
    /// Returns the intersection of nodes and relationships that lie on paths between the sets.
    /// 
    /// # Arguments
    /// * `source_nodes` - Starting nodes for forward traversal
    /// * `target_nodes` - Starting nodes for backward traversal
    /// * `direction` - Direction of traversal:
    ///   - `Outgoing`: Forward search uses outgoing edges, backward search uses incoming edges (default for finding paths from source to target)
    ///   - `Incoming`: Forward search uses incoming edges, backward search uses outgoing edges (reverse direction)
    ///   - `Both`: Both searches use both directions (bidirectional traversal)
    /// * `rel_types` - Optional filter: only follow relationships of these types
    /// * `node_filter` - Optional filter: returns `true` to include/continue from a node.
    ///   Takes `(node_id, snapshot)` and should return `true` to include the node.
    /// * `rel_filter` - Optional filter: returns `true` to follow a relationship.
    ///   Takes `(from_node, to_node, rel_type, csr_position, snapshot)` and should return `true` to follow.
    /// * `max_depth` - Optional maximum depth for each direction (default: no limit)
    /// 
    /// # Returns
    /// A tuple `(node_bitmap, rel_bitmap)` where:
    /// - `node_bitmap`: RoaringBitmap of node IDs on paths between source and target
    /// - `rel_bitmap`: RoaringBitmap of relationship CSR positions on paths between source and target
    /// 
    /// # Examples
    /// ```
    /// use rustychickpeas_core::bitmap::NodeSet;
    /// use rustychickpeas_core::types::Direction;
    /// use roaring::RoaringBitmap;
    /// 
    /// // Simple bidirectional search (default: Outgoing for forward, Incoming for backward)
    /// let source = NodeSet::new(RoaringBitmap::from_iter([0, 1]));
    /// let target = NodeSet::new(RoaringBitmap::from_iter([10, 11]));
    /// let (nodes, rels) = snapshot.bidirectional_bfs(
    ///     &source, &target, Direction::Outgoing, None, None, None, None
    /// );
    /// 
    /// // With relationship type filter
    /// let (nodes, rels) = snapshot.bidirectional_bfs(
    ///     &source, &target, Direction::Outgoing, Some(&["KNOWS", "WORKS_WITH"]), None, None, None
    /// );
    /// 
    /// // Bidirectional traversal (both directions)
    /// let (nodes, rels) = snapshot.bidirectional_bfs(
    ///     &source, &target, Direction::Both, None, None, None, None
    /// );
    /// ```
    pub fn bidirectional_bfs<NF, RF>(
        &self,
        source_nodes: &NodeSet,
        target_nodes: &NodeSet,
        direction: crate::types::Direction,
        rel_types: Option<&[&str]>,
        node_filter: Option<NF>,
        rel_filter: Option<RF>,
        max_depth: Option<u32>,
    ) -> (RoaringBitmap, RoaringBitmap)
    where
        NF: Fn(NodeId, &GraphSnapshot) -> bool,
        RF: Fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool,
    {
        // Convert relationship type strings to IDs if provided
        let rel_type_ids: Option<Vec<RelationshipType>> = rel_types.and_then(|types| {
            let ids: Vec<RelationshipType> = types
                .iter()
                .filter_map(|s| self.relationship_type_from_str(s))
                .collect();
            if ids.is_empty() {
                None
            } else {
                Some(ids)
            }
        });

        // Track visited nodes and relationships from forward and backward searches
        let mut forward_visited_nodes = RoaringBitmap::new();
        let mut forward_visited_rels = RoaringBitmap::new();
        let mut backward_visited_nodes = RoaringBitmap::new();
        let mut backward_visited_rels = RoaringBitmap::new();

        // Initialize queues: (node_id, depth)
        let mut forward_queue = std::collections::VecDeque::new();
        let mut backward_queue = std::collections::VecDeque::new();

        // Initialize forward search from source nodes
        for node_id in source_nodes.iter() {
            if node_filter.as_ref().map_or(true, |f| f(node_id, self)) {
                forward_visited_nodes.insert(node_id);
                forward_queue.push_back((node_id, 0u32));
            }
        }

        // Initialize backward search from target nodes
        for node_id in target_nodes.iter() {
            if node_filter.as_ref().map_or(true, |f| f(node_id, self)) {
                backward_visited_nodes.insert(node_id);
                backward_queue.push_back((node_id, 0u32));
            }
        }

        // Early exit if source and target sets overlap
        let intersection = &forward_visited_nodes & &backward_visited_nodes;
        if !intersection.is_empty() {
            // Return intersection of nodes and empty rel set (direct connection)
            return (intersection, RoaringBitmap::new());
        }

        // Alternate between forward and backward BFS until they meet or queues are empty
        while !forward_queue.is_empty() || !backward_queue.is_empty() {
            // Forward BFS step
            if !forward_queue.is_empty() {
                let current_level_size = forward_queue.len();
                for _ in 0..current_level_size {
                    let (current, depth) = forward_queue.pop_front().unwrap();
                    
                    // Check depth limit
                    if let Some(max) = max_depth {
                        if depth >= max {
                            continue;
                        }
                    }

                    // Get neighbors with CSR positions based on direction
                    let neighbors = match direction {
                        crate::types::Direction::Outgoing => {
                            self.get_outgoing_neighbors_with_positions(current, &rel_type_ids)
                        }
                        crate::types::Direction::Incoming => {
                            self.get_incoming_neighbors_with_positions(current, &rel_type_ids)
                        }
                        crate::types::Direction::Both => {
                            let mut both = self.get_outgoing_neighbors_with_positions(current, &rel_type_ids);
                            both.extend(self.get_incoming_neighbors_with_positions(current, &rel_type_ids));
                            both
                        }
                    };
                    
                    for (neighbor, rel_type, csr_pos) in neighbors {
                        // Check relationship filter
                        if let Some(ref rf) = rel_filter {
                            if !rf(current, neighbor, rel_type, csr_pos, self) {
                                continue;
                            }
                        }

                        // Check if we've met the backward search
                        if backward_visited_nodes.contains(neighbor) {
                            // Found intersection! Mark this relationship and node
                            forward_visited_rels.insert(csr_pos);
                            // Continue exploring from this node to find all paths (if not already visited)
                            if forward_visited_nodes.insert(neighbor) {
                                if node_filter.as_ref().map_or(true, |f| f(neighbor, self)) {
                                    forward_queue.push_back((neighbor, depth + 1));
                                }
                            }
                        } else {
                            // Check node filter and add to queue
                            if node_filter.as_ref().map_or(true, |f| f(neighbor, self)) {
                                if forward_visited_nodes.insert(neighbor) {
                                    // Only track relationship if we're continuing traversal
                                    forward_visited_rels.insert(csr_pos);
                                    forward_queue.push_back((neighbor, depth + 1));
                                }
                            }
                        }
                    }
                }
            }

            // Backward BFS step
            if !backward_queue.is_empty() {
                let current_level_size = backward_queue.len();
                for _ in 0..current_level_size {
                    let (current, depth) = backward_queue.pop_front().unwrap();
                    
                    // Check depth limit
                    if let Some(max) = max_depth {
                        if depth >= max {
                            continue;
                        }
                    }

                    // Get neighbors with CSR positions based on direction (backward uses opposite)
                    let neighbors = match direction {
                        crate::types::Direction::Outgoing => {
                            // Backward search uses incoming (reverse of forward)
                            self.get_incoming_neighbors_with_positions(current, &rel_type_ids)
                        }
                        crate::types::Direction::Incoming => {
                            // Backward search uses outgoing (reverse of forward)
                            self.get_outgoing_neighbors_with_positions(current, &rel_type_ids)
                        }
                        crate::types::Direction::Both => {
                            // Both directions for backward search too
                            let mut both = self.get_outgoing_neighbors_with_positions(current, &rel_type_ids);
                            both.extend(self.get_incoming_neighbors_with_positions(current, &rel_type_ids));
                            both
                        }
                    };
                    
                    for (neighbor, rel_type, csr_pos) in neighbors {
                        // Check relationship filter
                        if let Some(ref rf) = rel_filter {
                            if !rf(neighbor, current, rel_type, csr_pos, self) {
                                continue;
                            }
                        }

                        // Check if we've met the forward search
                        if forward_visited_nodes.contains(neighbor) {
                            // Found intersection! Mark this relationship and node
                            backward_visited_rels.insert(csr_pos);
                            // Continue exploring from this node to find all paths (if not already visited)
                            if backward_visited_nodes.insert(neighbor) {
                                if node_filter.as_ref().map_or(true, |f| f(neighbor, self)) {
                                    backward_queue.push_back((neighbor, depth + 1));
                                }
                            }
                        } else {
                            // Check node filter and add to queue
                            if node_filter.as_ref().map_or(true, |f| f(neighbor, self)) {
                                if backward_visited_nodes.insert(neighbor) {
                                    // Only track relationship if we're continuing traversal
                                    backward_visited_rels.insert(csr_pos);
                                    backward_queue.push_back((neighbor, depth + 1));
                                }
                            }
                        }
                    }
                }
            }
            
            // Continue until both queues are empty to explore all paths
            // Don't break early - we want to find all nodes on all paths
        }

        // Return intersection of nodes and union of relationships
        // The intersection includes all nodes that are reachable from both source and target,
        // which are the nodes on paths between them
        // The relationship union includes all relationships visited from both sides
        // that could be on paths between source and target
        let intersection_nodes = &forward_visited_nodes & &backward_visited_nodes;
        let union_rels = &forward_visited_rels | &backward_visited_rels;
        
        // If intersection is empty, there's no path between source and target
        // Return empty results
        if intersection_nodes.is_empty() {
            return (RoaringBitmap::new(), RoaringBitmap::new());
        }
        
        (intersection_nodes, union_rels)
    }

    /// BFS traversal from a set of starting nodes
    /// 
    /// Performs BFS from the starting nodes, following edges in the specified direction.
    /// Returns all nodes and relationships visited during the traversal.
    /// 
    /// # Arguments
    /// * `start_nodes` - Starting nodes for traversal
    /// * `direction` - Direction of traversal:
    ///   - `Outgoing`: Follow outgoing edges
    ///   - `Incoming`: Follow incoming edges
    ///   - `Both`: Follow both outgoing and incoming edges
    /// * `rel_types` - Optional filter: only follow relationships of these types
    /// * `node_filter` - Optional filter: returns `true` to include/continue from a node.
    ///   Takes `(node_id, snapshot)` and should return `true` to include the node.
    /// * `rel_filter` - Optional filter: returns `true` to follow a relationship.
    ///   Takes `(from_node, to_node, rel_type, csr_position, snapshot)` and should return `true` to follow.
    /// * `max_depth` - Optional maximum depth (default: no limit)
    /// 
    /// # Returns
    /// A tuple `(node_bitmap, rel_bitmap)` where:
    /// - `node_bitmap`: RoaringBitmap of node IDs visited during traversal
    /// - `rel_bitmap`: RoaringBitmap of relationship CSR positions traversed
    /// 
    /// # Examples
    /// ```
    /// use rustychickpeas_core::bitmap::NodeSet;
    /// use rustychickpeas_core::types::Direction;
    /// use roaring::RoaringBitmap;
    /// 
    /// // Simple BFS from a single node
    /// let start = NodeSet::new(RoaringBitmap::from_iter([0]));
    /// let (nodes, rels) = snapshot.bfs(
    ///     &start, Direction::Outgoing, None, None, None, None
    /// );
    /// 
    /// // BFS with relationship type filter
    /// let (nodes, rels) = snapshot.bfs(
    ///     &start, Direction::Outgoing, Some(&["KNOWS", "WORKS_WITH"]), None, None, None
    /// );
    /// 
    /// // BFS with max depth
    /// let (nodes, rels) = snapshot.bfs(
    ///     &start, Direction::Outgoing, None, None, None, Some(3)
    /// );
    /// 
    /// // BFS with node filter (only "Person" nodes)
    /// let node_filter = |node_id: NodeId, snapshot: &GraphSnapshot| -> bool {
    ///     snapshot.nodes_with_label("Person")
    ///         .map_or(false, |nodes| nodes.contains(node_id))
    /// };
    /// let (nodes, rels) = snapshot.bfs(
    ///     &start, Direction::Outgoing, None, Some(&node_filter), None, None
    /// );
    /// ```
    pub fn bfs<NF, RF>(
        &self,
        start_nodes: &NodeSet,
        direction: crate::types::Direction,
        rel_types: Option<&[&str]>,
        node_filter: Option<NF>,
        rel_filter: Option<RF>,
        max_depth: Option<u32>,
    ) -> (RoaringBitmap, RoaringBitmap)
    where
        NF: Fn(NodeId, &GraphSnapshot) -> bool,
        RF: Fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool,
    {
        // Convert relationship type strings to IDs if provided
        let rel_type_ids: Option<Vec<RelationshipType>> = rel_types.and_then(|types| {
            let ids: Vec<RelationshipType> = types
                .iter()
                .filter_map(|s| self.relationship_type_from_str(s))
                .collect();
            if ids.is_empty() {
                None
            } else {
                Some(ids)
            }
        });

        // BFS queue: (node_id, depth)
        let mut queue = std::collections::VecDeque::new();
        
        // Track visited nodes and relationships
        let mut visited_nodes = RoaringBitmap::new();
        let mut visited_rels = RoaringBitmap::new();

        // Initialize queue with starting nodes
        let start_node_ids: Vec<NodeId> = start_nodes.iter().collect();
        for node_id in start_node_ids {
            if node_filter.as_ref().map_or(true, |f| f(node_id, self)) {
                if visited_nodes.insert(node_id) {
                    queue.push_back((node_id, 0u32));
                }
            }
        }

        // BFS traversal
        while let Some((current, depth)) = queue.pop_front() {
            // Check depth limit
            if let Some(max) = max_depth {
                if depth >= max {
                    continue;
                }
            }

            // Get neighbors with CSR positions based on direction
            let neighbors = match direction {
                crate::types::Direction::Outgoing => {
                    self.get_outgoing_neighbors_with_positions(current, &rel_type_ids)
                }
                crate::types::Direction::Incoming => {
                    self.get_incoming_neighbors_with_positions(current, &rel_type_ids)
                }
                crate::types::Direction::Both => {
                    let mut both = self.get_outgoing_neighbors_with_positions(current, &rel_type_ids);
                    both.extend(self.get_incoming_neighbors_with_positions(current, &rel_type_ids));
                    both
                }
            };

            for (neighbor, rel_type, csr_pos) in neighbors {
                // Check relationship filter
                if let Some(ref rf) = rel_filter {
                    let from = current;
                    let to = neighbor;
                    if !rf(from, to, rel_type, csr_pos, self) {
                        continue;
                    }
                }

                // Check node filter and add to visited/queue
                if node_filter.as_ref().map_or(true, |f| f(neighbor, self)) {
                    // Mark relationship as visited
                    visited_rels.insert(csr_pos);
                    
                    // Add node to visited and queue if not already visited
                    if visited_nodes.insert(neighbor) {
                        queue.push_back((neighbor, depth + 1));
                    }
                }
            }
        }

        (visited_nodes, visited_rels)
    }

    /// Get outgoing neighbors with their CSR positions and relationship types
    /// Returns Vec of (neighbor_node_id, rel_type, csr_position)
    fn get_outgoing_neighbors_with_positions(
        &self,
        node_id: NodeId,
        rel_type_ids: &Option<Vec<RelationshipType>>,
    ) -> Vec<(NodeId, RelationshipType, u32)> {
        if node_id as usize >= self.out_offsets.len().saturating_sub(1) {
            return Vec::new();
        }
        let start = self.out_offsets[node_id as usize] as usize;
        let end = self.out_offsets[node_id as usize + 1] as usize;
        
        let mut result = Vec::new();
        
        if let Some(ref allowed_types) = rel_type_ids {
            let allowed_set: std::collections::HashSet<RelationshipType> = allowed_types.iter().copied().collect();
            for (idx, (&nbr, &rel_type)) in self.out_nbrs[start..end]
                .iter()
                .zip(self.out_types[start..end].iter())
                .enumerate()
            {
                if allowed_set.contains(&rel_type) {
                    result.push((nbr, rel_type, (start + idx) as u32));
                }
            }
        } else {
            for (idx, (&nbr, &rel_type)) in self.out_nbrs[start..end]
                .iter()
                .zip(self.out_types[start..end].iter())
                .enumerate()
            {
                result.push((nbr, rel_type, (start + idx) as u32));
            }
        }
        
        result
    }

    /// Get incoming neighbors with their CSR positions and relationship types
    /// Returns Vec of (neighbor_node_id, rel_type, csr_position)
    fn get_incoming_neighbors_with_positions(
        &self,
        node_id: NodeId,
        rel_type_ids: &Option<Vec<RelationshipType>>,
    ) -> Vec<(NodeId, RelationshipType, u32)> {
        if node_id as usize >= self.in_offsets.len().saturating_sub(1) {
            return Vec::new();
        }
        let start = self.in_offsets[node_id as usize] as usize;
        let end = self.in_offsets[node_id as usize + 1] as usize;
        
        let mut result = Vec::new();
        
        if let Some(ref allowed_types) = rel_type_ids {
            let allowed_set: std::collections::HashSet<RelationshipType> = allowed_types.iter().copied().collect();
            for (idx, (&nbr, &rel_type)) in self.in_nbrs[start..end]
                .iter()
                .zip(self.in_types[start..end].iter())
                .enumerate()
            {
                if allowed_set.contains(&rel_type) {
                    result.push((nbr, rel_type, (start + idx) as u32));
                }
            }
        } else {
            for (idx, (&nbr, &rel_type)) in self.in_nbrs[start..end]
                .iter()
                .zip(self.in_types[start..end].iter())
                .enumerate()
            {
                result.push((nbr, rel_type, (start + idx) as u32));
            }
        }
        
        result
    }

    /// Get nodes with a specific label
    /// 
    /// # Arguments
    /// * `label` - The label name (e.g., "Person")
    pub fn nodes_with_label(&self, label: &str) -> Option<&NodeSet> {
        let label_id = self.label_from_str(label)?;
        self.nodes_with_label_id(label_id)
    }

    /// Get nodes with a specific label (internal ID-based version)
    fn nodes_with_label_id(&self, label: Label) -> Option<&NodeSet> {
        self.label_index.get(&label)
    }

    /// Get relationships with a specific type
    /// 
    /// # Arguments
    /// * `rel_type` - The relationship type name (e.g., "KNOWS")
    pub fn relationships_with_type(&self, rel_type: &str) -> Option<&NodeSet> {
        let rel_type_id = self.relationship_type_from_str(rel_type)?;
        self.relationships_with_type_id(rel_type_id)
    }

    /// Get relationships with a specific type (internal ID-based version)
    fn relationships_with_type_id(&self, rel_type: RelationshipType) -> Option<&NodeSet> {
        self.type_index.get(&rel_type)
    }

    /// Get nodes with a specific property value
    /// 
    /// This method lazily builds the index for the property key on first access.
    /// The index is built by scanning the column and grouping nodes by value.
    /// 
    /// # Arguments
    /// * `label` - The label name (e.g., "Person")
    /// * `key` - The property key name (e.g., "name")
    /// * `value` - The property value to search for (can be `&str`, `String`, `i64`, `i32`, `f64`, `bool`, or `ValueId`)
    /// 
    /// # Examples
    /// ```
    /// // Find all Person nodes with name "Alice"
    /// let nodes = snapshot.nodes_with_property("Person", "name", "Alice");
    /// 
    /// // Find all Person nodes with age 30
    /// let nodes = snapshot.nodes_with_property("Person", "age", 30i64);
    /// 
    /// // Find all Person nodes with active = true
    /// let nodes = snapshot.nodes_with_property("Person", "active", true);
    /// ```
    pub fn nodes_with_property<V: IntoValueId>(&self, label: &str, key: &str, value: V) -> Option<NodeSet> {
        let label_id = self.label_from_str(label)?;
        let key_id = self.property_key_from_str(key)?;
        let value_id = value.into_value_id(self)?;
        self.nodes_with_property_id(label_id, key_id, value_id)
    }

    /// Get nodes with a specific property value (internal ID-based version)
    fn nodes_with_property_id(&self, label: Label, key: PropertyKey, value: ValueId) -> Option<NodeSet> {
        // Lock the index
        let mut index = self.prop_index.lock().unwrap();
        
        // Check if index for this (label, key) combination already exists
        let index_key = (label, key);
        if !index.contains_key(&index_key) {
            // Build index for this (label, key) combination by scanning the column
            // First, get all nodes with this label
            let label_nodes = self.nodes_with_label_id(label)?;
            
            // Get the property column
            let column = self.columns.get(&key)?;
            
            // Build index only for nodes with this label
            let key_index_final = Self::build_property_index_for_key_and_label(column, Some(&label_nodes));
            // Store the index for this (label, key) combination
            index.insert(index_key, key_index_final);
        }
        
        // Look up the value in the index
        index.get(&index_key)?.get(&value).cloned()
    }

    /// Get property value for a node
    /// 
    /// # Arguments
    /// * `node_id` - The node ID
    /// * `key` - The property key name (e.g., "name")
    pub fn prop(&self, node_id: NodeId, key: &str) -> Option<ValueId> {
        let key_id = self.property_key_from_str(key)?;
        self.prop_id(node_id, key_id)
    }

    /// Get property value for a node (internal ID-based version)
    fn prop_id(&self, node_id: NodeId, key: PropertyKey) -> Option<ValueId> {
        self.columns.get(&key)?.get(node_id)
    }

    /// Get property value for a relationship
    /// 
    /// # Arguments
    /// * `rel_csr_pos` - Relationship position in the outgoing CSR array (0 to n_rels-1)
    /// * `key` - The property key name (e.g., "verified")
    pub fn relationship_property(&self, rel_csr_pos: u32, key: &str) -> Option<ValueId> {
        let key_id = self.property_key_from_str(key)?;
        self.relationship_property_id(rel_csr_pos, key_id)
    }

    /// Get property value for a relationship (internal ID-based version)
    fn relationship_property_id(&self, rel_csr_pos: u32, key: PropertyKey) -> Option<ValueId> {
        self.rel_columns.get(&key)?.get(rel_csr_pos)
    }

    /// Get the version of this snapshot
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Resolve an interned string ID to a string
    pub fn resolve_string(&self, id: u32) -> Option<&str> {
        self.atoms.resolve(id)
    }

    /// Find a property key ID from a string name
    /// Returns None if the property key doesn't exist
    pub fn property_key_from_str(&self, key: &str) -> Option<PropertyKey> {
        self.atoms.get_id(key)
    }

    /// Find a label ID from a string name
    /// Returns None if the label doesn't exist
    pub fn label_from_str(&self, label: &str) -> Option<Label> {
        self.atoms.get_id(label).map(Label::new)
    }

    /// Find a value ID from a string value
    /// Returns None if the string value doesn't exist
    pub fn value_id_from_str(&self, value: &str) -> Option<ValueId> {
        self.atoms.get_id(value).map(ValueId::Str)
    }

    /// Find a relationship type ID from a string name
    /// Returns None if the relationship type doesn't exist
    pub fn relationship_type_from_str(&self, rel_type: &str) -> Option<RelationshipType> {
        self.atoms.get_id(rel_type).map(RelationshipType::new)
    }
}

impl Default for GraphSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphSnapshot {
    /// Create a GraphSnapshot from Parquet files using GraphBuilder
    pub fn from_parquet(
        nodes_path: Option<&str>,
        relationships_path: Option<&str>,
        node_id_column: Option<&str>,
        label_columns: Option<Vec<&str>>,
        node_property_columns: Option<Vec<&str>>,
        start_node_column: Option<&str>,
        end_node_column: Option<&str>,
        rel_type_column: Option<&str>,
        rel_property_columns: Option<Vec<&str>>,
    ) -> crate::error::Result<Self> {
        crate::graph_builder_parquet::read_from_parquet(
            nodes_path,
            relationships_path,
            node_id_column,
            label_columns,
            node_property_columns,
            start_node_column,
            end_node_column,
            rel_type_column,
            rel_property_columns,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_builder::GraphBuilder;
    use crate::types::Label;

    // Helper to create a simple snapshot for testing
    // This works around the into_vec() issue by manually creating atoms
    fn create_test_snapshot() -> GraphSnapshot {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(0), &["Person"]);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Company"]);
        builder.add_rel(0, 1, "KNOWS");
        builder.add_rel(1, 2, "WORKS_FOR");
        builder.set_prop_str(0, "name", "Alice");
        builder.set_prop_i64(0, "age", 30);
        
        // Manually create snapshot to avoid into_vec() issue
        // Calculate n based on max node ID used (nodes are 0, 1, 2, so n should be 3)
        let max_node_id = builder.node_labels.len()
            .max(builder.deg_out.len())
            .max(builder.deg_in.len())
            .saturating_sub(1);
        let n = (max_node_id + 1).max(3); // Ensure at least 3 for test nodes 0,1,2
        let m = builder.rels.len();
        
        // Build CSR
        let mut out_offsets = vec![0u32; n + 1];
        for i in 0..n {
            out_offsets[i + 1] = out_offsets[i] + builder.deg_out[i];
        }
        let mut out_nbrs = vec![0u32; m];
        let mut out_types = vec![RelationshipType::new(0); m];
        let mut out_pos = vec![0u32; n];
        for ((u, v), rel_type) in builder.rels.iter().zip(builder.rel_types.iter()) {
            let u_idx = *u as usize;
            let pos = (out_offsets[u_idx] + out_pos[u_idx]) as usize;
            out_nbrs[pos] = *v;
            out_types[pos] = *rel_type;
            out_pos[u_idx] += 1;
        }
        
        let mut in_offsets = vec![0u32; n + 1];
        for i in 0..n {
            in_offsets[i + 1] = in_offsets[i] + builder.deg_in[i];
        }
        let mut in_nbrs = vec![0u32; m];
        let mut in_types = vec![RelationshipType::new(0); m];
        let mut in_pos = vec![0u32; n];
        for ((u, v), rel_type) in builder.rels.iter().zip(builder.rel_types.iter()) {
            let v_idx = *v as usize;
            let pos = (in_offsets[v_idx] + in_pos[v_idx]) as usize;
            in_nbrs[pos] = *u;
            in_types[pos] = *rel_type;
            in_pos[v_idx] += 1;
        }
        
        // Build label index
        let mut label_index: HashMap<Label, Vec<NodeId>> = HashMap::new();
        for (node_id, labels) in builder.node_labels.iter().enumerate().take(n) {
            for label in labels {
                label_index.entry(*label).or_default().push(node_id as NodeId);
            }
        }
        use roaring::RoaringBitmap;
        let label_index: HashMap<Label, NodeSet> = label_index
            .into_iter()
            .map(|(label, mut nodes)| {
                nodes.sort_unstable();
                nodes.dedup();
                let bitmap = RoaringBitmap::from_sorted_iter(nodes.into_iter()).unwrap();
                (label, NodeSet::new(bitmap))
            })
            .collect();
        
        // Build type index
        let mut type_index: HashMap<RelationshipType, Vec<u32>> = HashMap::new();
        for (rel_idx, rel_type) in builder.rel_types.iter().enumerate() {
            type_index.entry(*rel_type).or_default().push(rel_idx as u32);
        }
        let type_index: HashMap<RelationshipType, NodeSet> = type_index
            .into_iter()
            .map(|(rel_type, mut rel_ids)| {
                rel_ids.sort_unstable();
                rel_ids.dedup();
                let bitmap = RoaringBitmap::from_sorted_iter(rel_ids.into_iter()).unwrap();
                (rel_type, NodeSet::new(bitmap))
            })
            .collect();
        
        // Build property columns (sparse for small test)
        let mut columns: HashMap<PropertyKey, Column> = HashMap::new();
        let name_key = builder.interner.get_or_intern("name");
        let age_key = builder.interner.get_or_intern("age");
        
        if let Some(pairs) = builder.node_col_str.get(&name_key) {
            let mut pairs = pairs.clone();
            pairs.sort_unstable_by_key(|(id, _)| *id);
            columns.insert(name_key, Column::SparseStr(pairs));
        }
        if let Some(pairs) = builder.node_col_i64.get(&age_key) {
            let mut pairs = pairs.clone();
            pairs.sort_unstable_by_key(|(id, _)| *id);
            columns.insert(age_key, Column::SparseI64(pairs));
        }
        
        // Create atoms manually - need to match the actual interner IDs
        // The interner assigns IDs sequentially, so we need to track what was interned
        let mut atoms_vec = vec!["".to_string()]; // ID 0 is always empty
        // Get all interned strings in order by resolving IDs
        let interner_len = builder.interner.len();
        for i in 1..interner_len {
            if let Some(s) = builder.interner.try_resolve(i as u32) {
                atoms_vec.push(s);
            }
        }
        let atoms = Atoms::new(atoms_vec);
        
        GraphSnapshot {
            n_nodes: n as u32,
            n_rels: m as u64,
            out_offsets,
            out_nbrs,
            out_types,
            in_offsets,
            in_nbrs,
            in_types,
            label_index,
            type_index,
            version: builder.version.clone(),
            columns,
            rel_columns: HashMap::new(),
            prop_index: Mutex::new(HashMap::new()),
            atoms,
        }
    }

    #[test]
    fn test_snapshot_new() {
        let snapshot = GraphSnapshot::new();
        assert_eq!(snapshot.n_nodes, 0);
        assert_eq!(snapshot.n_rels, 0);
        assert!(snapshot.version.is_none());
    }

    #[test]
    fn test_snapshot_default() {
        let snapshot = GraphSnapshot::default();
        assert_eq!(snapshot.n_nodes, 0);
        assert_eq!(snapshot.n_rels, 0);
    }

    #[test]
    fn test_atoms_new() {
        let atoms = Atoms::new(vec!["".to_string(), "hello".to_string()]);
        assert_eq!(atoms.strings.len(), 2);
    }

    #[test]
    fn test_atoms_resolve() {
        let atoms = Atoms::new(vec!["".to_string(), "hello".to_string(), "world".to_string()]);
        assert_eq!(atoms.resolve(0), Some(""));
        assert_eq!(atoms.resolve(1), Some("hello"));
        assert_eq!(atoms.resolve(2), Some("world"));
        assert_eq!(atoms.resolve(99), None);
    }

    #[test]
    fn test_get_out_neighbors() {
        let snapshot = create_test_snapshot();
        let neighbors = snapshot.out_neighbors(0);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 1);
        
        let neighbors = snapshot.out_neighbors(1);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 2);
        
        let neighbors = snapshot.out_neighbors(2);
        assert_eq!(neighbors.len(), 0);
    }

    #[test]
    fn test_get_in_neighbors() {
        let snapshot = create_test_snapshot();
        let neighbors = snapshot.in_neighbors(0);
        assert_eq!(neighbors.len(), 0);
        
        let neighbors = snapshot.in_neighbors(1);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 0);
        
        let neighbors = snapshot.in_neighbors(2);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 1);
    }

    #[test]
    fn test_get_out_neighbors_invalid() {
        let snapshot = create_test_snapshot();
        let neighbors = snapshot.out_neighbors(999);
        assert_eq!(neighbors.len(), 0);
    }

    #[test]
    fn test_get_in_neighbors_invalid() {
        let snapshot = create_test_snapshot();
        let neighbors = snapshot.in_neighbors(999);
        assert_eq!(neighbors.len(), 0);
    }

    #[test]
    fn test_can_reach_basic() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 -> 2 (via KNOWS and WORKS_FOR)
        
        // Direct neighbors
        assert!(snapshot.can_reach(0, 1, crate::types::Direction::Outgoing, None, None));
        assert!(snapshot.can_reach(1, 2, crate::types::Direction::Outgoing, None, None));
        
        // Multi-hop
        assert!(snapshot.can_reach(0, 2, crate::types::Direction::Outgoing, None, None));
        
        // Reverse direction (should not work for outgoing)
        assert!(!snapshot.can_reach(2, 0, crate::types::Direction::Outgoing, None, None));
        assert!(!snapshot.can_reach(1, 0, crate::types::Direction::Outgoing, None, None));
        
        // Same node
        assert!(snapshot.can_reach(0, 0, crate::types::Direction::Outgoing, None, None));
    }

    #[test]
    fn test_can_reach_with_rel_type_filter() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 (KNOWS), 1 -> 2 (WORKS_FOR)
        
        // Can reach via KNOWS
        assert!(snapshot.can_reach(0, 1, crate::types::Direction::Outgoing, Some(&["KNOWS"]), None));
        
        // Cannot reach 2 via KNOWS only (need WORKS_FOR)
        assert!(!snapshot.can_reach(0, 2, crate::types::Direction::Outgoing, Some(&["KNOWS"]), None));
        
        // Can reach 2 via both types
        assert!(snapshot.can_reach(0, 2, crate::types::Direction::Outgoing, Some(&["KNOWS", "WORKS_FOR"]), None));
        
        // Can reach 2 via WORKS_FOR only (from node 1)
        assert!(snapshot.can_reach(1, 2, crate::types::Direction::Outgoing, Some(&["WORKS_FOR"]), None));
    }

    #[test]
    fn test_can_reach_with_max_depth() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 -> 2
        
        // Can reach with depth 2
        assert!(snapshot.can_reach(0, 2, crate::types::Direction::Outgoing, None, Some(2)));
        
        // Cannot reach with depth 1 (only one hop allowed)
        assert!(!snapshot.can_reach(0, 2, crate::types::Direction::Outgoing, None, Some(1)));
        
        // Can reach direct neighbor with depth 1
        assert!(snapshot.can_reach(0, 1, crate::types::Direction::Outgoing, None, Some(1)));
    }

    #[test]
    fn test_can_reach_direction_both() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 -> 2
        
        // With Direction::Both, can traverse in both directions
        // From 2, can reach 1 via incoming edge
        assert!(snapshot.can_reach(2, 1, crate::types::Direction::Both, None, None));
        
        // From 2, can reach 0 via incoming edges (2 <- 1 <- 0)
        assert!(snapshot.can_reach(2, 0, crate::types::Direction::Both, None, None));
        
        // From 0, can still reach 2 via outgoing (0 -> 1 -> 2)
        assert!(snapshot.can_reach(0, 2, crate::types::Direction::Both, None, None));
    }

    #[test]
    fn test_can_reach_unreachable() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 -> 2
        
        // Node 999 doesn't exist, so unreachable
        assert!(!snapshot.can_reach(0, 999, crate::types::Direction::Outgoing, None, None));
        assert!(!snapshot.can_reach(999, 0, crate::types::Direction::Outgoing, None, None));
    }

    #[test]
    fn test_get_nodes_with_label() {
        let snapshot = create_test_snapshot();
        let nodes = snapshot.nodes_with_label("Person");
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
    }

    #[test]
    fn test_get_rels_with_type() {
        let snapshot = create_test_snapshot();
        let rels = snapshot.relationships_with_type("KNOWS");
        assert!(rels.is_some());
        let rels = rels.unwrap();
        assert_eq!(rels.len(), 1);
    }

    #[test]
    fn test_get_property() {
        let snapshot = create_test_snapshot();
        let prop = snapshot.prop(0, "name");
        assert!(prop.is_some());
        let alice_id = snapshot.value_id_from_str("Alice").unwrap();
        assert_eq!(prop, Some(alice_id));
    }

    #[test]
    fn test_get_property_nonexistent() {
        let snapshot = create_test_snapshot();
        let prop = snapshot.prop(999, "nonexistent");
        assert!(prop.is_none());
    }

    #[test]
    fn test_version() {
        let snapshot = create_test_snapshot();
        assert!(snapshot.version().is_none());
    }

    #[test]
    fn test_resolve_string() {
        let snapshot = create_test_snapshot();
        assert_eq!(snapshot.resolve_string(0), Some(""));
        // Find Person and Alice by searching atoms
        let person_idx = snapshot.atoms.strings.iter()
            .position(|s| s == "Person")
            .unwrap();
        let alice_idx = snapshot.atoms.strings.iter()
            .position(|s| s == "Alice")
            .unwrap();
        assert_eq!(snapshot.resolve_string(person_idx as u32), Some("Person"));
        assert_eq!(snapshot.resolve_string(alice_idx as u32), Some("Alice"));
        assert_eq!(snapshot.resolve_string(99), None);
    }

    #[test]
    fn test_column_dense_i64_get() {
        let col = Column::DenseI64(vec![10, 20, 30]);
        assert_eq!(col.get(0), Some(ValueId::I64(10)));
        assert_eq!(col.get(1), Some(ValueId::I64(20)));
        assert_eq!(col.get(2), Some(ValueId::I64(30)));
        assert_eq!(col.get(99), None);
    }

    #[test]
    fn test_column_dense_f64_get() {
        let col = Column::DenseF64(vec![1.5, 2.5, 3.5]);
        assert_eq!(col.get(0), Some(ValueId::from_f64(1.5)));
        assert_eq!(col.get(1), Some(ValueId::from_f64(2.5)));
    }

    #[test]
    fn test_column_dense_bool_get() {
        let mut bv = bitvec::vec::BitVec::new();
        bv.resize(3, false);
        bv.set(0, true);
        bv.set(2, true);
        let col = Column::DenseBool(bv);
        assert_eq!(col.get(0), Some(ValueId::Bool(true)));
        assert_eq!(col.get(1), Some(ValueId::Bool(false)));
        assert_eq!(col.get(2), Some(ValueId::Bool(true)));
    }

    #[test]
    fn test_column_dense_str_get() {
        let col = Column::DenseStr(vec![1, 2, 3]);
        assert_eq!(col.get(0), Some(ValueId::Str(1)));
        assert_eq!(col.get(1), Some(ValueId::Str(2)));
    }

    #[test]
    fn test_column_sparse_i64_get() {
        let col = Column::SparseI64(vec![(0, 10), (2, 30)]);
        assert_eq!(col.get(0), Some(ValueId::I64(10)));
        assert_eq!(col.get(1), None);
        assert_eq!(col.get(2), Some(ValueId::I64(30)));
    }

    #[test]
    fn test_column_sparse_f64_get() {
        let col = Column::SparseF64(vec![(0, 1.5), (2, 3.5)]);
        assert_eq!(col.get(0), Some(ValueId::from_f64(1.5)));
        assert_eq!(col.get(1), None);
    }

    #[test]
    fn test_column_sparse_bool_get() {
        let col = Column::SparseBool(vec![(0, true), (2, false)]);
        assert_eq!(col.get(0), Some(ValueId::Bool(true)));
        assert_eq!(col.get(1), None);
        assert_eq!(col.get(2), Some(ValueId::Bool(false)));
    }

    #[test]
    fn test_column_sparse_str_get() {
        let col = Column::SparseStr(vec![(0, 1), (2, 3)]);
        assert_eq!(col.get(0), Some(ValueId::Str(1)));
        assert_eq!(col.get(1), None);
        assert_eq!(col.get(2), Some(ValueId::Str(3)));
    }

    #[test]
    fn test_valueid_from_f64() {
        let val = ValueId::from_f64(3.14);
        assert_eq!(val.to_f64(), Some(3.14));
    }

    #[test]
    fn test_build_property_index_dense_i64() {
        let col = Column::DenseI64(vec![10, 20, 10]);
        let index = GraphSnapshot::build_property_index_for_key(&col);
        assert_eq!(index.len(), 2);
        assert!(index.contains_key(&ValueId::I64(10)));
        assert!(index.contains_key(&ValueId::I64(20)));
        let nodes_10 = index.get(&ValueId::I64(10)).unwrap();
        assert_eq!(nodes_10.len(), 2);
        assert!(nodes_10.contains(0));
        assert!(nodes_10.contains(2));
    }

    #[test]
    fn test_build_property_index_sparse_str() {
        let col = Column::SparseStr(vec![(0, 1), (2, 1)]);
        let index = GraphSnapshot::build_property_index_for_key(&col);
        assert_eq!(index.len(), 1);
        let nodes = index.get(&ValueId::Str(1)).unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(0));
        assert!(nodes.contains(2));
    }

    #[test]
    fn test_build_property_index_dense_f64() {
        let col = Column::DenseF64(vec![1.5, 2.5, 1.5]);
        let index = GraphSnapshot::build_property_index_for_key(&col);
        assert_eq!(index.len(), 2);
        let nodes_15 = index.get(&ValueId::from_f64(1.5)).unwrap();
        assert_eq!(nodes_15.len(), 2);
        assert!(nodes_15.contains(0));
        assert!(nodes_15.contains(2));
    }

    #[test]
    fn test_build_property_index_dense_bool() {
        let mut bv = bitvec::vec::BitVec::new();
        bv.resize(3, false);
        bv.set(0, true);
        bv.set(2, true);
        let col = Column::DenseBool(bv);
        let index = GraphSnapshot::build_property_index_for_key(&col);
        assert_eq!(index.len(), 2);
        let true_nodes = index.get(&ValueId::Bool(true)).unwrap();
        assert_eq!(true_nodes.len(), 2);
        assert!(true_nodes.contains(0));
        assert!(true_nodes.contains(2));
    }

    #[test]
    fn test_build_property_index_dense_str() {
        let col = Column::DenseStr(vec![1, 2, 1]);
        let index = GraphSnapshot::build_property_index_for_key(&col);
        assert_eq!(index.len(), 2);
        let nodes_1 = index.get(&ValueId::Str(1)).unwrap();
        assert_eq!(nodes_1.len(), 2);
        assert!(nodes_1.contains(0));
        assert!(nodes_1.contains(2));
    }

    #[test]
    fn test_build_property_index_sparse_f64() {
        let col = Column::SparseF64(vec![(0, 1.5), (2, 1.5)]);
        let index = GraphSnapshot::build_property_index_for_key(&col);
        assert_eq!(index.len(), 1);
        let nodes = index.get(&ValueId::from_f64(1.5)).unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_build_property_index_sparse_bool() {
        let col = Column::SparseBool(vec![(0, true), (2, true)]);
        let index = GraphSnapshot::build_property_index_for_key(&col);
        assert_eq!(index.len(), 1);
        let nodes = index.get(&ValueId::Bool(true)).unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_get_nodes_with_property_lazy() {
        let snapshot = create_test_snapshot();
        
        // First access should build the index lazily
        let nodes = snapshot.nodes_with_property("Person", "name", "Alice");
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes.contains(0));
    }

    #[test]
    fn test_get_nodes_with_property_nonexistent_key() {
        let snapshot = create_test_snapshot();
        let nodes = snapshot.nodes_with_property("Person", "nonexistent", 1i64);
        assert!(nodes.is_none());
    }

    #[test]
    fn test_get_nodes_with_property_nonexistent_value() {
        let snapshot = create_test_snapshot();
        let nodes = snapshot.nodes_with_property("Person", "name", "Nonexistent");
        assert!(nodes.is_none());
    }

    #[test]
    fn test_get_nodes_with_property_label_scoping() {
        // Test that nodes_with_property correctly scopes by label
        // Same property key on different labels should return different results
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        
        // Add nodes with different labels but same property key
        builder.add_node(Some(0), &["Person"]);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Company"]);
        builder.add_node(Some(3), &["Company"]);
        
        // Set same property key "name" on all nodes
        builder.set_prop_str(0, "name", "Alice");
        builder.set_prop_str(1, "name", "Bob");
        builder.set_prop_str(2, "name", "Acme Corp");
        builder.set_prop_str(3, "name", "Tech Inc");
        
        let snapshot = builder.finalize(None);
        
        // Query Person label - should only return Person nodes
        let person_nodes = snapshot.nodes_with_property("Person", "name", "Alice");
        assert!(person_nodes.is_some());
        let person_nodes = person_nodes.unwrap();
        assert_eq!(person_nodes.len(), 1);
        assert!(person_nodes.contains(0));
        
        let person_nodes = snapshot.nodes_with_property("Person", "name", "Bob");
        assert!(person_nodes.is_some());
        let person_nodes = person_nodes.unwrap();
        assert_eq!(person_nodes.len(), 1);
        assert!(person_nodes.contains(1));
        
        // Query Company label - should only return Company nodes
        let company_nodes = snapshot.nodes_with_property("Company", "name", "Acme Corp");
        assert!(company_nodes.is_some());
        let company_nodes = company_nodes.unwrap();
        assert_eq!(company_nodes.len(), 1);
        assert!(company_nodes.contains(2));
        
        // Query Person label for Company value - should return None
        let result = snapshot.nodes_with_property("Person", "name", "Acme Corp");
        assert!(result.is_none() || result.unwrap().len() == 0);
    }

    #[test]
    fn test_get_nodes_with_property_multiple_values() {
        // Test that multiple nodes with the same property value are all returned
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        
        builder.add_node(Some(0), &["Person"]);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_node(Some(3), &["Person"]);
        
        // Set same age for multiple nodes
        builder.set_prop_i64(0, "age", 30);
        builder.set_prop_i64(1, "age", 30);
        builder.set_prop_i64(2, "age", 25);
        builder.set_prop_i64(3, "age", 30);
        
        let snapshot = builder.finalize(None);
        
        // Should return 3 nodes with age 30
        let nodes = snapshot.nodes_with_property("Person", "age", 30i64);
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 3);
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(3));
        
        // Should return 1 node with age 25
        let nodes = snapshot.nodes_with_property("Person", "age", 25i64);
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes.contains(2));
    }

    #[test]
    fn test_get_nodes_with_property_all_types() {
        // Test nodes_with_property with all property value types
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        
        builder.add_node(Some(0), &["Person"]);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_node(Some(3), &["Person"]);
        
        builder.set_prop_str(0, "name", "Alice");
        builder.set_prop_i64(1, "age", 30);
        builder.set_prop_f64(2, "score", 95.5);
        builder.set_prop_bool(3, "active", true);
        
        let snapshot = builder.finalize(None);
        
        // Test string property
        let nodes = snapshot.nodes_with_property("Person", "name", "Alice");
        assert!(nodes.is_some());
        assert_eq!(nodes.unwrap().len(), 1);
        
        // Test i64 property
        let nodes = snapshot.nodes_with_property("Person", "age", 30i64);
        assert!(nodes.is_some());
        assert_eq!(nodes.unwrap().len(), 1);
        
        // Test f64 property
        let nodes = snapshot.nodes_with_property("Person", "score", 95.5);
        assert!(nodes.is_some());
        assert_eq!(nodes.unwrap().len(), 1);
        
        // Test bool property
        let nodes = snapshot.nodes_with_property("Person", "active", true);
        assert!(nodes.is_some());
        assert_eq!(nodes.unwrap().len(), 1);
    }

    #[test]
    fn test_get_nodes_with_label_nonexistent() {
        let snapshot = create_test_snapshot();
        let nodes = snapshot.nodes_with_label("Nonexistent");
        assert!(nodes.is_none());
    }

    #[test]
    fn test_get_rels_with_type_nonexistent() {
        let snapshot = create_test_snapshot();
        let rels = snapshot.relationships_with_type("Nonexistent");
        assert!(rels.is_none());
    }

    #[test]
    fn test_snapshot_with_version() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.set_version("v1.0");
        builder.add_node(Some(0), &["Person"]);
        
        // Manually create snapshot with version
        let max_node_id = builder.node_labels.len()
            .max(builder.deg_out.len())
            .max(builder.deg_in.len())
            .saturating_sub(1);
        let n = (max_node_id + 1).max(2); // Ensure at least 2 for test node 0
        let mut atoms_vec = vec!["".to_string()];
        let interner_len = builder.interner.len();
        for i in 1..interner_len {
            if let Some(s) = builder.interner.try_resolve(i as u32) {
                atoms_vec.push(s);
            }
        }
        
        let snapshot = GraphSnapshot {
            n_nodes: n as u32,
            n_rels: 0,
            out_offsets: vec![0, 0],
            out_nbrs: Vec::new(),
            out_types: Vec::new(),
            in_offsets: vec![0, 0],
            in_nbrs: Vec::new(),
            in_types: Vec::new(),
            label_index: HashMap::new(),
            type_index: HashMap::new(),
            version: builder.version.clone(),
            columns: HashMap::new(),
            rel_columns: HashMap::new(),
            prop_index: Mutex::new(HashMap::new()),
            atoms: Atoms::new(atoms_vec),
        };
        
        assert_eq!(snapshot.version(), Some("v1.0"));
    }

    #[test]
    fn test_valueid_to_f64_none() {
        // Test that non-F64 ValueIds return None
        assert_eq!(ValueId::Str(0).to_f64(), None);
        assert_eq!(ValueId::I64(42).to_f64(), None);
        assert_eq!(ValueId::Bool(true).to_f64(), None);
    }

    #[test]
    fn test_build_property_index_sparse_i64() {
        // Test building property index for sparse i64 column (line 198-200)
        let col = Column::SparseI64(vec![(0, 10), (2, 20), (5, 10)]);
        let index = GraphSnapshot::build_property_index_for_key(&col);
        assert_eq!(index.len(), 2);
        let nodes_10 = index.get(&ValueId::I64(10)).unwrap();
        assert_eq!(nodes_10.len(), 2);
        assert!(nodes_10.contains(0));
        assert!(nodes_10.contains(5));
        let nodes_20 = index.get(&ValueId::I64(20)).unwrap();
        assert_eq!(nodes_20.len(), 1);
        assert!(nodes_20.contains(2));
    }

    // Helper to create a snapshot with a path for bidirectional BFS testing
    // Graph: 0 -> 1 -> 2 -> 3, and also 0 -> 4 -> 3 (two paths from 0 to 3)
    fn create_bidirectional_test_snapshot() -> GraphSnapshot {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(0), &["Person"]);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_node(Some(3), &["Person"]);
        builder.add_node(Some(4), &["Person"]);
        builder.add_rel(0, 1, "KNOWS");
        builder.add_rel(1, 2, "KNOWS");
        builder.add_rel(2, 3, "KNOWS");
        builder.add_rel(0, 4, "KNOWS");
        builder.add_rel(4, 3, "KNOWS");
        builder.finalize(None)
    }

    #[test]
    fn test_bidirectional_bfs_basic() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: find paths from node 0 to node 3
        let source = NodeSet::new(RoaringBitmap::from_iter([0]));
        let target = NodeSet::new(RoaringBitmap::from_iter([3]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(&source, &target, crate::types::Direction::Outgoing, None, None, None, None);
        
        // Should find nodes 0, 1, 2, 3, 4 (all on paths from 0 to 3)
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(nodes.contains(4));
        assert!(!nodes.contains(5)); // Node 5 doesn't exist
        
        // Should find relationships on paths
        assert!(rels.len() > 0);
    }

    #[test]
    fn test_bidirectional_bfs_no_path() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: no path from node 3 to node 0 (reverse direction)
        let source = NodeSet::new(RoaringBitmap::from_iter([3]));
        let target = NodeSet::new(RoaringBitmap::from_iter([0]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(&source, &target, crate::types::Direction::Outgoing, None, None, None, None);
        
        // Should find intersection (both sets contain their starting nodes)
        // But no actual path, so intersection should be empty or just the endpoints
        // Actually, if source and target don't overlap initially, and no path exists,
        // the intersection should be empty
        assert!(nodes.is_empty() || nodes.len() == 0);
    }

    #[test]
    fn test_bidirectional_bfs_with_rel_type_filter() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: filter by relationship type
        let source = NodeSet::new(RoaringBitmap::from_iter([0]));
        let target = NodeSet::new(RoaringBitmap::from_iter([3]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source, &target, 
            crate::types::Direction::Outgoing,
            Some(&["KNOWS"]), 
            None, None, None
        );
        
        // Should still find the path
        assert!(nodes.contains(0));
        assert!(nodes.contains(3));
    }

    #[test]
    fn test_bidirectional_bfs_with_node_filter() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: filter nodes by label
        let source = NodeSet::new(RoaringBitmap::from_iter([0]));
        let target = NodeSet::new(RoaringBitmap::from_iter([3]));
        
        let node_filter = |node_id: NodeId, snapshot: &GraphSnapshot| -> bool {
            snapshot.nodes_with_label("Person")
                .map_or(false, |nodes| nodes.contains(node_id))
        };
        
        // Type annotations needed for None filters
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bidirectional_bfs(
            &source, &target, 
            crate::types::Direction::Outgoing,
            None, 
            Some(&node_filter), 
            None::<RelFilter>, 
            None
        );
        
        // Should find path since all nodes have "Person" label
        assert!(nodes.contains(0));
        assert!(nodes.contains(3));
    }

    #[test]
    fn test_bidirectional_bfs_with_max_depth() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: max depth limits traversal
        let source = NodeSet::new(RoaringBitmap::from_iter([0]));
        let target = NodeSet::new(RoaringBitmap::from_iter([3]));
        
        // With depth 1, should not reach node 3
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source, &target, 
            crate::types::Direction::Outgoing,
            None, 
            None, 
            None, 
            Some(1)
        );
        
        // Should not find node 3 with depth 1
        assert!(!nodes.contains(3));
        
        // With depth 3, should reach node 3
        let (nodes, _rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source, &target, 
            crate::types::Direction::Outgoing,
            None, 
            None, 
            None, 
            Some(3)
        );
        
        // Should find node 3 with depth 3
        assert!(nodes.contains(3));
    }

    #[test]
    fn test_bidirectional_bfs_multiple_sources_targets() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: multiple source and target nodes
        let source = NodeSet::new(RoaringBitmap::from_iter([0, 1]));
        let target = NodeSet::new(RoaringBitmap::from_iter([2, 3]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(&source, &target, crate::types::Direction::Outgoing, None, None, None, None);
        
        // Should find intersection nodes
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(rels.len() > 0);
    }

    // Helper to create a snapshot for BFS testing
    // Graph: 0 -> 1 -> 2 -> 3, and also 0 -> 4 -> 3, and 0 -> 5 (WORKS_FOR)
    fn create_bfs_test_snapshot() -> GraphSnapshot {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(0), &["Person"]);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_node(Some(3), &["Person"]);
        builder.add_node(Some(4), &["Person"]);
        builder.add_node(Some(5), &["Company"]);
        builder.add_rel(0, 1, "KNOWS");
        builder.add_rel(1, 2, "KNOWS");
        builder.add_rel(2, 3, "KNOWS");
        builder.add_rel(0, 4, "KNOWS");
        builder.add_rel(4, 3, "KNOWS");
        builder.add_rel(0, 5, "WORKS_FOR");
        builder.finalize(None)
    }

    #[test]
    fn test_bfs_basic() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: BFS from node 0, outgoing direction
        let start = NodeSet::new(RoaringBitmap::from_iter([0]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start, 
            crate::types::Direction::Outgoing, 
            None, None, None, None
        );
        
        // Should find nodes 0, 1, 2, 3, 4, 5 (all reachable from 0)
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(nodes.contains(4));
        assert!(nodes.contains(5));
        assert!(rels.len() > 0);
    }

    #[test]
    fn test_bfs_with_rel_type_filter() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: filter by relationship type "KNOWS" only
        let start = NodeSet::new(RoaringBitmap::from_iter([0]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start, 
            crate::types::Direction::Outgoing,
            Some(&["KNOWS"]), 
            None, None, None
        );
        
        // Should find nodes 0, 1, 2, 3, 4 (via KNOWS relationships)
        // Should NOT find node 5 (connected via WORKS_FOR)
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(nodes.contains(4));
        assert!(!nodes.contains(5));
    }

    #[test]
    fn test_bfs_with_node_filter() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: filter by node label "Person"
        let start = NodeSet::new(RoaringBitmap::from_iter([0]));
        
        let node_filter = |node_id: NodeId, snapshot: &GraphSnapshot| -> bool {
            snapshot.nodes_with_label("Person")
                .map_or(false, |nodes| nodes.contains(node_id))
        };
        
        // Type annotations needed for None filters
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs(
            &start, 
            crate::types::Direction::Outgoing,
            None, 
            Some(&node_filter), 
            None::<RelFilter>, 
            None
        );
        
        // Should find nodes 0, 1, 2, 3, 4 (all have "Person" label)
        // Should NOT find node 5 (has "Company" label)
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(nodes.contains(4));
        assert!(!nodes.contains(5));
    }

    #[test]
    fn test_bfs_with_max_depth() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: max depth limits traversal
        let start = NodeSet::new(RoaringBitmap::from_iter([0]));
        
        // With depth 1, should only reach direct neighbors
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start, 
            crate::types::Direction::Outgoing,
            None, 
            None, 
            None, 
            Some(1)
        );
        
        // Should find nodes 0, 1, 4, 5 (direct neighbors)
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(4));
        assert!(nodes.contains(5));
        // Should NOT find nodes 2, 3 (depth 2+)
        assert!(!nodes.contains(2));
        assert!(!nodes.contains(3));
        
        // With depth 2, should reach nodes 2
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start, 
            crate::types::Direction::Outgoing,
            None, 
            None, 
            None, 
            Some(2)
        );
        
        assert!(nodes.contains(2));
        // Should find node 3 (reachable via 0->4->3 at depth 2)
        assert!(nodes.contains(3));
    }

    #[test]
    fn test_bfs_with_direction_incoming() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: BFS from node 3, incoming direction (reverse traversal)
        let start = NodeSet::new(RoaringBitmap::from_iter([3]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start, 
            crate::types::Direction::Incoming, 
            None, None, None, None
        );
        
        // Should find nodes reachable by following incoming edges: 3, 2, 4, 1, 0
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(nodes.contains(4));
    }

    #[test]
    fn test_bfs_with_direction_both() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: BFS from node 2, both directions
        let start = NodeSet::new(RoaringBitmap::from_iter([2]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start, 
            crate::types::Direction::Both, 
            None, None, None, None
        );
        
        // Should find nodes reachable in both directions: 0, 1, 2, 3, 4
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(nodes.contains(4));
    }

    #[test]
    fn test_bfs_multiple_start_nodes() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Test: BFS from multiple starting nodes
        let start = NodeSet::new(RoaringBitmap::from_iter([0, 2]));
        
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start, 
            crate::types::Direction::Outgoing, 
            None, None, None, None
        );
        
        // Should find nodes reachable from either start node
        assert!(nodes.contains(0));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3)); // Reachable from both
        assert!(nodes.contains(1)); // Reachable from 0
    }

}

