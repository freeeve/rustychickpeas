//! Immutable graph snapshot optimized for read-only queries
//!
//! GraphSnapshot uses CSR (Compressed Sparse Row) format for adjacency
//! and columnar storage for properties, providing maximum query performance.

use crate::bitmap::NodeSet;
use crate::types::{Direction, Label, NodeId, PropertyKey, RelationshipType};
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
    #[inline]
    pub fn get(&self, node_id: NodeId) -> Option<ValueId> {
        self.get_dense(node_id).or_else(|| self.get_sparse(node_id))
    }

    /// Iterate over all (NodeId, ValueId) entries in this column
    pub fn iter_entries(&self) -> Box<dyn Iterator<Item = (NodeId, ValueId)> + '_> {
        match self {
            Column::DenseI64(col) => Box::new(
                col.iter().enumerate().map(|(i, &v)| (i as NodeId, ValueId::I64(v)))
            ),
            Column::DenseF64(col) => Box::new(
                col.iter().enumerate().map(|(i, &v)| (i as NodeId, ValueId::from_f64(v)))
            ),
            Column::DenseBool(col) => Box::new(
                col.iter().enumerate().map(|(i, b)| (i as NodeId, ValueId::Bool(*b)))
            ),
            Column::DenseStr(col) => Box::new(
                col.iter().enumerate().map(|(i, &v)| (i as NodeId, ValueId::Str(v)))
            ),
            Column::SparseI64(col) => Box::new(
                col.iter().map(|&(id, v)| (id, ValueId::I64(v)))
            ),
            Column::SparseF64(col) => Box::new(
                col.iter().map(|&(id, v)| (id, ValueId::from_f64(v)))
            ),
            Column::SparseBool(col) => Box::new(
                col.iter().map(|&(id, v)| (id, ValueId::Bool(v)))
            ),
            Column::SparseStr(col) => Box::new(
                col.iter().map(|&(id, v)| (id, ValueId::Str(v)))
            ),
        }
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
    #[cfg(test)]
    fn build_property_index_for_key(column: &Column) -> HashMap<ValueId, NodeSet> {
        Self::build_property_index_for_key_and_label(column, None)
    }

    /// Build the property index for a specific key and label using Column::iter_entries.
    /// If label_nodes is Some, only nodes in that set are included in the index.
    fn build_property_index_for_key_and_label(column: &Column, label_nodes: Option<&NodeSet>) -> HashMap<ValueId, NodeSet> {
        let mut key_index: HashMap<ValueId, Vec<NodeId>> = HashMap::new();

        for (node_id, val_id) in column.iter_entries() {
            if label_nodes.map_or(true, |nodes| nodes.contains(node_id)) {
                key_index.entry(val_id).or_default().push(node_id);
            }
        }

        key_index.into_iter().map(|(val_id, mut node_ids)| {
            node_ids.sort_unstable();
            node_ids.dedup();
            let bitmap = RoaringBitmap::from_sorted_iter(node_ids.into_iter()).unwrap();
            (val_id, NodeSet::new(bitmap))
        }).collect()
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

    /// Resolve relationship type strings to internal IDs. Returns None if the input
    /// is None or all strings are unknown (same semantics as "no filter").
    fn resolve_rel_types(&self, rel_types: Option<&[&str]>) -> Option<Vec<RelationshipType>> {
        rel_types.and_then(|types| {
            let ids: Vec<RelationshipType> = types
                .iter()
                .filter_map(|s| self.relationship_type_from_str(s))
                .collect();
            if ids.is_empty() { None } else { Some(ids) }
        })
    }

    /// Generic neighbor lookup from a CSR direction (offsets, nbrs, types).
    fn neighbors_by_type_from_csr(
        offsets: &[u32],
        nbrs: &[NodeId],
        types: &[RelationshipType],
        node_id: NodeId,
        allowed_types: Option<&std::collections::HashSet<RelationshipType>>,
    ) -> Vec<NodeId> {
        if node_id as usize >= offsets.len().saturating_sub(1) {
            return Vec::new();
        }
        let start = offsets[node_id as usize] as usize;
        let end = offsets[node_id as usize + 1] as usize;
        match allowed_types {
            None => nbrs[start..end].to_vec(),
            Some(allowed) => nbrs[start..end].iter()
                .zip(types[start..end].iter())
                .filter_map(|(&nbr, &rt)| if allowed.contains(&rt) { Some(nbr) } else { None })
                .collect(),
        }
    }

    /// Generic neighbor-with-positions lookup from a CSR direction.
    fn neighbors_with_positions_from_csr(
        offsets: &[u32],
        nbrs: &[NodeId],
        types: &[RelationshipType],
        node_id: NodeId,
        allowed_types: Option<&std::collections::HashSet<RelationshipType>>,
    ) -> Vec<(NodeId, RelationshipType, u32)> {
        if node_id as usize >= offsets.len().saturating_sub(1) {
            return Vec::new();
        }
        let start = offsets[node_id as usize] as usize;
        let end = offsets[node_id as usize + 1] as usize;
        let iter = nbrs[start..end].iter()
            .zip(types[start..end].iter())
            .enumerate()
            .map(|(idx, (&nbr, &rt))| (nbr, rt, (start + idx) as u32));
        match allowed_types {
            None => iter.collect(),
            Some(allowed) => iter.filter(|(_, rt, _)| allowed.contains(rt)).collect(),
        }
    }

    /// Get neighbors for a direction using a pre-computed allowed-types set.
    fn get_neighbors_for_direction(
        &self,
        node_id: NodeId,
        direction: Direction,
        allowed_types: &Option<std::collections::HashSet<RelationshipType>>,
    ) -> Vec<NodeId> {
        let at = allowed_types.as_ref();
        match direction {
            Direction::Outgoing => Self::neighbors_by_type_from_csr(
                &self.out_offsets, &self.out_nbrs, &self.out_types, node_id, at,
            ),
            Direction::Incoming => Self::neighbors_by_type_from_csr(
                &self.in_offsets, &self.in_nbrs, &self.in_types, node_id, at,
            ),
            Direction::Both => {
                let mut both = Self::neighbors_by_type_from_csr(
                    &self.out_offsets, &self.out_nbrs, &self.out_types, node_id, at,
                );
                both.extend(Self::neighbors_by_type_from_csr(
                    &self.in_offsets, &self.in_nbrs, &self.in_types, node_id, at,
                ));
                both
            }
        }
    }

    /// Get neighbors with CSR positions for a direction using a pre-computed allowed-types set.
    fn get_neighbors_with_positions_for_direction(
        &self,
        node_id: NodeId,
        direction: Direction,
        allowed_types: &Option<std::collections::HashSet<RelationshipType>>,
    ) -> Vec<(NodeId, RelationshipType, u32)> {
        let at = allowed_types.as_ref();
        match direction {
            Direction::Outgoing => Self::neighbors_with_positions_from_csr(
                &self.out_offsets, &self.out_nbrs, &self.out_types, node_id, at,
            ),
            Direction::Incoming => Self::neighbors_with_positions_from_csr(
                &self.in_offsets, &self.in_nbrs, &self.in_types, node_id, at,
            ),
            Direction::Both => {
                let mut both = Self::neighbors_with_positions_from_csr(
                    &self.out_offsets, &self.out_nbrs, &self.out_types, node_id, at,
                );
                both.extend(Self::neighbors_with_positions_from_csr(
                    &self.in_offsets, &self.in_nbrs, &self.in_types, node_id, at,
                ));
                both
            }
        }
    }

    /// Get the reversed direction for backward BFS passes
    fn reverse_direction(direction: Direction) -> Direction {
        match direction {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
            Direction::Both => Direction::Both,
        }
    }

    /// Get outgoing neighbors of a node (CSR format)
    #[inline]
    pub fn out_neighbors(&self, node_id: NodeId) -> &[NodeId] {
        if node_id as usize >= self.out_offsets.len().saturating_sub(1) {
            return &[];
        }
        let start = self.out_offsets[node_id as usize] as usize;
        let end = self.out_offsets[node_id as usize + 1] as usize;
        &self.out_nbrs[start..end]
    }

    /// Get incoming neighbors of a node (CSR format)
    #[inline]
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
    /// use rustychickpeas_core::GraphBuilder;
    /// 
    /// // Create a graph
    /// let mut builder = GraphBuilder::new(Some(10), Some(10));
    /// builder.add_node(Some(0), &["Person"]).unwrap();
    /// builder.add_node(Some(1), &["Person"]).unwrap();
    /// builder.add_node(Some(2), &["Company"]).unwrap();
    /// builder.add_rel(0, 1, "KNOWS").unwrap();
    /// builder.add_rel(0, 2, "WORKS_FOR").unwrap();
    /// let snapshot = builder.finalize(None);
    ///
    /// // Get neighbors connected via "KNOWS" relationships
    /// let neighbors = snapshot.out_neighbors_by_type(0, &["KNOWS"]);
    ///
    /// // Get neighbors connected via multiple relationship types
    /// let neighbors = snapshot.out_neighbors_by_type(0, &["KNOWS", "WORKS_FOR"]);
    /// ```
    pub fn out_neighbors_by_type(&self, node_id: NodeId, rel_types: &[&str]) -> Vec<NodeId> {
        let ids: Vec<RelationshipType> = rel_types.iter()
            .filter_map(|s| self.relationship_type_from_str(s))
            .collect();
        let allowed: Option<std::collections::HashSet<RelationshipType>> =
            if ids.is_empty() { None } else { Some(ids.into_iter().collect()) };
        Self::neighbors_by_type_from_csr(
            &self.out_offsets, &self.out_nbrs, &self.out_types, node_id, allowed.as_ref(),
        )
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
    /// use rustychickpeas_core::GraphBuilder;
    /// 
    /// // Create a graph
    /// let mut builder = GraphBuilder::new(Some(10), Some(10));
    /// builder.add_node(Some(0), &["Person"]).unwrap();
    /// builder.add_node(Some(1), &["Person"]).unwrap();
    /// builder.add_node(Some(2), &["Company"]).unwrap();
    /// builder.add_rel(1, 0, "KNOWS").unwrap();
    /// builder.add_rel(2, 0, "WORKS_FOR").unwrap();
    /// let snapshot = builder.finalize(None);
    ///
    /// // Get neighbors connected via "KNOWS" relationships
    /// let neighbors = snapshot.in_neighbors_by_type(0, &["KNOWS"]);
    ///
    /// // Get neighbors connected via multiple relationship types
    /// let neighbors = snapshot.in_neighbors_by_type(0, &["KNOWS", "WORKS_FOR"]);
    /// ```
    pub fn in_neighbors_by_type(&self, node_id: NodeId, rel_types: &[&str]) -> Vec<NodeId> {
        let ids: Vec<RelationshipType> = rel_types.iter()
            .filter_map(|s| self.relationship_type_from_str(s))
            .collect();
        let allowed: Option<std::collections::HashSet<RelationshipType>> =
            if ids.is_empty() { None } else { Some(ids.into_iter().collect()) };
        Self::neighbors_by_type_from_csr(
            &self.in_offsets, &self.in_nbrs, &self.in_types, node_id, allowed.as_ref(),
        )
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
    /// use rustychickpeas_core::{GraphBuilder, GraphSnapshot};
    /// use rustychickpeas_core::types::Direction;
    /// 
    /// // Create a simple graph
    /// let mut builder = GraphBuilder::new(Some(10), Some(10));
    /// builder.add_node(Some(5), &["Person"]).unwrap();
    /// builder.add_node(Some(10), &["Person"]).unwrap();
    /// builder.add_rel(5, 10, "KNOWS").unwrap();
    /// let snapshot = builder.finalize(None);
    /// 
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
        direction: Direction,
        rel_types: Option<&[&str]>,
        max_depth: Option<u32>,
    ) -> bool {
        if from == to {
            return true;
        }

        // Pre-compute HashSet once for the entire BFS
        let allowed_types: Option<std::collections::HashSet<RelationshipType>> =
            self.resolve_rel_types(rel_types).map(|ids| ids.into_iter().collect());

        let mut queue = std::collections::VecDeque::new();
        queue.push_back((from, 0u32));
        let mut visited = RoaringBitmap::new();
        visited.insert(from);

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(max) = max_depth {
                if depth >= max {
                    continue;
                }
            }

            for neighbor in self.get_neighbors_for_direction(current, direction, &allowed_types) {
                if neighbor == to {
                    return true;
                }
                if visited.insert(neighbor) {
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        false
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
    /// use rustychickpeas_core::{GraphBuilder, GraphSnapshot};
    /// use rustychickpeas_core::bitmap::NodeSet;
    /// use rustychickpeas_core::types::Direction;
    /// use roaring::RoaringBitmap;
    /// 
    /// // Create a simple graph
    /// let mut builder = GraphBuilder::new(Some(20), Some(20));
    /// builder.add_node(Some(0), &["Person"]).unwrap();
    /// builder.add_node(Some(1), &["Person"]).unwrap();
    /// builder.add_node(Some(10), &["Person"]).unwrap();
    /// builder.add_node(Some(11), &["Person"]).unwrap();
    /// builder.add_rel(0, 10, "KNOWS").unwrap();
    /// builder.add_rel(1, 11, "WORKS_WITH").unwrap();
    /// let snapshot = builder.finalize(None);
    /// 
    /// // Simple bidirectional search (default: Outgoing for forward, Incoming for backward)
    /// let source = NodeSet::new(RoaringBitmap::from_iter([0, 1]));
    /// let target = NodeSet::new(RoaringBitmap::from_iter([10, 11]));
    /// type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
    /// type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
    /// let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
    ///     &source, &target, Direction::Outgoing, None, None, None, None
    /// );
    /// 
    /// // With relationship type filter
    /// let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
    ///     &source, &target, Direction::Outgoing, Some(&["KNOWS", "WORKS_WITH"]), None, None, None
    /// );
    /// 
    /// // Bidirectional traversal (both directions)
    /// let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
    ///     &source, &target, Direction::Both, None, None, None, None
    /// );
    /// ```
    /// BFS expand step shared by forward and backward passes of bidirectional_bfs.
    fn bfs_expand_step<NF, RF>(
        &self,
        queue: &mut std::collections::VecDeque<(NodeId, u32)>,
        visited_nodes: &mut RoaringBitmap,
        visited_rels: &mut RoaringBitmap,
        other_visited_nodes: &RoaringBitmap,
        direction: Direction,
        allowed_types: &Option<std::collections::HashSet<RelationshipType>>,
        node_filter: &Option<NF>,
        rel_filter: &Option<RF>,
        max_depth: Option<u32>,
        is_backward: bool,
    ) where
        NF: Fn(NodeId, &GraphSnapshot) -> bool,
        RF: Fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool,
    {
        if queue.is_empty() {
            return;
        }
        let current_level_size = queue.len();
        for _ in 0..current_level_size {
            let (current, depth) = queue.pop_front().unwrap();
            if let Some(max) = max_depth {
                if depth >= max {
                    continue;
                }
            }
            let neighbors = self.get_neighbors_with_positions_for_direction(current, direction, allowed_types);
            for (neighbor, rel_type, csr_pos) in neighbors {
                if let Some(ref rf) = rel_filter {
                    let (from, to) = if is_backward { (neighbor, current) } else { (current, neighbor) };
                    if !rf(from, to, rel_type, csr_pos, self) {
                        continue;
                    }
                }
                if other_visited_nodes.contains(neighbor) {
                    visited_rels.insert(csr_pos);
                    if visited_nodes.insert(neighbor) {
                        if node_filter.as_ref().map_or(true, |f| f(neighbor, self)) {
                            queue.push_back((neighbor, depth + 1));
                        }
                    }
                } else if node_filter.as_ref().map_or(true, |f| f(neighbor, self)) {
                    if visited_nodes.insert(neighbor) {
                        visited_rels.insert(csr_pos);
                        queue.push_back((neighbor, depth + 1));
                    }
                }
            }
        }
    }

    pub fn bidirectional_bfs<NF, RF>(
        &self,
        source_nodes: &NodeSet,
        target_nodes: &NodeSet,
        direction: Direction,
        rel_types: Option<&[&str]>,
        node_filter: Option<NF>,
        rel_filter: Option<RF>,
        max_depth: Option<u32>,
    ) -> (RoaringBitmap, RoaringBitmap)
    where
        NF: Fn(NodeId, &GraphSnapshot) -> bool,
        RF: Fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool,
    {
        // Pre-compute allowed types HashSet once for the entire BFS
        let allowed_types: Option<std::collections::HashSet<RelationshipType>> =
            self.resolve_rel_types(rel_types).map(|ids| ids.into_iter().collect());

        let mut forward_visited_nodes = RoaringBitmap::new();
        let mut forward_visited_rels = RoaringBitmap::new();
        let mut backward_visited_nodes = RoaringBitmap::new();
        let mut backward_visited_rels = RoaringBitmap::new();
        let mut forward_queue = std::collections::VecDeque::new();
        let mut backward_queue = std::collections::VecDeque::new();

        for node_id in source_nodes.iter() {
            if node_filter.as_ref().map_or(true, |f| f(node_id, self)) {
                forward_visited_nodes.insert(node_id);
                forward_queue.push_back((node_id, 0u32));
            }
        }

        for node_id in target_nodes.iter() {
            if node_filter.as_ref().map_or(true, |f| f(node_id, self)) {
                backward_visited_nodes.insert(node_id);
                backward_queue.push_back((node_id, 0u32));
            }
        }

        let intersection = &forward_visited_nodes & &backward_visited_nodes;
        if !intersection.is_empty() {
            return (intersection, RoaringBitmap::new());
        }

        let backward_direction = Self::reverse_direction(direction);

        while !forward_queue.is_empty() || !backward_queue.is_empty() {
            self.bfs_expand_step(
                &mut forward_queue, &mut forward_visited_nodes, &mut forward_visited_rels,
                &backward_visited_nodes, direction, &allowed_types,
                &node_filter, &rel_filter, max_depth, false,
            );
            self.bfs_expand_step(
                &mut backward_queue, &mut backward_visited_nodes, &mut backward_visited_rels,
                &forward_visited_nodes, backward_direction, &allowed_types,
                &node_filter, &rel_filter, max_depth, true,
            );
        }

        let intersection_nodes = &forward_visited_nodes & &backward_visited_nodes;
        if intersection_nodes.is_empty() {
            return (RoaringBitmap::new(), RoaringBitmap::new());
        }

        // NOTE: union_rels includes all relationships visited from both sides,
        // not just those connecting nodes in the intersection. Filtering to only
        // path-relevant relationships would require path reconstruction.
        let union_rels = &forward_visited_rels | &backward_visited_rels;
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
    /// use rustychickpeas_core::{GraphBuilder, GraphSnapshot};
    /// use rustychickpeas_core::bitmap::NodeSet;
    /// use rustychickpeas_core::types::{Direction, NodeId};
    /// use roaring::RoaringBitmap;
    /// 
    /// // Create a simple graph
    /// let mut builder = GraphBuilder::new(Some(10), Some(10));
    /// builder.add_node(Some(0), &["Person"]).unwrap();
    /// builder.add_node(Some(1), &["Person"]).unwrap();
    /// builder.add_node(Some(2), &["Company"]).unwrap();
    /// builder.add_rel(0, 1, "KNOWS").unwrap();
    /// builder.add_rel(0, 2, "WORKS_FOR").unwrap();
    /// let snapshot = builder.finalize(None);
    ///
    /// // Simple BFS from a single node
    /// let start = NodeSet::new(RoaringBitmap::from_iter([0]));
    /// type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
    /// type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
    /// let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
    ///     &start, Direction::Outgoing, None, None, None, None
    /// );
    /// 
    /// // BFS with relationship type filter
    /// let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
    ///     &start, Direction::Outgoing, Some(&["KNOWS", "WORKS_WITH"]), None, None, None
    /// );
    /// 
    /// // BFS with max depth
    /// let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
    ///     &start, Direction::Outgoing, None, None, None, Some(3)
    /// );
    /// 
    /// // BFS with node filter (only "Person" nodes)
    /// let node_filter = |node_id: NodeId, snapshot: &GraphSnapshot| -> bool {
    ///     snapshot.nodes_with_label("Person")
    ///         .map_or(false, |nodes| nodes.contains(node_id))
    /// };
    /// let (nodes, rels) = snapshot.bfs(
    ///     &start, Direction::Outgoing, None, Some(&node_filter), None::<RelFilter>, None
    /// );
    /// ```
    pub fn bfs<NF, RF>(
        &self,
        start_nodes: &NodeSet,
        direction: Direction,
        rel_types: Option<&[&str]>,
        node_filter: Option<NF>,
        rel_filter: Option<RF>,
        max_depth: Option<u32>,
    ) -> (RoaringBitmap, RoaringBitmap)
    where
        NF: Fn(NodeId, &GraphSnapshot) -> bool,
        RF: Fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool,
    {
        // Pre-compute allowed types HashSet once for the entire BFS
        let allowed_types: Option<std::collections::HashSet<RelationshipType>> =
            self.resolve_rel_types(rel_types).map(|ids| ids.into_iter().collect());

        let mut queue = std::collections::VecDeque::new();
        let mut visited_nodes = RoaringBitmap::new();
        let mut visited_rels = RoaringBitmap::new();

        for node_id in start_nodes.iter() {
            if node_filter.as_ref().map_or(true, |f| f(node_id, self)) {
                if visited_nodes.insert(node_id) {
                    queue.push_back((node_id, 0u32));
                }
            }
        }

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(max) = max_depth {
                if depth >= max {
                    continue;
                }
            }

            let neighbors = self.get_neighbors_with_positions_for_direction(current, direction, &allowed_types);

            for (neighbor, rel_type, csr_pos) in neighbors {
                if let Some(ref rf) = rel_filter {
                    if !rf(current, neighbor, rel_type, csr_pos, self) {
                        continue;
                    }
                }

                if node_filter.as_ref().map_or(true, |f| f(neighbor, self)) {
                    visited_rels.insert(csr_pos);
                    if visited_nodes.insert(neighbor) {
                        queue.push_back((neighbor, depth + 1));
                    }
                }
            }
        }

        (visited_nodes, visited_rels)
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
    /// use rustychickpeas_core::GraphBuilder;
    /// 
    /// // Create a graph with properties
    /// let mut builder = GraphBuilder::new(Some(10), Some(10));
    /// builder.add_node(Some(0), &["Person"]).unwrap();
    /// builder.set_prop_str(0, "name", "Alice").unwrap();
    /// builder.set_prop_i64(0, "age", 30).unwrap();
    /// builder.set_prop_bool(0, "active", true).unwrap();
    /// let snapshot = builder.finalize(None);
    /// 
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

    /// Get nodes with a specific property value (internal ID-based version).
    /// Uses check-release-build-reacquire pattern to avoid holding the mutex during index build.
    fn nodes_with_property_id(&self, label: Label, key: PropertyKey, value: ValueId) -> Option<NodeSet> {
        let index_key = (label, key);

        // Check if the index already exists (short lock)
        {
            let index = self.prop_index.lock().unwrap();
            if let Some(key_index) = index.get(&index_key) {
                return key_index.get(&value).cloned();
            }
        }
        // Release lock, build index outside the lock
        let label_nodes = self.nodes_with_label_id(label)?;
        let column = self.columns.get(&key)?;
        let key_index_final = Self::build_property_index_for_key_and_label(column, Some(label_nodes));

        // Re-acquire lock and insert (another thread may have built it first)
        let mut index = self.prop_index.lock().unwrap();
        let entry = index.entry(index_key).or_insert(key_index_final);
        entry.get(&value).cloned()
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
        builder.add_node(Some(0), &["Person"]).unwrap();
        builder.add_node(Some(1), &["Person"]).unwrap();
        builder.add_node(Some(2), &["Company"]).unwrap();
        builder.add_rel(0, 1, "KNOWS").unwrap();
        builder.add_rel(1, 2, "WORKS_FOR").unwrap();
        builder.set_prop_str(0, "name", "Alice").unwrap();
        builder.set_prop_i64(0, "age", 30).unwrap();

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
        assert!(snapshot.can_reach(0, 1, Direction::Outgoing, None, None));
        assert!(snapshot.can_reach(1, 2, Direction::Outgoing, None, None));
        
        // Multi-hop
        assert!(snapshot.can_reach(0, 2, Direction::Outgoing, None, None));
        
        // Reverse direction (should not work for outgoing)
        assert!(!snapshot.can_reach(2, 0, Direction::Outgoing, None, None));
        assert!(!snapshot.can_reach(1, 0, Direction::Outgoing, None, None));
        
        // Same node
        assert!(snapshot.can_reach(0, 0, Direction::Outgoing, None, None));
    }

    #[test]
    fn test_can_reach_with_rel_type_filter() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 (KNOWS), 1 -> 2 (WORKS_FOR)
        
        // Can reach via KNOWS
        assert!(snapshot.can_reach(0, 1, Direction::Outgoing, Some(&["KNOWS"]), None));
        
        // Cannot reach 2 via KNOWS only (need WORKS_FOR)
        assert!(!snapshot.can_reach(0, 2, Direction::Outgoing, Some(&["KNOWS"]), None));
        
        // Can reach 2 via both types
        assert!(snapshot.can_reach(0, 2, Direction::Outgoing, Some(&["KNOWS", "WORKS_FOR"]), None));
        
        // Can reach 2 via WORKS_FOR only (from node 1)
        assert!(snapshot.can_reach(1, 2, Direction::Outgoing, Some(&["WORKS_FOR"]), None));
    }

    #[test]
    fn test_can_reach_with_max_depth() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 -> 2
        
        // Can reach with depth 2
        assert!(snapshot.can_reach(0, 2, Direction::Outgoing, None, Some(2)));
        
        // Cannot reach with depth 1 (only one hop allowed)
        assert!(!snapshot.can_reach(0, 2, Direction::Outgoing, None, Some(1)));
        
        // Can reach direct neighbor with depth 1
        assert!(snapshot.can_reach(0, 1, Direction::Outgoing, None, Some(1)));
    }

    #[test]
    fn test_can_reach_direction_both() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 -> 2
        
        // With Direction::Both, can traverse in both directions
        // From 2, can reach 1 via incoming edge
        assert!(snapshot.can_reach(2, 1, Direction::Both, None, None));
        
        // From 2, can reach 0 via incoming edges (2 <- 1 <- 0)
        assert!(snapshot.can_reach(2, 0, Direction::Both, None, None));
        
        // From 0, can still reach 2 via outgoing (0 -> 1 -> 2)
        assert!(snapshot.can_reach(0, 2, Direction::Both, None, None));
    }

    #[test]
    fn test_can_reach_unreachable() {
        let snapshot = create_test_snapshot();
        // Graph: 0 -> 1 -> 2
        
        // Node 999 doesn't exist, so unreachable
        assert!(!snapshot.can_reach(0, 999, Direction::Outgoing, None, None));
        assert!(!snapshot.can_reach(999, 0, Direction::Outgoing, None, None));
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
        builder.add_node(Some(0), &["Person"]).unwrap();
        builder.add_node(Some(1), &["Person"]).unwrap();
        builder.add_node(Some(2), &["Company"]).unwrap();
        builder.add_node(Some(3), &["Company"]).unwrap();

        // Set same property key "name" on all nodes
        builder.set_prop_str(0, "name", "Alice").unwrap();
        builder.set_prop_str(1, "name", "Bob").unwrap();
        builder.set_prop_str(2, "name", "Acme Corp").unwrap();
        builder.set_prop_str(3, "name", "Tech Inc").unwrap();
        
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
        
        builder.add_node(Some(0), &["Person"]).unwrap();
        builder.add_node(Some(1), &["Person"]).unwrap();
        builder.add_node(Some(2), &["Person"]).unwrap();
        builder.add_node(Some(3), &["Person"]).unwrap();

        // Set same age for multiple nodes
        builder.set_prop_i64(0, "age", 30).unwrap();
        builder.set_prop_i64(1, "age", 30).unwrap();
        builder.set_prop_i64(2, "age", 25).unwrap();
        builder.set_prop_i64(3, "age", 30).unwrap();
        
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
        
        builder.add_node(Some(0), &["Person"]).unwrap();
        builder.add_node(Some(1), &["Person"]).unwrap();
        builder.add_node(Some(2), &["Person"]).unwrap();
        builder.add_node(Some(3), &["Person"]).unwrap();

        builder.set_prop_str(0, "name", "Alice").unwrap();
        builder.set_prop_i64(1, "age", 30).unwrap();
        builder.set_prop_f64(2, "score", 95.5).unwrap();
        builder.set_prop_bool(3, "active", true).unwrap();
        
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
        builder.add_node(Some(0), &["Person"]).unwrap();

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
        builder.add_node(Some(0), &["Person"]).unwrap();
        builder.add_node(Some(1), &["Person"]).unwrap();
        builder.add_node(Some(2), &["Person"]).unwrap();
        builder.add_node(Some(3), &["Person"]).unwrap();
        builder.add_node(Some(4), &["Person"]).unwrap();
        builder.add_rel(0, 1, "KNOWS").unwrap();
        builder.add_rel(1, 2, "KNOWS").unwrap();
        builder.add_rel(2, 3, "KNOWS").unwrap();
        builder.add_rel(0, 4, "KNOWS").unwrap();
        builder.add_rel(4, 3, "KNOWS").unwrap();
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
        let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(&source, &target, Direction::Outgoing, None, None, None, None);
        
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
        let (nodes, _rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(&source, &target, Direction::Outgoing, None, None, None, None);
        
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
            Direction::Outgoing,
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
            Direction::Outgoing,
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
            Direction::Outgoing,
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
            Direction::Outgoing,
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
        let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(&source, &target, Direction::Outgoing, None, None, None, None);
        
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
        builder.add_node(Some(0), &["Person"]).unwrap();
        builder.add_node(Some(1), &["Person"]).unwrap();
        builder.add_node(Some(2), &["Person"]).unwrap();
        builder.add_node(Some(3), &["Person"]).unwrap();
        builder.add_node(Some(4), &["Person"]).unwrap();
        builder.add_node(Some(5), &["Company"]).unwrap();
        builder.add_rel(0, 1, "KNOWS").unwrap();
        builder.add_rel(1, 2, "KNOWS").unwrap();
        builder.add_rel(2, 3, "KNOWS").unwrap();
        builder.add_rel(0, 4, "KNOWS").unwrap();
        builder.add_rel(4, 3, "KNOWS").unwrap();
        builder.add_rel(0, 5, "WORKS_FOR").unwrap();
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
            Direction::Outgoing, 
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
            Direction::Outgoing,
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
            Direction::Outgoing,
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
            Direction::Outgoing,
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
            Direction::Outgoing,
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
            Direction::Incoming, 
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
            Direction::Both, 
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
            Direction::Outgoing, 
            None, None, None, None
        );
        
        // Should find nodes reachable from either start node
        assert!(nodes.contains(0));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3)); // Reachable from both
        assert!(nodes.contains(1)); // Reachable from 0
    }

}

