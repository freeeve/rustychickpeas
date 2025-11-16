//! Immutable graph snapshot optimized for read-only queries
//!
//! GraphSnapshot uses CSR (Compressed Sparse Row) format for adjacency
//! and columnar storage for properties, providing maximum query performance.

use crate::bitmap::NodeSet;
use crate::types::{Label, NodeId, PropertyKey, RelationshipType};
use hashbrown::HashMap;
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
}

impl Atoms {
    pub fn new(strings: Vec<String>) -> Self {
        Atoms { strings }
    }

    /// Resolve an interned string ID to a string
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(|s| s.as_str())
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
    /// Column registry: property key -> column storage
    pub columns: HashMap<PropertyKey, Column>,

    // --- Inverted property index (lazy-initialized) ---
    /// Lazy-initialized inverted index: property_key -> (value_id -> nodes with that property value)
    /// Indexes are built on first access to avoid memory overhead for unused properties
    pub prop_index: Mutex<HashMap<PropertyKey, HashMap<ValueId, NodeSet>>>,

    // --- String tables ---
    /// Flattened string interner
    pub atoms: Atoms,
}

impl GraphSnapshot {
    /// Build the property index for a specific key by scanning the column
    /// This is used both during finalization (for pre-indexed properties) and lazily on first access
    fn build_property_index_for_key(column: &Column) -> HashMap<ValueId, NodeSet> {
        use roaring::RoaringBitmap;
        let mut key_index: HashMap<ValueId, Vec<NodeId>> = HashMap::new();
        
        // Scan the column and group nodes by value
        match column {
            Column::DenseI64(col) => {
                for (node_id, &val) in col.iter().enumerate() {
                    key_index.entry(ValueId::I64(val)).or_default().push(node_id as u32);
                }
            }
            Column::DenseF64(col) => {
                for (node_id, &val) in col.iter().enumerate() {
                    key_index.entry(ValueId::from_f64(val)).or_default().push(node_id as u32);
                }
            }
            Column::DenseBool(col) => {
                for (node_id, val) in col.iter().enumerate() {
                    key_index.entry(ValueId::Bool(*val)).or_default().push(node_id as u32);
                }
            }
            Column::DenseStr(col) => {
                for (node_id, &str_id) in col.iter().enumerate() {
                    key_index.entry(ValueId::Str(str_id)).or_default().push(node_id as u32);
                }
            }
            Column::SparseI64(col) => {
                for &(node_id, val) in col.iter() {
                    key_index.entry(ValueId::I64(val)).or_default().push(node_id);
                }
            }
            Column::SparseF64(col) => {
                for &(node_id, val) in col.iter() {
                    key_index.entry(ValueId::from_f64(val)).or_default().push(node_id);
                }
            }
            Column::SparseBool(col) => {
                for &(node_id, val) in col.iter() {
                    key_index.entry(ValueId::Bool(val)).or_default().push(node_id);
                }
            }
            Column::SparseStr(col) => {
                for &(node_id, str_id) in col.iter() {
                    key_index.entry(ValueId::Str(str_id)).or_default().push(node_id);
                }
            }
        }
        
        // Convert Vec<NodeId> to NodeSet (RoaringBitmap)
        let mut key_index_final: HashMap<ValueId, NodeSet> = HashMap::new();
        for (val_id, mut node_ids) in key_index {
            node_ids.sort_unstable();
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
            prop_index: Mutex::new(HashMap::new()),
            atoms: Atoms::new(vec!["".to_string()]),
        }
    }

    /// Get outgoing neighbors of a node (CSR format)
    pub fn get_out_neighbors(&self, node_id: NodeId) -> &[NodeId] {
        if node_id as usize >= self.out_offsets.len().saturating_sub(1) {
            return &[];
        }
        let start = self.out_offsets[node_id as usize] as usize;
        let end = self.out_offsets[node_id as usize + 1] as usize;
        &self.out_nbrs[start..end]
    }

    /// Get incoming neighbors of a node (CSR format)
    pub fn get_in_neighbors(&self, node_id: NodeId) -> &[NodeId] {
        if node_id as usize >= self.in_offsets.len().saturating_sub(1) {
            return &[];
        }
        let start = self.in_offsets[node_id as usize] as usize;
        let end = self.in_offsets[node_id as usize + 1] as usize;
        &self.in_nbrs[start..end]
    }

    /// Get outgoing neighbors filtered by relationship types
    /// Returns only neighbors connected via relationships of the specified types
    pub fn get_out_neighbors_by_type(&self, node_id: NodeId, rel_types: &[RelationshipType]) -> Vec<NodeId> {
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
    pub fn get_in_neighbors_by_type(&self, node_id: NodeId, rel_types: &[RelationshipType]) -> Vec<NodeId> {
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

    /// Get property value for a node
    pub fn get_property(&self, node_id: NodeId, key: PropertyKey) -> Option<ValueId> {
        self.columns.get(&key)?.get(node_id)
    }

    /// Get nodes with a specific label
    pub fn get_nodes_with_label(&self, label: Label) -> Option<&NodeSet> {
        self.label_index.get(&label)
    }

    /// Get relationships with a specific type
    pub fn get_rels_with_type(&self, rel_type: RelationshipType) -> Option<&NodeSet> {
        self.type_index.get(&rel_type)
    }

    /// Get nodes with a specific property value
    /// 
    /// This method lazily builds the index for the property key on first access.
    /// The index is built by scanning the column and grouping nodes by value.
    pub fn get_nodes_with_property(&self, key: PropertyKey, value: ValueId) -> Option<NodeSet> {
        // Lock the index
        let mut index = self.prop_index.lock().unwrap();
        
        // Check if index for this key already exists
        if !index.contains_key(&key) {
            // Build index for this key by scanning the column
            if let Some(column) = self.columns.get(&key) {
                let key_index_final = Self::build_property_index_for_key(column);
                // Store the index for this key
                index.insert(key, key_index_final);
            } else {
                // Column doesn't exist, return None
                return None;
            }
        }
        
        // Look up the value in the index
        index.get(&key)?.get(&value).cloned()
    }

    /// Get the version of this snapshot
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Resolve an interned string ID to a string
    pub fn resolve_string(&self, id: u32) -> Option<&str> {
        self.atoms.resolve(id)
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
        crate::builder_parquet::read_from_parquet(
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
    use crate::builder::GraphBuilder;
    use crate::types::Label;

    // Helper to create a simple snapshot for testing
    // This works around the into_vec() issue by manually creating atoms
    fn create_test_snapshot() -> GraphSnapshot {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(1, &["Person"]);
        builder.add_node(2, &["Person"]);
        builder.add_node(3, &["Company"]);
        builder.add_rel(1, 2, "KNOWS");
        builder.add_rel(2, 3, "WORKS_FOR");
        builder.set_prop_str(1, "name", "Alice");
        builder.set_prop_i64(1, "age", 30);
        
        // Manually create snapshot to avoid into_vec() issue
        let n = builder.next_id as usize;
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
                let bitmap = RoaringBitmap::from_sorted_iter(rel_ids.into_iter()).unwrap();
                (rel_type, NodeSet::new(bitmap))
            })
            .collect();
        
        // Build property columns (sparse for small test)
        let mut columns: HashMap<PropertyKey, Column> = HashMap::new();
        let name_key = builder.interner.get_or_intern("name");
        let age_key = builder.interner.get_or_intern("age");
        
        if let Some(pairs) = builder.col_str.get(&name_key) {
            let mut pairs = pairs.clone();
            pairs.sort_unstable_by_key(|(id, _)| *id);
            columns.insert(name_key, Column::SparseStr(pairs));
        }
        if let Some(pairs) = builder.col_i64.get(&age_key) {
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
        let neighbors = snapshot.get_out_neighbors(0);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 1);
        
        let neighbors = snapshot.get_out_neighbors(1);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 2);
        
        let neighbors = snapshot.get_out_neighbors(2);
        assert_eq!(neighbors.len(), 0);
    }

    #[test]
    fn test_get_in_neighbors() {
        let snapshot = create_test_snapshot();
        let neighbors = snapshot.get_in_neighbors(0);
        assert_eq!(neighbors.len(), 0);
        
        let neighbors = snapshot.get_in_neighbors(1);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 0);
        
        let neighbors = snapshot.get_in_neighbors(2);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 1);
    }

    #[test]
    fn test_get_out_neighbors_invalid() {
        let snapshot = create_test_snapshot();
        let neighbors = snapshot.get_out_neighbors(999);
        assert_eq!(neighbors.len(), 0);
    }

    #[test]
    fn test_get_in_neighbors_invalid() {
        let snapshot = create_test_snapshot();
        let neighbors = snapshot.get_in_neighbors(999);
        assert_eq!(neighbors.len(), 0);
    }

    #[test]
    fn test_get_nodes_with_label() {
        let snapshot = create_test_snapshot();
        // Find Person label ID
        let person_id = snapshot.atoms.strings.iter()
            .position(|s| s == "Person")
            .unwrap() as u32;
        let person_label = Label::new(person_id);
        let nodes = snapshot.get_nodes_with_label(person_label);
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
    }

    #[test]
    fn test_get_rels_with_type() {
        let snapshot = create_test_snapshot();
        // Find KNOWS type ID
        let knows_id = snapshot.atoms.strings.iter()
            .position(|s| s == "KNOWS")
            .unwrap() as u32;
        let knows_type = RelationshipType::new(knows_id);
        let rels = snapshot.get_rels_with_type(knows_type);
        assert!(rels.is_some());
        let rels = rels.unwrap();
        assert_eq!(rels.len(), 1);
    }

    #[test]
    fn test_get_property() {
        let snapshot = create_test_snapshot();
        // Find the name key by resolving strings
        let name_key = snapshot.atoms.strings.iter()
            .position(|s| s == "name")
            .unwrap() as u32;
        let prop = snapshot.get_property(0, name_key);
        assert!(prop.is_some());
        // Find Alice string ID
        let alice_id = snapshot.atoms.strings.iter()
            .position(|s| s == "Alice")
            .unwrap() as u32;
        assert_eq!(prop, Some(ValueId::Str(alice_id)));
    }

    #[test]
    fn test_get_property_nonexistent() {
        let snapshot = create_test_snapshot();
        let prop = snapshot.get_property(999, 999);
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
        let name_key = snapshot.atoms.strings.iter()
            .position(|s| s == "name")
            .unwrap() as u32;
        let alice_id = snapshot.atoms.strings.iter()
            .position(|s| s == "Alice")
            .unwrap() as u32;
        
        // First access should build the index lazily
        let nodes = snapshot.get_nodes_with_property(name_key, ValueId::Str(alice_id));
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes.contains(0));
    }

    #[test]
    fn test_get_nodes_with_property_nonexistent_key() {
        let snapshot = create_test_snapshot();
        let nodes = snapshot.get_nodes_with_property(999, ValueId::I64(1));
        assert!(nodes.is_none());
    }

    #[test]
    fn test_get_nodes_with_property_nonexistent_value() {
        let snapshot = create_test_snapshot();
        let name_key = snapshot.atoms.strings.iter()
            .position(|s| s == "name")
            .unwrap() as u32;
        let nodes = snapshot.get_nodes_with_property(name_key, ValueId::Str(999));
        assert!(nodes.is_none());
    }

    #[test]
    fn test_get_nodes_with_label_nonexistent() {
        let snapshot = create_test_snapshot();
        let fake_label = Label::new(999);
        let nodes = snapshot.get_nodes_with_label(fake_label);
        assert!(nodes.is_none());
    }

    #[test]
    fn test_get_rels_with_type_nonexistent() {
        let snapshot = create_test_snapshot();
        let fake_type = RelationshipType::new(999);
        let rels = snapshot.get_rels_with_type(fake_type);
        assert!(rels.is_none());
    }

    #[test]
    fn test_snapshot_with_version() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.set_version("v1.0");
        builder.add_node(1, &["Person"]);
        
        // Manually create snapshot with version
        let n = builder.next_id as usize;
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

}

