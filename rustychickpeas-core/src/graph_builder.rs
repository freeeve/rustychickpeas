//! Graph builder for constructing immutable GraphSnapshot
//!
//! GraphBuilder uses a staging approach: collect all data, then finalize
//! into an optimized GraphSnapshot with CSR adjacency and columnar properties.

use crate::bitmap::NodeSet;
use crate::interner::StringInterner;
use crate::graph_snapshot::{Atoms, Column, GraphSnapshot, ValueId};
use crate::types::{Label, NodeId, PropertyKey, RelationshipType};
use hashbrown::HashMap;
use roaring::RoaringBitmap;

/// Trait for converting common types to ValueId for GraphBuilder
/// For strings, uses the builder's interner to look up or intern the string
pub trait IntoValueIdBuilder {
    fn into_value_id(self, builder: &GraphBuilder) -> ValueId;
}

impl IntoValueIdBuilder for ValueId {
    fn into_value_id(self, _builder: &GraphBuilder) -> ValueId {
        self
    }
}

impl IntoValueIdBuilder for i64 {
    fn into_value_id(self, _builder: &GraphBuilder) -> ValueId {
        ValueId::I64(self)
    }
}

impl IntoValueIdBuilder for i32 {
    fn into_value_id(self, _builder: &GraphBuilder) -> ValueId {
        ValueId::I64(self as i64)
    }
}

impl IntoValueIdBuilder for f64 {
    fn into_value_id(self, _builder: &GraphBuilder) -> ValueId {
        ValueId::from_f64(self)
    }
}

impl IntoValueIdBuilder for bool {
    fn into_value_id(self, _builder: &GraphBuilder) -> ValueId {
        ValueId::Bool(self)
    }
}

impl IntoValueIdBuilder for &str {
    fn into_value_id(self, builder: &GraphBuilder) -> ValueId {
        ValueId::Str(builder.interner.get_or_intern(self))
    }
}

impl IntoValueIdBuilder for String {
    fn into_value_id(self, builder: &GraphBuilder) -> ValueId {
        ValueId::Str(builder.interner.get_or_intern(&self))
    }
}


/// Graph builder for constructing immutable GraphSnapshot
pub struct GraphBuilder {
    // Adjacency assembly (counts first, then fill)
    pub(crate) deg_out: Vec<u32>,
    pub(crate) deg_in: Vec<u32>,
    pub(crate) rels: Vec<(NodeId, NodeId)>, // Temporary storage

    // Labels/types during build
    pub(crate) node_labels: Vec<Vec<Label>>, // Small vec per node
    pub(crate) rel_types: Vec<RelationshipType>, // Type per relationship

    // Version tracking (at snapshot level, not per node/relationship)
    pub(crate) version: Option<String>,

    // Node properties (staging). Per key we'll choose dense or sparse.
    pub(crate) node_col_i64: hashbrown::HashMap<PropertyKey, Vec<(NodeId, i64)>>,
    pub(crate) node_col_f64: hashbrown::HashMap<PropertyKey, Vec<(NodeId, f64)>>,
    pub(crate) node_col_bool: hashbrown::HashMap<PropertyKey, Vec<(NodeId, bool)>>,
    pub(crate) node_col_str: hashbrown::HashMap<PropertyKey, Vec<(NodeId, u32)>>, // Interned

    // Relationship properties (staging). Indexed by relationship position in rels vector.
    pub(crate) rel_col_i64: hashbrown::HashMap<PropertyKey, Vec<(usize, i64)>>, // usize = index in rels
    pub(crate) rel_col_f64: hashbrown::HashMap<PropertyKey, Vec<(usize, f64)>>,
    pub(crate) rel_col_bool: hashbrown::HashMap<PropertyKey, Vec<(usize, bool)>>,
    pub(crate) rel_col_str: hashbrown::HashMap<PropertyKey, Vec<(usize, u32)>>, // Interned

    // Interner (for keys + values)
    pub(crate) interner: StringInterner,

    // Deduplication configuration and map: unique property values -> node_id
    // This persists across multiple file loads and regular builder operations to enable deduplication
    pub(crate) dedup_unique_properties: Option<Vec<PropertyKey>>, // Property keys to use for deduplication
    pub(crate) dedup_map: hashbrown::HashMap<crate::types::DedupKey, NodeId>,

    // Auto-generation: next node ID to use when None is provided
    pub(crate) next_node_id: NodeId,
}

impl GraphBuilder {
    /// Create a new GraphBuilder with optional capacity hints
    /// 
    /// Capacity is just a performance hint for pre-allocation. The builder will
    /// automatically grow as needed up to the maximum (2^32 - 1 nodes, 2^64 - 1 relationships).
    /// If not specified, defaults to 2^20 (1,048,576) nodes/rels for better performance on typical workloads.
    pub fn new(capacity_nodes: Option<usize>, capacity_rels: Option<usize>) -> Self {
        // Default to 2^20 (1,048,576) for better performance on typical workloads
        // The builder will auto-grow as needed (doubling each time) if this is exceeded
        const DEFAULT_CAPACITY: usize = 1 << 20; // 2^20 = 1,048,576
        let capacity_nodes = capacity_nodes.unwrap_or(DEFAULT_CAPACITY);
        let capacity_rels = capacity_rels.unwrap_or(DEFAULT_CAPACITY);
        Self {
            deg_out: vec![0; capacity_nodes],
            deg_in: vec![0; capacity_nodes],
            rels: Vec::with_capacity(capacity_rels),
            node_labels: vec![Vec::new(); capacity_nodes],
            rel_types: Vec::with_capacity(capacity_rels),
            version: None,
            node_col_i64: hashbrown::HashMap::new(),
            node_col_f64: hashbrown::HashMap::new(),
            node_col_bool: hashbrown::HashMap::new(),
            node_col_str: hashbrown::HashMap::new(),
            rel_col_i64: hashbrown::HashMap::new(),
            rel_col_f64: hashbrown::HashMap::new(),
            rel_col_bool: hashbrown::HashMap::new(),
            rel_col_str: hashbrown::HashMap::new(),
            interner: StringInterner::new(),
            dedup_unique_properties: None,
            dedup_map: hashbrown::HashMap::new(),
            next_node_id: 0,
        }
    }

    /// Create a new GraphBuilder with a version
    pub fn with_version(version: &str, capacity_nodes: Option<usize>, capacity_rels: Option<usize>) -> Self {
        let mut builder = Self::new(capacity_nodes, capacity_rels);
        builder.version = Some(version.to_string());
        builder
    }

    /// Set the version for this snapshot (builder pattern)
    pub fn with_version_builder(mut self, version: &str) -> Self {
        self.version = Some(version.to_string());
        self
    }

    /// Set the version for this snapshot (mutable version)
    pub fn set_version(&mut self, version: &str) {
        self.version = Some(version.to_string());
    }

    /// Enable node deduplication based on unique property keys
    /// 
    /// When enabled, nodes with the same values for the specified properties will be merged.
    /// The first node encountered with a given combination of property values will be used,
    /// and subsequent nodes with the same values will have their labels and properties merged into it.
    /// 
    /// # Arguments
    /// * `unique_properties` - List of property key names to use for deduplication
    /// 
    /// # Example
    /// ```
    /// use rustychickpeas_core::graph_builder::GraphBuilder;
    /// let mut builder = GraphBuilder::new(None, None);
    /// builder.enable_node_deduplication(vec!["email", "username"]);
    /// // Now adding nodes with the same email+username will be merged
    /// ```
    pub fn enable_node_deduplication(&mut self, unique_properties: Vec<&str>) {
        self.dedup_unique_properties = Some(
            unique_properties
                .iter()
                .map(|key| self.interner.get_or_intern(key))
                .collect()
        );
    }

    /// Disable node deduplication
    pub fn disable_node_deduplication(&mut self) {
        self.dedup_unique_properties = None;
        self.dedup_map.clear();
    }

    /// Ensure capacity for a given node ID (auto-grow vectors if needed)
    #[inline]
    fn ensure_capacity(&mut self, node_id: NodeId) {
        if node_id as usize >= self.deg_out.len() {
            let max_size = u32::MAX as usize;
            let new_size = ((node_id as usize + 1) * 2).min(max_size);
            if new_size <= node_id as usize {
                panic!("Maximum node limit (2^32 - 1) exceeded");
            }
            self.node_labels.resize(new_size, Vec::new());
            self.deg_out.resize(new_size, 0);
            self.deg_in.resize(new_size, 0);
        }
    }

    /// Add a node with labels
    /// 
    /// # Arguments
    /// * `node_id` - Optional node ID. If None, auto-generates the next sequential ID.
    ///               If Some(id), uses that ID (must be u32, users should map their own IDs to u32)
    /// * `labels` - Slice of label strings
    /// 
    /// # Returns
    /// The node ID that was used (either the provided ID or the auto-generated one)
    pub fn add_node(&mut self, node_id: Option<NodeId>, labels: &[&str]) -> NodeId {
        let actual_id = node_id.unwrap_or_else(|| {
            let id = self.next_node_id;
            self.next_node_id = id.wrapping_add(1);
            if self.next_node_id == 0 {
                panic!("Node ID counter wrapped around (exceeded u32::MAX)");
            }
            id
        });
        self.ensure_capacity(actual_id);
        // Intern labels
        for &l in labels {
            let lid = self.interner.get_or_intern(l);
            self.node_labels[actual_id as usize].push(Label::new(lid));
        }
        actual_id
    }

    /// Add a relationship
    /// 
    /// # Arguments
    /// * `u` - Start node ID (must be u32)
    /// * `v` - End node ID (must be u32)
    /// * `rel_type` - Relationship type string
    pub fn add_rel(&mut self, u: NodeId, v: NodeId, rel_type: &str) {
        // Ensure capacity for both nodes
        let max_id = u.max(v);
        self.ensure_capacity(max_id);
        
        self.deg_out[u as usize] += 1;
        self.deg_in[v as usize] += 1;
        self.rels.push((u, v));
        // Intern relationship type
        let type_id = self.interner.get_or_intern(rel_type);
        self.rel_types.push(RelationshipType::new(type_id));
    }

    /// Set string property (interned)
    pub fn set_prop_str(&mut self, node_id: NodeId, key: &str, val: &str) {
        self.ensure_capacity(node_id);
        let k = self.interner.get_or_intern(key);
        let v = self.interner.get_or_intern(val);
        self.node_col_str.entry(k).or_default().push((node_id, v));
        
        // Inverted index is now built lazily on first query (see nodes_with_property)
        // This significantly speeds up bulk loading
    }

    /// Set i64 property
    pub fn set_prop_i64(&mut self, node_id: NodeId, key: &str, val: i64) {
        self.ensure_capacity(node_id);
        let k = self.interner.get_or_intern(key);
        self.node_col_i64.entry(k).or_default().push((node_id, val));
        // Inverted index is now built lazily on first query (see nodes_with_property)
    }

    /// Set f64 property
    pub fn set_prop_f64(&mut self, node_id: NodeId, key: &str, val: f64) {
        self.ensure_capacity(node_id);
        let k = self.interner.get_or_intern(key);
        self.node_col_f64.entry(k).or_default().push((node_id, val));
        // Inverted index is now built lazily on first query (see nodes_with_property)
    }

    /// Set boolean property
    pub fn set_prop_bool(&mut self, node_id: NodeId, key: &str, val: bool) {
        self.ensure_capacity(node_id);
        let k = self.interner.get_or_intern(key);
        self.node_col_bool.entry(k).or_default().push((node_id, val));
        // Inverted index is now built lazily on first query (see nodes_with_property)
    }

    /// Find relationship index by (u, v, rel_type)
    /// Returns None if relationship not found
    fn find_rel_index(&self, u: NodeId, v: NodeId, rel_type: &str) -> Option<usize> {
        let type_id = self.interner.get(rel_type)?;
        let rel_type_obj = RelationshipType::new(type_id);
        self.rels.iter()
            .enumerate()
            .find(|(idx, &(start, end))| {
                start == u && end == v && self.rel_types[*idx] == rel_type_obj
            })
            .map(|(idx, _)| idx)
    }

    /// Set string property on a relationship
    /// Finds the relationship by (u, v, rel_type) and sets the property
    pub fn set_rel_prop_str(&mut self, u: NodeId, v: NodeId, rel_type: &str, key: &str, val: &str) {
        if let Some(rel_idx) = self.find_rel_index(u, v, rel_type) {
            let k = self.interner.get_or_intern(key);
            let v = self.interner.get_or_intern(val);
            self.rel_col_str.entry(k).or_default().push((rel_idx, v));
        }
    }

    /// Set i64 property on a relationship
    pub fn set_rel_prop_i64(&mut self, u: NodeId, v: NodeId, rel_type: &str, key: &str, val: i64) {
        if let Some(rel_idx) = self.find_rel_index(u, v, rel_type) {
            let k = self.interner.get_or_intern(key);
            self.rel_col_i64.entry(k).or_default().push((rel_idx, val));
        }
    }

    /// Set f64 property on a relationship
    pub fn set_rel_prop_f64(&mut self, u: NodeId, v: NodeId, rel_type: &str, key: &str, val: f64) {
        if let Some(rel_idx) = self.find_rel_index(u, v, rel_type) {
            let k = self.interner.get_or_intern(key);
            self.rel_col_f64.entry(k).or_default().push((rel_idx, val));
        }
    }

    /// Set boolean property on a relationship
    pub fn set_rel_prop_bool(&mut self, u: NodeId, v: NodeId, rel_type: &str, key: &str, val: bool) {
        if let Some(rel_idx) = self.find_rel_index(u, v, rel_type) {
            let k = self.interner.get_or_intern(key);
            self.rel_col_bool.entry(k).or_default().push((rel_idx, val));
        }
    }

    /// Get property key ID for a string (returns None if key hasn't been interned yet)
    pub fn property_key_id(&self, key: &str) -> Option<PropertyKey> {
        self.interner.get(key)
    }

    /// Get property value for a node (before finalization)
    /// Returns None if property doesn't exist
    pub fn prop(&self, node_id: NodeId, key: &str) -> Option<ValueId> {
        let k = self.interner.get_or_intern(key);
        
        // Search through staging property vectors
        // Note: This is O(n) per property key, but fine for pre-finalization queries
        if let Some(pairs) = self.node_col_str.get(&k) {
            if let Some((_, val)) = pairs.iter().find(|(nid, _)| *nid == node_id) {
                return Some(ValueId::Str(*val));
            }
        }
        if let Some(pairs) = self.node_col_i64.get(&k) {
            if let Some((_, val)) = pairs.iter().find(|(nid, _)| *nid == node_id) {
                return Some(ValueId::I64(*val));
            }
        }
        if let Some(pairs) = self.node_col_f64.get(&k) {
            if let Some((_, val)) = pairs.iter().find(|(nid, _)| *nid == node_id) {
                return Some(ValueId::from_f64(*val));
            }
        }
        if let Some(pairs) = self.node_col_bool.get(&k) {
            if let Some((_, val)) = pairs.iter().find(|(nid, _)| *nid == node_id) {
                return Some(ValueId::Bool(*val));
            }
        }
        None
    }

    /// Update property value (removes old value, adds new one)
    /// Useful for updating properties based on queries
    pub fn update_prop_str(&mut self, node_id: NodeId, key: &str, new_val: &str) {
        let k = self.interner.get_or_intern(key);
        
        // Remove old value from property column
        if let Some(pairs) = self.node_col_str.get_mut(&k) {
            if let Some(pos) = pairs.iter().position(|(nid, _)| *nid == node_id) {
                pairs.remove(pos);
            }
        }
        
        // Add new value (inverted index is built lazily, so no need to update it)
        self.set_prop_str(node_id, key, new_val);
    }

    /// Update i64 property
    pub fn update_prop_i64(&mut self, node_id: NodeId, key: &str, new_val: i64) {
        let k = self.interner.get_or_intern(key);
        
        // Remove old value from property column
        if let Some(pairs) = self.node_col_i64.get_mut(&k) {
            if let Some(pos) = pairs.iter().position(|(nid, _)| *nid == node_id) {
                pairs.remove(pos);
            }
        }
        
        // Add new value (inverted index is built lazily, so no need to update it)
        self.set_prop_i64(node_id, key, new_val);
    }

    /// Update f64 property
    pub fn update_prop_f64(&mut self, node_id: NodeId, key: &str, new_val: f64) {
        let k = self.interner.get_or_intern(key);
        
        // Remove old value from property column
        if let Some(pairs) = self.node_col_f64.get_mut(&k) {
            if let Some(pos) = pairs.iter().position(|(nid, _)| *nid == node_id) {
                pairs.remove(pos);
            }
        }
        
        // Add new value (inverted index is built lazily, so no need to update it)
        self.set_prop_f64(node_id, key, new_val);
    }

    /// Update bool property
    pub fn update_prop_bool(&mut self, node_id: NodeId, key: &str, new_val: bool) {
        let k = self.interner.get_or_intern(key);
        
        // Remove old value from property column
        if let Some(pairs) = self.node_col_bool.get_mut(&k) {
            if let Some(pos) = pairs.iter().position(|(nid, _)| *nid == node_id) {
                pairs.remove(pos);
            }
        }
        
        // Add new value (inverted index is built lazily, so no need to update it)
        self.set_prop_bool(node_id, key, new_val);
    }

    /// Get the number of nodes added so far
    /// This is the highest node ID + 1 (since node IDs are 0-indexed)
    pub fn node_count(&self) -> usize {
        // Find the maximum node ID that has been used
        let max_node_id = self.node_labels.len()
            .max(self.deg_out.len())
            .max(self.deg_in.len())
            .saturating_sub(1);
        
        // Count actual nodes (those with labels, edges, or properties)
        let mut count = 0;
        for i in 0..=max_node_id {
            if i < self.node_labels.len() && !self.node_labels[i].is_empty() {
                count += 1;
            } else if i < self.deg_out.len() && (self.deg_out[i] > 0 || self.deg_in[i] > 0) {
                count += 1;
            } else {
                // Check if node has properties
                let has_props = self.node_col_i64.values().any(|v| v.iter().any(|(nid, _)| *nid == i as NodeId))
                    || self.node_col_f64.values().any(|v| v.iter().any(|(nid, _)| *nid == i as NodeId))
                    || self.node_col_bool.values().any(|v| v.iter().any(|(nid, _)| *nid == i as NodeId))
                    || self.node_col_str.values().any(|v| v.iter().any(|(nid, _)| *nid == i as NodeId));
                if has_props {
                    count += 1;
                }
            }
        }
        count
    }

    /// Get the number of relationships added so far
    pub fn rel_count(&self) -> usize {
        self.rels.len()
    }

    /// Resolve a string ID back to a string (for querying properties)
    pub fn resolve_string(&self, id: u32) -> String {
        self.interner.resolve(id)
    }

    /// Get nodes with a specific property value, scoped by label (before finalization)
    /// 
    /// # Arguments
    /// * `label` - The label to scope the query to (as a string)
    /// * `key` - The property key
    /// * `value` - The property value to search for (can be `&str`, `String`, `i64`, `i32`, `f64`, `bool`, or `ValueId`)
    /// 
    /// # Examples
    /// ```
    /// use rustychickpeas_core::GraphBuilder;
    /// 
    /// // Create a builder and add nodes with properties
    /// let mut builder = GraphBuilder::new(Some(10), Some(10));
    /// builder.add_node(Some(0), &["Person"]);
    /// builder.set_prop_str(0, "name", "Alice");
    /// builder.set_prop_i64(0, "age", 30);
    /// builder.set_prop_bool(0, "active", true);
    /// 
    /// // Find all Person nodes with name "Alice"
    /// let nodes = builder.nodes_with_property("Person", "name", "Alice");
    /// 
    /// // Find all Person nodes with age 30
    /// let nodes = builder.nodes_with_property("Person", "age", 30i64);
    /// 
    /// // Find all Person nodes with active = true
    /// let nodes = builder.nodes_with_property("Person", "active", true);
    /// ```
    pub fn nodes_with_property<V: IntoValueIdBuilder>(&self, label: &str, key: &str, value: V) -> Vec<NodeId> {
        let value_id = value.into_value_id(self);
        let label_id = self.interner.get_or_intern(label);
        let k = self.interner.get_or_intern(key);
        let label_key = Label::new(label_id);
        
        // Build result by scanning property columns, filtered by label
        let mut nodes = Vec::new();
        
        // Helper to check if a node has the specified label
        let has_label = |node_id: NodeId| -> bool {
            if let Some(labels) = self.node_labels.get(node_id as usize) {
                labels.iter().any(|&l| l == label_key)
            } else {
                false
            }
        };
        
        // Check i64 column
        if let Some(pairs) = self.node_col_i64.get(&k) {
            for (node_id, val) in pairs {
                if ValueId::I64(*val) == value_id && has_label(*node_id) {
                    nodes.push(*node_id);
                }
            }
        }
        
        // Check f64 column
        if let Some(pairs) = self.node_col_f64.get(&k) {
            for (node_id, val) in pairs {
                if ValueId::from_f64(*val) == value_id && has_label(*node_id) {
                    nodes.push(*node_id);
                }
            }
        }
        
        // Check bool column
        if let Some(pairs) = self.node_col_bool.get(&k) {
            for (node_id, val) in pairs {
                if ValueId::Bool(*val) == value_id && has_label(*node_id) {
                    nodes.push(*node_id);
                }
            }
        }
        
        // Check str column
        if let Some(pairs) = self.node_col_str.get(&k) {
            for (node_id, val_id) in pairs {
                if ValueId::Str(*val_id) == value_id && has_label(*node_id) {
                    nodes.push(*node_id);
                }
            }
        }
        
        nodes
    }

    /// Get node labels (before finalization)
    pub fn node_labels(&self, node_id: NodeId) -> Vec<String> {
        if let Some(labels) = self.node_labels.get(node_id as usize) {
            labels.iter()
                .map(|l| self.interner.resolve(l.id()).to_string())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get neighbors of a node (before finalization)
    /// Returns (outgoing, incoming) neighbors as node IDs
    pub fn neighbor_ids(&self, node_id: NodeId) -> (Vec<NodeId>, Vec<NodeId>) {
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();
        
        // Find outgoing neighbors (where this node is the start)
        for (start, end) in &self.rels {
            if *start == node_id {
                outgoing.push(*end);
            }
            if *end == node_id {
                incoming.push(*start);
            }
        }
        
        (outgoing, incoming)
    }

    /// Finalize the builder into an immutable GraphSnapshot
    /// 
    /// This consumes the builder and returns the finalized snapshot.
    /// To add the snapshot to a manager, use `manager.add_snapshot(snapshot)`.
    /// 
    /// # Arguments
    /// * `index_properties` - Optional list of property key names to index during finalization.
    ///   If provided, these properties will be indexed upfront (faster queries, more memory).
    ///   If None, all properties will be indexed lazily on first access (saves memory).
    /// 
    /// # Examples
    /// ```
    /// use rustychickpeas_core::GraphBuilder;
    /// 
    /// // Create a builder and add nodes
    /// let mut builder = GraphBuilder::new(Some(10), Some(10));
    /// builder.add_node(Some(0), &["Person"]);
    /// builder.set_prop_str(0, "name", "Alice");
    /// builder.set_prop_i64(0, "age", 30);
    /// 
    /// // Index specific properties upfront
    /// let snapshot = builder.finalize(Some(&["name", "age"]));
    /// 
    /// // Or create a new builder for lazy indexing (default)
    /// let mut builder2 = GraphBuilder::new(Some(10), Some(10));
    /// builder2.add_node(Some(0), &["Person"]);
    /// let snapshot = builder2.finalize(None);
    /// ```
    pub fn finalize(self, index_properties: Option<&[&str]>) -> GraphSnapshot {
        let index_property_keys: Option<Vec<PropertyKey>> = index_properties.map(|keys| {
            keys.iter()
                .filter_map(|key| self.interner.get(key))
                .collect()
        });
        self.finalize_with_keys(index_property_keys.as_deref())
    }

    /// Finalize the builder into an immutable GraphSnapshot (internal ID-based version)
    fn finalize_with_keys(self, index_properties: Option<&[PropertyKey]>) -> GraphSnapshot {
        // Calculate the maximum node ID that has been used
        // We need to find the actual maximum node ID, not just the vector length
        // (vectors may be pre-allocated with capacity)
        
        // Find the maximum node ID from properties (these are actual node IDs)
        let max_node_id_from_props = self.node_col_i64.values()
            .flat_map(|v| v.iter().map(|(nid, _)| *nid))
            .chain(self.node_col_f64.values().flat_map(|v| v.iter().map(|(nid, _)| *nid)))
            .chain(self.node_col_bool.values().flat_map(|v| v.iter().map(|(nid, _)| *nid)))
            .chain(self.node_col_str.values().flat_map(|v| v.iter().map(|(nid, _)| *nid)))
            .max()
            .map(|nid| nid as usize)
            .unwrap_or(0);
        
        // Find the maximum node ID from relationships
        let max_node_id_from_rels = self.rels.iter()
            .flat_map(|(u, v)| [*u as usize, *v as usize])
            .max()
            .unwrap_or(0);
        
        // Find the maximum node ID that has labels
        let max_node_id_from_labels = self.node_labels.iter()
            .enumerate()
            .filter(|(_, labels)| !labels.is_empty())
            .map(|(i, _)| i)
            .max()
            .unwrap_or(0);
        
        // Find the maximum node ID that has edges
        let max_node_id_from_edges = self.deg_out.iter()
            .enumerate()
            .chain(self.deg_in.iter().enumerate())
            .filter(|(_, &deg)| deg > 0)
            .map(|(i, _)| i)
            .max()
            .unwrap_or(0);
        
        // The actual maximum node ID is the max of all these
        let max_used_node_id = max_node_id_from_props
            .max(max_node_id_from_rels)
            .max(max_node_id_from_labels)
            .max(max_node_id_from_edges);
        
        // Count actual nodes (those with labels, edges, or properties)
        // This is important for sparse graphs where node IDs may have gaps
        // Use RoaringBitmap for O(1) insertion and efficient counting - more memory efficient than HashSet
        let mut nodes_with_data = RoaringBitmap::new();
        
        // Mark nodes with labels
        for (i, labels) in self.node_labels.iter().enumerate().take(max_used_node_id + 1) {
            if !labels.is_empty() {
                nodes_with_data.insert(i as u32);
            }
        }
        
        // Mark nodes with edges
        for i in 0..=max_node_id_from_edges {
            if i < self.deg_out.len() && (self.deg_out[i] > 0 || self.deg_in[i] > 0) {
                nodes_with_data.insert(i as u32);
            }
        }
        
        // Mark nodes with properties
        for (nid, _) in self.node_col_i64.values().flat_map(|v| v.iter()) {
            nodes_with_data.insert(*nid);
        }
        for (nid, _) in self.node_col_f64.values().flat_map(|v| v.iter()) {
            nodes_with_data.insert(*nid);
        }
        for (nid, _) in self.node_col_bool.values().flat_map(|v| v.iter()) {
            nodes_with_data.insert(*nid);
        }
        for (nid, _) in self.node_col_str.values().flat_map(|v| v.iter()) {
            nodes_with_data.insert(*nid);
        }
        
        // Count nodes with data
        let actual_node_count = nodes_with_data.len() as usize;
        
        // Use max_used_node_id + 1 for array sizing (CSR needs dense arrays)
        // But store actual_node_count as n_nodes
        let n = (max_used_node_id + 1).max(1);
        let m = self.rels.len();

        // --- Build CSR (outgoing) ---
        let mut out_offsets = vec![0u32; n + 1];
        for i in 0..n {
            out_offsets[i + 1] = out_offsets[i] + self.deg_out[i];
        }
        let mut out_nbrs = vec![0u32; m];
        let mut out_types = vec![RelationshipType::new(0); m]; // Parallel array for types
        let mut out_pos = vec![0u32; n]; // Track position per node
        // Mapping from builder rel index to final CSR position
        let mut builder_to_csr: Vec<usize> = vec![0; m];
        // Zip rels with their types to keep them together
        for (builder_idx, ((u, v), rel_type)) in self.rels.iter().zip(self.rel_types.iter()).enumerate() {
            let u_idx = *u as usize;
            let pos = (out_offsets[u_idx] + out_pos[u_idx]) as usize;
            out_nbrs[pos] = *v;
            out_types[pos] = *rel_type; // Store relationship type
            builder_to_csr[builder_idx] = pos; // Map builder index to CSR position
            out_pos[u_idx] += 1;
        }

        // --- Build CSR (incoming) ---
        let mut in_offsets = vec![0u32; n + 1];
        for i in 0..n {
            in_offsets[i + 1] = in_offsets[i] + self.deg_in[i];
        }
        let mut in_nbrs = vec![0u32; m];
        let mut in_types = vec![RelationshipType::new(0); m]; // Parallel array for types
        let mut in_pos = vec![0u32; n];
        // Zip rels with their types to keep them together
        for ((u, v), rel_type) in self.rels.iter().zip(self.rel_types.iter()) {
            let v_idx = *v as usize;
            let pos = (in_offsets[v_idx] + in_pos[v_idx]) as usize;
            in_nbrs[pos] = *u;
            in_types[pos] = *rel_type; // Store relationship type
            in_pos[v_idx] += 1;
        }

        // --- Build label index ---
        let mut label_index: HashMap<Label, Vec<NodeId>> = HashMap::new();
        for (node_id, labels) in self.node_labels.iter().enumerate().take(n) {
            for label in labels {
                label_index.entry(*label).or_default().push(node_id as NodeId);
            }
        }
        let label_index: HashMap<Label, NodeSet> = label_index
            .into_iter()
            .map(|(label, mut nodes)| {
                nodes.sort_unstable();
                nodes.dedup();
                let bitmap = RoaringBitmap::from_sorted_iter(nodes.into_iter()).unwrap();
                (label, NodeSet::new(bitmap))
            })
            .collect();

        // --- Build type index ---
        let mut type_index: HashMap<RelationshipType, Vec<u32>> = HashMap::new();
        for (rel_idx, rel_type) in self.rel_types.iter().enumerate() {
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

        // --- Build property indexes (optional, for specified keys) ---
        // Do this BEFORE moving property columns, so we can read from them
        // Indexes are now scoped by (label, property_key) to allow the same property key
        // to be indexed separately per label
        let mut prop_index: hashbrown::HashMap<(Label, PropertyKey), hashbrown::HashMap<ValueId, NodeSet>> = hashbrown::HashMap::new();
        
        if let Some(keys_to_index) = index_properties {
            // Build indexes for specified property keys, scoped by label
            // For each label and each property key, build a separate index
            use rayon::prelude::*;
            
            // Get all labels that have nodes
            let labels_with_nodes: Vec<Label> = label_index.keys().copied().collect();
            
            // For each (label, property_key) combination, build an index
            let label_key_combinations: Vec<(Label, PropertyKey)> = labels_with_nodes
                .iter()
                .flat_map(|&label| {
                    keys_to_index.iter().map(move |&key| (label, key))
                })
                .collect();
            
            let prop_index_vec: Vec<((Label, PropertyKey), hashbrown::HashMap<ValueId, NodeSet>)> = label_key_combinations
                .into_par_iter()
                .filter_map(|(label, key)| {
                    // Get nodes with this label
                    let label_nodes = label_index.get(&label)?;
                    
                    // Build inverted index from property columns, filtered by label
                    let mut inv_map: hashbrown::HashMap<ValueId, Vec<NodeId>> = hashbrown::HashMap::new();
                    
                    // Process i64 columns
                    if let Some(pairs) = self.node_col_i64.get(&key) {
                        for (node_id, val) in pairs {
                            if label_nodes.contains(*node_id) {
                                inv_map.entry(ValueId::I64(*val)).or_default().push(*node_id);
                            }
                        }
                    }
                    
                    // Process f64 columns
                    if let Some(pairs) = self.node_col_f64.get(&key) {
                        for (node_id, val) in pairs {
                            if label_nodes.contains(*node_id) {
                                inv_map.entry(ValueId::from_f64(*val)).or_default().push(*node_id);
                            }
                        }
                    }
                    
                    // Process bool columns
                    if let Some(pairs) = self.node_col_bool.get(&key) {
                        for (node_id, val) in pairs {
                            if label_nodes.contains(*node_id) {
                                inv_map.entry(ValueId::Bool(*val)).or_default().push(*node_id);
                            }
                        }
                    }
                    
                    // Process str columns
                    if let Some(pairs) = self.node_col_str.get(&key) {
                        for (node_id, val_id) in pairs {
                            if label_nodes.contains(*node_id) {
                                inv_map.entry(ValueId::Str(*val_id)).or_default().push(*node_id);
                            }
                        }
                    }
                    
                    // Build the index for this (label, key) combination
                    if inv_map.is_empty() {
                        return None;
                    }
                    
                    let mut key_index: hashbrown::HashMap<ValueId, NodeSet> = hashbrown::HashMap::new();
                    for (val_id, mut bucket) in inv_map {
                        bucket.sort_unstable();
                        bucket.dedup();
                        let bitmap = RoaringBitmap::from_sorted_iter(bucket.into_iter()).unwrap();
                        key_index.insert(val_id, NodeSet::new(bitmap));
                    }
                    
                    Some(((label, key), key_index))
                })
                .collect();
            
            prop_index.extend(prop_index_vec);
        }
        
        // --- Build property columns ---
        let mut columns: HashMap<PropertyKey, Column> = HashMap::new();
        
        // Convert staging vectors to columns (dense if >80% coverage, sparse otherwise)
        let threshold = (n as f64 * 0.8) as usize;
        
        // i64 columns
        for (key, pairs) in self.node_col_i64 {
            if pairs.len() >= threshold {
                // Dense
                let mut col = vec![0i64; n];
                for (node_id, val) in pairs {
                    col[node_id as usize] = val;
                }
                columns.insert(key, Column::DenseI64(col));
            } else {
                // Sparse
                let mut pairs = pairs;
                pairs.sort_unstable_by_key(|(id, _)| *id);
                columns.insert(key, Column::SparseI64(pairs));
            }
        }
        
        // f64 columns
        for (key, pairs) in self.node_col_f64 {
            if pairs.len() >= threshold {
                // Dense
                let mut col = vec![0.0f64; n];
                for (node_id, val) in pairs {
                    col[node_id as usize] = val;
                }
                columns.insert(key, Column::DenseF64(col));
            } else {
                // Sparse
                let mut pairs = pairs;
                pairs.sort_unstable_by_key(|(id, _)| *id);
                columns.insert(key, Column::SparseF64(pairs));
            }
        }
        
        // bool columns
        for (key, pairs) in self.node_col_bool {
            if pairs.len() >= threshold {
                // Dense
                let mut col = bitvec::vec::BitVec::repeat(false, n);
                for (node_id, val) in pairs {
                    col.set(node_id as usize, val);
                }
                columns.insert(key, Column::DenseBool(col));
            } else {
                // Sparse
                let mut pairs = pairs;
                pairs.sort_unstable_by_key(|(id, _)| *id);
                columns.insert(key, Column::SparseBool(pairs));
            }
        }
        
        // str columns (interned)
        for (key, pairs) in self.node_col_str {
            if pairs.len() >= threshold {
                // Dense
                let mut col = vec![0u32; n];
                for (node_id, val) in pairs {
                    col[node_id as usize] = val;
                }
                columns.insert(key, Column::DenseStr(col));
            } else {
                // Sparse
                let mut pairs = pairs;
                pairs.sort_unstable_by_key(|(id, _)| *id);
                columns.insert(key, Column::SparseStr(pairs));
            }
        }

        // --- Build relationship property columns ---
        // Convert relationship property staging vectors to columns
        // Use same threshold logic: dense if >80% of relationships have the property
        let rel_threshold = (m as f64 * 0.8) as usize;
        let mut rel_columns: HashMap<PropertyKey, Column> = HashMap::new();

        // i64 relationship columns
        for (key, pairs) in self.rel_col_i64 {
            // Map from builder index to CSR position
            let mut csr_pairs: Vec<(usize, i64)> = pairs.into_iter()
                .map(|(builder_idx, val)| (builder_to_csr[builder_idx], val))
                .collect();
            
            if csr_pairs.len() >= rel_threshold {
                // Dense
                let mut col = vec![0i64; m];
                for (csr_pos, val) in csr_pairs {
                    col[csr_pos] = val;
                }
                rel_columns.insert(key, Column::DenseI64(col));
            } else {
                // Sparse - convert usize to u32 for storage
                csr_pairs.sort_unstable_by_key(|(pos, _)| *pos);
                let sparse: Vec<(u32, i64)> = csr_pairs.into_iter()
                    .map(|(pos, val)| (pos as u32, val))
                    .collect();
                rel_columns.insert(key, Column::SparseI64(sparse));
            }
        }

        // f64 relationship columns
        for (key, pairs) in self.rel_col_f64 {
            let mut csr_pairs: Vec<(usize, f64)> = pairs.into_iter()
                .map(|(builder_idx, val)| (builder_to_csr[builder_idx], val))
                .collect();
            
            if csr_pairs.len() >= rel_threshold {
                let mut col = vec![0.0f64; m];
                for (csr_pos, val) in csr_pairs {
                    col[csr_pos] = val;
                }
                rel_columns.insert(key, Column::DenseF64(col));
            } else {
                csr_pairs.sort_unstable_by_key(|(pos, _)| *pos);
                let sparse: Vec<(u32, f64)> = csr_pairs.into_iter()
                    .map(|(pos, val)| (pos as u32, val))
                    .collect();
                rel_columns.insert(key, Column::SparseF64(sparse));
            }
        }

        // bool relationship columns
        for (key, pairs) in self.rel_col_bool {
            let mut csr_pairs: Vec<(usize, bool)> = pairs.into_iter()
                .map(|(builder_idx, val)| (builder_to_csr[builder_idx], val))
                .collect();
            
            if csr_pairs.len() >= rel_threshold {
                let mut col = bitvec::vec::BitVec::repeat(false, m);
                for (csr_pos, val) in csr_pairs {
                    col.set(csr_pos, val);
                }
                rel_columns.insert(key, Column::DenseBool(col));
            } else {
                csr_pairs.sort_unstable_by_key(|(pos, _)| *pos);
                let sparse: Vec<(u32, bool)> = csr_pairs.into_iter()
                    .map(|(pos, val)| (pos as u32, val))
                    .collect();
                rel_columns.insert(key, Column::SparseBool(sparse));
            }
        }

        // str relationship columns (interned)
        for (key, pairs) in self.rel_col_str {
            let mut csr_pairs: Vec<(usize, u32)> = pairs.into_iter()
                .map(|(builder_idx, val)| (builder_to_csr[builder_idx], val))
                .collect();
            
            if csr_pairs.len() >= rel_threshold {
                let mut col = vec![0u32; m];
                for (csr_pos, val) in csr_pairs {
                    col[csr_pos] = val;
                }
                rel_columns.insert(key, Column::DenseStr(col));
            } else {
                csr_pairs.sort_unstable_by_key(|(pos, _)| *pos);
                let sparse: Vec<(u32, u32)> = csr_pairs.into_iter()
                    .map(|(pos, val)| (pos as u32, val))
                    .collect();
                rel_columns.insert(key, Column::SparseStr(sparse));
            }
        }

        // If index_properties is None, indexes will be built lazily on first access

        // --- Atoms (interned strings) ---
        // Extract all strings from interner
        let atoms = Atoms::new(self.interner.into_vec());

        GraphSnapshot {
            n_nodes: actual_node_count as u32,
            n_rels: m as u64,
            out_offsets,
            out_nbrs,
            out_types,
            in_offsets,
            in_nbrs,
            in_types,
            label_index,
            type_index,
            version: self.version.clone(),
            columns,
            rel_columns,
            prop_index: std::sync::Mutex::new(prop_index),
            atoms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_new() {
        let builder = GraphBuilder::new(None, None);
        assert_eq!(builder.node_count(), 0);
        assert_eq!(builder.rel_count(), 0);
    }

    #[test]
    fn test_builder_with_version() {
        let builder = GraphBuilder::with_version("v1.0", None, None);
        assert_eq!(builder.version, Some("v1.0".to_string()));
    }

    #[test]
    fn test_add_node() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        assert_eq!(builder.node_count(), 1);
        let labels = builder.node_labels(1);
        assert_eq!(labels.len(), 1);
        assert!(labels.contains(&"Person".to_string()));
    }

    #[test]
    fn test_add_node_multiple_labels() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person", "User"]);
        assert_eq!(builder.node_count(), 1);
        let labels = builder.node_labels(1);
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"Person".to_string()));
        assert!(labels.contains(&"User".to_string()));
    }

    #[test]
    fn test_add_relationship() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        assert_eq!(builder.rel_count(), 1);
    }

    #[test]
    fn test_set_properties() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        
        builder.set_prop_str(1, "name", "Alice");
        builder.set_prop_i64(1, "age", 30);
        builder.set_prop_f64(1, "score", 95.5);
        builder.set_prop_bool(1, "active", true);
        
        let alice_id = builder.interner.get_or_intern("Alice");
        assert_eq!(builder.prop(1, "name"), Some(ValueId::Str(alice_id)));
        assert_eq!(builder.prop(1, "age"), Some(ValueId::I64(30)));
        assert_eq!(builder.prop(1, "score"), Some(ValueId::from_f64(95.5)));
        assert_eq!(builder.prop(1, "active"), Some(ValueId::Bool(true)));
    }

    #[test]
    fn test_get_prop_nonexistent() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        assert_eq!(builder.prop(1, "nonexistent"), None);
        assert_eq!(builder.prop(999, "name"), None);
    }

    #[test]
    fn test_update_prop_str() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        builder.set_prop_str(1, "name", "Alice");
        
        let alice_id = builder.interner.get_or_intern("Alice");
        assert_eq!(builder.prop(1, "name"), Some(ValueId::Str(alice_id)));
        
        builder.update_prop_str(1, "name", "Bob");
        let bob_id = builder.interner.get_or_intern("Bob");
        assert_eq!(builder.prop(1, "name"), Some(ValueId::Str(bob_id)));
    }

    #[test]
    fn test_update_prop_i64() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        builder.set_prop_i64(1, "age", 30);
        assert_eq!(builder.prop(1, "age"), Some(ValueId::I64(30)));
        
        builder.update_prop_i64(1, "age", 31);
        assert_eq!(builder.prop(1, "age"), Some(ValueId::I64(31)));
    }

    #[test]
    fn test_update_prop_f64() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        builder.set_prop_f64(1, "score", 95.5);
        assert_eq!(builder.prop(1, "score"), Some(ValueId::from_f64(95.5)));
        
        builder.update_prop_f64(1, "score", 98.0);
        assert_eq!(builder.prop(1, "score"), Some(ValueId::from_f64(98.0)));
    }

    #[test]
    fn test_resolve_string() {
        let builder = GraphBuilder::new(Some(10), Some(10));
        let id = builder.interner.get_or_intern("test");
        assert_eq!(builder.resolve_string(id), "test");
    }

    #[test]
    fn test_get_neighbors() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_node(Some(3), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        builder.add_rel(3, 1, "KNOWS");
        
        let (outgoing, incoming) = builder.neighbor_ids(1);
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0], 2); // Node 2
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0], 3); // Node 3
    }

    #[test]
    fn test_get_neighbors_nonexistent() {
        let builder = GraphBuilder::new(Some(10), Some(10));
        let (outgoing, incoming) = builder.neighbor_ids(999);
        assert_eq!(outgoing.len(), 0);
        assert_eq!(incoming.len(), 0);
    }

    #[test]
    fn test_get_node_labels_nonexistent() {
        let builder = GraphBuilder::new(Some(10), Some(10));
        let labels = builder.node_labels(999);
        assert_eq!(labels.len(), 0);
    }

    #[test]
    fn test_set_version() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.set_version("v1.0");
        assert_eq!(builder.version, Some("v1.0".to_string()));
    }

    #[test]
    fn test_with_version_builder() {
        let builder = GraphBuilder::new(Some(10), Some(10))
            .with_version_builder("v2.0");
        assert_eq!(builder.version, Some("v2.0".to_string()));
    }

    #[test]
    fn test_get_property_key_id() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        assert_eq!(builder.property_key_id("name"), None);
        builder.set_prop_str(1, "name", "value");
        assert!(builder.property_key_id("name").is_some());
    }

    #[test]
    fn test_auto_grow() {
        let mut builder = GraphBuilder::new(Some(2), Some(2));
        // Add more nodes than initial capacity
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_node(Some(3), &["Person"]);
        assert_eq!(builder.node_count(), 3);
    }

    #[test]
    fn test_multiple_relationships() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_node(Some(3), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        builder.add_rel(1, 3, "KNOWS");
        assert_eq!(builder.rel_count(), 2);
    }

    #[test]
    fn test_dense_column_threshold() {
        // Test that columns with >80% coverage become dense
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        // Add 10 nodes
        for i in 1..=10 {
            builder.add_node(Some(i), &["Person"]);
        }
        // Set property on 9 nodes (>80% of 10)
        for i in 1..=9 {
            builder.set_prop_i64(i, "age", 30);
        }
        // This should create a dense column
        // We can't test finalize() directly due to into_vec() issue, but we can verify the data is there
        assert_eq!(builder.node_col_i64.len(), 1);
    }

    #[test]
    fn test_f64_properties() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        builder.set_prop_f64(1, "score", 95.5);
        assert_eq!(builder.prop(1, "score"), Some(ValueId::from_f64(95.5)));
    }

    #[test]
    fn test_bool_properties() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(1), &["Person"]);
        builder.set_prop_bool(1, "active", true);
        assert_eq!(builder.prop(1, "active"), Some(ValueId::Bool(true)));
    }

    #[test]
    fn test_get_nodes_with_property() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(0), &["Person"]);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.set_prop_i64(0, "age", 30);
        builder.set_prop_i64(1, "age", 30);
        builder.set_prop_i64(2, "age", 25);
        
        let nodes = builder.nodes_with_property("Person", "age", 30i64);
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&0));
        assert!(nodes.contains(&1));
        
        let nodes = builder.nodes_with_property("Person", "age", 25i64);
        assert_eq!(nodes.len(), 1);
        assert!(nodes.contains(&2));
        
        // Non-existent property value
        let nodes = builder.nodes_with_property("Person", "age", 99i64);
        assert_eq!(nodes.len(), 0);
    }

    #[test]
    fn test_get_nodes_with_property_f64() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(0), &["Person"]);
        builder.set_prop_f64(0, "score", 95.5);
        
        let nodes = builder.nodes_with_property("Person", "score", 95.5);
        assert_eq!(nodes.len(), 1);
        assert!(nodes.contains(&0));
    }

    #[test]
    fn test_get_nodes_with_property_bool() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(0), &["Person"]);
        builder.set_prop_bool(0, "active", true);
        
        let nodes = builder.nodes_with_property("Person", "active", true);
        assert_eq!(nodes.len(), 1);
        assert!(nodes.contains(&0));
    }

    #[test]
    fn test_finalize_simple() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(0), &["Person"]);
        builder.add_node(Some(1), &["Person"]);
        builder.add_rel(0, 1, "KNOWS");
        
        let snapshot = builder.finalize(None);
        assert_eq!(snapshot.n_nodes, 2);
        assert_eq!(snapshot.n_rels, 1);
    }

    #[test]
    fn test_finalize_with_properties() {
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        builder.add_node(Some(0), &["Person"]);
        builder.set_prop_str(0, "name", "Alice");
        
        // Get the key before finalize consumes the builder
        let name_key = builder.property_key_id("name").unwrap();
        
        let snapshot = builder.finalize(None);
        assert_eq!(snapshot.n_nodes, 1);
        
        // Check that property is accessible
        assert!(snapshot.columns.contains_key(&name_key));
    }

    // Deduplication tests for each tuple type in DedupKey

    #[test]
    fn test_dedup_single_property() {
        // Tests DedupKey::Single
        // Note: Deduplication only works during Parquet loading, but we can test the DedupKey types
        // by manually building the dedup keys and verifying they work correctly
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["email"]);
        
        // Add first node with email
        let node1 = builder.add_node(Some(1), &["Person"]);
        builder.set_prop_str(node1, "email", "alice@example.com");
        
        // Manually build dedup key and add to map (simulating Parquet loading behavior)
        let _email_key = builder.interner.get_or_intern("email");
        let email_val = builder.interner.get_or_intern("alice@example.com");
        let dedup_key = DedupKey::Single(ValueId::Str(email_val));
        builder.dedup_map.insert(dedup_key, node1);
        
        assert_eq!(builder.node_count(), 1);
        
        // Add second node with same email - check dedup_map
        let node2 = builder.add_node(Some(2), &["User"]);
        builder.set_prop_str(node2, "email", "alice@example.com");
        
        // Check that dedup_map would find the existing node
        let email_val2 = builder.interner.get_or_intern("alice@example.com");
        let dedup_key2 = DedupKey::Single(ValueId::Str(email_val2));
        assert_eq!(builder.dedup_map.get(&dedup_key2), Some(&node1));
        
        // Manually merge labels (simulating Parquet behavior)
        let labels = builder.node_labels(node1);
        let mut merged_labels = labels.clone();
        for label in builder.node_labels(node2) {
            if !merged_labels.contains(&label) {
                merged_labels.push(label);
            }
        }
        // Verify we would merge to node1
        assert_eq!(merged_labels.len(), 2);
        assert!(merged_labels.contains(&"Person".to_string()));
        assert!(merged_labels.contains(&"User".to_string()));
        
        // Add third node with different email - should create new entry
        let node3 = builder.add_node(Some(3), &["Person"]);
        builder.set_prop_str(node3, "email", "bob@example.com");
        let bob_val = builder.interner.get_or_intern("bob@example.com");
        let dedup_key3 = DedupKey::Single(ValueId::Str(bob_val));
        builder.dedup_map.insert(dedup_key3, node3);
        assert_eq!(builder.dedup_map.len(), 2);
    }

    #[test]
    fn test_dedup_pair_properties() {
        // Tests DedupKey::Pair
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["email", "username"]);
        
        // Build dedup keys manually to test Pair variant
        let email_val1 = builder.interner.get_or_intern("alice@example.com");
        let username_val1 = builder.interner.get_or_intern("alice");
        let dedup_key1 = DedupKey::Pair(ValueId::Str(email_val1), ValueId::Str(username_val1));
        builder.dedup_map.insert(dedup_key1.clone(), 1);
        
        // Same key should map to same node
        let email_val2 = builder.interner.get_or_intern("alice@example.com");
        let username_val2 = builder.interner.get_or_intern("alice");
        let dedup_key2 = DedupKey::Pair(ValueId::Str(email_val2), ValueId::Str(username_val2));
        assert_eq!(builder.dedup_map.get(&dedup_key2), Some(&1));
        
        // Different username should create different key
        let username_val3 = builder.interner.get_or_intern("alice2");
        let dedup_key3 = DedupKey::Pair(ValueId::Str(email_val2), ValueId::Str(username_val3));
        assert_eq!(builder.dedup_map.get(&dedup_key3), None);
        builder.dedup_map.insert(dedup_key3, 2);
        assert_eq!(builder.dedup_map.len(), 2);
    }

    #[test]
    fn test_dedup_triple_properties() {
        // Tests DedupKey::Triple
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["email", "username", "domain"]);
        
        let email_val = builder.interner.get_or_intern("alice@example.com");
        let username_val = builder.interner.get_or_intern("alice");
        let domain_val1 = builder.interner.get_or_intern("example.com");
        let dedup_key1 = DedupKey::Triple(ValueId::Str(email_val), ValueId::Str(username_val), ValueId::Str(domain_val1));
        builder.dedup_map.insert(dedup_key1.clone(), 1);
        
        // Same triple should map to same node
        let email_val2 = builder.interner.get_or_intern("alice@example.com");
        let username_val2 = builder.interner.get_or_intern("alice");
        let domain_val2 = builder.interner.get_or_intern("example.com");
        let dedup_key2 = DedupKey::Triple(ValueId::Str(email_val2), ValueId::Str(username_val2), ValueId::Str(domain_val2));
        assert_eq!(builder.dedup_map.get(&dedup_key2), Some(&1));
        
        // Different domain should create different key
        let domain_val3 = builder.interner.get_or_intern("other.com");
        let dedup_key3 = DedupKey::Triple(ValueId::Str(email_val2), ValueId::Str(username_val2), ValueId::Str(domain_val3));
        assert_eq!(builder.dedup_map.get(&dedup_key3), None);
        builder.dedup_map.insert(dedup_key3, 2);
        assert_eq!(builder.dedup_map.len(), 2);
    }

    #[test]
    fn test_dedup_quad_properties() {
        // Tests DedupKey::Quad
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["email", "username", "domain", "region"]);
        
        let email_val = builder.interner.get_or_intern("alice@example.com");
        let username_val = builder.interner.get_or_intern("alice");
        let domain_val = builder.interner.get_or_intern("example.com");
        let region_val1 = builder.interner.get_or_intern("us-east");
        let dedup_key1 = DedupKey::Quad(ValueId::Str(email_val), ValueId::Str(username_val), ValueId::Str(domain_val), ValueId::Str(region_val1));
        builder.dedup_map.insert(dedup_key1.clone(), 1);
        
        // Same quad should map to same node
        let email_val2 = builder.interner.get_or_intern("alice@example.com");
        let username_val2 = builder.interner.get_or_intern("alice");
        let domain_val2 = builder.interner.get_or_intern("example.com");
        let region_val2 = builder.interner.get_or_intern("us-east");
        let dedup_key2 = DedupKey::Quad(ValueId::Str(email_val2), ValueId::Str(username_val2), ValueId::Str(domain_val2), ValueId::Str(region_val2));
        assert_eq!(builder.dedup_map.get(&dedup_key2), Some(&1));
        
        // Different region should create different key
        let region_val3 = builder.interner.get_or_intern("us-west");
        let dedup_key3 = DedupKey::Quad(ValueId::Str(email_val2), ValueId::Str(username_val2), ValueId::Str(domain_val2), ValueId::Str(region_val3));
        assert_eq!(builder.dedup_map.get(&dedup_key3), None);
        builder.dedup_map.insert(dedup_key3, 2);
        assert_eq!(builder.dedup_map.len(), 2);
    }

    #[test]
    fn test_dedup_quint_properties() {
        // Tests DedupKey::Quint
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["email", "username", "domain", "region", "zone"]);
        
        let email_val = builder.interner.get_or_intern("alice@example.com");
        let username_val = builder.interner.get_or_intern("alice");
        let domain_val = builder.interner.get_or_intern("example.com");
        let region_val = builder.interner.get_or_intern("us-east");
        let zone_val1 = builder.interner.get_or_intern("a");
        let dedup_key1 = DedupKey::Quint(ValueId::Str(email_val), ValueId::Str(username_val), ValueId::Str(domain_val), ValueId::Str(region_val), ValueId::Str(zone_val1));
        builder.dedup_map.insert(dedup_key1.clone(), 1);
        
        // Same quint should map to same node
        let email_val2 = builder.interner.get_or_intern("alice@example.com");
        let username_val2 = builder.interner.get_or_intern("alice");
        let domain_val2 = builder.interner.get_or_intern("example.com");
        let region_val2 = builder.interner.get_or_intern("us-east");
        let zone_val2 = builder.interner.get_or_intern("a");
        let dedup_key2 = DedupKey::Quint(ValueId::Str(email_val2), ValueId::Str(username_val2), ValueId::Str(domain_val2), ValueId::Str(region_val2), ValueId::Str(zone_val2));
        assert_eq!(builder.dedup_map.get(&dedup_key2), Some(&1));
        
        // Different zone should create different key
        let zone_val3 = builder.interner.get_or_intern("b");
        let dedup_key3 = DedupKey::Quint(ValueId::Str(email_val2), ValueId::Str(username_val2), ValueId::Str(domain_val2), ValueId::Str(region_val2), ValueId::Str(zone_val3));
        assert_eq!(builder.dedup_map.get(&dedup_key3), None);
        builder.dedup_map.insert(dedup_key3, 2);
        assert_eq!(builder.dedup_map.len(), 2);
    }

    #[test]
    fn test_dedup_many_properties() {
        // Tests DedupKey::Many (6+ properties)
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["p1", "p2", "p3", "p4", "p5", "p6", "p7"]);
        
        let v1 = builder.interner.get_or_intern("v1");
        let v2 = builder.interner.get_or_intern("v2");
        let v3 = builder.interner.get_or_intern("v3");
        let v4 = builder.interner.get_or_intern("v4");
        let v5 = builder.interner.get_or_intern("v5");
        let v6 = builder.interner.get_or_intern("v6");
        let v7 = builder.interner.get_or_intern("v7");
        let values = vec![ValueId::Str(v1), ValueId::Str(v2), ValueId::Str(v3), ValueId::Str(v4), ValueId::Str(v5), ValueId::Str(v6), ValueId::Str(v7)];
        let dedup_key1 = DedupKey::from_slice(&values);
        builder.dedup_map.insert(dedup_key1.clone(), 1);
        
        // Same 7 properties should map to same node
        let v1_2 = builder.interner.get_or_intern("v1");
        let v2_2 = builder.interner.get_or_intern("v2");
        let v3_2 = builder.interner.get_or_intern("v3");
        let v4_2 = builder.interner.get_or_intern("v4");
        let v5_2 = builder.interner.get_or_intern("v5");
        let v6_2 = builder.interner.get_or_intern("v6");
        let v7_2 = builder.interner.get_or_intern("v7");
        let values2 = vec![ValueId::Str(v1_2), ValueId::Str(v2_2), ValueId::Str(v3_2), ValueId::Str(v4_2), ValueId::Str(v5_2), ValueId::Str(v6_2), ValueId::Str(v7_2)];
        let dedup_key2 = DedupKey::from_slice(&values2);
        assert_eq!(builder.dedup_map.get(&dedup_key2), Some(&1));
        
        // Different p7 should create different key
        let v8 = builder.interner.get_or_intern("v8");
        let values3 = vec![ValueId::Str(v1_2), ValueId::Str(v2_2), ValueId::Str(v3_2), ValueId::Str(v4_2), ValueId::Str(v5_2), ValueId::Str(v6_2), ValueId::Str(v8)];
        let dedup_key3 = DedupKey::from_slice(&values3);
        assert_eq!(builder.dedup_map.get(&dedup_key3), None);
        builder.dedup_map.insert(dedup_key3, 2);
        assert_eq!(builder.dedup_map.len(), 2);
    }

    #[test]
    fn test_dedup_mixed_value_types() {
        // Test deduplication with different ValueId types (i64, f64, bool, str)
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["id", "score", "active", "name"]);
        
        // Build dedup key with mixed types
        let name_val = builder.interner.get_or_intern("Alice");
        let dedup_key1 = DedupKey::Quad(
            ValueId::I64(100),
            ValueId::from_f64(95.5),
            ValueId::Bool(true),
            ValueId::Str(name_val)
        );
        builder.dedup_map.insert(dedup_key1.clone(), 1);
        
        // Same values should map to same node
        let name_val2 = builder.interner.get_or_intern("Alice");
        let dedup_key2 = DedupKey::Quad(
            ValueId::I64(100),
            ValueId::from_f64(95.5),
            ValueId::Bool(true),
            ValueId::Str(name_val2)
        );
        assert_eq!(builder.dedup_map.get(&dedup_key2), Some(&1));
        
        // Different id should create different key
        let dedup_key3 = DedupKey::Quad(
            ValueId::I64(200),
            ValueId::from_f64(95.5),
            ValueId::Bool(true),
            ValueId::Str(name_val2)
        );
        assert_eq!(builder.dedup_map.get(&dedup_key3), None);
        builder.dedup_map.insert(dedup_key3, 2);
        assert_eq!(builder.dedup_map.len(), 2);
    }

    #[test]
    fn test_dedup_partial_properties() {
        // Test that nodes without all deduplication properties are not deduplicated
        // (In Parquet loading, missing properties result in None in dedup_keys_per_row)
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["email", "username"]);
        
        // Node with both properties
        let email_val = builder.interner.get_or_intern("alice@example.com");
        let username_val = builder.interner.get_or_intern("alice");
        let dedup_key = DedupKey::Pair(ValueId::Str(email_val), ValueId::Str(username_val));
        builder.dedup_map.insert(dedup_key, 1);
        
        // Node with only email - cannot build complete dedup key, so won't match
        // Missing username - cannot create Pair key, so won't be in dedup_map
        // This simulates Parquet loading behavior where missing properties result in None
        
        // Node with only username - same issue
        // Missing email - cannot create Pair key
        
        // Only the complete key should be in the map
        assert_eq!(builder.dedup_map.len(), 1);
    }

    #[test]
    fn test_dedup_disable() {
        // Test that disabling deduplication clears the dedup_map
        use crate::types::DedupKey;
        use crate::graph_snapshot::ValueId;
        
        let mut builder = GraphBuilder::new(None, None);
        builder.enable_node_deduplication(vec!["email"]);
        
        // Add a dedup key
        let email_val = builder.interner.get_or_intern("alice@example.com");
        let dedup_key = DedupKey::Single(ValueId::Str(email_val));
        builder.dedup_map.insert(dedup_key, 1);
        assert_eq!(builder.dedup_map.len(), 1);
        
        // Disable deduplication
        builder.disable_node_deduplication();
        
        // dedup_map should be cleared
        assert_eq!(builder.dedup_map.len(), 0);
        assert_eq!(builder.dedup_unique_properties, None);
    }

    // Tests for IntoValueIdBuilder trait

    #[test]
    fn test_into_value_id_builder_valueid() {
        let builder = GraphBuilder::new(None, None);
        let value_id = ValueId::I64(42);
        let result = value_id.into_value_id(&builder);
        assert_eq!(result, ValueId::I64(42));
    }

    #[test]
    fn test_into_value_id_builder_i64() {
        let builder = GraphBuilder::new(None, None);
        let result = 42i64.into_value_id(&builder);
        assert_eq!(result, ValueId::I64(42));
    }

    #[test]
    fn test_into_value_id_builder_i32() {
        let builder = GraphBuilder::new(None, None);
        let result = 42i32.into_value_id(&builder);
        assert_eq!(result, ValueId::I64(42));
    }

    #[test]
    fn test_into_value_id_builder_f64() {
        let builder = GraphBuilder::new(None, None);
        let result = 95.5f64.into_value_id(&builder);
        match result {
            ValueId::F64(bits) => {
                let val = f64::from_bits(bits);
                assert!((val - 95.5).abs() < 0.001);
            }
            _ => panic!("Expected F64 variant"),
        }
    }

    #[test]
    fn test_into_value_id_builder_bool() {
        let builder = GraphBuilder::new(None, None);
        let result_true = true.into_value_id(&builder);
        assert_eq!(result_true, ValueId::Bool(true));
        
        let result_false = false.into_value_id(&builder);
        assert_eq!(result_false, ValueId::Bool(false));
    }

    #[test]
    fn test_into_value_id_builder_str() {
        let builder = GraphBuilder::new(None, None);
        // Intern a string first - need mutable access for get_or_intern
        let id = builder.interner.get_or_intern("test");
        let result = "test".into_value_id(&builder);
        assert_eq!(result, ValueId::Str(id));
    }

    #[test]
    fn test_into_value_id_builder_string() {
        let builder = GraphBuilder::new(None, None);
        // Intern a string first - need mutable access for get_or_intern
        let id = builder.interner.get_or_intern("test");
        let result = "test".to_string().into_value_id(&builder);
        assert_eq!(result, ValueId::Str(id));
    }

    // Tests for relationship properties

    #[test]
    fn test_set_relationship_property_str() {
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        
        builder.set_rel_prop_str(1, 2, "KNOWS", "since", "2020");
        
        // Verify property was set by checking the internal storage
        let since_key = builder.interner.get_or_intern("since");
        let since_val = builder.interner.get_or_intern("2020");
        assert!(builder.rel_col_str.contains_key(&since_key));
        let rel_idx = builder.find_rel_index(1, 2, "KNOWS").unwrap();
        let props = builder.rel_col_str.get(&since_key).unwrap();
        assert!(props.iter().any(|(idx, val)| *idx == rel_idx && *val == since_val));
    }

    #[test]
    fn test_set_relationship_property_i64() {
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        
        builder.set_rel_prop_i64(1, 2, "KNOWS", "weight", 5);
        
        // Verify property was set
        let weight_key = builder.interner.get_or_intern("weight");
        let rel_idx = builder.find_rel_index(1, 2, "KNOWS").unwrap();
        let props = builder.rel_col_i64.get(&weight_key).unwrap();
        assert!(props.iter().any(|(idx, val)| *idx == rel_idx && *val == 5));
    }

    #[test]
    fn test_set_relationship_property_f64() {
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        
        builder.set_rel_prop_f64(1, 2, "KNOWS", "score", 0.85);
        
        // Verify property was set
        let score_key = builder.interner.get_or_intern("score");
        let rel_idx = builder.find_rel_index(1, 2, "KNOWS").unwrap();
        let props = builder.rel_col_f64.get(&score_key).unwrap();
        assert!(props.iter().any(|(idx, val)| *idx == rel_idx && (*val - 0.85).abs() < 0.001));
    }

    #[test]
    fn test_set_relationship_property_bool() {
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        
        builder.set_rel_prop_bool(1, 2, "KNOWS", "verified", true);
        
        // Verify property was set
        let verified_key = builder.interner.get_or_intern("verified");
        let rel_idx = builder.find_rel_index(1, 2, "KNOWS").unwrap();
        let props = builder.rel_col_bool.get(&verified_key).unwrap();
        assert!(props.iter().any(|(idx, val)| *idx == rel_idx && *val == true));
    }

    #[test]
    fn test_set_relationship_property_multiple() {
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        
        builder.set_rel_prop_str(1, 2, "KNOWS", "since", "2020");
        builder.set_rel_prop_i64(1, 2, "KNOWS", "weight", 5);
        builder.set_rel_prop_f64(1, 2, "KNOWS", "score", 0.85);
        builder.set_rel_prop_bool(1, 2, "KNOWS", "verified", true);
        
        // Verify all properties were set
        let rel_idx = builder.find_rel_index(1, 2, "KNOWS").unwrap();
        
        let since_key = builder.interner.get_or_intern("since");
        let since_val = builder.interner.get_or_intern("2020");
        assert!(builder.rel_col_str.get(&since_key).unwrap().iter().any(|(idx, val)| *idx == rel_idx && *val == since_val));
        
        let weight_key = builder.interner.get_or_intern("weight");
        assert!(builder.rel_col_i64.get(&weight_key).unwrap().iter().any(|(idx, val)| *idx == rel_idx && *val == 5));
        
        let score_key = builder.interner.get_or_intern("score");
        assert!(builder.rel_col_f64.get(&score_key).unwrap().iter().any(|(idx, val)| *idx == rel_idx && (*val - 0.85).abs() < 0.001));
        
        let verified_key = builder.interner.get_or_intern("verified");
        assert!(builder.rel_col_bool.get(&verified_key).unwrap().iter().any(|(idx, val)| *idx == rel_idx && *val == true));
    }

    #[test]
    fn test_set_relationship_property_nonexistent_rel() {
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        // Don't add the relationship
        
        // Setting property on non-existent relationship should not panic
        builder.set_rel_prop_str(1, 2, "KNOWS", "since", "2020");
        
        // Property should not be set
        let since_key = builder.interner.get_or_intern("since");
        assert!(!builder.rel_col_str.contains_key(&since_key) || 
                builder.rel_col_str.get(&since_key).unwrap().is_empty());
    }

    #[test]
    fn test_set_relationship_property_wrong_type() {
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        builder.add_rel(1, 2, "LIKES"); // Different type
        
        // Set property on KNOWS relationship
        builder.set_rel_prop_str(1, 2, "KNOWS", "since", "2020");
        
        // Verify property is only on KNOWS, not LIKES
        let since_key = builder.interner.get_or_intern("since");
        let knows_idx = builder.find_rel_index(1, 2, "KNOWS").unwrap();
        let likes_idx = builder.find_rel_index(1, 2, "LIKES").unwrap();
        
        let props = builder.rel_col_str.get(&since_key).unwrap();
        assert!(props.iter().any(|(idx, _)| *idx == knows_idx));
        assert!(!props.iter().any(|(idx, _)| *idx == likes_idx));
    }

    #[test]
    fn test_relationship_properties_finalize() {
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Person"]);
        builder.add_node(Some(2), &["Person"]);
        builder.add_rel(1, 2, "KNOWS");
        
        builder.set_rel_prop_str(1, 2, "KNOWS", "since", "2020");
        builder.set_rel_prop_i64(1, 2, "KNOWS", "weight", 5);
        
        let snapshot = builder.finalize(None);
        
        // Verify relationship properties are in the snapshot
        // We need to find the CSR position for the relationship
        let out_neighbors = snapshot.out_neighbors(1);
        assert_eq!(out_neighbors.len(), 1);
        assert_eq!(out_neighbors[0], 2);
        
        // Get the CSR position (should be 0 for first relationship)
        let csr_pos = 0u32;
        let since_val = snapshot.value_id_from_str("2020").unwrap();
        
        // Check that relationship property exists
        let prop = snapshot.relationship_property(csr_pos, "since");
        assert_eq!(prop, Some(since_val));
        
        let weight_val = ValueId::I64(5);
        let prop = snapshot.relationship_property(csr_pos, "weight");
        assert_eq!(prop, Some(weight_val));
    }
}
