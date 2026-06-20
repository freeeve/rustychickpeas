//! GraphSnapshot Python wrapper

use crate::direction::Direction;
use crate::node::Node;
use crate::relationship::Relationship;
use crate::utils::{py_to_property_value, value_id_to_pyobject};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use roaring::RoaringBitmap;
use rustychickpeas_core::bitmap::NodeSet;
use rustychickpeas_core::types::PropertyKey;
use rustychickpeas_core::{
    AggOp, ColumnDtype, GraphSnapshot as CoreGraphSnapshot, Label, RelationshipRef,
    RelationshipType, ValueId,
};
use std::os::raw::{c_char, c_int, c_void};
use std::sync::{Arc, Mutex, PoisonError};

/// Python wrapper for GraphSnapshot
#[pyclass(name = "GraphSnapshot")]
pub struct GraphSnapshot {
    pub(crate) snapshot: std::sync::Arc<CoreGraphSnapshot>,
}

/// Iterator over the node IDs of a GraphSnapshot
///
/// Node IDs in a finalized snapshot are dense in `0..n_nodes` (the same range
/// accepted by `GraphSnapshot.node()`), so iteration only needs the bounds.
#[pyclass(name = "NodeIdIter")]
pub struct NodeIdIter {
    current: u32,
    end: u32,
}

#[pymethods]
impl NodeIdIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u32> {
        if slf.current < slf.end {
            let id = slf.current;
            slf.current += 1;
            Some(id)
        } else {
            None
        }
    }
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

    /// Convert a Python value to a core [`ValueId`], resolving a string to its
    /// interned atom id. Returns `Ok(None)` when a string value is not interned
    /// in this snapshot — no node can carry it, so predicate/lookup callers
    /// short-circuit to "no match" rather than erroring.
    fn py_value_to_id_opt(&self, value: &Bound<'_, PyAny>) -> PyResult<Option<ValueId>> {
        use rustychickpeas_core::PropertyValue;
        Ok(match py_to_property_value(value)? {
            PropertyValue::String(s) => self.get_string_id(&s).map(ValueId::Str),
            PropertyValue::Integer(i) => Some(ValueId::I64(i)),
            PropertyValue::Float(f) => Some(ValueId::from_f64(f)),
            PropertyValue::Boolean(b) => Some(ValueId::Bool(b)),
            PropertyValue::InternedString(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "InternedString not supported here",
                ));
            }
        })
    }
}

impl GraphSnapshot {
    /// Internal constructor (not exposed to Python)
    /// Takes ownership of a GraphSnapshot
    pub(crate) fn new(snapshot: CoreGraphSnapshot) -> Self {
        Self {
            snapshot: std::sync::Arc::new(snapshot),
        }
    }

    /// Internal constructor from Arc (for manager.get_graph_snapshot)
    pub(crate) fn from_arc(snapshot: std::sync::Arc<CoreGraphSnapshot>) -> Self {
        Self { snapshot }
    }
}

#[pymethods]
impl GraphSnapshot {
    fn __repr__(&self) -> String {
        let version = self
            .snapshot
            .version()
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

    /// Iterate over all node IDs in the snapshot (0..n_nodes)
    ///
    /// Yields exactly the node IDs accepted by `node()`.
    fn __iter__(&self) -> NodeIdIter {
        NodeIdIter {
            current: 0,
            end: self.snapshot.n_nodes,
        }
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
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Label '{}' not found",
                label
            )));
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
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Node ID {} out of range (max: {})",
                node_id,
                self.snapshot.n_nodes.saturating_sub(1)
            )));
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
    fn relationships(
        &self,
        node_id: u32,
        direction: Direction,
        rel_types: Option<Vec<String>>,
    ) -> PyResult<Vec<Relationship>> {
        use rustychickpeas_core::types::RelationshipType;

        if node_id >= self.snapshot.n_nodes {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Node ID {} out of range (max: {})",
                node_id,
                self.snapshot.n_nodes.saturating_sub(1)
            )));
        }

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

    /// Neighbor node IDs in `direction`, optionally restricted to `rel_types`
    /// (deduplicated, ascending, when types are given).
    #[pyo3(signature = (node_id, direction, rel_types=None))]
    fn neighbor_ids(
        &self,
        node_id: u32,
        direction: Direction,
        rel_types: Option<Vec<String>>,
    ) -> Vec<u32> {
        match rel_types {
            None => self.snapshot.neighbors(node_id, direction.into()).collect(),
            Some(types) => {
                let mut set = RoaringBitmap::new();
                for t in &types {
                    for n in self
                        .snapshot
                        .neighbors_by_type(node_id, direction.into(), t.as_str())
                    {
                        set.insert(n);
                    }
                }
                set.iter().collect()
            }
        }
    }

    /// Histogram of the neighbours reached from `sources` via `rel_type` rels in
    /// `direction`: for each source node, count how many of its `rel_type`
    /// neighbours land on each target. Returns a dict mapping target node id to
    /// count. The whole aggregation runs in Rust on a single call, so it is far
    /// faster than counting neighbours in a Python loop.
    fn neighbor_counts(
        &self,
        sources: Vec<u32>,
        direction: Direction,
        rel_type: &str,
    ) -> std::collections::HashMap<u32, usize> {
        // Core returns a hashbrown map; collect into a std map so PyO3 hands Python a dict.
        self.snapshot
            .neighbor_counts(sources, direction.into(), rel_type)
            .into_iter()
            .collect()
    }

    /// Get neighbors of a node as Node objects
    /// Returns a list of Node objects for neighbors in the specified direction
    #[pyo3(signature = (node_id, direction, rel_types=None))]
    fn neighbors(
        &self,
        node_id: u32,
        direction: Direction,
        rel_types: Option<Vec<String>>,
    ) -> PyResult<Vec<Node>> {
        // Get relationships (optionally type-filtered) and extract neighbor IDs.
        let rels = self.relationships(node_id, direction, rel_types)?;
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

        Ok(neighbor_ids
            .into_iter()
            .map(|id| Node {
                snapshot: self.snapshot.clone(),
                node_id: id,
            })
            .collect())
    }

    /// Degree of a node — O(1) from the CSR offsets when untyped; with
    /// `rel_type`, the count of neighbors reached via that type.
    #[pyo3(signature = (node_id, direction, rel_type=None))]
    fn degree(&self, node_id: u32, direction: Direction, rel_type: Option<&str>) -> usize {
        match rel_type {
            Some(rt) => self
                .snapshot
                .neighbors_by_type(node_id, direction.into(), rt)
                .count(),
            None => crate::utils::csr_degree(&self.snapshot, node_id, direction.into()),
        }
    }

    /// Whether `node_id` carries `label` — an O(1) label-index check, vs the
    /// `"X" in node_labels(n)` scan it replaces.
    fn has_label(&self, node_id: u32, label: &str) -> bool {
        self.snapshot.has_label(node_id, label)
    }

    /// Whether `node_id` has any neighbor via `rel_type` in `direction`
    /// (existence check, short-circuits on the first match).
    fn has_rel(&self, node_id: u32, direction: Direction, rel_type: &str) -> bool {
        self.snapshot.has_rel(node_id, direction.into(), rel_type)
    }

    /// Whether `node_id` has a neighbor (via `rel_type`, `direction`) whose node
    /// property `key` equals `value` — resolves the value once, then compares ids
    /// per neighbor (vs a per-neighbor Python property read).
    fn has_neighbor_with_property(
        &self,
        node_id: u32,
        direction: Direction,
        rel_type: &str,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let Some(value_id) = self.py_value_to_id_opt(value)? else {
            return Ok(false);
        };
        Ok(self
            .snapshot
            .has_neighbor_with_property(node_id, direction.into(), rel_type, key, value_id))
    }

    /// The smallest node carrying `label` with property `key` == `value`, or
    /// `None` — collapses `nodes_with_property(..)[0]`, label-scoped so a `name`
    /// shared across labels stays unambiguous.
    fn node_with_label_property(
        &self,
        label: &str,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Option<u32>> {
        let Some(value_id) = self.py_value_to_id_opt(value)? else {
            return Ok(None);
        };
        Ok(self.snapshot.node_with_label_property(label, key, value_id))
    }

    /// First neighbor of `node_id` via `rel_type` in `direction`, or `None` —
    /// returns the id (not a `Node`), short-circuiting the scan.
    fn first_neighbor(&self, node_id: u32, direction: Direction, rel_type: &str) -> Option<u32> {
        self.snapshot
            .first_neighbor(node_id, direction.into(), rel_type)
    }

    /// Follow a fixed chain of `(direction, rel_type)` steps from `start`, taking
    /// the first neighbor at each; `None` if a step has no neighbor. Returns the
    /// final node id (not a list of `Node`s).
    fn follow(&self, start: u32, steps: Vec<(Direction, String)>) -> Option<u32> {
        let steps: Vec<(rustychickpeas_core::Direction, &str)> = steps
            .iter()
            .map(|(d, r)| ((*d).into(), r.as_str()))
            .collect();
        self.snapshot.follow(start, &steps)
    }

    /// The root each node reaches by following the *functional* `rel` in `direction`
    /// (each node has one successor — e.g. a message's `replyOf` thread root); a node
    /// already terminal maps to itself. Returns a `NodeArray` array indexed by node id —
    /// `roots[node]` or `memoryview(roots)` in a hot loop. The forest-root array is
    /// built once and cached on the snapshot, so this is the bulk form to reach for
    /// over a per-node `root_via`. `None` if `rel` is unknown.
    fn roots_via(&self, rel: &str, direction: Direction) -> Option<NodeArray> {
        let rt = self.snapshot.relationship_type_from_str(rel)?;
        let inner = self.snapshot.chain_roots(direction.into(), rt);
        let len = inner.len() as ffi::Py_ssize_t;
        Some(NodeArray {
            inner,
            shape: [len],
            strides: [4],
        })
    }

    /// The root of a single `node` via the functional `rel` in `direction` (see
    /// `roots_via`). Convenience for a one-off lookup; in a per-node loop prefer
    /// `roots_via` and index it. `None` if `rel` is unknown.
    fn root_via(&self, node: u32, rel: &str, direction: Direction) -> Option<u32> {
        let rt = self.snapshot.relationship_type_from_str(rel)?;
        Some(self.snapshot.chain_root(node, direction.into(), rt))
    }

    /// The single neighbor each node reaches via the *functional* `rel` in `direction`
    /// (one hop — e.g. a message's `hasCreator` -> its creator). The depth-1 sibling of
    /// `roots_via`: where that follows the chain to its terminal, this takes one step.
    /// Returns a `NodeArray` indexed by node id; a node with no such neighbor maps to
    /// `u32::MAX` (4294967295). Built fresh each call (one `first_neighbor` per node,
    /// GIL released), so hold the result for a hot loop. `None` if `rel` is unknown.
    fn neighbor_via(&self, py: Python<'_>, rel: &str, direction: Direction) -> Option<NodeArray> {
        let rt = self.snapshot.relationship_type_from_str(rel)?;
        let dir: rustychickpeas_core::Direction = direction.into();
        let snapshot = self.snapshot.clone();
        let inner: Arc<[u32]> = py.allow_threads(move || {
            let v: Vec<u32> = (0..snapshot.n_nodes)
                .map(|node| snapshot.first_neighbor(node, dir, rt).unwrap_or(u32::MAX))
                .collect();
            v.into()
        });
        let len = inner.len() as ffi::Py_ssize_t;
        Some(NodeArray {
            inner,
            shape: [len],
            strides: [4],
        })
    }

    /// Fold relationship `rel` (in `direction`) into a `PairWeights` map by projecting
    /// both endpoints of each rel through `projection` (a `NodeArray`, e.g. from
    /// `neighbor_via` or `roots_via`) — the one-mode / bipartite projection ("network
    /// folding") of a relation onto a derived node set. For each `rel` rel `a -> b`,
    /// the unordered pair `(min, max)` of `projection[a]` / `projection[b]` gets one
    /// count; self-pairs and endpoints mapping to the `u32::MAX` sentinel are skipped.
    /// Runs the parallel core kernel with the GIL released. The result stays resident
    /// (no per-pair Python object) so it can drive a native weighted `dijkstra` without
    /// a per-rel callback; `to_dict()` materializes it. E.g. BI Q19's person
    /// interaction graph: ``g.fold_via("replyOf", Direction.Outgoing,
    /// g.neighbor_via("hasCreator", Direction.Incoming))``.
    fn fold_via(
        &self,
        py: Python<'_>,
        rel: &str,
        direction: Direction,
        projection: &NodeArray,
    ) -> PairWeights {
        let snapshot = self.snapshot.clone();
        let dir: rustychickpeas_core::Direction = direction.into();
        let rel = rel.to_owned();
        let proj = projection.inner.clone();
        let map: std::collections::HashMap<(u32, u32), u64> =
            py.allow_threads(move || snapshot.fold_via(&rel, dir, proj.as_ref()).into_iter().collect());
        PairWeights {
            inner: Arc::new(map),
        }
    }

    /// Single-source weighted shortest paths (Dijkstra) from `source` along `rel` in
    /// `direction`, with rel costs derived from a resident `weights` map (`PairWeights`,
    /// e.g. from `fold_via`). The cost of rel `(u, v)` is `1.0 / (weights[(u, v)] + base)`;
    /// a pair absent from `weights` is untraversable when `prune_missing` (else costs
    /// `1.0 / base`). Returns `{node_id: cost}` for every node reached (the source maps to
    /// `0.0`); pass `target` to stop once it is settled. The weight lookup runs inside the
    /// native kernel with the GIL released — no per-rel Python callback. E.g. BI Q19's
    /// interaction path: ``g.dijkstra(p1, Direction.Outgoing, "knows", weights=interaction,
    /// base=0.0, prune_missing=True)``.
    #[pyo3(signature = (source, direction, rel, *, weights, base=0.0, prune_missing=false, target=None))]
    fn dijkstra(
        &self,
        py: Python<'_>,
        source: u32,
        direction: Direction,
        rel: &str,
        weights: &PairWeights,
        base: f64,
        prune_missing: bool,
        target: Option<u32>,
    ) -> std::collections::HashMap<u32, f64> {
        let snapshot = self.snapshot.clone();
        let dir: rustychickpeas_core::Direction = direction.into();
        let rel = rel.to_owned();
        let map = weights.inner.clone();
        py.allow_threads(move || {
            let paths = snapshot.dijkstra(source, dir, rel.as_str(), target, |from, r| {
                let key = if from < r.neighbor {
                    (from, r.neighbor)
                } else {
                    (r.neighbor, from)
                };
                match map.get(&key) {
                    Some(&w) => 1.0 / (w as f64 + base),
                    None if prune_missing => f64::INFINITY,
                    None => 1.0 / base,
                }
            });
            paths.into_distances().into_iter().collect()
        })
    }

    /// Build a `NeighborGroups` query over each source node's `rel` neighbors (in
    /// `direction`): group each source's neighbors by a projected attribute and
    /// reduce per source. Nothing runs until a terminal (`.sizes()` /
    /// `.top_by_size(...)`). E.g. BI Q4's biggest single-country membership per
    /// forum: ``g.neighbor_groups(forums, "hasMember", Direction.Outgoing)
    /// .project([(Direction.Outgoing, "isLocatedIn"), (Direction.Outgoing, "isPartOf")])
    /// .top_by_size(100, tie="flid")``.
    fn neighbor_groups(
        &self,
        sources: Vec<u32>,
        rel: String,
        direction: Direction,
    ) -> NeighborGroups {
        NeighborGroups {
            snapshot: self.snapshot.clone(),
            sources,
            rel,
            direction: direction.into(),
            project: Vec::new(),
        }
    }

    /// The string property `key` of `node_id`, or `None` when absent **or empty**
    /// (a dense string column stores a missing value as `""`).
    fn prop_str(&self, node_id: u32, key: &str) -> Option<String> {
        self.snapshot.prop_str(node_id, key).map(str::to_string)
    }

    /// All nodes within `min_hops..=max_hops` of `seed`, expanding only along
    /// `rel_type` in `direction` — the typed k-hop neighborhood as a list of ids
    /// (excludes `seed`). `min_hops` defaults to 1.
    #[pyo3(signature = (seed, direction, rel_type, max_hops, min_hops=1))]
    fn neighborhood(
        &self,
        seed: u32,
        direction: Direction,
        rel_type: &str,
        max_hops: u32,
        min_hops: u32,
    ) -> Vec<u32> {
        self.snapshot
            .neighborhood(seed, direction.into(), rel_type, min_hops..=max_hops)
            .iter()
            .collect()
    }

    /// The dense `i64` column `key` as a list (one value per node id), or `None`
    /// when the column is absent or not a dense `i64` column. Built on the
    /// zero-copy slice reader; the slice is copied to cross PyO3.
    fn i64_column(&self, key: &str) -> Option<Vec<i64>> {
        self.snapshot
            .col(key)
            .map(|c| c.i64())
            .and_then(|c| c.as_slice().map(<[i64]>::to_vec))
    }

    /// The interned id (code) for `s` in this snapshot, or `None` if `s` was never
    /// interned (so no node can carry it). Resolve filter targets to codes once,
    /// then compare them against a string [`Column`]'s codes (dtype `'string'`)
    /// vectorized — e.g. `numpy.isin(numpy.asarray(g.column("lang")), [c1, c2])`.
    fn string_id(&self, s: &str) -> Option<u32> {
        self.get_string_id(s)
    }

    /// A dense property column as a self-describing [`Column`] (its dtype is
    /// intrinsic — no `.i64()` narrowing), or `None` when the key is absent or the
    /// column is not stored densely. The `Column` supports the buffer protocol, so
    /// `numpy.asarray(col)` / `pyarrow.py_buffer(col)` / `memoryview(col)` read it
    /// zero-copy, and `col.to_pylist()` gives a plain Python list.
    fn column(&self, key: &str) -> Option<Column> {
        Column::build(self.snapshot.clone(), key)
    }

    /// Low-level grouped reduction over dense `i64` node columns, run in Rust with
    /// the GIL released. Prefer the fluent [`GraphSnapshot::aggregate`] builder —
    /// this is the kernel it calls, kept public for direct use.
    ///
    /// Scans the nodes of each label in `labels`. A row counts toward `total` when
    /// it passes every `pre_filters` predicate `(column, op, value)` (op ∈
    /// `<,<=,>,>=,==,!=`); rows additionally passing every `group_filters` predicate
    /// are grouped. The group key is the label index (when `group_label`), then each
    /// `group_cols` value, then a bucket index per `group_bins` `(column, bounds)`
    /// (bucket = count of `bounds <= value`). Returns `(rows, total)` with
    /// `rows = [(key_tuple, count, sum), ...]`. All referenced columns must be dense
    /// `i64` columns; a missing label or non-dense/-i64 column raises `ValueError`.
    #[pyo3(signature = (labels, pre_filters=vec![], group_filters=vec![], group_label=false, group_cols=vec![], group_bins=vec![], sum_col=None))]
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn group_reduce(
        &self,
        py: Python<'_>,
        labels: Vec<String>,
        pre_filters: Vec<(String, String, i64)>,
        group_filters: Vec<(String, String, i64)>,
        group_label: bool,
        group_cols: Vec<String>,
        group_bins: Vec<(String, Vec<i64>)>,
        sum_col: Option<String>,
    ) -> PyResult<(Vec<(Vec<i64>, u64, i64)>, u64)> {
        let mut group: Vec<GroupSpec> = group_cols.into_iter().map(GroupSpec::Col).collect();
        group.extend(group_bins.into_iter().map(|(c, e)| GroupSpec::Bin(c, e)));
        let agg = build_core_agg(
            &self.snapshot,
            &labels,
            &pre_filters,
            &group_filters,
            group_label,
            &group,
            sum_col.as_deref(),
            None,
            None,
        )?;
        let res = py
            .allow_threads(|| agg.run())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let rows = res
            .rows
            .into_iter()
            .map(|r| (r.key, r.count, r.sum))
            .collect();
        Ok((rows, res.total))
    }

    /// Start a fluent aggregation over the given node labels — the Pythonic front
    /// for [`group_reduce`](Self::group_reduce). Chain `.where(col, op, value)` /
    /// `.having(col, op, value)` / `.by(col)` / `.bin(col, bounds)` / `.by_label()` /
    /// `.sum(col)`, then `.run()` for a result with `.total` and self-describing dict
    /// `.rows` (the source label comes back as its name). The heavy scan runs in Rust
    /// with the GIL released — no numpy/pyarrow needed.
    #[pyo3(signature = (*labels))]
    fn aggregate(&self, labels: Vec<String>) -> Aggregation {
        Aggregation {
            snapshot: self.snapshot.clone(),
            labels,
            where_filters: Vec::new(),
            having_filters: Vec::new(),
            by_label: false,
            group: Vec::new(),
            sum_col: None,
            through: None,
            neighbor_filter: None,
        }
    }

    /// Weighted (or unweighted) shortest-path cost from `source` to `target`, or
    /// `None` if `target` is unreachable.
    ///
    /// With `weight_property`, each followed rel costs that f64/i64 rel
    /// property (an rel that lacks it is skipped); without it, every rel costs
    /// 1.0 (a hop count). `rel_types` optionally restricts which relationship
    /// types are followed (all types when `None`). Weights must be non-negative;
    /// the search is a bidirectional Dijkstra.
    #[pyo3(signature = (source, target, direction=Direction::Both, rel_types=None, weight_property=None))]
    fn shortest_path(
        &self,
        py: Python<'_>,
        source: u32,
        target: u32,
        direction: Direction,
        rel_types: Option<Vec<String>>,
        weight_property: Option<String>,
    ) -> PyResult<Option<f64>> {
        if source >= self.snapshot.n_nodes || target >= self.snapshot.n_nodes {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Node ID out of range (max: {})",
                self.snapshot.n_nodes.saturating_sub(1)
            )));
        }
        let types: Vec<&str> = rel_types
            .as_ref()
            .map(|t| t.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        let snapshot = self.snapshot.clone();
        // No Python is called during the search, so release the GIL.
        let cost = py.allow_threads(move || {
            let weight = |_from: u32, rel: &RelationshipRef| -> f64 {
                match &weight_property {
                    Some(prop) => match snapshot.rel_prop(rel.pos, prop).map(|p| p.value()) {
                        Some(ValueId::F64(bits)) => f64::from_bits(bits),
                        Some(ValueId::I64(w)) => w as f64,
                        _ => f64::INFINITY,
                    },
                    None => 1.0,
                }
            };
            snapshot.weighted_shortest_path(source, target, direction.into(), &types[..], weight)
        });
        Ok(cost)
    }

    /// Get relationships by type using the type_index bitmap for O(1) lookup
    /// Returns Relationship objects for all relationships of the specified type
    fn relationships_with_type(&self, rel_type: String) -> PyResult<Vec<Relationship>> {
        let rel_type_id = self.rel_type_from_str(&rel_type).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Relationship type '{}' not found",
                rel_type
            ))
        })?;

        if let Some(bitmap) = self.snapshot.type_index.get(&rel_type_id) {
            let relationships: Vec<Relationship> = bitmap
                .iter()
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

    /// Get all nodes (returns all node IDs that have data: labels, rels, or properties)
    fn all_nodes(&self) -> PyResult<Vec<u32>> {
        use std::collections::HashSet;
        let mut nodes = HashSet::new();

        // Add nodes with labels
        for (_label, node_set) in &self.snapshot.label_index {
            for node_id in node_set.iter() {
                nodes.insert(node_id);
            }
        }

        // Add nodes with rels (check CSR arrays)
        // Nodes with outgoing rels
        for node_id in 0..self.snapshot.out_offsets.len().saturating_sub(1) {
            let start = self.snapshot.out_offsets[node_id] as usize;
            let end = self.snapshot.out_offsets[node_id + 1] as usize;
            if start < end {
                nodes.insert(node_id as u32);
            }
        }

        // Nodes with incoming rels
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
                rustychickpeas_core::Column::DenseI64(_)
                | rustychickpeas_core::Column::DenseF64(_)
                | rustychickpeas_core::Column::DenseBool(_)
                | rustychickpeas_core::Column::DenseStr(_) => {
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
                rustychickpeas_core::Column::RankI64 { present, .. }
                | rustychickpeas_core::Column::RankF64 { present, .. }
                | rustychickpeas_core::Column::RankBool { present, .. }
                | rustychickpeas_core::Column::RankStr { present, .. } => {
                    for pos in present.iter_ones() {
                        nodes.insert(pos as u32);
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
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Relationship index {} out of range (max: {})",
                rel_index,
                max_index.saturating_sub(1)
            )));
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
    fn relationship_by_nodes(
        &self,
        start_node: u32,
        end_node: u32,
    ) -> PyResult<Option<Relationship>> {
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
    fn get_property(&self, node_id: u32, key: String) -> PyResult<Option<PyObject>> {
        let key_id = self.property_key_from_str(&key);
        if key_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Property key '{}' not found",
                key
            )));
        }

        let value_id = self.snapshot.prop(node_id, &key).map(|p| p.value());

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
    fn nodes_with_property(&self, label: String, key: String, value: &Bound<'_, PyAny>) -> PyResult<Vec<u32>> {
        // Check if label exists - need to do this before calling nodes_with_property()
        // because it returns None for both "label doesn't exist" and "no matches"
        let label_id = self.label_from_str(&label);
        if label_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Label '{}' not found",
                label
            )));
        }

        // Check if property key exists
        let key_id = self.property_key_from_str(&key);
        if key_id.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Property key '{}' not found",
                key
            )));
        }

        // Both label and key exist, now convert value and query
        let prop_value = py_to_property_value(value)?;
        let value_id = match prop_value {
            rustychickpeas_core::PropertyValue::String(s) => {
                // Need to find string ID
                let sid = self.get_string_id(&s).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Property value string '{}' not found",
                        s
                    ))
                })?;
                ValueId::Str(sid)
            }
            rustychickpeas_core::PropertyValue::Integer(i) => ValueId::I64(i),
            rustychickpeas_core::PropertyValue::Float(f) => ValueId::from_f64(f),
            rustychickpeas_core::PropertyValue::Boolean(b) => ValueId::Bool(b),
            rustychickpeas_core::PropertyValue::InternedString(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "InternedString not supported in GraphSnapshot queries",
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
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (nodes_path=None, relationships_path=None, node_id_column=None, label_columns=None, node_property_columns=None, start_node_column=None, end_node_column=None, rel_type_column=None, rel_property_columns=None))]
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
        let label_cols = label_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let node_prop_cols = node_property_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let rel_prop_cols = rel_property_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());

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

    /// Write this snapshot to an RCPG file on disk. With `topology_only=True`,
    /// omit the property columns for a lean, traversal-only file (per-node data
    /// is expected to live in a record store instead).
    #[pyo3(signature = (path, topology_only=false))]
    fn write_rcpg(&self, path: String, topology_only: bool) -> PyResult<()> {
        if topology_only {
            use rustychickpeas_core::format::rcpg::WriteOptions;
            use std::io::Write;
            let mut file = std::io::BufWriter::new(
                std::fs::File::create(&path)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?,
            );
            self.snapshot
                .write_rcpg_with(&mut file, &WriteOptions::topology_only())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            file.flush()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        } else {
            self.snapshot
                .write_rcpg_file(&path)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        }
        Ok(())
    }

    /// Read a snapshot from an RCPG file on disk (the property index rebuilds
    /// lazily on first use).
    #[staticmethod]
    fn read_rcpg(path: String) -> PyResult<GraphSnapshot> {
        let snapshot = CoreGraphSnapshot::read_rcpg_file(&path)
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
    ///   - Outgoing: Forward search uses outgoing rels, backward uses incoming (default for finding paths from source to target)
    ///   - Incoming: Forward search uses incoming rels, backward uses outgoing (reverse direction)
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
    #[allow(clippy::too_many_arguments)]
    fn bidirectional_bfs(
        &self,
        py: Python<'_>,
        source_nodes: Vec<u32>,
        target_nodes: Vec<u32>,
        direction: Direction,
        rel_types: Option<Vec<String>>,
        node_filter: Option<PyObject>,
        rel_filter: Option<PyObject>,
        max_depth: Option<u32>,
    ) -> PyResult<(Vec<u32>, Vec<u32>)> {
        let source_set = NodeSet::from(RoaringBitmap::from_iter(source_nodes.iter().copied()));
        let target_set = NodeSet::from(RoaringBitmap::from_iter(target_nodes.iter().copied()));

        use rustychickpeas_core::types::Direction as CoreDirection;
        let rust_direction = match direction {
            Direction::Outgoing => CoreDirection::Outgoing,
            Direction::Incoming => CoreDirection::Incoming,
            Direction::Both => CoreDirection::Both,
        };

        let rel_types_str: Option<Vec<&str>> = rel_types
            .as_ref()
            .map(|types| types.iter().map(|s| s.as_str()).collect());

        // Error cell to capture Python exceptions from filter callbacks
        let error_cell: Arc<Mutex<Option<PyErr>>> = Arc::new(Mutex::new(None));

        // Release the GIL for the traversal; filter callbacks re-acquire it
        // per invocation via Python::with_gil.
        let (node_bitmap, rel_bitmap) = py.allow_threads(|| {
            if let (Some(nf_obj), Some(rf_obj)) = (node_filter.as_ref(), rel_filter.as_ref()) {
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
                        if nf_err
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_some()
                        {
                            return false;
                        }
                        Python::with_gil(|py| {
                            match nf_obj
                                .call1(py, (node_id,))
                                .and_then(|r| r.extract::<bool>(py))
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    *nf_err.lock().unwrap_or_else(PoisonError::into_inner) =
                                        Some(e);
                                    false
                                }
                            }
                        })
                    }),
                    Some(
                        move |from: u32,
                              to: u32,
                              rel_type: RelationshipType,
                              csr_pos: u32,
                              snapshot: &CoreGraphSnapshot|
                              -> bool {
                            if rf_err
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .is_some()
                            {
                                return false;
                            }
                            let rf_obj = rf_obj.clone();
                            Python::with_gil(|py| {
                                let rel_type_str = snapshot
                                    .resolve_string(rel_type.id())
                                    .unwrap_or("")
                                    .to_string();
                                match rf_obj
                                    .call1(py, (from, to, rel_type_str, csr_pos))
                                    .and_then(|r| r.extract::<bool>(py))
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        *rf_err.lock().unwrap_or_else(PoisonError::into_inner) =
                                            Some(e);
                                        false
                                    }
                                }
                            })
                        },
                    ),
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
                        if nf_err
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_some()
                        {
                            return false;
                        }
                        Python::with_gil(|py| {
                            match nf_obj
                                .call1(py, (node_id,))
                                .and_then(|r| r.extract::<bool>(py))
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    *nf_err.lock().unwrap_or_else(PoisonError::into_inner) =
                                        Some(e);
                                    false
                                }
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
                    Some(
                        move |from: u32,
                              to: u32,
                              rel_type: RelationshipType,
                              csr_pos: u32,
                              snapshot: &CoreGraphSnapshot|
                              -> bool {
                            if rf_err
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .is_some()
                            {
                                return false;
                            }
                            let rf_obj = rf_obj.clone();
                            Python::with_gil(|py| {
                                let rel_type_str = snapshot
                                    .resolve_string(rel_type.id())
                                    .unwrap_or("")
                                    .to_string();
                                match rf_obj
                                    .call1(py, (from, to, rel_type_str, csr_pos))
                                    .and_then(|r| r.extract::<bool>(py))
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        *rf_err.lock().unwrap_or_else(PoisonError::into_inner) =
                                            Some(e);
                                        false
                                    }
                                }
                            })
                        },
                    ),
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
            }
        });

        // Propagate any Python exception captured during BFS
        if let Some(err) = error_cell
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            return Err(err);
        }

        Ok((node_bitmap.iter().collect(), rel_bitmap.iter().collect()))
    }

    /// BFS traversal from a set of starting nodes
    ///
    /// Performs BFS from the starting nodes, following rels in the specified direction.
    /// Returns all nodes and relationships visited during the traversal.
    ///
    /// # Arguments
    /// * `start_nodes` - List of starting node IDs
    /// * `direction` - Direction of traversal (Direction.Outgoing, Direction.Incoming, or Direction.Both)
    ///   - Outgoing: Follow outgoing rels
    ///   - Incoming: Follow incoming rels
    ///   - Both: Follow both outgoing and incoming rels
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
    #[allow(clippy::too_many_arguments)]
    fn bfs(
        &self,
        py: Python<'_>,
        start_nodes: Vec<u32>,
        direction: Direction,
        rel_types: Option<Vec<String>>,
        node_filter: Option<PyObject>,
        rel_filter: Option<PyObject>,
        max_depth: Option<u32>,
    ) -> PyResult<(Vec<u32>, Vec<u32>)> {
        let start_set = NodeSet::from(RoaringBitmap::from_iter(start_nodes.iter().copied()));

        use rustychickpeas_core::types::Direction as CoreDirection;
        let rust_direction = match direction {
            Direction::Outgoing => CoreDirection::Outgoing,
            Direction::Incoming => CoreDirection::Incoming,
            Direction::Both => CoreDirection::Both,
        };

        let rel_types_str: Option<Vec<&str>> = rel_types
            .as_ref()
            .map(|types| types.iter().map(|s| s.as_str()).collect());

        // Error cell to capture Python exceptions from filter callbacks
        let error_cell: Arc<Mutex<Option<PyErr>>> = Arc::new(Mutex::new(None));

        // Release the GIL for the traversal; filter callbacks re-acquire it
        // per invocation via Python::with_gil.
        let (node_bitmap, rel_bitmap) = py.allow_threads(|| {
            if let (Some(nf_obj), Some(rf_obj)) = (node_filter.as_ref(), rel_filter.as_ref()) {
                let nf_obj = nf_obj.clone();
                let rf_obj = rf_obj.clone();
                let nf_err = error_cell.clone();
                let rf_err = error_cell.clone();
                self.snapshot.bfs(
                    &start_set,
                    rust_direction,
                    rel_types_str.as_deref(),
                    Some(move |node_id: u32, _snapshot: &CoreGraphSnapshot| -> bool {
                        if nf_err
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_some()
                        {
                            return false;
                        }
                        Python::with_gil(|py| {
                            match nf_obj
                                .call1(py, (node_id,))
                                .and_then(|r| r.extract::<bool>(py))
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    *nf_err.lock().unwrap_or_else(PoisonError::into_inner) =
                                        Some(e);
                                    false
                                }
                            }
                        })
                    }),
                    Some(
                        move |from: u32,
                              to: u32,
                              rel_type: RelationshipType,
                              csr_pos: u32,
                              snapshot: &CoreGraphSnapshot|
                              -> bool {
                            if rf_err
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .is_some()
                            {
                                return false;
                            }
                            let rf_obj = rf_obj.clone();
                            Python::with_gil(|py| {
                                let rel_type_str = snapshot
                                    .resolve_string(rel_type.id())
                                    .unwrap_or("")
                                    .to_string();
                                match rf_obj
                                    .call1(py, (from, to, rel_type_str, csr_pos))
                                    .and_then(|r| r.extract::<bool>(py))
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        *rf_err.lock().unwrap_or_else(PoisonError::into_inner) =
                                            Some(e);
                                        false
                                    }
                                }
                            })
                        },
                    ),
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
                        if nf_err
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_some()
                        {
                            return false;
                        }
                        Python::with_gil(|py| {
                            match nf_obj
                                .call1(py, (node_id,))
                                .and_then(|r| r.extract::<bool>(py))
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    *nf_err.lock().unwrap_or_else(PoisonError::into_inner) =
                                        Some(e);
                                    false
                                }
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
                    Some(
                        move |from: u32,
                              to: u32,
                              rel_type: RelationshipType,
                              csr_pos: u32,
                              snapshot: &CoreGraphSnapshot|
                              -> bool {
                            if rf_err
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .is_some()
                            {
                                return false;
                            }
                            let rf_obj = rf_obj.clone();
                            Python::with_gil(|py| {
                                let rel_type_str = snapshot
                                    .resolve_string(rel_type.id())
                                    .unwrap_or("")
                                    .to_string();
                                match rf_obj
                                    .call1(py, (from, to, rel_type_str, csr_pos))
                                    .and_then(|r| r.extract::<bool>(py))
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        *rf_err.lock().unwrap_or_else(PoisonError::into_inner) =
                                            Some(e);
                                        false
                                    }
                                }
                            })
                        },
                    ),
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
            }
        });

        // Propagate any Python exception captured during BFS
        if let Some(err) = error_cell
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            return Err(err);
        }

        Ok((node_bitmap.iter().collect(), rel_bitmap.iter().collect()))
    }

    /// Shortest hop-distance from `start` to every node reachable along `rel_types`
    /// in `direction`, bounded to `max_depth` hops. Returns `{node_id: distance}`
    /// (start is distance 0); `rel_types=None` follows every type. The typed
    /// bounded BFS behind hop-distance filters (e.g. "friends 3..4 hops away").
    #[pyo3(signature = (start, direction, *, rel_types=None, max_depth=None))]
    fn bfs_distances(
        &self,
        py: Python<'_>,
        start: u32,
        direction: Direction,
        rel_types: Option<Vec<String>>,
        max_depth: Option<u32>,
    ) -> std::collections::HashMap<u32, u32> {
        let snapshot = self.snapshot.clone();
        let dir: rustychickpeas_core::types::Direction = direction.into();
        py.allow_threads(move || {
            let types: Vec<&str> = rel_types
                .as_ref()
                .map(|t| t.iter().map(|s| s.as_str()).collect())
                .unwrap_or_default();
            snapshot
                .bfs_distances(start, dir, types.as_slice(), max_depth)
                .into_iter()
                .collect()
        })
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
        if from_node >= self.snapshot.n_nodes {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "from_node {} out of range (max: {})",
                from_node,
                self.snapshot.n_nodes.saturating_sub(1)
            )));
        }
        if to_node >= self.snapshot.n_nodes {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "to_node {} out of range (max: {})",
                to_node,
                self.snapshot.n_nodes.saturating_sub(1)
            )));
        }

        use rustychickpeas_core::types::Direction as CoreDirection;
        let rust_direction = match direction {
            Direction::Outgoing => CoreDirection::Outgoing,
            Direction::Incoming => CoreDirection::Incoming,
            Direction::Both => CoreDirection::Both,
        };

        let rel_types_str: Option<Vec<&str>> = rel_types
            .as_ref()
            .map(|types| types.iter().map(|s| s.as_str()).collect());

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

/// A dense property column exposed to Python as a self-describing, buffer-protocol
/// array (its dtype is intrinsic — no `.i64()` narrowing). Built by
/// [`GraphSnapshot::column`]; holds an `Arc` to the snapshot so the zero-copy
/// buffer it hands out (numpy / pyarrow / memoryview) stays valid for its lifetime.
#[pyclass(name = "Column")]
pub struct Column {
    snapshot: Arc<CoreGraphSnapshot>,
    key: String,
    dtype: ColumnDtype,
    len: usize,
    itemsize: isize,
    // One-element shape/strides the buffer view points at (must outlive the view;
    // they live in this object, which view.obj keeps alive).
    shape: [ffi::Py_ssize_t; 1],
    strides: [ffi::Py_ssize_t; 1],
    // Booleans are bit-packed in core; expanded to one 0/1 byte per node so the
    // buffer has a standard layout. `None` (zero-copy) for the other dtypes.
    bool_bytes: Option<Vec<u8>>,
}

impl Column {
    /// Build a Column for a dense node column, or `None` if absent / not dense.
    fn build(snapshot: Arc<CoreGraphSnapshot>, key: &str) -> Option<Column> {
        let col = snapshot.col(key)?;
        let dtype = col.dtype();
        let (len, itemsize, bool_bytes) = match dtype {
            ColumnDtype::I64 => (col.i64().as_slice()?.len(), 8isize, None),
            ColumnDtype::F64 => (col.f64().as_slice()?.len(), 8, None),
            ColumnDtype::Str => (col.str().as_ids()?.len(), 4, None),
            ColumnDtype::Bool => {
                let bytes: Vec<u8> = col.bool().as_slice()?.iter().map(|b| *b as u8).collect();
                (bytes.len(), 1, Some(bytes))
            }
        };
        Some(Column {
            snapshot,
            key: key.to_string(),
            dtype,
            len,
            itemsize,
            shape: [len as ffi::Py_ssize_t],
            strides: [itemsize as ffi::Py_ssize_t],
            bool_bytes,
        })
    }

    /// Raw data pointer + struct-format char for the buffer view. The pointer is
    /// into the immutable snapshot (or `bool_bytes`) and is stable for the lifetime.
    fn buffer_ptr_format(&self) -> (*const u8, &'static [u8]) {
        match self.dtype {
            ColumnDtype::I64 => {
                let s = self.snapshot.col(&self.key).unwrap().i64().as_slice().unwrap();
                (s.as_ptr() as *const u8, b"q\0")
            }
            ColumnDtype::F64 => {
                let s = self.snapshot.col(&self.key).unwrap().f64().as_slice().unwrap();
                (s.as_ptr() as *const u8, b"d\0")
            }
            ColumnDtype::Str => {
                let s = self.snapshot.col(&self.key).unwrap().str().as_ids().unwrap();
                (s.as_ptr() as *const u8, b"I\0")
            }
            ColumnDtype::Bool => (self.bool_bytes.as_ref().unwrap().as_ptr(), b"B\0"),
        }
    }
}

#[pymethods]
impl Column {
    /// The numpy/struct dtype name: 'int64' | 'float64' | 'bool' | 'string'.
    #[getter]
    fn dtype(&self) -> &'static str {
        match self.dtype {
            ColumnDtype::I64 => "int64",
            ColumnDtype::F64 => "float64",
            ColumnDtype::Bool => "bool",
            ColumnDtype::Str => "string",
        }
    }

    fn __len__(&self) -> usize {
        self.len
    }

    fn __repr__(&self) -> String {
        format!(
            "Column(key='{}', dtype='{}', len={})",
            self.key,
            self.dtype(),
            self.len
        )
    }

    /// The column as a plain Python list. String columns resolve interned ids to
    /// `str`; numeric/bool columns return `int` / `float` / `bool`.
    fn to_pylist(&self, py: Python<'_>) -> PyResult<PyObject> {
        match self.dtype {
            ColumnDtype::I64 => self
                .snapshot
                .col(&self.key)
                .unwrap()
                .i64()
                .as_slice()
                .unwrap()
                .to_vec()
                .into_py_any(py),
            ColumnDtype::F64 => self
                .snapshot
                .col(&self.key)
                .unwrap()
                .f64()
                .as_slice()
                .unwrap()
                .to_vec()
                .into_py_any(py),
            ColumnDtype::Bool => {
                let v: Vec<bool> = self
                    .bool_bytes
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|&b| b != 0)
                    .collect();
                v.into_py_any(py)
            }
            ColumnDtype::Str => {
                let ids = self.snapshot.col(&self.key).unwrap().str().as_ids().unwrap();
                let v: Vec<&str> = ids
                    .iter()
                    .map(|&id| self.snapshot.resolve_string(id).unwrap_or(""))
                    .collect();
                v.into_py_any(py)
            }
        }
    }

    /// Buffer protocol: expose the dense bytes zero-copy (read-only, 1-D,
    /// C-contiguous). `view.obj` takes a new reference to this Column so the backing
    /// memory stays alive while the view is held.
    unsafe fn __getbuffer__(
        slf: PyRef<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(pyo3::exceptions::PyBufferError::new_err("view is null"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "column buffer is read-only",
            ));
        }
        let (ptr, format) = slf.buffer_ptr_format();
        let obj = slf.as_ptr();
        ffi::Py_INCREF(obj);
        (*view).obj = obj;
        (*view).buf = ptr as *mut c_void;
        (*view).len = (slf.len as isize) * slf.itemsize;
        (*view).readonly = 1;
        (*view).itemsize = slf.itemsize;
        (*view).ndim = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            format.as_ptr() as *mut c_char
        } else {
            std::ptr::null_mut()
        };
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            slf.shape.as_ptr() as *mut ffi::Py_ssize_t
        } else {
            std::ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            slf.strides.as_ptr() as *mut ffi::Py_ssize_t
        } else {
            std::ptr::null_mut()
        };
        (*view).suboffsets = std::ptr::null_mut();
        (*view).internal = std::ptr::null_mut();
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
        // Nothing to free: format is static, shape/strides live in self, and
        // CPython decrefs view.obj.
    }
}

/// A `node -> node id` array, indexed by node id: `arr[node]` or `memoryview(arr)`
/// for a hot loop (zero-copy buffer, format 'I'/u32; `u32::MAX` = none). Returned by
/// [`GraphSnapshot::roots_via`] (the chain terminal of a functional relation) and
/// [`GraphSnapshot::neighbor_via`] (its one-hop neighbor).
#[pyclass]
pub struct NodeArray {
    inner: Arc<[u32]>,
    shape: [ffi::Py_ssize_t; 1],
    strides: [ffi::Py_ssize_t; 1],
}

#[pymethods]
impl NodeArray {
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __getitem__(&self, index: isize) -> PyResult<u32> {
        let n = self.inner.len() as isize;
        let i = if index < 0 { index + n } else { index };
        if i < 0 || i >= n {
            return Err(pyo3::exceptions::PyIndexError::new_err("node id out of range"));
        }
        Ok(self.inner[i as usize])
    }

    fn __repr__(&self) -> String {
        format!("NodeArray(len={})", self.inner.len())
    }

    /// The whole array as a Python list of node ids.
    fn to_pylist(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.inner.to_vec().into_py_any(py)
    }

    /// Buffer protocol: expose the u32 array zero-copy (read-only, 1-D, format 'I').
    unsafe fn __getbuffer__(
        slf: PyRef<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(pyo3::exceptions::PyBufferError::new_err("view is null"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "roots buffer is read-only",
            ));
        }
        let obj = slf.as_ptr();
        ffi::Py_INCREF(obj);
        (*view).obj = obj;
        (*view).buf = slf.inner.as_ptr() as *mut c_void;
        (*view).len = (slf.inner.len() as isize) * 4;
        (*view).readonly = 1;
        (*view).itemsize = 4;
        (*view).ndim = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            b"I\0".as_ptr() as *mut c_char
        } else {
            std::ptr::null_mut()
        };
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            slf.shape.as_ptr() as *mut ffi::Py_ssize_t
        } else {
            std::ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            slf.strides.as_ptr() as *mut ffi::Py_ssize_t
        } else {
            std::ptr::null_mut()
        };
        (*view).suboffsets = std::ptr::null_mut();
        (*view).internal = std::ptr::null_mut();
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

/// An immutable `(node, node) -> count` map keyed by the *unordered* pair — the
/// resident result of [`GraphSnapshot::fold_via`] (a one-mode projection). Kept native
/// so it can drive a weighted [`GraphSnapshot::dijkstra`] without a per-rel Python
/// callback; dict-like for inspection (`pw[(a, b)]`, `(a, b) in pw`, `len(pw)`,
/// `pw.to_dict()`). Lookups normalize the key to `(min, max)`.
#[pyclass]
pub struct PairWeights {
    inner: Arc<std::collections::HashMap<(u32, u32), u64>>,
}

#[pymethods]
impl PairWeights {
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __contains__(&self, key: (u32, u32)) -> bool {
        let (a, b) = key;
        let k = if a < b { (a, b) } else { (b, a) };
        self.inner.contains_key(&k)
    }

    fn __getitem__(&self, key: (u32, u32)) -> PyResult<u64> {
        let (a, b) = key;
        let k = if a < b { (a, b) } else { (b, a) };
        self.inner
            .get(&k)
            .copied()
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!("{:?}", key)))
    }

    /// The count for the unordered pair `(a, b)`, or `default` (`None`) when absent.
    #[pyo3(signature = (a, b, default=None))]
    fn get(&self, a: u32, b: u32, default: Option<u64>) -> Option<u64> {
        let k = if a < b { (a, b) } else { (b, a) };
        self.inner.get(&k).copied().or(default)
    }

    fn __repr__(&self) -> String {
        format!("PairWeights(pairs={})", self.inner.len())
    }

    /// Materialize as a Python dict `{(a, b): count}` (keys are `(min, max)`).
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let d = pyo3::types::PyDict::new(py);
        for (&(a, b), &c) in self.inner.iter() {
            d.set_item((a, b), c)?;
        }
        d.into_py_any(py)
    }
}

/// One group dimension for the Python-side aggregation spec: a raw `i64` column,
/// or a column bucketed by ascending `bounds`.
#[derive(Clone)]
enum GroupSpec {
    Col(String),
    Bin(String, Vec<i64>),
}

/// Build the core [`rustychickpeas_core::Aggregation`] from a Python-side spec.
/// All the scan/parallelism lives in core; this just translates the spec.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn build_core_agg<'a>(
    snapshot: &'a CoreGraphSnapshot,
    labels: &[String],
    where_filters: &[(String, String, i64)],
    having_filters: &[(String, String, i64)],
    by_label: bool,
    group: &[GroupSpec],
    sum_col: Option<&str>,
    through: Option<(&str, rustychickpeas_core::types::Direction)>,
    neighbor_filter: Option<&[u32]>,
) -> PyResult<rustychickpeas_core::Aggregation<'a>> {
    let op = |s: &str| {
        AggOp::parse(s).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    };
    let mut agg = snapshot.aggregate(labels.iter().cloned());
    for (c, o, v) in where_filters {
        agg = agg.filter(c.clone(), op(o)?, *v);
    }
    for (c, o, v) in having_filters {
        agg = agg.having(c.clone(), op(o)?, *v);
    }
    if by_label {
        agg = agg.by_label();
    }
    for gs in group {
        agg = match gs {
            GroupSpec::Col(c) => agg.by(c.clone()),
            GroupSpec::Bin(c, e) => agg.bin(c.clone(), e.clone()),
        };
    }
    if let Some(c) = sum_col {
        agg = agg.sum(c);
    }
    if let Some((rt, dir)) = through {
        agg = agg.through(rt, dir);
    }
    if let Some(ids) = neighbor_filter {
        agg = agg.only_neighbors(ids.iter().copied());
    }
    Ok(agg)
}

/// Lazy neighbor-grouping query (see `GraphSnapshot.neighbor_groups`). Immutable:
/// `.project(...)` returns a new builder; `.sizes()` / `.top_by_size(...)` run it
/// in parallel with the GIL released.
#[pyclass]
#[derive(Clone)]
pub struct NeighborGroups {
    snapshot: Arc<CoreGraphSnapshot>,
    sources: Vec<u32>,
    rel: String,
    direction: rustychickpeas_core::types::Direction,
    project: Vec<(rustychickpeas_core::types::Direction, String)>,
}

#[pymethods]
impl NeighborGroups {
    /// Project each neighbor to its group node via a `follow`-style list of
    /// `(direction, rel_type)` steps. Returns a new builder.
    fn project(&self, steps: Vec<(Direction, String)>) -> NeighborGroups {
        NeighborGroups {
            snapshot: self.snapshot.clone(),
            sources: self.sources.clone(),
            rel: self.rel.clone(),
            direction: self.direction,
            project: steps.into_iter().map(|(d, r)| (d.into(), r)).collect(),
        }
    }

    /// Per source, the size of its largest cohort: `(source, size)`.
    fn sizes(&self, py: Python<'_>) -> Vec<(u32, u32)> {
        let snapshot = self.snapshot.clone();
        let sources = self.sources.clone();
        let rel = self.rel.clone();
        let direction = self.direction;
        let project = self.project.clone();
        py.allow_threads(move || {
            let steps: Vec<(rustychickpeas_core::types::Direction, &str)> =
                project.iter().map(|(d, r)| (*d, r.as_str())).collect();
            snapshot
                .neighbor_groups(&sources, &rel, direction)
                .project(&steps)
                .sizes()
        })
    }

    /// The top `n` sources by largest cohort size: `(source, size)`, size
    /// descending. Ties break by the `tie` node property (read as i64, ascending)
    /// when given — so the order can match a query's output-id ordering — else by
    /// source id ascending.
    #[pyo3(signature = (n, tie=None))]
    fn top_by_size(&self, py: Python<'_>, n: usize, tie: Option<String>) -> Vec<(u32, u32)> {
        let snapshot = self.snapshot.clone();
        let sources = self.sources.clone();
        let rel = self.rel.clone();
        let direction = self.direction;
        let project = self.project.clone();
        py.allow_threads(move || {
            let steps: Vec<(rustychickpeas_core::types::Direction, &str)> =
                project.iter().map(|(d, r)| (*d, r.as_str())).collect();
            snapshot
                .neighbor_groups(&sources, &rel, direction)
                .project(&steps)
                .top_by_size(n, tie.as_deref())
        })
    }
}

/// Fluent aggregation builder (immutable: each step returns a new builder), created
/// by [`GraphSnapshot::aggregate`]. `.run()` executes the scan and returns an
/// [`AggResult`].
#[pyclass(name = "Aggregation")]
#[derive(Clone)]
pub struct Aggregation {
    snapshot: Arc<CoreGraphSnapshot>,
    labels: Vec<String>,
    where_filters: Vec<(String, String, i64)>,
    having_filters: Vec<(String, String, i64)>,
    by_label: bool,
    group: Vec<GroupSpec>,
    sum_col: Option<String>,
    through: Option<(String, rustychickpeas_core::types::Direction)>,
    neighbor_filter: Option<Vec<u32>>,
}

#[pymethods]
impl Aggregation {
    fn __repr__(&self) -> String {
        format!(
            "Aggregation(labels={:?}, where={}, having={}, group_dims={}, sum={:?})",
            self.labels,
            self.where_filters.len(),
            self.having_filters.len(),
            self.by_label as usize + self.group.len(),
            self.sum_col,
        )
    }

    /// Population predicate `column op value` (op ∈ `<,<=,>,>=,==,!=`); rows passing
    /// all of these count toward `total`.
    #[pyo3(name = "where")]
    fn where_(&self, column: String, op: String, value: i64) -> Aggregation {
        let mut a = self.clone();
        a.where_filters.push((column, op, value));
        a
    }

    /// Extra predicate applied to grouped rows only (after the population filters).
    fn having(&self, column: String, op: String, value: i64) -> Aggregation {
        let mut a = self.clone();
        a.having_filters.push((column, op, value));
        a
    }

    /// Group by the source node label (returned in rows as its name).
    fn by_label(&self) -> Aggregation {
        let mut a = self.clone();
        a.by_label = true;
        a
    }

    /// Group by a dense `i64` column's value.
    fn by(&self, column: String) -> Aggregation {
        let mut a = self.clone();
        a.group.push(GroupSpec::Col(column));
        a
    }

    /// Group by a column bucketed at ascending `bounds` (bucket = count of
    /// `bounds <= value`); the row field is `"{column}_bin"`.
    fn bin(&self, column: String, bounds: Vec<i64>) -> Aggregation {
        let mut a = self.clone();
        a.group.push(GroupSpec::Bin(column, bounds));
        a
    }

    /// Also sum this `i64` column per group (row field `"sum"`).
    fn sum(&self, column: String) -> Aggregation {
        let mut a = self.clone();
        a.sum_col = Some(column);
        a
    }

    /// Count rels of `rel_type`/`direction` out of each source node instead of
    /// counting nodes, grouping additionally by the neighbor id (row field
    /// `"neighbor"`). `total` still counts source nodes.
    fn through(&self, rel_type: String, direction: Direction) -> Aggregation {
        let mut a = self.clone();
        a.through = Some((rel_type, direction.into()));
        a
    }

    /// With `through`, count only neighbors whose node id is in `node_ids` (others
    /// skipped), so `.rows` has just those neighbors.
    fn only_neighbors(&self, node_ids: Vec<u32>) -> Aggregation {
        let mut a = self.clone();
        a.neighbor_filter = Some(node_ids);
        a
    }

    /// Execute: returns an [`AggResult`] with `.total` and self-describing dict
    /// `.rows` (keys: the group fields, then `"count"` and `"sum"` if requested).
    fn run(&self, py: Python<'_>) -> PyResult<AggResult> {
        let agg = build_core_agg(
            &self.snapshot,
            &self.labels,
            &self.where_filters,
            &self.having_filters,
            self.by_label,
            &self.group,
            self.sum_col.as_deref(),
            self.through.as_ref().map(|(rt, dir)| (rt.as_str(), *dir)),
            self.neighbor_filter.as_deref(),
        )?;
        // The parallel scan lives in core; release the GIL while it runs.
        let res = py
            .allow_threads(|| agg.run())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let has_sum = self.sum_col.is_some();

        let rows = pyo3::types::PyList::empty(py);
        for row in &res.rows {
            let d = pyo3::types::PyDict::new(py);
            for (pos, (field, val)) in res.fields.iter().zip(row.key.iter()).enumerate() {
                if self.by_label && pos == 0 {
                    // The label key is its index into `labels`; emit the name.
                    d.set_item(field, &self.labels[*val as usize])?;
                } else {
                    d.set_item(field, *val)?;
                }
            }
            d.set_item("count", row.count)?;
            if has_sum {
                d.set_item("sum", row.sum)?;
            }
            rows.append(d)?;
        }
        Ok(AggResult {
            total: res.total,
            rows: rows.into_any().unbind(),
        })
    }
}

/// Result of [`Aggregation::run`]: `total` (population count) and `rows`
/// (a list of self-describing dicts).
#[pyclass(name = "AggResult")]
pub struct AggResult {
    #[pyo3(get)]
    total: u64,
    #[pyo3(get)]
    rows: PyObject,
}

#[pymethods]
impl AggResult {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let n = self.rows.bind(py).len()?;
        Ok(format!("AggResult(total={}, rows={} groups)", self.total, n))
    }
}
