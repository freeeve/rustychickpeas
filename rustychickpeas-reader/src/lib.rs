//! Split-residency reader for RustyChickpeas graphs.
//!
//! The residency model: the RCPG graph file (topology, label/type indexes,
//! string table) loads **fully into memory**, so traversals run locally with
//! zero network round trips per hop. Per-node records stay remote in an RRSR
//! record store and are read through batched byte-range fetches.
//!
//! - [`GraphReader`] — resident traversal over parsed RCPG bytes.
//! - [`records`] — re-exports roaringrange's `RecordStore`/`RangeFetch`
//!   (RRSR v1 + v2/zstd) for the remote side; pair with
//!   `rustychickpeas_format::rrsr::RecordIndex::plan_ranges` when driving
//!   fetches yourself.
//! - `wasm` feature — browser bindings; see the `wasm` module.
//!
//! Memory budget: resident adjacency costs ~8 bytes per relationship
//! (u32 neighbor in each direction) plus offsets and type arrays. Practical
//! ceiling in a browser tab is on the order of tens of millions of
//! relationships; beyond that a range-fetched adjacency variant is needed
//! (out of scope here).

use std::collections::HashMap;

use roaring::RoaringBitmap;
use rustychickpeas_format::{rcpg, ColumnData, FormatError, GraphSection};

pub mod records;

#[cfg(feature = "wasm")]
pub mod wasm;

/// Traversal direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

/// A property value read from a resident column. String values are resolved
/// from atom IDs to borrowed strings, so the lifetime is tied to the reader.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PropValue<'a> {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(&'a str),
}

/// Resident, read-only graph parsed from RCPG bytes.
pub struct GraphReader {
    graph: GraphSection,
    atom_ids: HashMap<String, u32>,
}

impl GraphReader {
    /// Parse RCPG bytes (the whole file) into a resident reader,
    /// materializing every section including property columns. Prefer
    /// [`GraphReader::topology_only`] for large graphs unless you need
    /// resident properties.
    pub fn from_rcpg_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        Self::from_rcpg_bytes_with(bytes, &rcpg::ParseOptions::default())
    }

    /// Parse RCPG bytes keeping only topology resident (adjacency,
    /// label/type indexes, atoms); property column sections are skipped
    /// even when the file contains them. Per-node data should come from a
    /// range-fetched record store instead.
    pub fn topology_only(bytes: &[u8]) -> Result<Self, FormatError> {
        Self::from_rcpg_bytes_with(bytes, &rcpg::ParseOptions::topology_only())
    }

    /// Parse RCPG bytes with explicit section options.
    pub fn from_rcpg_bytes_with(
        bytes: &[u8],
        opts: &rcpg::ParseOptions,
    ) -> Result<Self, FormatError> {
        let graph = rcpg::parse_with(bytes, opts)?;
        let atom_ids = graph
            .atoms
            .iter()
            .enumerate()
            .map(|(id, s)| (s.clone(), id as u32))
            .collect();
        Ok(GraphReader { graph, atom_ids })
    }

    /// The underlying parsed graph.
    pub fn graph(&self) -> &GraphSection {
        &self.graph
    }

    /// Actual node count (not the CSR ID-space size).
    pub fn node_count(&self) -> u32 {
        self.graph.n_nodes
    }

    /// Relationship count.
    pub fn relationship_count(&self) -> u64 {
        self.graph.n_rels
    }

    /// CSR ID-space size; valid node IDs are `0..csr_id_space()`, though
    /// IDs never added have empty adjacency and no labels.
    pub fn csr_id_space(&self) -> u32 {
        self.graph.csr_id_space()
    }

    /// Resolve an atom ID to its string.
    pub fn atom(&self, id: u32) -> Option<&str> {
        self.graph.atoms.get(id as usize).map(|s| s.as_str())
    }

    /// Look up the atom ID for a string.
    pub fn atom_id(&self, s: &str) -> Option<u32> {
        self.atom_ids.get(s).copied()
    }

    /// Neighbors of `node_id` in the given direction (empty for unknown IDs).
    /// [`Direction::Both`] returns outgoing neighbors followed by incoming ones.
    pub fn neighbors(&self, node_id: u32, direction: Direction) -> Vec<u32> {
        match direction {
            Direction::Outgoing => self.graph.out_neighbors(node_id).to_vec(),
            Direction::Incoming => self.graph.in_neighbors(node_id).to_vec(),
            Direction::Both => {
                let mut neighbors = self.graph.out_neighbors(node_id).to_vec();
                neighbors.extend_from_slice(self.graph.in_neighbors(node_id));
                neighbors
            }
        }
    }

    /// Neighbors of `node_id` in the given direction reached via a relationship
    /// type. [`Direction::Both`] returns outgoing neighbors followed by incoming.
    pub fn neighbors_by_type(
        &self,
        node_id: u32,
        direction: Direction,
        rel_type: &str,
    ) -> Vec<u32> {
        let Some(type_atom) = self.atom_id(rel_type) else {
            return Vec::new();
        };
        let mut neighbors = Vec::new();
        if matches!(direction, Direction::Outgoing | Direction::Both) {
            neighbors.extend(self.neighbors_by_type_dir(node_id, type_atom, Direction::Outgoing));
        }
        if matches!(direction, Direction::Incoming | Direction::Both) {
            neighbors.extend(self.neighbors_by_type_dir(node_id, type_atom, Direction::Incoming));
        }
        neighbors
    }

    /// Single-direction relationship-type neighbor scan over the resident CSR.
    fn neighbors_by_type_dir(&self, node_id: u32, type_atom: u32, dir: Direction) -> Vec<u32> {
        let (offsets, nbrs, types) = match dir {
            Direction::Outgoing => (
                &self.graph.out_offsets,
                &self.graph.out_nbrs,
                &self.graph.out_types,
            ),
            _ => (
                &self.graph.in_offsets,
                &self.graph.in_nbrs,
                &self.graph.in_types,
            ),
        };
        let i = node_id as usize;
        if i + 1 >= offsets.len() {
            return Vec::new();
        }
        let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
        if start > end || end > nbrs.len() || end > types.len() {
            return Vec::new();
        }
        (start..end)
            .filter(|&k| types[k] == type_atom)
            .map(|k| nbrs[k])
            .collect()
    }

    /// Labels of a node (scans the label index bitmaps).
    pub fn node_labels(&self, node_id: u32) -> Vec<&str> {
        self.graph
            .label_index
            .iter()
            .filter(|(_, bm)| bm.contains(node_id))
            .filter_map(|(atom, _)| self.atom(*atom))
            .collect()
    }

    /// Value of node property `key` for `node_id`, or `None` when the key is
    /// unknown, its column wasn't loaded (e.g. parsed via
    /// [`GraphReader::topology_only`]), or the node carries no value in a
    /// sparse column. String values resolve from atom IDs to borrowed strings.
    /// Mirrors core's `Column::get` — dense columns index directly, sparse
    /// columns binary-search the sorted `(id, value)` pairs.
    pub fn node_prop(&self, node_id: u32, key: &str) -> Option<PropValue<'_>> {
        let key_atom = self.atom_id(key)?;
        let col = self
            .graph
            .node_columns
            .binary_search_by_key(&key_atom, |(k, _)| *k)
            .ok()
            .map(|i| &self.graph.node_columns[i].1)?;
        self.column_value(col, node_id)
    }

    /// Read the value at `id` from a resident column, resolving string atoms.
    fn column_value(&self, col: &ColumnData, id: u32) -> Option<PropValue<'_>> {
        let i = id as usize;
        let atoms = &self.graph.atoms;
        match col {
            ColumnData::DenseI64(v) => v.get(i).map(|&x| PropValue::Int(x)),
            ColumnData::DenseF64(v) => v.get(i).map(|&x| PropValue::Float(x)),
            ColumnData::DenseBool(b) => b.get(i).map(|bit| PropValue::Bool(*bit)),
            ColumnData::DenseStr(v) => v.get(i).map(|&a| PropValue::Str(atom_str(atoms, a))),
            ColumnData::SparseI64(p) => sparse_get(p, id).map(PropValue::Int),
            ColumnData::SparseF64(p) => sparse_get(p, id).map(PropValue::Float),
            ColumnData::SparseBool(p) => sparse_get(p, id).map(PropValue::Bool),
            ColumnData::SparseStr(p) => {
                sparse_get(p, id).map(|a| PropValue::Str(atom_str(atoms, a)))
            }
        }
    }

    /// Outgoing rels of `node_id` as `(neighbor, csr_pos)` pairs. `csr_pos` is
    /// the outgoing-CSR position that keys relationship-property columns, so
    /// pair it with [`GraphReader::rel_prop`] to filter traversal by an rel
    /// property. Empty for IDs outside the CSR space.
    pub fn out_rels(&self, node_id: u32) -> Vec<(u32, u32)> {
        let i = node_id as usize;
        let offsets = &self.graph.out_offsets;
        if i + 1 >= offsets.len() {
            return Vec::new();
        }
        let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
        if start > end || end > self.graph.out_nbrs.len() {
            return Vec::new();
        }
        (start..end)
            .map(|p| (self.graph.out_nbrs[p], p as u32))
            .collect()
    }

    /// Value of relationship property `key` at outgoing-CSR position `csr_pos`
    /// (from [`GraphReader::out_rels`]), or `None` when the key is unknown, the
    /// column wasn't loaded (e.g. parsed via [`GraphReader::topology_only`]), or
    /// the rel has no value in a sparse column. Relationship properties are
    /// keyed by outgoing-CSR position, so incoming-side lookups aren't
    /// addressable here. Resolves string atoms, mirroring [`GraphReader::node_prop`].
    pub fn rel_prop(&self, csr_pos: u32, key: &str) -> Option<PropValue<'_>> {
        let key_atom = self.atom_id(key)?;
        let col = self
            .graph
            .rel_columns
            .binary_search_by_key(&key_atom, |(k, _)| *k)
            .ok()
            .map(|i| &self.graph.rel_columns[i].1)?;
        self.column_value(col, csr_pos)
    }

    /// Node IDs carrying a label.
    pub fn nodes_with_label(&self, label: &str) -> Option<&RoaringBitmap> {
        let atom = self.atom_id(label)?;
        self.graph
            .label_index
            .iter()
            .find(|(a, _)| *a == atom)
            .map(|(_, bm)| bm)
    }

    /// All label strings present in the graph.
    pub fn labels(&self) -> Vec<&str> {
        self.graph
            .label_index
            .iter()
            .filter_map(|(atom, _)| self.atom(*atom))
            .collect()
    }

    /// All relationship type strings present in the graph.
    pub fn relationship_types(&self) -> Vec<&str> {
        self.graph
            .type_index
            .iter()
            .filter_map(|(atom, _)| self.atom(*atom))
            .collect()
    }

    /// Breadth-first expansion from `start`, up to `max_depth` hops.
    /// Returns visited node IDs (excluding `start`) in BFS order. All work
    /// happens against the resident arrays — no I/O.
    pub fn bfs(&self, start: u32, max_depth: u32, direction: Direction) -> Vec<u32> {
        let mut visited = RoaringBitmap::new();
        visited.insert(start);
        let mut order = Vec::new();
        let mut frontier = vec![start];

        for _ in 0..max_depth {
            let mut next = Vec::new();
            for &node in &frontier {
                let expand = |nbrs: &[u32], next: &mut Vec<u32>, visited: &mut RoaringBitmap| {
                    for &nbr in nbrs {
                        if visited.insert(nbr) {
                            next.push(nbr);
                        }
                    }
                };
                match direction {
                    Direction::Outgoing => {
                        expand(self.graph.out_neighbors(node), &mut next, &mut visited)
                    }
                    Direction::Incoming => {
                        expand(self.graph.in_neighbors(node), &mut next, &mut visited)
                    }
                    Direction::Both => {
                        expand(self.graph.out_neighbors(node), &mut next, &mut visited);
                        expand(self.graph.in_neighbors(node), &mut next, &mut visited);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            order.extend_from_slice(&next);
            frontier = next;
        }
        order
    }

    /// Select the (offsets, neighbors, types) CSR arrays for a single
    /// direction; `Both` is handled by callers scanning each side.
    fn adjacency(&self, dir: Direction) -> (&[u32], &[u32], &[u32]) {
        match dir {
            Direction::Outgoing => (
                &self.graph.out_offsets,
                &self.graph.out_nbrs,
                &self.graph.out_types,
            ),
            _ => (
                &self.graph.in_offsets,
                &self.graph.in_nbrs,
                &self.graph.in_types,
            ),
        }
    }

    /// The first neighbor of `node_id` reached via `rel_type` in `direction`,
    /// or `None`. Short-circuits the CSR scan, so it is cheaper than
    /// [`neighbors_by_type`](Self::neighbors_by_type) when one neighbor is all
    /// you need (a single-cardinality rel like `isLocatedIn`). `Both` prefers
    /// the first outgoing match, else the first incoming.
    pub fn first_neighbor(
        &self,
        node_id: u32,
        direction: Direction,
        rel_type: &str,
    ) -> Option<u32> {
        let type_atom = self.atom_id(rel_type)?;
        if matches!(direction, Direction::Outgoing | Direction::Both) {
            if let Some(n) = self.first_neighbor_dir(node_id, type_atom, Direction::Outgoing) {
                return Some(n);
            }
        }
        if matches!(direction, Direction::Incoming | Direction::Both) {
            if let Some(n) = self.first_neighbor_dir(node_id, type_atom, Direction::Incoming) {
                return Some(n);
            }
        }
        None
    }

    /// First type-matching neighbor in one direction (short-circuit scan).
    fn first_neighbor_dir(&self, node_id: u32, type_atom: u32, dir: Direction) -> Option<u32> {
        let (offsets, nbrs, types) = self.adjacency(dir);
        let i = node_id as usize;
        if i + 1 >= offsets.len() {
            return None;
        }
        let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
        if start > end || end > nbrs.len() || end > types.len() {
            return None;
        }
        (start..end)
            .find(|&k| types[k] == type_atom)
            .map(|k| nbrs[k])
    }

    /// Typed neighbors via a pre-resolved relationship-type atom (both
    /// directions, like [`neighbors_by_type`](Self::neighbors_by_type)), so a
    /// BFS-style expansion resolves the atom once instead of per node.
    fn typed_neighbors(&self, node_id: u32, direction: Direction, type_atom: u32) -> Vec<u32> {
        let mut neighbors = Vec::new();
        if matches!(direction, Direction::Outgoing | Direction::Both) {
            neighbors.extend(self.neighbors_by_type_dir(node_id, type_atom, Direction::Outgoing));
        }
        if matches!(direction, Direction::Incoming | Direction::Both) {
            neighbors.extend(self.neighbors_by_type_dir(node_id, type_atom, Direction::Incoming));
        }
        neighbors
    }

    /// Follow a fixed chain of `(direction, rel_type)` steps from `start`,
    /// taking the [`first_neighbor`](Self::first_neighbor) at each; `None` if a
    /// step has no such neighbor. Empty `steps` returns `start`.
    pub fn follow(&self, start: u32, steps: &[(Direction, &str)]) -> Option<u32> {
        let mut node = start;
        for &(dir, rel) in steps {
            node = self.first_neighbor(node, dir, rel)?;
        }
        Some(node)
    }

    /// Whether `node_id` has any neighbor via `rel_type` in `direction` — an
    /// existence check that short-circuits on the first match.
    pub fn has_rel(&self, node_id: u32, direction: Direction, rel_type: &str) -> bool {
        self.first_neighbor(node_id, direction, rel_type).is_some()
    }

    /// Whether `node_id` has a neighbor (via `rel_type`, `direction`) whose node
    /// property `key` equals `value`. Reads the neighbor property from the
    /// resident columns, so it needs properties loaded (not `topology_only`).
    pub fn has_neighbor_with_property(
        &self,
        node_id: u32,
        direction: Direction,
        rel_type: &str,
        key: &str,
        value: PropValue<'_>,
    ) -> bool {
        self.neighbors_by_type(node_id, direction, rel_type)
            .into_iter()
            .any(|nbr| self.node_prop(nbr, key) == Some(value))
    }

    /// Deduplicated union of the neighbors reached via any of `rel_types` in
    /// `direction`, ascending by id. (The non-deduped, order-preserving form is
    /// a per-type [`neighbors_by_type`](Self::neighbors_by_type) loop.)
    pub fn neighbors_by_types(
        &self,
        node_id: u32,
        direction: Direction,
        rel_types: &[&str],
    ) -> Vec<u32> {
        let mut set = RoaringBitmap::new();
        for &rel_type in rel_types {
            for nbr in self.neighbors_by_type(node_id, direction, rel_type) {
                set.insert(nbr);
            }
        }
        set.iter().collect()
    }

    /// Degree of `node_id` in `direction`, O(1) from the resident CSR offsets
    /// (no scan). `Both` sums the two sides.
    pub fn degree(&self, node_id: u32, direction: Direction) -> u32 {
        let one = |offsets: &[u32]| {
            let i = node_id as usize;
            if i + 1 >= offsets.len() {
                0
            } else {
                offsets[i + 1] - offsets[i]
            }
        };
        match direction {
            Direction::Outgoing => one(&self.graph.out_offsets),
            Direction::Incoming => one(&self.graph.in_offsets),
            Direction::Both => one(&self.graph.out_offsets) + one(&self.graph.in_offsets),
        }
    }

    /// All nodes within `1..=max_hops` of `seed`, expanding only along
    /// `rel_type` in `direction` — the typed k-hop neighborhood, as a
    /// [`RoaringBitmap`] so it composes with label sets. Excludes `seed`; all
    /// work is over the resident CSR (no I/O).
    pub fn neighborhood(
        &self,
        seed: u32,
        direction: Direction,
        rel_type: &str,
        max_hops: u32,
    ) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        let Some(type_atom) = self.atom_id(rel_type) else {
            return result;
        };
        let mut visited = RoaringBitmap::new();
        visited.insert(seed);
        let mut frontier = vec![seed];
        for _ in 0..max_hops {
            let mut next = Vec::new();
            for &node in &frontier {
                for nbr in self.typed_neighbors(node, direction, type_atom) {
                    if visited.insert(nbr) {
                        result.insert(nbr);
                        next.push(nbr);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        result
    }
}

/// Resident trigram search over a fully-loaded `.rrs` index.
///
/// For small indexes that fit in memory — like the browser demo, where the
/// graph is already resident — the whole `.rrs` is loaded and queried in
/// process. Large indexes should be range-fetched with roaringrange directly.
/// Doc IDs returned by [`ResidentSearch::search`] share the graph's node-ID
/// space, so a hit traverses and fetches its record with no remapping.
pub struct ResidentSearch {
    index: roaringrange::Index<roaringrange::MemoryFetch>,
}

impl ResidentSearch {
    /// Open a `.rrs` trigram index from its full bytes.
    pub fn open(rrs_bytes: Vec<u8>) -> std::result::Result<Self, roaringrange::IndexError> {
        let fetch = roaringrange::MemoryFetch::new(rrs_bytes);
        // `MemoryFetch` reads resolve synchronously, so the open future is ready
        // on the first poll — no async runtime needed (this compiles for wasm).
        let index = poll_ready(roaringrange::Index::open(fetch))
            .expect("MemoryFetch open future resolves synchronously")?;
        Ok(ResidentSearch { index })
    }

    /// Doc IDs matching `query`, best-ranked first, up to `limit`. Returns an
    /// empty vec on a query error (e.g. a query shorter than one trigram).
    pub fn search(&self, query: &str, limit: usize) -> Vec<u32> {
        poll_ready(self.index.search(query, limit))
            .and_then(|r| r.ok())
            .unwrap_or_default()
    }
}

/// Drive an immediately-ready future to completion with a single poll, via a
/// no-op waker. Returns `None` if the future is pending (never the case for the
/// in-memory `MemoryFetch`-backed searches here). Keeps an async runtime out of
/// the wasm-safe reader.
fn poll_ready<F: std::future::Future>(fut: F) -> Option<F::Output> {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = Box::pin(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// Binary-search a sorted `(id, value)` sparse column for `id`.
fn sparse_get<V: Copy>(pairs: &[(u32, V)], id: u32) -> Option<V> {
    pairs
        .binary_search_by_key(&id, |(k, _)| *k)
        .ok()
        .map(|i| pairs[i].1)
}

/// Resolve a string atom, falling back to "" for out-of-range atom IDs.
fn atom_str(atoms: &[String], atom: u32) -> &str {
    atoms.get(atom as usize).map_or("", String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Three nodes; one dense i64 column ("year") and one sparse str column
    /// ("name") present on nodes 0 and 2. Round-tripped through the RCPG codec
    /// so the test exercises the real parse path, not a hand-built reader.
    fn reader() -> GraphReader {
        let mut g = GraphSection {
            n_nodes: 3,
            out_offsets: vec![0, 0, 0, 0],
            in_offsets: vec![0, 0, 0, 0],
            atoms: vec![
                String::new(),       // 0 = ""
                "year".to_string(),  // 1 = key
                "name".to_string(),  // 2 = key
                "Alice".to_string(), // 3 = value
                "Bob".to_string(),   // 4 = value
            ],
            ..Default::default()
        };
        g.node_columns = vec![
            (1, ColumnData::DenseI64(vec![2001, 2002, 2003])),
            (2, ColumnData::SparseStr(vec![(0, 3), (2, 4)])),
        ];
        let mut bytes = Vec::new();
        rcpg::write(&g, &mut bytes).unwrap();
        GraphReader::from_rcpg_bytes(&bytes).unwrap()
    }

    #[test]
    fn node_prop_dense_and_sparse() {
        let r = reader();
        assert_eq!(r.node_prop(0, "year"), Some(PropValue::Int(2001)));
        assert_eq!(r.node_prop(2, "year"), Some(PropValue::Int(2003)));
        assert_eq!(r.node_prop(0, "name"), Some(PropValue::Str("Alice")));
        assert_eq!(r.node_prop(2, "name"), Some(PropValue::Str("Bob")));
        // node 1 has no entry in the sparse "name" column
        assert_eq!(r.node_prop(1, "name"), None);
        // unknown key resolves to no column
        assert_eq!(r.node_prop(0, "missing"), None);
    }

    #[test]
    fn node_prop_none_when_topology_only() {
        let mut g = GraphSection {
            n_nodes: 1,
            out_offsets: vec![0, 0],
            in_offsets: vec![0, 0],
            atoms: vec![String::new(), "year".to_string()],
            ..Default::default()
        };
        g.node_columns = vec![(1, ColumnData::DenseI64(vec![2001]))];
        let mut bytes = Vec::new();
        rcpg::write(&g, &mut bytes).unwrap();
        let r = GraphReader::topology_only(&bytes).unwrap();
        // property columns are skipped, so even a present key returns None
        assert_eq!(r.node_prop(0, "year"), None);
    }

    #[test]
    fn resident_search_returns_doc_ids() {
        use roaring::RoaringBitmap;
        // Build a 2-doc trigram index inline (doc 0 = "graph database").
        let docs = ["graph database", "vector search"];
        let mut postings: HashMap<u64, RoaringBitmap> = HashMap::new();
        for (id, text) in docs.iter().enumerate() {
            for key in roaringrange::ngram_keys(text, 3) {
                postings.entry(key).or_default().insert(id as u32);
            }
        }
        let entries: Vec<(u64, Vec<u8>)> = postings
            .into_iter()
            .map(|(k, bm)| (k, roaringrange::build::serialize_posting(&bm)))
            .collect();
        let mut rrs = Vec::new();
        roaringrange::build::write_index(&mut rrs, 3, 0, entries).unwrap();

        let search = ResidentSearch::open(rrs).unwrap();
        let hits = search.search("graph database", 10);
        assert!(hits.contains(&0), "expected doc 0 in {hits:?}");
        assert!(!hits.contains(&1), "doc 1 should not match, got {hits:?}");
    }

    #[test]
    fn rel_prop_over_resident_rel_columns() {
        // Two nodes, one rel 0->1 (CITES) carrying a "weight" property = 42.
        let mut g = GraphSection {
            n_nodes: 2,
            n_rels: 1,
            out_offsets: vec![0, 1, 1],
            out_nbrs: vec![1],
            out_types: vec![2], // atom 2 = "CITES"
            in_offsets: vec![0, 0, 1],
            in_nbrs: vec![0],
            in_types: vec![2],
            atoms: vec![
                String::new(),        // 0
                "weight".to_string(), // 1 = int rel-prop key
                "CITES".to_string(),  // 2 = rel type
                "score".to_string(),  // 3 = float rel-prop key
            ],
            ..Default::default()
        };
        // rel columns are sorted by key atom: weight(1) then score(3).
        g.rel_columns = vec![
            (1, ColumnData::DenseI64(vec![42])),
            (3, ColumnData::DenseF64(vec![0.75])),
        ];
        let mut bytes = Vec::new();
        rcpg::write(&g, &mut bytes).unwrap();
        let r = GraphReader::from_rcpg_bytes(&bytes).unwrap();

        // out_rels yields (neighbor, csr_pos); the only rel is at position 0.
        assert_eq!(r.out_rels(0), vec![(1, 0)]);
        assert_eq!(r.out_rels(1), Vec::new());
        // rel_prop reads the rel's properties at that position — int and float.
        assert_eq!(r.rel_prop(0, "weight"), Some(PropValue::Int(42)));
        assert_eq!(r.rel_prop(0, "score"), Some(PropValue::Float(0.75)));
        assert_eq!(r.rel_prop(0, "missing"), None);

        // topology_only drops rel columns, but the topology (out_rels) stays.
        let r2 = GraphReader::topology_only(&bytes).unwrap();
        assert_eq!(r2.rel_prop(0, "weight"), None);
        assert_eq!(r2.out_rels(0), vec![(1, 0)]);
    }

    /// A small typed graph for the traversal primitives. Rels:
    ///   0 -KNOWS-> 1, 0 -KNOWS-> 2, 0 -LIVES-> 4, 1 -KNOWS-> 3.
    /// Node "name": 1 = Bob, 2 = Carol. Round-tripped through the RCPG codec.
    fn traversal_reader() -> GraphReader {
        let mut g = GraphSection {
            n_nodes: 5,
            n_rels: 4,
            out_offsets: vec![0, 3, 4, 4, 4, 4],
            out_nbrs: vec![1, 2, 4, 3],
            out_types: vec![1, 1, 2, 1], // KNOWS, KNOWS, LIVES, KNOWS
            in_offsets: vec![0, 0, 1, 2, 3, 4],
            in_nbrs: vec![0, 0, 1, 0],
            in_types: vec![1, 1, 1, 2],
            atoms: vec![
                String::new(),       // 0 = ""
                "KNOWS".to_string(), // 1
                "LIVES".to_string(), // 2
                "name".to_string(),  // 3 = key
                "Bob".to_string(),   // 4
                "Carol".to_string(), // 5
            ],
            ..Default::default()
        };
        g.node_columns = vec![(3, ColumnData::DenseStr(vec![0, 4, 5, 0, 0]))];
        let mut bytes = Vec::new();
        rcpg::write(&g, &mut bytes).unwrap();
        GraphReader::from_rcpg_bytes(&bytes).unwrap()
    }

    #[test]
    fn first_neighbor_follow_has_rel() {
        let r = traversal_reader();
        // first_neighbor short-circuits to the first type match in CSR order.
        assert_eq!(r.first_neighbor(0, Direction::Outgoing, "KNOWS"), Some(1));
        assert_eq!(r.first_neighbor(0, Direction::Outgoing, "LIVES"), Some(4));
        assert_eq!(r.first_neighbor(0, Direction::Outgoing, "NOPE"), None);
        assert_eq!(r.first_neighbor(3, Direction::Outgoing, "KNOWS"), None); // leaf
        assert_eq!(r.first_neighbor(3, Direction::Incoming, "KNOWS"), Some(1));
        // follow chains first_neighbor: 0 -KNOWS-> 1 -KNOWS-> 3.
        let path = &[
            (Direction::Outgoing, "KNOWS"),
            (Direction::Outgoing, "KNOWS"),
        ];
        assert_eq!(r.follow(0, path), Some(3));
        assert_eq!(r.follow(0, &[]), Some(0)); // no steps -> start
        assert_eq!(r.follow(0, &[(Direction::Outgoing, "NOPE")]), None);
        // has_rel existence check
        assert!(r.has_rel(0, Direction::Outgoing, "KNOWS"));
        assert!(!r.has_rel(0, Direction::Outgoing, "NOPE"));
        assert!(!r.has_rel(3, Direction::Outgoing, "KNOWS"));
    }

    #[test]
    fn degree_types_neighborhood_property() {
        let r = traversal_reader();
        // degree is O(1) from the offsets: node 0 has 3 out, 0 in.
        assert_eq!(r.degree(0, Direction::Outgoing), 3);
        assert_eq!(r.degree(0, Direction::Incoming), 0);
        assert_eq!(r.degree(1, Direction::Both), 2); // 1 out (->3) + 1 in (0->)
                                                     // deduped union of KNOWS+LIVES from 0, ascending.
        assert_eq!(
            r.neighbors_by_types(0, Direction::Outgoing, &["KNOWS", "LIVES"]),
            vec![1, 2, 4]
        );
        // typed 2-hop neighborhood from 0 via KNOWS: {1,2} then {3}.
        let nbhd: Vec<u32> = r
            .neighborhood(0, Direction::Outgoing, "KNOWS", 2)
            .iter()
            .collect();
        assert_eq!(nbhd, vec![1, 2, 3]);
        let one: Vec<u32> = r
            .neighborhood(0, Direction::Outgoing, "KNOWS", 1)
            .iter()
            .collect();
        assert_eq!(one, vec![1, 2]);
        // has_neighbor_with_property reads the neighbor's resident node property.
        assert!(r.has_neighbor_with_property(
            0,
            Direction::Outgoing,
            "KNOWS",
            "name",
            PropValue::Str("Bob")
        ));
        assert!(!r.has_neighbor_with_property(
            0,
            Direction::Outgoing,
            "KNOWS",
            "name",
            PropValue::Str("Zara")
        ));
        // the LIVES neighbor (node 4) carries no name, so no Bob match that way.
        assert!(!r.has_neighbor_with_property(
            0,
            Direction::Outgoing,
            "LIVES",
            "name",
            PropValue::Str("Bob")
        ));
    }
}
