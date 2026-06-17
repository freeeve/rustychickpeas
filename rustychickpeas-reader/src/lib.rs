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
}
