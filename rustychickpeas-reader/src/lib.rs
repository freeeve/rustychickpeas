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
use rustychickpeas_format::{rcpg, FormatError, GraphSection};

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
