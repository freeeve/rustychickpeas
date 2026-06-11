//! Plain-data model of a serialized graph snapshot.
//!
//! `GraphSection` mirrors `rustychickpeas_core::GraphSnapshot` minus the
//! lazily built property index (which is derived data and rebuilt on
//! demand). Keeping this crate's model free of core types lets readers
//! depend on the codec without pulling in the database.

use roaring::RoaringBitmap;

/// Columnar property storage, mirroring core's `Column`.
///
/// Dense columns are indexed by ID; sparse columns are `(id, value)` pairs
/// sorted by ID. String values are atom IDs into [`GraphSection::atoms`].
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    DenseI64(Vec<i64>),
    DenseF64(Vec<f64>),
    DenseBool(bitvec::vec::BitVec),
    DenseStr(Vec<u32>),
    SparseI64(Vec<(u32, i64)>),
    SparseF64(Vec<(u32, f64)>),
    SparseBool(Vec<(u32, bool)>),
    SparseStr(Vec<(u32, u32)>),
}

/// The complete decoded content of an RCPG file.
///
/// All string-ish values (labels, relationship types, property keys,
/// string property values) are atom IDs into `atoms`; atom 0 is always
/// the empty string.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphSection {
    /// Actual number of distinct nodes (NOT the CSR ID-space size; see
    /// `out_offsets`).
    pub n_nodes: u32,
    /// Number of relationships.
    pub n_rels: u64,

    /// Outgoing CSR offsets: len = csr_id_space + 1. IDs never added have
    /// empty ranges, so the ID space can exceed `n_nodes` under sparse IDs.
    pub out_offsets: Vec<u32>,
    /// Outgoing neighbors, len = n_rels.
    pub out_nbrs: Vec<u32>,
    /// Outgoing relationship type atoms, parallel to `out_nbrs`.
    pub out_types: Vec<u32>,
    /// Incoming CSR offsets, sized like `out_offsets`.
    pub in_offsets: Vec<u32>,
    /// Incoming neighbors, len = n_rels.
    pub in_nbrs: Vec<u32>,
    /// Incoming relationship type atoms, parallel to `in_nbrs`.
    pub in_types: Vec<u32>,

    /// Label atom -> node IDs with that label, sorted by label atom.
    pub label_index: Vec<(u32, RoaringBitmap)>,
    /// Type atom -> outgoing-CSR relationship positions, sorted by type atom.
    pub type_index: Vec<(u32, RoaringBitmap)>,

    /// Node property columns: (key atom, column), sorted by key atom.
    pub node_columns: Vec<(u32, ColumnData)>,
    /// Relationship property columns (indexed by outgoing-CSR position).
    pub rel_columns: Vec<(u32, ColumnData)>,

    /// Optional snapshot version string.
    pub version: Option<String>,

    /// String table; index 0 is always "".
    pub atoms: Vec<String>,
}

impl GraphSection {
    /// Size of the CSR ID space (max_node_id + 1 for non-empty graphs).
    pub fn csr_id_space(&self) -> u32 {
        (self.out_offsets.len().saturating_sub(1)) as u32
    }

    /// Outgoing neighbors of `node_id`, empty for IDs outside the space.
    pub fn out_neighbors(&self, node_id: u32) -> &[u32] {
        neighbors(&self.out_offsets, &self.out_nbrs, node_id)
    }

    /// Incoming neighbors of `node_id`, empty for IDs outside the space.
    pub fn in_neighbors(&self, node_id: u32) -> &[u32] {
        neighbors(&self.in_offsets, &self.in_nbrs, node_id)
    }
}

fn neighbors<'a>(offsets: &[u32], nbrs: &'a [u32], node_id: u32) -> &'a [u32] {
    let i = node_id as usize;
    if i + 1 >= offsets.len() {
        return &[];
    }
    let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
    if start > end || end > nbrs.len() {
        return &[];
    }
    &nbrs[start..end]
}
