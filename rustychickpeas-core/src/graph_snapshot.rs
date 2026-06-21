//! Immutable graph snapshot optimized for read-only queries
//!
//! GraphSnapshot uses CSR (Compressed Sparse Row) format for adjacency
//! and columnar storage for properties, providing maximum query performance.

use crate::bitmap::NodeSet;
use crate::fulltext::FullTextField;
use crate::geo::GeoIndex;
use crate::types::{Direction, Label, NodeId, PropertyKey, RelationshipType};
use hashbrown::HashMap;
use roaring::RoaringBitmap;
use std::sync::{Arc, Mutex, PoisonError};

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
    /// Rank/select i64 column for the moderately-sparse band: O(1) reads without
    /// the binary search of [`Column::SparseI64`]. `present` marks which positions
    /// carry a value; `block_rank[b]` caches the number of set bits before block
    /// `b` (`RANK_BLOCK` positions per block); `values` holds the values in
    /// ascending-position order. Build via [`Column::build_rank_i64`].
    RankI64 {
        present: bitvec::vec::BitVec,
        block_rank: Vec<u32>,
        values: Vec<i64>,
    },
    /// Rank/select f64 column (see [`Column::RankI64`]).
    RankF64 {
        present: bitvec::vec::BitVec,
        block_rank: Vec<u32>,
        values: Vec<f64>,
    },
    /// Rank/select boolean column; values packed in a bitvec (see [`Column::RankI64`]).
    RankBool {
        present: bitvec::vec::BitVec,
        block_rank: Vec<u32>,
        values: bitvec::vec::BitVec,
    },
    /// Rank/select string column of interned ids (see [`Column::RankI64`]).
    RankStr {
        present: bitvec::vec::BitVec,
        block_rank: Vec<u32>,
        values: Vec<u32>,
    },
}

/// Positions per rank block for [`Column::RankI64`]. A `get` does one indexed
/// `block_rank` read plus a popcount over at most this many bits (8 × u64).
const RANK_BLOCK: usize = 512;

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
        let pos = node_id as usize;
        match self {
            Column::RankI64 {
                present,
                block_rank,
                values,
            } => Self::rank_at(present, block_rank, pos).map(|i| ValueId::I64(values[i])),
            Column::RankF64 {
                present,
                block_rank,
                values,
            } => Self::rank_at(present, block_rank, pos).map(|i| ValueId::from_f64(values[i])),
            Column::RankBool {
                present,
                block_rank,
                values,
            } => Self::rank_at(present, block_rank, pos).map(|i| ValueId::Bool(values[i])),
            Column::RankStr {
                present,
                block_rank,
                values,
            } => Self::rank_at(present, block_rank, pos).map(|i| ValueId::Str(values[i])),
            _ => self.get_dense(node_id).or_else(|| self.get_sparse(node_id)),
        }
    }

    /// Index into a rank/select column's `values` for `pos`, or `None` if `pos`
    /// carries no value. O(1): one `block_rank` read plus a popcount over at most
    /// `RANK_BLOCK` bits.
    #[inline]
    fn rank_at(present: &bitvec::vec::BitVec, block_rank: &[u32], pos: usize) -> Option<usize> {
        if pos >= present.len() || !present[pos] {
            return None;
        }
        let b = pos / RANK_BLOCK;
        Some(block_rank[b] as usize + present[b * RANK_BLOCK..pos].count_ones())
    }

    /// Build the block-rank index for a presence bitvec: `block_rank[b]` is the
    /// number of set bits before block `b` (`RANK_BLOCK` positions per block).
    fn block_rank_for(present: &bitvec::vec::BitVec, len: usize) -> Vec<u32> {
        let nblocks = len.div_ceil(RANK_BLOCK);
        let mut block_rank = Vec::with_capacity(nblocks + 1);
        let mut acc = 0u32;
        block_rank.push(0u32);
        for b in 0..nblocks {
            let start = b * RANK_BLOCK;
            let end = ((b + 1) * RANK_BLOCK).min(len);
            acc += present[start..end].count_ones() as u32;
            block_rank.push(acc);
        }
        block_rank
    }

    /// Build a rank/select i64 column from `(position, value)` pairs **sorted by
    /// position** over a column space of `len` positions. Reads are O(1): one
    /// `block_rank` index plus a bounded popcount. Uses `len/8` bits for presence
    /// plus a block-rank index of `len/RANK_BLOCK` u32s — smaller than the
    /// equivalent [`Column::SparseI64`] pair array once fill exceeds ~1/64.
    pub(crate) fn build_rank_i64(sorted_pairs: &[(u32, i64)], len: usize) -> Column {
        let mut present = bitvec::vec::BitVec::repeat(false, len);
        let mut values = Vec::with_capacity(sorted_pairs.len());
        for &(pos, val) in sorted_pairs {
            present.set(pos as usize, true);
            values.push(val);
        }
        let block_rank = Self::block_rank_for(&present, len);
        Column::RankI64 {
            present,
            block_rank,
            values,
        }
    }

    /// Build a rank/select f64 column from position-sorted pairs. See [`Column::build_rank_i64`].
    pub(crate) fn build_rank_f64(sorted_pairs: &[(u32, f64)], len: usize) -> Column {
        let mut present = bitvec::vec::BitVec::repeat(false, len);
        let mut values = Vec::with_capacity(sorted_pairs.len());
        for &(pos, val) in sorted_pairs {
            present.set(pos as usize, true);
            values.push(val);
        }
        let block_rank = Self::block_rank_for(&present, len);
        Column::RankF64 {
            present,
            block_rank,
            values,
        }
    }

    /// Build a rank/select boolean column from position-sorted pairs. See [`Column::build_rank_i64`].
    pub(crate) fn build_rank_bool(sorted_pairs: &[(u32, bool)], len: usize) -> Column {
        let mut present = bitvec::vec::BitVec::repeat(false, len);
        let mut values = bitvec::vec::BitVec::with_capacity(sorted_pairs.len());
        for &(pos, val) in sorted_pairs {
            present.set(pos as usize, true);
            values.push(val);
        }
        let block_rank = Self::block_rank_for(&present, len);
        Column::RankBool {
            present,
            block_rank,
            values,
        }
    }

    /// Build a rank/select string (interned id) column from position-sorted pairs.
    /// See [`Column::build_rank_i64`].
    pub(crate) fn build_rank_str(sorted_pairs: &[(u32, u32)], len: usize) -> Column {
        let mut present = bitvec::vec::BitVec::repeat(false, len);
        let mut values = Vec::with_capacity(sorted_pairs.len());
        for &(pos, val) in sorted_pairs {
            present.set(pos as usize, true);
            values.push(val);
        }
        let block_rank = Self::block_rank_for(&present, len);
        Column::RankStr {
            present,
            block_rank,
            values,
        }
    }

    /// The dense `i64` values as a contiguous slice indexed by node id, or `None`
    /// if this column is not a dense `i64` column. Lets a bulk scan index raw
    /// values instead of constructing and matching a [`ValueId`] per row; pair
    /// with [`NodeSet::as_range`](crate::bitmap::NodeSet::as_range) when a label's
    /// ids form a contiguous block.
    pub fn as_i64_slice(&self) -> Option<&[i64]> {
        match self {
            Column::DenseI64(col) => Some(col.as_slice()),
            _ => None,
        }
    }

    /// The dense `f64` values as a contiguous slice, or `None`. (Provided for
    /// symmetry; the BI queries read f64 weights from maps, not columns.)
    pub fn as_f64_slice(&self) -> Option<&[f64]> {
        match self {
            Column::DenseF64(col) => Some(col.as_slice()),
            _ => None,
        }
    }

    /// The dense boolean values as a bit slice indexed by node id, or `None`.
    pub fn as_bool_slice(&self) -> Option<&bitvec::slice::BitSlice> {
        match self {
            Column::DenseBool(col) => Some(col.as_bitslice()),
            _ => None,
        }
    }

    /// The dense string-property values as a slice of interned string ids, or
    /// `None`. Resolve a comparison value to its id once (and compare `u32`s)
    /// instead of resolving a string per row; recover text via
    /// [`GraphSnapshot::resolve_string`].
    pub fn as_str_ids(&self) -> Option<&[u32]> {
        match self {
            Column::DenseStr(col) => Some(col.as_slice()),
            _ => None,
        }
    }

    /// Iterate over all (NodeId, ValueId) entries in this column
    pub fn iter_entries(&self) -> Box<dyn Iterator<Item = (NodeId, ValueId)> + '_> {
        match self {
            Column::DenseI64(col) => Box::new(
                col.iter()
                    .enumerate()
                    .map(|(i, &v)| (i as NodeId, ValueId::I64(v))),
            ),
            Column::DenseF64(col) => Box::new(
                col.iter()
                    .enumerate()
                    .map(|(i, &v)| (i as NodeId, ValueId::from_f64(v))),
            ),
            Column::DenseBool(col) => Box::new(
                col.iter()
                    .enumerate()
                    .map(|(i, b)| (i as NodeId, ValueId::Bool(*b))),
            ),
            Column::DenseStr(col) => Box::new(
                col.iter()
                    .enumerate()
                    .map(|(i, &v)| (i as NodeId, ValueId::Str(v))),
            ),
            Column::SparseI64(col) => Box::new(col.iter().map(|&(id, v)| (id, ValueId::I64(v)))),
            Column::SparseF64(col) => {
                Box::new(col.iter().map(|&(id, v)| (id, ValueId::from_f64(v))))
            }
            Column::SparseBool(col) => Box::new(col.iter().map(|&(id, v)| (id, ValueId::Bool(v)))),
            Column::SparseStr(col) => Box::new(col.iter().map(|&(id, v)| (id, ValueId::Str(v)))),
            Column::RankI64 {
                present, values, ..
            } => Box::new(
                present
                    .iter_ones()
                    .zip(values.iter())
                    .map(|(pos, &v)| (pos as NodeId, ValueId::I64(v))),
            ),
            Column::RankF64 {
                present, values, ..
            } => Box::new(
                present
                    .iter_ones()
                    .zip(values.iter())
                    .map(|(pos, &v)| (pos as NodeId, ValueId::from_f64(v))),
            ),
            Column::RankBool {
                present, values, ..
            } => Box::new(
                present
                    .iter_ones()
                    .zip(values.iter())
                    .map(|(pos, b)| (pos as NodeId, ValueId::Bool(*b))),
            ),
            Column::RankStr {
                present, values, ..
            } => Box::new(
                present
                    .iter_ones()
                    .zip(values.iter())
                    .map(|(pos, &v)| (pos as NodeId, ValueId::Str(v))),
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

/// A relationship incident to a node, as returned by
/// [`GraphSnapshot::relationships`].
///
/// Carries the other endpoint, the relationship type, the direction relative to
/// the queried node, and the CSR position used to read the relationship's
/// properties via [`GraphSnapshot::rel_prop`] — which the plain
/// neighbor accessors do not expose. `pos` is valid for property access in
/// Weight mode for [`GraphSnapshot::co_occurring`] — how each co-occurring node's
/// weight is accumulated over the shared centers (declarative, so the kernel needs
/// no per-element callback).
#[derive(Debug, Clone, Copy)]
pub enum CoWeight<'a> {
    /// The number of shared centers (co-occurrence events) per co-occurring node.
    Count,
    /// The number of *distinct* values of the property `key` (read off each shared
    /// center) per co-occurring node — e.g. distinct co-occurrence days. A center
    /// lacking `key` contributes nothing.
    Distinct(&'a str),
}

/// **both** directions (for incoming relationships it is mapped to the position
/// where the property is stored).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationshipRef {
    /// The other endpoint of the relationship, relative to the queried node.
    pub neighbor: NodeId,
    /// Relationship type.
    pub rel_type: RelationshipType,
    /// Direction relative to the queried node: `Outgoing` if it is the source,
    /// `Incoming` if it is the destination.
    pub direction: Direction,
    /// CSR position; pass to [`GraphSnapshot::rel_prop`] to read
    /// this relationship's properties.
    pub pos: u32,
}

/// Recompute the incoming->outgoing CSR position map from the adjacency arrays
/// alone (used when a snapshot is reconstructed without builder permutations,
/// e.g. on deserialization). Pairs the k-th `u->v` rel of a given type in the
/// outgoing CSR with the k-th such rel in the incoming CSR; both CSRs preserve
/// insertion order within a group, so the pairing matches the original rels.
pub(crate) fn compute_in_to_out_from_csr(
    out_offsets: &[u32],
    out_nbrs: &[NodeId],
    out_types: &[RelationshipType],
    in_offsets: &[u32],
    in_nbrs: &[NodeId],
    in_types: &[RelationshipType],
) -> Vec<u32> {
    use std::collections::{HashMap, VecDeque};
    let n = in_offsets.len().saturating_sub(1);
    let m = out_nbrs.len();
    let mut groups: HashMap<(NodeId, NodeId, RelationshipType), VecDeque<u32>> = HashMap::new();
    for v in 0..n {
        for inpos in in_offsets[v]..in_offsets[v + 1] {
            let key = (
                in_nbrs[inpos as usize],
                v as NodeId,
                in_types[inpos as usize],
            );
            groups.entry(key).or_default().push_back(inpos);
        }
    }
    let mut in_to_out = vec![0u32; m];
    for u in 0..n {
        for outpos in out_offsets[u]..out_offsets[u + 1] {
            let key = (
                u as NodeId,
                out_nbrs[outpos as usize],
                out_types[outpos as usize],
            );
            if let Some(inpos) = groups.get_mut(&key).and_then(|q| q.pop_front()) {
                in_to_out[inpos as usize] = outpos;
            }
        }
    }
    in_to_out
}

/// The result of a shortest-path search ([`GraphSnapshot::dijkstra`]): the
/// shortest distance to every reached node, with predecessors for path
/// reconstruction.
#[derive(Debug, Clone)]
pub struct ShortestPaths {
    source: NodeId,
    dist: HashMap<NodeId, f64>,
    prev: HashMap<NodeId, NodeId>,
}

impl ShortestPaths {
    /// The shortest distance from the source to `node`, or `None` if unreached.
    pub fn distance(&self, node: NodeId) -> Option<f64> {
        self.dist.get(&node).copied()
    }

    /// Whether `node` was reached from the source.
    pub fn reached(&self, node: NodeId) -> bool {
        self.dist.contains_key(&node)
    }

    /// All reached nodes paired with their shortest distance from the source,
    /// consuming the result — the bulk export behind a `{node: distance}` map (the
    /// per-node [`distance`](Self::distance) only answers nodes the caller already
    /// knows). The source itself is included at distance `0.0`.
    pub fn into_distances(self) -> HashMap<NodeId, f64> {
        self.dist
    }

    /// The shortest path from the source to `target` as a node sequence (source
    /// first, target last), or `None` if `target` was not reached.
    pub fn path_to(&self, target: NodeId) -> Option<Vec<NodeId>> {
        if !self.dist.contains_key(&target) {
            return None;
        }
        let mut path = vec![target];
        let mut cur = target;
        while cur != self.source {
            cur = *self.prev.get(&cur)?;
            path.push(cur);
        }
        path.reverse();
        Some(path)
    }
}

/// Min-heap entry for Dijkstra, ordered by ascending cost.
struct DijkstraState {
    cost: f64,
    node: NodeId,
}
impl PartialEq for DijkstraState {
    fn eq(&self, other: &Self) -> bool {
        self.cost == other.cost && self.node == other.node
    }
}
impl Eq for DijkstraState {}
impl Ord for DijkstraState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse cost so BinaryHeap (a max-heap) yields the smallest cost first.
        other
            .cost
            .total_cmp(&self.cost)
            .then_with(|| self.node.cmp(&other.node))
    }
}
impl PartialOrd for DijkstraState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A built forest-root array, returned by
/// [`chain_roots`](GraphSnapshot::chain_roots): index it by node id for the
/// terminal of that node's functional-relationship chain. Shared (`Arc`) so a
/// hot loop can hold it and index lock-free.
pub type ChainRoots = Arc<[NodeId]>;

/// A resolved `i64` property column: [`I64Col::get`] reads a position via the
/// dense `Vec<i64>` fast path when the column is dense, else the general column
/// lookup. Obtained by narrowing a [`Col`] with [`Col::i64`] — itself built from
/// [`GraphSnapshot::col`] (node columns, indexed by node id) or
/// [`GraphSnapshot::rel_col`] (relationship columns, indexed by CSR position).
#[derive(Clone, Copy)]
pub struct I64Col<'a> {
    inner: I64ColInner<'a>,
}

#[derive(Clone, Copy)]
enum I64ColInner<'a> {
    Dense(&'a [i64]),
    Other(&'a Column),
}

impl<'a> I64Col<'a> {
    /// The `i64` value at `pos` (a node id for node columns, a relationship CSR
    /// position for rel columns), or `None` when absent.
    #[inline]
    pub fn get(&self, pos: NodeId) -> Option<i64> {
        match &self.inner {
            I64ColInner::Dense(slice) => slice.get(pos as usize).copied(),
            I64ColInner::Other(col) => match col.get(pos) {
                Some(ValueId::I64(v)) => Some(v),
                _ => None,
            },
        }
    }

    /// The underlying dense `i64` slice when the column is dense — index it by node
    /// id / CSR position directly in a hot loop; `None` for a sparse/rank column
    /// (fall back to [`get`](Self::get) then).
    #[inline]
    pub fn as_slice(&self) -> Option<&'a [i64]> {
        match self.inner {
            I64ColInner::Dense(slice) => Some(slice),
            I64ColInner::Other(_) => None,
        }
    }
}

/// A resolved boolean property column (the boolean analogue of [`I64Col`]);
/// obtained by narrowing a [`Col`] with [`Col::bool`].
#[derive(Clone, Copy)]
pub struct BoolCol<'a> {
    inner: BoolColInner<'a>,
}

#[derive(Clone, Copy)]
enum BoolColInner<'a> {
    Dense(&'a bitvec::slice::BitSlice),
    Other(&'a Column),
}

impl<'a> BoolCol<'a> {
    /// The boolean value at node `pos`, or `None` when absent.
    #[inline]
    pub fn get(&self, pos: NodeId) -> Option<bool> {
        match &self.inner {
            BoolColInner::Dense(slice) => slice.get(pos as usize).map(|b| *b),
            BoolColInner::Other(col) => match col.get(pos) {
                Some(ValueId::Bool(v)) => Some(v),
                _ => None,
            },
        }
    }

    /// The underlying dense bit slice when the column is dense — index it by node id
    /// directly in a hot loop; `None` for a sparse/rank column.
    #[inline]
    pub fn as_slice(&self) -> Option<&'a bitvec::slice::BitSlice> {
        match self.inner {
            BoolColInner::Dense(slice) => Some(slice),
            BoolColInner::Other(_) => None,
        }
    }
}

/// A resolved `f64` property column (the `f64` analogue of [`I64Col`]); obtained
/// by narrowing a [`Col`] with [`Col::f64`].
#[derive(Clone, Copy)]
pub struct F64Col<'a> {
    inner: F64ColInner<'a>,
}

#[derive(Clone, Copy)]
enum F64ColInner<'a> {
    Dense(&'a [f64]),
    Other(&'a Column),
}

impl<'a> F64Col<'a> {
    /// The `f64` value at `pos`, or `None` when absent.
    #[inline]
    pub fn get(&self, pos: NodeId) -> Option<f64> {
        match &self.inner {
            F64ColInner::Dense(slice) => slice.get(pos as usize).copied(),
            F64ColInner::Other(col) => col.get(pos).and_then(|v| v.to_f64()),
        }
    }

    /// The underlying dense `f64` slice when the column is dense — index it by node
    /// id / CSR position directly in a hot loop; `None` for a sparse/rank column.
    #[inline]
    pub fn as_slice(&self) -> Option<&'a [f64]> {
        match self.inner {
            F64ColInner::Dense(slice) => Some(slice),
            F64ColInner::Other(_) => None,
        }
    }
}

/// A resolved string property column exposing interned string ids: [`StrCol::id`]
/// reads the id at a position; [`StrCol::as_ids`] exposes the dense id slice for
/// vectorized comparison (resolve a target string to its id once and compare
/// `u32`s). Recover text via [`GraphSnapshot::resolve_string`]. Obtained by
/// narrowing a [`Col`] with [`Col::str`].
#[derive(Clone, Copy)]
pub struct StrCol<'a> {
    inner: StrColInner<'a>,
}

#[derive(Clone, Copy)]
enum StrColInner<'a> {
    Dense(&'a [u32]),
    Other(&'a Column),
}

impl<'a> StrCol<'a> {
    /// The interned string id at `pos`, or `None` when absent.
    #[inline]
    pub fn id(&self, pos: NodeId) -> Option<u32> {
        match &self.inner {
            StrColInner::Dense(slice) => slice.get(pos as usize).copied(),
            StrColInner::Other(col) => match col.get(pos) {
                Some(ValueId::Str(id)) => Some(id),
                _ => None,
            },
        }
    }

    /// The underlying dense interned-id slice when the column is dense — compare it
    /// against ids resolved from target strings; `None` for a sparse/rank column.
    #[inline]
    pub fn as_ids(&self) -> Option<&'a [u32]> {
        match self.inner {
            StrColInner::Dense(slice) => Some(slice),
            StrColInner::Other(_) => None,
        }
    }
}

/// The logical element type of a [`Col`], reported by [`Col::dtype`] without
/// narrowing — the dtype analogue of a pandas/polars column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnDtype {
    I64,
    F64,
    Bool,
    Str,
}

/// A resolved property column: the key -> column lookup is done once, then
/// narrowed to a typed reader with [`Col::i64`] / [`Col::f64`] / [`Col::bool`] /
/// [`Col::str`] — mirroring the polars `column(name).i64()` shape. Build with
/// [`GraphSnapshot::col`] (node columns, indexed by node id) or
/// [`GraphSnapshot::rel_col`] (relationship columns, indexed by CSR position).
#[derive(Clone, Copy)]
pub struct Col<'a> {
    col: &'a Column,
}

impl<'a> Col<'a> {
    /// Narrow to a typed `i64` reader. Reads take the dense slice fast path when
    /// the column is dense (see [`I64Col`]); a non-`i64` column reads back as
    /// absent, like a mistyped [`prop`](GraphSnapshot::prop).
    #[inline]
    pub fn i64(self) -> I64Col<'a> {
        I64Col {
            inner: match self.col.as_i64_slice() {
                Some(slice) => I64ColInner::Dense(slice),
                None => I64ColInner::Other(self.col),
            },
        }
    }

    /// Narrow to a typed boolean reader (the boolean analogue of [`Col::i64`]).
    #[inline]
    pub fn bool(self) -> BoolCol<'a> {
        BoolCol {
            inner: match self.col.as_bool_slice() {
                Some(slice) => BoolColInner::Dense(slice),
                None => BoolColInner::Other(self.col),
            },
        }
    }

    /// Narrow to a typed `f64` reader (the `f64` analogue of [`Col::i64`]).
    #[inline]
    pub fn f64(self) -> F64Col<'a> {
        F64Col {
            inner: match self.col.as_f64_slice() {
                Some(slice) => F64ColInner::Dense(slice),
                None => F64ColInner::Other(self.col),
            },
        }
    }

    /// Narrow to a typed string reader exposing interned ids (the string analogue
    /// of [`Col::i64`]); compare its ids against ids resolved from target strings.
    #[inline]
    pub fn str(self) -> StrCol<'a> {
        StrCol {
            inner: match self.col.as_str_ids() {
                Some(slice) => StrColInner::Dense(slice),
                None => StrColInner::Other(self.col),
            },
        }
    }

    /// The logical element type of this column, without narrowing — pick the
    /// matching typed reader (or buffer dtype) from it.
    #[inline]
    pub fn dtype(self) -> ColumnDtype {
        match self.col {
            Column::DenseI64(_) | Column::SparseI64(_) | Column::RankI64 { .. } => ColumnDtype::I64,
            Column::DenseF64(_) | Column::SparseF64(_) | Column::RankF64 { .. } => ColumnDtype::F64,
            Column::DenseBool(_) | Column::SparseBool(_) | Column::RankBool { .. } => {
                ColumnDtype::Bool
            }
            Column::DenseStr(_) | Column::SparseStr(_) | Column::RankStr { .. } => ColumnDtype::Str,
        }
    }
}

/// A resolved property value, narrowed to a typed read with [`Prop::str`] /
/// [`Prop::i64`] / [`Prop::bool`] / [`Prop::f64`] — the single-value analogue of
/// [`Col`], mirroring the polars `…​.i64()` shape. Build with
/// [`GraphSnapshot::prop`]; use [`Prop::value`] for the raw [`ValueId`].
#[derive(Clone, Copy)]
pub struct Prop<'a> {
    g: &'a GraphSnapshot,
    value: ValueId,
}

impl<'a> Prop<'a> {
    /// The raw [`ValueId`] — escape hatch for matching the variant directly.
    #[inline]
    pub fn value(self) -> ValueId {
        self.value
    }

    /// The string value, or `None` if not a string **or empty**. Dense string
    /// columns store a missing value as `""`, so this folds in the
    /// `filter(|s| !s.is_empty())` callers would otherwise repeat.
    #[inline]
    pub fn str(self) -> Option<&'a str> {
        match self.value {
            ValueId::Str(id) => match self.g.resolve_string(id) {
                Some(s) if !s.is_empty() => Some(s),
                _ => None,
            },
            _ => None,
        }
    }

    /// The `i64` value, or `None` if the property is not an `i64`.
    #[inline]
    pub fn i64(self) -> Option<i64> {
        match self.value {
            ValueId::I64(v) => Some(v),
            _ => None,
        }
    }

    /// The boolean value, or `None` if the property is not a boolean.
    #[inline]
    pub fn bool(self) -> Option<bool> {
        match self.value {
            ValueId::Bool(v) => Some(v),
            _ => None,
        }
    }

    /// The `f64` value, or `None` if the property is not an `f64`.
    #[inline]
    pub fn f64(self) -> Option<f64> {
        self.value.to_f64()
    }
}

impl core::fmt::Debug for Prop<'_> {
    /// Show just the value; the snapshot back-reference is noise.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(&self.value, f)
    }
}

/// Typed reads lifted over `Option<`[`Prop`]`>`, so a [`GraphSnapshot::prop`] result
/// reads directly: `g.prop(n, k).str()` flattens the narrow (vs
/// `…​.and_then(|p| p.str())`), and the `*_or` variants fold in a default for the
/// ubiquitous "typed value or fallback" read. Bring into scope to use; the same
/// `str`/`i64`/`bool`/`f64` names work on a bare [`Prop`] too.
pub trait PropExt<'a> {
    /// The string value, or `None` if absent, not a string, **or empty**.
    fn str(self) -> Option<&'a str>;
    /// The `i64` value, or `None` if absent or not an `i64`.
    fn i64(self) -> Option<i64>;
    /// The boolean value, or `None` if absent or not a boolean.
    fn bool(self) -> Option<bool>;
    /// The `f64` value, or `None` if absent or not an `f64`.
    fn f64(self) -> Option<f64>;
    /// The string value, or `default` if absent / not a string / empty.
    fn str_or(self, default: &'a str) -> &'a str;
    /// The `i64` value, or `default` if absent or not an `i64`.
    fn i64_or(self, default: i64) -> i64;
    /// The boolean value, or `default` if absent or not a boolean.
    fn bool_or(self, default: bool) -> bool;
    /// The `f64` value, or `default` if absent or not an `f64`.
    fn f64_or(self, default: f64) -> f64;
}

impl<'a> PropExt<'a> for Option<Prop<'a>> {
    #[inline]
    fn str(self) -> Option<&'a str> {
        self.and_then(Prop::str)
    }
    #[inline]
    fn i64(self) -> Option<i64> {
        self.and_then(Prop::i64)
    }
    #[inline]
    fn bool(self) -> Option<bool> {
        self.and_then(Prop::bool)
    }
    #[inline]
    fn f64(self) -> Option<f64> {
        self.and_then(Prop::f64)
    }
    #[inline]
    fn str_or(self, default: &'a str) -> &'a str {
        self.and_then(Prop::str).unwrap_or(default)
    }
    #[inline]
    fn i64_or(self, default: i64) -> i64 {
        self.and_then(Prop::i64).unwrap_or(default)
    }
    #[inline]
    fn bool_or(self, default: bool) -> bool {
        self.and_then(Prop::bool).unwrap_or(default)
    }
    #[inline]
    fn f64_or(self, default: f64) -> f64 {
        self.and_then(Prop::f64).unwrap_or(default)
    }
}

/// A lazy query over each source node's neighbours of one relationship type,
/// grouped by a projected attribute and reduced per source. Built by
/// [`GraphSnapshot::neighbor_groups`]; nothing runs until a terminal
/// ([`sizes`](Self::sizes) / [`top_by_size`](Self::top_by_size)) is called.
///
/// Each source's neighbours are projected through a
/// [`follow`](GraphSnapshot::follow)-style chain to a "group" node; the cohort
/// sharing one group is what gets counted. Reductions run in parallel over the
/// sources with the projection and counting kept native, so the (often millions
/// of) intermediate `(source, group)` pairs never cross back to the caller — the
/// terminal reduces first.
pub struct NeighborGroups<'a> {
    graph: &'a GraphSnapshot,
    sources: &'a [NodeId],
    rel: &'a str,
    direction: Direction,
    project: Vec<(Direction, &'a str)>,
}

impl<'a> NeighborGroups<'a> {
    /// Project each neighbour to its group node via a chain of first-neighbour
    /// `(direction, rel_type)` steps (like [`follow`](GraphSnapshot::follow)).
    /// Without a projection, neighbours group by their own id (every cohort is 1).
    #[must_use]
    pub fn project(mut self, steps: &[(Direction, &'a str)]) -> Self {
        self.project = steps.to_vec();
        self
    }

    /// Per source, the size of its largest cohort, as `(source, size)` — the raw
    /// reduction. Sources with no projectable neighbours yield `0`. Runs in
    /// parallel over the sources; an unknown `rel`/projection type yields all-zero
    /// sizes.
    pub fn sizes(&self) -> Vec<(NodeId, u32)> {
        use rayon::prelude::*;
        let g = self.graph;
        let Some(rel_t) = g.relationship_type_from_str(self.rel) else {
            return self.sources.iter().map(|&s| (s, 0)).collect();
        };
        let Some(proj_steps) = self
            .project
            .iter()
            .map(|&(d, r)| g.relationship_type_from_str(r).map(|t| (d, t)))
            .collect::<Option<Vec<(Direction, RelationshipType)>>>()
        else {
            return self.sources.iter().map(|&s| (s, 0)).collect();
        };
        self.sources
            .par_iter()
            .map(|&src| {
                // Tally this source's neighbours by their projected group node and
                // keep the largest cohort. The map is thread-local, so no sharing.
                let mut counts: HashMap<NodeId, u32> = HashMap::new();
                for m in g.neighbors_by_type(src, self.direction, rel_t) {
                    let mut cur = m;
                    let mut projected = true;
                    for &(d, t) in &proj_steps {
                        match g.first_neighbor(cur, d, t) {
                            Some(next) => cur = next,
                            None => {
                                projected = false;
                                break;
                            }
                        }
                    }
                    if projected {
                        *counts.entry(cur).or_insert(0) += 1;
                    }
                }
                (src, counts.values().copied().max().unwrap_or(0))
            })
            .collect()
    }

    /// The `n` sources with the largest cohorts, as `(source, size)`, size
    /// descending. Ties break by the `tie` node property (read as i64, ascending)
    /// when given — so a caller can match a query's output-id ordering — else by
    /// source id ascending (always deterministic). Reduces in parallel via
    /// [`sizes`](Self::sizes), then sorts.
    pub fn top_by_size(&self, n: usize, tie: Option<&str>) -> Vec<(NodeId, u32)> {
        let mut sizes = self.sizes();
        match tie {
            Some(key) => sizes.sort_by(|a, b| {
                b.1.cmp(&a.1).then_with(|| {
                    self.graph
                        .prop(a.0, key)
                        .i64_or(0)
                        .cmp(&self.graph.prop(b.0, key).i64_or(0))
                })
            }),
            None => sizes.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0))),
        }
        sizes.truncate(n);
        sizes
    }
}

/// Immutable graph snapshot optimized for read-only queries
#[derive(Debug)]
pub struct GraphSnapshot {
    // --- Core shape (CSR) ---
    /// Actual number of nodes in the graph (the count of distinct node IDs
    /// that were added to the builder).
    ///
    /// Note: this is NOT necessarily the size of the CSR ID space. The CSR
    /// offset arrays (`out_offsets`/`in_offsets`) are sized by
    /// `max_node_id + 2` (one slot per ID in `0..=max_node_id`, plus the
    /// trailing offset), so when node IDs are sparse (contain gaps),
    /// `out_offsets.len() - 1` exceeds `n_nodes`. Use
    /// `out_offsets.len() - 1` for the CSR ID space size; do not derive it
    /// from `n_nodes`.
    pub n_nodes: u32,
    /// Number of relationships
    pub n_rels: u64,

    // CSR (outgoing relationships)
    /// Outgoing offsets: len = max_node_id + 2, i.e. one entry per ID in
    /// `0..=max_node_id` plus a trailing offset. This can exceed
    /// `n_nodes + 1` when node IDs are sparse; IDs that were never added
    /// simply have empty ranges.
    /// out_offsets[i] to out_offsets[i+1] gives the range in out_nbrs for node i
    pub out_offsets: Vec<u32>,
    /// Outgoing neighbors: len = n_rels
    /// Contains destination node IDs
    pub out_nbrs: Vec<NodeId>,
    /// Outgoing relationship types: len = n_rels
    /// Parallel to out_nbrs, contains relationship type for each rel
    pub out_types: Vec<RelationshipType>,
    // CSR (incoming relationships) - optional
    /// Incoming offsets: len = max_node_id + 2 (same sizing as `out_offsets`;
    /// can exceed `n_nodes + 1` when node IDs are sparse)
    pub in_offsets: Vec<u32>,
    /// Incoming neighbors: len = n_rels
    /// Contains source node IDs
    pub in_nbrs: Vec<NodeId>,
    /// Incoming relationship types: len = n_rels
    /// Parallel to in_nbrs, contains relationship type for each rel
    pub in_types: Vec<RelationshipType>,

    /// Maps an incoming CSR position to the outgoing CSR position of the same
    /// rel, so relationship properties (stored by outgoing position) can be
    /// read for incoming relationships. Empty when the graph has no
    /// relationship properties (the map would be unused).
    pub in_to_out: Vec<u32>,

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

    // --- Full-text index (lazy-initialized) ---
    /// Lazy boolean inverted index: (label, property_key) -> token postings,
    /// built per field on first `full_text_search` call. See [`crate::fulltext`].
    pub fulltext_index: Mutex<HashMap<(Label, PropertyKey), FullTextField>>,

    // --- Geo-spatial index (lazy-initialized) ---
    /// Lazy geo index: (label, lat_key, lon_key) -> k-d tree over coordinates,
    /// built per field on first geo query. See [`crate::geo`].
    pub geo_index: Mutex<HashMap<(Label, PropertyKey, PropertyKey), GeoIndex>>,

    // --- Forest-root index (lazy, built once per (direction, rel_type)) ---
    /// Lazily built forest-root arrays: `(direction, rel_type)` -> a per-node
    /// array whose `[node]` is the terminal of that node's functional `rel_type`
    /// chain. Built on first access by
    /// [`chain_roots`](GraphSnapshot::chain_roots); the returned `Arc` slice is
    /// indexed lock-free, so a hot loop pays no per-node synchronization.
    pub chain_root_index: Mutex<HashMap<(Direction, RelationshipType), ChainRoots>>,

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
    fn build_property_index_for_key_and_label(
        column: &Column,
        label_nodes: Option<&NodeSet>,
    ) -> HashMap<ValueId, NodeSet> {
        let mut key_index: HashMap<ValueId, Vec<NodeId>> = HashMap::new();

        for (node_id, val_id) in column.iter_entries() {
            if label_nodes.is_none_or(|nodes| nodes.contains(node_id)) {
                key_index.entry(val_id).or_default().push(node_id);
            }
        }

        key_index
            .into_iter()
            .map(|(val_id, mut node_ids)| {
                node_ids.sort_unstable();
                node_ids.dedup();
                let bitmap = RoaringBitmap::from_sorted_iter(node_ids).unwrap();
                (val_id, NodeSet::from(bitmap))
            })
            .collect()
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
            in_to_out: Vec::new(),
            label_index: HashMap::new(),
            type_index: HashMap::new(),
            version: None,
            columns: HashMap::new(),
            rel_columns: HashMap::new(),
            prop_index: Mutex::new(HashMap::new()),
            fulltext_index: Mutex::new(HashMap::new()),
            geo_index: Mutex::new(HashMap::new()),
            chain_root_index: Mutex::new(HashMap::new()),
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
            if ids.is_empty() {
                None
            } else {
                Some(ids)
            }
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
            Some(allowed) => nbrs[start..end]
                .iter()
                .zip(types[start..end].iter())
                .filter_map(|(&nbr, &rt)| {
                    if allowed.contains(&rt) {
                        Some(nbr)
                    } else {
                        None
                    }
                })
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
        let iter = nbrs[start..end]
            .iter()
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
                &self.out_offsets,
                &self.out_nbrs,
                &self.out_types,
                node_id,
                at,
            ),
            Direction::Incoming => Self::neighbors_by_type_from_csr(
                &self.in_offsets,
                &self.in_nbrs,
                &self.in_types,
                node_id,
                at,
            ),
            Direction::Both => {
                let mut both = Self::neighbors_by_type_from_csr(
                    &self.out_offsets,
                    &self.out_nbrs,
                    &self.out_types,
                    node_id,
                    at,
                );
                both.extend(Self::neighbors_by_type_from_csr(
                    &self.in_offsets,
                    &self.in_nbrs,
                    &self.in_types,
                    node_id,
                    at,
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
                &self.out_offsets,
                &self.out_nbrs,
                &self.out_types,
                node_id,
                at,
            ),
            Direction::Incoming => Self::neighbors_with_positions_from_csr(
                &self.in_offsets,
                &self.in_nbrs,
                &self.in_types,
                node_id,
                at,
            ),
            Direction::Both => {
                let mut both = Self::neighbors_with_positions_from_csr(
                    &self.out_offsets,
                    &self.out_nbrs,
                    &self.out_types,
                    node_id,
                    at,
                );
                both.extend(Self::neighbors_with_positions_from_csr(
                    &self.in_offsets,
                    &self.in_nbrs,
                    &self.in_types,
                    node_id,
                    at,
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

    /// The number of nodes in the graph.
    #[inline]
    pub fn node_count(&self) -> u32 {
        self.n_nodes
    }

    /// The number of relationships in the graph.
    #[inline]
    pub fn relationship_count(&self) -> u64 {
        self.n_rels
    }

    /// All neighbors of a node in the given direction, as a lazy iterator
    /// ([`Direction::Both`] yields outgoing then incoming). `.collect()` it for a
    /// `Vec`, `.next()` for the first, or iterate it directly to avoid allocating.
    /// For per-relationship type or property access during a traversal, use
    /// [`relationships`](Self::relationships) instead.
    pub fn neighbors(&self, node_id: NodeId, direction: Direction) -> NeighborsByType<'_> {
        self.neighbors_by_type(node_id, direction, &[] as &[&str])
    }

    /// Get the neighbors of a node in the given direction, restricted to
    /// relationships of the given types (an empty `rel_types` matches all types).
    ///
    /// [`Direction::Both`] returns matching outgoing neighbors followed by
    /// matching incoming ones.
    ///
    /// # Arguments
    /// * `node_id` - The node ID
    /// * `direction` - Which relationships to follow ([`Direction::Outgoing`],
    ///   [`Direction::Incoming`], or [`Direction::Both`])
    /// * `rel_types` - Relationship type names (e.g., `&["KNOWS", "WORKS_WITH"]`)
    ///
    /// # Examples
    /// ```
    /// use rustychickpeas_core::GraphBuilder;
    /// use rustychickpeas_core::types::Direction;
    ///
    /// // Create a graph
    /// let mut builder = GraphBuilder::new(Some(10), Some(10));
    /// builder.add_node(Some(0), &["Person"]).unwrap();
    /// builder.add_node(Some(1), &["Person"]).unwrap();
    /// builder.add_node(Some(2), &["Company"]).unwrap();
    /// builder.add_relationship(0, 1, "KNOWS").unwrap();
    /// builder.add_relationship(0, 2, "WORKS_FOR").unwrap();
    /// let snapshot = builder.finalize(None);
    ///
    /// // Returns a lazy iterator; collect it for a Vec, or iterate directly.
    /// let knows: Vec<_> = snapshot.neighbors_by_type(0, Direction::Outgoing, "KNOWS").collect();
    /// assert_eq!(knows, vec![1]);
    ///
    /// // Multiple types (resolved per call); a single &str or a pre-resolved
    /// // `rel_type(..)` are also accepted.
    /// let count = snapshot
    ///     .neighbors_by_type(0, Direction::Outgoing, &["KNOWS", "WORKS_FOR"])
    ///     .count();
    /// assert_eq!(count, 2);
    /// ```
    pub fn neighbors_by_type(
        &self,
        node_id: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
    ) -> NeighborsByType<'_> {
        let matcher = rel_types.into_match(self);
        self.neighbors_with(node_id, direction, matcher)
    }

    /// [`neighbors_by_type`](Self::neighbors_by_type) over a pre-resolved
    /// [`RelMatch`], so a hot caller (e.g. [`bfs_distances`](Self::bfs_distances)
    /// or [`chain_root`](Self::chain_root)) resolves the type filter once instead
    /// of on every node.
    fn neighbors_with(
        &self,
        node_id: NodeId,
        direction: Direction,
        matcher: RelMatch,
    ) -> NeighborsByType<'_> {
        let node = node_id as usize;
        let out = if matches!(direction, Direction::Outgoing | Direction::Both) {
            rel_range(&self.out_offsets, node)
        } else {
            0..0
        };
        let inc = if matches!(direction, Direction::Incoming | Direction::Both) {
            rel_range(&self.in_offsets, node)
        } else {
            0..0
        };
        NeighborsByType {
            graph: self,
            out,
            inc,
            matcher,
        }
    }

    /// Fold `node`'s `direction` neighbors of the given relationship type(s) in
    /// parallel, then merge the per-worker results — `neighbors_by_type` composed
    /// with a parallel fold.
    ///
    /// `identity` seeds each worker's accumulator, `fold` folds one neighbor into
    /// an accumulator, and `reduce` merges two accumulators. The neighbor list
    /// (usually small) is materialized once, then folded across threads; like
    /// [`NodeSet::par_fold`](crate::bitmap::NodeSet::par_fold) the parallelism is
    /// a private implementation detail, so the signature is pure `std` and callers
    /// depend on neither a parallelism crate nor the storage layout. Use this when
    /// each neighbor drives substantial independent work (e.g. a shortest-path
    /// search per source); for light per-neighbor work, iterate
    /// [`neighbors_by_type`](Self::neighbors_by_type) sequentially.
    pub fn par_neighbor_fold<T, ID, F, R>(
        &self,
        node: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
        identity: ID,
        fold: F,
        reduce: R,
    ) -> T
    where
        T: Send,
        ID: Fn() -> T + Sync + Send,
        F: Fn(T, NodeId) -> T + Sync + Send,
        R: Fn(T, T) -> T + Sync + Send,
    {
        use rayon::prelude::*;
        let neighbors: Vec<NodeId> = self.neighbors_by_type(node, direction, rel_types).collect();
        neighbors
            .into_par_iter()
            .fold(&identity, &fold)
            .reduce(&identity, &reduce)
    }

    /// Get the relationships incident to a node in the given direction,
    /// optionally filtered by relationship type (an empty `rel_types` matches
    /// all types).
    ///
    /// Unlike [`neighbors`](Self::neighbors), which yields only node IDs in a
    /// direction, each [`RelationshipRef`] carries the
    /// [`pos`](RelationshipRef::pos) needed to read the relationship's
    /// properties via [`rel_prop`](Self::rel_prop) —
    /// for incoming relationships as well as outgoing (the incoming position is
    /// mapped to where the property is stored). Use this when a query must read
    /// a per-relationship property during traversal, e.g. filtering `KNOWS`
    /// rels by a `creationDate`.
    pub fn relationships(
        &self,
        node_id: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
    ) -> RelationshipsByType<'_> {
        self.relationships_with(node_id, direction, rel_types.into_match(self))
    }

    /// [`relationships`](Self::relationships) over a pre-resolved [`RelMatch`],
    /// so a hot caller (e.g. [`dijkstra`](Self::dijkstra)) resolves the type
    /// filter once instead of on every node.
    fn relationships_with(
        &self,
        node_id: NodeId,
        direction: Direction,
        matcher: RelMatch,
    ) -> RelationshipsByType<'_> {
        let node = node_id as usize;
        let out = if matches!(direction, Direction::Outgoing | Direction::Both) {
            rel_range(&self.out_offsets, node)
        } else {
            0..0
        };
        let inc = if matches!(direction, Direction::Incoming | Direction::Both) {
            rel_range(&self.in_offsets, node)
        } else {
            0..0
        };
        RelationshipsByType {
            graph: self,
            out,
            inc,
            matcher,
        }
    }

    /// Weighted single-source shortest paths via Dijkstra's algorithm.
    ///
    /// The `weight` closure returns the cost of traversing a relationship; it
    /// receives the step's source node and the [`RelationshipRef`], so it can
    /// read a stored rel weight via [`RelationshipRef::pos`] (with
    /// [`rel_prop`](Self::rel_prop)) or compute a
    /// derived cost. Weights must be **non-negative** (Dijkstra's assumption).
    ///
    /// Pass a `target` to stop as soon as its shortest distance is known
    /// (single-pair search), or `None` to reach all nodes. `direction` and
    /// `rel_types` restrict which relationships are followed, as in
    /// [`relationships`](Self::relationships).
    pub fn dijkstra<W>(
        &self,
        source: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
        target: Option<NodeId>,
        weight: W,
    ) -> ShortestPaths
    where
        W: Fn(NodeId, &RelationshipRef) -> f64,
    {
        // Resolve the type filter once; the per-node clone is cheap for the
        // common All/One cases (a single type like "knows").
        let matcher = rel_types.into_match(self);
        let mut dist: HashMap<NodeId, f64> = HashMap::new();
        let mut prev: HashMap<NodeId, NodeId> = HashMap::new();
        let mut heap = std::collections::BinaryHeap::new();
        dist.insert(source, 0.0);
        heap.push(DijkstraState {
            cost: 0.0,
            node: source,
        });

        while let Some(DijkstraState { cost, node }) = heap.pop() {
            // Skip stale heap entries (a shorter path was found after pushing).
            if cost > *dist.get(&node).unwrap_or(&f64::INFINITY) {
                continue;
            }
            if Some(node) == target {
                break;
            }
            for rel in self.relationships_with(node, direction, matcher.clone()) {
                let next = cost + weight(node, &rel);
                if next < *dist.get(&rel.neighbor).unwrap_or(&f64::INFINITY) {
                    dist.insert(rel.neighbor, next);
                    prev.insert(rel.neighbor, node);
                    heap.push(DijkstraState {
                        cost: next,
                        node: rel.neighbor,
                    });
                }
            }
        }
        ShortestPaths { source, dist, prev }
    }

    /// Weighted shortest-path *cost* from `source` to `target` via bidirectional
    /// Dijkstra: it searches from both ends and meets in the middle, exploring far
    /// fewer nodes than a one-directional search for a point-to-point query on a
    /// large, well-connected component. Returns `None` if `target` is unreachable.
    ///
    /// `weight` returns a non-negative rel cost, as in [`dijkstra`](Self::dijkstra)
    /// (an infinite weight prunes the rel). The backward search follows the
    /// reverse of `direction`, so `weight` must be symmetric — the usual case for
    /// an undirected `Direction::Both` graph (e.g. `KNOWS`). When you need
    /// distances to *many* targets from one source, or the path itself, use the
    /// single-source [`dijkstra`](Self::dijkstra) instead.
    pub fn weighted_shortest_path<W>(
        &self,
        source: NodeId,
        target: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
        weight: W,
    ) -> Option<f64>
    where
        W: Fn(NodeId, &RelationshipRef) -> f64,
    {
        if source == target {
            return Some(0.0);
        }
        let matcher = rel_types.into_match(self);
        let rev = Self::reverse_direction(direction);

        // The two `dist` maps and `heap` frontiers are pure working state: a fresh
        // pair of `HashMap`s and `BinaryHeap`s every call churned ~1 MB on a deep
        // weighted search. They're reused from a thread-local scratch instead and
        // `clear()`ed (which keeps capacity) at the top of each call, so steady
        // state allocates nothing. Thread-local so parallel callers never contend.
        #[derive(Default)]
        struct WspScratch {
            dist_f: HashMap<NodeId, f64>,
            dist_b: HashMap<NodeId, f64>,
            heap_f: std::collections::BinaryHeap<DijkstraState>,
            heap_b: std::collections::BinaryHeap<DijkstraState>,
        }
        thread_local! {
            static WSP_SCRATCH: std::cell::RefCell<WspScratch> =
                std::cell::RefCell::new(WspScratch::default());
        }

        WSP_SCRATCH.with(|cell| {
            // Deref the guard once to a `&mut WspScratch` so the borrow checker can
            // split-borrow disjoint fields (`heap_f`/`dist_f`/`dist_b`) below.
            let mut guard = cell.borrow_mut();
            let s = &mut *guard;
            s.dist_f.clear();
            s.dist_b.clear();
            s.heap_f.clear();
            s.heap_b.clear();

            s.dist_f.insert(source, 0.0);
            s.dist_b.insert(target, 0.0);
            s.heap_f.push(DijkstraState {
                cost: 0.0,
                node: source,
            });
            s.heap_b.push(DijkstraState {
                cost: 0.0,
                node: target,
            });
            // Best meeting cost found so far; the frontiers can't beat `top_f + top_b`.
            let mut best = f64::INFINITY;

            while !s.heap_f.is_empty() || !s.heap_b.is_empty() {
                let top_f = s.heap_f.peek().map_or(f64::INFINITY, |st| st.cost);
                let top_b = s.heap_b.peek().map_or(f64::INFINITY, |st| st.cost);
                if top_f + top_b >= best {
                    break;
                }
                // Expand whichever frontier is currently smaller.
                let (heap, dist, other, dir) = if top_f <= top_b {
                    (&mut s.heap_f, &mut s.dist_f, &s.dist_b, direction)
                } else {
                    (&mut s.heap_b, &mut s.dist_b, &s.dist_f, rev)
                };
                let Some(DijkstraState { cost, node }) = heap.pop() else {
                    continue;
                };
                if cost > *dist.get(&node).unwrap_or(&f64::INFINITY) {
                    continue; // stale heap entry
                }
                // `node` is settled on this side; if the other side reached it too,
                // the two halves form a candidate path.
                if let Some(&other_cost) = other.get(&node) {
                    if cost + other_cost < best {
                        best = cost + other_cost;
                    }
                }
                for rel in self.relationships_with(node, dir, matcher.clone()) {
                    let next = cost + weight(node, &rel);
                    if next < *dist.get(&rel.neighbor).unwrap_or(&f64::INFINITY) {
                        dist.insert(rel.neighbor, next);
                        heap.push(DijkstraState {
                            cost: next,
                            node: rel.neighbor,
                        });
                    }
                }
            }
            best.is_finite().then_some(best)
        })
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
    /// builder.add_relationship(5, 10, "KNOWS").unwrap();
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
        let allowed_types: Option<std::collections::HashSet<RelationshipType>> = self
            .resolve_rel_types(rel_types)
            .map(|ids| ids.into_iter().collect());

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
    ///   - `Outgoing`: Forward search uses outgoing rels, backward search uses incoming rels (default for finding paths from source to target)
    ///   - `Incoming`: Forward search uses incoming rels, backward search uses outgoing rels (reverse direction)
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
    /// builder.add_relationship(0, 10, "KNOWS").unwrap();
    /// builder.add_relationship(1, 11, "WORKS_WITH").unwrap();
    /// let snapshot = builder.finalize(None);
    ///
    /// // Simple bidirectional search (default: Outgoing for forward, Incoming for backward)
    /// let source = NodeSet::from(RoaringBitmap::from_iter([0, 1]));
    /// let target = NodeSet::from(RoaringBitmap::from_iter([10, 11]));
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
    #[allow(clippy::too_many_arguments)]
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
            let neighbors =
                self.get_neighbors_with_positions_for_direction(current, direction, allowed_types);
            for (neighbor, rel_type, csr_pos) in neighbors {
                if let Some(ref rf) = rel_filter {
                    let (from, to) = if is_backward {
                        (neighbor, current)
                    } else {
                        (current, neighbor)
                    };
                    if !rf(from, to, rel_type, csr_pos, self) {
                        continue;
                    }
                }
                if other_visited_nodes.contains(neighbor) {
                    visited_rels.insert(csr_pos);
                    if visited_nodes.insert(neighbor)
                        && node_filter.as_ref().is_none_or(|f| f(neighbor, self))
                    {
                        queue.push_back((neighbor, depth + 1));
                    }
                } else if node_filter.as_ref().is_none_or(|f| f(neighbor, self))
                    && visited_nodes.insert(neighbor)
                {
                    visited_rels.insert(csr_pos);
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
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
        let allowed_types: Option<std::collections::HashSet<RelationshipType>> = self
            .resolve_rel_types(rel_types)
            .map(|ids| ids.into_iter().collect());

        let mut forward_visited_nodes = RoaringBitmap::new();
        let mut forward_visited_rels = RoaringBitmap::new();
        let mut backward_visited_nodes = RoaringBitmap::new();
        let mut backward_visited_rels = RoaringBitmap::new();
        let mut forward_queue = std::collections::VecDeque::new();
        let mut backward_queue = std::collections::VecDeque::new();

        for node_id in source_nodes.iter() {
            if node_filter.as_ref().is_none_or(|f| f(node_id, self)) {
                forward_visited_nodes.insert(node_id);
                forward_queue.push_back((node_id, 0u32));
            }
        }

        for node_id in target_nodes.iter() {
            if node_filter.as_ref().is_none_or(|f| f(node_id, self)) {
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
                &mut forward_queue,
                &mut forward_visited_nodes,
                &mut forward_visited_rels,
                &backward_visited_nodes,
                direction,
                &allowed_types,
                &node_filter,
                &rel_filter,
                max_depth,
                false,
            );
            self.bfs_expand_step(
                &mut backward_queue,
                &mut backward_visited_nodes,
                &mut backward_visited_rels,
                &forward_visited_nodes,
                backward_direction,
                &allowed_types,
                &node_filter,
                &rel_filter,
                max_depth,
                true,
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
    /// Performs BFS from the starting nodes, following rels in the specified direction.
    /// Returns all nodes and relationships visited during the traversal.
    ///
    /// # Arguments
    /// * `start_nodes` - Starting nodes for traversal
    /// * `direction` - Direction of traversal:
    ///   - `Outgoing`: Follow outgoing rels
    ///   - `Incoming`: Follow incoming rels
    ///   - `Both`: Follow both outgoing and incoming rels
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
    /// builder.add_relationship(0, 1, "KNOWS").unwrap();
    /// builder.add_relationship(0, 2, "WORKS_FOR").unwrap();
    /// let snapshot = builder.finalize(None);
    ///
    /// // Simple BFS from a single node
    /// let start = NodeSet::from(RoaringBitmap::from_iter([0]));
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
        let allowed_types: Option<std::collections::HashSet<RelationshipType>> = self
            .resolve_rel_types(rel_types)
            .map(|ids| ids.into_iter().collect());

        let mut queue = std::collections::VecDeque::new();
        let mut visited_nodes = RoaringBitmap::new();
        let mut visited_rels = RoaringBitmap::new();

        for node_id in start_nodes.iter() {
            if node_filter.as_ref().is_none_or(|f| f(node_id, self))
                && visited_nodes.insert(node_id)
            {
                queue.push_back((node_id, 0u32));
            }
        }

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(max) = max_depth {
                if depth >= max {
                    continue;
                }
            }

            let neighbors =
                self.get_neighbors_with_positions_for_direction(current, direction, &allowed_types);

            for (neighbor, rel_type, csr_pos) in neighbors {
                if let Some(ref rf) = rel_filter {
                    if !rf(current, neighbor, rel_type, csr_pos, self) {
                        continue;
                    }
                }

                if node_filter.as_ref().is_none_or(|f| f(neighbor, self)) {
                    visited_rels.insert(csr_pos);
                    if visited_nodes.insert(neighbor) {
                        queue.push_back((neighbor, depth + 1));
                    }
                }
            }
        }

        (visited_nodes, visited_rels)
    }

    /// The per-node forest-root array for the functional `rel_type` chain in
    /// `direction`: index it by `node` to get the terminal of that node's chain —
    /// the node reached by following the single outgoing `rel_type` rel until one
    /// with no such rel (a node already terminal maps to itself).
    ///
    /// The array is built once per `(direction, rel_type)` in `O(node_count)` with
    /// path compression, then cached, so a hot loop should call this **once** and
    /// index the returned slice lock-free rather than calling
    /// [`chain_root`](Self::chain_root) per node. Intended for a relationship that
    /// is *functional* in `direction` (each node has at most one outgoing
    /// `rel_type` rel), so the reachable structure is a forest — e.g. walking
    /// `replyOf` from a reply up to its root message. Malformed, non-functional
    /// data (a node with several such rels, or a cycle) follows the first
    /// neighbor in CSR order and is broken by a `node_count` depth cap, resolving
    /// deterministically. Pass a pre-resolved [`RelationshipType`] (via
    /// [`rel_type`](Self::rel_type)).
    pub fn chain_roots(&self, direction: Direction, rel_type: RelationshipType) -> ChainRoots {
        let key = (direction, rel_type);
        {
            let index = self
                .chain_root_index
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(roots) = index.get(&key) {
                return Arc::clone(roots);
            }
        }
        // Build outside the lock; another thread may race us, in which case the
        // first-inserted array wins (both are identical).
        let roots: ChainRoots = self.build_chain_roots(direction, rel_type).into();
        let mut index = self
            .chain_root_index
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Arc::clone(index.entry(key).or_insert(roots))
    }

    /// The terminal of `node`'s functional `rel_type` chain in `direction` (a node
    /// already terminal maps to itself). Convenience wrapper over
    /// [`chain_roots`](Self::chain_roots); for a per-node hot loop, call
    /// `chain_roots` once and index the slice instead of calling this per node.
    pub fn chain_root(
        &self,
        node: NodeId,
        direction: Direction,
        rel_type: RelationshipType,
    ) -> NodeId {
        self.chain_roots(direction, rel_type)
            .get(node as usize)
            .copied()
            .unwrap_or(node)
    }

    /// Build the forest-root array for `(direction, rel_type)`, where `root[n]` is
    /// the terminal of `n`'s chain. `O(node_count)`: each node is resolved once
    /// via path compression. A depth cap of `node_count` breaks any cycle a
    /// malformed, non-functional graph might contain, so the build always
    /// terminates and is deterministic.
    fn build_chain_roots(&self, direction: Direction, rel_type: RelationshipType) -> Vec<NodeId> {
        let n = self.n_nodes as usize;
        let matcher = RelMatch::one(rel_type);
        const UNRESOLVED: NodeId = NodeId::MAX;
        let mut root = vec![UNRESOLVED; n];
        let mut path: Vec<NodeId> = Vec::new();
        for start in 0..n as NodeId {
            if root[start as usize] != UNRESOLVED {
                continue;
            }
            path.clear();
            let mut cur = start;
            let terminal = loop {
                let resolved = root[cur as usize];
                if resolved != UNRESOLVED {
                    break resolved;
                }
                match self.neighbors_with(cur, direction, matcher.clone()).next() {
                    // `path.len() <= n` bounds a malformed cycle (a valid chain has
                    // at most `n - 1` rels, so this never trips on a forest).
                    Some(parent) if path.len() <= n => {
                        path.push(cur);
                        cur = parent;
                    }
                    _ => break cur,
                }
            };
            for &node in &path {
                root[node as usize] = terminal;
            }
            root[start as usize] = terminal;
        }
        root
    }

    /// Unweighted single-source breadth-first search returning the hop distance
    /// from `start` to every reached node (`start` itself maps to `0`).
    ///
    /// `max_depth = Some(d)` restricts the search to nodes within `d` hops of
    /// `start`; `None` reaches the entire connected component. `direction` and
    /// `rel_types` restrict which relationships are followed, exactly as in
    /// [`neighbors_by_type`](Self::neighbors_by_type).
    ///
    /// Distances are integer hop counts and no predecessor map is built, so this
    /// is the primitive to reach for when you need "every node within k hops" or
    /// the eccentricity of `start` over an unweighted relationship — cheaper than
    /// [`dijkstra`](Self::dijkstra) with a unit weight, which carries f64 costs
    /// and path-reconstruction state you would discard.
    pub fn bfs_distances(
        &self,
        start: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
        max_depth: Option<u32>,
    ) -> HashMap<NodeId, u32> {
        // The visited/distance set is the BFS hot path. A `HashMap` hashes and
        // probes on every neighbor; a dense `Vec<u32>` indexed by node id is a
        // direct O(1) access instead. To avoid an O(n) clear (or a per-call
        // multi-MB memset) we stamp each run with a generation counter: a node
        // counts as visited this run iff `gen[node] == cur`. The scratch is
        // thread-local so parallel callers never contend on it. The returned map
        // is unchanged (built pre-sized from only the visited nodes).
        #[derive(Default)]
        struct BfsScratch {
            gen: Vec<u32>,
            dist: Vec<u32>,
            cur: u32,
        }
        thread_local! {
            static BFS_SCRATCH: std::cell::RefCell<BfsScratch> =
                std::cell::RefCell::new(BfsScratch::default());
        }

        let matcher = rel_types.into_match(self);
        let n = self.node_count() as usize;
        BFS_SCRATCH.with(|cell| {
            let mut s = cell.borrow_mut();
            if s.gen.len() < n {
                s.gen.resize(n, 0);
                s.dist.resize(n, 0);
            }
            // Bump the run stamp; reset once on the (4-billion-call) wrap.
            s.cur = s.cur.wrapping_add(1);
            if s.cur == 0 {
                s.gen.iter_mut().for_each(|g| *g = 0);
                s.cur = 1;
            }
            let cur = s.cur;

            s.gen[start as usize] = cur;
            s.dist[start as usize] = 0;
            let mut touched: Vec<NodeId> = vec![start];
            let mut frontier: Vec<NodeId> = vec![start];
            let mut depth = 0u32;
            while !frontier.is_empty() {
                if max_depth.is_some_and(|max| depth >= max) {
                    break;
                }
                let next_depth = depth + 1;
                let mut next: Vec<NodeId> = Vec::new();
                for &node in &frontier {
                    for neighbor in self.neighbors_with(node, direction, matcher.clone()) {
                        let ni = neighbor as usize;
                        if s.gen[ni] != cur {
                            s.gen[ni] = cur;
                            s.dist[ni] = next_depth;
                            next.push(neighbor);
                            touched.push(neighbor);
                        }
                    }
                }
                frontier = next;
                depth = next_depth;
            }

            let mut dist: HashMap<NodeId, u32> = HashMap::with_capacity(touched.len());
            for node in touched {
                dist.insert(node, s.dist[node as usize]);
            }
            dist
        })
    }

    /// The set of nodes whose hop-distance from `seed` (along `rel_types` in
    /// `direction`) lies in `hops` — the bounded **neighborhood** around `seed`,
    /// returned as a [`NodeSet`] so downstream membership is O(1) and intersecting
    /// it with another set (e.g. "2-hop friends ∩ members of a country") is one
    /// bitmap op. `hops` is a closed range: `1..=2` is "one or two hops out"
    /// (excludes `seed`), `0..=2` includes `seed`, `2..=2` is exactly two hops.
    /// A bounded level-synchronous BFS that collects straight into the set (no
    /// intermediate distance map); the visited scratch is thread-local, mirroring
    /// [`bfs_distances`](Self::bfs_distances).
    pub fn neighborhood(
        &self,
        seed: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
        hops: std::ops::RangeInclusive<u32>,
    ) -> NodeSet {
        let (lo, hi) = (*hops.start(), *hops.end());
        let mut result = NodeSet::empty();
        if lo > hi {
            return result;
        }
        // Gen-stamped visited scratch (the bfs_distances pattern): a node counts as
        // visited this run iff `gen[node] == cur`, avoiding an O(n) clear per call.
        #[derive(Default)]
        struct NbhdScratch {
            gen: Vec<u32>,
            cur: u32,
        }
        thread_local! {
            static NBHD_SCRATCH: std::cell::RefCell<NbhdScratch> =
                std::cell::RefCell::new(NbhdScratch::default());
        }
        let matcher = rel_types.into_match(self);
        let n = self.node_count() as usize;
        NBHD_SCRATCH.with(|cell| {
            let mut s = cell.borrow_mut();
            if s.gen.len() < n {
                s.gen.resize(n, 0);
            }
            s.cur = s.cur.wrapping_add(1);
            if s.cur == 0 {
                s.gen.iter_mut().for_each(|g| *g = 0);
                s.cur = 1;
            }
            let cur = s.cur;
            s.gen[seed as usize] = cur;
            if lo == 0 {
                result.insert(seed);
            }
            let mut frontier: Vec<NodeId> = vec![seed];
            let mut depth = 0u32;
            while depth < hi && !frontier.is_empty() {
                depth += 1;
                let mut next: Vec<NodeId> = Vec::new();
                for &node in &frontier {
                    for neighbor in self.neighbors_with(node, direction, matcher.clone()) {
                        let ni = neighbor as usize;
                        if s.gen[ni] != cur {
                            s.gen[ni] = cur;
                            next.push(neighbor);
                            if depth >= lo {
                                result.insert(neighbor);
                            }
                        }
                    }
                }
                frontier = next;
            }
        });
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
    pub fn nodes_with_property<V: IntoValueId>(
        &self,
        label: &str,
        key: &str,
        value: V,
    ) -> Option<NodeSet> {
        let label_id = self.label_from_str(label)?;
        let key_id = self.property_key_from_str(key)?;
        let value_id = value.into_value_id(self)?;
        self.nodes_with_property_id(label_id, key_id, value_id)
    }

    /// Get nodes with a specific property value (internal ID-based version).
    /// Uses check-release-build-reacquire pattern to avoid holding the mutex during index build.
    fn nodes_with_property_id(
        &self,
        label: Label,
        key: PropertyKey,
        value: ValueId,
    ) -> Option<NodeSet> {
        let index_key = (label, key);

        // Check if the index already exists (short lock)
        {
            let index = self
                .prop_index
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(key_index) = index.get(&index_key) {
                return key_index.get(&value).cloned();
            }
        }
        // Release lock, build index outside the lock
        let label_nodes = self.nodes_with_label_id(label)?;
        let column = self.columns.get(&key)?;
        let key_index_final =
            Self::build_property_index_for_key_and_label(column, Some(label_nodes));

        // Re-acquire lock and insert (another thread may have built it first)
        let mut index = self
            .prop_index
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = index.entry(index_key).or_insert(key_index_final);
        entry.get(&value).cloned()
    }

    /// Whether `node` carries `label` — the fundamental label-membership test
    /// (`nodes_with_label` + bitmap `contains`), exposed so callers need not pair
    /// the two by hand.
    pub fn has_label(&self, node: NodeId, label: &str) -> bool {
        self.label_from_str(label)
            .and_then(|l| self.nodes_with_label_id(l))
            .is_some_and(|ns| ns.contains(node))
    }

    /// Neighbours of `node` via `rel_types` in `direction` that are members of
    /// `set` — the typed traversal filtered by a node set, as a bitmap membership
    /// test. `set` can be a label's nodes ([`nodes_with_label`](Self::nodes_with_label)),
    /// an [`full_text_search`](Self::full_text_search) / geo result, or any precomputed [`NodeSet`]; this
    /// generalizes "typed neighbours carrying a label" and the per-candidate set
    /// intersections in co-occurrence queries. Duplicate rels are preserved, so
    /// it still composes with counting. `rel_types` accepts a single type, a slice
    /// of types, or `None` for any type (see [`RelTypeFilter`]).
    pub fn neighbors_in_set<'a>(
        &'a self,
        node: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
        set: &'a NodeSet,
    ) -> impl Iterator<Item = NodeId> + 'a {
        self.neighbors_by_type(node, direction, rel_types)
            .filter(move |&n| set.contains(n))
    }

    /// The first neighbour of `node` along `rel_types` in `direction`, or `None`
    /// -- the `neighbors_by_type(..).next()` idiom that pervades single-step
    /// lookups (a message's creator, a person's city, ...). `rel_types` accepts a
    /// single type, a slice, or `None` for any type (see [`RelTypeFilter`]).
    pub fn first_neighbor(
        &self,
        node: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
    ) -> Option<NodeId> {
        self.neighbors_by_type(node, direction, rel_types).next()
    }

    /// Walk a fixed chain of single-relationship steps from `start`, taking the
    /// first neighbour at each `(direction, rel_type)` step -- e.g. person ->
    /// city -> country. Returns `None` as soon as a step has no neighbour. (Each
    /// step follows exactly one rel type; for a step with alternative types, call
    /// [`first_neighbor`](Self::first_neighbor) directly.)
    pub fn follow(&self, start: NodeId, steps: &[(Direction, &str)]) -> Option<NodeId> {
        let mut cur = start;
        for &(direction, rel) in steps {
            cur = self.first_neighbor(cur, direction, rel)?;
        }
        Some(cur)
    }

    /// Build a [`NeighborGroups`] query over each source node's `rel` neighbours
    /// (in `direction`): group every source's neighbours by a projected attribute
    /// and reduce per source. Nothing runs until a terminal is called. E.g. BI
    /// Q4's biggest single-country membership per forum:
    ///
    /// ```ignore
    /// g.neighbor_groups(&forums, "hasMember", Direction::Outgoing)
    ///     .project(&[(Direction::Outgoing, "isLocatedIn"), (Direction::Outgoing, "isPartOf")])
    ///     .top_by_size(100, Some("flid"))
    /// ```
    pub fn neighbor_groups<'a>(
        &'a self,
        sources: &'a [NodeId],
        rel: &'a str,
        direction: Direction,
    ) -> NeighborGroups<'a> {
        NeighborGroups {
            graph: self,
            sources,
            rel,
            direction,
            project: Vec::new(),
        }
    }

    /// Fold relationship `rel` (in `direction`) into a weighted node-pair map by
    /// projecting both endpoints of each rel through `projection` — the one-mode /
    /// bipartite projection ("network folding") of a relation onto a derived node
    /// set. For every `rel` rel `a -> b`, map `a' = projection[a]` and
    /// `b' = projection[b]` and add one to the count of the *unordered* pair
    /// `(min(a',b'), max(a',b'))`. Self-pairs (`a' == b'`) and endpoints projecting
    /// to the `u32::MAX` sentinel (no neighbour) are skipped.
    ///
    /// `projection` is a flat `node -> node` array indexed by node id — typically the
    /// `Vec`/slice behind a one-hop functional neighbour map (the binding's
    /// `neighbor_via`) or a chain-root map ([`chain_roots`](Self::chain_roots)). The
    /// canonical use is BI Q19 / IC14's interaction graph: fold `replyOf` rels
    /// (comment -> parent) through each message's `hasCreator`, yielding a
    /// person-pair -> reply-count map. Runs in parallel over the node range (each
    /// worker folds into a thread-local map, merged at the end); an unknown `rel`
    /// yields an empty map.
    pub fn fold_via(
        &self,
        rel: &str,
        direction: Direction,
        projection: &[NodeId],
    ) -> HashMap<(NodeId, NodeId), u64> {
        use rayon::prelude::*;
        let Some(rel_t) = self.relationship_type_from_str(rel) else {
            return HashMap::new();
        };
        (0..self.n_nodes)
            .into_par_iter()
            .fold(HashMap::new, |mut acc, src| {
                let Some(&a) = projection.get(src as usize) else {
                    return acc;
                };
                if a == u32::MAX {
                    return acc;
                }
                for dst in self.neighbors_by_type(src, direction, rel_t) {
                    if let Some(&b) = projection.get(dst as usize) {
                        if b != u32::MAX && a != b {
                            let key = if a < b { (a, b) } else { (b, a) };
                            *acc.entry(key).or_insert(0) += 1;
                        }
                    }
                }
                acc
            })
            .reduce(HashMap::new, |a, b| {
                let (mut large, small) = if a.len() >= b.len() { (a, b) } else { (b, a) };
                for (k, v) in small {
                    *large.entry(k).or_insert(0) += v;
                }
                large
            })
    }

    /// Seeded co-occurrence — one-mode / bipartite projection by shared neighbour.
    /// From `seed`, over relationship `rel`: the nodes that share a `rel`-neighbour
    /// with `seed` (`seed -(rel,direction)-> shared centers -(rel, reversed)-> the
    /// co-occurring nodes`), `seed` itself excluded. Each co-occurring node's weight
    /// is its shared-center count ([`CoWeight::Count`]) or the number of distinct
    /// values of a center property ([`CoWeight::Distinct`], e.g. distinct days).
    /// Returns `{other: weight}`. The seeded row of the node-node co-occurrence
    /// matrix; the by-shared-neighbour complement of [`fold_via`](Self::fold_via)'s
    /// by-rel-endpoint projection. Unknown `rel`/`key` yields an empty map.
    pub fn co_occurring(
        &self,
        seed: NodeId,
        rel: &str,
        direction: Direction,
        weight: CoWeight,
    ) -> HashMap<NodeId, u64> {
        let Some(rel_t) = self.relationship_type_from_str(rel) else {
            return HashMap::new();
        };
        let back = Self::reverse_direction(direction);
        match weight {
            CoWeight::Count => {
                let mut counts: HashMap<NodeId, u64> = HashMap::new();
                for center in self.neighbors_by_type(seed, direction, rel_t) {
                    for other in self.neighbors_by_type(center, back, rel_t) {
                        if other != seed {
                            *counts.entry(other).or_insert(0) += 1;
                        }
                    }
                }
                counts
            }
            CoWeight::Distinct(key) => {
                let Some(key_id) = self.property_key_from_str(key) else {
                    return HashMap::new();
                };
                let mut sets: HashMap<NodeId, std::collections::HashSet<ValueId>> = HashMap::new();
                for center in self.neighbors_by_type(seed, direction, rel_t) {
                    // The distinct value carried by this center (e.g. its day); a
                    // center without it contributes no co-occurrence.
                    let Some(val) = self.prop_id(center, key_id) else {
                        continue;
                    };
                    for other in self.neighbors_by_type(center, back, rel_t) {
                        if other != seed {
                            sets.entry(other).or_default().insert(val);
                        }
                    }
                }
                sets.into_iter().map(|(o, s)| (o, s.len() as u64)).collect()
            }
        }
    }

    /// Whether `node` has at least one `rel_types` neighbour in `direction` — the
    /// existence predicate (`neighbors_by_type(..).next().is_some()`) behind facet
    /// "has any X rel" checks.
    pub fn has_rel(
        &self,
        node: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
    ) -> bool {
        self.first_neighbor(node, direction, rel_types).is_some()
    }

    /// Whether any `rel_types` neighbour of `node` in `direction` has property `key`
    /// equal to `value` — e.g. "linked to a node whose `uri` is X". The comparison
    /// value is resolved to its id once (outside the neighbour scan), so this is a
    /// per-neighbour id compare rather than a per-neighbour string resolve.
    pub fn has_neighbor_with_property<V: IntoValueId>(
        &self,
        node: NodeId,
        direction: Direction,
        rel_types: impl RelTypeFilter,
        key: &str,
        value: V,
    ) -> bool {
        let (Some(key_id), Some(value_id)) =
            (self.property_key_from_str(key), value.into_value_id(self))
        else {
            return false;
        };
        let Some(column) = self.columns.get(&key_id) else {
            return false;
        };
        self.neighbors_by_type(node, direction, rel_types)
            .any(|t| column.get(t) == Some(value_id))
    }

    /// Find a single node by a property value across ALL labels (unlike
    /// [`nodes_with_property`](Self::nodes_with_property), which is label-scoped).
    /// Useful for a unique key such as a `uri`; returns the smallest matching node
    /// id. The label-free `(key, value)` index is built lazily on first call and
    /// cached under a reserved sentinel label.
    pub fn node_with_property<V: IntoValueId>(&self, key: &str, value: V) -> Option<NodeId> {
        let any = Label::new(u32::MAX);
        let key_id = self.property_key_from_str(key)?;
        let value_id = value.into_value_id(self)?;
        {
            let index = self
                .prop_index
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(key_index) = index.get(&(any, key_id)) {
                return key_index.get(&value_id).and_then(|ns| ns.iter().next());
            }
        }
        let column = self.columns.get(&key_id)?;
        let key_index = Self::build_property_index_for_key_and_label(column, None);
        let mut index = self
            .prop_index
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = index.entry((any, key_id)).or_insert(key_index);
        entry.get(&value_id).and_then(|ns| ns.iter().next())
    }

    /// Find a single node with `label` whose property `key` equals `value` — the
    /// label-scoped sibling of [`node_with_property`](Self::node_with_property), returning
    /// the smallest matching node id (or `None`). Use this where the key is unique only
    /// *within* a label (a `name` shared across labels, a per-type LDBC id), so the
    /// label-free [`node_with_property`] could match a node of a different label. Reuses
    /// the cached `(label, key)` index that [`nodes_with_property`](Self::nodes_with_property)
    /// builds, but takes the first matching node directly — without cloning the value's
    /// `NodeSet` (the single-node analog of `nodes_with_property`).
    pub fn node_with_label_property<V: IntoValueId>(
        &self,
        label: &str,
        key: &str,
        value: V,
    ) -> Option<NodeId> {
        let label_id = self.label_from_str(label)?;
        let key_id = self.property_key_from_str(key)?;
        let value_id = value.into_value_id(self)?;
        let index_key = (label_id, key_id);
        {
            let index = self
                .prop_index
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(key_index) = index.get(&index_key) {
                return key_index.get(&value_id).and_then(|ns| ns.iter().next());
            }
        }
        let label_nodes = self.nodes_with_label_id(label_id)?;
        let column = self.columns.get(&key_id)?;
        let key_index = Self::build_property_index_for_key_and_label(column, Some(label_nodes));
        let mut index = self
            .prop_index
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = index.entry(index_key).or_insert(key_index);
        entry.get(&value_id).and_then(|ns| ns.iter().next())
    }

    /// Histogram of the neighbour nodes reached from `sources` via `rel_type` in
    /// `direction` (how many of the sources point to each neighbour) — the
    /// "count rels into each neighbour" aggregation behind many group-by-count
    /// queries.
    pub fn neighbor_counts(
        &self,
        sources: impl IntoIterator<Item = NodeId>,
        direction: Direction,
        rel_type: impl RelTypeFilter,
    ) -> HashMap<NodeId, usize> {
        // Count into a thread-local dense buffer keyed by target id instead of a
        // growing HashMap: a direct index avoids per-target hashing and the map's
        // doubling reallocs. A per-call generation stamp means no O(n) clear, and
        // thread-local storage means parallel callers never contend. The hot loop
        // also hoists the rel-type resolution out (resolve once, not per source).
        #[derive(Default)]
        struct CountScratch {
            val: Vec<u32>,
            gen: Vec<u32>,
            cur: u32,
            touched: Vec<NodeId>,
        }
        thread_local! {
            static COUNT_SCRATCH: std::cell::RefCell<CountScratch> =
                std::cell::RefCell::new(CountScratch::default());
        }

        let matcher = rel_type.into_match(self);
        let n = self.node_count() as usize;
        COUNT_SCRATCH.with(|cell| {
            let mut s = cell.borrow_mut();
            if s.val.len() < n {
                s.val.resize(n, 0);
                s.gen.resize(n, 0);
            }
            s.cur = s.cur.wrapping_add(1);
            if s.cur == 0 {
                s.gen.iter_mut().for_each(|g| *g = 0);
                s.cur = 1;
            }
            let cur = s.cur;
            s.touched.clear();

            for src in sources {
                for t in self.neighbors_with(src, direction, matcher.clone()) {
                    let ti = t as usize;
                    if s.gen[ti] != cur {
                        s.gen[ti] = cur;
                        s.val[ti] = 0;
                        s.touched.push(t);
                    }
                    s.val[ti] += 1;
                }
            }

            let mut counts: HashMap<NodeId, usize> = HashMap::with_capacity(s.touched.len());
            for i in 0..s.touched.len() {
                let t = s.touched[i];
                counts.insert(t, s.val[t as usize] as usize);
            }
            counts
        })
    }

    /// Full-text search: nodes of `label` whose `key` string property contains
    /// every token in `query` (boolean AND). Returns the empty set for an
    /// unknown label/key, an empty query, or a token no document contains.
    ///
    /// The per-`(label, key)` inverted index is built lazily on first call and
    /// cached (the same check-release-build-reacquire pattern as the equality
    /// index). The result is a [`NodeSet`], so it composes with
    /// [`nodes_with_label`](Self::nodes_with_label) and other node sets via
    /// `&`/`|`/`-`.
    pub fn full_text_search(&self, label: &str, key: &str, query: &str) -> NodeSet {
        let (Some(label_id), Some(key_id)) =
            (self.label_from_str(label), self.property_key_from_str(key))
        else {
            return NodeSet::empty();
        };
        let index_key = (label_id, key_id);

        // Fast path: index already built (short lock).
        {
            let index = self
                .fulltext_index
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(field) = index.get(&index_key) {
                return field.query(query);
            }
        }

        // Build outside the lock, then cache (a racing build is identical).
        let field = self.build_fulltext_field(label_id, key_id);
        let result = field.query(query);
        let mut index = self
            .fulltext_index
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        index.entry(index_key).or_insert(field);
        result
    }

    /// BM25-ranked full-text search: the top `k` nodes of `label` by relevance
    /// of their `key` string property to `query` (disjunctive — a node scores
    /// for every query token it contains). Returns `(node, score)` pairs sorted
    /// by score descending, ties broken by ascending node id. Shares the same
    /// lazily built `(label, key)` index as [`full_text_search`](Self::full_text_search).
    pub fn full_text_search_ranked(
        &self,
        label: &str,
        key: &str,
        query: &str,
        k: usize,
    ) -> Vec<(NodeId, f32)> {
        let (Some(label_id), Some(key_id)) =
            (self.label_from_str(label), self.property_key_from_str(key))
        else {
            return Vec::new();
        };
        let index_key = (label_id, key_id);

        // Fast path: index already built (short lock).
        {
            let index = self
                .fulltext_index
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(field) = index.get(&index_key) {
                return field.query_ranked(query, k);
            }
        }

        // Build outside the lock, then cache (a racing build is identical).
        let field = self.build_fulltext_field(label_id, key_id);
        let result = field.query_ranked(query, k);
        let mut index = self
            .fulltext_index
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        index.entry(index_key).or_insert(field);
        result
    }

    /// Build the inverted index for one `(label, key)` text field by scanning
    /// the string column and tokenizing each labelled node's value.
    fn build_fulltext_field(&self, label: Label, key: PropertyKey) -> FullTextField {
        let label_nodes = self.nodes_with_label_id(label);
        let Some(column) = self.columns.get(&key) else {
            return FullTextField::default();
        };
        let docs = column
            .iter_entries()
            .filter_map(|(node, value)| match value {
                ValueId::Str(sid) if label_nodes.is_some_and(|ns| ns.contains(node)) => {
                    self.atoms.resolve(sid).map(|text| (node, text))
                }
                _ => None,
            });
        FullTextField::build(docs)
    }

    /// Geo-spatial search: nodes of `label` within `km` great-circle distance of
    /// `(lat, lon)`, using the `lat_key`/`lon_key` f64 properties. The result is
    /// a [`NodeSet`], composing with [`full_text_search`](Self::full_text_search) and
    /// [`nodes_with_label`](Self::nodes_with_label) via `&`/`|`/`-`. The
    /// per-`(label, lat_key, lon_key)` index is built lazily and cached.
    pub fn geo_within_radius(
        &self,
        label: &str,
        lat_key: &str,
        lon_key: &str,
        lat: f64,
        lon: f64,
        km: f64,
    ) -> NodeSet {
        self.with_geo_index(label, lat_key, lon_key, |gi| gi.within_radius(lat, lon, km))
            .unwrap_or_else(NodeSet::empty)
    }

    /// Geo-spatial search: nodes of `label` whose `(lat_key, lon_key)` fall in
    /// the lat/lon rectangle with corners `min = (lat, lon)` and `max =
    /// (lat, lon)`. When `min.1 > max.1` the box crosses the antimeridian.
    /// Returns a [`NodeSet`].
    pub fn geo_within_bbox(
        &self,
        label: &str,
        lat_key: &str,
        lon_key: &str,
        min: (f64, f64),
        max: (f64, f64),
    ) -> NodeSet {
        self.with_geo_index(label, lat_key, lon_key, |gi| {
            gi.within_bbox(min.0, min.1, max.0, max.1)
        })
        .unwrap_or_else(NodeSet::empty)
    }

    /// Geo-spatial search: the `k` nodes of `label` nearest `(lat, lon)` as
    /// `(node, distance_km)`, sorted by increasing distance (ties by node id).
    pub fn geo_knn(
        &self,
        label: &str,
        lat_key: &str,
        lon_key: &str,
        lat: f64,
        lon: f64,
        k: usize,
    ) -> Vec<(NodeId, f64)> {
        self.with_geo_index(label, lat_key, lon_key, |gi| gi.knn(lat, lon, k))
            .unwrap_or_default()
    }

    /// Run `query` against the lazily built, cached geo index for
    /// `(label, lat_key, lon_key)`. Returns `None` if any name is unknown.
    fn with_geo_index<R>(
        &self,
        label: &str,
        lat_key: &str,
        lon_key: &str,
        query: impl FnOnce(&GeoIndex) -> R,
    ) -> Option<R> {
        let key = (
            self.label_from_str(label)?,
            self.property_key_from_str(lat_key)?,
            self.property_key_from_str(lon_key)?,
        );

        // Fast path: index already built (short lock).
        {
            let index = self
                .geo_index
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(gi) = index.get(&key) {
                return Some(query(gi));
            }
        }

        // Build outside the lock, then cache (a racing build is identical).
        let gi = self.build_geo_index(key.0, key.1, key.2);
        let result = query(&gi);
        let mut index = self
            .geo_index
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        index.entry(key).or_insert(gi);
        Some(result)
    }

    /// Build the geo index for one `(label, lat_key, lon_key)` field by reading
    /// each labelled node's lat/lon f64 properties.
    fn build_geo_index(
        &self,
        label: Label,
        lat_key: PropertyKey,
        lon_key: PropertyKey,
    ) -> GeoIndex {
        let label_nodes = self.nodes_with_label_id(label);
        let (Some(lat_col), Some(lon_col)) =
            (self.columns.get(&lat_key), self.columns.get(&lon_key))
        else {
            return GeoIndex::default();
        };
        let points = lat_col.iter_entries().filter_map(|(node, lat_val)| {
            if !label_nodes.is_some_and(|ns| ns.contains(node)) {
                return None;
            }
            let lat = lat_val.to_f64()?;
            let lon = lon_col.get(node)?.to_f64()?;
            Some((node, lat, lon))
        });
        GeoIndex::build(points)
    }

    /// Get property value for a node
    ///
    /// # Arguments
    /// * `node_id` - The node ID
    /// * `key` - The property key name (e.g., "name")
    pub fn prop(&self, node_id: NodeId, key: &str) -> Option<Prop<'_>> {
        let key_id = self.property_key_from_str(key)?;
        Some(Prop {
            g: self,
            value: self.prop_id(node_id, key_id)?,
        })
    }

    /// Get property value for a node (internal ID-based version)
    fn prop_id(&self, node_id: NodeId, key: PropertyKey) -> Option<ValueId> {
        self.columns.get(&key)?.get(node_id)
    }

    /// Read a relationship property by CSR position — the relationship analogue of
    /// [`prop`](Self::prop), returning the same narrowable [`Prop`]. `rel_csr_pos` is
    /// the position in the outgoing CSR array (0..n_rels), as carried by
    /// [`RelationshipRef::pos`]. Narrow with [`Prop::i64`] / [`Prop::f64`] / … or
    /// [`PropExt`] on the `Option`, exactly like a node `prop`.
    pub fn rel_prop(&self, rel_csr_pos: u32, key: &str) -> Option<Prop<'_>> {
        let key_id = self.property_key_from_str(key)?;
        Some(Prop {
            g: self,
            value: self.relationship_property_id(rel_csr_pos, key_id)?,
        })
    }

    /// Get property value for a relationship (internal ID-based version)
    fn relationship_property_id(&self, rel_csr_pos: u32, key: PropertyKey) -> Option<ValueId> {
        self.rel_columns.get(&key)?.get(rel_csr_pos)
    }

    /// A resolved reader for the node property `key`, or `None` if no such column
    /// exists. Narrow it to a typed reader with [`Col::i64`] / [`Col::bool`] and
    /// hoist out of a hot loop instead of calling [`prop`](Self::prop) (which
    /// re-resolves the key and matches [`ValueId`]) per node — mirroring the polars
    /// `column(name).i64()` shape.
    pub fn col(&self, key: &str) -> Option<Col<'_>> {
        Some(Col {
            col: self.columns.get(&self.property_key_from_str(key)?)?,
        })
    }

    /// The raw dense [`Column`] for `key` (any value type), for in-crate kernels that
    /// read values generically by node id (`column.get(node) -> Option<ValueId>`) —
    /// e.g. the aggregate projected-property filter. `None` if the key is absent.
    pub(crate) fn column_ref(&self, key: &str) -> Option<&Column> {
        self.columns.get(&self.property_key_from_str(key)?)
    }

    /// A resolved reader for the relationship property `key`, indexed by CSR
    /// position — the relationship analogue of [`col`](Self::col). Narrow with
    /// [`Col::i64`] / [`Col::bool`]; the position is a [`RelationshipRef::pos`]
    /// (see [`rel_prop`](Self::rel_prop)).
    pub fn rel_col(&self, key: &str) -> Option<Col<'_>> {
        Some(Col {
            col: self.rel_columns.get(&self.property_key_from_str(key)?)?,
        })
    }

    /// Thin alias of [`prop`](Self::prop)`(..).and_then(|p| p.`[`str`](Prop::str)`())`,
    /// retained for brevity; prefer the `prop(..).and_then(|p| p.str())` chain. `None`
    /// when absent **or empty** (dense string columns store a missing value as `""`).
    pub fn prop_str(&self, node: NodeId, key: &str) -> Option<&str> {
        self.prop(node, key).and_then(|p| p.str())
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

    /// Resolve a relationship-type name to its interned [`RelationshipType`], or
    /// `None` if no rel of that type exists. Short alias for
    /// [`relationship_type_from_str`](Self::relationship_type_from_str); resolve
    /// once and pass the result to a traversal method in a hot loop to skip the
    /// per-call string lookup.
    #[inline]
    pub fn rel_type(&self, name: &str) -> Option<RelationshipType> {
        self.relationship_type_from_str(name)
    }
}

impl Default for GraphSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphSnapshot {
    /// Create a GraphSnapshot from Parquet files using GraphBuilder
    #[allow(clippy::too_many_arguments)]
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
        builder.add_relationship(0, 1, "KNOWS").unwrap();
        builder.add_relationship(1, 2, "WORKS_FOR").unwrap();
        builder.set_prop_str(0, "name", "Alice").unwrap();
        builder.set_prop_i64(0, "age", 30).unwrap();

        // Manually create snapshot to avoid into_vec() issue
        // Calculate n based on max node ID used (nodes are 0, 1, 2, so n should be 3)
        let max_node_id = builder
            .node_labels
            .len()
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
                label_index
                    .entry(*label)
                    .or_default()
                    .push(node_id as NodeId);
            }
        }
        use roaring::RoaringBitmap;
        let label_index: HashMap<Label, NodeSet> = label_index
            .into_iter()
            .map(|(label, mut nodes)| {
                nodes.sort_unstable();
                nodes.dedup();
                let bitmap = RoaringBitmap::from_sorted_iter(nodes).unwrap();
                (label, NodeSet::from(bitmap))
            })
            .collect();

        // Build type index
        let mut type_index: HashMap<RelationshipType, Vec<u32>> = HashMap::new();
        for (rel_idx, rel_type) in builder.rel_types.iter().enumerate() {
            type_index
                .entry(*rel_type)
                .or_default()
                .push(rel_idx as u32);
        }
        let type_index: HashMap<RelationshipType, NodeSet> = type_index
            .into_iter()
            .map(|(rel_type, mut rel_ids)| {
                rel_ids.sort_unstable();
                rel_ids.dedup();
                let bitmap = RoaringBitmap::from_sorted_iter(rel_ids).unwrap();
                (rel_type, NodeSet::from(bitmap))
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
            in_to_out: Vec::new(),
            label_index,
            type_index,
            version: builder.version.clone(),
            columns,
            rel_columns: HashMap::new(),
            prop_index: Mutex::new(HashMap::new()),
            fulltext_index: Mutex::new(HashMap::new()),
            geo_index: Mutex::new(HashMap::new()),
            chain_root_index: Mutex::new(HashMap::new()),
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
        let atoms = Atoms::new(vec![
            "".to_string(),
            "hello".to_string(),
            "world".to_string(),
        ]);
        assert_eq!(atoms.resolve(0), Some(""));
        assert_eq!(atoms.resolve(1), Some("hello"));
        assert_eq!(atoms.resolve(2), Some("world"));
        assert_eq!(atoms.resolve(99), None);
    }

    #[test]
    fn test_get_neighbors_outgoing() {
        let snapshot = create_test_snapshot();
        assert_eq!(
            snapshot
                .neighbors(0, Direction::Outgoing)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            snapshot
                .neighbors(1, Direction::Outgoing)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(snapshot.neighbors(2, Direction::Outgoing).count(), 0);
    }

    #[test]
    fn test_get_neighbors_incoming() {
        let snapshot = create_test_snapshot();
        assert_eq!(snapshot.neighbors(0, Direction::Incoming).count(), 0);
        assert_eq!(
            snapshot
                .neighbors(1, Direction::Incoming)
                .collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(
            snapshot
                .neighbors(2, Direction::Incoming)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn test_get_neighbors_outgoing_invalid() {
        let snapshot = create_test_snapshot();
        assert_eq!(snapshot.neighbors(999, Direction::Outgoing).count(), 0);
    }

    #[test]
    fn test_get_neighbors_incoming_invalid() {
        let snapshot = create_test_snapshot();
        assert_eq!(snapshot.neighbors(999, Direction::Incoming).count(), 0);
    }

    #[test]
    fn test_par_neighbor_fold() {
        let mut b = GraphBuilder::new(Some(20), Some(20));
        for i in 0..6 {
            b.add_node(Some(i), &["P"]).unwrap();
        }
        for t in 1..6 {
            b.add_relationship(0, t, "K").unwrap(); // 0 -> {1,2,3,4,5}
        }
        let g = b.finalize(None);
        let sum: u64 = g.par_neighbor_fold(
            0,
            Direction::Outgoing,
            "K",
            || 0u64,
            |a, n| a + n as u64,
            |a, b| a + b,
        );
        assert_eq!(sum, 1 + 2 + 3 + 4 + 5);
        // Matches the sequential fold over the same neighbors.
        let seq: u64 = g
            .neighbors_by_type(0, Direction::Outgoing, "K")
            .map(|n| n as u64)
            .sum();
        assert_eq!(sum, seq);
    }

    #[test]
    fn test_neighbor_counts() {
        let mut b = GraphBuilder::new(Some(20), Some(20));
        for i in 0..6 {
            b.add_node(Some(i), &["P"]).unwrap();
        }
        // sources {0,1,2} fan into shared targets with varying multiplicity.
        b.add_relationship(0, 3, "K").unwrap();
        b.add_relationship(1, 3, "K").unwrap();
        b.add_relationship(2, 3, "K").unwrap();
        b.add_relationship(0, 4, "K").unwrap();
        b.add_relationship(1, 4, "K").unwrap();
        b.add_relationship(2, 5, "K").unwrap();
        let g = b.finalize(None);

        let counts = g.neighbor_counts([0u32, 1, 2], Direction::Outgoing, "K");
        assert_eq!(counts.get(&3), Some(&3));
        assert_eq!(counts.get(&4), Some(&2));
        assert_eq!(counts.get(&5), Some(&1));
        assert_eq!(counts.len(), 3);

        // A second call must reuse the scratch independently (generation stamp),
        // not carry over the first call's counts.
        let again = g.neighbor_counts([2u32], Direction::Outgoing, "K");
        assert_eq!(again.get(&3), Some(&1));
        assert_eq!(again.get(&5), Some(&1));
        assert_eq!(again.len(), 2);

        // Unknown rel type matches nothing.
        assert!(g
            .neighbor_counts([0u32, 1, 2], Direction::Outgoing, "NOPE")
            .is_empty());
    }

    #[test]
    fn test_fold_via() {
        // Persons 0..3, Messages 3..7. hasCreator points creator -> message (so a
        // message's creator is its Incoming hasCreator neighbour, as the LDBC loader
        // stores it); replyOf points reply -> parent.
        let mut b = GraphBuilder::new(Some(20), Some(20));
        for i in 0..3 {
            b.add_node(Some(i), &["Person"]).unwrap();
        }
        for i in 3..7 {
            b.add_node(Some(i), &["Msg"]).unwrap();
        }
        for &(c, m) in &[(0u32, 3u32), (1, 4), (0, 5), (2, 6)] {
            b.add_relationship(c, m, "hasCreator").unwrap();
        }
        // Reply rels and the creator-pair each folds to:
        //   4->3 (1,0)  5->3 (0,0 self, skip)  6->4 (2,1)  6->3 (2,0)  5->4 (0,1)
        for &(reply, parent) in &[(4u32, 3u32), (5, 3), (6, 4), (6, 3), (5, 4)] {
            b.add_relationship(reply, parent, "replyOf").unwrap();
        }
        let g = b.finalize(None);

        // Projection = each node's creator (the neighbor_via("hasCreator", In) array).
        let proj: Vec<u32> = (0..g.n_nodes)
            .map(|n| {
                g.first_neighbor(n, Direction::Incoming, "hasCreator")
                    .unwrap_or(u32::MAX)
            })
            .collect();

        let m = g.fold_via("replyOf", Direction::Outgoing, &proj);
        assert_eq!(m.get(&(0, 1)), Some(&2)); // 4->3 and 5->4
        assert_eq!(m.get(&(1, 2)), Some(&1)); // 6->4
        assert_eq!(m.get(&(0, 2)), Some(&1)); // 6->3
        assert_eq!(m.len(), 3); // 5->3 self-pair excluded

        // Unknown rel folds to nothing.
        assert!(g.fold_via("NOPE", Direction::Outgoing, &proj).is_empty());
    }

    #[test]
    fn test_weighted_shortest_path() {
        let mut b = GraphBuilder::new(Some(20), Some(20));
        for i in 0..6 {
            b.add_node(Some(i), &["P"]).unwrap();
        }
        for &(u, v) in &[(0, 1), (1, 2), (2, 3), (3, 4), (0, 2), (2, 4)] {
            b.add_relationship(u, v, "K").unwrap();
        }
        let g = b.finalize(None);
        // Integer-valued weights so sums are exact in f64 regardless of association.
        let w = |from: u32, rel: &RelationshipRef| {
            (from as i64 - rel.neighbor as i64).unsigned_abs() as f64
        };
        // Bidirectional must agree with the single-source Dijkstra it mirrors.
        let bidi = g.weighted_shortest_path(0, 4, Direction::Both, "K", w);
        let uni = g
            .dijkstra(0, Direction::Both, "K", Some(4), w)
            .distance(4)
            .filter(|d| d.is_finite());
        assert_eq!(bidi, uni);
        assert!(bidi.is_some());
        // Trivial and unreachable cases.
        assert_eq!(
            g.weighted_shortest_path(0, 0, Direction::Both, "K", w),
            Some(0.0)
        );
        assert_eq!(
            g.weighted_shortest_path(0, 5, Direction::Both, "K", w),
            None
        );
    }

    #[test]
    fn test_column_typed_slices() {
        let i = Column::DenseI64(vec![10, 20, 30]);
        assert_eq!(i.as_i64_slice(), Some(&[10i64, 20, 30][..]));
        assert_eq!(i.as_f64_slice(), None);
        assert_eq!(i.as_str_ids(), None);
        assert_eq!(i.as_bool_slice(), None);

        let s = Column::DenseStr(vec![1, 2, 3]);
        assert_eq!(s.as_str_ids(), Some(&[1u32, 2, 3][..]));
        assert_eq!(s.as_i64_slice(), None);

        let f = Column::DenseF64(vec![1.5, 2.5]);
        assert_eq!(f.as_f64_slice(), Some(&[1.5f64, 2.5][..]));

        let mut bv = bitvec::vec::BitVec::new();
        bv.push(true);
        bv.push(false);
        bv.push(true);
        let b = Column::DenseBool(bv);
        let bs = b.as_bool_slice().unwrap();
        assert!(bs[0] && !bs[1] && bs[2]);
        assert_eq!(b.as_i64_slice(), None);

        // Sparse columns expose no dense slice.
        assert_eq!(Column::SparseI64(vec![(0, 5), (2, 7)]).as_i64_slice(), None);
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
        assert!(snapshot.can_reach(
            0,
            2,
            Direction::Outgoing,
            Some(&["KNOWS", "WORKS_FOR"]),
            None
        ));

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
        // From 2, can reach 1 via incoming rel
        assert!(snapshot.can_reach(2, 1, Direction::Both, None, None));

        // From 2, can reach 0 via incoming rels (2 <- 1 <- 0)
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
        assert_eq!(prop.map(|p| p.value()), Some(alice_id));
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
        let person_idx = snapshot
            .atoms
            .strings
            .iter()
            .position(|s| s == "Person")
            .unwrap();
        let alice_idx = snapshot
            .atoms
            .strings
            .iter()
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
    fn test_column_rank_i64_get_basic() {
        // First position, gap, and last position over a small space.
        let col = Column::build_rank_i64(&[(0, 10), (2, 30), (4, 50)], 5);
        assert_eq!(col.get(0), Some(ValueId::I64(10)));
        assert_eq!(col.get(1), None);
        assert_eq!(col.get(2), Some(ValueId::I64(30)));
        assert_eq!(col.get(3), None);
        assert_eq!(col.get(4), Some(ValueId::I64(50)));
        // Out of range.
        assert_eq!(col.get(5), None);
        assert_eq!(col.get(99), None);
    }

    #[test]
    fn test_column_rank_i64_matches_sparse_across_blocks() {
        // Spread entries across several RANK_BLOCK (512) blocks, including the
        // exact block boundaries, and assert byte-for-byte parity with a sparse
        // column over the whole space.
        let len = RANK_BLOCK * 4 + 7;
        let positions = [
            0,
            1,
            RANK_BLOCK - 1,
            RANK_BLOCK,
            RANK_BLOCK + 1,
            RANK_BLOCK * 2,
            RANK_BLOCK * 3 + 5,
            len - 1,
        ];
        let pairs: Vec<(u32, i64)> = positions
            .iter()
            .map(|&p| (p as u32, (p as i64) * 7 + 1))
            .collect();
        let rank = Column::build_rank_i64(&pairs, len);
        let sparse = Column::SparseI64(pairs.clone());
        for pos in 0..(len as u32 + 3) {
            assert_eq!(rank.get(pos), sparse.get(pos), "mismatch at pos {pos}");
        }
        // iter_entries parity (ascending position order).
        let from_rank: Vec<_> = rank.iter_entries().collect();
        let expect: Vec<_> = pairs.iter().map(|&(p, v)| (p, ValueId::I64(v))).collect();
        assert_eq!(from_rank, expect);
    }

    #[test]
    fn test_column_rank_f64_bool_str_match_sparse() {
        // Same cross-block positions, exercised for the f64/bool/str variants.
        let len = RANK_BLOCK * 3 + 9;
        let positions: Vec<u32> = [
            0u32,
            1,
            RANK_BLOCK as u32,
            RANK_BLOCK as u32 + 2,
            len as u32 - 1,
        ]
        .to_vec();

        let f64_pairs: Vec<(u32, f64)> = positions.iter().map(|&p| (p, p as f64 * 0.25)).collect();
        let rank_f = Column::build_rank_f64(&f64_pairs, len);
        let sparse_f = Column::SparseF64(f64_pairs);
        for pos in 0..(len as u32 + 2) {
            assert_eq!(rank_f.get(pos), sparse_f.get(pos), "f64 mismatch at {pos}");
        }

        let bool_pairs: Vec<(u32, bool)> = positions.iter().map(|&p| (p, p % 2 == 0)).collect();
        let rank_b = Column::build_rank_bool(&bool_pairs, len);
        let sparse_b = Column::SparseBool(bool_pairs);
        for pos in 0..(len as u32 + 2) {
            assert_eq!(rank_b.get(pos), sparse_b.get(pos), "bool mismatch at {pos}");
        }

        let str_pairs: Vec<(u32, u32)> = positions.iter().map(|&p| (p, p * 3 + 100)).collect();
        let rank_s = Column::build_rank_str(&str_pairs, len);
        let sparse_s = Column::SparseStr(str_pairs);
        for pos in 0..(len as u32 + 2) {
            assert_eq!(rank_s.get(pos), sparse_s.get(pos), "str mismatch at {pos}");
        }
    }

    #[test]
    fn test_valueid_from_f64() {
        let val = ValueId::from_f64(2.5);
        assert_eq!(val.to_f64(), Some(2.5));
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
        assert!(result.is_none() || result.unwrap().is_empty());
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
    fn test_column_readers_and_neighbor_sugar() {
        // person0 -knows-> person1; person0 -isLocatedIn-> city2 -isPartOf-> country3.
        let mut b = GraphBuilder::new(Some(4), Some(4));
        b.add_node(Some(0), &["Person"]).unwrap();
        b.add_node(Some(1), &["Person"]).unwrap();
        b.add_node(Some(2), &["City"]).unwrap();
        b.add_node(Some(3), &["Country"]).unwrap();
        b.set_prop_i64(0, "plid", 100).unwrap();
        b.set_prop_bool(0, "active", true).unwrap();
        b.set_prop_str(0, "name", "Alice").unwrap();
        b.set_prop_str(1, "name", "").unwrap(); // empty string -> prop_str treats as absent
        b.add_relationship(0, 1, "knows").unwrap();
        b.add_relationship(0, 2, "isLocatedIn").unwrap();
        b.add_relationship(2, 3, "isPartOf").unwrap();
        let g = b.finalize(None);

        // first_neighbor: the neighbors_by_type(..).next() idiom, both directions.
        assert_eq!(g.first_neighbor(0, Direction::Outgoing, "knows"), Some(1));
        assert_eq!(
            g.first_neighbor(0, Direction::Outgoing, "isLocatedIn"),
            Some(2)
        );
        assert_eq!(g.first_neighbor(1, Direction::Incoming, "knows"), Some(0));
        assert_eq!(
            g.first_neighbor(0, Direction::Outgoing, "nonexistent"),
            None
        );

        // follow: chained one-of-each-step walk person -> city -> country.
        assert_eq!(
            g.follow(
                0,
                &[
                    (Direction::Outgoing, "isLocatedIn"),
                    (Direction::Outgoing, "isPartOf")
                ]
            ),
            Some(3)
        );
        assert_eq!(g.follow(0, &[]), Some(0)); // no steps -> start
        assert_eq!(g.follow(1, &[(Direction::Outgoing, "isLocatedIn")]), None); // broken chain

        // Typed column readers: resolve the key once, narrow to a type, read by node.
        assert_eq!(g.col("plid").unwrap().i64().get(0), Some(100));
        assert!(g.col("nonexistent").is_none());
        assert_eq!(g.col("active").unwrap().bool().get(0), Some(true));

        // prop_str: present value, empty-as-absent, and missing key all distinguished.
        assert_eq!(g.prop_str(0, "name"), Some("Alice"));
        assert_eq!(g.prop_str(1, "name"), None); // empty dense slot reads back as absent
        assert_eq!(g.prop_str(0, "missing"), None);
    }

    #[test]
    fn test_typed_f64_str_dtype_readers() {
        let mut b = GraphBuilder::new(Some(3), Some(0));
        for i in 0..3u32 {
            b.add_node(Some(i), &["N"]).unwrap();
            b.set_prop_i64(i, "n", i as i64).unwrap();
            b.set_prop_f64(i, "score", i as f64 + 0.5).unwrap();
            b.set_prop_bool(i, "flag", i % 2 == 0).unwrap();
            b.set_prop_str(i, "name", &format!("p{i}")).unwrap();
        }
        let g = b.finalize(None);

        // f64 reader: per-value get + dense slice fast path.
        let f = g.col("score").unwrap().f64();
        assert_eq!(f.get(0), Some(0.5));
        assert_eq!(f.get(2), Some(2.5));
        assert_eq!(f.as_slice(), Some(&[0.5, 1.5, 2.5][..]));

        // str reader: interned ids + dense id slice; ids resolve to the strings.
        let s = g.col("name").unwrap().str();
        let ids = s.as_ids().expect("dense str column");
        assert_eq!(ids.len(), 3);
        assert_eq!(s.id(1), Some(ids[1]));
        let names: Vec<&str> = ids
            .iter()
            .map(|&id| g.resolve_string(id).unwrap())
            .collect();
        assert_eq!(names, vec!["p0", "p1", "p2"]);

        // dtype reports the logical type without narrowing.
        assert_eq!(g.col("n").unwrap().dtype(), ColumnDtype::I64);
        assert_eq!(g.col("score").unwrap().dtype(), ColumnDtype::F64);
        assert_eq!(g.col("flag").unwrap().dtype(), ColumnDtype::Bool);
        assert_eq!(g.col("name").unwrap().dtype(), ColumnDtype::Str);
    }

    #[test]
    fn test_node_by_label_property_is_label_scoped() {
        // The same `name` on two different labels: a Tag and a Country both "X".
        let mut b = GraphBuilder::new(Some(2), Some(0));
        b.add_node(Some(0), &["Tag"]).unwrap();
        b.add_node(Some(1), &["Country"]).unwrap();
        b.set_prop_str(0, "name", "X").unwrap();
        b.set_prop_str(1, "name", "X").unwrap();
        let g = b.finalize(None);

        // Label-scoped: resolves to the node of the requested label, not the global first.
        assert_eq!(g.node_with_label_property("Country", "name", "X"), Some(1));
        assert_eq!(g.node_with_label_property("Tag", "name", "X"), Some(0));
        // Unknown label or value -> None.
        assert_eq!(g.node_with_label_property("Person", "name", "X"), None);
        assert_eq!(g.node_with_label_property("Country", "name", "Y"), None);
    }

    #[test]
    fn test_has_rel_and_neighbor_property() {
        // work0 -about-> entity1 (uri "u:x"); work0 -category-> cat2 (uri "u:cat").
        let mut b = GraphBuilder::new(Some(3), Some(2));
        b.add_node(Some(0), &["Work"]).unwrap();
        b.add_node(Some(1), &["Entity"]).unwrap();
        b.add_node(Some(2), &["Category"]).unwrap();
        b.set_prop_str(1, "uri", "u:x").unwrap();
        b.set_prop_str(2, "uri", "u:cat").unwrap();
        b.add_relationship(0, 1, "about").unwrap();
        b.add_relationship(0, 2, "category").unwrap();
        let g = b.finalize(None);

        // has_rel: existence of an outgoing rel of a given type.
        assert!(g.has_rel(0, Direction::Outgoing, "about"));
        assert!(!g.has_rel(0, Direction::Outgoing, "mentions"));
        assert!(!g.has_rel(1, Direction::Outgoing, "about"));

        // has_neighbor_with_property: a neighbour reached by `rel` has `key` == value.
        assert!(g.has_neighbor_with_property(0, Direction::Outgoing, "about", "uri", "u:x"));
        assert!(g.has_neighbor_with_property(0, Direction::Outgoing, "category", "uri", "u:cat"));
        // The cat is reached by `category`, not `about`, so this is false.
        assert!(!g.has_neighbor_with_property(0, Direction::Outgoing, "about", "uri", "u:cat"));
        // A value absent from the graph -> false (no node interns it).
        assert!(!g.has_neighbor_with_property(0, Direction::Outgoing, "about", "uri", "u:none"));
    }

    #[test]
    fn test_neighborhood_bounded_hops() {
        // path 0->1->2->3 plus a 1-hop branch 0->4 (all `knows`).
        let mut b = GraphBuilder::new(Some(5), Some(4));
        for i in 0..5 {
            b.add_node(Some(i), &["Person"]).unwrap();
        }
        b.add_relationship(0, 1, "knows").unwrap();
        b.add_relationship(1, 2, "knows").unwrap();
        b.add_relationship(2, 3, "knows").unwrap();
        b.add_relationship(0, 4, "knows").unwrap();
        let g = b.finalize(None);

        let sorted = |ns: NodeSet| {
            let mut v: Vec<u32> = ns.iter().collect();
            v.sort_unstable();
            v
        };
        let out = Direction::Outgoing;
        assert_eq!(sorted(g.neighborhood(0, out, "knows", 1..=1)), vec![1, 4]); // exactly 1 hop
        assert_eq!(sorted(g.neighborhood(0, out, "knows", 2..=2)), vec![2]); // exactly 2 hops
        assert_eq!(
            sorted(g.neighborhood(0, out, "knows", 1..=2)),
            vec![1, 2, 4]
        ); // 1 or 2 hops
        assert_eq!(
            sorted(g.neighborhood(0, out, "knows", 0..=2)),
            vec![0, 1, 2, 4]
        ); // includes seed
           // `2..=1` is intentionally a reversed/empty hop range — it must yield nothing.
        #[allow(clippy::reversed_empty_ranges)]
        let empty = g.neighborhood(0, out, "knows", 2..=1);
        assert!(empty.iter().next().is_none());
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
        let max_node_id = builder
            .node_labels
            .len()
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
            in_to_out: Vec::new(),
            label_index: HashMap::new(),
            type_index: HashMap::new(),
            version: builder.version.clone(),
            columns: HashMap::new(),
            rel_columns: HashMap::new(),
            prop_index: Mutex::new(HashMap::new()),
            fulltext_index: Mutex::new(HashMap::new()),
            geo_index: Mutex::new(HashMap::new()),
            chain_root_index: Mutex::new(HashMap::new()),
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
        builder.add_relationship(0, 1, "KNOWS").unwrap();
        builder.add_relationship(1, 2, "KNOWS").unwrap();
        builder.add_relationship(2, 3, "KNOWS").unwrap();
        builder.add_relationship(0, 4, "KNOWS").unwrap();
        builder.add_relationship(4, 3, "KNOWS").unwrap();
        builder.finalize(None)
    }

    #[test]
    fn test_bidirectional_bfs_basic() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;

        // Test: find paths from node 0 to node 3
        let source = NodeSet::from(RoaringBitmap::from_iter([0]));
        let target = NodeSet::from(RoaringBitmap::from_iter([3]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source,
            &target,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
        );

        // Should find nodes 0, 1, 2, 3, 4 (all on paths from 0 to 3)
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(nodes.contains(4));
        assert!(!nodes.contains(5)); // Node 5 doesn't exist

        // Should find relationships on paths
        assert!(!rels.is_empty());
    }

    #[test]
    fn test_bidirectional_bfs_no_path() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;

        // Test: no path from node 3 to node 0 (reverse direction)
        let source = NodeSet::from(RoaringBitmap::from_iter([3]));
        let target = NodeSet::from(RoaringBitmap::from_iter([0]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source,
            &target,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
        );

        // Should find intersection (both sets contain their starting nodes)
        // But no actual path, so intersection should be empty or just the endpoints
        // Actually, if source and target don't overlap initially, and no path exists,
        // the intersection should be empty
        assert!(nodes.is_empty() || nodes.is_empty());
    }

    #[test]
    fn test_bidirectional_bfs_with_rel_type_filter() {
        let snapshot = create_bidirectional_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;

        // Test: filter by relationship type
        let source = NodeSet::from(RoaringBitmap::from_iter([0]));
        let target = NodeSet::from(RoaringBitmap::from_iter([3]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source,
            &target,
            Direction::Outgoing,
            Some(&["KNOWS"]),
            None,
            None,
            None,
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
        let source = NodeSet::from(RoaringBitmap::from_iter([0]));
        let target = NodeSet::from(RoaringBitmap::from_iter([3]));

        let node_filter = |node_id: NodeId, snapshot: &GraphSnapshot| -> bool {
            snapshot
                .nodes_with_label("Person")
                .is_some_and(|nodes| nodes.contains(node_id))
        };

        // Type annotations needed for None filters
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bidirectional_bfs(
            &source,
            &target,
            Direction::Outgoing,
            None,
            Some(&node_filter),
            None::<RelFilter>,
            None,
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
        let source = NodeSet::from(RoaringBitmap::from_iter([0]));
        let target = NodeSet::from(RoaringBitmap::from_iter([3]));

        // With depth 1, should not reach node 3
        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source,
            &target,
            Direction::Outgoing,
            None,
            None,
            None,
            Some(1),
        );

        // Should not find node 3 with depth 1
        assert!(!nodes.contains(3));

        // With depth 3, should reach node 3
        let (nodes, _rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source,
            &target,
            Direction::Outgoing,
            None,
            None,
            None,
            Some(3),
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
        let source = NodeSet::from(RoaringBitmap::from_iter([0, 1]));
        let target = NodeSet::from(RoaringBitmap::from_iter([2, 3]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
            &source,
            &target,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
        );

        // Should find intersection nodes
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(!rels.is_empty());
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
        builder.add_relationship(0, 1, "KNOWS").unwrap();
        builder.add_relationship(1, 2, "KNOWS").unwrap();
        builder.add_relationship(2, 3, "KNOWS").unwrap();
        builder.add_relationship(0, 4, "KNOWS").unwrap();
        builder.add_relationship(4, 3, "KNOWS").unwrap();
        builder.add_relationship(0, 5, "WORKS_FOR").unwrap();
        builder.finalize(None)
    }

    #[test]
    fn test_bfs_basic() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;

        // Test: BFS from node 0, outgoing direction
        let start = NodeSet::from(RoaringBitmap::from_iter([0]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
        );

        // Should find nodes 0, 1, 2, 3, 4, 5 (all reachable from 0)
        assert!(nodes.contains(0));
        assert!(nodes.contains(1));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3));
        assert!(nodes.contains(4));
        assert!(nodes.contains(5));
        assert!(!rels.is_empty());
    }

    #[test]
    fn test_bfs_with_rel_type_filter() {
        let snapshot = create_bfs_test_snapshot();
        use crate::bitmap::NodeSet;
        use roaring::RoaringBitmap;

        // Test: filter by relationship type "KNOWS" only
        let start = NodeSet::from(RoaringBitmap::from_iter([0]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start,
            Direction::Outgoing,
            Some(&["KNOWS"]),
            None,
            None,
            None,
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
        let start = NodeSet::from(RoaringBitmap::from_iter([0]));

        let node_filter = |node_id: NodeId, snapshot: &GraphSnapshot| -> bool {
            snapshot
                .nodes_with_label("Person")
                .is_some_and(|nodes| nodes.contains(node_id))
        };

        // Type annotations needed for None filters
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs(
            &start,
            Direction::Outgoing,
            None,
            Some(&node_filter),
            None::<RelFilter>,
            None,
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
        let start = NodeSet::from(RoaringBitmap::from_iter([0]));

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
            Some(1),
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
            Some(2),
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
        let start = NodeSet::from(RoaringBitmap::from_iter([3]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start,
            Direction::Incoming,
            None,
            None,
            None,
            None,
        );

        // Should find nodes reachable by following incoming rels: 3, 2, 4, 1, 0
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
        let start = NodeSet::from(RoaringBitmap::from_iter([2]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) =
            snapshot.bfs::<NodeFilter, RelFilter>(&start, Direction::Both, None, None, None, None);

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
        let start = NodeSet::from(RoaringBitmap::from_iter([0, 2]));

        // Type annotations needed for None filters
        type NodeFilter = fn(NodeId, &GraphSnapshot) -> bool;
        type RelFilter = fn(NodeId, NodeId, RelationshipType, u32, &GraphSnapshot) -> bool;
        let (nodes, _rels) = snapshot.bfs::<NodeFilter, RelFilter>(
            &start,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
        );

        // Should find nodes reachable from either start node
        assert!(nodes.contains(0));
        assert!(nodes.contains(2));
        assert!(nodes.contains(3)); // Reachable from both
        assert!(nodes.contains(1)); // Reachable from 0
    }

    #[test]
    fn test_chain_root_walks_reply_forest_to_root() {
        // replyOf forest (child -replyOf-> parent): 0 is a root message,
        //   1 -> 0, 2 -> 1, 3 -> 1, 4 -> 2; 5 is a separate childless root.
        // (Contiguous ids so external == internal node id.)
        let mut builder = GraphBuilder::new(Some(10), Some(10));
        for id in [0, 1, 2, 3, 4, 5] {
            builder.add_node(Some(id), &["Message"]).unwrap();
        }
        builder.add_relationship(1, 0, "replyOf").unwrap();
        builder.add_relationship(2, 1, "replyOf").unwrap();
        builder.add_relationship(3, 1, "replyOf").unwrap();
        builder.add_relationship(4, 2, "replyOf").unwrap();
        let snapshot = builder.finalize(None);
        let reply_of = snapshot.rel_type("replyOf").unwrap();

        // Every node in the tree resolves to root 0.
        for n in [0, 1, 2, 3, 4] {
            assert_eq!(snapshot.chain_root(n, Direction::Outgoing, reply_of), 0);
        }
        // A node with no parent rel is its own root.
        assert_eq!(snapshot.chain_root(5, Direction::Outgoing, reply_of), 5);
        // The cached second call returns the same terminal.
        assert_eq!(snapshot.chain_root(4, Direction::Outgoing, reply_of), 0);

        // chain_roots exposes the whole per-node array (built once, cached).
        let roots = snapshot.chain_roots(Direction::Outgoing, reply_of);
        assert_eq!(roots[2], 0);
        assert_eq!(roots[5], 5);
        // The (direction, rel_type) index entry is now present.
        let index = snapshot
            .chain_root_index
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(index.contains_key(&(Direction::Outgoing, reply_of)));
    }

    #[test]
    fn test_chain_root_terminates_on_cycle() {
        // Malformed, non-functional data: a 3-cycle. chain_root must terminate
        // via the depth cap and return a node deterministically rather than hang.
        let mut builder = GraphBuilder::new(Some(4), Some(4));
        for id in [0, 1, 2] {
            builder.add_node(Some(id), &["N"]).unwrap();
        }
        builder.add_relationship(0, 1, "loops").unwrap();
        builder.add_relationship(1, 2, "loops").unwrap();
        builder.add_relationship(2, 0, "loops").unwrap();
        let snapshot = builder.finalize(None);
        let loops = snapshot.rel_type("loops").unwrap();
        let root = snapshot.chain_root(0, Direction::Outgoing, loops);
        assert!([0, 1, 2].contains(&root));
    }

    #[test]
    fn test_neighbor_groups_sizes_and_top_by_size() {
        // Two countries (0,1); cities 2->0, 3->1; persons 4,5 in country 0, 6 in
        // country 1. Forums group their members by country; the largest cohort wins.
        let mut b = GraphBuilder::new(Some(16), Some(16));
        for id in 0..10 {
            b.add_node(Some(id), &["N"]).unwrap();
        }
        b.add_relationship(2, 0, "isPartOf").unwrap();
        b.add_relationship(3, 1, "isPartOf").unwrap();
        b.add_relationship(4, 2, "isLocatedIn").unwrap();
        b.add_relationship(5, 2, "isLocatedIn").unwrap();
        b.add_relationship(6, 3, "isLocatedIn").unwrap();
        // forum 7: members 4,5 (country 0), 6 (country 1) -> largest cohort 2.
        b.add_relationship(7, 4, "hasMember").unwrap();
        b.add_relationship(7, 5, "hasMember").unwrap();
        b.add_relationship(7, 6, "hasMember").unwrap();
        b.add_relationship(8, 6, "hasMember").unwrap(); // forum 8: largest cohort 1
        b.add_relationship(9, 4, "hasMember").unwrap(); // forum 9: largest cohort 1
        b.set_prop_i64(7, "fid", 100).unwrap();
        b.set_prop_i64(8, "fid", 20).unwrap();
        b.set_prop_i64(9, "fid", 10).unwrap();
        let g = b.finalize(None);

        let forums = [7u32, 8, 9];
        let project = [
            (Direction::Outgoing, "isLocatedIn"),
            (Direction::Outgoing, "isPartOf"),
        ];
        let sizes = g
            .neighbor_groups(&forums, "hasMember", Direction::Outgoing)
            .project(&project)
            .sizes();
        assert_eq!(sizes, vec![(7, 2), (8, 1), (9, 1)]);

        // No tie key: ties (forums 8,9 both size 1) break by source id ascending.
        let by_id = g
            .neighbor_groups(&forums, "hasMember", Direction::Outgoing)
            .project(&project)
            .top_by_size(3, None);
        assert_eq!(by_id, vec![(7, 2), (8, 1), (9, 1)]);

        // Tie by the "fid" property: among size-1 forums, fid 10 (forum 9) precedes
        // fid 20 (forum 8).
        let by_fid = g
            .neighbor_groups(&forums, "hasMember", Direction::Outgoing)
            .project(&project)
            .top_by_size(2, Some("fid"));
        assert_eq!(by_fid, vec![(7, 2), (9, 1)]);
    }

    #[test]
    fn test_bfs_distances_hop_counts() {
        // 0 ->1 ->2 ->3 and 0 ->4 ->3 (KNOWS); 0 ->5 (WORKS_FOR).
        let snapshot = create_bfs_test_snapshot();
        let dist = snapshot.bfs_distances(0, Direction::Outgoing, "KNOWS", None);
        assert_eq!(dist.get(&0), Some(&0));
        assert_eq!(dist.get(&1), Some(&1));
        assert_eq!(dist.get(&4), Some(&1));
        assert_eq!(dist.get(&2), Some(&2));
        // 3 is reached at depth 2 via 0->4->3, not depth 3 via 0->1->2->3.
        assert_eq!(dist.get(&3), Some(&2));
        // 5 sits behind a WORKS_FOR rel, so a KNOWS BFS never reaches it.
        assert_eq!(dist.get(&5), None);
    }

    #[test]
    fn test_bfs_distances_max_depth_and_eccentricity() {
        let snapshot = create_bfs_test_snapshot();
        // Bounded to one hop: the start plus its direct KNOWS neighbors only.
        let near = snapshot.bfs_distances(0, Direction::Outgoing, "KNOWS", Some(1));
        assert_eq!(near.len(), 3); // 0, 1, 4
        assert_eq!(near.get(&1), Some(&1));
        assert_eq!(near.get(&4), Some(&1));
        assert!(!near.contains_key(&2));
        // Eccentricity of 0 over KNOWS is the maximum hop distance, 2.
        let all = snapshot.bfs_distances(0, Direction::Outgoing, "KNOWS", None);
        assert_eq!(all.values().copied().max(), Some(2));
    }
}

/// How a relationship-type filter matches an rel's [`RelationshipType`].
///
/// Opaque, produced from a [`RelTypeFilter`] argument. The common single-type
/// filter is stored as one value (no allocation, single-compare matching); two or
/// more types spill to a `Vec`. Build from already-resolved types with
/// `collect()`, or via [`RelMatch::all`] / [`RelMatch::one`].
#[derive(Clone, Debug)]
pub struct RelMatch(RelMatchRepr);

#[derive(Clone, Debug)]
enum RelMatchRepr {
    /// Match every relationship type (an empty filter).
    All,
    /// A single resolved type — the common case, allocation-free.
    One(RelationshipType),
    /// Two or more resolved types (or zero = match nothing).
    Heap(Vec<RelationshipType>),
}

impl RelMatch {
    /// A filter that matches every relationship type.
    #[inline]
    pub fn all() -> RelMatch {
        RelMatch(RelMatchRepr::All)
    }

    /// A filter matching a single resolved type (no allocation).
    #[inline]
    pub fn one(rt: RelationshipType) -> RelMatch {
        RelMatch(RelMatchRepr::One(rt))
    }

    #[inline]
    fn matches(&self, rt: RelationshipType) -> bool {
        match &self.0 {
            RelMatchRepr::All => true,
            RelMatchRepr::One(t) => *t == rt,
            RelMatchRepr::Heap(v) => v.contains(&rt),
        }
    }
}

/// Build a filter from already-resolved types: a single type stays inline (no
/// allocation), two or more spill to a `Vec`. An empty iterator matches nothing —
/// use [`RelMatch::all`] for "match every type".
impl FromIterator<RelationshipType> for RelMatch {
    fn from_iter<I: IntoIterator<Item = RelationshipType>>(iter: I) -> Self {
        let mut it = iter.into_iter();
        match it.next() {
            None => RelMatch(RelMatchRepr::Heap(Vec::new())),
            Some(first) => match it.next() {
                None => RelMatch(RelMatchRepr::One(first)),
                Some(second) => {
                    let mut v = Vec::with_capacity(2 + it.size_hint().0);
                    v.push(first);
                    v.push(second);
                    v.extend(it);
                    RelMatch(RelMatchRepr::Heap(v))
                }
            },
        }
    }
}

/// A relationship-type filter accepted by the traversal methods
/// ([`neighbors_by_type`](GraphSnapshot::neighbors_by_type),
/// [`relationships`](GraphSnapshot::relationships),
/// [`dijkstra`](GraphSnapshot::dijkstra)).
///
/// Implemented for `&str`, `&[&str]` (and arrays), a pre-resolved
/// [`RelationshipType`], and `&[RelationshipType]`. Pass a string for convenience
/// (resolved on the spot) or a pre-resolved [`RelationshipType`] (via
/// [`rel_type`](GraphSnapshot::rel_type)) in a hot loop to skip the per-call
/// string lookup. An empty slice matches all types.
pub trait RelTypeFilter {
    /// Resolve this filter against the snapshot's interned relationship types.
    fn into_match(self, graph: &GraphSnapshot) -> RelMatch;
}

impl RelTypeFilter for RelationshipType {
    #[inline]
    fn into_match(self, _graph: &GraphSnapshot) -> RelMatch {
        RelMatch::one(self)
    }
}

impl RelTypeFilter for &str {
    #[inline]
    fn into_match(self, graph: &GraphSnapshot) -> RelMatch {
        // An unresolvable name has no rels, so it matches nothing.
        graph.relationship_type_from_str(self).into_iter().collect()
    }
}

impl RelTypeFilter for &[&str] {
    fn into_match(self, graph: &GraphSnapshot) -> RelMatch {
        if self.is_empty() {
            RelMatch::all()
        } else {
            self.iter()
                .filter_map(|s| graph.relationship_type_from_str(s))
                .collect()
        }
    }
}

impl<const N: usize> RelTypeFilter for &[&str; N] {
    #[inline]
    fn into_match(self, graph: &GraphSnapshot) -> RelMatch {
        self.as_slice().into_match(graph)
    }
}

impl RelTypeFilter for &[RelationshipType] {
    fn into_match(self, _graph: &GraphSnapshot) -> RelMatch {
        if self.is_empty() {
            RelMatch::all()
        } else {
            self.iter().copied().collect()
        }
    }
}

impl<const N: usize> RelTypeFilter for &[RelationshipType; N] {
    #[inline]
    fn into_match(self, graph: &GraphSnapshot) -> RelMatch {
        self.as_slice().into_match(graph)
    }
}

impl<T: RelTypeFilter> RelTypeFilter for Option<T> {
    /// `None` matches any relationship type; `Some(t)` defers to `t`. Lets a
    /// caller thread an optional type constraint through the whole neighbour /
    /// counting family without a separate "all types" entry point.
    #[inline]
    fn into_match(self, graph: &GraphSnapshot) -> RelMatch {
        match self {
            Some(t) => t.into_match(graph),
            None => RelMatch::all(),
        }
    }
}

/// CSR rel-index range `[start, end)` for a node in a direction's offset array.
#[inline]
fn rel_range(offsets: &[u32], node: usize) -> core::ops::Range<usize> {
    if node + 1 >= offsets.len() {
        return 0..0;
    }
    offsets[node] as usize..offsets[node + 1] as usize
}

/// Lazy, allocation-free iterator over a node's type-filtered neighbors, yielding
/// outgoing matches then incoming ones. Returned by
/// [`neighbors`](GraphSnapshot::neighbors) and
/// [`neighbors_by_type`](GraphSnapshot::neighbors_by_type); `.collect()` it for a
/// `Vec`.
pub struct NeighborsByType<'a> {
    graph: &'a GraphSnapshot,
    out: core::ops::Range<usize>,
    inc: core::ops::Range<usize>,
    matcher: RelMatch,
}

impl Iterator for NeighborsByType<'_> {
    type Item = NodeId;

    #[inline]
    fn next(&mut self) -> Option<NodeId> {
        for k in &mut self.out {
            if self.matcher.matches(self.graph.out_types[k]) {
                return Some(self.graph.out_nbrs[k]);
            }
        }
        for k in &mut self.inc {
            if self.matcher.matches(self.graph.in_types[k]) {
                return Some(self.graph.in_nbrs[k]);
            }
        }
        None
    }
}

/// Lazy, allocation-free iterator over a node's type-filtered relationships, each
/// carrying [`RelationshipRef::pos`] for property access, outgoing then incoming.
/// Returned by [`relationships`](GraphSnapshot::relationships).
pub struct RelationshipsByType<'a> {
    graph: &'a GraphSnapshot,
    out: core::ops::Range<usize>,
    inc: core::ops::Range<usize>,
    matcher: RelMatch,
}

impl Iterator for RelationshipsByType<'_> {
    type Item = RelationshipRef;

    #[inline]
    fn next(&mut self) -> Option<RelationshipRef> {
        for pos in &mut self.out {
            let rel_type = self.graph.out_types[pos];
            if self.matcher.matches(rel_type) {
                return Some(RelationshipRef {
                    neighbor: self.graph.out_nbrs[pos],
                    rel_type,
                    direction: Direction::Outgoing,
                    pos: pos as u32,
                });
            }
        }
        for inpos in &mut self.inc {
            let rel_type = self.graph.in_types[inpos];
            if self.matcher.matches(rel_type) {
                // Map the incoming position to where the property is stored.
                let pos = self
                    .graph
                    .in_to_out
                    .get(inpos)
                    .copied()
                    .unwrap_or(inpos as u32);
                return Some(RelationshipRef {
                    neighbor: self.graph.in_nbrs[inpos],
                    rel_type,
                    direction: Direction::Incoming,
                    pos,
                });
            }
        }
        None
    }
}
