//! A lightweight node-id set (RoaringBitmap-backed) so set-returning query
//! results compose in Rust instead of round-tripping through a Python `set`.

use pyo3::prelude::*;
use roaring::RoaringBitmap;

/// A set of node IDs. Build one from any id list (e.g. `nodes_with_label`,
/// `neighborhood`, `neighbor_ids`) and combine with `&` / `|` / `-` (or the named
/// `intersect` / `union` / `difference`) — all on RoaringBitmaps, no Python `set`.
#[pyclass(name = "NodeSet")]
pub struct NodeSet {
    pub(crate) inner: RoaringBitmap,
}

impl NodeSet {
    pub(crate) fn from_bitmap(inner: RoaringBitmap) -> Self {
        NodeSet { inner }
    }
}

#[pymethods]
impl NodeSet {
    /// Build from an iterable of node ids (empty if omitted).
    #[new]
    #[pyo3(signature = (ids=None))]
    fn new(ids: Option<Vec<u32>>) -> Self {
        let mut inner = RoaringBitmap::new();
        if let Some(ids) = ids {
            inner.extend(ids);
        }
        NodeSet { inner }
    }

    fn __len__(&self) -> usize {
        self.inner.len() as usize
    }

    fn __contains__(&self, node_id: u32) -> bool {
        self.inner.contains(node_id)
    }

    fn __bool__(&self) -> bool {
        !self.inner.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn __repr__(&self) -> String {
        format!("NodeSet(len={})", self.inner.len())
    }

    /// The ids as a sorted list.
    fn to_list(&self) -> Vec<u32> {
        self.inner.iter().collect()
    }

    /// Insert `node_id`; returns whether it was newly added.
    fn add(&mut self, node_id: u32) -> bool {
        self.inner.insert(node_id)
    }

    fn intersect(&self, other: &NodeSet) -> NodeSet {
        NodeSet::from_bitmap(&self.inner & &other.inner)
    }

    fn union(&self, other: &NodeSet) -> NodeSet {
        NodeSet::from_bitmap(&self.inner | &other.inner)
    }

    fn difference(&self, other: &NodeSet) -> NodeSet {
        NodeSet::from_bitmap(&self.inner - &other.inner)
    }

    fn __and__(&self, other: &NodeSet) -> NodeSet {
        self.intersect(other)
    }

    fn __or__(&self, other: &NodeSet) -> NodeSet {
        self.union(other)
    }

    fn __sub__(&self, other: &NodeSet) -> NodeSet {
        self.difference(other)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> NodeSetIter {
        NodeSetIter {
            ids: slf.inner.iter().collect::<Vec<_>>().into_iter(),
        }
    }
}

/// Iterator over a [`NodeSet`]'s ids (ascending).
#[pyclass]
pub struct NodeSetIter {
    ids: std::vec::IntoIter<u32>,
}

#[pymethods]
impl NodeSetIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u32> {
        slf.ids.next()
    }
}
