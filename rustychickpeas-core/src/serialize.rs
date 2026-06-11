//! RCPG serialization for GraphSnapshot.
//!
//! Converts between the in-memory snapshot and the on-disk
//! `rustychickpeas_format` model. The lazily built property index is not
//! serialized; it is derived data and rebuilds on first use after reading.
//!
//! Snapshots whose label/type sets use the Bitset representation are
//! converted to roaring bitmaps on write; reading always produces the
//! Roaring representation.

use hashbrown::HashMap;
use roaring::RoaringBitmap;
use rustychickpeas_format::{rcpg, ColumnData, FormatError, GraphSection};
use std::io::Write;
use std::sync::Mutex;

use crate::bitmap::NodeSet;
use crate::error::{GraphError, Result};
use crate::graph_snapshot::{Atoms, Column, GraphSnapshot};
use crate::types::{Label, RelationshipType};

impl From<FormatError> for GraphError {
    fn from(e: FormatError) -> Self {
        match e {
            FormatError::Io(io) => GraphError::IoError(io),
            other => GraphError::SerializationError(other.to_string()),
        }
    }
}

fn nodeset_to_bitmap(set: &NodeSet) -> RoaringBitmap {
    match set {
        NodeSet::Roaring(bm) => bm.clone(),
        NodeSet::Bitset(_) => set.iter().collect(),
    }
}

fn column_to_data(col: &Column) -> ColumnData {
    match col {
        Column::DenseI64(v) => ColumnData::DenseI64(v.clone()),
        Column::DenseF64(v) => ColumnData::DenseF64(v.clone()),
        Column::DenseBool(v) => ColumnData::DenseBool(v.clone()),
        Column::DenseStr(v) => ColumnData::DenseStr(v.clone()),
        Column::SparseI64(v) => ColumnData::SparseI64(v.clone()),
        Column::SparseF64(v) => ColumnData::SparseF64(v.clone()),
        Column::SparseBool(v) => ColumnData::SparseBool(v.clone()),
        Column::SparseStr(v) => ColumnData::SparseStr(v.clone()),
    }
}

fn data_to_column(data: ColumnData) -> Column {
    match data {
        ColumnData::DenseI64(v) => Column::DenseI64(v),
        ColumnData::DenseF64(v) => Column::DenseF64(v),
        ColumnData::DenseBool(v) => Column::DenseBool(v),
        ColumnData::DenseStr(v) => Column::DenseStr(v),
        ColumnData::SparseI64(v) => Column::SparseI64(v),
        ColumnData::SparseF64(v) => Column::SparseF64(v),
        ColumnData::SparseBool(v) => Column::SparseBool(v),
        ColumnData::SparseStr(v) => Column::SparseStr(v),
    }
}

impl GraphSnapshot {
    /// Convert this snapshot to the plain on-disk data model.
    pub fn to_graph_section(&self) -> GraphSection {
        let mut label_index: Vec<(u32, RoaringBitmap)> = self
            .label_index
            .iter()
            .map(|(label, set)| (label.id(), nodeset_to_bitmap(set)))
            .collect();
        label_index.sort_unstable_by_key(|(atom, _)| *atom);

        let mut type_index: Vec<(u32, RoaringBitmap)> = self
            .type_index
            .iter()
            .map(|(t, set)| (t.id(), nodeset_to_bitmap(set)))
            .collect();
        type_index.sort_unstable_by_key(|(atom, _)| *atom);

        let mut node_columns: Vec<(u32, ColumnData)> = self
            .columns
            .iter()
            .map(|(key, col)| (*key, column_to_data(col)))
            .collect();
        node_columns.sort_unstable_by_key(|(key, _)| *key);

        let mut rel_columns: Vec<(u32, ColumnData)> = self
            .rel_columns
            .iter()
            .map(|(key, col)| (*key, column_to_data(col)))
            .collect();
        rel_columns.sort_unstable_by_key(|(key, _)| *key);

        GraphSection {
            n_nodes: self.n_nodes,
            n_rels: self.n_rels,
            out_offsets: self.out_offsets.clone(),
            out_nbrs: self.out_nbrs.clone(),
            out_types: self.out_types.iter().map(|t| t.id()).collect(),
            in_offsets: self.in_offsets.clone(),
            in_nbrs: self.in_nbrs.clone(),
            in_types: self.in_types.iter().map(|t| t.id()).collect(),
            label_index,
            type_index,
            node_columns,
            rel_columns,
            version: self.version.clone(),
            atoms: self.atoms.strings.clone(),
        }
    }

    /// Build a snapshot from the on-disk data model.
    pub fn from_graph_section(section: GraphSection) -> Self {
        let label_index: HashMap<Label, NodeSet> = section
            .label_index
            .into_iter()
            .map(|(atom, bm)| (Label::new(atom), NodeSet::Roaring(bm)))
            .collect();
        let type_index: HashMap<RelationshipType, NodeSet> = section
            .type_index
            .into_iter()
            .map(|(atom, bm)| (RelationshipType::new(atom), NodeSet::Roaring(bm)))
            .collect();
        let columns: HashMap<u32, Column> = section
            .node_columns
            .into_iter()
            .map(|(key, data)| (key, data_to_column(data)))
            .collect();
        let rel_columns: HashMap<u32, Column> = section
            .rel_columns
            .into_iter()
            .map(|(key, data)| (key, data_to_column(data)))
            .collect();

        GraphSnapshot {
            n_nodes: section.n_nodes,
            n_rels: section.n_rels,
            out_offsets: section.out_offsets,
            out_nbrs: section.out_nbrs,
            out_types: section
                .out_types
                .into_iter()
                .map(RelationshipType::new)
                .collect(),
            in_offsets: section.in_offsets,
            in_nbrs: section.in_nbrs,
            in_types: section
                .in_types
                .into_iter()
                .map(RelationshipType::new)
                .collect(),
            label_index,
            type_index,
            version: section.version,
            columns,
            rel_columns,
            prop_index: Mutex::new(HashMap::new()),
            atoms: Atoms::new(section.atoms),
        }
    }

    /// Serialize this snapshot to RCPG bytes (see rustychickpeas-format's
    /// FORMAT.md for the layout).
    pub fn write_rcpg<W: Write>(&self, out: &mut W) -> Result<()> {
        rcpg::write(&self.to_graph_section(), out)?;
        Ok(())
    }

    /// Read a snapshot from RCPG bytes. The property index rebuilds lazily
    /// on first use.
    pub fn read_rcpg(bytes: &[u8]) -> Result<GraphSnapshot> {
        Ok(Self::from_graph_section(rcpg::parse(bytes)?))
    }

    /// Serialize to an RCPG file on disk.
    pub fn write_rcpg_file(&self, path: &str) -> Result<()> {
        let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);
        self.write_rcpg(&mut file)
    }

    /// Read a snapshot from an RCPG file on disk.
    pub fn read_rcpg_file(path: &str) -> Result<GraphSnapshot> {
        let bytes = std::fs::read(path)?;
        Self::read_rcpg(&bytes)
    }
}
