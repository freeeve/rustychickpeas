//! RCPG: the RustyChickpeas graph file.
//!
//! See `FORMAT.md` in this crate for the frozen byte layout. Quick sketch
//! (all integers little-endian):
//!
//! ```text
//! header (16 B):  magic "RCPG" | version u16 | flags u16 | section_count u32 | reserved u32
//! directory:      section_count x { id u32, reserved u32, offset u64, length u64 }
//! sections:       at the directory offsets (relative to file start)
//! ```
//!
//! Section IDs: 1 atoms, 2 meta, 3 nodes, 4 relationships, 5 node columns,
//! 6 relationship columns. Unknown section IDs are ignored on read so the
//! format can grow.

use roaring::RoaringBitmap;
use std::io::Write;

use crate::cursor::{write as w, Cursor};
use crate::graph::{ColumnData, GraphSection};
use crate::{FormatError, Result};

pub const MAGIC: &[u8; 4] = b"RCPG";
pub const VERSION: u16 = 1;

const SECTION_ATOMS: u32 = 1;
const SECTION_META: u32 = 2;
const SECTION_NODES: u32 = 3;
const SECTION_RELS: u32 = 4;
const SECTION_NODE_COLS: u32 = 5;
const SECTION_REL_COLS: u32 = 6;

const HEADER_LEN: usize = 16;
const DIR_ENTRY_LEN: usize = 24;

/// Controls which optional sections a writer emits. Topology (atoms, meta,
/// nodes, relationships) is always written; property columns are optional
/// so large graphs can ship a lean traversal-only file and leave per-node
/// data to a range-fetched record store.
#[derive(Debug, Clone, Copy)]
pub struct WriteOptions {
    pub node_columns: bool,
    pub rel_columns: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            node_columns: true,
            rel_columns: true,
        }
    }
}

impl WriteOptions {
    /// Topology only: no property column sections.
    pub fn topology_only() -> Self {
        WriteOptions {
            node_columns: false,
            rel_columns: false,
        }
    }
}

/// Controls which optional sections a reader materializes. Skipped sections
/// cost nothing to parse and nothing in resident memory, regardless of
/// whether the file contains them.
#[derive(Debug, Clone, Copy)]
pub struct ParseOptions {
    pub node_columns: bool,
    pub rel_columns: bool,
}

impl Default for ParseOptions {
    fn default() -> Self {
        ParseOptions {
            node_columns: true,
            rel_columns: true,
        }
    }
}

impl ParseOptions {
    /// Topology only: ignore property column sections even when present.
    pub fn topology_only() -> Self {
        ParseOptions {
            node_columns: false,
            rel_columns: false,
        }
    }
}

/// Serialize a graph to RCPG bytes, including all sections.
pub fn write<W: Write>(graph: &GraphSection, out: &mut W) -> Result<()> {
    write_with(graph, out, &WriteOptions::default())
}

/// Serialize a graph to RCPG bytes, emitting optional sections per `opts`.
pub fn write_with<W: Write>(graph: &GraphSection, out: &mut W, opts: &WriteOptions) -> Result<()> {
    let mut sections: Vec<(u32, Vec<u8>)> = vec![
        (SECTION_ATOMS, encode_atoms(&graph.atoms)?),
        (SECTION_META, encode_meta(graph)?),
        (SECTION_NODES, encode_nodes(graph)?),
        (SECTION_RELS, encode_rels(graph)?),
    ];
    if opts.node_columns {
        sections.push((SECTION_NODE_COLS, encode_columns(&graph.node_columns)?));
    }
    if opts.rel_columns {
        sections.push((SECTION_REL_COLS, encode_columns(&graph.rel_columns)?));
    }

    out.write_all(MAGIC)?;
    w::u16(out, VERSION)?;
    w::u16(out, 0)?; // flags
    w::u32(out, sections.len() as u32)?;
    w::u32(out, 0)?; // reserved

    let mut offset = (HEADER_LEN + sections.len() * DIR_ENTRY_LEN) as u64;
    for (id, bytes) in &sections {
        w::u32(out, *id)?;
        w::u32(out, 0)?; // reserved
        w::u64(out, offset)?;
        w::u64(out, bytes.len() as u64)?;
        offset += bytes.len() as u64;
    }
    for (_, bytes) in &sections {
        out.write_all(bytes)?;
    }
    Ok(())
}

/// Parse RCPG bytes into a [`GraphSection`], materializing all sections.
/// Never panics on malformed input; all structural problems surface as
/// [`FormatError::Corrupt`].
pub fn parse(bytes: &[u8]) -> Result<GraphSection> {
    parse_with(bytes, &ParseOptions::default())
}

/// Parse RCPG bytes, materializing optional sections per `opts`. Use
/// [`ParseOptions::topology_only`] to keep property columns out of memory
/// on large graphs.
pub fn parse_with(bytes: &[u8], opts: &ParseOptions) -> Result<GraphSection> {
    let mut c = Cursor::new(bytes);
    let magic = c.take(4)?;
    if magic != MAGIC {
        return Err(FormatError::Corrupt("bad magic, not an RCPG file".into()));
    }
    let version = c.u16()?;
    if version > VERSION {
        return Err(FormatError::UnsupportedVersion {
            format: "RCPG",
            version,
        });
    }
    let _flags = c.u16()?;
    let section_count = c.u32()? as usize;
    let _reserved = c.u32()?;
    if section_count > 1024 {
        return Err(FormatError::Corrupt(format!(
            "implausible section count {}",
            section_count
        )));
    }

    let mut graph = GraphSection::default();
    let mut directory = Vec::with_capacity(section_count);
    for _ in 0..section_count {
        let id = c.u32()?;
        let _reserved = c.u32()?;
        let offset = c.u64()? as usize;
        let length = c.u64()? as usize;
        let end = offset.checked_add(length).ok_or_else(|| {
            FormatError::Corrupt(format!("section {} offset+length overflows", id))
        })?;
        if end > bytes.len() {
            return Err(FormatError::Corrupt(format!(
                "section {} extends past end of file ({} > {})",
                id,
                end,
                bytes.len()
            )));
        }
        directory.push((id, offset, length));
    }

    for (id, offset, length) in directory {
        let body = &bytes[offset..offset + length];
        match id {
            SECTION_ATOMS => graph.atoms = decode_atoms(body)?,
            SECTION_META => decode_meta(body, &mut graph)?,
            SECTION_NODES => decode_nodes(body, &mut graph)?,
            SECTION_RELS => decode_rels(body, &mut graph)?,
            SECTION_NODE_COLS if opts.node_columns => graph.node_columns = decode_columns(body)?,
            SECTION_REL_COLS if opts.rel_columns => graph.rel_columns = decode_columns(body)?,
            SECTION_NODE_COLS | SECTION_REL_COLS => {} // present but skipped
            _ => {} // forward compatibility: ignore unknown sections
        }
    }
    Ok(graph)
}

// --- atoms ---------------------------------------------------------------

fn encode_atoms(atoms: &[String]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    w::u32(&mut buf, atoms.len() as u32)?;
    for s in atoms {
        w::string(&mut buf, s)?;
    }
    Ok(buf)
}

fn decode_atoms(body: &[u8]) -> Result<Vec<String>> {
    let mut c = Cursor::new(body);
    let count = c.len_prefix(1)?;
    let mut atoms = Vec::with_capacity(count);
    for _ in 0..count {
        atoms.push(c.string()?);
    }
    Ok(atoms)
}

// --- meta ----------------------------------------------------------------

fn encode_meta(graph: &GraphSection) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    match &graph.version {
        Some(v) => {
            w::u8(&mut buf, 1)?;
            w::string(&mut buf, v)?;
        }
        None => w::u8(&mut buf, 0)?,
    }
    Ok(buf)
}

fn decode_meta(body: &[u8], graph: &mut GraphSection) -> Result<()> {
    let mut c = Cursor::new(body);
    if c.u8()? == 1 {
        graph.version = Some(c.string()?);
    }
    Ok(())
}

// --- nodes (label index) ---------------------------------------------------

fn encode_nodes(graph: &GraphSection) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    w::u32(&mut buf, graph.n_nodes)?;
    encode_bitmap_index(&mut buf, &graph.label_index)?;
    Ok(buf)
}

fn decode_nodes(body: &[u8], graph: &mut GraphSection) -> Result<()> {
    let mut c = Cursor::new(body);
    graph.n_nodes = c.u32()?;
    graph.label_index = decode_bitmap_index(&mut c)?;
    Ok(())
}

// --- relationships (CSR + type index) --------------------------------------

fn encode_rels(graph: &GraphSection) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    w::u64(&mut buf, graph.n_rels)?;
    w::u32_vec(&mut buf, &graph.out_offsets)?;
    w::u32_vec(&mut buf, &graph.out_nbrs)?;
    w::u32_vec(&mut buf, &graph.out_types)?;
    w::u32_vec(&mut buf, &graph.in_offsets)?;
    w::u32_vec(&mut buf, &graph.in_nbrs)?;
    w::u32_vec(&mut buf, &graph.in_types)?;
    encode_bitmap_index(&mut buf, &graph.type_index)?;
    Ok(buf)
}

fn decode_rels(body: &[u8], graph: &mut GraphSection) -> Result<()> {
    let mut c = Cursor::new(body);
    graph.n_rels = c.u64()?;
    graph.out_offsets = c.u32_vec()?;
    graph.out_nbrs = c.u32_vec()?;
    graph.out_types = c.u32_vec()?;
    graph.in_offsets = c.u32_vec()?;
    graph.in_nbrs = c.u32_vec()?;
    graph.in_types = c.u32_vec()?;
    graph.type_index = decode_bitmap_index(&mut c)?;
    Ok(())
}

// --- bitmap indexes (label/type -> roaring) ---------------------------------

fn encode_bitmap_index(buf: &mut Vec<u8>, index: &[(u32, RoaringBitmap)]) -> Result<()> {
    w::u32(buf, index.len() as u32)?;
    for (atom, bitmap) in index {
        w::u32(buf, *atom)?;
        w::u32(buf, bitmap.serialized_size() as u32)?;
        bitmap.serialize_into(&mut *buf)?;
    }
    Ok(())
}

fn decode_bitmap_index(c: &mut Cursor) -> Result<Vec<(u32, RoaringBitmap)>> {
    let count = c.len_prefix(8)?;
    let mut index = Vec::with_capacity(count);
    for _ in 0..count {
        let atom = c.u32()?;
        let len = c.len_prefix(1)?;
        let bytes = c.take(len)?;
        let bitmap = RoaringBitmap::deserialize_from(bytes)
            .map_err(|e| FormatError::Corrupt(format!("invalid roaring bitmap: {}", e)))?;
        index.push((atom, bitmap));
    }
    Ok(index)
}

// --- property columns --------------------------------------------------------

const COL_DENSE_I64: u8 = 0;
const COL_DENSE_F64: u8 = 1;
const COL_DENSE_BOOL: u8 = 2;
const COL_DENSE_STR: u8 = 3;
const COL_SPARSE_I64: u8 = 4;
const COL_SPARSE_F64: u8 = 5;
const COL_SPARSE_BOOL: u8 = 6;
const COL_SPARSE_STR: u8 = 7;

fn encode_columns(columns: &[(u32, ColumnData)]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    w::u32(&mut buf, columns.len() as u32)?;
    for (key, col) in columns {
        w::u32(&mut buf, *key)?;
        match col {
            ColumnData::DenseI64(v) => {
                w::u8(&mut buf, COL_DENSE_I64)?;
                w::u32(&mut buf, v.len() as u32)?;
                for &x in v {
                    w::i64(&mut buf, x)?;
                }
            }
            ColumnData::DenseF64(v) => {
                w::u8(&mut buf, COL_DENSE_F64)?;
                w::u32(&mut buf, v.len() as u32)?;
                for &x in v {
                    w::f64(&mut buf, x)?;
                }
            }
            ColumnData::DenseBool(bits) => {
                w::u8(&mut buf, COL_DENSE_BOOL)?;
                w::u32(&mut buf, bits.len() as u32)?;
                let mut packed = vec![0u8; bits.len().div_ceil(8)];
                for (i, bit) in bits.iter().enumerate() {
                    if *bit {
                        packed[i / 8] |= 1 << (i % 8);
                    }
                }
                buf.write_all(&packed)?;
            }
            ColumnData::DenseStr(v) => {
                w::u8(&mut buf, COL_DENSE_STR)?;
                w::u32_vec(&mut buf, v)?;
            }
            ColumnData::SparseI64(v) => {
                w::u8(&mut buf, COL_SPARSE_I64)?;
                w::u32(&mut buf, v.len() as u32)?;
                for (id, x) in v {
                    w::u32(&mut buf, *id)?;
                    w::i64(&mut buf, *x)?;
                }
            }
            ColumnData::SparseF64(v) => {
                w::u8(&mut buf, COL_SPARSE_F64)?;
                w::u32(&mut buf, v.len() as u32)?;
                for (id, x) in v {
                    w::u32(&mut buf, *id)?;
                    w::f64(&mut buf, *x)?;
                }
            }
            ColumnData::SparseBool(v) => {
                w::u8(&mut buf, COL_SPARSE_BOOL)?;
                w::u32(&mut buf, v.len() as u32)?;
                for (id, x) in v {
                    w::u32(&mut buf, *id)?;
                    w::u8(&mut buf, *x as u8)?;
                }
            }
            ColumnData::SparseStr(v) => {
                w::u8(&mut buf, COL_SPARSE_STR)?;
                w::u32(&mut buf, v.len() as u32)?;
                for (id, x) in v {
                    w::u32(&mut buf, *id)?;
                    w::u32(&mut buf, *x)?;
                }
            }
        }
    }
    Ok(buf)
}

fn decode_columns(body: &[u8]) -> Result<Vec<(u32, ColumnData)>> {
    let mut c = Cursor::new(body);
    let count = c.len_prefix(5)?;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let key = c.u32()?;
        let tag = c.u8()?;
        let col = match tag {
            COL_DENSE_I64 => {
                let len = c.len_prefix(8)?;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    v.push(c.i64()?);
                }
                ColumnData::DenseI64(v)
            }
            COL_DENSE_F64 => {
                let len = c.len_prefix(8)?;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    v.push(c.f64()?);
                }
                ColumnData::DenseF64(v)
            }
            COL_DENSE_BOOL => {
                let bit_len = c.u32()? as usize;
                let packed = c.take(bit_len.div_ceil(8))?;
                let mut bits = bitvec::vec::BitVec::new();
                bits.resize(bit_len, false);
                for i in 0..bit_len {
                    if packed[i / 8] & (1 << (i % 8)) != 0 {
                        bits.set(i, true);
                    }
                }
                ColumnData::DenseBool(bits)
            }
            COL_DENSE_STR => ColumnData::DenseStr(c.u32_vec()?),
            COL_SPARSE_I64 => {
                let len = c.len_prefix(12)?;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    v.push((c.u32()?, c.i64()?));
                }
                ColumnData::SparseI64(v)
            }
            COL_SPARSE_F64 => {
                let len = c.len_prefix(12)?;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    v.push((c.u32()?, c.f64()?));
                }
                ColumnData::SparseF64(v)
            }
            COL_SPARSE_BOOL => {
                let len = c.len_prefix(5)?;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    v.push((c.u32()?, c.u8()? != 0));
                }
                ColumnData::SparseBool(v)
            }
            COL_SPARSE_STR => {
                let len = c.len_prefix(8)?;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    v.push((c.u32()?, c.u32()?));
                }
                ColumnData::SparseStr(v)
            }
            other => {
                return Err(FormatError::Corrupt(format!(
                    "unknown column tag {} at offset {}",
                    other,
                    c.position()
                )))
            }
        };
        columns.push((key, col));
    }
    Ok(columns)
}
