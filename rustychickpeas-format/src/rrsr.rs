//! RRSR record store: byte-compatible with roaringrange's RECORDS.md.
//!
//! Layout (all integers little-endian):
//!
//! ```text
//! .idx: magic "RRSR" | version u16 | reserved u16 | count u32 | reserved2 u32
//!       then (count + 1) x u64 offsets into .bin
//! .bin: record payloads concatenated in ID order; record d spans
//!       bin[off[d] .. off[d+1]]
//! ```
//!
//! This module implements version 1 (raw payloads). Version 2 framing
//! (per-record zstd with a shared dictionary) is read-rejected here; the
//! browser reader uses roaringrange's own RecordStore implementation,
//! which supports both.

use std::io::Write;

use crate::cursor::{write as w, Cursor};
use crate::{FormatError, Result};

pub const MAGIC: &[u8; 4] = b"RRSR";
pub const VERSION: u16 = 1;

const HEADER_LEN: usize = 16;

/// Write a version-1 record store. `records` are payloads in ID order
/// (record i belongs to ID i). Produces the `.idx` and `.bin` streams.
pub fn write<'a, W1, W2, I>(idx: &mut W1, bin: &mut W2, records: I) -> Result<()>
where
    W1: Write,
    W2: Write,
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut offsets: Vec<u64> = vec![0];
    let mut pos = 0u64;
    for record in records {
        bin.write_all(record)?;
        pos += record.len() as u64;
        offsets.push(pos);
    }

    idx.write_all(MAGIC)?;
    w::u16(idx, VERSION)?;
    w::u16(idx, 0)?; // reserved
    w::u32(idx, (offsets.len() - 1) as u32)?;
    w::u32(idx, 0)?; // reserved2
    for off in offsets {
        w::u64(idx, off)?;
    }
    Ok(())
}

/// Parsed `.idx` file: offsets into the `.bin` payload stream.
#[derive(Debug, Clone)]
pub struct RecordIndex {
    offsets: Vec<u64>,
}

impl RecordIndex {
    /// Parse a version-1 `.idx` file.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let mut c = Cursor::new(bytes);
        let magic = c.take(4)?;
        if magic != MAGIC {
            return Err(FormatError::Corrupt("bad magic, not an RRSR index".into()));
        }
        let version = c.u16()?;
        if version != VERSION {
            return Err(FormatError::UnsupportedVersion {
                format: "RRSR",
                version,
            });
        }
        let _reserved = c.u16()?;
        let count = c.u32()? as usize;
        let _reserved2 = c.u32()?;
        if bytes.len() != HEADER_LEN + (count + 1) * 8 {
            return Err(FormatError::Corrupt(format!(
                "index length {} doesn't match count {}",
                bytes.len(),
                count
            )));
        }
        let mut offsets = Vec::with_capacity(count + 1);
        for _ in 0..=count {
            offsets.push(c.u64()?);
        }
        if offsets.windows(2).any(|p| p[0] > p[1]) {
            return Err(FormatError::Corrupt("offsets not monotonic".into()));
        }
        Ok(RecordIndex { offsets })
    }

    /// Number of records.
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// True if the store holds no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Byte range of record `id` in the `.bin` file, or None if out of range.
    pub fn record_range(&self, id: u32) -> Option<(u64, u64)> {
        let i = id as usize;
        if i + 1 >= self.offsets.len() {
            return None;
        }
        Some((self.offsets[i], self.offsets[i + 1]))
    }

    /// Plan batched range reads for a set of record IDs: sorts and
    /// deduplicates the IDs, then coalesces ranges whose gap is at most
    /// `max_gap` bytes into single reads. Returns `(start, end)` byte
    /// ranges over the `.bin` file, in ascending order. IDs out of range
    /// are ignored.
    pub fn plan_ranges(&self, ids: &[u32], max_gap: u64) -> Vec<(u64, u64)> {
        let mut sorted: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|&id| (id as usize) + 1 < self.offsets.len())
            .collect();
        sorted.sort_unstable();
        sorted.dedup();

        let mut ranges: Vec<(u64, u64)> = Vec::new();
        for id in sorted {
            let (start, end) = self.record_range(id).unwrap();
            if start == end {
                continue; // empty record, nothing to fetch
            }
            match ranges.last_mut() {
                Some((_, last_end)) if start <= *last_end + max_gap => {
                    *last_end = (*last_end).max(end);
                }
                _ => ranges.push((start, end)),
            }
        }
        ranges
    }
}
