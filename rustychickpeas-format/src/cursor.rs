//! Bounds-checked little-endian reader over a byte slice.

use crate::{FormatError, Result};

pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(FormatError::Corrupt(format!(
                "unexpected end of input: need {} bytes at offset {}, have {}",
                n,
                self.pos,
                self.remaining()
            )));
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.u64()?))
    }

    /// Read a length prefix that will be used to allocate `elem_size`-byte
    /// elements, rejecting lengths that exceed the remaining input (prevents
    /// huge allocations from corrupt length fields).
    pub fn len_prefix(&mut self, elem_size: usize) -> Result<usize> {
        let len = self.u32()? as usize;
        let need = len.checked_mul(elem_size).ok_or_else(|| {
            FormatError::Corrupt(format!("length overflow at offset {}", self.pos))
        })?;
        if need > self.remaining() {
            return Err(FormatError::Corrupt(format!(
                "declared length {} ({} bytes) exceeds remaining input {} at offset {}",
                len,
                need,
                self.remaining(),
                self.pos
            )));
        }
        Ok(len)
    }

    pub fn u32_vec(&mut self) -> Result<Vec<u32>> {
        let len = self.len_prefix(4)?;
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(self.u32()?);
        }
        Ok(v)
    }

    pub fn string(&mut self) -> Result<String> {
        let len = self.len_prefix(1)?;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| FormatError::Corrupt(format!("invalid utf8 at offset {}", self.pos)))
    }
}

/// Little-endian writer helpers over any `std::io::Write`.
pub(crate) mod write {
    use crate::Result;
    use std::io::Write;

    pub fn u8<W: Write>(w: &mut W, v: u8) -> Result<()> {
        Ok(w.write_all(&[v])?)
    }

    pub fn u16<W: Write>(w: &mut W, v: u16) -> Result<()> {
        Ok(w.write_all(&v.to_le_bytes())?)
    }

    pub fn u32<W: Write>(w: &mut W, v: u32) -> Result<()> {
        Ok(w.write_all(&v.to_le_bytes())?)
    }

    pub fn u64<W: Write>(w: &mut W, v: u64) -> Result<()> {
        Ok(w.write_all(&v.to_le_bytes())?)
    }

    pub fn i64<W: Write>(w: &mut W, v: i64) -> Result<()> {
        Ok(w.write_all(&v.to_le_bytes())?)
    }

    pub fn f64<W: Write>(w: &mut W, v: f64) -> Result<()> {
        u64(w, v.to_bits())
    }

    pub fn u32_vec<W: Write>(w: &mut W, v: &[u32]) -> Result<()> {
        u32(w, v.len() as u32)?;
        for &x in v {
            u32(w, x)?;
        }
        Ok(())
    }

    pub fn string<W: Write>(w: &mut W, s: &str) -> Result<()> {
        u32(w, s.len() as u32)?;
        Ok(w.write_all(s.as_bytes())?)
    }
}
