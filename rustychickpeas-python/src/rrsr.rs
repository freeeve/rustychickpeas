//! Python bindings for the RRSR record store (split-residency node records).
//!
//! An RRSR store is an `idx` + `bin` file pair: `bin` holds concatenated byte
//! records and `idx` maps record id -> byte range. It pairs with a topology-only
//! RCPG graph so a loader can emit node payloads (e.g. metadata) once, keyed by
//! the same id space, for the search-then-traverse reader flow.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rustychickpeas_core::format::rrsr;
use std::fs::File;
use std::io::{BufWriter, Write};

fn io_err(e: std::io::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string())
}

/// Write byte records to an RRSR store (`idx_path` + `bin_path`). Record `i` is
/// addressed by id `i`, so pass them in node-ID order. `records` is any iterable
/// of `bytes`.
#[pyfunction]
pub fn write_rrsr(idx_path: String, bin_path: String, records: Vec<Vec<u8>>) -> PyResult<()> {
    let mut idx = BufWriter::new(File::create(&idx_path).map_err(io_err)?);
    let mut bin = BufWriter::new(File::create(&bin_path).map_err(io_err)?);
    rrsr::write(&mut idx, &mut bin, records.iter().map(|r| r.as_slice()))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    idx.flush().map_err(io_err)?;
    bin.flush().map_err(io_err)?;
    Ok(())
}

/// Read every record back from an RRSR store as a list of `bytes` (record `i` at
/// index `i`). For round-tripping/verification; resident traversal uses the
/// reader crate's ranged fetches rather than loading every record at once.
#[pyfunction]
pub fn read_rrsr(py: Python<'_>, idx_path: String, bin_path: String) -> PyResult<Vec<Py<PyBytes>>> {
    let idx_bytes = std::fs::read(&idx_path).map_err(io_err)?;
    let bin_bytes = std::fs::read(&bin_path).map_err(io_err)?;
    let index = rrsr::RecordIndex::parse(&idx_bytes)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    let mut out = Vec::with_capacity(index.len());
    for i in 0..index.len() as u32 {
        let (start, end) = index.record_range(i).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("record {i} out of range"))
        })?;
        // Vec<u8> would map to a Python list[int]; PyBytes gives `bytes`.
        out.push(PyBytes::new(py, &bin_bytes[start as usize..end as usize]).into());
    }
    Ok(out)
}
