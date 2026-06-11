//! On-disk serialization formats for RustyChickpeas.
//!
//! Two formats live here, designed for the split-residency reader model
//! (resident graph topology, range-fetched records):
//!
//! - **RCPG** ([`rcpg`]) — the graph file: string table, label/type
//!   indexes, CSR adjacency, and property columns, laid out in
//!   independently locatable sections. A traversal reader loads this file
//!   wholesale (or skips the property sections).
//! - **RRSR** ([`rrsr`]) — the record store, byte-compatible with
//!   roaringrange's RECORDS.md spec: `.idx` (header + N+1 u64 offsets) +
//!   `.bin` (concatenated payloads in ID order), so records can be served
//!   via batched HTTP range reads.
//!
//! This crate has no I/O, async, or platform dependencies beyond `roaring`
//! and `bitvec`, and builds for `wasm32-unknown-unknown`. Readers never
//! panic on malformed input; every parse error surfaces as
//! [`FormatError::Corrupt`].

pub mod error;
pub mod graph;
pub mod rcpg;
pub mod rrsr;

mod cursor;

pub use error::FormatError;
pub use graph::{ColumnData, GraphSection};

/// Result alias for format operations.
pub type Result<T> = std::result::Result<T, FormatError>;
