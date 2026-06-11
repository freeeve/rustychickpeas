//! Remote record access.
//!
//! Records are read through roaringrange's `RecordStore`, which understands
//! both RRSR v1 (raw) and v2 (framed, optionally zstd-compressed against a
//! shared dictionary — enable this crate's `zstd` feature). The store is
//! generic over [`RangeFetch`]; supply a fetcher for your transport (memory
//! and file fetchers ship with roaringrange; its `wasm` feature has a
//! browser `fetch()` implementation).
//!
//! When driving raw HTTP yourself instead, parse the `.idx` with
//! [`rustychickpeas_format::rrsr::RecordIndex`] and use its `plan_ranges`
//! to coalesce record reads into few range requests.

pub use roaringrange::fetch::{FetchError, MemoryFetch, RangeFetch};
pub use roaringrange::records::RecordStore;

pub use rustychickpeas_format::rrsr::RecordIndex;
