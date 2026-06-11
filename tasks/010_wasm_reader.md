# WASM reader: resident adjacency, ranged record gets

A browser/WASM reader for the 009 serialization format with a split
residency model:

- **Resident**: nodes + relationships sections (CSR adjacency, label/type
  bitmaps, atoms) load fully into memory at boot so traversals (neighbors,
  BFS, k-hop, label/type filters) run locally with no network round trips
  per hop.
- **Remote**: records section stays on object storage/CDN; record reads go
  through batched HTTP range GETs — sort requested node IDs, coalesce
  adjacent/nearby offsets into few ranges (borrow roaringrange's
  RecordStore reader pattern, including zstd shared-dictionary framing).

Implementation notes:
- New crate (e.g. `rustychickpeas-reader` or `-wasm`), NOT the core crate:
  core's tokio/object_store/parquet/rayon deps don't build for
  wasm32-unknown-unknown. Reader deps: roaring, byte parsing, a fetch
  abstraction (trait over HTTP Range; wasm impl via fetch/JS binding,
  native impl for tests).
- Decide whether the RecordStore reader is vendored, depended on (if
  roaringrange's rust crate exposes it as a library), or reimplemented to
  the frozen RRSR spec. Conformance test against roaringrange-written
  record stores either way.
- Memory budget: resident adjacency is ~8 bytes/edge (u32 neighbor x2
  directions) + offsets; fine up to tens of millions of edges in a browser
  tab. Document the practical ceiling; huge graphs (OpenAlex-scale
  citation graph) would need the range-fetched-adjacency variant — out of
  scope here, noted for later.
- wasm-pack packaging + a minimal JS demo (load snapshot, run BFS, click a
  node, fetch its record via ranged get).

Depends on: [009] snapshot serialization format.
