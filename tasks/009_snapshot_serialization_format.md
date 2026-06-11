# Snapshot serialization format: separate nodes / relationships / records

Design and implement an on-disk serialization format for `GraphSnapshot`,
laid out in independently loadable sections so a remote reader can choose
what to keep resident:

- **Nodes section** — node count, label index (portable RoaringBitmap
  serialization, byte-compatible with the roaringrange family), string
  table (atoms).
- **Relationships section** — CSR adjacency: out/in offsets, neighbor
  arrays, parallel type arrays, type index bitmaps. This is the part a
  traversal reader wants fully in memory.
- **Records section** — per-node payloads (properties or opaque
  app-defined records). Borrow the RRSR record store format from
  roaringrange (RECORDS.md): `.idx` = 16-byte header (magic "RRSR",
  version, count) + N+1 u64 offsets; `.bin` = concatenated payloads in
  node-ID order; optional `.dict` for zstd-with-shared-dictionary
  (version 2 framing). Record d spans bin[off[d]..off[d+1]].

Requirements:
- Versioned magic + header with section offsets/lengths so sections can
  be range-fetched or memory-mapped independently.
- Little-endian, fixed-width integers; u32 node IDs (shared rank-ordered
  ID space convention: assign IDs in descending static rank so low IDs =
  most important nodes).
- Writer lives in rustychickpeas-core on GraphSnapshot (e.g.
  `write_sections(...)`); reader for the full format also in core for
  round-trip tests.
- Round-trip property tests: builder -> finalize -> write -> read ->
  identical query results.

Related: [010] wasm reader. Format docs should live in the repo as
FORMAT.md-style specs (one per section), matching roaringrange's habit of
freezing specs in markdown.
