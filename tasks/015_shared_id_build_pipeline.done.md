# Shared-ID build pipeline (roaringrange + RCPG from one corpus)

The search-then-traverse browser flow needs both artifact families built
over ONE rank-ordered u32 ID space: doc IDs (roaringrange index/records)
== node IDs (RCPG graph). Build a pipeline that:
- assigns IDs in descending static rank (citations etc.)
- emits the roaringrange index family AND graph.rcpg (topology-only) +
  records.{idx,bin} from the same corpus
- demo candidate: an OpenAlex slice (one topic's citation neighborhood or
  top-N-cited works) — full 1.8B-edge graph exceeds the resident wasm
  budget (~tens of millions of rels)

Open design point from 2026-06-11 discussion: relationship-property
filters have no record-store equivalent (records are node-keyed); options
are opt-in rel columns (often small) or a rel-records store keyed by
outgoing-CSR position.

## Done

### New crate `rustychickpeas-pipeline`
`build_shared_corpus(docs, out_dir, cfg)` (and `build_artifacts` for in-memory
bytes) takes a `Document { key, text, rank, links, record }` corpus and:
1. rank-sorts descending and assigns contiguous IDs `0..N` (id 0 = top rank),
   matching roaringrange's rank-ordered doc-ID space;
2. emits `index.rrs` — trigram postings keyed by the assigned ID, via
   `roaringrange::{ngram_keys, build::serialize_posting, build::write_index}`;
3. emits `graph.rcpg` — topology-only; node ID == doc ID, link edges resolved
   `key -> id` through the same map (core `GraphBuilder` + `write_rcpg_with`);
4. emits `records.{idx,bin}` — payloads in ID order (`format::rrsr::write`).

A `shared_id_alignment` test proves the payoff: a search hit's doc ID indexes
directly into the graph (has the right CITES edges) and the record store.

### Synthetic driver
`examples/generate_shared_demo.rs` builds a deterministic power-law citation
corpus (300 works, ~1050 edges) and regenerates the demo artifacts. Real
OpenAlex ingest is a thin follow-up adapter over the same `Document` model.

### Browser search wiring (reader)
- `ResidentSearch` + wasm `WasmSearch`: load the small `.rrs` fully into memory
  and query in-process, driving roaringrange's async `Index::search` over a
  synchronous `MemoryFetch` with a single poll (`poll_ready`, no async runtime —
  compiles for wasm). Returned doc IDs share the graph's node-ID space.
- Demo rewritten into **search → traverse → ranged record fetch**: a query hits
  `WasmSearch`, the hit IDs feed `WasmGraph` traversal and a coalesced
  `WasmRecordIndex` range fetch — one wasm module, one ID space.

## Verification
- `cargo test -p rustychickpeas-pipeline` (alignment, incl. live trigram search)
  and `-p rustychickpeas-reader` (resident search) green; clippy clean; rustfmt.
- `wasm-pack build` produces the reader wasm with `WasmGraph` / `WasmSearch` /
  `WasmRecordIndex`; demo JS method names verified against the bindings.
- Artifacts regenerate with correct magic (RRSI / RCPG / RRSR).

## Follow-ups
- rel-property filters — **resolved** in
  [028](028_reader_rel_property_accessors.done.md) via option (a): resident rel
  columns with `GraphReader::out_edges` + `rel_prop` (a rel-records store stays a
  future option for heavy edge payloads).
- OpenAlex ingest — **done (MVP)** in the sibling `openalex-graph-demo` repo (kept
  separate so the ingest-only deps `serde_json`/`flate2` stay out of this
  workspace). Maps OpenAlex works → the `Document` model, top-N-cited slicing via
  a bounded heap, reads `.jsonl[.gz]`, in-slice edge filtering; tested on a
  synthetic sample + an end-to-end run. Future ("full") work: S3 streaming,
  topic/concept slicing, abstract reconstruction.

**Status: done.**
