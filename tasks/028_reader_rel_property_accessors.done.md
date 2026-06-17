# 028 — Relationship-property accessors (resolve 015's rel-property design point)

Follow-up from [015](015_shared_id_build_pipeline.done.md): relationship-property
filters had no place in the node-keyed record store. Resolved pragmatically with
**option (a): resident rel columns** — the format already stores `rel_columns`
(keyed by outgoing-CSR position); expose them read-side, mirroring `node_prop`.

## Done

- `GraphReader::out_edges(node_id) -> Vec<(neighbor, csr_pos)>` — outgoing edges
  paired with their outgoing-CSR position, the key into the rel-property columns.
- `GraphReader::rel_prop(csr_pos, key) -> Option<PropValue>` — reads a
  relationship property at a CSR position, resolving string atoms. Reuses the
  `column_value` helper from `node_prop` (028 adds no column-read code). Returns
  `None` for unknown keys, sparse misses, or a `topology_only` parse.
- Wasm: `WasmGraph.outEdges(nodeId)` (flat `[nbr0, pos0, nbr1, pos1, …]`) and
  `WasmGraph.relProp(csrPos, key)` (natural JS value or `undefined`). The JS
  conversion is shared with `nodeProp` via a `prop_to_js` helper.

Usage — filter a traversal by edge property:
```rust
for (nbr, pos) in reader.out_edges(node) {
    if reader.rel_prop(pos, "year") == Some(PropValue::Int(2020)) {
        // follow this edge
    }
}
```

## Scope / limits

- Properties are keyed by **outgoing-CSR position**, so incoming-side edge-property
  lookups aren't addressable through this API (inherent to the storage). Filter on
  the outgoing side.
- **Option (b)** — a separate range-fetched rel-records store keyed by CSR
  position — stays a future option for *heavy* edge payloads; resident columns
  are the right default for the small edge data (weights, dates) that's typical.

## Verification

Round-trip test: a 2-node graph with a `weight` rel column written through the
RCPG codec, then `out_edges` + `rel_prop` read it back; `topology_only` drops the
column but keeps the topology. `cargo test -p rustychickpeas-reader` green; clippy
clean native + `wasm32`; rustfmt.

**Status: done.**
