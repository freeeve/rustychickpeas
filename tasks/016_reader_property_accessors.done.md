# Property accessors on GraphReader / WasmGraph

When properties ARE loaded (small graphs, loadProperties=true), the
reader parses node/rel columns into GraphSection but exposes no accessor
methods. Add GraphReader::node_prop(id, key) -> Option<value> (resolving
str atoms) and the wasm equivalent, enabling property-dependent traversal
filters in the browser for small graphs. ~50 lines; mirrors core's
Column::get dense/sparse lookup.

## Done

- `PropValue<'a>` — `Int(i64) | Float(f64) | Bool(bool) | Str(&'a str)`; string
  values resolve from atom IDs to borrowed strings (lifetime tied to the reader).
- `GraphReader::node_prop(node_id, key) -> Option<PropValue<'_>>`: binary-search
  the key-atom-sorted `node_columns`, then read the value. Mirrors core's
  `Column::get` — dense columns index directly, sparse columns binary-search the
  sorted `(id, value)` pairs. Covers all 8 `ColumnData` variants (Rank columns
  serialize to Sparse, so the reader never sees them). Returns `None` for an
  unknown key, a node absent from a sparse column, or a `topology_only` parse
  (property sections skipped).
- Wasm `WasmGraph.nodeProp(nodeId, key) -> JsValue`: natural JS type (number via
  f64 exact to 2^53-1, boolean, string) or `undefined` — ready for
  property-dependent traversal filters in the browser.

## Verification

- Unit tests round-trip a `GraphSection` (dense i64 + sparse str columns)
  through the RCPG codec and read values back: dense hit, sparse hit, sparse
  miss, unknown key, and `topology_only` → all `None`/values as expected.
- `cargo test -p rustychickpeas-reader` green (7 tests); clippy clean on native
  and `wasm32-unknown-unknown --features wasm`; rustfmt applied.

Non-core (reader crate), so no core API sign-off gate. `rel_prop` left out —
rel columns are keyed by outgoing-CSR position, which isn't a natural public
argument; add it if a browser consumer needs rel-property filters.

**Status: done.**
