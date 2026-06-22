# 029 — API doc cleanup (no behavior change)

Doc-only fixes surfaced by the 0.9.0 API-consistency review. No public signature
changes.

## Done
- Unscrambled the crossed `CoWeight` / `RelationshipRef` doc comments in
  `core/graph_snapshot.rs` — each type now carries its own doc again.
- `core/analytics.rs` `sssp` doc: "additive **edge** weights" → "additive **rel**
  weights" (the lone "edge" leak on the public surface).
- `py/graph_snapshot.rs` `column` / `string_id` docstrings now lead with
  `memoryview(col)` / `to_pylist()`; numpy/pyarrow demoted to optional
  buffer-protocol consumers ("never required to filter").
- Upgraded stub/internal-leaking docstrings: `relationships_with_type` (dropped
  the "type_index bitmap" impl leak; states the not-found raise), `all_nodes`,
  `all_relationships`, and the `Relationship` accessors `start_node`/`end_node`/
  `reltype`/`id`.

## Verification
- `cargo check -p rustychickpeas-core` and `-p rustychickpeas-python`: clean.
- `cargo doc -p rustychickpeas-core --no-deps`: builds; CoWeight/RelationshipRef
  render correctly (no new intra-doc-link warnings).
- grep: no "edge" in core/python `src/` outside the "edge case" idiom.
