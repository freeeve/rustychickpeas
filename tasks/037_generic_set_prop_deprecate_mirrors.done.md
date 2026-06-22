# 037 — Generic set_prop + remove the type-suffixed mirrors

## Decision
Given "no aliases" for 0.10.0: **remove** the redundant type-suffixed Python
setters (the generic auto-typed forms cover every case), and add the missing
generic core node `set_prop` for Rust-side symmetry.

## Done
- Core: added generic `GraphBuilder::set_prop<V: IntoValueIdBuilder>(node, key,
  value)` (dispatches on the value's `ValueId` variant to the typed columns; same
  error contract as the typed setters). Core typed setters retained — the Python
  generics dispatch to them. Added `test_set_prop_generic`.
- Python: removed the 12 type-suffixed wrappers — `set_prop_{str,i64,f64,bool}`,
  `update_prop_{str,i64,f64,bool}`, `set_relationship_prop_{str,i64,f64,bool}`.
  Kept the generic `set_prop` / `update_prop` / `set_relationship_prop` (+ bulk
  `set_relationship_props`).
- Converted ~19 test call sites across test_rels_with_props/test_analytics/
  test_graph_snapshot_builder/test_graph_snapshot to the generic setters
  (name-only change; the generic auto-detects type).

## Note
`i64_column` -> `column()` is NOT part of this task (column() returns a Column,
not a list); left as-is per the 035-era note.

## Verification
- `cargo clippy -p rustychickpeas-core -p rustychickpeas-python --all-targets
  -- -D warnings`: clean. Core: `test_set_prop_generic` passes.
- `maturin develop`; full suite `-k "not benchmark"`: 270 passed.
