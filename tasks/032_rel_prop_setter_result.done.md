# 032 — set_relationship_prop_* must not silently drop (core)

## Finding
`core/graph_builder.rs` rel-prop setters (`set_relationship_prop_{str,i64,f64,
bool}`) returned `()` and silently no-op'd when the `(u,v,rel_type)` wasn't found,
while the node setters return `Result<(), GraphError>`. A mistyped rel type
dropped the property with zero signal.

## Done
- `error.rs`: added `GraphError::RelationshipNotFound(u32, u32, String)`.
- `graph_builder.rs`: the four `set_relationship_prop_*` now return
  `Result<(), GraphError>` — `Err(RelationshipNotFound(u, v, rel_type))` on a
  missing rel, else `Ok(())`. `set_relationship_props_by_index` (index-based) and
  bulk `set_relationship_props` (matched-count `usize`) unchanged.
- Python wrapper (`graph_snapshot_builder.rs`): typed + auto-typed
  (`set_relationship_prop`) + bulk (`set_relationship_props`) setters map `Err →
  PyValueError`, so Python raises instead of silently dropping (matches how
  `relationships_with_type` already raises `ValueError` for an unknown rel type).
- Tests: flipped `test_set_relationship_property_nonexistent_rel` to assert the
  `Err`; added Python `test_set_relationship_prop_missing_rel_raises` (typed,
  auto-typed, bulk). Fixed all in-repo Rust call sites (all in `#[cfg(test)]`).

## Follow-up
The separate **LDBC repo** calls these setters and will need a pass to handle the
new `Result` (out of scope here).

## Verification
- `cargo clippy -p rustychickpeas-core --all-targets -- -D warnings`: clean
  (no `unused_must_use`). Core lib: 322 passed.
- `maturin develop`; full python suite `-k "not benchmark"`: 269 passed.
