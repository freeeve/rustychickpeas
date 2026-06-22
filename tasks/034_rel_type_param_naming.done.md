# 034 — One canonical relationship-type param name

## Finding
`rel` vs `rel_type`/`rel_types` split the API. The `*_via`/`co_occurring`/
`neighbor_groups`/`dijkstra` set used singular `rel`; the older neighbor
primitives and the builder use `rel_type`/`rel_types`.

## Done (Python kwarg rename — the API-visible part)
Renamed the singular `rel` kwarg → `rel_type` (plural list params stay
`rel_types`) on every Python method that had it: `roots_via`, `root_via`,
`neighbor_via`, `fold_via`, `dijkstra`, `neighbor_groups`, `co_occurring`, plus
the internal `NeighborGroups.rel` field. Updated bodies, pyo3 signatures, and
docstring param-references. Left `rel` untouched where it means a relationship
*instance* (`for rel in rels`, the `RelationshipRef` closure var) or appears as
prose ("per-rel callback", "the rel count").

## Note
Core param names (`fold_via`/`co_occurring`/`neighbor_groups` take `rel: &str`)
are NOT part of the Rust API (callers are positional), so the core rename is
folded into **035**, where those same signatures change type to
`impl RelTypeFilter`. End state after 034+035: `rel_type` everywhere.

## Verification
- `cargo clippy -p rustychickpeas-python --all-targets -- -D warnings`: clean.
- No test used the old `rel=` kwarg (all positional), so nothing broke.
- `maturin develop`; full suite `-k "not benchmark"`: 270 passed, incl. new
  `test_via_family_rel_type_kwarg` exercising `roots_via(rel_type=...)` etc.
