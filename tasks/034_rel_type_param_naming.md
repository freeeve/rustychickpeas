# 034 — One canonical relationship-type param name (sign-off)

## Finding
`rel` vs `rel_type`/`rel_types` splits the API on both sides. The `*_via`/
`co_occurring`/`neighbor_groups`/`dijkstra` set uses `rel`; older neighbor
primitives (`has_rel`, `degree`, `neighbor_ids`, `neighborhood`) and the builder
(`add_relationship(rel_type)`) use `rel_type`/`rel_types`. Same string, two names.

## Proposed change (to agree)
Standardize on `rel_type` (precise — a type, not an instance; matches the builder
+ core `Rel`/`RelLoadSpec`). Singular `rel_type` (required) vs plural `rel_types`
(optional list). Alias `rel` where already shipped (Python kw back-compat).

## Sign-off: REQUIRED (renames shipped public params). Sequence after 033 so the
*_via methods are renamed once.

## Verification: tests updated; aliases accepted; pytest + cargo test green.
