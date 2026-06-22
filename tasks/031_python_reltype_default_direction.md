# 031 — Python: rel_type() accessor + default direction=Outgoing

Two non-breaking Pythonic ergonomics fixes. Python-only.

## Findings
- `relationship.rs:67` `reltype()` is the one non-snake_case, non-`rel_type`-
  aligned name. Rename to `rel_type()`; keep `reltype` as a deprecated alias.
- Read-side neighbor methods (`neighbors`, `neighbor_ids`, `relationships`,
  `degree`, and the Node mirrors) require an explicit `direction`. Default it to
  `Direction.Outgoing` (cf. Gremlin `out()`, Cypher `-->`, and `shortest_path`'s
  existing `direction=Both` default).

## Sign-off: not required (additive default + aliased rename).

## Verification
- pytest green; a call with no direction returns outgoing neighbors.
- `rel.rel_type()` works; `rel.reltype()` still works (deprecated).
