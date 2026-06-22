# 031 — Python: rel_type() accessor + default direction=Outgoing

Two non-breaking Pythonic ergonomics fixes. Python-only.

## Done
- `relationship.rs`: renamed `reltype()` → `rel_type()` (snake_case, matches the
  `rel_type` param used everywhere else). Kept `reltype()` as a documented alias
  delegating to `rel_type()`, so existing callers still work. Updated `__repr__`
  to call `rel_type()`.
- Defaulted `direction=Direction::Outgoing` on the read-side neighbor methods so
  `g.neighbor_ids(n)` / `g.neighbors(n)` / `g.relationships(n)` / `g.degree(n)`
  and the `Node` mirrors (`node.neighbor_ids()`/`relationships()`/`degree()`)
  work without an explicit direction (cf. Gremlin `out()`; mirrors
  `shortest_path`'s existing `direction=Both` default). Params stay
  positional-or-keyword — explicit calls unchanged.

## Note
No runtime `DeprecationWarning` on `reltype()` — Rust `#[deprecated]` only warns
Rust callers, and the crate has no Python-warning machinery. The alias is
doc-deprecated; add a runtime warning later if desired.

## Verification
`maturin develop`; full suite `pytest -k "not benchmark"` — 268 passed, incl. new
`test_rel_type_alias_and_default_direction` (default-Outgoing on snapshot + Node
methods; `rel_type()` == `reltype()`).
