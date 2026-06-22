# 033 — Unify the *_via family across core and Python (sign-off)

## Finding
Python: `roots_via(rel, direction)` / `root_via(node, rel, direction)` /
`neighbor_via(rel, direction)` (py/graph_snapshot.rs:474/488/499).
Core: `chain_roots(direction, rel_type)` / `chain_root(...)`
(core/graph_snapshot.rs:2150/2175) — different name AND flipped param order; no
core `neighbor_via`. `fold_via`/`co_occurring`/`neighbor_groups` already match on
both sides. CLAUDE.md's primitive exercise cites "prefer roots_via over
chain_roots" as prior art (Oracle CONNECT_BY_ROOT; successor/predecessor).

## Proposed change (to agree)
- Rename core `chain_roots`→`roots_via`, `chain_root`→`root_via` (deprecated
  aliases for one release).
- Decide the core home/name for `neighbor_via` (Python builds it today).
- Unify param order to `(node?, rel, direction)` across both layers.

## Sign-off: REQUIRED. Run the primitive exercise before editing core.

## Verification: core + python tests; `roots_via` usable from Rust; aliases warn.
