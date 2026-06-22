# 033 — Unify the *_via family across core and Python

## Finding
Python exposed `roots_via`/`root_via`/`neighbor_via`; core called the chain-root
concept `chain_roots`/`chain_root` (different name AND flipped param order), and
`neighbor_via` lived only in Python. `fold_via`/`co_occurring`/`neighbor_groups`
already matched on both sides.

## Done (clean rename, no aliases — 0.10.0 will be cut at the end)
- Core renames: `chain_roots`→`roots_via`, `chain_root`→`root_via`,
  `build_chain_roots`→`build_roots_via`, type `ChainRoots`→`RootsVia`, cache field
  `chain_root_index`→`roots_via_index` (across graph_snapshot.rs, serialize.rs,
  graph_builder.rs).
- Param order aligned to the family / Python: `roots_via(rel_type, direction)`,
  `root_via(node, rel_type, direction)` (node-first like `neighbors`). Internal
  cache key + private `build_roots_via` keep `(direction, rel_type)`.
- Added core `neighbor_via(rel_type, direction) -> RootsVia` (the depth-1 sibling
  of `roots_via`: one `first_neighbor` per node, `NodeId::MAX` sentinel). The
  Python `neighbor_via` now delegates to it (was an inline loop).
- Tests: core `test_neighbor_via_one_hop`; existing root_via tests updated to the
  new arg order.

## Verification
- `cargo clippy -p rustychickpeas-core --all-targets -- -D warnings`: clean.
  Core lib: 323 passed (incl. neighbor_via test).
- `maturin develop`; python via tests (roots_via/root_via/neighbor_via/fold_via)
  pass; full suite `-k "not benchmark"`: 269 passed. (Python method names were
  already roots_via/root_via/neighbor_via — surface unchanged; only core renamed.)

## Follow-up
The separate LDBC repo calls `chain_roots`/`chain_root` — needs the rename pass
there too (out of scope here), folded into the 0.10.0 cut.
