# 038 — Unify the not-found contract (get_property)

## Decision
Scope to **get_property only** (lowest-risk): an unknown property key returns
`None` (dict.get semantics, matching `column`/`prop_str`/`co_occurring`).
`nodes_with_label`/`nodes_with_property` keep raising on an unknown label/key.

## Done
- `GraphSnapshot.get_property` (graph_snapshot.rs) and `Node.get_property`
  (node.rs): dropped the "Property key not found" `ValueError`; an unknown key now
  resolves to `None` (the underlying `prop()` already returns `None`). Out-of-range
  node ids likewise return `None`.
- `Relationship.get_property` and the builder's `get_property` already returned
  `None` — now consistent across all four.
- Updated the raise-expecting tests (test_graph_snapshot, test_node,
  test_rusty_chickpeas) to assert `is None`; simplified the rcpg topology-only
  test (no longer needs a `try/except ValueError`).
- `nodes_with_property`'s unknown-key/label raise tests are unchanged (still
  raise, per the decision).

## Verification
- `cargo clippy -p rustychickpeas-python -- -D warnings`: clean.
- `maturin develop`; affected files (graph_snapshot/node/rusty_chickpeas/
  relationship): 120 passed.
