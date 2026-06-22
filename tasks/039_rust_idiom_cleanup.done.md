# 039 — Rust idiom cleanup (contained parts)

Scope (per maintainer): the contained, low-risk parts only. The 109-site
`property`->`prop` / `node_id`->`node` naming convergence was **skipped** (pure
cosmetic, high churn, hard break with no aliases; `nodes_with_property` is already
clear and `node_id` is arguably more descriptive).

## Done
- **`with_version` idiom fix**: `with_version` was an associated *constructor*
  while the chainable consuming form was awkwardly named `with_version_builder`.
  Now `with_version(self, &str) -> Self` is the `with_*` builder form
  (`GraphBuilder::new(..).with_version(..)`); removed the constructor and the
  `with_version_builder` name; `set_version(&mut self)` unchanged. Updated all 5
  callers (manager, Python builder ctor, 2 tests, 1 integration test).
- **`PropertyValue::as_string` panic fix**: returned `String` and `.expect()`-
  panicked when an `InternedString` had no interner; now returns `Option<String>`
  (`None` only for that case). No production callers (tests only), so non-breaking
  in practice. Tests updated + a `None`-case test added.

## Not done (deliberate)
- **`Column::iter_entries` `Box<dyn Iterator>` -> `impl Iterator`**: kept boxed.
  A true `impl Iterator` needs a hand-written 11-variant enum iterator (~60 lines
  of boilerplate + bug risk) to avoid one tiny `Box` allocation per *call* on a
  cold path (lazy index build / geo queries / a test) — a poor trade for a
  Low-severity nit. Left as-is with this rationale.
- The `property`->`prop` / `node_id`->`node` convergence (109 sites) — skipped.

## Verification
- `cargo clippy -p rustychickpeas-core -p rustychickpeas-python --all-targets
  -- -D warnings`: clean. Core lib: 324 passed; rcpg integration test: 5 passed.
- `maturin develop`; versioned-builder tests (rusty_chickpeas + builder): 62 passed.
