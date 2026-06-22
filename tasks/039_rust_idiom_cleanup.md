# 039 — Rust idiom cleanup (sign-off where signatures change)

Grab-bag of rustic-idiom fixes from the review.

## Findings + changes
- `with_version`/`with_version_builder` invert the `with_*` idiom: make
  `with_version(self, ..) -> Self` the chainable form (drop `_builder`), keep
  `set_version(&mut self)`. [breaking → sign-off]
- `node` vs `node_id` and `prop` vs `property` drift: converge on `node` + the
  short `prop` stem (`nodes_with_prop`, etc.); alias old. [breaking → sign-off]
- `Column::iter_entries -> Box<dyn Iterator>`: return `impl Iterator + '_`
  (drop the heap alloc / dyn dispatch). [non-breaking]
- `PropertyValue::as_string` panics via `.expect` when no interner is supplied:
  return `Option<String>` or document the precondition prominently. [decide]

## Sign-off: REQUIRED for the renames; the iterator + panic fixes are safe.

## Verification: cargo test green; `cargo doc` clean.
