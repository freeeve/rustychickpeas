# 037 — Generic core set_prop + deprecate type-suffixed mirrors (sign-off)

## Finding
Python has clean auto-typed `set_prop`/`update_prop`/`set_relationship_prop`, so
the `*_i64/f64/str/bool` forms are deprecation candidates. But core's node side
has NO generic `set_prop<V: IntoValueIdBuilder>` (only the rel side has a generic
path via `set_relationship_props_by_index`). `i64_column` → `column()` is NOT a
drop-in (`column()` returns a `Column`, not a list).

## Proposed change
- Add core generic `set_prop<V: IntoValueIdBuilder>` (node) for parity.
- Mark the type-suffixed Python mirrors `set_prop_*`/`update_prop_*`/
  `set_relationship_prop_*` as deprecated (keep working).
- Document the `i64_column` → `list(memoryview(g.column(k)))` migration; do NOT
  remove `i64_column` in this task.

## Sign-off: REQUIRED.

## Verification: generic core setter test; deprecation visible; pytest +
cargo test green.
