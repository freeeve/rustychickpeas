# 032 — set_relationship_prop_* must not silently drop (core)

## Finding
`core/graph_builder.rs` rel-prop setters (`set_relationship_prop_{str,i64,f64,
bool}`) return `()` and silently no-op when the `(u,v,rel_type)` isn't found,
while the node setters return `Result<(), GraphError>`. A mistyped rel type drops
the property with zero signal (documented by
`test_set_relationship_property_nonexistent_rel`). The bulk
`set_relationship_props` reports a matched-count `usize`, so the two paths
disagree on the same failure mode.

## Proposed change
Singular rel-prop setters return `Result<(), GraphError>` (RelNotFound), aligning
with the node setters. Python wrapper raises on Err. Bulk keeps its count.

## Sign-off: REQUIRED (core contract change). Note the failure-mode rationale.

## Verification
- New core test: setting a prop on a missing rel returns Err.
- Python test asserts the raise; pytest + cargo test green.
