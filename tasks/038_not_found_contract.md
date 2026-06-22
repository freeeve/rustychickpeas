# 038 — Unify the not-found contract (None vs exception) (sign-off)

## Finding
No single rule for an absent key/label. `get_property("missing")` raises;
`column`/`prop_str`/`first_neighbor`/`co_occurring` return None/`{}`. A caller
can't predict try/except vs `is None` (py/graph_snapshot.rs + node.rs).

## Proposed change (to agree)
Value-returning lookups (`get_property`, and ideally `nodes_with_label`/
`nodes_with_property`) return None/empty on unknown key/label (à la `dict.get`);
reserve exceptions for out-of-range node ids (a programming error).

## Sign-off: REQUIRED (semantics change to get_property et al.).

## Verification: tests assert None on unknown key; out-of-range still raises;
pytest green.
