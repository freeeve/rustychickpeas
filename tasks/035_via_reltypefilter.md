# 035 — fold_via/co_occurring/neighbor_groups take impl RelTypeFilter (core)

## Finding
These take `rel: &str` (core/graph_snapshot.rs:2588/2635/2557) while every other
traversal takes `impl RelTypeFilter` (accepts `&str` OR a pre-resolved
`RelationshipType`). The kernel re-does the string→id lookup per call; the Python
string arg means a resolved type can never reach the hot loop.

## Proposed change
Widen to `rel: impl RelTypeFilter` (resolve once internally via `into_match`).
Python surface unchanged (still passes a string). Pure perf/ergonomics on the
Rust side; no behavior change.

## Sign-off: REQUIRED (core signature change), but behavior is identical.

## Verification: cargo test green; a pre-resolved `RelationshipType` compiles
through fold_via; relevant bench unaffected or better.
