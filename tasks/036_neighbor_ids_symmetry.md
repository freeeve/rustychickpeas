# 036 — builder/snapshot neighbor_ids symmetry (sign-off)

## Finding
Builder `neighbor_ids(node) -> (outgoing, incoming)` (no direction); snapshot
`neighbor_ids(node, direction) -> Vec<NodeId>`. Same name, different signature and
return shape across the builder→snapshot transition — a surprise when moving from
builder to snapshot.

## Proposed change
Give the builder's `neighbor_ids` a `direction` param (default `Both`) matching
the snapshot, returning a single list; keep the `(out, in)` tuple form only under
a distinct name if still needed.

## Sign-off: REQUIRED (builder signature change).

## Verification: builder/snapshot calls line up; cargo test + pytest green.
