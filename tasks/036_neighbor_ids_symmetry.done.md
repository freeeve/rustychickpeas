# 036 — builder/snapshot neighbor_ids symmetry

## Finding
Builder `neighbor_ids(node) -> (outgoing, incoming)` (no direction); snapshot
`neighbor_ids(node, direction) -> Vec`. Same name, different signature and return
shape across the builder->snapshot transition.

## Done
- Core builder: `neighbor_ids(node, direction: Direction) -> Vec<NodeId>` (was
  `(Vec, Vec)`), matching `GraphSnapshot::neighbor_ids`. `Outgoing` / `Incoming` /
  `Both` (out then in).
- Python builder: `neighbor_ids(node_id, direction=Direction.Outgoing) -> list`
  (default Outgoing, like the snapshot's Python method); `direction.into()` to
  core.
- Updated the two core tests and the Python builder test to the new shape.

## Verification
- `cargo clippy -p rustychickpeas-core -p rustychickpeas-python --all-targets
  -- -D warnings`: clean. Core neighbor tests pass.
- `maturin develop`; test_graph_snapshot_builder.py: 39 passed.
