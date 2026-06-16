# Bounded BFS hop-distances (LDBC opportunity C) — DONE

Added core (maintainer sign-off: `HashMap<NodeId,u32>` return shape):

```rust
pub fn bfs_distances(
    &self,
    start: NodeId,
    direction: Direction,
    rel_types: impl RelTypeFilter,
    max_depth: Option<u32>,
) -> HashMap<NodeId, u32>
```

Level-synchronous single-source BFS: returns each reached node -> hop
distance, `start` mapped to 0; `max_depth = Some(d)` bounds to distance
<= d, `None` reaches the whole component. Built on a shared private
`neighbors_with(node, dir, RelMatch)` helper (resolves the type filter
once). Core tests: `test_bfs_distances_hop_counts`,
`test_bfs_distances_max_depth_and_eccentricity`.

Wired in `rustychickpeas-ldbc`:
- Q10 `q10_experts` (faithful_c.rs) — replaced the hand-rolled
  level-frontier BFS with `bfs_distances(start, Outgoing, knows,
  Some(max_dist))`.
- `knows_reachability` (faithful_b.rs) — replaced the
  `dijkstra(.., |_,_| 1.0)` unit-weight stand-in; reach count =
  `dist.len()`-style filter, eccentricity = `dist.values().max()`.

Both single-source, so no lock/contention concerns. Value-identical:
all 23 SF1 result files byte-identical to baseline.
