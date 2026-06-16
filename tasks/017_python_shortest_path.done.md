# Expose weighted shortest paths in the Python API

Core has `GraphSnapshot::dijkstra` (single-source) and the new
`weighted_shortest_path` (point-to-point, bidirectional Dijkstra), but the
Python bindings only expose *unweighted* reachability
(`bfs`/`can_reach`/`bidirectional_bfs`) — there is no weighted shortest path
in Python. Both core methods take a Rust `weight` closure, which isn't
Python-friendly, so expose a property/unweighted variant:

- `snapshot.shortest_path(source, target, rel_types=None, direction=Both,
  weight_property=None) -> float | None` — point-to-point cost via
  `weighted_shortest_path`. When `weight_property` is given, read that f64
  edge property (via `relationship_property`) as the edge weight; otherwise
  every followed edge costs 1.0 (hop count). Returns `None` if unreachable.
- (Optional) `snapshot.shortest_path_lengths(source, rel_types=None,
  direction=Both, weight_property=None) -> dict[int, float]` — single-source
  distances via `dijkstra`.

Add tests to tests/test_graph_snapshot.py: reachable, unreachable,
unweighted hop-count, and weighted (a graph where the cheap edge differs
from the fewest-hops edge).
