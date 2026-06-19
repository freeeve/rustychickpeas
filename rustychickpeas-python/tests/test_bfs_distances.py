"""Tests for GraphSnapshot.bfs_distances (bounded hop-distance map)."""

from rustychickpeas import GraphSnapshotBuilder, Direction


def _line_graph():
    # 0 -> 1 -> 2 -> 3 via directed "knows"
    b = GraphSnapshotBuilder()
    for nid in range(4):
        b.add_node(["Person"], node_id=nid)
    for u, v in [(0, 1), (1, 2), (2, 3)]:
        b.add_relationship(u, v, "knows")
    return b.finalize()


def test_bfs_distances_basic():
    g = _line_graph()
    assert g.bfs_distances(0, Direction.Outgoing, rel_types=["knows"]) == {0: 0, 1: 1, 2: 2, 3: 3}


def test_bfs_distances_max_depth():
    g = _line_graph()
    d = g.bfs_distances(0, Direction.Outgoing, rel_types=["knows"], max_depth=2)
    assert d == {0: 0, 1: 1, 2: 2}


def test_bfs_distances_rel_type_filter():
    g = _line_graph()
    # No "likes" edges, so only the start node is reachable.
    assert g.bfs_distances(0, Direction.Outgoing, rel_types=["likes"]) == {0: 0}
