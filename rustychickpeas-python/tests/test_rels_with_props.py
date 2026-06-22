"""Tests for the bulk rel accessors rels_with_props (parallel lists) and rel_view
(zero-copy buffer arrays) — the property-bearing siblings of neighbor_ids."""

from rustychickpeas import GraphSnapshotBuilder, Direction


def _graph():
    # 0 -[transfer ts/amt]-> 1, 2 ; 3 -[transfer]-> 0 (incoming).
    b = GraphSnapshotBuilder()
    for nid in range(4):
        b.add_node(["Account"], node_id=nid)
    for u, v, ts, amt in [(0, 1, 100, 5.0), (0, 2, 200, 7.5), (3, 0, 50, 1.0)]:
        b.add_relationship(u, v, "transfer")
        b.set_relationship_prop_i64(u, v, "transfer", "ts", ts)
        b.set_relationship_prop_f64(u, v, "transfer", "amt", amt)
    return b.finalize()


def test_rels_with_props_outgoing():
    g = _graph()
    nbrs, (ts, amt) = g.rels_with_props(0, Direction.Outgoing, "transfer", ["ts", "amt"])
    assert list(zip(nbrs, ts, amt)) == [(1, 100, 5.0), (2, 200, 7.5)]


def test_rels_with_props_incoming():
    g = _graph()
    nbrs, (ts,) = g.rels_with_props(0, Direction.Incoming, "transfer", ["ts"])
    assert list(zip(nbrs, ts)) == [(3, 50)]  # neighbor = source of the incoming rel


def test_rels_with_props_matches_relationships():
    g = _graph()
    old = [(r.end_node().id(), r.get_property("ts"), r.get_property("amt"))
           for r in g.relationships(0, Direction.Outgoing, ["transfer"])]
    nbrs, (ts, amt) = g.rels_with_props(0, Direction.Outgoing, "transfer", ["ts", "amt"])
    assert list(zip(nbrs, ts, amt)) == old


def test_rel_view_buffers():
    g = _graph()
    v = g.rel_view(0, Direction.Outgoing, "transfer", ["ts", "amt"])
    assert len(v) == 2
    assert list(memoryview(v.neighbors)) == [1, 2]
    assert list(memoryview(v.col("ts"))) == [100, 200]
    assert memoryview(v.col("ts")).format == "q"
    assert list(memoryview(v.col("amt"))) == [5.0, 7.5]
    assert abs(sum(memoryview(v.col("amt"))) - 12.5) < 1e-9  # C-speed reduction
    assert v.col("missing") is None


def test_unknown_rel_type_empty():
    g = _graph()
    nbrs, cols = g.rels_with_props(0, Direction.Outgoing, "nope", ["ts"])
    assert nbrs == [] and cols == [[]]
    assert len(g.rel_view(0, Direction.Outgoing, "nope", ["ts"])) == 0


def test_set_relationship_prop_missing_rel_raises():
    """A property set on a non-existent rel now raises instead of silently
    dropping (typed, auto-typed, and bulk setters)."""
    import pytest

    b = GraphSnapshotBuilder()
    for nid in range(3):
        b.add_node(["Account"], node_id=nid)
    b.add_relationship(0, 1, "transfer")
    with pytest.raises(ValueError):
        b.set_relationship_prop_i64(0, 2, "transfer", "ts", 100)  # no 0->2 rel
    with pytest.raises(ValueError):
        b.set_relationship_prop_i64(0, 1, "nope", "ts", 100)  # wrong rel type
    with pytest.raises(ValueError):
        b.set_relationship_prop(0, 2, "transfer", "ts", 100)  # auto-typed
    with pytest.raises(ValueError):
        b.set_relationship_props(0, 2, "transfer", {"ts": 100})  # bulk
    # The existing rel still accepts properties.
    b.set_relationship_prop_i64(0, 1, "transfer", "ts", 100)
    nbrs, (ts,) = b.finalize().rels_with_props(0, Direction.Outgoing, "transfer", ["ts"])
    assert list(zip(nbrs, ts)) == [(1, 100)]
