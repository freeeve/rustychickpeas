"""Tests for the whole-graph analytics bindings (pagerank / wcc / cdlp / lcc /
sssp) — thin PyO3 wrappers over the core GraphSnapshot methods. Outputs are
node-indexed lists. Mirrors rustychickpeas-core/src/analytics.rs tests."""

import math

from rustychickpeas import GraphSnapshotBuilder


def _build(n, rels):
    b = GraphSnapshotBuilder(capacity_nodes=n + 1, capacity_rels=1)
    for i in range(n):
        b.add_node(["V"], node_id=i)
    for u, v in rels:
        b.add_relationship(u, v, "e")
    return b.finalize()


def _build_weighted(n, rels):
    b = GraphSnapshotBuilder(capacity_nodes=n + 1, capacity_rels=1)
    for i in range(n):
        b.add_node(["V"], node_id=i)
    for u, v, w in rels:
        b.add_relationship(u, v, "e")
        b.set_relationship_prop_f64(u, v, "e", "weight", w)
    return b.finalize()


def test_sssp_weighted_and_unweighted():
    g = _build_weighted(4, [(0, 1, 2.0), (1, 2, 3.0), (0, 2, 10.0)])
    d = g.sssp(0, True, "weight")
    assert abs(d[0]) < 1e-9 and abs(d[1] - 2.0) < 1e-9 and abs(d[2] - 5.0) < 1e-9
    assert math.isinf(d[3])
    # unit weights (weight_key=None): the direct 0->2 hop wins at distance 1.
    du = g.sssp(0, True, None)
    assert abs(du[2] - 1.0) < 1e-9


def test_wcc_label_is_min_node_id():
    g = _build(5, [(0, 1), (1, 2), (3, 4)])
    assert g.wcc() == [0, 0, 0, 3, 3]


def test_pagerank_uniform_cycle_and_sink():
    g = _build(3, [(0, 1), (1, 2), (2, 0)])
    pr = g.pagerank(True, 0.85, 30)
    assert all(abs(p - 1.0 / 3.0) < 1e-9 for p in pr)
    assert abs(sum(pr) - 1.0) < 1e-9
    g2 = _build(2, [(0, 1)])
    pr2 = g2.pagerank(True, 0.85, 1)
    assert abs(pr2[0] - 0.2875) < 1e-9 and abs(pr2[1] - 0.7125) < 1e-9


def test_cdlp_default_and_seeded():
    g = _build(3, [(0, 1), (1, 2), (2, 0)])
    assert g.cdlp(False, 2) == [0, 0, 0]
    assert g.cdlp(False, 2, [10, 20, 30]) == [10, 10, 10]


def test_lcc_triangle_with_pendant():
    g = _build(4, [(0, 1), (1, 2), (2, 0), (0, 3)])
    c = g.lcc(False)
    assert abs(c[0] - 1.0 / 3.0) < 1e-9
    assert abs(c[1] - 1.0) < 1e-9 and abs(c[2] - 1.0) < 1e-9
    assert abs(c[3]) < 1e-9


def test_co_occurring_count_and_distinct():
    from rustychickpeas import Direction

    # Entities A=0,B=1,C=2; works 3,4,5 (work -about-> entity), each with a `day`.
    b = GraphSnapshotBuilder(capacity_nodes=6, capacity_rels=6)
    for i in range(6):
        b.add_node(["V"], node_id=i)
    for w, e in [(3, 0), (3, 1), (4, 0), (4, 1), (5, 0), (5, 2)]:
        b.add_relationship(w, e, "about")
    b.set_prop(3, "day", 10)
    b.set_prop(4, "day", 10)
    b.set_prop(5, "day", 20)
    g = b.finalize()

    counts = g.co_occurring(0, "about", Direction.Incoming)  # default weight="count"
    assert counts == {1: 2, 2: 1}  # B shares 2 works with A, C shares 1; seed excluded

    days = g.co_occurring(0, "about", Direction.Incoming, "distinct", "day")
    assert days == {1: 1, 2: 1}  # B's two works share day 10 -> 1 distinct day

    # unknown rel -> empty; distinct without a key -> error
    assert g.co_occurring(0, "nope", Direction.Incoming) == {}
    try:
        g.co_occurring(0, "about", Direction.Incoming, "distinct")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_aggregate_where_via():
    from rustychickpeas import Direction

    # Msgs 0,1 are roots (lang en/de); 2 replyOf 0, 3 replyOf 1. Scalar cols so all pass.
    b = GraphSnapshotBuilder(capacity_nodes=4, capacity_rels=2)
    for i in range(4):
        b.add_node(["Msg"], node_id=i)
        b.set_prop(i, "day", 5)
        b.set_prop(i, "content", 1)
        b.set_prop(i, "len", 10)
    b.set_prop(0, "lang", "en")
    b.set_prop(1, "lang", "de")
    b.add_relationship(2, 0, "replyOf")
    b.add_relationship(3, 1, "replyOf")
    g = b.finalize()

    roots = g.roots_via("replyOf", Direction.Outgoing)  # 0->0,1->1,2->0,3->1
    # Keep messages whose thread root's lang is "en": nodes 0 and 2.
    res = g.aggregate("Msg").where("day", ">", 0).where_via(roots, "lang", ["en"]).run()
    assert res.total == 2
