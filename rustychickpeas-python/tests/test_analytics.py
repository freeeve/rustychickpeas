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
    assert d[0] == 0.0 and d[1] == 2.0 and d[2] == 5.0
    assert math.isinf(d[3])
    # unit weights (weight_key=None): the direct 0->2 hop wins at distance 1.
    du = g.sssp(0, True, None)
    assert du[2] == 1.0


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
    assert c[3] == 0.0
