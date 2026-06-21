"""Tests for fold_via (one-mode / bipartite projection -> PairWeights) and the
native weighted dijkstra that consumes the resident PairWeights map."""

from rustychickpeas import GraphSnapshotBuilder, Direction


def _interaction_graph():
    # Persons 0..2 (+ isolated person 7), Messages 3..6. hasCreator points creator ->
    # message (a message's creator is its Incoming hasCreator neighbor, as the LDBC
    # loader stores it); replyOf points reply -> parent. knows rels (both directions)
    # let dijkstra run over the projected interaction graph. Contiguous ids (fold_via /
    # neighbor_via / dijkstra all index by node id).
    b = GraphSnapshotBuilder()
    for nid in (0, 1, 2, 7):
        b.add_node(["Person"], node_id=nid)
    for nid in (3, 4, 5, 6):
        b.add_node(["Msg"], node_id=nid)
    for creator, msg in [(0, 3), (1, 4), (0, 5), (2, 6)]:
        b.add_relationship(creator, msg, "hasCreator")
    # reply -> parent; creator-pair each folds to:
    #   4->3 (1,0)  5->3 (0,0 self, skip)  6->4 (2,1)  6->3 (2,0)  5->4 (0,1)
    for reply, parent in [(4, 3), (5, 3), (6, 4), (6, 3), (5, 4)]:
        b.add_relationship(reply, parent, "replyOf")
    # knows among persons (both directions); 7 knows 0 but never interacted.
    for u, v in [(0, 1), (1, 2), (0, 2), (0, 7)]:
        b.add_relationship(u, v, "knows")
        b.add_relationship(v, u, "knows")
    return b.finalize()


def _interaction(g):
    return g.fold_via("replyOf", Direction.Outgoing,
                      g.neighbor_via("hasCreator", Direction.Incoming))


def test_fold_via_counts():
    pw = _interaction(_interaction_graph())
    assert pw.to_dict() == {(0, 1): 2, (1, 2): 1, (0, 2): 1}  # (0,0) self-pair excluded


def test_pairweights_dict_like():
    pw = _interaction(_interaction_graph())
    assert len(pw) == 3
    assert (0, 1) in pw and (1, 0) in pw          # keys are unordered
    assert (0, 3) not in pw
    assert pw[(1, 0)] == 2                          # normalized to (min, max)
    assert pw.get(0, 2) == 1
    assert pw.get(9, 9) is None
    assert pw.get(9, 9, 0) == 0
    assert all(a < b for (a, b) in pw.to_dict())   # every key is (min, max)


def test_fold_via_unknown_rel_empty():
    g = _interaction_graph()
    pw = g.fold_via("nope", Direction.Outgoing,
                    g.neighbor_via("hasCreator", Direction.Incoming))
    assert len(pw) == 0
    assert pw.to_dict() == {}


def test_dijkstra_prune_missing():
    # Q19 mode: cost(u,v) = 1/interaction; pairs that never interacted are untraversable.
    g = _interaction_graph()
    dist = g.dijkstra(0, Direction.Outgoing, "knows",
                      weights=_interaction(g), base=0.0, prune_missing=True)
    # 0->1 = 1/2; 0->2 = min(1/1 direct, 1/2 + 1/1) = 1.0; person 7 pruned (no interaction).
    assert dist == {0: 0.0, 1: 0.5, 2: 1.0}


def test_dijkstra_base_no_prune():
    # Q15 mode: cost(u,v) = 1/(w+1); every knows rel traversable (absent -> 1/1).
    g = _interaction_graph()
    dist = g.dijkstra(0, Direction.Outgoing, "knows",
                      weights=_interaction(g), base=1.0, prune_missing=False)
    assert abs(dist[0]) < 1e-12
    assert abs(dist[1] - 1 / 3) < 1e-12        # 1/(2+1)
    assert abs(dist[2] - 0.5) < 1e-12            # 1/(1+1) direct beats 1/3 + 1/2
    assert abs(dist[7] - 1.0) < 1e-12            # absent pair -> 1/(0+1)


def test_dijkstra_target_early_exit():
    g = _interaction_graph()
    dist = g.dijkstra(0, Direction.Outgoing, "knows",
                      weights=_interaction(g), base=0.0, prune_missing=True, target=2)
    assert abs(dist[2] - 1.0) < 1e-12
