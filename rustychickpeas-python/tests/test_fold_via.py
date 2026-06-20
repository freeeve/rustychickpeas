"""Tests for GraphSnapshot.fold_via (one-mode / bipartite projection of a relation)."""

from rustychickpeas import GraphSnapshotBuilder, Direction


def _interaction_graph():
    # Persons 0..2, Messages 3..6. hasCreator points creator -> message (a message's
    # creator is its Incoming hasCreator neighbor, as the LDBC loader stores it);
    # replyOf points reply -> parent. Contiguous ids (fold_via projects via a NodeArray
    # indexed by node id).
    b = GraphSnapshotBuilder()
    for nid, label in [(0, "Person"), (1, "Person"), (2, "Person")]:
        b.add_node([label], node_id=nid)
    for nid in (3, 4, 5, 6):
        b.add_node(["Msg"], node_id=nid)
    for creator, msg in [(0, 3), (1, 4), (0, 5), (2, 6)]:
        b.add_relationship(creator, msg, "hasCreator")
    # reply -> parent; creator-pair each folds to:
    #   4->3 (1,0)  5->3 (0,0 self, skip)  6->4 (2,1)  6->3 (2,0)  5->4 (0,1)
    for reply, parent in [(4, 3), (5, 3), (6, 4), (6, 3), (5, 4)]:
        b.add_relationship(reply, parent, "replyOf")
    return b.finalize()


def test_fold_via_counts():
    g = _interaction_graph()
    creators = g.neighbor_via("hasCreator", Direction.Incoming)
    interaction = g.fold_via("replyOf", Direction.Outgoing, creators)
    assert interaction == {(0, 1): 2, (1, 2): 1, (0, 2): 1}  # (0,0) self-pair excluded


def test_fold_via_keys_are_unordered():
    g = _interaction_graph()
    creators = g.neighbor_via("hasCreator", Direction.Incoming)
    interaction = g.fold_via("replyOf", Direction.Outgoing, creators)
    # Every pair key is (min, max) — no (1, 0) or (2, 1) appear.
    assert all(a < b for (a, b) in interaction)


def test_fold_via_unknown_rel_empty():
    g = _interaction_graph()
    creators = g.neighbor_via("hasCreator", Direction.Incoming)
    assert g.fold_via("nope", Direction.Outgoing, creators) == {}
