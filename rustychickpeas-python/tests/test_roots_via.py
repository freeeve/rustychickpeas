"""Tests for GraphSnapshot.roots_via / root_via (functional-relation chain roots)."""

from rustychickpeas import GraphSnapshotBuilder, Direction


def _thread():
    # C2 -> C1 -> P0 via replyOf; P3 is its own root. Contiguous ids (roots_via
    # indexes by node id, as the real loader's contiguous ids do).
    b = GraphSnapshotBuilder()
    for nid, label in [(0, "Post"), (1, "Comment"), (2, "Comment"), (3, "Post")]:
        b.add_node([label], node_id=nid)
    b.add_relationship(1, 0, "replyOf")
    b.add_relationship(2, 1, "replyOf")
    return b.finalize()


def test_roots_via_getitem():
    roots = _thread().roots_via("replyOf", Direction.Outgoing)
    assert len(roots) == 4
    assert [roots[i] for i in range(4)] == [0, 0, 0, 3]  # everyone -> thread root; P3 itself


def test_roots_via_buffer():
    mv = memoryview(_thread().roots_via("replyOf", Direction.Outgoing))
    assert mv.format == "I"
    assert list(mv) == [0, 0, 0, 3]


def test_root_via_single():
    g = _thread()
    assert g.root_via(2, "replyOf", Direction.Outgoing) == 0
    assert g.root_via(0, "replyOf", Direction.Outgoing) == 0  # a terminal maps to itself
    assert g.root_via(3, "replyOf", Direction.Outgoing) == 3


def test_roots_via_unknown_rel():
    g = _thread()
    assert g.roots_via("nope", Direction.Outgoing) is None
    assert g.root_via(0, "nope", Direction.Outgoing) is None
