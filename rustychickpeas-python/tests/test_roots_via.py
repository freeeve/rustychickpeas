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


def test_via_family_rel_type_kwarg():
    # The relationship-type arg is the canonical `rel_type` keyword across the family.
    g = _thread()
    assert [g.roots_via(rel_type="replyOf", direction=Direction.Outgoing)[i] for i in range(4)] == [
        0,
        0,
        0,
        3,
    ]
    assert g.root_via(2, rel_type="replyOf", direction=Direction.Outgoing) == 0
    assert g.neighbor_via(rel_type="replyOf", direction=Direction.Outgoing)[2] == 1  # one hop


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


def test_neighbor_via():
    # P0 created M2(Post), P1 created M3(Comment); M4(Post) has no creator. The
    # one-hop functional neighbor (message -> creator); no-neighbor -> u32::MAX.
    b = GraphSnapshotBuilder()
    for nid, label in [(0, "Person"), (1, "Person"), (2, "Post"), (3, "Comment"), (4, "Post")]:
        b.add_node([label], node_id=nid)
    b.add_relationship(0, 2, "hasCreator")
    b.add_relationship(1, 3, "hasCreator")
    g = b.finalize()
    none = 0xFFFFFFFF
    cr = g.neighbor_via("hasCreator", Direction.Incoming)
    assert [cr[i] for i in range(5)] == [none, none, 0, 1, none]
    assert list(memoryview(cr)) == [none, none, 0, 1, none]
    assert g.neighbor_via("nope", Direction.Incoming) is None
