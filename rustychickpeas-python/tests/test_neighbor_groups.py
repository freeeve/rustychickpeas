"""Tests for the GraphSnapshot.neighbor_groups builder (project + sizes/top_by_size)."""

from rustychickpeas import GraphSnapshotBuilder, Direction

# project a member to its country: member -> city -> country
_PROJECT = [(Direction.Outgoing, "isLocatedIn"), (Direction.Outgoing, "isPartOf")]


def _build():
    """Two countries (0,1); cities 2->0, 3->1; persons 4,5 in country 0, 6 in
    country 1. Forum 7 has members in both countries (largest cohort 2); forums 8,9
    each have one (cohort 1). Forums carry an "fid" for the tie-break test."""
    b = GraphSnapshotBuilder()
    for nid in range(10):
        b.add_node(["N"], node_id=nid)
    for u, v, rel in [
        (2, 0, "isPartOf"), (3, 1, "isPartOf"),
        (4, 2, "isLocatedIn"), (5, 2, "isLocatedIn"), (6, 3, "isLocatedIn"),
        (7, 4, "hasMember"), (7, 5, "hasMember"), (7, 6, "hasMember"),
        (8, 6, "hasMember"), (9, 4, "hasMember"),
    ]:
        b.add_relationship(u, v, rel)
    b.set_prop(7, "fid", 100)
    b.set_prop(8, "fid", 20)
    b.set_prop(9, "fid", 10)
    return b.finalize()


def test_sizes_largest_cohort_per_source():
    g = _build()
    ng = g.neighbor_groups([7, 8, 9], "hasMember", Direction.Outgoing).project(_PROJECT)
    assert ng.sizes() == [(7, 2), (8, 1), (9, 1)]


def test_top_by_size_tie_optional():
    g = _build()
    ng = g.neighbor_groups([7, 8, 9], "hasMember", Direction.Outgoing).project(_PROJECT)
    # No tie key: ties (forums 8,9 both size 1) break by source id ascending.
    assert ng.top_by_size(3) == [(7, 2), (8, 1), (9, 1)]
    assert ng.top_by_size(2) == [(7, 2), (8, 1)]
    # Tie by the "fid" property: among size-1 forums, fid 10 (node 9) precedes fid 20 (node 8).
    assert ng.top_by_size(3, tie="fid") == [(7, 2), (9, 1), (8, 1)]
    assert ng.top_by_size(2, tie="fid") == [(7, 2), (9, 1)]


def test_no_projection_groups_by_neighbor_identity():
    # Without .project(), each neighbor is its own group -> every cohort is 1.
    g = _build()
    ng = g.neighbor_groups([7], "hasMember", Direction.Outgoing)
    assert ng.sizes() == [(7, 1)]


def test_unknown_rel_type_yields_zero():
    g = _build()
    ng = g.neighbor_groups([7, 8], "nope", Direction.Outgoing).project(_PROJECT)
    assert ng.sizes() == [(7, 0), (8, 0)]
