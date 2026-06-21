"""Read-primitive bindings exposed in task 143:
has_label, has_rel, has_neighbor_with_property, node_with_label_property,
first_neighbor, follow, prop_str, neighborhood, i64_column.
"""

from rustychickpeas import RustyChickpeas, Direction

O = Direction.Outgoing
I = Direction.Incoming


def chain():
    """0 -KNOWS-> 1 -KNOWS-> 2; each Person has name=node{i} and age=30+i."""
    m = RustyChickpeas()
    b = m.create_builder()
    for i in range(3):
        b.add_node(["Person"], node_id=i)
        b.set_prop(i, "name", f"node{i}")
        b.set_prop(i, "age", 30 + i)
    for i in range(2):
        b.add_relationship(i, i + 1, "KNOWS")
    b.set_version("t")
    b.finalize_into(m)
    return m.graph_snapshot("t")


def test_has_label():
    g = chain()
    assert g.has_label(0, "Person")
    assert not g.has_label(0, "Company")


def test_has_rel():
    g = chain()
    assert g.has_rel(0, O, "KNOWS")
    assert not g.has_rel(2, O, "KNOWS")  # leaf
    assert not g.has_rel(0, O, "LIKES")  # unknown type


def test_first_neighbor():
    g = chain()
    assert g.first_neighbor(0, O, "KNOWS") == 1
    assert g.first_neighbor(2, O, "KNOWS") is None
    assert g.first_neighbor(1, I, "KNOWS") == 0  # incoming


def test_follow():
    g = chain()
    assert g.follow(0, [(O, "KNOWS"), (O, "KNOWS")]) == 2
    assert g.follow(0, []) == 0  # no steps -> start
    assert g.follow(0, [(O, "LIKES")]) is None  # broken chain


def test_prop_str():
    g = chain()
    assert g.prop_str(0, "name") == "node0"
    assert g.prop_str(0, "missing") is None


def test_node_with_label_property():
    g = chain()
    assert g.node_with_label_property("Person", "name", "node1") == 1
    assert g.node_with_label_property("Person", "name", "nobody") is None


def test_has_neighbor_with_property():
    g = chain()
    assert g.has_neighbor_with_property(0, O, "KNOWS", "name", "node1")
    # node2 is 2 hops away, not a direct KNOWS neighbor of 0
    assert not g.has_neighbor_with_property(0, O, "KNOWS", "name", "node2")
    assert not g.has_neighbor_with_property(0, O, "KNOWS", "name", "nobody")


def test_neighborhood():
    g = chain()
    assert g.neighborhood(0, O, "KNOWS", 2) == [1, 2]
    assert g.neighborhood(0, O, "KNOWS", 1) == [1]
    assert g.neighborhood(0, O, "KNOWS", 2, 2) == [2]  # min_hops=2 -> only the 2-hop ring


def test_i64_column():
    g = chain()
    assert g.i64_column("age") == [30, 31, 32]
    assert g.i64_column("missing") is None
    assert g.i64_column("name") is None  # string column, not i64


def test_degree():
    g = chain()
    assert g.degree(0, O) == 1
    assert g.degree(1, Direction.Both) == 2  # 1 out (->2) + 1 in (0->)
    assert g.degree(2, O) == 0
    assert g.degree(0, O, "KNOWS") == 1  # typed
    assert g.degree(0, O, "LIKES") == 0


def test_neighbor_ids_rel_types():
    g = chain()
    assert g.neighbor_ids(0, O) == [1]
    assert g.neighbor_ids(1, O) == [2]
    assert g.neighbor_ids(0, O, ["KNOWS"]) == [1]
    assert g.neighbor_ids(0, O, ["LIKES"]) == []


def test_node_degree():
    g = chain()
    n0 = g.node(0)
    assert n0.degree(O) == 1
    assert n0.degree(O, "KNOWS") == 1
    assert n0.degree(O, "LIKES") == 0


def test_node_set():
    from rustychickpeas import NodeSet

    a = NodeSet([1, 2, 3])
    b = NodeSet([2, 3, 4])
    assert len(a) == 3
    assert 2 in a and 5 not in a
    assert (a & b).to_list() == [2, 3]
    assert (a | b).to_list() == [1, 2, 3, 4]
    assert (a - b).to_list() == [1]
    assert a.intersect(b).to_list() == [2, 3]
    assert sorted(a) == [1, 2, 3]  # __iter__ (sorted() consumes the iterable directly)
    assert NodeSet().is_empty() and not NodeSet()
    assert NodeSet([1])  # __bool__
    # composes with query results without a Python set()
    g = chain()
    persons = NodeSet(g.nodes_with_label("Person"))
    fof = NodeSet(g.neighborhood(0, O, "KNOWS", 2))
    assert (persons & fof).to_list() == [1, 2]


def test_relationships_incoming_via_in_to_out():
    # 0 -KNOWS-> 1 -KNOWS-> 2: node 1 has one incoming (from 0) and one outgoing (to 2).
    g = chain()
    n1 = g.node(1)
    inc = n1.relationships(Direction.Incoming)
    assert len(inc) == 1
    both = n1.relationships(Direction.Both)
    assert len(both) == 2
