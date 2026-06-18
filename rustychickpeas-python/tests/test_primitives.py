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
