"""Tests for Python binding ergonomics: snapshot iteration, stable hashes,
and big-int overflow error messages."""

import pytest
from rustychickpeas import Direction, RustyChickpeas


def build_snapshot(n_nodes=3):
    """Build a small snapshot with n_nodes nodes and a couple of relationships"""
    manager = RustyChickpeas()
    builder = manager.create_builder()
    for i in range(n_nodes):
        builder.add_node(["Person"], node_id=i)
        builder.set_prop(i, "name", f"node{i}")
    for i in range(n_nodes - 1):
        builder.add_relationship(i, i + 1, "KNOWS")
    builder.set_version("test")
    builder.finalize_into(manager)
    return manager.graph_snapshot("test")


class TestSnapshotIteration:
    """Iterating a GraphSnapshot yields node IDs"""

    def test_iter_yields_expected_ids(self):
        snapshot = build_snapshot(5)
        assert list(snapshot) == [0, 1, 2, 3, 4]

    def test_iter_matches_len(self):
        snapshot = build_snapshot(7)
        assert len(list(snapshot)) == len(snapshot)

    def test_iter_ids_are_valid_for_node(self):
        """Every yielded ID must be accepted by node()"""
        snapshot = build_snapshot(4)
        for node_id in snapshot:
            node = snapshot.node(node_id)
            assert node.id() == node_id

    def test_iter_is_restartable(self):
        """Each __iter__ call returns a fresh iterator"""
        snapshot = build_snapshot(3)
        assert list(snapshot) == list(snapshot) == [0, 1, 2]

    def test_iter_independent_iterators(self):
        snapshot = build_snapshot(3)
        it1 = iter(snapshot)
        it2 = iter(snapshot)
        assert next(it1) == 0
        assert next(it1) == 1
        assert next(it2) == 0

    def test_iter_exhaustion(self):
        snapshot = build_snapshot(2)
        it = iter(snapshot)
        assert next(it) == 0
        assert next(it) == 1
        with pytest.raises(StopIteration):
            next(it)


class TestStableHash:
    """Node and Relationship hashes are deterministic across builds/releases"""

    # splitmix64(id): literal values pinned so any hasher change fails this test
    EXPECTED_NODE_HASHES = {
        0: -2152535657050944081,
        1: -7995527694508729151,
        2: -7541218347953203506,
    }

    def test_node_hash_stable_literals(self):
        snapshot = build_snapshot(3)
        for node_id, expected in self.EXPECTED_NODE_HASHES.items():
            assert hash(snapshot.node(node_id)) == expected

    def test_relationship_hash_stable_literals(self):
        snapshot = build_snapshot(3)
        # Relationship hash is splitmix64 of the CSR rel_index
        rel0 = snapshot.relationship(0)
        rel1 = snapshot.relationship(1)
        assert hash(rel0) == -2152535657050944081
        assert hash(rel1) == -7995527694508729151

    def test_equal_nodes_have_equal_hashes(self):
        snapshot = build_snapshot(3)
        a = snapshot.node(1)
        b = snapshot.node(1)
        assert a == b
        assert hash(a) == hash(b)

    def test_unequal_nodes_have_different_hashes(self):
        snapshot = build_snapshot(3)
        assert hash(snapshot.node(0)) != hash(snapshot.node(1))

    def test_equal_relationships_have_equal_hashes(self):
        snapshot = build_snapshot(3)
        a = snapshot.relationship(0)
        b = snapshot.relationship(0)
        assert a == b
        assert hash(a) == hash(b)

    def test_nodes_usable_in_sets(self):
        snapshot = build_snapshot(3)
        s = {snapshot.node(0), snapshot.node(0), snapshot.node(1)}
        assert len(s) == 2


class TestBigIntOverflow:
    """Out-of-range Python ints produce a specific overflow error"""

    def test_big_int_error_mentions_range(self):
        snapshot = build_snapshot(2)
        with pytest.raises(OverflowError) as exc_info:
            snapshot.nodes_with_property("Person", "name", 2**100)
        msg = str(exc_info.value)
        assert "i64" in msg or "range" in msg

    def test_negative_big_int_error_mentions_range(self):
        snapshot = build_snapshot(2)
        with pytest.raises(OverflowError) as exc_info:
            snapshot.nodes_with_property("Person", "name", -(2**100))
        msg = str(exc_info.value)
        assert "i64" in msg or "range" in msg

    def test_in_range_int_still_works(self):
        snapshot = build_snapshot(2)
        # Max i64 should not raise OverflowError (no match is fine)
        result = snapshot.nodes_with_property("Person", "name", 2**63 - 1)
        assert result == []

    def test_non_scalar_still_type_error(self):
        snapshot = build_snapshot(2)
        with pytest.raises(TypeError):
            snapshot.nodes_with_property("Person", "name", [1, 2])


class TestBfsWithCallbackAfterGilRelease:
    """BFS releases the GIL; Python filter callbacks must still work"""

    def test_bfs_with_python_node_filter(self):
        snapshot = build_snapshot(5)

        def node_filter(node_id):
            return node_id != 3

        nodes, _ = snapshot.bfs([0], Direction.Outgoing, node_filter=node_filter)
        assert 3 not in nodes
        assert 0 in nodes

    def test_bfs_filter_exception_propagates(self):
        snapshot = build_snapshot(5)

        def bad_filter(node_id):
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            snapshot.bfs([0], Direction.Outgoing, node_filter=bad_filter)

    def test_bidirectional_bfs_with_python_filters(self):
        snapshot = build_snapshot(5)

        def node_filter(node_id):
            return True

        def rel_filter(from_node, to_node, rel_type, csr_pos):
            return rel_type == "KNOWS"

        nodes, _ = snapshot.bidirectional_bfs(
            [0], [4], Direction.Outgoing,
            node_filter=node_filter, rel_filter=rel_filter,
        )
        assert 0 in nodes
        assert 4 in nodes
