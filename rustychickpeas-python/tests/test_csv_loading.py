"""Tests for CSV bulk loading, including property-based relationship endpoints."""

import gzip
import os
import tempfile

import pytest

from rustychickpeas import GraphSnapshotBuilder, Direction, Ref, Rel, Prop


def _write(path, text):
    with open(path, "w") as f:
        f.write(text)


class TestMultiRelLoader:
    """load_relationships_from_csv_multi with Ref/Rel/Prop typed specs."""

    def test_multi_rel_one_pass(self):
        with tempfile.TemporaryDirectory() as d:
            nodes = os.path.join(d, "nodes.csv")
            posts = os.path.join(d, "posts.csv")
            _write(nodes, "id|label\n10|Person\n20|Person\n100|Post\n101|Post\n")
            # merged-FK file: each Post row carries its creator + a weight.
            _write(posts, "id|creator|w\n100|10|5\n101|20|7\n")

            b = GraphSnapshotBuilder()
            b.load_nodes_from_csv(
                nodes, label_columns=["label"], property_columns=["id"], delimiter="|"
            )
            counts = b.load_relationships_from_csv_multi(
                posts,
                [
                    Rel("hasCreator", Ref("creator", "Person"), Ref("id", "Post")),
                    Rel("weighted", Ref("id", "Post"), Ref("creator", "Person"),
                        props=[Prop("weight", "w", int)]),
                ],
                delimiter="|",
            )
            assert counts == [2, 2]
            g = b.finalize()
            assert g.relationship_count() == 4

            # Load order: Person 10->0, 20->1; Post 100->2, 101->3.
            # "weighted" goes Post -> Person and carries the renamed/typed prop.
            rels = g.relationships(2, Direction.Outgoing, ["weighted"])
            assert len(rels) == 1
            assert rels[0].get_property("weight") == 5  # renamed "w"->"weight", typed int

    def test_ref_rel_prop_repr(self):
        assert "Person" in repr(Ref("creator", "Person"))
        assert "hasCreator" in repr(Rel("hasCreator", Ref("a", "A"), Ref("b", "B")))
        assert "weight" in repr(Prop("weight", "w", int))

    def test_prop_bad_type_raises(self):
        with pytest.raises(TypeError, match="int, float, bool, or str"):
            Prop("x", "x", list)


class TestCsvStringEndpoints:
    """Relationships referenced by a direct id column (string endpoint)."""

    def test_load_nodes_and_relationships(self):
        with tempfile.TemporaryDirectory() as d:
            nodes = os.path.join(d, "nodes.csv")
            rels = os.path.join(d, "rels.csv")
            _write(
                nodes,
                "id,label,name\n0,Person,Alice\n1,Person,Bob\n2,Person,Carol\n",
            )
            _write(rels, "from,to,type\n0,1,KNOWS\n1,2,KNOWS\n")

            builder = GraphSnapshotBuilder()
            builder.load_nodes_from_csv(
                nodes,
                node_id_column="id",
                label_columns=["label"],
                property_columns=["name"],
            )
            pairs = builder.load_relationships_from_csv(
                rels, "from", "to", rel_type_column="type"
            )

            assert sorted(pairs) == [(0, 1), (1, 2)]
            snap = builder.finalize()
            assert snap.node_count() == 3
            assert snap.relationship_count() == 2
            assert snap.neighbor_ids(0, Direction.Outgoing) == [1]

    def test_gzip_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            nodes = os.path.join(d, "nodes.csv.gz")
            rels = os.path.join(d, "rels.csv.gz")
            with gzip.open(nodes, "wt") as f:
                f.write("id,label\n0,Person\n1,Person\n")
            with gzip.open(rels, "wt") as f:
                f.write("from,to,type\n0,1,KNOWS\n")

            builder = GraphSnapshotBuilder()
            builder.load_nodes_from_csv(nodes, node_id_column="id", label_columns=["label"])
            pairs = builder.load_relationships_from_csv(
                rels, "from", "to", rel_type_column="type"
            )
            assert pairs == [(0, 1)]


class TestCsvPropertyEndpoints:
    """Relationships referenced by a node property (LDBC-style: i64, colliding ids)."""

    def test_single_property_reference(self):
        with tempfile.TemporaryDirectory() as d:
            nodes = os.path.join(d, "nodes.csv")
            rels = os.path.join(d, "rels.csv")
            # External ids overflow u32 and collide across labels; internal ids auto-assigned.
            _write(
                nodes,
                "ext_id|label\n"
                "1000000000001|Comment\n"
                "1000000000001|Person\n"
                "1000000000002|Person\n",
            )
            _write(rels, "from|to|type\n1000000000001|1000000000002|KNOWS\n")

            builder = GraphSnapshotBuilder()
            builder.load_nodes_from_csv(
                nodes,
                label_columns=["label"],
                property_columns=["ext_id"],
                delimiter="|",
            )
            pairs = builder.load_relationships_from_csv(
                rels,
                {"column": "from", "property_key": "ext_id", "label": "Person"},
                {"column": "to", "property_key": "ext_id", "label": "Person"},
                rel_type_column="type",
                delimiter="|",
            )

            # Label filter must pick the Person (internal 1), not the colliding Comment (0).
            assert pairs == [(1, 2)]
            snap = builder.finalize()
            assert snap.relationship_count() == 1
            assert snap.neighbor_ids(1, Direction.Outgoing) == [2]

    def test_unmatched_property_reference_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            nodes = os.path.join(d, "nodes.csv")
            rels = os.path.join(d, "rels.csv")
            _write(nodes, "ext_id,label\n10,Person\n20,Person\n")
            _write(rels, "from,to,type\n10,20,KNOWS\n10,999,KNOWS\n")

            builder = GraphSnapshotBuilder()
            builder.load_nodes_from_csv(
                nodes, label_columns=["label"], property_columns=["ext_id"]
            )
            pairs = builder.load_relationships_from_csv(
                rels,
                {"column": "from", "property_key": "ext_id", "label": "Person"},
                {"column": "to", "property_key": "ext_id", "label": "Person"},
                rel_type_column="type",
            )
            assert pairs == [(0, 1)]

    def test_composite_property_reference(self):
        with tempfile.TemporaryDirectory() as d:
            nodes = os.path.join(d, "nodes.csv")
            rels = os.path.join(d, "rels.csv")
            _write(nodes, "first,last,label\nAlice,Smith,Person\nBob,Jones,Person\n")
            _write(rels, "f1,l1,f2,l2,type\nAlice,Smith,Bob,Jones,KNOWS\n")

            builder = GraphSnapshotBuilder()
            builder.load_nodes_from_csv(
                nodes, label_columns=["label"], property_columns=["first", "last"]
            )
            pairs = builder.load_relationships_from_csv(
                rels,
                {
                    "columns": ["f1", "l1"],
                    "property_keys": ["first", "last"],
                    "label": "Person",
                },
                {
                    "columns": ["f2", "l2"],
                    "property_keys": ["first", "last"],
                    "label": "Person",
                },
                rel_type_column="type",
            )
            assert pairs == [(0, 1)]

    def test_bad_column_raises(self):
        with tempfile.TemporaryDirectory() as d:
            rels = os.path.join(d, "rels.csv")
            _write(rels, "from,to,type\n0,1,KNOWS\n")
            builder = GraphSnapshotBuilder()
            with pytest.raises(Exception, match="not found"):
                builder.load_relationships_from_csv(
                    rels, "source", "to", rel_type_column="type"
                )
