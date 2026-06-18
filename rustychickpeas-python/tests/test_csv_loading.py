"""Tests for CSV bulk loading, including property-based relationship endpoints."""

import gzip
import os
import tempfile

import pytest

from rustychickpeas import GraphSnapshotBuilder, Direction


def _write(path, text):
    with open(path, "w") as f:
        f.write(text)


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
