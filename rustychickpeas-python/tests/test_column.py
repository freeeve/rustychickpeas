"""Tests for GraphSnapshot.column() — self-describing buffer-protocol columns."""

import gc

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from rustychickpeas import Direction, GraphSnapshotBuilder


def _build():
    b = GraphSnapshotBuilder()
    for i in range(4):
        b.add_node(["N"], node_id=i)
        b.set_prop(i, "n", i * 10)  # int64
        b.set_prop(i, "score", i + 0.5)  # float64
        b.set_prop(i, "flag", i % 2 == 0)  # bool
        b.set_prop(i, "name", f"p{i}")  # string
    return b.finalize()


def test_int64_column_buffer_and_dtype():
    col = _build().column("n")
    assert col.dtype == "int64"
    assert len(col) == 4
    assert col.to_pylist() == [0, 10, 20, 30]
    mv = memoryview(col)
    assert mv.format == "q" and mv.itemsize == 8 and mv.readonly
    assert mv.tolist() == [0, 10, 20, 30]
    # pyarrow zero-copy wrap of the same memory.
    arr = pa.Array.from_buffers(pa.int64(), len(col), [None, pa.py_buffer(col)])
    assert arr.to_pylist() == [0, 10, 20, 30]


def test_float64_column():
    col = _build().column("score")
    assert col.dtype == "float64"
    assert col.to_pylist() == [0.5, 1.5, 2.5, 3.5]
    mv = memoryview(col)
    assert mv.format == "d" and mv.itemsize == 8
    assert mv.tolist() == [0.5, 1.5, 2.5, 3.5]


def test_bool_column():
    col = _build().column("flag")
    assert col.dtype == "bool"
    assert col.to_pylist() == [True, False, True, False]
    mv = memoryview(col)
    assert mv.format == "B" and mv.itemsize == 1  # uint8 0/1
    assert mv.tolist() == [1, 0, 1, 0]


def test_string_column_codes_and_values():
    col = _build().column("name")
    assert col.dtype == "string"
    assert col.to_pylist() == ["p0", "p1", "p2", "p3"]
    mv = memoryview(col)
    assert mv.format == "I" and mv.itemsize == 4  # uint32 interned codes
    codes = mv.tolist()
    assert len(codes) == 4 and len(set(codes)) == 4  # distinct strings -> distinct codes


def test_string_id_vectorized_filter():
    # The intended string-column usage: resolve filter targets to codes once, then
    # compare against the column's codes (here with pyarrow's vectorized is_in).
    g = _build()
    col = g.column("name")
    codes = pa.Array.from_buffers(pa.uint32(), len(col), [None, pa.py_buffer(col)])
    targets = [g.string_id("p1"), g.string_id("p3")]
    assert all(t is not None for t in targets)
    mask = pc.is_in(codes, value_set=pa.array(targets, pa.uint32()))
    assert mask.to_pylist() == [False, True, False, True]
    # A string that was never interned resolves to None (matches nothing).
    assert g.string_id("not-present") is None


def test_aggregate_through_counts_edges_by_neighbor():
    b = GraphSnapshotBuilder()
    for nid, label in [(0, "Post"), (1, "Post"), (2, "Tag"), (3, "Tag")]:
        b.add_node([label], node_id=nid)
    b.add_relationship(0, 2, "hasTag")
    b.add_relationship(0, 3, "hasTag")
    b.add_relationship(1, 2, "hasTag")
    g = b.finalize()

    res = g.aggregate("Post").through("hasTag", Direction.Outgoing).run()
    assert res.total == 2  # two source Post nodes
    counts = {r["neighbor"]: r["count"] for r in res.rows}
    assert counts == {2: 2, 3: 1}  # tag 2 has 2 edges, tag 3 has 1


def test_absent_column_is_none():
    assert _build().column("missing") is None


def test_group_reduce():
    b = GraphSnapshotBuilder()
    # (id, label, day, content, len)
    rows_in = [
        (0, "Post", 5, 1, 10),  # day<8, content -> grouped: label0, bucket0
        (1, "Post", 5, 1, 50),  # grouped: label0, bucket1
        (2, "Comment", 5, 0, 20),  # day<8 (in total) but no content (not grouped)
        (3, "Comment", 9, 1, 100),  # day>=8 -> excluded entirely
    ]
    for nid, label, day, content, ln in rows_in:
        b.add_node([label], node_id=nid)
        b.set_prop(nid, "day", day)
        b.set_prop(nid, "content", content)
        b.set_prop(nid, "len", ln)
    g = b.finalize()

    rows, total = g.group_reduce(
        labels=["Post", "Comment"],
        pre_filters=[("day", "<", 8)],
        group_filters=[("content", "!=", 0)],
        group_label=True,
        group_bins=[("len", [40, 80, 160])],
        sum_col="len",
    )
    assert total == 3  # nodes 0,1,2 are before the cutoff
    grouped = {tuple(k): (c, s) for k, c, s in rows}
    assert grouped == {(0, 0): (1, 10), (0, 1): (1, 50)}


def test_group_reduce_bad_column_raises():
    g = _build()  # "name" is a string column, not i64
    with pytest.raises(Exception, match="not a dense i64 column"):
        g.group_reduce(labels=["N"], group_cols=["name"])


def _agg_graph():
    b = GraphSnapshotBuilder()
    for nid, label, day, content, ln in [
        (0, "Post", 5, 1, 10),
        (1, "Post", 5, 1, 50),
        (2, "Comment", 5, 0, 20),
        (3, "Comment", 9, 1, 100),
    ]:
        b.add_node([label], node_id=nid)
        b.set_prop(nid, "day", day)
        b.set_prop(nid, "content", content)
        b.set_prop(nid, "len", ln)
    return b.finalize()


def test_aggregate_builder():
    g = _agg_graph()
    res = (
        g.aggregate("Post", "Comment")
        .where("day", "<", 8)
        .having("content", "!=", 0)
        .by_label()
        .bin("len", [40, 80, 160])
        .sum("len")
        .run()
    )
    assert res.total == 3
    # Self-describing rows with the label resolved to its name.
    rows = {(r["label"], r["len_bin"]): (r["count"], r["sum"]) for r in res.rows}
    assert rows == {("Post", 0): (1, 10), ("Post", 1): (1, 50)}


def test_aggregate_builder_is_immutable():
    g = _agg_graph()
    base = g.aggregate("Post").where("day", "<", 8)
    # Extending `base` two different ways must not mutate `base`.
    a = base.by_label().run()
    b = base.bin("len", [40]).run()
    assert a.total == b.total == 2  # both Posts pass day<8
    assert all("len_bin" not in row for row in a.rows)
    assert all("len_bin" in row for row in b.rows)


def test_buffer_keeps_backing_alive():
    # The view's obj reference must keep the Column (and its snapshot Arc) alive
    # even when no Python name holds the Column.
    mv = memoryview(_build().column("n"))
    gc.collect()
    assert mv.tolist() == [0, 10, 20, 30]
