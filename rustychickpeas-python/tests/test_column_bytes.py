"""Tests for GraphSnapshot.i64_column_bytes (raw dense-column export)."""

import struct

from rustychickpeas import GraphSnapshotBuilder


def _graph_with_i64_column():
    b = GraphSnapshotBuilder()
    for i in range(5):
        b.add_node(["N"], node_id=i)
        b.set_prop(i, "v", i * 10)
    return b.finalize()


def test_i64_column_bytes_roundtrip():
    g = _graph_with_i64_column()
    raw = g.i64_column_bytes("v")
    assert raw is not None
    n = len(raw) // 8
    assert n == 5
    vals = list(struct.unpack(f"={n}q", raw))  # native-endian i64
    assert vals == [0, 10, 20, 30, 40]
    # Agrees with the list-returning accessor.
    assert g.i64_column("v") == vals


def test_i64_column_bytes_absent_returns_none():
    g = _graph_with_i64_column()
    assert g.i64_column_bytes("missing") is None
