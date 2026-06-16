"""Tests for the RRSR record-store bindings (write_rrsr / read_rrsr)."""

import pytest

from rustychickpeas import read_rrsr, write_rrsr


def test_round_trip(tmp_path):
    records = [b"alpha", b"", b"gamma-longer", b"d"]
    idx = str(tmp_path / "r.idx")
    binp = str(tmp_path / "r.bin")
    write_rrsr(idx, binp, records)
    assert read_rrsr(idx, binp) == records


def test_empty_store(tmp_path):
    idx = str(tmp_path / "e.idx")
    binp = str(tmp_path / "e.bin")
    write_rrsr(idx, binp, [])
    assert read_rrsr(idx, binp) == []


def test_read_missing_raises(tmp_path):
    with pytest.raises(Exception):
        read_rrsr(str(tmp_path / "nope.idx"), str(tmp_path / "nope.bin"))
