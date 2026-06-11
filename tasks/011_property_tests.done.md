# Property tests for parsers and NodeSet

proptest-based tests in rustychickpeas-core/tests/property_tests.rs:
- NodeSet adaptive ops (and/or/sub) match RoaringBitmap reference
  semantics across Roaring/Bitset representation combinations
- CSV round-trip: hostile strings (quotes, commas, newlines, unicode),
  ints/floats/bools survive write -> load_nodes_from_csv
- Parquet round-trip: random typed columns with nulls survive
  write -> load_nodes_from_parquet
- Graph round-trip: random edge lists -> finalize -> CSR neighbors match
  a naive adjacency model; label index membership correct
