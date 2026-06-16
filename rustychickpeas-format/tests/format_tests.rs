//! Round-trip and robustness tests for RCPG and RRSR.

use proptest::prelude::*;
use roaring::RoaringBitmap;
use rustychickpeas_format::{rcpg, rrsr, ColumnData, GraphSection};

fn sample_graph() -> GraphSection {
    let mut labels = RoaringBitmap::new();
    labels.insert(0);
    labels.insert(2);
    let mut types = RoaringBitmap::new();
    types.insert(0);
    types.insert(1);

    let mut bits = bitvec::vec::BitVec::new();
    bits.resize(3, false);
    bits.set(1, true);

    GraphSection {
        n_nodes: 3,
        n_rels: 2,
        out_offsets: vec![0, 1, 2, 2],
        out_nbrs: vec![1, 2],
        out_types: vec![4, 5],
        in_offsets: vec![0, 0, 1, 2],
        in_nbrs: vec![0, 1],
        in_types: vec![4, 5],
        label_index: vec![(1, labels)],
        type_index: vec![(4, types)],
        node_columns: vec![
            (2, ColumnData::DenseI64(vec![10, 20, 30])),
            (3, ColumnData::SparseStr(vec![(0, 6), (2, 7)])),
            (8, ColumnData::DenseBool(bits)),
        ],
        rel_columns: vec![(9, ColumnData::SparseF64(vec![(0, 0.5)]))],
        version: Some("v1.0".to_string()),
        atoms: vec![
            "".into(),
            "Person".into(),
            "age".into(),
            "name".into(),
            "KNOWS".into(),
            "LIKES".into(),
            "Alice".into(),
            "Bob".into(),
            "active".into(),
            "weight".into(),
        ],
    }
}

#[test]
fn rcpg_round_trip() {
    let graph = sample_graph();
    let mut bytes = Vec::new();
    rcpg::write(&graph, &mut bytes).unwrap();
    let parsed = rcpg::parse(&bytes).unwrap();
    assert_eq!(parsed, graph);
}

#[test]
fn rcpg_neighbors_accessors() {
    let graph = sample_graph();
    assert_eq!(graph.out_neighbors(0), &[1]);
    assert_eq!(graph.out_neighbors(2), &[] as &[u32]);
    assert_eq!(graph.in_neighbors(2), &[1]);
    assert_eq!(graph.out_neighbors(999), &[] as &[u32]);
}

#[test]
fn rcpg_topology_only_write_omits_columns() {
    let graph = sample_graph();
    let mut full = Vec::new();
    rcpg::write(&graph, &mut full).unwrap();
    let mut lean = Vec::new();
    rcpg::write_with(&graph, &mut lean, &rcpg::WriteOptions::topology_only()).unwrap();
    assert!(lean.len() < full.len());

    let parsed = rcpg::parse(&lean).unwrap();
    assert!(parsed.node_columns.is_empty());
    assert!(parsed.rel_columns.is_empty());
    // topology intact
    assert_eq!(parsed.out_offsets, graph.out_offsets);
    assert_eq!(parsed.label_index, graph.label_index);
    assert_eq!(parsed.atoms, graph.atoms);
}

#[test]
fn rcpg_topology_only_parse_skips_present_columns() {
    let graph = sample_graph();
    let mut bytes = Vec::new();
    rcpg::write(&graph, &mut bytes).unwrap();

    let parsed = rcpg::parse_with(&bytes, &rcpg::ParseOptions::topology_only()).unwrap();
    assert!(parsed.node_columns.is_empty());
    assert!(parsed.rel_columns.is_empty());
    assert_eq!(parsed.out_nbrs, graph.out_nbrs);
    assert_eq!(parsed.type_index, graph.type_index);

    // full parse of the same bytes still materializes everything
    assert_eq!(rcpg::parse(&bytes).unwrap(), graph);
}

#[test]
fn rcpg_rejects_bad_magic() {
    let err = rcpg::parse(b"NOPE0000000000000000").unwrap_err();
    assert!(err.to_string().contains("magic"), "{}", err);
}

#[test]
fn rcpg_rejects_future_version() {
    let graph = sample_graph();
    let mut bytes = Vec::new();
    rcpg::write(&graph, &mut bytes).unwrap();
    bytes[4] = 0xFF; // version low byte
    bytes[5] = 0xFF;
    assert!(matches!(
        rcpg::parse(&bytes),
        Err(rustychickpeas_format::FormatError::UnsupportedVersion { .. })
    ));
}

#[test]
fn rcpg_rejects_truncation_everywhere() {
    let graph = sample_graph();
    let mut bytes = Vec::new();
    rcpg::write(&graph, &mut bytes).unwrap();
    // Every strict prefix must produce an error, never a panic and never
    // a silently-wrong success
    for cut in 0..bytes.len() {
        assert!(
            rcpg::parse(&bytes[..cut]).is_err(),
            "truncation at {} parsed successfully",
            cut
        );
    }
}

#[test]
fn rrsr_round_trip_and_ranges() {
    let records: Vec<&[u8]> = vec![b"alpha", b"", b"gamma-longer", b"d"];
    let mut idx = Vec::new();
    let mut bin = Vec::new();
    rrsr::write(&mut idx, &mut bin, records.iter().copied()).unwrap();

    let index = rrsr::RecordIndex::parse(&idx).unwrap();
    assert_eq!(index.len(), 4);
    for (i, record) in records.iter().enumerate() {
        let (start, end) = index.record_range(i as u32).unwrap();
        assert_eq!(&bin[start as usize..end as usize], *record);
    }
    assert_eq!(index.record_range(4), None);

    // Adjacent records coalesce; the empty record contributes nothing
    assert_eq!(index.plan_ranges(&[0, 1, 2, 3], 0), vec![(0, 18)]);
    // Out-of-range IDs ignored; disjoint records with zero gap tolerance
    // still coalesce because record 1 is empty (gap is 0 bytes)
    assert_eq!(index.plan_ranges(&[3, 0, 99], 0), vec![(0, 5), (17, 18)]);
    // Large gap tolerance merges everything
    assert_eq!(index.plan_ranges(&[0, 3], 1 << 20), vec![(0, 18)]);
}

#[test]
fn rrsr_rejects_bad_input() {
    assert!(rrsr::RecordIndex::parse(b"RRSR").is_err());
    assert!(rrsr::RecordIndex::parse(b"XXXX0000000000000000").is_err());
    // Count that doesn't match the file length
    let mut idx = Vec::new();
    let mut bin = Vec::new();
    rrsr::write(&mut idx, &mut bin, [b"abc" as &[u8]]).unwrap();
    idx.truncate(idx.len() - 1);
    assert!(rrsr::RecordIndex::parse(&idx).is_err());
}

proptest! {
    #[test]
    fn rcpg_random_round_trip(
        n_nodes in 0u32..100,
        out_nbrs in proptest::collection::vec(0u32..100, 0..50),
        atoms in proptest::collection::vec("[a-z]{0,8}", 1..20),
        dense in proptest::collection::vec(proptest::num::i64::ANY, 0..50),
        sparse in proptest::collection::btree_map(0u32..100, proptest::num::i64::ANY, 0..20),
    ) {
        let m = out_nbrs.len();
        let mut bm = RoaringBitmap::new();
        for &x in &out_nbrs { bm.insert(x); }
        let graph = GraphSection {
            n_nodes,
            n_rels: m as u64,
            out_offsets: vec![0, m as u32],
            out_types: vec![0; m],
            in_offsets: vec![0, m as u32],
            in_nbrs: out_nbrs.clone(),
            in_types: vec![0; m],
            out_nbrs,
            label_index: vec![(1, bm)],
            type_index: vec![],
            node_columns: vec![
                (2, ColumnData::DenseI64(dense)),
                (3, ColumnData::SparseI64(sparse.into_iter().collect())),
            ],
            rel_columns: vec![],
            version: None,
            atoms,
        };
        let mut bytes = Vec::new();
        rcpg::write(&graph, &mut bytes).unwrap();
        prop_assert_eq!(rcpg::parse(&bytes).unwrap(), graph);
    }

    #[test]
    fn rrsr_random_round_trip(
        records in proptest::collection::vec(proptest::collection::vec(proptest::num::u8::ANY, 0..64), 0..40),
    ) {
        let mut idx = Vec::new();
        let mut bin = Vec::new();
        rrsr::write(&mut idx, &mut bin, records.iter().map(|r| r.as_slice())).unwrap();
        let index = rrsr::RecordIndex::parse(&idx).unwrap();
        prop_assert_eq!(index.len(), records.len());
        for (i, record) in records.iter().enumerate() {
            let (start, end) = index.record_range(i as u32).unwrap();
            prop_assert_eq!(&bin[start as usize..end as usize], record.as_slice());
        }
        // Planned ranges must cover every non-empty requested record
        let ids: Vec<u32> = (0..records.len() as u32).collect();
        let ranges = index.plan_ranges(&ids, 0);
        for (i, record) in records.iter().enumerate() {
            if record.is_empty() { continue; }
            let (start, end) = index.record_range(i as u32).unwrap();
            prop_assert!(
                ranges.iter().any(|&(rs, re)| rs <= start && end <= re),
                "record {} not covered", i
            );
        }
    }

    /// Arbitrary bytes must parse-or-error, never panic — verifying the reader's
    /// "never panics on malformed input" guarantee against random *content*
    /// (the truncation test only covers prefixes of otherwise-valid data).
    #[test]
    fn rcpg_arbitrary_bytes_never_panic(
        bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..1024),
    ) {
        // A panic here fails the property; the Ok/Err result itself is irrelevant.
        let _ = rcpg::parse(&bytes);
    }

    /// A valid RCPG with corrupted bytes (the "right length, wrong content" space
    /// that truncation misses) must still parse-or-error without panicking.
    #[test]
    fn rcpg_mutation_never_panic(
        mutations in proptest::collection::vec(
            (proptest::num::usize::ANY, proptest::num::u8::ANY),
            1..16,
        ),
    ) {
        let mut bytes = Vec::new();
        rcpg::write(&sample_graph(), &mut bytes).unwrap();
        for (idx, byte) in mutations {
            let i = idx % bytes.len();
            bytes[i] = byte;
        }
        let _ = rcpg::parse(&bytes);
    }
}
