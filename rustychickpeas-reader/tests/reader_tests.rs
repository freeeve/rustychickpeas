//! End-to-end reader tests: graphs written by rustychickpeas-core must
//! traverse identically through GraphReader, and record stores written by
//! rustychickpeas-format must read back through roaringrange's RecordStore
//! (the cross-library conformance requirement).

use rustychickpeas_core::graph_builder::GraphBuilder;
use rustychickpeas_format::rrsr;
use rustychickpeas_reader::records::{MemoryFetch, RecordStore};
use rustychickpeas_reader::{Direction, GraphReader};

fn demo_graph_bytes() -> Vec<u8> {
    let mut builder = GraphBuilder::with_version("demo", None, None);
    builder.add_node(Some(0), &["Person"]).unwrap();
    builder.add_node(Some(1), &["Person"]).unwrap();
    builder.add_node(Some(2), &["Company"]).unwrap();
    builder.add_node(Some(7), &["Company"]).unwrap(); // sparse gap 3..=6
    builder.set_prop_str(0, "name", "alice").unwrap(); // exercises column sections
    builder.add_relationship(0, 1, "KNOWS").unwrap();
    builder.add_relationship(1, 2, "WORKS_FOR").unwrap();
    builder.add_relationship(0, 2, "WORKS_FOR").unwrap();
    builder.add_relationship(2, 7, "PARENT_OF").unwrap();
    let snapshot = builder.finalize(None);
    let mut bytes = Vec::new();
    snapshot.write_rcpg(&mut bytes).unwrap();
    bytes
}

#[test]
fn reader_traversal_matches_core() {
    let bytes = demo_graph_bytes();
    let reader = GraphReader::from_rcpg_bytes(&bytes).unwrap();

    assert_eq!(reader.node_count(), 4);
    assert_eq!(reader.relationship_count(), 4);
    assert_eq!(reader.csr_id_space(), 8); // sparse IDs: space is max_id + 1

    assert_eq!(reader.neighbors(0, Direction::Outgoing), vec![1, 2]);
    assert_eq!(reader.neighbors(2, Direction::Incoming), vec![1, 0]);
    assert_eq!(reader.neighbors(5, Direction::Outgoing), Vec::<u32>::new()); // gap ID
    assert_eq!(
        reader.neighbors_by_type(0, Direction::Outgoing, "WORKS_FOR"),
        vec![2]
    );
    assert_eq!(
        reader.neighbors_by_type(0, Direction::Outgoing, "NOPE"),
        Vec::<u32>::new()
    );
    assert_eq!(
        reader.neighbors_by_type(2, Direction::Incoming, "WORKS_FOR"),
        vec![1, 0]
    );

    assert_eq!(reader.node_labels(0), vec!["Person"]);
    assert_eq!(reader.node_labels(7), vec!["Company"]);
    let companies: Vec<u32> = reader.nodes_with_label("Company").unwrap().iter().collect();
    assert_eq!(companies, vec![2, 7]);
    assert!(reader.nodes_with_label("Missing").is_none());

    let mut labels = reader.labels();
    labels.sort_unstable();
    assert_eq!(labels, vec!["Company", "Person"]);
    let mut types = reader.relationship_types();
    types.sort_unstable();
    assert_eq!(types, vec!["KNOWS", "PARENT_OF", "WORKS_FOR"]);
}

#[test]
fn reader_bfs() {
    let bytes = demo_graph_bytes();
    let reader = GraphReader::from_rcpg_bytes(&bytes).unwrap();

    // 0 -> {1, 2} -> {7} (2 was already seen via the direct rel)
    assert_eq!(reader.bfs(0, 1, Direction::Outgoing), vec![1, 2]);
    assert_eq!(reader.bfs(0, 3, Direction::Outgoing), vec![1, 2, 7]);
    assert_eq!(reader.bfs(7, 2, Direction::Incoming), vec![2, 1, 0]);
    assert_eq!(reader.bfs(7, 0, Direction::Both), Vec::<u32>::new());
}

#[test]
fn reader_rejects_garbage() {
    assert!(GraphReader::from_rcpg_bytes(b"not a graph").is_err());
}

#[test]
fn topology_only_reader_traverses_without_columns() {
    // The demo graph carries property columns; a topology-only reader must
    // skip them while traversal stays fully functional
    let bytes = demo_graph_bytes();
    let reader = GraphReader::topology_only(&bytes).unwrap();
    assert!(reader.graph().node_columns.is_empty());
    assert!(reader.graph().rel_columns.is_empty());
    assert_eq!(reader.neighbors(0, Direction::Outgoing), vec![1, 2]);
    assert_eq!(
        reader.neighbors_by_type(0, Direction::Outgoing, "WORKS_FOR"),
        vec![2]
    );
    assert_eq!(reader.bfs(0, 3, Direction::Outgoing), vec![1, 2, 7]);
    assert_eq!(reader.node_labels(7), vec!["Company"]);
}

#[test]
fn roaringrange_reads_our_record_store() {
    let records: Vec<&[u8]> = vec![
        br#"{"name":"Alice"}"#,
        br#"{"name":"Bob"}"#,
        b"",
        br#"{"name":"Acme","kind":"company"}"#,
    ];
    let mut idx = Vec::new();
    let mut bin = Vec::new();
    rrsr::write(&mut idx, &mut bin, records.iter().copied()).unwrap();

    // The conformance requirement: roaringrange's reference reader must
    // accept stores written by rustychickpeas-format byte-for-byte.
    let store = futures::executor::block_on(RecordStore::open(
        MemoryFetch::new(idx),
        MemoryFetch::new(bin),
    ))
    .unwrap();
    assert_eq!(store.len(), 4);

    for (i, expected) in records.iter().enumerate() {
        let got = futures::executor::block_on(store.get(i as u32)).unwrap();
        assert_eq!(got.as_deref(), Some(*expected), "record {}", i);
    }
    let many = futures::executor::block_on(store.get_many(&[3, 0])).unwrap();
    assert_eq!(many[0].as_deref(), Some(records[3]));
    assert_eq!(many[1].as_deref(), Some(records[0]));
    // out-of-range id is None, not an error
    let missing = futures::executor::block_on(store.get(99)).unwrap();
    assert_eq!(missing, None);
}
