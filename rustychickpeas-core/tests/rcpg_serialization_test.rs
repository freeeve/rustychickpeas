//! End-to-end RCPG serialization tests: build -> finalize -> write ->
//! read -> identical query results.

use proptest::prelude::*;
use rustychickpeas_core::graph_builder::GraphBuilder;
use rustychickpeas_core::graph_snapshot::{GraphSnapshot, ValueId};
use rustychickpeas_core::types::Direction;

fn assert_snapshots_equivalent(a: &GraphSnapshot, b: &GraphSnapshot, max_id: u32) {
    assert_eq!(a.n_nodes, b.n_nodes);
    assert_eq!(a.n_rels, b.n_rels);
    assert_eq!(a.version(), b.version());
    for id in 0..=max_id {
        assert_eq!(
            a.neighbors(id, Direction::Outgoing).collect::<Vec<_>>(),
            b.neighbors(id, Direction::Outgoing).collect::<Vec<_>>(),
            "out({})",
            id
        );
        assert_eq!(
            a.neighbors(id, Direction::Incoming).collect::<Vec<_>>(),
            b.neighbors(id, Direction::Incoming).collect::<Vec<_>>(),
            "in({})",
            id
        );
    }
}

#[test]
fn rcpg_round_trip_with_properties() {
    let mut builder = GraphBuilder::with_version("v2.3", None, None);
    builder.add_node(Some(0), &["Person"]).unwrap();
    builder.add_node(Some(1), &["Person", "Admin"]).unwrap();
    // sparse IDs: skip 2..=9
    builder.add_node(Some(10), &["Company"]).unwrap();
    builder.set_prop_str(0, "name", "Alice").unwrap();
    builder.set_prop_str(1, "name", "Bob").unwrap();
    builder.set_prop_i64(0, "age", 30).unwrap();
    builder.set_prop_f64(1, "score", 99.5).unwrap();
    builder.set_prop_bool(10, "active", true).unwrap();
    builder.add_relationship(0, 1, "KNOWS").unwrap();
    builder.add_relationship(1, 10, "WORKS_FOR").unwrap();
    builder.add_relationship(0, 10, "WORKS_FOR").unwrap();
    let snapshot = builder.finalize(None);

    let mut bytes = Vec::new();
    snapshot.write_rcpg(&mut bytes).unwrap();
    let restored = GraphSnapshot::read_rcpg(&bytes).unwrap();

    assert_snapshots_equivalent(&snapshot, &restored, 10);

    // labels survive (atom IDs and bitmap contents)
    for label in ["Person", "Admin", "Company"] {
        let a: Vec<u32> = snapshot.nodes_with_label(label).unwrap().iter().collect();
        let b: Vec<u32> = restored.nodes_with_label(label).unwrap().iter().collect();
        assert_eq!(a, b, "label {}", label);
    }

    // typed traversal survives (exercises type_index + atoms)
    assert_eq!(
        snapshot
            .neighbors_by_type(0, Direction::Outgoing, &["WORKS_FOR"])
            .collect::<Vec<_>>(),
        restored
            .neighbors_by_type(0, Direction::Outgoing, &["WORKS_FOR"])
            .collect::<Vec<_>>()
    );

    // properties survive, including string resolution through atoms
    for (id, key) in [
        (0, "name"),
        (1, "name"),
        (0, "age"),
        (1, "score"),
        (10, "active"),
    ] {
        let (a, b) = (snapshot.prop(id, key), restored.prop(id, key));
        assert_eq!(a, b, "prop({}, {})", id, key);
        if let Some(ValueId::Str(s)) = a {
            assert_eq!(snapshot.resolve_string(s), restored.resolve_string(s));
        }
    }
    assert_eq!(restored.prop(5, "name"), None); // gap ID has nothing

    // lazy property index rebuilds and answers correctly after a read
    let by_name: Vec<u32> = restored
        .nodes_with_property("Person", "name", "Alice")
        .map(|s| s.iter().collect())
        .unwrap_or_default();
    assert_eq!(by_name, vec![0]);
}

#[test]
fn rcpg_topology_only_write() {
    use rustychickpeas_core::format::rcpg::WriteOptions;

    let mut builder = GraphBuilder::new(None, None);
    builder.add_node(Some(0), &["Person"]).unwrap();
    builder.add_node(Some(1), &["Person"]).unwrap();
    builder.set_prop_str(0, "name", "Alice").unwrap();
    builder.set_prop_i64(1, "age", 25).unwrap();
    builder.add_relationship(0, 1, "KNOWS").unwrap();
    let snapshot = builder.finalize(None);

    let mut full = Vec::new();
    snapshot.write_rcpg(&mut full).unwrap();
    let mut lean = Vec::new();
    snapshot
        .write_rcpg_with(&mut lean, &WriteOptions::topology_only())
        .unwrap();
    assert!(lean.len() < full.len());

    let restored = GraphSnapshot::read_rcpg(&lean).unwrap();
    // traversal and labels intact, properties absent
    assert_eq!(
        restored.neighbors(0, Direction::Outgoing).collect::<Vec<_>>(),
        vec![1]
    );
    let people: Vec<u32> = restored
        .nodes_with_label("Person")
        .unwrap()
        .iter()
        .collect();
    assert_eq!(people, vec![0, 1]);
    assert_eq!(restored.prop(0, "name"), None);
    assert_eq!(restored.prop(1, "age"), None);
}

#[test]
fn rcpg_file_round_trip() {
    let mut builder = GraphBuilder::new(None, None);
    builder.add_node(Some(0), &["N"]).unwrap();
    builder.add_relationship(0, 0, "SELF").unwrap();
    let snapshot = builder.finalize(None);

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("graph.rcpg");
    snapshot.write_rcpg_file(path.to_str().unwrap()).unwrap();
    let restored = GraphSnapshot::read_rcpg_file(path.to_str().unwrap()).unwrap();
    assert_eq!(restored.n_nodes, 1);
    assert_eq!(
        restored.neighbors(0, Direction::Outgoing).collect::<Vec<_>>(),
        vec![0]
    );
}

#[test]
fn rcpg_empty_graph_round_trip() {
    let builder = GraphBuilder::new(None, None);
    let snapshot = builder.finalize(None);
    let mut bytes = Vec::new();
    snapshot.write_rcpg(&mut bytes).unwrap();
    let restored = GraphSnapshot::read_rcpg(&bytes).unwrap();
    assert_eq!(restored.n_nodes, 0);
    assert_eq!(restored.n_rels, 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn rcpg_random_graph_round_trip(
        // sparse IDs on purpose: stride creates gaps in the ID space
        ids in proptest::collection::btree_set(0u32..500, 1..60),
        edges in proptest::collection::vec((0usize..60, 0usize..60, 0usize..2), 0..150),
        props in proptest::collection::btree_map(0usize..60, ("[a-z]{0,12}", proptest::num::i64::ANY), 0..30),
    ) {
        const REL_TYPES: [&str; 2] = ["A", "B"];
        let ids: Vec<u32> = ids.into_iter().collect();

        let mut builder = GraphBuilder::new(None, None);
        for &id in &ids {
            builder.add_node(Some(id), &["Node"]).unwrap();
        }
        for (u, v, t) in &edges {
            let (u, v) = (ids[u % ids.len()], ids[v % ids.len()]);
            builder.add_relationship(u, v, REL_TYPES[*t]).unwrap();
        }
        for (i, (s, n)) in &props {
            let id = ids[i % ids.len()];
            builder.set_prop_str(id, "tag", s).unwrap();
            builder.set_prop_i64(id, "num", *n).unwrap();
        }
        let snapshot = builder.finalize(None);

        let mut bytes = Vec::new();
        snapshot.write_rcpg(&mut bytes).unwrap();
        let restored = GraphSnapshot::read_rcpg(&bytes).unwrap();

        prop_assert_eq!(restored.n_nodes, snapshot.n_nodes);
        prop_assert_eq!(restored.n_rels, snapshot.n_rels);
        let max_id = ids.iter().copied().max().unwrap();
        for id in 0..=max_id {
            prop_assert_eq!(snapshot.neighbors(id, Direction::Outgoing).collect::<Vec<_>>(), restored.neighbors(id, Direction::Outgoing).collect::<Vec<_>>());
            prop_assert_eq!(snapshot.neighbors(id, Direction::Incoming).collect::<Vec<_>>(), restored.neighbors(id, Direction::Incoming).collect::<Vec<_>>());
            let (a, b) = (snapshot.prop(id, "num"), restored.prop(id, "num"));
            prop_assert_eq!(a, b);
            match (snapshot.prop(id, "tag"), restored.prop(id, "tag")) {
                (Some(ValueId::Str(x)), Some(ValueId::Str(y))) => {
                    prop_assert_eq!(snapshot.resolve_string(x), restored.resolve_string(y));
                }
                (a, b) => prop_assert_eq!(a, b),
            }
        }
        let a: Vec<u32> = snapshot.nodes_with_label("Node").unwrap().iter().collect();
        let b: Vec<u32> = restored.nodes_with_label("Node").unwrap().iter().collect();
        prop_assert_eq!(a, b);
    }
}
