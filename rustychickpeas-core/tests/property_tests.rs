//! Property-based tests for parsers, NodeSet, and CSR construction.
//!
//! These use proptest to exercise edge cases that example-based tests miss:
//! hostile CSV strings, null-heavy parquet columns, representation switching
//! in NodeSet, and sparse/duplicated edge lists.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use roaring::RoaringBitmap;
use rustychickpeas_core::bitmap::NodeSet;
use rustychickpeas_core::graph_builder::GraphBuilder;
use rustychickpeas_core::graph_snapshot::ValueId;
use rustychickpeas_core::types::Direction;

// ---------------------------------------------------------------------------
// NodeSet: adaptive Roaring/Bitset representations must agree with a
// BTreeSet reference model for every operator and representation combination
// ---------------------------------------------------------------------------

fn roaring_of(ids: &BTreeSet<u32>) -> NodeSet {
    NodeSet::new(ids.iter().copied().collect::<RoaringBitmap>())
}

fn bitset_of(ids: &BTreeSet<u32>) -> NodeSet {
    let max = ids.iter().next_back().copied().unwrap_or(0);
    let mut bv = bitvec::vec::BitVec::new();
    bv.resize(max as usize + 1, false);
    for &id in ids {
        bv.set(id as usize, true);
    }
    NodeSet::new_bitset(bv)
}

fn collect_sorted(ns: &NodeSet) -> Vec<u32> {
    let mut v: Vec<u32> = ns.iter().collect();
    v.sort_unstable();
    v
}

/// All representations of the same logical set, for combination testing.
/// Bitset is only built for small-id sets, matching its intended use.
fn representations(ids: &BTreeSet<u32>) -> Vec<NodeSet> {
    let mut reps = vec![roaring_of(ids)];
    if ids.iter().next_back().copied().unwrap_or(0) < 256 {
        reps.push(bitset_of(ids));
    }
    reps
}

proptest! {
    #[test]
    fn nodeset_ops_match_reference(
        a in proptest::collection::btree_set(0u32..512, 0..300),
        b in proptest::collection::btree_set(0u32..512, 0..300),
    ) {
        let and_ref: Vec<u32> = a.intersection(&b).copied().collect();
        let or_ref: Vec<u32> = a.union(&b).copied().collect();
        let sub_ref: Vec<u32> = a.difference(&b).copied().collect();

        for ra in representations(&a) {
            for rb in representations(&b) {
                prop_assert_eq!(collect_sorted(&(&ra & &rb)), and_ref.clone());
                prop_assert_eq!(collect_sorted(&(&ra | &rb)), or_ref.clone());
                prop_assert_eq!(collect_sorted(&(&ra - &rb)), sub_ref.clone());
            }
        }
    }

    #[test]
    fn nodeset_mutation_matches_reference(
        ops in proptest::collection::vec((proptest::bool::ANY, 0u32..600), 0..200),
    ) {
        let mut model: BTreeSet<u32> = BTreeSet::new();
        let mut ns = NodeSet::empty();
        for (insert, id) in ops {
            if insert {
                prop_assert_eq!(ns.insert(id), model.insert(id));
            } else {
                prop_assert_eq!(ns.remove(id), model.remove(&id));
            }
            prop_assert_eq!(ns.len(), model.len());
            prop_assert_eq!(ns.contains(id), model.contains(&id));
        }
        prop_assert_eq!(collect_sorted(&ns), model.into_iter().collect::<Vec<_>>());
    }
}

// ---------------------------------------------------------------------------
// CSR construction: random edge lists must round-trip through finalize()
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn csr_neighbors_match_edge_list(
        n in 1u32..150,
        edges in proptest::collection::vec((0u32..150, 0u32..150, 0usize..2), 0..400),
        labels in proptest::collection::vec(0usize..3, 1..150),
    ) {
        const REL_TYPES: [&str; 2] = ["A", "B"];
        const LABELS: [&str; 3] = ["Person", "Company", "City"];

        let mut builder = GraphBuilder::new(None, None);
        let mut label_model: BTreeMap<&str, BTreeSet<u32>> = BTreeMap::new();
        for id in 0..n {
            let label = LABELS[labels[id as usize % labels.len()]];
            builder.add_node(Some(id), &[label]).unwrap();
            label_model.entry(label).or_default().insert(id);
        }

        let mut out_model: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        let mut in_model: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        let mut edge_count = 0u64;
        for (u, v, t) in edges {
            let (u, v) = (u % n, v % n);
            builder.add_relationship(u, v, REL_TYPES[t]).unwrap();
            out_model.entry(u).or_default().push(v);
            in_model.entry(v).or_default().push(u);
            edge_count += 1;
        }

        let snapshot = builder.finalize(None);
        prop_assert_eq!(snapshot.n_nodes, n);
        prop_assert_eq!(snapshot.n_rels, edge_count);

        for id in 0..n {
            let mut out: Vec<u32> = snapshot.neighbors(id, Direction::Outgoing).collect();
            out.sort_unstable();
            let mut expected_out = out_model.remove(&id).unwrap_or_default();
            expected_out.sort_unstable();
            prop_assert_eq!(out, expected_out, "neighbors({}, Outgoing)", id);

            let mut inn: Vec<u32> = snapshot.neighbors(id, Direction::Incoming).collect();
            inn.sort_unstable();
            let mut expected_in = in_model.remove(&id).unwrap_or_default();
            expected_in.sort_unstable();
            prop_assert_eq!(inn, expected_in, "neighbors({}, Incoming)", id);
        }

        for (label, ids) in label_model {
            let set = snapshot.nodes_with_label(label).unwrap();
            let got: Vec<u32> = collect_sorted(set);
            prop_assert_eq!(got, ids.into_iter().collect::<Vec<_>>(), "label {}", label);
        }
    }
}

// ---------------------------------------------------------------------------
// CSV round-trip: hostile strings and typed columns survive write -> load
// ---------------------------------------------------------------------------

/// Strings that stress CSV quoting: commas, quotes, newlines, unicode.
fn hostile_string() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9 ,\"'\n\t\u{00e9}\u{4e16}\u{1f600}-]{0,40}").unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn csv_nodes_round_trip(
        rows in proptest::collection::btree_map(
            0u32..10_000,
            (hostile_string(), proptest::num::i64::ANY, -1e9f64..1e9f64, proptest::bool::ANY),
            1..50,
        ),
    ) {
        use rustychickpeas_core::graph_builder_csv::CsvColumnType;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nodes.csv");
        let mut w = csv::Writer::from_path(&path).unwrap();
        w.write_record(["id", "label", "name", "count", "score", "active"]).unwrap();
        for (id, (name, count, score, active)) in &rows {
            w.write_record([
                id.to_string(),
                "Person".to_string(),
                name.clone(),
                count.to_string(),
                score.to_string(),
                active.to_string(),
            ])
            .unwrap();
        }
        w.flush().unwrap();
        drop(w);

        let mut types = hashbrown::HashMap::new();
        types.insert("name", CsvColumnType::String);
        types.insert("count", CsvColumnType::Int64);
        types.insert("score", CsvColumnType::Float64);
        types.insert("active", CsvColumnType::Bool);

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder
            .load_nodes_from_csv(
                path.to_str().unwrap(),
                Some("id"),
                Some(vec!["label"]),
                Some(vec!["name", "count", "score", "active"]),
                None,
                Some(types),
                None,
                b',',
            )
            .unwrap();
        prop_assert_eq!(node_ids.len(), rows.len());

        let snapshot = builder.finalize(None);
        for (id, (name, count, score, active)) in &rows {
            match snapshot.prop(*id, "name") {
                Some(ValueId::Str(s)) => {
                    prop_assert_eq!(snapshot.resolve_string(s).unwrap(), name, "name for node {}", id)
                }
                // empty CSV fields are skipped by design
                None => prop_assert!(name.is_empty(), "name missing for node {}", id),
                other => prop_assert!(false, "unexpected name value {:?}", other),
            }
            prop_assert_eq!(snapshot.prop(*id, "count"), Some(ValueId::I64(*count)));
            match snapshot.prop(*id, "score") {
                Some(v) => prop_assert_eq!(v.to_f64().unwrap(), *score),
                None => prop_assert!(false, "score missing for node {}", id),
            }
            prop_assert_eq!(snapshot.prop(*id, "active"), Some(ValueId::Bool(*active)));
        }
    }
}

// ---------------------------------------------------------------------------
// Parquet round-trip: typed columns with nulls survive write -> load
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn parquet_nodes_round_trip(
        rows in proptest::collection::btree_map(
            0u32..10_000,
            (
                proptest::option::of(hostile_string()),
                proptest::option::of(proptest::num::i64::ANY),
                proptest::option::of(-1e9f64..1e9f64),
                proptest::option::of(proptest::bool::ANY),
            ),
            1..50,
        ),
    ) {
        use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]);

        let ids = Int64Array::from(rows.keys().map(|&id| id as i64).collect::<Vec<_>>());
        let names = StringArray::from(rows.values().map(|r| r.0.clone()).collect::<Vec<_>>());
        let counts = Int64Array::from(rows.values().map(|r| r.1).collect::<Vec<_>>());
        let scores = Float64Array::from(rows.values().map(|r| r.2).collect::<Vec<_>>());
        let actives = BooleanArray::from(rows.values().map(|r| r.3).collect::<Vec<_>>());

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(ids),
                Arc::new(names),
                Arc::new(counts),
                Arc::new(scores),
                Arc::new(actives),
            ],
        )
        .unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nodes.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder
            .load_nodes_from_parquet(
                path.to_str().unwrap(),
                Some("id"),
                None,
                Some(vec!["name", "count", "score", "active"]),
                None,
                Some("Person"),
            )
            .unwrap();
        prop_assert_eq!(node_ids.len(), rows.len());

        let snapshot = builder.finalize(None);
        for (id, (name, count, score, active)) in &rows {
            match (snapshot.prop(*id, "name"), name) {
                (Some(ValueId::Str(s)), Some(expected)) => {
                    prop_assert_eq!(snapshot.resolve_string(s).unwrap(), expected)
                }
                (None, None) => {}
                (got, expected) => prop_assert!(
                    false,
                    "name mismatch for node {}: got {:?}, expected {:?}",
                    id, got, expected
                ),
            }
            prop_assert_eq!(snapshot.prop(*id, "count"), count.map(ValueId::I64));
            match (snapshot.prop(*id, "score"), score) {
                (Some(v), Some(expected)) => prop_assert_eq!(v.to_f64().unwrap(), *expected),
                (None, None) => {}
                (got, expected) => prop_assert!(
                    false,
                    "score mismatch for node {}: got {:?}, expected {:?}",
                    id, got, expected
                ),
            }
            prop_assert_eq!(snapshot.prop(*id, "active"), active.map(ValueId::Bool));
        }
    }
}
