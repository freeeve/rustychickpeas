//! Generates the demo dataset for the browser example: a small social graph
//! as `demo/graph.rcpg` plus an RRSR record store (`demo/records.idx` /
//! `demo/records.bin`) with one JSON record per node.
//!
//! Run from the rustychickpeas-reader directory:
//! ```bash
//! cargo run --example generate_demo
//! ```

use rustychickpeas_core::graph_builder::GraphBuilder;
use rustychickpeas_format::rrsr;

const PEOPLE: u32 = 60;
const COMPANIES: u32 = 8;

fn main() {
    let mut builder = GraphBuilder::with_version("demo-v1", None, None);

    // IDs 0..PEOPLE are people, PEOPLE..PEOPLE+COMPANIES are companies
    for id in 0..PEOPLE {
        builder.add_node(Some(id), &["Person"]).unwrap();
        builder
            .set_prop_str(id, "name", &format!("person-{}", id))
            .unwrap();
    }
    for c in 0..COMPANIES {
        let id = PEOPLE + c;
        builder.add_node(Some(id), &["Company"]).unwrap();
        builder
            .set_prop_str(id, "name", &format!("company-{}", c))
            .unwrap();
    }

    // Deterministic small-world-ish wiring
    for i in 0..PEOPLE {
        builder.add_rel(i, (i + 1) % PEOPLE, "KNOWS").unwrap();
        builder.add_rel(i, (i * 7 + 3) % PEOPLE, "KNOWS").unwrap();
        builder
            .add_rel(i, PEOPLE + (i % COMPANIES), "WORKS_FOR")
            .unwrap();
    }
    for c in 0..COMPANIES.saturating_sub(1) {
        builder
            .add_rel(PEOPLE + c, PEOPLE + c + 1, "PARTNER_OF")
            .unwrap();
    }

    let snapshot = builder.finalize(None);

    std::fs::create_dir_all("demo").unwrap();
    snapshot.write_rcpg_file("demo/graph.rcpg").unwrap();

    // One JSON record per node ID (the "heavy" payload that stays remote)
    let records: Vec<Vec<u8>> = (0..PEOPLE + COMPANIES)
        .map(|id| {
            let (kind, name) = if id < PEOPLE {
                ("person", format!("person-{}", id))
            } else {
                ("company", format!("company-{}", id - PEOPLE))
            };
            format!(
                r#"{{"id":{},"kind":"{}","name":"{}","bio":"This record lives in records.bin and was fetched with an HTTP Range request."}}"#,
                id, kind, name
            )
            .into_bytes()
        })
        .collect();

    let mut idx = std::io::BufWriter::new(std::fs::File::create("demo/records.idx").unwrap());
    let mut bin = std::io::BufWriter::new(std::fs::File::create("demo/records.bin").unwrap());
    rrsr::write(&mut idx, &mut bin, records.iter().map(|r| r.as_slice())).unwrap();

    println!(
        "wrote demo/graph.rcpg ({} nodes, {} rels), demo/records.idx + records.bin ({} records)",
        snapshot.n_nodes,
        snapshot.n_rels,
        records.len()
    );
}
