//! Shared-ID build pipeline.
//!
//! From ONE ranked corpus this emits three artifact families over ONE
//! contiguous `u32` ID space:
//!
//! - a roaringrange **search index** (`index.rrs`, trigram),
//! - an **RCPG graph** (`graph.rcpg`, topology-only), and
//! - an **RRSR record store** (`records.idx` / `records.bin`).
//!
//! Documents are assigned IDs `0..N` in **descending static rank** (id 0 =
//! highest rank), matching roaringrange's rank-ordered doc-ID space. Because
//! the same ID indexes all three, a search hit's doc ID *is* the graph node ID
//! *is* the record ID — the browser flow searches, traverses, and fetches the
//! record with no ID remapping.
//!
//! ```no_run
//! use rustychickpeas_pipeline::{build_shared_corpus, BuildConfig, Document};
//! use std::path::Path;
//!
//! let docs = vec![Document {
//!     key: "W1".into(),
//!     text: "graph databases".into(),
//!     rank: 100,
//!     links: vec![],
//!     record: br#"{"id":"W1"}"#.to_vec(),
//! }];
//! build_shared_corpus(docs, Path::new("out"), &BuildConfig::default()).unwrap();
//! ```

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use roaring::RoaringBitmap;
use rustychickpeas_core::graph_builder::GraphBuilder;
use rustychickpeas_format::{rcpg, rrsr};

/// Pipeline result; errors box the underlying codec/builder error.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// One document in the corpus, addressed by a caller-chosen external key.
pub struct Document {
    /// Stable external identifier (e.g. an OpenAlex work ID). [`Document::links`]
    /// reference these keys, not the assigned `u32` IDs.
    pub key: String,
    /// Free text indexed for trigram search (e.g. the title).
    pub text: String,
    /// Static rank score; higher = more important. Documents are assigned IDs
    /// in DESCENDING score, so the highest-scored document becomes id 0.
    pub rank: i64,
    /// External keys this document links to (e.g. cited works). Links to keys
    /// absent from the corpus are dropped.
    pub links: Vec<String>,
    /// Payload kept remote in the RRSR store (e.g. a JSON record).
    pub record: Vec<u8>,
}

/// Knobs for the build; [`BuildConfig::default`] suits a citation corpus.
pub struct BuildConfig {
    /// Relationship type for the link rels (e.g. `"CITES"`).
    pub rel_type: String,
    /// Node label for every document node (e.g. `"Work"`).
    pub node_label: String,
    /// Trigram size for the search index; the reader queries with the same size
    /// (it is stored in the `.rrs` header).
    pub gram_size: u16,
    /// RCPG snapshot version tag.
    pub version: String,
}

impl Default for BuildConfig {
    fn default() -> Self {
        BuildConfig {
            rel_type: "CITES".to_string(),
            node_label: "Work".to_string(),
            gram_size: 3,
            version: "shared-v1".to_string(),
        }
    }
}

/// What the build produced.
pub struct Manifest {
    /// Number of documents (= node count = record count).
    pub n_docs: u32,
    /// Number of link rels retained (links to unknown keys are dropped).
    pub n_rels: u64,
    /// External key -> assigned `u32` ID, for resolving inputs against the
    /// shared ID space after the build.
    pub id_of_key: HashMap<String, u32>,
}

/// The artifact byte streams plus the [`Manifest`]. Use this when you want the
/// bytes in memory (tests, uploads); [`build_shared_corpus`] writes them to
/// files instead.
pub struct Artifacts {
    pub index_rrs: Vec<u8>,
    pub graph_rcpg: Vec<u8>,
    pub records_idx: Vec<u8>,
    pub records_bin: Vec<u8>,
    pub manifest: Manifest,
}

/// Build every artifact in memory. Documents are rank-sorted and assigned IDs
/// `0..N` before any artifact is written, so all three share that ID space.
pub fn build_artifacts(mut docs: Vec<Document>, cfg: &BuildConfig) -> Result<Artifacts> {
    // 1. Rank-order (descending) and assign contiguous IDs. A stable sort keeps
    //    equal-rank inputs in their original order for reproducibility.
    docs.sort_by_key(|d| std::cmp::Reverse(d.rank));
    let id_of_key: HashMap<String, u32> = docs
        .iter()
        .enumerate()
        .map(|(i, d)| (d.key.clone(), i as u32))
        .collect();
    let n_docs = docs.len() as u32;

    // 2. Search index: trigram postings keyed by the assigned ID.
    let mut postings: HashMap<u64, RoaringBitmap> = HashMap::new();
    for (id, d) in docs.iter().enumerate() {
        for key in roaringrange::ngram_keys(&d.text, cfg.gram_size as usize) {
            postings.entry(key).or_default().insert(id as u32);
        }
    }
    let entries: Vec<(u64, Vec<u8>)> = postings
        .into_iter()
        .map(|(k, bm)| (k, roaringrange::build::serialize_posting(&bm)))
        .collect();
    let mut index_rrs = Vec::new();
    roaringrange::build::write_index(&mut index_rrs, cfg.gram_size, 0, entries)?;

    // 3. Graph (topology-only): node ID == doc ID; rels resolved key -> ID.
    let mut builder = GraphBuilder::new(Some(n_docs as usize), None).with_version(&cfg.version);
    let label = [cfg.node_label.as_str()];
    for id in 0..n_docs {
        builder.add_node(Some(id), &label)?;
    }
    let mut n_rels = 0u64;
    for (id, d) in docs.iter().enumerate() {
        for link in &d.links {
            if let Some(&dst) = id_of_key.get(link) {
                builder.add_relationship(id as u32, dst, &cfg.rel_type)?;
                n_rels += 1;
            }
        }
    }
    let snapshot = builder.finalize(None);
    let mut graph_rcpg = Vec::new();
    snapshot.write_rcpg_with(&mut graph_rcpg, &rcpg::WriteOptions::topology_only())?;

    // 4. Records: payloads concatenated in ID order.
    let mut records_idx = Vec::new();
    let mut records_bin = Vec::new();
    rrsr::write(
        &mut records_idx,
        &mut records_bin,
        docs.iter().map(|d| d.record.as_slice()),
    )?;

    Ok(Artifacts {
        index_rrs,
        graph_rcpg,
        records_idx,
        records_bin,
        manifest: Manifest {
            n_docs,
            n_rels,
            id_of_key,
        },
    })
}

/// Build every artifact and write `index.rrs`, `graph.rcpg`, `records.idx`,
/// and `records.bin` into `out_dir` (created if absent). Returns the
/// [`Manifest`].
pub fn build_shared_corpus(
    docs: Vec<Document>,
    out_dir: &Path,
    cfg: &BuildConfig,
) -> Result<Manifest> {
    let a = build_artifacts(docs, cfg)?;
    std::fs::create_dir_all(out_dir)?;
    write_file(&out_dir.join("index.rrs"), &a.index_rrs)?;
    write_file(&out_dir.join("graph.rcpg"), &a.graph_rcpg)?;
    write_file(&out_dir.join("records.idx"), &a.records_idx)?;
    write_file(&out_dir.join("records.bin"), &a.records_bin)?;
    Ok(a.manifest)
}

fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    f.write_all(bytes)?;
    f.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    fn doc(key: &str, text: &str, rank: i64, links: &[&str]) -> Document {
        Document {
            key: key.to_string(),
            text: text.to_string(),
            rank,
            links: links.iter().map(|s| s.to_string()).collect(),
            record: format!(r#"{{"key":"{key}","title":"{text}"}}"#).into_bytes(),
        }
    }

    /// The whole point: a search hit's doc ID indexes directly into the graph
    /// and the record store. Build a tiny citation corpus and prove the ID is
    /// shared across all three artifacts.
    #[test]
    fn shared_id_alignment() {
        // Ranks chosen so the assigned IDs are deterministic: greenfield=0
        // (rank 300), then graphdb=1 (200), then sqlite=2 (100).
        let docs = vec![
            doc("graphdb", "graph database traversal", 200, &["sqlite"]),
            doc(
                "greenfield",
                "greenfield architecture notes",
                300,
                &["graphdb", "sqlite"],
            ),
            doc("sqlite", "embedded sql engine", 100, &[]),
        ];
        let a = build_artifacts(docs, &BuildConfig::default()).unwrap();
        let m = &a.manifest;

        // 1. IDs assigned in descending rank.
        assert_eq!(m.n_docs, 3);
        assert_eq!(m.id_of_key["greenfield"], 0);
        assert_eq!(m.id_of_key["graphdb"], 1);
        assert_eq!(m.id_of_key["sqlite"], 2);
        assert_eq!(m.n_rels, 3); // greenfield->{graphdb,sqlite}, graphdb->sqlite

        // 2. Graph: node ID == doc ID, rels resolved against the same space.
        let g = rcpg::parse(&a.graph_rcpg).unwrap();
        assert_eq!(g.n_nodes, 3);
        // greenfield (id 0) cites graphdb (id 1) and sqlite (id 2).
        let mut out0 = g.out_neighbors(0).to_vec();
        out0.sort_unstable();
        assert_eq!(out0, vec![1, 2]);
        // graphdb (id 1) cites sqlite (id 2).
        assert_eq!(g.out_neighbors(1), &[2]);

        // 3. Records: record ID == doc ID, payload in ID order.
        let ri = rrsr::RecordIndex::parse(&a.records_idx).unwrap();
        assert_eq!(ri.len(), 3);
        let (s, e) = ri.record_range(0).unwrap();
        let rec0 = std::str::from_utf8(&a.records_bin[s as usize..e as usize]).unwrap();
        assert!(rec0.contains(r#""key":"greenfield""#), "record 0 = {rec0}");

        // 4. Search: a hit's doc ID lands on the right node and record.
        let idx = block_on(roaringrange::Index::open(roaringrange::MemoryFetch::new(
            a.index_rrs.clone(),
        )))
        .unwrap();
        let hits = block_on(idx.search("graph database", 10)).unwrap();
        assert!(
            hits.contains(&m.id_of_key["graphdb"]),
            "expected graphdb (id {}) in hits {hits:?}",
            m.id_of_key["graphdb"]
        );
        // That same ID has an outgoing CITES rel and its own record.
        let hit = m.id_of_key["graphdb"];
        assert!(!g.out_neighbors(hit).is_empty());
        assert!(ri.record_range(hit).is_some());
    }
}
