//! Generates the search-then-traverse browser demo dataset: a synthetic
//! citation corpus run through the shared-ID pipeline, producing
//! `index.rrs` (search), `graph.rcpg` (topology), and `records.{idx,bin}`
//! over one rank-ordered ID space.
//!
//! Run from the workspace root (writes to the reader's demo dir by default):
//! ```bash
//! cargo run -p rustychickpeas-pipeline --example generate_shared_demo
//! # or to a custom dir:
//! cargo run -p rustychickpeas-pipeline --example generate_shared_demo -- some/out/dir
//! ```

use std::path::PathBuf;

use rustychickpeas_pipeline::{build_shared_corpus, BuildConfig, Document};

/// Number of synthetic works in the corpus.
const N: u32 = 300;
/// Vocabulary that titles are drawn from, so trigram search has real hits.
const WORDS: &[&str] = &[
    "graph",
    "database",
    "neural",
    "network",
    "query",
    "optimization",
    "distributed",
    "systems",
    "learning",
    "vector",
    "search",
    "index",
    "memory",
    "parallel",
    "compression",
    "traversal",
    "ranking",
    "embedding",
    "cache",
    "storage",
];

/// Deterministic LCG so the demo is reproducible without a dependency.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

/// A 3-4 word title chosen deterministically from the vocabulary by work index.
fn title(s: u32) -> String {
    let w = |k: u32| WORDS[(k as usize) % WORDS.len()];
    format!(
        "{} {} {} {}",
        w(s * 3),
        w(s * 7 + 1),
        w(s * 5 + 2),
        w(s * 11 + 4)
    )
}

fn main() {
    let out_dir: PathBuf = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("rustychickpeas-reader/demo"));

    let mut rng = Lcg(0x0BAD_C0DE_1234_5678);

    // Works are emitted most-popular first: popularity is a strictly decreasing
    // Zipf-like curve, so the pipeline's rank sort assigns id == index s. Each
    // work cites a few earlier (more popular) works — uniform over `0..s`, which
    // makes popular (low-id) works accumulate the most citations.
    let docs: Vec<Document> = (0..N)
        .map(|s| {
            let cited_by = 100_000 / (s as i64 + 1);
            let title = title(s);
            let degree = if s == 0 { 0 } else { (rng.next() % 8) as u32 };
            let mut links: Vec<String> = (0..degree)
                .map(|_| format!("W{}", rng.next() as u32 % s.max(1)))
                .collect();
            links.sort();
            links.dedup();
            let record =
                format!(r#"{{"id":"W{s}","title":"{title}","kind":"work","cited_by":{cited_by}}}"#)
                    .into_bytes();
            Document {
                key: format!("W{s}"),
                text: title,
                rank: cited_by,
                links,
                record,
            }
        })
        .collect();

    let manifest =
        build_shared_corpus(docs, &out_dir, &BuildConfig::default()).expect("build shared corpus");

    println!(
        "wrote {}/index.rrs + graph.rcpg + records.{{idx,bin}}: {} works, {} citation rels over one id space",
        out_dir.display(),
        manifest.n_docs,
        manifest.n_rels
    );
}
