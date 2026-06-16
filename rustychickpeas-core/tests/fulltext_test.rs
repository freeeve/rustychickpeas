//! End-to-end full-text search over a finalized `GraphSnapshot`, exercising the
//! lazy index build (column scan + label filter + interner resolve), boolean
//! query semantics, label scoping, and composition with `nodes_with_label`.

use rustychickpeas_core::GraphBuilder;

fn docs_graph() -> rustychickpeas_core::GraphSnapshot {
    let mut b = GraphBuilder::new(Some(8), Some(0));
    b.add_node(Some(0), &["Doc"]).unwrap();
    b.set_prop_str(0, "body", "The quick brown fox").unwrap();
    b.add_node(Some(1), &["Doc"]).unwrap();
    b.set_prop_str(1, "body", "Lazy brown dog").unwrap();
    b.add_node(Some(2), &["Doc"]).unwrap();
    b.set_prop_str(2, "body", "Quick red fox").unwrap();
    // A non-Doc node that would match, to prove label scoping.
    b.add_node(Some(3), &["Other"]).unwrap();
    b.set_prop_str(3, "body", "quick brown irrelevant").unwrap();
    b.finalize(None)
}

fn hits(g: &rustychickpeas_core::GraphSnapshot, query: &str) -> Vec<u32> {
    let mut h: Vec<u32> = g.fts("Doc", "body", query).iter().collect();
    h.sort_unstable();
    h
}

#[test]
fn single_term_matches_documents() {
    let g = docs_graph();
    assert_eq!(hits(&g, "brown"), [0, 1]);
    assert_eq!(hits(&g, "fox"), [0, 2]);
}

#[test]
fn multi_term_is_conjunctive_and_case_insensitive() {
    let g = docs_graph();
    assert_eq!(hits(&g, "QUICK fox"), [0, 2]);
    assert_eq!(hits(&g, "lazy DOG"), [1]);
}

#[test]
fn results_are_label_scoped() {
    let g = docs_graph();
    // Node 3 ("Other") contains "irrelevant" but must not appear under "Doc".
    assert!(g.fts("Doc", "body", "irrelevant").is_empty());
}

#[test]
fn composes_with_label_set_via_intersection() {
    let g = docs_graph();
    let doc_nodes = g.nodes_with_label("Doc").unwrap();
    let mut composed: Vec<u32> = (&g.fts("Doc", "body", "quick") & doc_nodes)
        .iter()
        .collect();
    composed.sort_unstable();
    assert_eq!(composed, [0, 2]);
}

#[test]
fn unknown_label_key_or_term_is_empty() {
    let g = docs_graph();
    assert!(g.fts("Nope", "body", "fox").is_empty());
    assert!(g.fts("Doc", "nope", "fox").is_empty());
    assert!(g.fts("Doc", "body", "missingword").is_empty());
    assert!(g.fts("Doc", "body", "").is_empty());
}

#[test]
fn second_query_uses_cached_index() {
    let g = docs_graph();
    // First call builds and caches; second must return the same result.
    assert_eq!(hits(&g, "brown"), [0, 1]);
    assert_eq!(hits(&g, "brown"), [0, 1]);
    assert_eq!(hits(&g, "fox"), [0, 2]);
}
