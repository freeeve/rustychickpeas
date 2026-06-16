//! Full-text index over string node properties: boolean retrieval + BM25.
//!
//! A lazy, label-scoped inverted index that mirrors the equality `prop_index`
//! on [`GraphSnapshot`](crate::graph_snapshot::GraphSnapshot): the first
//! `fts`/`fts_ranked` call for a `(label, key)` pair tokenizes that column's
//! strings into postings and caches the result; later queries reuse it.
//!
//! Each term's postings carry both a membership [`RoaringBitmap`] (surfaced as a
//! [`NodeSet`](crate::bitmap::NodeSet)) and per-document term frequencies, plus a
//! per-document length table. So:
//!   * [`FullTextField::query`] is **boolean AND** — `NodeSet` set algebra (`&`),
//!     composing with `nodes_with_label` for free.
//!   * [`FullTextField::query_ranked`] is **disjunctive BM25** top-k — a document
//!     scores for every query token it contains, ranked by relevance.
//!
//! Tokenization is lowercase + split on non-alphanumeric; stopword removal and
//! stemming are deferred.

use hashbrown::HashMap;
use roaring::RoaringBitmap;

use crate::bitmap::NodeSet;

/// BM25 term-frequency saturation parameter.
const BM25_K1: f32 = 1.2;
/// BM25 length-normalization parameter.
const BM25_B: f32 = 0.75;

/// Postings for one token: which nodes contain it (membership) and how often
/// (term frequency, for ranking).
#[derive(Debug, Default)]
struct TermPostings {
    /// Nodes whose indexed text contains this token (boolean membership).
    docs: RoaringBitmap,
    /// node -> number of occurrences of this token in that node's text.
    tf: HashMap<u32, u32>,
}

/// Inverted index for a single `(label, property)` text field.
#[derive(Debug, Default)]
pub struct FullTextField {
    /// token -> its postings.
    postings: HashMap<String, TermPostings>,
    /// node -> total token count of its indexed text (BM25 document length).
    doc_len: HashMap<u32, u32>,
    /// Sum of all document lengths (for the average, used by BM25).
    total_len: u64,
}

impl FullTextField {
    /// Build the index from already label-filtered `(node_id, text)` documents.
    pub fn build<'a>(docs: impl Iterator<Item = (u32, &'a str)>) -> Self {
        let mut postings: HashMap<String, TermPostings> = HashMap::new();
        let mut doc_len: HashMap<u32, u32> = HashMap::new();
        let mut total_len: u64 = 0;
        for (node, text) in docs {
            let mut len = 0u32;
            for token in tokenize(text) {
                len += 1;
                let tp = postings.entry(token).or_default();
                tp.docs.insert(node);
                *tp.tf.entry(node).or_insert(0) += 1;
            }
            if len > 0 {
                *doc_len.entry(node).or_insert(0) += len;
                total_len += u64::from(len);
            }
        }
        FullTextField {
            postings,
            doc_len,
            total_len,
        }
    }

    /// Nodes whose text contains **every** token in `query` (boolean AND). An
    /// empty query, or any token absent from this field, yields the empty set.
    pub fn query(&self, query: &str) -> NodeSet {
        let mut acc: Option<RoaringBitmap> = None;
        let mut saw_token = false;
        for token in tokenize(query) {
            saw_token = true;
            let Some(tp) = self.postings.get(&token) else {
                return NodeSet::empty();
            };
            acc = Some(match acc {
                Some(prev) => &prev & &tp.docs,
                None => tp.docs.clone(),
            });
            if acc.as_ref().is_some_and(RoaringBitmap::is_empty) {
                return NodeSet::empty();
            }
        }
        if saw_token {
            NodeSet::new(acc.unwrap_or_default())
        } else {
            NodeSet::empty()
        }
    }

    /// The top `k` nodes by BM25 relevance to `query` (disjunctive: a node
    /// scores for every query token it contains). Results are sorted by score
    /// descending, ties broken by ascending node id for determinism. An empty
    /// query, `k == 0`, or an empty field yields no results.
    pub fn query_ranked(&self, query: &str, k: usize) -> Vec<(u32, f32)> {
        let n = self.doc_len.len();
        if n == 0 || k == 0 {
            return Vec::new();
        }
        let avgdl = self.total_len as f32 / n as f32;

        let mut scores: HashMap<u32, f32> = HashMap::new();
        for token in tokenize(query) {
            let Some(tp) = self.postings.get(&token) else {
                continue;
            };
            let df = tp.docs.len() as f32;
            // Robertson/Spärck-Jones IDF (always non-negative).
            let idf = (1.0 + (n as f32 - df + 0.5) / (df + 0.5)).ln();
            for (&node, &raw_tf) in &tp.tf {
                let tf = raw_tf as f32;
                let dl = *self.doc_len.get(&node).unwrap_or(&0) as f32;
                let denom = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl);
                *scores.entry(node).or_insert(0.0) += idf * (tf * (BM25_K1 + 1.0)) / denom;
            }
        }

        let mut ranked: Vec<(u32, f32)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        ranked.truncate(k);
        ranked
    }

    /// Number of distinct indexed tokens (diagnostics/tests).
    pub fn term_count(&self) -> usize {
        self.postings.len()
    }

    /// Number of indexed documents (nodes with non-empty text).
    pub fn doc_count(&self) -> usize {
        self.doc_len.len()
    }
}

/// Tokenize `text` into lowercased, alphanumeric runs. Unicode-aware: splits on
/// every non-alphanumeric character. No stopword removal or stemming (v1).
pub fn tokenize(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|tok| !tok.is_empty())
        .map(str::to_lowercase)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(docs: &[(u32, &str)]) -> FullTextField {
        FullTextField::build(docs.iter().copied())
    }

    fn sorted_hits(f: &FullTextField, query: &str) -> Vec<u32> {
        let mut hits: Vec<u32> = f.query(query).iter().collect();
        hits.sort_unstable();
        hits
    }

    fn ranked_ids(f: &FullTextField, query: &str, k: usize) -> Vec<u32> {
        f.query_ranked(query, k).into_iter().map(|(n, _)| n).collect()
    }

    // ---- tokenizer + boolean retrieval ----

    #[test]
    fn tokenize_lowercases_and_splits_on_punct() {
        let toks: Vec<String> = tokenize("Hello, WORLD! it's foo_bar").collect();
        assert_eq!(toks, ["hello", "world", "it", "s", "foo", "bar"]);
    }

    #[test]
    fn single_term_returns_matching_nodes() {
        let f = field(&[(1, "the quick brown fox"), (2, "brown bear"), (3, "red fox")]);
        assert_eq!(sorted_hits(&f, "brown"), [1, 2]);
        assert_eq!(sorted_hits(&f, "fox"), [1, 3]);
    }

    #[test]
    fn multi_term_query_is_conjunctive() {
        let f = field(&[(1, "quick brown fox"), (2, "brown bear"), (3, "quick red fox")]);
        assert_eq!(sorted_hits(&f, "quick fox"), [1, 3]);
        assert_eq!(sorted_hits(&f, "brown bear"), [2]);
    }

    #[test]
    fn unknown_token_yields_empty() {
        let f = field(&[(1, "alpha beta")]);
        assert!(f.query("alpha gamma").is_empty());
        assert!(f.query("zzz").is_empty());
    }

    #[test]
    fn empty_or_punctuation_only_query_yields_empty() {
        let f = field(&[(1, "alpha")]);
        assert!(f.query("").is_empty());
        assert!(f.query("   ,. ").is_empty());
    }

    #[test]
    fn matching_is_case_insensitive() {
        let f = field(&[(1, "Rust Programming")]);
        assert_eq!(sorted_hits(&f, "RUST"), [1]);
        assert_eq!(sorted_hits(&f, "programming"), [1]);
    }

    #[test]
    fn repeated_token_indexes_node_once_in_membership() {
        let f = field(&[(7, "go go go")]);
        assert_eq!(sorted_hits(&f, "go"), [7]);
        assert_eq!(f.term_count(), 1);
        assert_eq!(f.doc_count(), 1);
    }

    #[test]
    fn empty_field_queries_empty() {
        let f = FullTextField::default();
        assert!(f.query("anything").is_empty());
        assert!(f.query_ranked("anything", 10).is_empty());
        assert_eq!(f.term_count(), 0);
    }

    // ---- BM25 ranking ----

    #[test]
    fn ranked_orders_by_term_frequency() {
        // Doc 1 mentions "apple" twice, doc 2 once -> doc 1 ranks first.
        let f = field(&[(1, "apple apple banana"), (2, "apple cherry"), (3, "banana date")]);
        assert_eq!(ranked_ids(&f, "apple", 10), [1, 2]);
    }

    #[test]
    fn ranked_is_disjunctive_and_rewards_more_matches() {
        let f = field(&[
            (1, "apple banana"), // both query terms
            (2, "apple cherry"), // one term
            (3, "banana date"),  // one term
        ]);
        // Doc 1 contains both terms, so it must rank above the single-term docs.
        assert_eq!(ranked_ids(&f, "apple banana", 10)[0], 1);
        // All three are returned (disjunctive).
        assert_eq!(ranked_ids(&f, "apple banana", 10).len(), 3);
    }

    #[test]
    fn ranked_respects_top_k() {
        let f = field(&[(1, "apple apple"), (2, "apple"), (3, "apple")]);
        assert_eq!(ranked_ids(&f, "apple", 1), [1]);
        assert_eq!(ranked_ids(&f, "apple", 2).len(), 2);
    }

    #[test]
    fn rarer_term_outscores_common_term() {
        // "zebra" appears in 1 of 4 docs (high idf); "common" in all 4 (low idf).
        let f = field(&[
            (1, "common zebra"),
            (2, "common"),
            (3, "common"),
            (4, "common"),
        ]);
        let zebra = f.query_ranked("zebra", 10)[0].1;
        let common = f.query_ranked("common", 10)[0].1;
        assert!(zebra > common, "rare zebra {zebra} should outscore common {common}");
    }

    #[test]
    fn ranked_ties_break_by_node_id() {
        let f = field(&[(5, "kiwi"), (3, "kiwi"), (9, "kiwi")]);
        // Identical docs -> identical scores -> ascending node id order.
        assert_eq!(ranked_ids(&f, "kiwi", 10), [3, 5, 9]);
    }

    #[test]
    fn ranked_empty_query_or_zero_k_is_empty() {
        let f = field(&[(1, "apple")]);
        assert!(f.query_ranked("", 10).is_empty());
        assert!(f.query_ranked("apple", 0).is_empty());
        assert!(f.query_ranked("missing", 10).is_empty());
    }

    /// Invariant fuzz: tokenization never panics and emits non-empty,
    /// whitespace-free tokens across pathological inputs.
    #[test]
    fn tokenize_invariants_hold_for_varied_inputs() {
        let big = "a".repeat(5000);
        let inputs: Vec<&str> = vec![
            "",
            " ",
            "\0\t\n\r",
            "...,,,###",
            "MiXeD CaSe",
            "🚀rocket🚀science🚀",
            "Ünïcödé wörds café",
            "123 abc_def 456",
            "trailing-dash-",
            "-leading",
            &big,
        ];
        for input in inputs {
            for tok in tokenize(input) {
                assert!(!tok.is_empty(), "no empty tokens for {input:?}");
                assert!(
                    !tok.chars().any(char::is_whitespace),
                    "token {tok:?} has whitespace"
                );
            }
        }
    }
}
