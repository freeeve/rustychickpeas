//! Boolean full-text index over string node properties.
//!
//! A lazy, label-scoped inverted index that mirrors the equality `prop_index`
//! on [`GraphSnapshot`](crate::graph_snapshot::GraphSnapshot): the first
//! `fts(label, key, query)` call for a `(label, key)` pair tokenizes that
//! column's strings into `token -> postings` and caches the result; later
//! queries reuse it. Postings are [`RoaringBitmap`]s surfaced as
//! [`NodeSet`](crate::bitmap::NodeSet), so multi-term AND is the existing
//! `NodeSet` set algebra (`&`) and a result composes with `nodes_with_label`
//! and other node sets for free.
//!
//! v1 is boolean retrieval only (no relevance ranking). Tokenization is
//! lowercase + split on non-alphanumeric; stopword removal and stemming are
//! deferred.

use hashbrown::HashMap;
use roaring::RoaringBitmap;

use crate::bitmap::NodeSet;

/// Inverted index for a single `(label, property)` text field.
#[derive(Debug, Default)]
pub struct FullTextField {
    /// token -> the nodes whose indexed property text contains that token.
    postings: HashMap<String, RoaringBitmap>,
}

impl FullTextField {
    /// Build the index from already label-filtered `(node_id, text)` documents.
    pub fn build<'a>(docs: impl Iterator<Item = (u32, &'a str)>) -> Self {
        let mut postings: HashMap<String, RoaringBitmap> = HashMap::new();
        for (node, text) in docs {
            for token in tokenize(text) {
                postings.entry(token).or_default().insert(node);
            }
        }
        FullTextField { postings }
    }

    /// Nodes whose text contains **every** token in `query` (boolean AND). An
    /// empty query, or any token absent from this field, yields the empty set.
    pub fn query(&self, query: &str) -> NodeSet {
        let mut acc: Option<RoaringBitmap> = None;
        let mut saw_token = false;
        for token in tokenize(query) {
            saw_token = true;
            let Some(postings) = self.postings.get(&token) else {
                return NodeSet::empty();
            };
            acc = Some(match acc {
                Some(prev) => &prev & postings,
                None => postings.clone(),
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

    /// Number of distinct indexed tokens (diagnostics/tests).
    pub fn term_count(&self) -> usize {
        self.postings.len()
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
    fn repeated_token_indexes_node_once() {
        let f = field(&[(7, "go go go")]);
        assert_eq!(sorted_hits(&f, "go"), [7]);
        assert_eq!(f.term_count(), 1);
    }

    #[test]
    fn empty_field_queries_empty() {
        let f = FullTextField::default();
        assert!(f.query("anything").is_empty());
        assert_eq!(f.term_count(), 0);
    }

    /// Invariant fuzz: tokenization never panics and emits non-empty,
    /// whitespace-free tokens across pathological inputs (unicode, control
    /// chars, all-punctuation, very long).
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
