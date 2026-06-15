//! LDBC Social Network Benchmark - Business Intelligence (BI) Queries
//!
//! Criterion benchmarks for LDBC SNB BI workload queries.
//! These benchmarks require LDBC data files (see tests/ldbc_snb_bi_benchmark.rs for data setup).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rustychickpeas_core::types::Direction;
use rustychickpeas_core::{GraphBuilder, GraphSnapshot};
use std::env;
use std::sync::OnceLock;

/// Build a synthetic graph with LDBC SNB-like structure so the BI query
/// benchmarks have data to run against in CI without the multi-GB dataset.
///
/// The generated graph has Person, Post, Comment and Tag nodes wired with
/// `hasCreator` (Person -> Post/Comment), `hasTag` (Post/Comment -> Tag) and
/// `hasInterest` (Person -> Tag) relationships, matching what the BI queries
/// traverse. Tag selection is deliberately skewed so a small subset of tags
/// dominates, mimicking the power-law tag popularity that BI3/BI6 surface.
/// Sizes scale linearly with the `LDBC_SYNTH_SCALE` environment variable
/// (default 1); generation is fully deterministic for stable measurements.
///
/// To benchmark against a real LDBC SNB dataset instead, use the integration
/// test `tests/ldbc_snb_bi_benchmark.rs` with `LDBC_DATA_DIR` pointing at the
/// Parquet snapshot.
fn build_synthetic_ldbc_graph() -> GraphSnapshot {
    let scale: usize = env::var("LDBC_SYNTH_SCALE")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&s| s >= 1)
        .unwrap_or(1);

    let n_person = 2_000 * scale;
    let n_post = 5_000 * scale;
    let n_comment = 15_000 * scale;
    let n_tag = 200 * scale;
    let tags_per_message = 3;
    let interests_per_person = 5;

    // Contiguous id ranges per node type.
    let person_base = 0u32;
    let post_base = person_base + n_person as u32;
    let comment_base = post_base + n_post as u32;
    let tag_base = comment_base + n_comment as u32;
    let total_nodes = n_person + n_post + n_comment + n_tag;

    let mut builder = GraphBuilder::new(Some(total_nodes), Some(total_nodes * 6));

    for i in 0..n_person {
        builder
            .add_node(Some(person_base + i as u32), &["Person"])
            .unwrap();
    }
    for i in 0..n_post {
        builder
            .add_node(Some(post_base + i as u32), &["Post"])
            .unwrap();
    }
    for i in 0..n_comment {
        builder
            .add_node(Some(comment_base + i as u32), &["Comment"])
            .unwrap();
    }
    for i in 0..n_tag {
        builder
            .add_node(Some(tag_base + i as u32), &["Tag"])
            .unwrap();
    }

    // Deterministic, skewed tag picker: ~70% of references hit the most popular
    // quarter of tags, the rest spread across the full range.
    let popular_tags = (n_tag / 4).max(1);
    let pick_tag = |seed: usize, k: usize| -> u32 {
        let v = seed
            .wrapping_mul(2_654_435_761)
            .wrapping_add(k.wrapping_mul(40_503));
        let idx = if v % 10 < 7 {
            v % popular_tags
        } else {
            v % n_tag
        };
        tag_base + idx as u32
    };

    // Person -> Tag interests.
    for p in 0..n_person {
        for k in 0..interests_per_person {
            let tag = pick_tag(p, k + 101);
            builder
                .add_relationship(person_base + p as u32, tag, "hasInterest")
                .unwrap();
        }
    }

    // Posts: a creator (Person -> Post) and a few tags (Post -> Tag).
    for m in 0..n_post {
        let post = post_base + m as u32;
        let creator = person_base + (m % n_person) as u32;
        builder
            .add_relationship(creator, post, "hasCreator")
            .unwrap();
        for k in 0..tags_per_message {
            let tag = pick_tag(m, k);
            builder.add_relationship(post, tag, "hasTag").unwrap();
        }
    }

    // Comments: a creator (Person -> Comment) and a few tags (Comment -> Tag).
    for m in 0..n_comment {
        let comment = comment_base + m as u32;
        let creator = person_base + ((m * 7 + 3) % n_person) as u32;
        builder
            .add_relationship(creator, comment, "hasCreator")
            .unwrap();
        for k in 0..tags_per_message {
            let tag = pick_tag(m + 9_973, k);
            builder.add_relationship(comment, tag, "hasTag").unwrap();
        }
    }

    builder.finalize(None)
}

/// Global graph cache - built once and reused across benchmarks
static GRAPH_CACHE: OnceLock<GraphSnapshot> = OnceLock::new();

fn get_ldbc_graph() -> &'static GraphSnapshot {
    GRAPH_CACHE.get_or_init(build_synthetic_ldbc_graph)
}

fn get_criterion() -> Criterion {
    let mut criterion = Criterion::default();

    if let Ok(baseline) = env::var("BENCHMARK_BASELINE") {
        criterion = criterion.save_baseline(baseline);
    }

    criterion
}

/// BI1: Tag Evolution
/// Find tags that are used together in posts/comments
fn bi1_tag_evolution_benchmark(c: &mut Criterion) {
    let graph = get_ldbc_graph();

    let mut group = c.benchmark_group("ldbc_snb_bi1_tag_evolution");

    group.bench_function("query", |b| {
        b.iter(|| {
            let posts = graph
                .nodes_with_label("Post")
                .map(|s| s.iter().collect::<Vec<_>>())
                .unwrap_or_default();
            let comments = graph
                .nodes_with_label("Comment")
                .map(|s| s.iter().collect::<Vec<_>>())
                .unwrap_or_default();

            let mut tag_pairs: std::collections::HashMap<(u32, u32), u32> =
                std::collections::HashMap::new();

            for &post_id in &posts {
                let post_tags =
                    graph.neighbors_by_type(black_box(post_id), Direction::Outgoing, &["hasTag"]);

                for i in 0..post_tags.len() {
                    for j in (i + 1)..post_tags.len() {
                        let pair = if post_tags[i] < post_tags[j] {
                            (post_tags[i], post_tags[j])
                        } else {
                            (post_tags[j], post_tags[i])
                        };
                        *tag_pairs.entry(pair).or_insert(0) += 1;
                    }
                }
            }

            for &comment_id in &comments {
                let comment_tags = graph.neighbors_by_type(
                    black_box(comment_id),
                    Direction::Outgoing,
                    &["hasTag"],
                );

                for i in 0..comment_tags.len() {
                    for j in (i + 1)..comment_tags.len() {
                        let pair = if comment_tags[i] < comment_tags[j] {
                            (comment_tags[i], comment_tags[j])
                        } else {
                            (comment_tags[j], comment_tags[i])
                        };
                        *tag_pairs.entry(pair).or_insert(0) += 1;
                    }
                }
            }

            black_box(tag_pairs);
        });
    });

    group.finish();
}

/// BI2: Tag Person Path
/// Find paths between persons through shared tags
fn bi2_tag_person_path_benchmark(c: &mut Criterion) {
    let graph = get_ldbc_graph();

    let mut group = c.benchmark_group("ldbc_snb_bi2_tag_person_path");

    group.bench_function("query", |b| {
        b.iter(|| {
            let persons = graph
                .nodes_with_label("Person")
                .map(|s| s.iter().collect::<Vec<_>>())
                .unwrap_or_default();
            let mut paths: Vec<(u32, u32)> = Vec::new();

            // Find paths through shared tags (simplified - just find persons with shared tags)
            for i in 0..persons.len().min(100) {
                for j in (i + 1)..persons.len().min(100) {
                    let person1_tags = graph.neighbors_by_type(
                        black_box(persons[i]),
                        Direction::Outgoing,
                        &["hasInterest"],
                    );
                    let person2_tags = graph.neighbors_by_type(
                        black_box(persons[j]),
                        Direction::Outgoing,
                        &["hasInterest"],
                    );

                    // Check for shared tags
                    for &tag1 in &person1_tags {
                        if person2_tags.contains(&tag1) {
                            paths.push((persons[i], persons[j]));
                            break;
                        }
                    }
                }
            }

            black_box(paths);
        });
    });

    group.finish();
}

/// BI3: Popular Topics
/// Find the most popular tags (by number of posts/comments)
fn bi3_popular_topics_benchmark(c: &mut Criterion) {
    let graph = get_ldbc_graph();

    let mut group = c.benchmark_group("ldbc_snb_bi3_popular_topics");

    group.bench_function("query", |b| {
        b.iter(|| {
            let mut tag_counts: std::collections::HashMap<u32, u32> =
                std::collections::HashMap::new();

            if let Some(posts) = graph.nodes_with_label("Post") {
                for post_id in posts.iter() {
                    let tags = graph.neighbors_by_type(
                        black_box(post_id),
                        Direction::Outgoing,
                        &["hasTag"],
                    );
                    for tag in tags {
                        *tag_counts.entry(tag).or_insert(0) += 1;
                    }
                }
            }

            if let Some(comments) = graph.nodes_with_label("Comment") {
                for comment_id in comments.iter() {
                    let tags = graph.neighbors_by_type(
                        black_box(comment_id),
                        Direction::Outgoing,
                        &["hasTag"],
                    );
                    for tag in tags {
                        *tag_counts.entry(tag).or_insert(0) += 1;
                    }
                }
            }

            let mut tag_vec: Vec<(u32, u32)> = tag_counts.into_iter().collect();
            tag_vec.sort_by_key(|e| std::cmp::Reverse(e.1));
            let top_tags: Vec<(u32, u32)> = tag_vec.into_iter().take(10).collect();

            black_box(top_tags);
        });
    });

    group.finish();
}

/// BI4: Top Commenters
/// Find persons who have made the most comments
fn bi4_top_commenters_benchmark(c: &mut Criterion) {
    let graph = get_ldbc_graph();

    let mut group = c.benchmark_group("ldbc_snb_bi4_top_commenters");

    group.bench_function("query", |b| {
        b.iter(|| {
            let mut person_comment_counts: std::collections::HashMap<u32, u32> =
                std::collections::HashMap::new();

            if let Some(comments) = graph.nodes_with_label("Comment") {
                for comment_id in comments.iter() {
                    let creators = graph.neighbors(black_box(comment_id), Direction::Incoming);

                    for &creator_id in &creators {
                        if let Some(persons) = graph.nodes_with_label("Person") {
                            if persons.contains(creator_id) {
                                *person_comment_counts.entry(creator_id).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }

            let mut person_vec: Vec<(u32, u32)> = person_comment_counts.into_iter().collect();
            person_vec.sort_by_key(|e| std::cmp::Reverse(e.1));
            let top_commenters: Vec<(u32, u32)> = person_vec.into_iter().take(10).collect();

            black_box(top_commenters);
        });
    });

    group.finish();
}

/// BI5: Active Users
/// Find persons with the most posts
fn bi5_active_users_benchmark(c: &mut Criterion) {
    let graph = get_ldbc_graph();

    let mut group = c.benchmark_group("ldbc_snb_bi5_active_users");

    group.bench_function("query", |b| {
        b.iter(|| {
            let mut person_post_counts: std::collections::HashMap<u32, u32> =
                std::collections::HashMap::new();

            if let Some(posts) = graph.nodes_with_label("Post") {
                for post_id in posts.iter() {
                    let creators = graph.neighbors(black_box(post_id), Direction::Incoming);

                    for &creator_id in &creators {
                        if let Some(persons) = graph.nodes_with_label("Person") {
                            if persons.contains(creator_id) {
                                *person_post_counts.entry(creator_id).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }

            let mut person_vec: Vec<(u32, u32)> = person_post_counts.into_iter().collect();
            person_vec.sort_by_key(|e| std::cmp::Reverse(e.1));
            let top_users: Vec<(u32, u32)> = person_vec.into_iter().take(10).collect();

            black_box(top_users);
        });
    });

    group.finish();
}

/// BI6: Tag Co-occurrence
/// Find tags that frequently appear together
fn bi6_tag_cooccurrence_benchmark(c: &mut Criterion) {
    let graph = get_ldbc_graph();

    let mut group = c.benchmark_group("ldbc_snb_bi6_tag_cooccurrence");

    group.bench_function("query", |b| {
        b.iter(|| {
            let mut cooccurrence: std::collections::HashMap<(u32, u32), u32> =
                std::collections::HashMap::new();

            if let Some(posts) = graph.nodes_with_label("Post") {
                for post_id in posts.iter() {
                    let tags = graph.neighbors_by_type(
                        black_box(post_id),
                        Direction::Outgoing,
                        &["hasTag"],
                    );

                    for i in 0..tags.len() {
                        for j in (i + 1)..tags.len() {
                            let pair = if tags[i] < tags[j] {
                                (tags[i], tags[j])
                            } else {
                                (tags[j], tags[i])
                            };
                            *cooccurrence.entry(pair).or_insert(0) += 1;
                        }
                    }
                }
            }

            let mut cooccurrence_vec: Vec<((u32, u32), u32)> = cooccurrence.into_iter().collect();
            cooccurrence_vec.sort_by_key(|e| std::cmp::Reverse(e.1));
            let top_pairs: Vec<((u32, u32), u32)> = cooccurrence_vec.into_iter().take(10).collect();

            black_box(top_pairs);
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = get_criterion();
    targets =
        bi1_tag_evolution_benchmark,
        bi2_tag_person_path_benchmark,
        bi3_popular_topics_benchmark,
        bi4_top_commenters_benchmark,
        bi5_active_users_benchmark,
        bi6_tag_cooccurrence_benchmark
}
criterion_main!(benches);
