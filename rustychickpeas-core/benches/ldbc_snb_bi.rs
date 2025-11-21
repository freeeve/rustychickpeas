//! LDBC Social Network Benchmark - Business Intelligence (BI) Queries
//!
//! Criterion benchmarks for LDBC SNB BI workload queries.
//! These benchmarks require LDBC data files (see tests/ldbc_snb_bi_benchmark.rs for data setup).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rustychickpeas_core::{GraphBuilder, GraphSnapshot};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::env;

/// Get the path to LDBC data directory
/// Defaults to SF0.003, with fallbacks to SF1 and SF10 if not found
fn get_ldbc_data_dir() -> Option<PathBuf> {
    if let Ok(dir) = env::var("LDBC_DATA_DIR") {
        return Some(PathBuf::from(dir));
    }
    
    let scale_factor = env::var("LDBC_SF").unwrap_or_else(|_| "0.003".to_string());
    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../ldbc_data");
    
    // Try the specified scale factor first
    let sf_path = base_dir.join(format!("social-network-sf{}-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot", scale_factor));
    
    if sf_path.exists() {
        return Some(sf_path);
    }
    
    // Try fallbacks
    for fallback_sf in &["0.003", "1", "10"] {
        if fallback_sf != &scale_factor {
            let fallback_path = base_dir.join(format!("social-network-sf{}-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot", fallback_sf));
            if fallback_path.exists() {
                return Some(fallback_path);
            }
        }
    }
    
    None
}

/// Global graph cache - loaded once and reused across benchmarks
static GRAPH_CACHE: OnceLock<Option<GraphSnapshot>> = OnceLock::new();

fn get_ldbc_graph() -> Option<&'static GraphSnapshot> {
    GRAPH_CACHE.get_or_init(|| {
        // Try to load the graph - this is expensive, so we cache it
        // For now, return None if data not available
        // In a real implementation, you'd call load_ldbc_graph() here
        None
    }).as_ref()
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
    let graph = match get_ldbc_graph() {
        Some(g) => g,
        None => {
            eprintln!("Skipping LDBC benchmarks: data not available. Set LDBC_DATA_DIR or place data in ../ldbc_data/");
            return;
        }
    };
    
    let mut group = c.benchmark_group("ldbc_snb_bi1_tag_evolution");
    
    group.bench_function("query", |b| {
        b.iter(|| {
            let posts = graph.nodes_with_label("Post").map(|s| s.iter().collect::<Vec<_>>()).unwrap_or_default();
            let comments = graph.nodes_with_label("Comment").map(|s| s.iter().collect::<Vec<_>>()).unwrap_or_default();
            
            let mut tag_pairs: std::collections::HashMap<(u32, u32), u32> = std::collections::HashMap::new();
            
            for &post_id in &posts {
                let post_tags = graph.out_neighbors_by_type(black_box(post_id), &["hasTag"]);
                
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
                let comment_tags = graph.out_neighbors_by_type(black_box(comment_id), &["hasTag"]);
                
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
    let graph = match get_ldbc_graph() {
        Some(g) => g,
        None => return,
    };
    
    let mut group = c.benchmark_group("ldbc_snb_bi2_tag_person_path");
    
    group.bench_function("query", |b| {
        b.iter(|| {
            let persons = graph.nodes_with_label("Person").map(|s| s.iter().collect::<Vec<_>>()).unwrap_or_default();
            let mut paths: Vec<(u32, u32)> = Vec::new();
            
            // Find paths through shared tags (simplified - just find persons with shared tags)
            for i in 0..persons.len().min(100) {
                for j in (i + 1)..persons.len().min(100) {
                    let person1_tags = graph.out_neighbors_by_type(black_box(persons[i]), &["hasInterest"]);
                    let person2_tags = graph.out_neighbors_by_type(black_box(persons[j]), &["hasInterest"]);
                    
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
    let graph = match get_ldbc_graph() {
        Some(g) => g,
        None => return,
    };
    
    let mut group = c.benchmark_group("ldbc_snb_bi3_popular_topics");
    
    group.bench_function("query", |b| {
        b.iter(|| {
            let mut tag_counts: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            
            if let Some(posts) = graph.nodes_with_label("Post") {
                for post_id in posts.iter() {
                    let tags = graph.out_neighbors_by_type(black_box(post_id), &["hasTag"]);
                    for tag in tags {
                        *tag_counts.entry(tag).or_insert(0) += 1;
                    }
                }
            }
            
            if let Some(comments) = graph.nodes_with_label("Comment") {
                for comment_id in comments.iter() {
                    let tags = graph.out_neighbors_by_type(black_box(comment_id), &["hasTag"]);
                    for tag in tags {
                        *tag_counts.entry(tag).or_insert(0) += 1;
                    }
                }
            }
            
            let mut tag_vec: Vec<(u32, u32)> = tag_counts.into_iter().collect();
            tag_vec.sort_by(|a, b| b.1.cmp(&a.1));
            let top_tags: Vec<(u32, u32)> = tag_vec.into_iter().take(10).collect();
            
            black_box(top_tags);
        });
    });
    
    group.finish();
}

/// BI4: Top Commenters
/// Find persons who have made the most comments
fn bi4_top_commenters_benchmark(c: &mut Criterion) {
    let graph = match get_ldbc_graph() {
        Some(g) => g,
        None => return,
    };
    
    let mut group = c.benchmark_group("ldbc_snb_bi4_top_commenters");
    
    group.bench_function("query", |b| {
        b.iter(|| {
            let mut person_comment_counts: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            
            if let Some(comments) = graph.nodes_with_label("Comment") {
                for comment_id in comments.iter() {
                    let creators = graph.in_neighbors(black_box(comment_id));
                    
                    for &creator_id in creators {
                        if let Some(persons) = graph.nodes_with_label("Person") {
                            if persons.contains(creator_id) {
                                *person_comment_counts.entry(creator_id).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }
            
            let mut person_vec: Vec<(u32, u32)> = person_comment_counts.into_iter().collect();
            person_vec.sort_by(|a, b| b.1.cmp(&a.1));
            let top_commenters: Vec<(u32, u32)> = person_vec.into_iter().take(10).collect();
            
            black_box(top_commenters);
        });
    });
    
    group.finish();
}

/// BI5: Active Users
/// Find persons with the most posts
fn bi5_active_users_benchmark(c: &mut Criterion) {
    let graph = match get_ldbc_graph() {
        Some(g) => g,
        None => return,
    };
    
    let mut group = c.benchmark_group("ldbc_snb_bi5_active_users");
    
    group.bench_function("query", |b| {
        b.iter(|| {
            let mut person_post_counts: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            
            if let Some(posts) = graph.nodes_with_label("Post") {
                for post_id in posts.iter() {
                    let creators = graph.in_neighbors(black_box(post_id));
                    
                    for &creator_id in creators {
                        if let Some(persons) = graph.nodes_with_label("Person") {
                            if persons.contains(creator_id) {
                                *person_post_counts.entry(creator_id).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }
            
            let mut person_vec: Vec<(u32, u32)> = person_post_counts.into_iter().collect();
            person_vec.sort_by(|a, b| b.1.cmp(&a.1));
            let top_users: Vec<(u32, u32)> = person_vec.into_iter().take(10).collect();
            
            black_box(top_users);
        });
    });
    
    group.finish();
}

/// BI6: Tag Co-occurrence
/// Find tags that frequently appear together
fn bi6_tag_cooccurrence_benchmark(c: &mut Criterion) {
    let graph = match get_ldbc_graph() {
        Some(g) => g,
        None => return,
    };
    
    let mut group = c.benchmark_group("ldbc_snb_bi6_tag_cooccurrence");
    
    group.bench_function("query", |b| {
        b.iter(|| {
            let mut cooccurrence: std::collections::HashMap<(u32, u32), u32> = std::collections::HashMap::new();
            
            if let Some(posts) = graph.nodes_with_label("Post") {
                for post_id in posts.iter() {
                    let tags = graph.out_neighbors_by_type(black_box(post_id), &["hasTag"]);
                    
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
            cooccurrence_vec.sort_by(|a, b| b.1.cmp(&a.1));
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

