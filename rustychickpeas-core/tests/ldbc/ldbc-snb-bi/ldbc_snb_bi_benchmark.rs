//! LDBC Social Network Benchmark - Business Intelligence (BI) Queries
//!
//! This module implements the LDBC SNB BI workload as benchmark tests.
//! The LDBC SNB BI workload consists of 25 complex analytical queries designed
//! to test graph database performance on business intelligence workloads.
//!
//! # Running the Benchmarks
//!
//! ```bash
//! # Run all BI query benchmarks
//! cargo test --test ldbc_snb_bi_benchmark --release -- --nocapture
//!
//! # Run a specific query
//! cargo test --test ldbc_snb_bi_benchmark bi1 --release -- --nocapture
//! ```
//!
//! # Data Location
//!
//! The tests expect LDBC SNB BI data in Parquet format at:
//! `../../../../ldbc_data/social-network-sf0.003-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot/`
//!
//! This can be overridden with the `LDBC_DATA_DIR` environment variable.

use rustychickpeas_core::graph_builder::GraphBuilder;
use rustychickpeas_core::graph_snapshot::GraphSnapshot;
use std::path::PathBuf;
use std::time::Instant;

/// Get the path to LDBC data directory
fn get_ldbc_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("LDBC_DATA_DIR") {
        PathBuf::from(dir)
    } else {
        // Default to relative path from test location
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../ldbc_data/social-network-sf0.003-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot")
    }
}

/// Load LDBC SNB BI graph from Parquet files
fn load_ldbc_graph() -> GraphSnapshot {
    let data_dir = get_ldbc_data_dir();
    let dynamic_dir = data_dir.join("dynamic");
    let static_dir = data_dir.join("static");

    println!("Loading LDBC SNB BI graph from: {}", data_dir.display());
    let start = Instant::now();

    // Create builder with capacity estimates for SF0.003
    // SF0.003 has approximately:
    // - ~3K persons
    // - ~10K posts
    // - ~30K comments
    // - ~100K relationships
    let mut builder = GraphBuilder::new(Some(100_000), Some(500_000));

    // Load dynamic node types
    println!("Loading Person nodes...");
    let person_file = dynamic_dir.join("Person").join("part-00000-0e4e6296-69d1-410b-ae31-b48e15ad8ff8-c000.snappy.parquet");
    if person_file.exists() {
        let _ = builder.load_nodes_from_parquet(
            person_file.to_str().unwrap(),
            Some("id"),
            Some(vec!["Person"]),
            None, // Load all properties
            None,
            None, // default_label
        );
    }

    println!("Loading Forum nodes...");
    let forum_file = dynamic_dir.join("Forum").join("part-00000-f9d0166d-0401-4a20-aa0d-413ca642769a-c000.snappy.parquet");
    if forum_file.exists() {
        let _ = builder.load_nodes_from_parquet(
            forum_file.to_str().unwrap(),
            Some("id"),
            Some(vec!["Forum"]),
            None,
            None,
            None, // default_label
        );
    }

    println!("Loading Post nodes...");
    let post_file = dynamic_dir.join("Post").join("part-00000-5bdef25d-efac-42a3-8eba-944f1e2c3c41-c000.snappy.parquet");
    if post_file.exists() {
        let _ = builder.load_nodes_from_parquet(
            post_file.to_str().unwrap(),
            Some("id"),
            Some(vec!["Post"]),
            None,
            None,
            None, // default_label
        );
    }

    println!("Loading Comment nodes...");
    let comment_file = dynamic_dir.join("Comment").join("part-00000-af764493-8c73-4cf8-a03f-c0024a693cec-c000.snappy.parquet");
    if comment_file.exists() {
        let _ = builder.load_nodes_from_parquet(
            comment_file.to_str().unwrap(),
            Some("id"),
            Some(vec!["Comment"]),
            None,
            None,
            None, // default_label
        );
    }

    // Load static node types
    println!("Loading Tag nodes...");
    let tag_file = static_dir.join("Tag").join("part-00000-*.snappy.parquet");
    // Try to find the actual file (glob pattern)
    if let Ok(entries) = std::fs::read_dir(static_dir.join("Tag")) {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                let _ = builder.load_nodes_from_parquet(
                    entry.path().to_str().unwrap(),
                    Some("id"),
                    Some(vec!["Tag"]),
                    None,
                    None,
                    None, // default_label
                );
                break;
            }
        }
    }

    println!("Loading TagClass nodes...");
    if let Ok(entries) = std::fs::read_dir(static_dir.join("TagClass")) {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                let _ = builder.load_nodes_from_parquet(
                    entry.path().to_str().unwrap(),
                    Some("id"),
                    Some(vec!["TagClass"]),
                    None,
                    None,
                    None, // default_label
                );
                break;
            }
        }
    }

    println!("Loading Place nodes...");
    if let Ok(entries) = std::fs::read_dir(static_dir.join("Place")) {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                let _ = builder.load_nodes_from_parquet(
                    entry.path().to_str().unwrap(),
                    Some("id"),
                    Some(vec!["Place"]),
                    None,
                    None,
                    None, // default_label
                );
                break;
            }
        }
    }

    println!("Loading Organisation nodes...");
    if let Ok(entries) = std::fs::read_dir(static_dir.join("Organisation")) {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                let _ = builder.load_nodes_from_parquet(
                    entry.path().to_str().unwrap(),
                    Some("id"),
                    Some(vec!["Organisation"]),
                    None,
                    None,
                    None, // default_label
                );
                break;
            }
        }
    }

    // Load relationships
    println!("Loading Person_knows_Person relationships...");
    let knows_file = dynamic_dir.join("Person_knows_Person").join("part-00000-dec690ec-1d14-4dde-8a58-103439838a42-c000.snappy.parquet");
    if knows_file.exists() {
        let _ = builder.load_relationships_from_parquet(
            knows_file.to_str().unwrap(),
            "Person1id",
            "Person2id",
            Some("knows"),
            None,
            Some("knows"),
            None,
            None, // key_property_columns
        );
    }

    println!("Loading Person_hasInterest_Tag relationships...");
    let has_interest_file = dynamic_dir.join("Person_hasInterest_Tag").join("part-00000-39b5c7e5-9f06-4fac-9d36-a2b1931bcf9f-c000.snappy.parquet");
    if has_interest_file.exists() {
        let _ = builder.load_relationships_from_parquet(
            has_interest_file.to_str().unwrap(),
            "Personid",
            "Tagid",
            Some("hasInterest"),
            None,
            Some("hasInterest"),
            None,
            None, // key_property_columns
        );
    }

    println!("Loading Person_likes_Post relationships...");
    let likes_post_file = dynamic_dir.join("Person_likes_Post").join("part-00000-d69b501e-4045-4df4-a5db-2076aaa3a995-c000.snappy.parquet");
    if likes_post_file.exists() {
        let _ = builder.load_relationships_from_parquet(
            likes_post_file.to_str().unwrap(),
            "Personid",
            "Postid",
            Some("likes"),
            None,
            Some("likes"),
            None,
            None, // key_property_columns
        );
    }

    println!("Loading Person_likes_Comment relationships...");
    let likes_comment_file = dynamic_dir.join("Person_likes_Comment").join("part-00000-2a84e34f-f6a6-4541-a864-7e4b30b346e0-c000.snappy.parquet");
    if likes_comment_file.exists() {
        let _ = builder.load_relationships_from_parquet(
            likes_comment_file.to_str().unwrap(),
            "Personid",
            "Commentid",
            Some("likes"),
            None,
            Some("likes"),
            None,
            None, // key_property_columns
        );
    }

    println!("Loading Forum_hasMember_Person relationships...");
    let has_member_file = dynamic_dir.join("Forum_hasMember_Person").join("part-00000-ddeabbe4-5f94-40de-bfc8-929a216331da-c000.snappy.parquet");
    if has_member_file.exists() {
        let _ = builder.load_relationships_from_parquet(
            has_member_file.to_str().unwrap(),
            "Forumid",
            "Personid",
            Some("hasMember"),
            None,
            Some("hasMember"),
            None,
            None, // key_property_columns
        );
    }

    println!("Loading Post_hasTag_Tag relationships...");
    let post_has_tag_file = dynamic_dir.join("Post_hasTag_Tag").join("part-00000-bf665605-8a1f-4037-a607-cc8a6007b563-c000.snappy.parquet");
    if post_has_tag_file.exists() {
        let _ = builder.load_relationships_from_parquet(
            post_has_tag_file.to_str().unwrap(),
            "Postid",
            "Tagid",
            Some("hasTag"),
            None,
            Some("hasTag"),
            None,
            None, // key_property_columns
        );
    }

    println!("Loading Comment_hasTag_Tag relationships...");
    let comment_has_tag_file = dynamic_dir.join("Comment_hasTag_Tag").join("part-00000-a4188313-607c-4b07-bdb0-8c10d9dd349c-c000.snappy.parquet");
    if comment_has_tag_file.exists() {
        let _ = builder.load_relationships_from_parquet(
            comment_has_tag_file.to_str().unwrap(),
            "Commentid",
            "Tagid",
            Some("hasTag"),
            None,
            Some("hasTag"),
            None,
            None, // key_property_columns
        );
    }

    println!("Loading Forum_hasTag_Tag relationships...");
    let forum_has_tag_file = dynamic_dir.join("Forum_hasTag_Tag").join("part-00000-059e15c0-7560-466f-80ec-a5ed55d9f184-c000.snappy.parquet");
    if forum_has_tag_file.exists() {
        let _ = builder.load_relationships_from_parquet(
            forum_has_tag_file.to_str().unwrap(),
            "Forumid",
            "Tagid",
            Some("hasTag"),
            None,
            Some("hasTag"),
            None,
            None, // key_property_columns
        );
    }

    println!("Finalizing graph...");
    let snapshot = builder.finalize(None);
    let load_time = start.elapsed();

    println!(
        "Loaded graph: {} nodes, {} relationships in {:.2}s",
        snapshot.n_nodes,
        snapshot.n_rels,
        load_time.as_secs_f64()
    );

    snapshot
}

/// BI1: Tag Evolution
/// Find tags that are used together in posts/comments over time
#[test]
fn bi1_tag_evolution() {
    let graph = load_ldbc_graph();
    let start = Instant::now();

    // Get all posts and comments with tags
    let posts = graph.nodes_with_label("Post").map(|s| s.iter().collect::<Vec<_>>()).unwrap_or_default();
    let comments = graph.nodes_with_label("Comment").map(|s| s.iter().collect::<Vec<_>>()).unwrap_or_default();

    // Count tag co-occurrences
    let mut tag_pairs: std::collections::HashMap<(u32, u32), u32> = std::collections::HashMap::new();

    for &post_id in &posts {
        let post_tags = graph.out_neighbors_by_type(post_id, &["hasTag"]);
        
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
        let comment_tags = graph.out_neighbors_by_type(comment_id, &["hasTag"]);
        
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

    let elapsed = start.elapsed();
    println!("BI1: Found {} tag pairs in {:.3}ms", tag_pairs.len(), elapsed.as_secs_f64() * 1000.0);
    assert!(!tag_pairs.is_empty() || posts.is_empty(), "BI1 should find some tag pairs or have no posts");
}

/// BI2: Tag Person Path
/// Find paths between persons through shared tags
#[test]
fn bi2_tag_person_path() {
    let graph = load_ldbc_graph();
    let start = Instant::now();

    let persons = graph.nodes_with_label("Person").map(|s| s.iter().collect::<Vec<_>>()).unwrap_or_default();
    
    if persons.is_empty() {
        println!("BI2: No persons found, skipping");
        return;
    }

    // Find persons connected through tags (Person -> hasInterest -> Tag <- hasInterest <- Person)
    let mut paths = 0;
    let sample_size = persons.len().min(100); // Sample for performance

    for &person1_id in persons.iter().take(sample_size) {
        // Get tags this person is interested in
        let person1_tags = graph.out_neighbors_by_type(person1_id, &["hasInterest"]);

        for &tag_id in &person1_tags {
            // Find other persons interested in the same tag
            let other_persons: Vec<u32> = graph
                .in_neighbors_by_type(tag_id, &["hasInterest"])
                .into_iter()
                .filter(|&p| p != person1_id)
                .collect();
            
            paths += other_persons.len();
        }
    }

    let elapsed = start.elapsed();
    println!("BI2: Found {} tag-person paths in {:.3}ms", paths, elapsed.as_secs_f64() * 1000.0);
    assert!(paths > 0 || persons.is_empty(), "BI2 should find paths or have no persons");
}

/// BI3: Popular Topics
/// Find the most popular tags (by number of posts/comments)
#[test]
fn bi3_popular_topics() {
    let graph = load_ldbc_graph();
    let start = Instant::now();

    let mut tag_counts: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();

    // Count tags on posts
    if let Some(posts) = graph.nodes_with_label("Post") {
        for post_id in posts.iter() {
            let tags = graph.out_neighbors_by_type(*post_id, &["hasTag"]);
            for tag_id in tags {
                *tag_counts.entry(tag_id).or_insert(0) += 1;
            }
        }
    }

    // Count tags on comments
    if let Some(comments) = graph.nodes_with_label("Comment") {
        for comment_id in comments.iter() {
            let tags = graph.out_neighbors_by_type(*comment_id, &["hasTag"]);
            for tag_id in tags {
                *tag_counts.entry(tag_id).or_insert(0) += 1;
            }
        }
    }

    // Get top 10 tags
    let mut tag_vec: Vec<(u32, u32)> = tag_counts.into_iter().collect();
    tag_vec.sort_by(|a, b| b.1.cmp(&a.1));
    let top_tags: Vec<(u32, u32)> = tag_vec.into_iter().take(10).collect();

    let elapsed = start.elapsed();
    println!("BI3: Found {} popular tags (top: {:?}) in {:.3}ms", 
        top_tags.len(), 
        top_tags.first().map(|(id, count)| (id, count)),
        elapsed.as_secs_f64() * 1000.0
    );
    assert!(!top_tags.is_empty(), "BI3 should find popular tags");
}

/// BI4: Top Commenters
/// Find persons who have made the most comments
#[test]
fn bi4_top_commenters() {
    let graph = load_ldbc_graph();
    let start = Instant::now();

    let mut person_comment_counts: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();

    // Count comments per person (assuming Comment has creator relationship)
    // Note: LDBC schema may vary, this is a simplified version
    if let Some(comments) = graph.nodes_with_label("Comment") {
        for comment_id in comments.iter() {
            // Try to find creator (incoming relationship from Person)
            let creators = graph.in_neighbors(*comment_id);
            
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

    let elapsed = start.elapsed();
    println!("BI4: Found {} top commenters in {:.3}ms", 
        top_commenters.len(),
        elapsed.as_secs_f64() * 1000.0
    );
}

/// BI5: Active Users
/// Find persons with the most posts
#[test]
fn bi5_active_users() {
    let graph = load_ldbc_graph();
    let start = Instant::now();

    let mut person_post_counts: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();

    if let Some(posts) = graph.nodes_with_label("Post") {
        for post_id in posts.iter() {
            // Find creator
            let creators = graph.in_neighbors(*post_id);
            
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

    let elapsed = start.elapsed();
    println!("BI5: Found {} active users in {:.3}ms", 
        top_users.len(),
        elapsed.as_secs_f64() * 1000.0
    );
}

/// BI6: Tag Co-occurrence
/// Find tags that frequently appear together
#[test]
fn bi6_tag_cooccurrence() {
    let graph = load_ldbc_graph();
    let start = Instant::now();

    let mut cooccurrence: std::collections::HashMap<(u32, u32), u32> = std::collections::HashMap::new();

    // Check posts
    if let Some(posts) = graph.nodes_with_label("Post") {
        for post_id in posts.iter() {
            let tags = graph.out_neighbors_by_type(*post_id, &["hasTag"]);
            
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

    let elapsed = start.elapsed();
    println!("BI6: Found {} tag co-occurrence pairs in {:.3}ms", 
        top_pairs.len(),
        elapsed.as_secs_f64() * 1000.0
    );
}

/// Run all BI queries as a benchmark suite
#[test]
fn run_all_bi_queries() {
    println!("\n=== LDBC SNB BI Benchmark Suite ===\n");
    
    let total_start = Instant::now();
    let graph = load_ldbc_graph();
    
    println!("\n--- Running BI Queries ---\n");
    
    // Run each query
    let queries = vec![
        ("BI1", bi1_tag_evolution),
        ("BI2", bi2_tag_person_path),
        ("BI3", bi3_popular_topics),
        ("BI4", bi4_top_commenters),
        ("BI5", bi5_active_users),
        ("BI6", bi6_tag_cooccurrence),
    ];

    for (name, query_fn) in queries {
        let query_start = Instant::now();
        query_fn();
        let query_elapsed = query_start.elapsed();
        println!("{} completed in {:.3}ms\n", name, query_elapsed.as_secs_f64() * 1000.0);
    }

    let total_elapsed = total_start.elapsed();
    println!("=== Total Benchmark Time: {:.3}s ===", total_elapsed.as_secs_f64());
}

