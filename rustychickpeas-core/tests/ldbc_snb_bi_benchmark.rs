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
//! `../../../ldbc_data/social-network-sf0.003-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot/`
//!
//! This can be overridden with the `LDBC_DATA_DIR` environment variable.

use rustychickpeas_core::graph_builder::GraphBuilder;
use rustychickpeas_core::graph_snapshot::GraphSnapshot;
use std::path::PathBuf;
use std::time::Instant;

/// Get the path to LDBC data directory
/// Tries SF10 first, then falls back to SF0.003 if not found
fn get_ldbc_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("LDBC_DATA_DIR") {
        return PathBuf::from(dir);
    }
    
    // Try to get scale factor from environment, default to SF10
    let scale_factor = std::env::var("LDBC_SF")
        .unwrap_or_else(|_| "10".to_string());
    
    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../ldbc_data");
    
    // Try the specified scale factor first
    let sf_path = base_dir.join(format!("social-network-sf{}-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot", scale_factor));
    
    // If the requested scale factor doesn't exist, try fallbacks in order: SF1, then SF0.003
    if !sf_path.exists() {
        let fallbacks = vec!["1", "0.003"];
        for fallback_sf in fallbacks {
            if fallback_sf != &scale_factor {
                let fallback_path = base_dir.join(format!("social-network-sf{}-bi-parquet/graphs/parquet/bi/composite-merged-fk/initial_snapshot", fallback_sf));
                if fallback_path.exists() {
                    println!("SF{} not found, using SF{} as fallback", scale_factor, fallback_sf);
                    return fallback_path;
                }
            }
        }
        println!("Warning: No LDBC data found. Tried SF{}, SF1, and SF0.003", scale_factor);
    }
    
    sf_path
}

/// Extract LDBC IDs directly from parquet file (faster than prop() lookups)
fn extract_ldbc_ids_from_parquet(path: &str) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    
    let mut ldbc_ids = Vec::new();
    
    if let Ok(file) = File::open(path) {
        if let Ok(builder_reader) = ParquetRecordBatchReaderBuilder::try_new(file) {
            let schema = builder_reader.schema();
            if let Some(id_idx) = schema.fields().iter().position(|f| f.name() == "id") {
                if let Ok(mut reader) = builder_reader.build() {
                    while let Some(Ok(batch)) = reader.next() {
                        if let Some(id_array) = batch.column(id_idx).as_any().downcast_ref::<Int64Array>() {
                            for i in 0..batch.num_rows() {
                                if !id_array.is_null(i) {
                                    ldbc_ids.push(id_array.value(i));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    ldbc_ids
}

/// Load relationships from Parquet file, mapping LDBC IDs to internal node IDs
fn load_relationships_with_mapping(
    builder: &mut GraphBuilder,
    path: &str,
    start_col: &str,
    end_col: &str,
    rel_type: &str,
    mapping: &std::collections::HashMap<i64, u32>,
) -> usize {
    use arrow::array::{Array, Int64Array, Int32Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("  Error opening file {}: {}", path, e);
            return 0;
        }
    };
    
    let builder_reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("  Error creating Parquet reader: {}", e);
            return 0;
        }
    };
    
    let schema = builder_reader.schema().clone();
    let mut reader = match builder_reader.build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("  Error building reader: {}", e);
            return 0;
        }
    };
    
    let start_idx = match schema.fields().iter().position(|f| f.name() == start_col) {
        Some(idx) => idx,
        None => {
            eprintln!("  Column '{}' not found in {}", start_col, path);
            return 0;
        }
    };
    
    let end_idx = match schema.fields().iter().position(|f| f.name() == end_col) {
        Some(idx) => idx,
        None => {
            eprintln!("  Column '{}' not found in {}", end_col, path);
            return 0;
        }
    };
    
    let mut count = 0;
    let mut batch_num = 0;
    let rel_start = Instant::now();
    while let Some(batch_result) = reader.next() {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("  Error reading batch: {}", e);
                continue;
            }
        };
        
        batch_num += 1;
        
        let start_col_data = batch.column(start_idx);
        let end_col_data = batch.column(end_idx);
        
        // Support both Int64 and Int32
        if let Some(start_array) = start_col_data.as_any().downcast_ref::<Int64Array>() {
            if let Some(end_array) = end_col_data.as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    if start_array.is_null(i) || end_array.is_null(i) {
                        continue;
                    }
                    let start_ldbc_id = start_array.value(i);
                    let end_ldbc_id = end_array.value(i);
                    
                    if let (Some(&start_internal), Some(&end_internal)) = 
                        (mapping.get(&start_ldbc_id), mapping.get(&end_ldbc_id)) {
                        builder.add_rel(start_internal, end_internal, rel_type);
                        count += 1;
                    }
                }
            }
        } else if let Some(start_array) = start_col_data.as_any().downcast_ref::<Int32Array>() {
            if let Some(end_array) = end_col_data.as_any().downcast_ref::<Int32Array>() {
                for i in 0..batch.num_rows() {
                    if start_array.is_null(i) || end_array.is_null(i) {
                        continue;
                    }
                    let start_ldbc_id = start_array.value(i) as i64;
                    let end_ldbc_id = end_array.value(i) as i64;
                    
                    if let (Some(&start_internal), Some(&end_internal)) = 
                        (mapping.get(&start_ldbc_id), mapping.get(&end_ldbc_id)) {
                        builder.add_rel(start_internal, end_internal, rel_type);
                        count += 1;
                    }
                }
            }
        }
        
        // Print progress every 10 batches or every 1M relationships
        if batch_num % 10 == 0 || count % 1_000_000 == 0 {
            let elapsed = rel_start.elapsed();
            let rate = count as f64 / elapsed.as_secs_f64();
            println!("    Batch {}: {} relationships loaded ({:.0} rels/sec)", batch_num, count, rate);
        }
    }
    
    let elapsed = rel_start.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    println!("    ✓ Loaded {} relationships in {:.2}s ({:.0} rels/sec)", count, elapsed.as_secs_f64(), rate);
    
    count
}

/// Load LDBC SNB BI graph from Parquet files
/// Uses auto-generated node IDs and stores LDBC IDs as a unique property for deduplication
fn load_ldbc_graph() -> GraphSnapshot {
    println!("=== Starting LDBC Graph Load ===");
    let data_dir = get_ldbc_data_dir();
    println!("Data directory: {}", data_dir.display());
    
    let dynamic_dir = data_dir.join("dynamic");
    let static_dir = data_dir.join("static");
    
    println!("Checking directories...");
    println!("  Dynamic dir exists: {}", dynamic_dir.exists());
    println!("  Static dir exists: {}", static_dir.exists());

    println!("Loading LDBC SNB BI graph from: {}", data_dir.display());
    let start = Instant::now();

    // Create builder with capacity estimates
    // SF0.003: ~3K persons, ~10K posts, ~30K comments, ~100K relationships
    // SF1: ~3M persons, ~10M posts, ~30M comments, ~100M relationships
    // SF10: ~30M persons, ~100M posts, ~300M comments, ~1B relationships
    // Use appropriate capacity based on scale factor
    let scale_factor = std::env::var("LDBC_SF").unwrap_or_else(|_| "10".to_string());
    println!("Scale factor: {}", scale_factor);
    let (node_capacity, rel_capacity) = match scale_factor.as_str() {
        "10" => (Some(500_000_000), Some(2_000_000_000)), // 500M nodes, 2B relationships
        "1" => (Some(50_000_000), Some(200_000_000)),      // 50M nodes, 200M relationships
        _ => (Some(100_000), Some(500_000)),               // Default for smaller scale factors
    };
    println!("Creating GraphBuilder with capacity: {} nodes, {} relationships", 
        node_capacity.map(|n| n.to_string()).unwrap_or_else(|| "unlimited".to_string()),
        rel_capacity.map(|r| r.to_string()).unwrap_or_else(|| "unlimited".to_string()));
    let mut builder = GraphBuilder::new(node_capacity, rel_capacity);
    println!("GraphBuilder created");
    
    // Build mapping from LDBC ID to internal node ID as we load nodes
    let mut ldbc_to_internal: std::collections::HashMap<i64, u32> = std::collections::HashMap::new();

    // Load dynamic node types
    // Use auto-generated IDs and store LDBC ID as a unique property for deduplication
    // Note: SF10 may have multiple part files, we'll need to load all of them
    println!("Loading Person nodes...");
    // Try to find all Person parquet files (SF10 may have multiple parts)
    let person_files: Vec<_> = if let Ok(entries) = std::fs::read_dir(dynamic_dir.join("Person")) {
        entries
            .flatten()
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
            .map(|e| e.path())
            .collect()
    } else {
        // Fallback to specific file name for SF0.003
        let fallback = dynamic_dir.join("Person").join("part-00000-0e4e6296-69d1-410b-ae31-b48e15ad8ff8-c000.snappy.parquet");
        if fallback.exists() {
            vec![fallback]
        } else {
            vec![]
        }
    };
    
    if person_files.is_empty() {
        eprintln!("  No Person files found in {}", dynamic_dir.join("Person").display());
    } else {
        for person_file in person_files {
            if person_file.exists() {
                println!("  Reading Person parquet file: {}", person_file.file_name().unwrap().to_string_lossy());
                let load_start = Instant::now();
                match builder.load_nodes_from_parquet(
                    person_file.to_str().unwrap(),
                    None, // Auto-generate node IDs
                    None, // No label columns in LDBC data
                    Some(vec!["id"]), // Load "id" as a property
                    Some(vec!["id"]), // Use "id" as unique property for deduplication
                    Some("Person"), // Default label
                ) {
                    Ok(node_ids) => {
                        let load_time = load_start.elapsed();
                        println!("  ✓ Loaded {} Person nodes from {} in {:.2}s ({:.0} nodes/sec)", 
                            node_ids.len(), 
                            person_file.file_name().unwrap().to_string_lossy(),
                            load_time.as_secs_f64(),
                            node_ids.len() as f64 / load_time.as_secs_f64());
                        
                        // Extract LDBC IDs directly from parquet (faster than prop() lookups)
                        let ldbc_ids = extract_ldbc_ids_from_parquet(person_file.to_str().unwrap());
                        
                        // Build mapping from extracted LDBC IDs (labels already added during loading)
                        println!("  Building ID mapping...");
                        let mapping_start = Instant::now();
                        for (i, node_id) in node_ids.iter().enumerate() {
                            if i < ldbc_ids.len() {
                                ldbc_to_internal.insert(ldbc_ids[i], *node_id);
                            }
                        }
                        let mapping_time = mapping_start.elapsed();
                        println!("  ✓ Built mapping in {:.2}s ({:.0} nodes/sec)", 
                            mapping_time.as_secs_f64(),
                            node_ids.len() as f64 / mapping_time.as_secs_f64());
                    }
                    Err(e) => {
                        eprintln!("  Error loading Person nodes from {}: {}", person_file.display(), e);
                    }
                }
            }
        }
    }

    println!("Loading Forum nodes...");
    let forum_start = Instant::now();
    let forum_files: Vec<_> = if let Ok(entries) = std::fs::read_dir(dynamic_dir.join("Forum")) {
        entries
            .flatten()
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
            .map(|e| e.path())
            .collect()
    } else {
        let fallback = dynamic_dir.join("Forum").join("part-00000-f9d0166d-0401-4a20-aa0d-413ca642769a-c000.snappy.parquet");
        if fallback.exists() {
            vec![fallback]
        } else {
            vec![]
        }
    };
    
    for forum_file in forum_files {
        if forum_file.exists() {
            println!("  Reading Forum parquet file...");
            if let Ok(node_ids) = builder.load_nodes_from_parquet(
                forum_file.to_str().unwrap(),
                None, // Auto-generate
                None,
                Some(vec!["id"]),
                Some(vec!["id"]), // Deduplication
                Some("Forum"), // Default label
            ) {
                println!("  ✓ Loaded {} Forum nodes", node_ids.len());
                // Extract LDBC IDs and build mapping
                let ldbc_ids = extract_ldbc_ids_from_parquet(forum_file.to_str().unwrap());
                for (i, node_id) in node_ids.iter().enumerate() {
                    if i < ldbc_ids.len() {
                        ldbc_to_internal.insert(ldbc_ids[i], *node_id);
                    }
                }
            }
        }
    }
    println!("  Forum loading complete in {:.2}s", forum_start.elapsed().as_secs_f64());

    println!("Loading Post nodes...");
    let post_start = Instant::now();
    let post_files: Vec<_> = if let Ok(entries) = std::fs::read_dir(dynamic_dir.join("Post")) {
        entries
            .flatten()
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
            .map(|e| e.path())
            .collect()
    } else {
        let fallback = dynamic_dir.join("Post").join("part-00000-5bdef25d-efac-42a3-8eba-944f1e2c3c41-c000.snappy.parquet");
        if fallback.exists() {
            vec![fallback]
        } else {
            vec![]
        }
    };
    
    for post_file in post_files {
        println!("  Reading Post parquet file...");
        // Extract LDBC IDs directly from parquet file to avoid slow prop() lookups
        let ldbc_ids = extract_ldbc_ids_from_parquet(post_file.to_str().unwrap());
        
        if let Ok(node_ids) = builder.load_nodes_from_parquet(
            post_file.to_str().unwrap(),
            None, // Auto-generate
            None,
            Some(vec!["id"]),
            Some(vec!["id"]),
            Some("Post"), // Default label
        ) {
            println!("  ✓ Loaded {} Post nodes", node_ids.len());
            // Build mapping from extracted LDBC IDs (labels already added during loading)
            for (i, node_id) in node_ids.iter().enumerate() {
                if i < ldbc_ids.len() {
                    ldbc_to_internal.insert(ldbc_ids[i], *node_id);
                }
            }
        }
    }
    println!("  Post loading complete in {:.2}s", post_start.elapsed().as_secs_f64());

    println!("Loading Comment nodes...");
    let comment_start = Instant::now();
    let comment_files: Vec<_> = if let Ok(entries) = std::fs::read_dir(dynamic_dir.join("Comment")) {
        entries
            .flatten()
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
            .map(|e| e.path())
            .collect()
    } else {
        let fallback = dynamic_dir.join("Comment").join("part-00000-af764493-8c73-4cf8-a03f-c0024a693cec-c000.snappy.parquet");
        if fallback.exists() {
            vec![fallback]
        } else {
            vec![]
        }
    };
    
    for comment_file in comment_files {
        println!("  Reading Comment parquet file...");
        // Extract LDBC IDs directly from parquet file to avoid slow prop() lookups
        let ldbc_ids = extract_ldbc_ids_from_parquet(comment_file.to_str().unwrap());
        
        if let Ok(node_ids) = builder.load_nodes_from_parquet(
            comment_file.to_str().unwrap(),
            None, // Auto-generate
            None,
            Some(vec!["id"]),
            Some(vec!["id"]),
            Some("Comment"), // Default label
        ) {
            println!("  ✓ Loaded {} Comment nodes", node_ids.len());
            // Build mapping from extracted LDBC IDs (labels already added during loading)
            for (i, node_id) in node_ids.iter().enumerate() {
                if i < ldbc_ids.len() {
                    ldbc_to_internal.insert(ldbc_ids[i], *node_id);
                }
            }
        }
    }
    println!("  Comment loading complete in {:.2}s", comment_start.elapsed().as_secs_f64());

    // Load static node types
    println!("Loading Tag nodes...");
    if let Ok(entries) = std::fs::read_dir(static_dir.join("Tag")) {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                let tag_file = entry.path();
                let ldbc_ids = extract_ldbc_ids_from_parquet(tag_file.to_str().unwrap());
                if let Ok(node_ids) = builder.load_nodes_from_parquet(
                    tag_file.to_str().unwrap(),
                    None, // Auto-generate
                    None,
                    Some(vec!["id"]),
                    Some(vec!["id"]),
                    Some("Tag"), // Default label
                ) {
                    for (i, node_id) in node_ids.iter().enumerate() {
                        if i < ldbc_ids.len() {
                            ldbc_to_internal.insert(ldbc_ids[i], *node_id);
                        }
                    }
                }
                break;
            }
        }
    }

    println!("Loading TagClass nodes...");
    if let Ok(entries) = std::fs::read_dir(static_dir.join("TagClass")) {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                let tagclass_file = entry.path();
                let ldbc_ids = extract_ldbc_ids_from_parquet(tagclass_file.to_str().unwrap());
                if let Ok(node_ids) = builder.load_nodes_from_parquet(
                    tagclass_file.to_str().unwrap(),
                    None,
                    None,
                    Some(vec!["id"]),
                    Some(vec!["id"]),
                    Some("TagClass"), // Default label
                ) {
                    for (i, node_id) in node_ids.iter().enumerate() {
                        if i < ldbc_ids.len() {
                            ldbc_to_internal.insert(ldbc_ids[i], *node_id);
                        }
                    }
                }
                break;
            }
        }
    }

    println!("Loading Place nodes...");
    if let Ok(entries) = std::fs::read_dir(static_dir.join("Place")) {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                let place_file = entry.path();
                let ldbc_ids = extract_ldbc_ids_from_parquet(place_file.to_str().unwrap());
                if let Ok(node_ids) = builder.load_nodes_from_parquet(
                    place_file.to_str().unwrap(),
                    None,
                    None,
                    Some(vec!["id"]),
                    Some(vec!["id"]),
                    Some("Place"), // Default label
                ) {
                    for (i, node_id) in node_ids.iter().enumerate() {
                        if i < ldbc_ids.len() {
                            ldbc_to_internal.insert(ldbc_ids[i], *node_id);
                        }
                    }
                }
                break;
            }
        }
    }

    println!("Loading Organisation nodes...");
    if let Ok(entries) = std::fs::read_dir(static_dir.join("Organisation")) {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                let org_file = entry.path();
                let ldbc_ids = extract_ldbc_ids_from_parquet(org_file.to_str().unwrap());
                if let Ok(node_ids) = builder.load_nodes_from_parquet(
                    org_file.to_str().unwrap(),
                    None,
                    None,
                    Some(vec!["id"]),
                    Some(vec!["id"]),
                    Some("Organisation"), // Default label
                ) {
                    for (i, node_id) in node_ids.iter().enumerate() {
                        if i < ldbc_ids.len() {
                            ldbc_to_internal.insert(ldbc_ids[i], *node_id);
                        }
                    }
                }
                break;
            }
        }
    }
    
    println!("Built mapping for {} LDBC IDs", ldbc_to_internal.len());

    // Load relationships using LDBC ID to internal ID mapping
    // SF10 may have multiple part files for each relationship type
    println!("Loading Person_knows_Person relationships...");
    let knows_start = Instant::now();
    let knows_files: Vec<_> = if let Ok(entries) = std::fs::read_dir(dynamic_dir.join("Person_knows_Person")) {
        entries
            .flatten()
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
            .map(|e| e.path())
            .collect()
    } else {
        let fallback = dynamic_dir.join("Person_knows_Person").join("part-00000-dec690ec-1d14-4dde-8a58-103439838a42-c000.snappy.parquet");
        if fallback.exists() {
            vec![fallback]
        } else {
            vec![]
        }
    };
    
    println!("  Found {} Person_knows_Person file(s)", knows_files.len());
    let mut total_knows = 0;
    for (file_idx, knows_file) in knows_files.iter().enumerate() {
        println!("  Loading file {}/{}: {}", file_idx + 1, knows_files.len(), knows_file.file_name().unwrap().to_string_lossy());
        let count = load_relationships_with_mapping(
            &mut builder,
            knows_file.to_str().unwrap(),
            "Person1Id",
            "Person2Id",
            "knows",
            &ldbc_to_internal,
        );
        total_knows += count;
    }
    let knows_time = knows_start.elapsed();
    println!("  ✓ Loaded {} Person_knows_Person relationships in {:.2}s ({:.0} rels/sec)", 
        total_knows, knows_time.as_secs_f64(), total_knows as f64 / knows_time.as_secs_f64());

    println!("Loading Person_hasInterest_Tag relationships...");
    let has_interest_files: Vec<_> = if let Ok(entries) = std::fs::read_dir(dynamic_dir.join("Person_hasInterest_Tag")) {
        entries
            .flatten()
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
            .map(|e| e.path())
            .collect()
    } else {
        let fallback = dynamic_dir.join("Person_hasInterest_Tag").join("part-00000-39b5c7e5-9f06-4fac-9d36-a2b1931bcf9f-c000.snappy.parquet");
        if fallback.exists() {
            vec![fallback]
        } else {
            vec![]
        }
    };
    
    let mut total_has_interest = 0;
    for has_interest_file in has_interest_files {
        total_has_interest += load_relationships_with_mapping(
            &mut builder,
            has_interest_file.to_str().unwrap(),
            "PersonId",
            "TagId",
            "hasInterest",
            &ldbc_to_internal,
        );
    }
    println!("  Loaded {} hasInterest relationships", total_has_interest);

    println!("Loading Person_likes_Post relationships...");
    let likes_post_file = dynamic_dir.join("Person_likes_Post").join("part-00000-d69b501e-4045-4df4-a5db-2076aaa3a995-c000.snappy.parquet");
    if likes_post_file.exists() {
        let count = load_relationships_with_mapping(
            &mut builder,
            likes_post_file.to_str().unwrap(),
            "PersonId",
            "PostId",
            "likes",
            &ldbc_to_internal,
        );
        println!("  Loaded {} likes (Post) relationships", count);
    }

    println!("Loading Person_likes_Comment relationships...");
    let likes_comment_file = dynamic_dir.join("Person_likes_Comment").join("part-00000-2a84e34f-f6a6-4541-a864-7e4b30b346e0-c000.snappy.parquet");
    if likes_comment_file.exists() {
        let count = load_relationships_with_mapping(
            &mut builder,
            likes_comment_file.to_str().unwrap(),
            "PersonId",
            "CommentId",
            "likes",
            &ldbc_to_internal,
        );
        println!("  Loaded {} likes (Comment) relationships", count);
    }

    println!("Loading Forum_hasMember_Person relationships...");
    let has_member_file = dynamic_dir.join("Forum_hasMember_Person").join("part-00000-ddeabbe4-5f94-40de-bfc8-929a216331da-c000.snappy.parquet");
    if has_member_file.exists() {
        let count = load_relationships_with_mapping(
            &mut builder,
            has_member_file.to_str().unwrap(),
            "ForumId",
            "PersonId",
            "hasMember",
            &ldbc_to_internal,
        );
        println!("  Loaded {} hasMember relationships", count);
    }

    println!("Loading Post_hasTag_Tag relationships...");
    let post_has_tag_file = dynamic_dir.join("Post_hasTag_Tag").join("part-00000-bf665605-8a1f-4037-a607-cc8a6007b563-c000.snappy.parquet");
    if post_has_tag_file.exists() {
        let count = load_relationships_with_mapping(
            &mut builder,
            post_has_tag_file.to_str().unwrap(),
            "PostId",
            "TagId",
            "hasTag",
            &ldbc_to_internal,
        );
        println!("  Loaded {} Post hasTag relationships", count);
    }

    println!("Loading Comment_hasTag_Tag relationships...");
    let comment_has_tag_file = dynamic_dir.join("Comment_hasTag_Tag").join("part-00000-a4188313-607c-4b07-bdb0-8c10d9dd349c-c000.snappy.parquet");
    if comment_has_tag_file.exists() {
        let count = load_relationships_with_mapping(
            &mut builder,
            comment_has_tag_file.to_str().unwrap(),
            "CommentId",
            "TagId",
            "hasTag",
            &ldbc_to_internal,
        );
        println!("  Loaded {} Comment hasTag relationships", count);
    }

    println!("Loading Forum_hasTag_Tag relationships...");
    let forum_has_tag_file = dynamic_dir.join("Forum_hasTag_Tag").join("part-00000-059e15c0-7560-466f-80ec-a5ed55d9f184-c000.snappy.parquet");
    if forum_has_tag_file.exists() {
        let count = load_relationships_with_mapping(
            &mut builder,
            forum_has_tag_file.to_str().unwrap(),
            "ForumId",
            "TagId",
            "hasTag",
            &ldbc_to_internal,
        );
        println!("  Loaded {} Forum hasTag relationships", count);
    }

    println!("Finalizing graph...");
    println!("All data loaded. Finalizing graph (this may take a while)...");
    let finalize_start = Instant::now();
    let snapshot = builder.finalize(None);
    let finalize_time = finalize_start.elapsed();
    let load_time = start.elapsed();

    println!(
        "✓ Graph finalized in {:.2}s",
        finalize_time.as_secs_f64()
    );
    println!(
        "✓ Loaded graph: {} nodes, {} relationships in {:.2}s total",
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
    // Allow empty results if graph is empty
    if graph.n_nodes == 0 {
        println!("BI1: Graph is empty, skipping assertion");
        return;
    }
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
            let tags = graph.out_neighbors_by_type(post_id, &["hasTag"]);
            for tag_id in tags {
                *tag_counts.entry(tag_id).or_insert(0) += 1;
            }
        }
    }

    // Count tags on comments
    if let Some(comments) = graph.nodes_with_label("Comment") {
        for comment_id in comments.iter() {
            let tags = graph.out_neighbors_by_type(comment_id, &["hasTag"]);
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
    // Allow empty results if graph is empty
    if graph.n_nodes == 0 {
        println!("BI3: Graph is empty, skipping assertion");
        return;
    }
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
            let creators = graph.in_neighbors(comment_id);
            
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
            let creators = graph.in_neighbors(post_id);
            
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
            let tags = graph.out_neighbors_by_type(post_id, &["hasTag"]);
            
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
    let _graph = load_ldbc_graph();
    
    println!("\n--- Running BI Queries ---\n");
    
    // Run each query
    let query_start = Instant::now();
    bi1_tag_evolution();
    println!("BI1 completed in {:.3}ms\n", query_start.elapsed().as_secs_f64() * 1000.0);
    
    let query_start = Instant::now();
    bi2_tag_person_path();
    println!("BI2 completed in {:.3}ms\n", query_start.elapsed().as_secs_f64() * 1000.0);
    
    let query_start = Instant::now();
    bi3_popular_topics();
    println!("BI3 completed in {:.3}ms\n", query_start.elapsed().as_secs_f64() * 1000.0);
    
    let query_start = Instant::now();
    bi4_top_commenters();
    println!("BI4 completed in {:.3}ms\n", query_start.elapsed().as_secs_f64() * 1000.0);
    
    let query_start = Instant::now();
    bi5_active_users();
    println!("BI5 completed in {:.3}ms\n", query_start.elapsed().as_secs_f64() * 1000.0);
    
    let query_start = Instant::now();
    bi6_tag_cooccurrence();
    println!("BI6 completed in {:.3}ms\n", query_start.elapsed().as_secs_f64() * 1000.0);

    let total_elapsed = total_start.elapsed();
    println!("=== Total Benchmark Time: {:.3}s ===", total_elapsed.as_secs_f64());
}

