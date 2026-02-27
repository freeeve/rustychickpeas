//! Benchmarks for GraphBuilder operations
//!
//! Tests the performance of building graphs using GraphBuilder,
//! including adding nodes, relationships, and properties.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rustychickpeas_core::GraphBuilder;
use std::env;

fn get_criterion() -> Criterion {
    let mut criterion = Criterion::default();
    
    // If BENCHMARK_BASELINE is set, use it as the baseline for comparison
    // This allows comparing against previous tags/commits
    if let Ok(baseline) = env::var("BENCHMARK_BASELINE") {
        criterion = criterion.save_baseline(baseline);
    }
    
    criterion
}

fn builder_add_nodes_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_add_nodes");
    
    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    for i in 0..size {
                        builder.add_node(Some(black_box(i as u32)), &["Person"]).unwrap();
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn builder_add_nodes_with_labels_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_add_nodes_with_labels");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    for i in 0..size {
                        let labels = if i % 2 == 0 {
                            &["Person", "User"][..]
                        } else {
                            &["Person", "Admin"][..]
                        };
                        builder.add_node(Some(black_box(i as u32)), labels).unwrap();
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn builder_add_relationships_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_add_relationships");
    
    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    // First add nodes
                    for i in 0..size {
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                    }
                    // Then add relationships
                    for i in 0..size {
                        let from = i as u64;
                        let to = ((i + 1) % size) as u64;
                        builder.add_rel(from as u32, to as u32, "KNOWS").unwrap();
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn builder_add_properties_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_add_properties");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    for i in 0..size {
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                        builder.set_prop_str(i as u32, "name", &format!("Person{}", i)).unwrap();
                        builder.set_prop_i64(i as u32, "age", (20 + i % 50) as i64).unwrap();
                        builder.set_prop_bool(i as u32, "active", i % 2 == 0).unwrap();
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn builder_finalize_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_finalize");
    
    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                // Setup: create a graph with nodes and relationships
                let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                for i in 0..size {
                    builder.add_node(Some(i as u32), &["Person"]).unwrap();
                }
                for i in 0..size {
                    let from = i as u32;
                    let to = ((i + 1) % size) as u32;
                    builder.add_rel(from, to, "KNOWS").unwrap();
                }
                
                b.iter(|| {
                    let mut builder_clone = GraphBuilder::new(Some(size), Some(size * 2));
                    for i in 0..size {
                        builder_clone.add_node(Some(i as u32), &["Person"]).unwrap();
                    }
                    for i in 0..size {
                        let from = i as u32;
                        let to = ((i + 1) % size) as u32;
                        builder_clone.add_rel(from, to, "KNOWS").unwrap();
                    }
                    let snapshot = builder_clone.finalize(None);
                    black_box(snapshot);
                });
            },
        );
    }
    group.finish();
}

fn builder_node_deduplication_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_node_deduplication");
    
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    // Enable deduplication on "email" property
                    builder.enable_node_deduplication(vec!["email"]);
                    
                    // Add nodes with duplicate emails (every 10 nodes share the same email)
                    for i in 0..size {
                        let email = format!("user{}@example.com", i / 10);
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                        builder.set_prop_str(i as u32, "email", &email).unwrap();
                        builder.set_prop_str(i as u32, "name", &format!("Person{}", i)).unwrap();
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn builder_node_deduplication_multi_key_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_node_deduplication_multi_key");
    
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    // Enable deduplication on multiple properties
                    builder.enable_node_deduplication(vec!["email", "username"]);
                    
                    // Add nodes with duplicate email+username combinations
                    for i in 0..size {
                        let email = format!("user{}@example.com", i / 10);
                        let username = format!("user{}", i / 10);
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                        builder.set_prop_str(i as u32, "email", &email).unwrap();
                        builder.set_prop_str(i as u32, "username", &username).unwrap();
                        builder.set_prop_str(i as u32, "name", &format!("Person{}", i)).unwrap();
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn builder_relationship_deduplication_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_relationship_deduplication");
    
    for size in [1000, 10000, 100000].iter() {
        
        // Benchmark: CreateUniqueByRelType (default for relationships)
        group.bench_with_input(
            BenchmarkId::new("by_type", *size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    // Add nodes
                    for i in 0..size {
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                    }
                    // Add relationships - many duplicates (same type between same nodes)
                    for i in 0..size {
                        let from = (i % (size / 10).max(1)) as u32;
                        let to = ((i + 1) % (size / 10).max(1)) as u32;
                        builder.add_rel(from, to, "KNOWS").unwrap();
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn builder_deduplication_vs_no_dedup_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_deduplication_overhead");
    
    for size in [10000, 100000].iter() {
        // Without deduplication
        group.bench_with_input(
            BenchmarkId::new("no_dedup", *size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    for i in 0..size {
                        let email = format!("user{}@example.com", i);
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                        builder.set_prop_str(i as u32, "email", &email).unwrap();
                    }
                    black_box(builder);
                });
            },
        );

        // With deduplication (but no actual duplicates)
        group.bench_with_input(
            BenchmarkId::new("dedup_no_duplicates", *size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    builder.enable_node_deduplication(vec!["email"]);
                    for i in 0..size {
                        let email = format!("user{}@example.com", i);
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                        builder.set_prop_str(i as u32, "email", &email).unwrap();
                    }
                    black_box(builder);
                });
            },
        );

        // With deduplication (with duplicates)
        group.bench_with_input(
            BenchmarkId::new("dedup_with_duplicates", *size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size * 2));
                    builder.enable_node_deduplication(vec!["email"]);
                    for i in 0..size {
                        let email = format!("user{}@example.com", i / 10); // 10 nodes per email
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                        builder.set_prop_str(i as u32, "email", &email).unwrap();
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = get_criterion();
    targets = 
        builder_add_nodes_benchmark,
        builder_add_nodes_with_labels_benchmark,
        builder_add_relationships_benchmark,
        builder_add_properties_benchmark,
        builder_finalize_benchmark,
        builder_node_deduplication_benchmark,
        builder_node_deduplication_multi_key_benchmark,
        builder_relationship_deduplication_benchmark,
        builder_deduplication_vs_no_dedup_benchmark
}
criterion_main!(benches);

