//! Benchmarks for GraphBuilder operations
//!
//! Tests the performance of building graphs using GraphBuilder,
//! including adding nodes, relationships, and properties.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rustychickpeas_core::GraphBuilder;

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
                        builder.add_node(black_box(i as u64), &["Person"]);
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
                        builder.add_node(black_box(i as u64), labels);
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
                        builder.add_node(i as u64, &["Person"]);
                    }
                    // Then add relationships
                    for i in 0..size {
                        let from = i as u64;
                        let to = ((i + 1) % size) as u64;
                        builder.add_rel(from, to, "KNOWS");
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
                        builder.add_node(i as u64, &["Person"]);
                        builder.set_prop_str(i as u64, "name", &format!("Person{}", i));
                        builder.set_prop_i64(i as u64, "age", (20 + i % 50) as i64);
                        builder.set_prop_bool(i as u64, "active", i % 2 == 0);
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
                let mut builder = GraphBuilder::new(size, size * 2);
                for i in 0..size {
                    builder.add_node(i as u64, &["Person"]);
                }
                for i in 0..size {
                    let from = i as u64;
                    let to = ((i + 1) % size) as u64;
                    builder.add_rel(from, to, "KNOWS");
                }
                
                b.iter(|| {
                    let mut builder_clone = GraphBuilder::new(size, size * 2);
                    for i in 0..size {
                        builder_clone.add_node(i as u64, &["Person"]);
                    }
                    for i in 0..size {
                        let from = i as u64;
                        let to = ((i + 1) % size) as u64;
                        builder_clone.add_rel(from, to, "KNOWS");
                    }
                    let snapshot = builder_clone.finalize();
                    black_box(snapshot);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    builder_add_nodes_benchmark,
    builder_add_nodes_with_labels_benchmark,
    builder_add_relationships_benchmark,
    builder_add_properties_benchmark,
    builder_finalize_benchmark
);
criterion_main!(benches);

