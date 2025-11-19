//! Benchmarks for GraphSnapshot query operations
//!
//! Tests the performance of querying immutable GraphSnapshot instances,
//! including neighbor lookups, property access, and label queries.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rustychickpeas_core::{GraphBuilder, GraphSnapshot};
use std::env;

fn get_criterion() -> Criterion {
    let mut criterion = Criterion::default();
    
    // If BENCHMARK_BASELINE is set, use it as the baseline for comparison
    if let Ok(baseline) = env::var("BENCHMARK_BASELINE") {
        criterion = criterion.save_baseline(baseline);
    }
    
    criterion
}

fn setup_snapshot(num_nodes: usize, num_rels: usize) -> GraphSnapshot {
    let mut builder = GraphBuilder::new(Some(num_nodes), Some(num_rels));
    
    // Add nodes with labels and properties
    for i in 0..num_nodes {
        let labels = if i % 3 == 0 {
            &["Person", "User"][..]
        } else if i % 3 == 1 {
            &["Person", "Admin"][..]
        } else {
            &["Company"][..]
        };
        builder.add_node(Some(i as u32), labels);
        builder.set_prop_str(i as u32, "name", &format!("Entity{}", i));
        builder.set_prop_i64(i as u32, "id", i as i64);
    }
    
    // Add relationships
    for i in 0..num_rels {
        let from = (i % num_nodes) as u64;
        let to = ((i + 1) % num_nodes) as u64;
        let rel_type = if i % 2 == 0 { "KNOWS" } else { "WORKS_FOR" };
        builder.add_rel(from as u32, to as u32, rel_type);
    }
    
    builder.finalize(None)
}

fn snapshot_get_neighbors_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_get_neighbors");
    
    for size in [100, 1000, 10000, 100000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        let test_node = (*size / 2) as u32;
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let neighbors = snapshot.out_neighbors(black_box(test_node));
                    black_box(neighbors);
                });
            },
        );
    }
    group.finish();
}

fn snapshot_get_property_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_get_property");
    
    for size in [100, 1000, 10000, 100000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        let test_node = (*size / 2) as u32;
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let prop = snapshot.prop(black_box(test_node), "name");
                    black_box(prop);
                });
            },
        );
    }
    group.finish();
}

fn snapshot_get_nodes_with_label_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_get_nodes_with_label");
    
    for size in [100, 1000, 10000, 100000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let nodes = snapshot.nodes_with_label("Person");
                    black_box(nodes);
                });
            },
        );
    }
    group.finish();
}

fn snapshot_get_nodes_with_property_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_get_nodes_with_property");
    
    for size in [100, 1000, 10000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        let test_value = 42i64;
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let nodes = snapshot.nodes_with_property("Person", "id", black_box(test_value));
                    black_box(nodes);
                });
            },
        );
    }
    group.finish();
}

fn snapshot_get_degree_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_get_degree");
    
    for size in [100, 1000, 10000, 100000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        let test_node = (*size / 2) as u32;
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    // Calculate degree from neighbors
                    let neighbors = snapshot.out_neighbors(black_box(test_node));
                    let degree = neighbors.len();
                    black_box(degree);
                });
            },
        );
    }
    group.finish();
}

fn snapshot_traversal_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_traversal");
    
    for size in [100, 1000, 10000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        let start_node = 0u32;
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    // 2-hop traversal
                    let mut count = 0;
                    let neighbors1 = snapshot.out_neighbors(black_box(start_node));
                    for &n1 in neighbors1.iter().take(10) {
                        let neighbors2 = snapshot.out_neighbors(n1);
                        count += neighbors2.len();
                    }
                    black_box(count);
                });
            },
        );
    }
    group.finish();
}

fn bidirectional_bfs_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bidirectional_bfs");
    
    for size in [100, 1000, 10000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        
        // Create source and target node sets
        use rustychickpeas_core::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Source nodes: first 10% of nodes
        let source_nodes: Vec<u32> = (0..(*size / 10)).map(|i| i as u32).collect();
        let source = NodeSet::new(RoaringBitmap::from_iter(source_nodes.iter().copied()));
        
        // Target nodes: last 10% of nodes
        let target_nodes: Vec<u32> = ((*size * 9 / 10)..*size).map(|i| i as u32).collect();
        let target = NodeSet::new(RoaringBitmap::from_iter(target_nodes.iter().copied()));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                // Type annotations needed for None filters
                type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
                        black_box(&source),
                        black_box(&target),
                        rustychickpeas_core::types::Direction::Outgoing,
                        None,
                        None,
                        None,
                        None,
                    );
                    black_box((nodes, rels));
                });
            },
        );
    }
    group.finish();
}

fn bidirectional_bfs_with_filters_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bidirectional_bfs_with_filters");
    
    for size in [100, 1000, 10000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        
        use rustychickpeas_core::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        let source_nodes: Vec<u32> = (0..(*size / 10)).map(|i| i as u32).collect();
        let source = NodeSet::new(RoaringBitmap::from_iter(source_nodes.iter().copied()));
        
        let target_nodes: Vec<u32> = ((*size * 9 / 10)..*size).map(|i| i as u32).collect();
        let target = NodeSet::new(RoaringBitmap::from_iter(target_nodes.iter().copied()));
        
        group.bench_with_input(
            BenchmarkId::new("with_rel_type_filter", *size),
            size,
            |b, _| {
                type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
                        black_box(&source),
                        black_box(&target),
                        rustychickpeas_core::types::Direction::Outgoing,
                        Some(&["KNOWS"]),
                        None,
                        None,
                        None,
                    );
                    black_box((nodes, rels));
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("with_node_filter", *size),
            size,
            |b, _| {
                let node_filter = |node_id: u32, snapshot: &GraphSnapshot| -> bool {
                    snapshot.nodes_with_label("Person")
                        .map_or(false, |nodes| nodes.contains(node_id))
                };
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bidirectional_bfs(
                        black_box(&source),
                        black_box(&target),
                        rustychickpeas_core::types::Direction::Outgoing,
                        None,
                        Some(&node_filter),
                        None::<RelFilter>,
                        None,
                    );
                    black_box((nodes, rels));
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("with_max_depth", *size),
            size,
            |b, _| {
                type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bidirectional_bfs::<NodeFilter, RelFilter>(
                        black_box(&source),
                        black_box(&target),
                        rustychickpeas_core::types::Direction::Outgoing,
                        None,
                        None,
                        None,
                        Some(5),
                    );
                    black_box((nodes, rels));
                });
            },
        );
    }
    group.finish();
}

fn bfs_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bfs");
    
    for size in [100, 1000, 10000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        
        use rustychickpeas_core::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        // Start nodes: first 10% of nodes
        let start_nodes: Vec<u32> = (0..(*size / 10)).map(|i| i as u32).collect();
        let start = NodeSet::new(RoaringBitmap::from_iter(start_nodes.iter().copied()));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                // Type annotations needed for None filters
                type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
                        black_box(&start),
                        rustychickpeas_core::types::Direction::Outgoing,
                        None,
                        None,
                        None,
                        None,
                    );
                    black_box((nodes, rels));
                });
            },
        );
    }
    group.finish();
}

fn bfs_with_filters_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bfs_with_filters");
    
    for size in [100, 1000, 10000].iter() {
        let snapshot = setup_snapshot(*size, *size * 2);
        
        use rustychickpeas_core::bitmap::NodeSet;
        use roaring::RoaringBitmap;
        
        let start_nodes: Vec<u32> = (0..(*size / 10)).map(|i| i as u32).collect();
        let start = NodeSet::new(RoaringBitmap::from_iter(start_nodes.iter().copied()));
        
        group.bench_with_input(
            BenchmarkId::new("with_rel_type_filter", *size),
            size,
            |b, _| {
                type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
                        black_box(&start),
                        rustychickpeas_core::types::Direction::Outgoing,
                        Some(&["KNOWS"]),
                        None,
                        None,
                        None,
                    );
                    black_box((nodes, rels));
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("with_node_filter", *size),
            size,
            |b, _| {
                let node_filter = |node_id: u32, snapshot: &GraphSnapshot| -> bool {
                    snapshot.nodes_with_label("Person")
                        .map_or(false, |nodes| nodes.contains(node_id))
                };
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bfs(
                        black_box(&start),
                        rustychickpeas_core::types::Direction::Outgoing,
                        None,
                        Some(&node_filter),
                        None::<RelFilter>,
                        None,
                    );
                    black_box((nodes, rels));
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("with_max_depth", *size),
            size,
            |b, _| {
                type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
                        black_box(&start),
                        rustychickpeas_core::types::Direction::Outgoing,
                        None,
                        None,
                        None,
                        Some(5),
                    );
                    black_box((nodes, rels));
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("with_direction_both", *size),
            size,
            |b, _| {
                type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
                type RelFilter = fn(u32, u32, rustychickpeas_core::types::RelationshipType, u32, &GraphSnapshot) -> bool;
                b.iter(|| {
                    let (nodes, rels) = snapshot.bfs::<NodeFilter, RelFilter>(
                        black_box(&start),
                        rustychickpeas_core::types::Direction::Both,
                        None,
                        None,
                        None,
                        None,
                    );
                    black_box((nodes, rels));
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
        snapshot_get_neighbors_benchmark,
        snapshot_get_property_benchmark,
        snapshot_get_nodes_with_label_benchmark,
        snapshot_get_nodes_with_property_benchmark,
        snapshot_get_degree_benchmark,
        snapshot_traversal_benchmark,
        bidirectional_bfs_benchmark,
        bidirectional_bfs_with_filters_benchmark,
        bfs_benchmark,
        bfs_with_filters_benchmark
}
criterion_main!(benches);

