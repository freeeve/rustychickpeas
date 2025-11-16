//! Benchmarks for GraphSnapshot query operations
//!
//! Tests the performance of querying immutable GraphSnapshot instances,
//! including neighbor lookups, property access, and label queries.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rustychickpeas_core::{GraphBuilder, GraphSnapshot};

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
        builder.add_node(i as u64, labels);
        builder.set_prop_str(i as u64, "name", &format!("Entity{}", i));
        builder.set_prop_i64(i as u64, "id", i as i64);
    }
    
    // Add relationships
    for i in 0..num_rels {
        let from = (i % num_nodes) as u64;
        let to = ((i + 1) % num_nodes) as u64;
        let rel_type = if i % 2 == 0 { "KNOWS" } else { "WORKS_FOR" };
        builder.add_rel(from, to, rel_type);
    }
    
    builder.finalize()
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
                    let neighbors = snapshot.get_out_neighbors(black_box(test_node));
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
        // Find the "name" property key ID
        let name_key = snapshot.atoms.strings.iter()
            .position(|s| s == "name")
            .map(|i| i as u32)
            .unwrap_or(0);
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let prop = snapshot.get_property(black_box(test_node), name_key);
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
        // Find the "Person" label ID
        let person_label_id = snapshot.atoms.strings.iter()
            .position(|s| s == "Person")
            .map(|i| rustychickpeas_core::Label::new(i as u32))
            .unwrap_or(rustychickpeas_core::Label::new(0));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let nodes = snapshot.get_nodes_with_label(black_box(person_label_id));
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
        // Find the "id" property key ID
        let id_key = snapshot.atoms.strings.iter()
            .position(|s| s == "id")
            .map(|i| i as u32)
            .unwrap_or(0);
        use rustychickpeas_core::ValueId;
        let test_value = ValueId::I64(42);
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let nodes = snapshot.get_nodes_with_property(id_key, black_box(test_value));
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
                    let neighbors = snapshot.get_out_neighbors(black_box(test_node));
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
                    let neighbors1 = snapshot.get_out_neighbors(black_box(start_node));
                    for &n1 in neighbors1.iter().take(10) {
                        let neighbors2 = snapshot.get_out_neighbors(n1);
                        count += neighbors2.len();
                    }
                    black_box(count);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    snapshot_get_neighbors_benchmark,
    snapshot_get_property_benchmark,
    snapshot_get_nodes_with_label_benchmark,
    snapshot_get_nodes_with_property_benchmark,
    snapshot_get_degree_benchmark,
    snapshot_traversal_benchmark
);
criterion_main!(benches);

