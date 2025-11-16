//! Benchmarks for bulk loading operations
//!
//! Tests the performance of loading data from Parquet files
//! and building GraphSnapshot instances.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rustychickpeas_core::{GraphBuilder, GraphSnapshot};
use std::fs::File;
use tempfile::TempDir;
use parquet::file::properties::WriterProperties;
use parquet::arrow::ArrowWriter;
use arrow::array::{Int64Array, StringArray, BooleanArray};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};

fn create_test_parquet_file(num_rows: usize, temp_dir: &TempDir) -> std::path::PathBuf {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]);
    
    let ids: Vec<i64> = (1..=num_rows as i64).collect();
    let labels: Vec<String> = (1..=num_rows).map(|i| {
        if i % 2 == 0 { "Person" } else { "Company" }
    }).map(|s| s.to_string()).collect();
    let names: Vec<String> = (1..=num_rows).map(|i| format!("Entity{}", i)).collect();
    let active: Vec<bool> = (1..=num_rows).map(|i| i % 2 == 0).collect();
    
    let id_array = Int64Array::from(ids);
    let label_array = StringArray::from(labels);
    let name_array = StringArray::from(names);
    let active_array = BooleanArray::from(active);
    
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema.clone()),
        vec![
            std::sync::Arc::new(id_array),
            std::sync::Arc::new(label_array),
            std::sync::Arc::new(name_array),
            std::sync::Arc::new(active_array),
        ],
    ).unwrap();
    
    let file_path = temp_dir.path().join("nodes.parquet");
    let file = File::create(&file_path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, std::sync::Arc::new(schema), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    
    file_path
}

fn bulk_load_nodes_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_load_nodes");
    
    for size in [1000, 10000, 100000, 1_000_000, 2_000_000, 5_000_000, 25_000_000].iter() {
        // Skip very large files in regular benchmarks (too slow for CI)
        // Use --bench bulk_load_nodes -- --include-ignored to run
        if *size > 1_000_000 {
            group.bench_with_input(
                BenchmarkId::from_parameter(size),
                size,
                |b, _| {
                    b.iter_custom(|iters| {
                        // Create file once per iteration
                        let temp_dir = TempDir::new().unwrap();
                        let parquet_file = create_test_parquet_file(*size, &temp_dir);
                        
                        let start = std::time::Instant::now();
                        for _i in 0..iters {
                            let mut builder = GraphBuilder::new(Some(*size), Some(0));
                            builder.load_nodes_from_parquet(
                                parquet_file.to_str().unwrap(),
                                Some("id"),
                                Some(vec!["label"]),
                                Some(vec!["name", "active"]),
                            ).unwrap();
                            black_box(builder);
                        }
                        start.elapsed()
                    });
                },
            );
        } else {
            let temp_dir = TempDir::new().unwrap();
            let parquet_file = create_test_parquet_file(*size, &temp_dir);
            
            group.bench_with_input(
                BenchmarkId::from_parameter(size),
                size,
                |b, _| {
                    b.iter(|| {
                        let mut builder = GraphBuilder::new(Some(*size), Some(0));
                        builder.load_nodes_from_parquet(
                            parquet_file.to_str().unwrap(),
                            Some("id"),
                            Some(vec!["label"]),
                            Some(vec!["name", "active"]),
                        ).unwrap();
                        black_box(builder);
                    });
                },
            );
        }
    }
    group.finish();
}

fn bulk_load_complete_graph_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_load_complete_graph");
    
    for size in [1000, 10000].iter() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_file = create_test_parquet_file(*size, &temp_dir);
        
        // Create relationships file
        let rels_schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from_ids: Vec<i64> = (1..=*size as i64).collect();
        let to_ids: Vec<i64> = (2..=*size as i64).chain(std::iter::once(1)).collect();
        let types: Vec<String> = (1..=*size).map(|i| {
            if i % 2 == 0 { "KNOWS" } else { "WORKS_FOR" }
        }).map(|s| s.to_string()).collect();
        
        let from_array = Int64Array::from(from_ids);
        let to_array = Int64Array::from(to_ids);
        let type_array = StringArray::from(types);
        
        let rels_batch = RecordBatch::try_new(
            std::sync::Arc::new(rels_schema.clone()),
            vec![
                std::sync::Arc::new(from_array),
                std::sync::Arc::new(to_array),
                std::sync::Arc::new(type_array),
            ],
        ).unwrap();
        
        let rels_file_path = temp_dir.path().join("relationships.parquet");
        let rels_file = File::create(&rels_file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(rels_file, std::sync::Arc::new(rels_schema), Some(props)).unwrap();
        writer.write(&rels_batch).unwrap();
        writer.close().unwrap();
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let snapshot = GraphSnapshot::from_parquet(
                        Some(nodes_file.to_str().unwrap()),
                        Some(rels_file_path.to_str().unwrap()),
                        Some("id"),
                        Some(vec!["label"]),
                        Some(vec!["name", "active"]),
                        Some("from"),
                        Some("to"),
                        Some("type"),
                        None,
                    ).unwrap();
                    black_box(snapshot);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bulk_load_nodes_benchmark,
    bulk_load_complete_graph_benchmark
);
criterion_main!(benches);

