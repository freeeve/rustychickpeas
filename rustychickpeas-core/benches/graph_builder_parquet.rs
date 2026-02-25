//! Benchmarks for bulk loading operations
//!
//! Tests the performance of loading data from Parquet files
//! and building GraphSnapshot instances.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rustychickpeas_core::{GraphBuilder, GraphSnapshot, RelationshipDeduplication};
use std::fs::File;
use std::env;
use tempfile::TempDir;
use parquet::file::properties::WriterProperties;

fn get_criterion() -> Criterion {
    let mut criterion = Criterion::default();
    
    // If BENCHMARK_BASELINE is set, use it as the baseline for comparison
    if let Ok(baseline) = env::var("BENCHMARK_BASELINE") {
        criterion = criterion.save_baseline(baseline);
    }
    
    criterion
}
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
    
    for size in [1000, 10000, 100000].iter() {
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
                        None, // unique_properties
                    ).unwrap();
                    black_box(builder);
                });
            },
        );
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

fn bulk_load_nodes_with_deduplication_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_load_nodes_with_deduplication");
    
    for size in [1000, 10000, 100000].iter() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create Parquet file with duplicate emails (every 10 nodes share the same email)
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        
        let ids: Vec<i64> = (1..=*size as i64).collect();
        let labels: Vec<String> = (1..=*size).map(|_| "Person".to_string()).collect();
        let emails: Vec<String> = (1..=*size).map(|i| format!("user{}@example.com", i / 10)).collect();
        let names: Vec<String> = (1..=*size).map(|i| format!("Person{}", i)).collect();
        
        let id_array = Int64Array::from(ids);
        let label_array = StringArray::from(labels);
        let email_array = StringArray::from(emails);
        let name_array = StringArray::from(names);
        
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema.clone()),
            vec![
                std::sync::Arc::new(id_array),
                std::sync::Arc::new(label_array),
                std::sync::Arc::new(email_array),
                std::sync::Arc::new(name_array),
            ],
        ).unwrap();
        
        let file_path = temp_dir.path().join("nodes_dedup.parquet");
        let file = File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, std::sync::Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(*size), Some(0));
                    builder.load_nodes_from_parquet(
                        file_path.to_str().unwrap(),
                        Some("id"),
                        Some(vec!["label"]),
                        Some(vec!["email", "name"]),
                        Some(vec!["email"]), // unique_properties for deduplication
                    ).unwrap();
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn bulk_load_relationships_with_deduplication_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_load_relationships_with_deduplication");
    
    for size in [1000, 10000, 100000].iter() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create nodes file
        let nodes_file = create_test_parquet_file(*size, &temp_dir);
        
        // Create relationships file with duplicates
        let rels_schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        // Create many duplicate relationships (same type between same nodes)
        let size_val = *size;
        let from_ids: Vec<i64> = (0..size_val).map(|i| (i % (size_val / 10).max(1)) as i64 + 1).collect();
        let to_ids: Vec<i64> = (0..size_val).map(|i| ((i + 1) % (size_val / 10).max(1)) as i64 + 1).collect();
        let types: Vec<String> = (0..size_val).map(|_| "KNOWS".to_string()).collect();
        
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
        
        let rels_file_path = temp_dir.path().join("relationships_dedup.parquet");
        let rels_file = File::create(&rels_file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(rels_file, std::sync::Arc::new(rels_schema), Some(props)).unwrap();
        writer.write(&rels_batch).unwrap();
        writer.close().unwrap();
        
        // Benchmark with CreateUniqueByRelType deduplication
        group.bench_with_input(
            BenchmarkId::new("by_type", *size),
            size,
            |b, _| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(*size), Some(*size));
                    builder.load_nodes_from_parquet(
                        nodes_file.to_str().unwrap(),
                        Some("id"),
                        Some(vec!["label"]),
                        Some(vec!["name", "active"]),
                        None,
                    ).unwrap();
                    builder.load_relationships_from_parquet(
                        rels_file_path.to_str().unwrap(),
                        "from",
                        "to",
                        Some("type"),
                        None,
                        None,
                        Some(RelationshipDeduplication::CreateUniqueByRelType),
                        None, // key_property_columns
                    ).unwrap();
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn parquet_vs_regular_builder_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_vs_regular_builder");
    
    for size in [10000, 100000].iter() {
        let temp_dir = TempDir::new().unwrap();
        let parquet_file = create_test_parquet_file(*size, &temp_dir);
        
        // Benchmark: Parquet loading
        group.bench_with_input(
            BenchmarkId::new("parquet", *size),
            size,
            |b, _| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(*size), Some(0));
                    builder.load_nodes_from_parquet(
                        parquet_file.to_str().unwrap(),
                        Some("id"),
                        Some(vec!["label"]),
                        Some(vec!["name", "active"]),
                        None,
                    ).unwrap();
                    black_box(builder);
                });
            },
        );
        
        // Benchmark: Regular GraphBuilder (simulating same data)
        group.bench_with_input(
            BenchmarkId::new("regular", *size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(0));
                    for i in 0..size {
                        let labels = if i % 2 == 0 { vec!["Person"] } else { vec!["Company"] };
                        builder.add_node(Some(i as u32), &labels).unwrap();
                        builder.set_prop_str(i as u32, "name", &format!("Entity{}", i));
                        builder.set_prop_bool(i as u32, "active", i % 2 == 0);
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn parquet_vs_regular_with_dedup_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_vs_regular_with_dedup");
    
    for size in [10000, 100000].iter() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create Parquet file with duplicate emails
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        
        let ids: Vec<i64> = (1..=*size as i64).collect();
        let labels: Vec<String> = (1..=*size).map(|i| {
            if i % 2 == 0 { "Person" } else { "Company" }
        }).map(|s| s.to_string()).collect();
        let emails: Vec<String> = (1..=*size).map(|i| format!("user{}@example.com", i / 10)).collect();
        let names: Vec<String> = (1..=*size).map(|i| format!("Entity{}", i)).collect();
        
        let id_array = Int64Array::from(ids);
        let label_array = StringArray::from(labels);
        let email_array = StringArray::from(emails);
        let name_array = StringArray::from(names);
        
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema.clone()),
            vec![
                std::sync::Arc::new(id_array),
                std::sync::Arc::new(label_array),
                std::sync::Arc::new(email_array),
                std::sync::Arc::new(name_array),
            ],
        ).unwrap();
        
        let file_path = temp_dir.path().join("nodes_dedup_compare.parquet");
        let file = File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, std::sync::Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        // Benchmark: Parquet loading with deduplication
        group.bench_with_input(
            BenchmarkId::new("parquet_dedup", *size),
            size,
            |b, _| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(*size), Some(0));
                    builder.load_nodes_from_parquet(
                        file_path.to_str().unwrap(),
                        Some("id"),
                        Some(vec!["label"]),
                        Some(vec!["email", "name"]),
                        Some(vec!["email"]), // deduplication
                    ).unwrap();
                    black_box(builder);
                });
            },
        );
        
        // Benchmark: Regular GraphBuilder with deduplication (simulating same data)
        group.bench_with_input(
            BenchmarkId::new("regular_dedup", *size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(0));
                    builder.enable_node_deduplication(vec!["email"]);
                    for i in 0..size {
                        let labels = if i % 2 == 0 { vec!["Person"] } else { vec!["Company"] };
                        let email = format!("user{}@example.com", i / 10);
                        builder.add_node(Some(i as u32), &labels).unwrap();
                        builder.set_prop_str(i as u32, "email", &email);
                        builder.set_prop_str(i as u32, "name", &format!("Entity{}", i));
                    }
                    black_box(builder);
                });
            },
        );
    }
    group.finish();
}

fn finalize_with_deduplication_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("finalize_with_deduplication");
    
    for size in [10000, 100000].iter() {
        // Setup: build graph with duplicates
        let mut setup_builder = GraphBuilder::new(Some(*size), Some(*size));
        setup_builder.enable_node_deduplication(vec!["email"]);
        for i in 0..*size {
            let email = format!("user{}@example.com", i / 10);
            setup_builder.add_node(Some(i as u32), &["Person"]).unwrap();
            setup_builder.set_prop_str(i as u32, "email", &email);
        }
        for i in 0..*size {
            let from = (i % (*size / 10).max(1)) as u32;
            let to = ((i + 1) % (*size / 10).max(1)) as u32;
            setup_builder.add_rel(from, to, "KNOWS");
        }
        
        // Benchmark: Finalize with deduplication
        group.bench_with_input(
            BenchmarkId::new("with_dedup", *size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size));
                    builder.enable_node_deduplication(vec!["email"]);
                    for i in 0..size {
                        let email = format!("user{}@example.com", i / 10);
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                        builder.set_prop_str(i as u32, "email", &email);
                    }
                    for i in 0..size {
                        let from = (i % (size / 10).max(1)) as u32;
                        let to = ((i + 1) % (size / 10).max(1)) as u32;
                        builder.add_rel(from, to, "KNOWS");
                    }
                    let snapshot = builder.finalize(None);
                    black_box(snapshot);
                });
            },
        );
        
        // Benchmark: Finalize without deduplication
        group.bench_with_input(
            BenchmarkId::new("no_dedup", *size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = GraphBuilder::new(Some(size), Some(size));
                    for i in 0..size {
                        let email = format!("user{}@example.com", i / 10);
                        builder.add_node(Some(i as u32), &["Person"]).unwrap();
                        builder.set_prop_str(i as u32, "email", &email);
                    }
                    for i in 0..size {
                        let from = (i % (size / 10).max(1)) as u32;
                        let to = ((i + 1) % (size / 10).max(1)) as u32;
                        builder.add_rel(from, to, "KNOWS");
                    }
                    let snapshot = builder.finalize(None);
                    black_box(snapshot);
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
        bulk_load_nodes_benchmark,
        bulk_load_complete_graph_benchmark,
        bulk_load_nodes_with_deduplication_benchmark,
        bulk_load_relationships_with_deduplication_benchmark,
        parquet_vs_regular_builder_benchmark,
        parquet_vs_regular_with_dedup_benchmark,
        finalize_with_deduplication_benchmark
}
criterion_main!(benches);

