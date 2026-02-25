//! Integration tests for S3 Parquet loading
//! 
//! These tests require LocalStack to be running. To set up LocalStack:
//! 
//! ```bash
//! docker run --rm -d \
//!   --name localstack \
//!   -p 4566:4566 \
//!   -e SERVICES=s3 \
//!   localstack/localstack
//! ```
//! 
//! To run these tests:
//! ```bash
//! cargo test --test s3_integration_test -- --ignored
//! ```

use rustychickpeas_core::graph_builder::GraphBuilder;
use tempfile::TempDir;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use std::sync::Arc;

/// Check if LocalStack is available
fn is_localstack_available() -> bool {
    let client = match reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    
    match client.get("http://localhost:4566/_localstack/health").send() {
        Ok(resp) => {
            if let Ok(json) = resp.json::<serde_json::Value>() {
                json.get("services")
                    .and_then(|s| s.get("s3"))
                    .and_then(|s| s.as_str())
                    .map(|status| status == "running" || status == "available")
                    .unwrap_or(false)
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

/// Create a test nodes Parquet file with properties
fn create_test_nodes_parquet(temp_dir: &TempDir, filename: &str) -> std::path::PathBuf {
    use arrow::array::{Int64Array, StringArray, Float64Array, BooleanArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]);
    
    let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let labels = StringArray::from(vec!["Person", "Person", "Company", "Person", "Company"]);
    let names = StringArray::from(vec!["Alice", "Bob", "Acme", "Charlie", "Beta"]);
    let ages = Int64Array::from(vec![Some(30), Some(25), None, Some(35), Some(40)]);
    let scores = Float64Array::from(vec![Some(95.5), Some(88.0), None, Some(92.5), Some(90.0)]);
    let active = BooleanArray::from(vec![Some(true), Some(false), Some(true), None, Some(false)]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ids),
            Arc::new(labels),
            Arc::new(names),
            Arc::new(ages),
            Arc::new(scores),
            Arc::new(active),
        ],
    )
    .unwrap();
    
    let file_path = temp_dir.path().join(filename);
    let file = std::fs::File::create(&file_path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    
    file_path
}

/// Create a test relationships Parquet file with properties
fn create_test_relationships_parquet(temp_dir: &TempDir, filename: &str) -> std::path::PathBuf {
    use arrow::array::{Int64Array, StringArray, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    
    let schema = Schema::new(vec![
        Field::new("from", DataType::Int64, false),
        Field::new("to", DataType::Int64, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, true),
        Field::new("since", DataType::Int64, true),
    ]);
    
    let from = Int64Array::from(vec![1, 2, 3, 4]);
    let to = Int64Array::from(vec![2, 3, 4, 5]);
    let types = StringArray::from(vec!["KNOWS", "WORKS_FOR", "KNOWS", "WORKS_FOR"]);
    let weights = Float64Array::from(vec![Some(0.8), Some(1.0), Some(0.5), None]);
    let since = Int64Array::from(vec![Some(2020), Some(2019), Some(2021), Some(2018)]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(from),
            Arc::new(to),
            Arc::new(types),
            Arc::new(weights),
            Arc::new(since),
        ],
    )
    .unwrap();
    
    let file_path = temp_dir.path().join(filename);
    let file = std::fs::File::create(&file_path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    
    file_path
}

/// Check if bucket exists and create it if it doesn't, using object_store API
/// LocalStack auto-creates buckets on first PUT, so we use that mechanism
async fn ensure_bucket_exists(bucket: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Configure S3 client for LocalStack
    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint("http://localhost:4566")
        .with_region("us-east-1")
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .with_allow_http(true)
        .build()?;
    
    let store: Arc<dyn ObjectStore> = Arc::new(s3);
    
    // Try to check if bucket exists by listing (empty list is fine)
    use futures::StreamExt;
    let mut stream = store.list(None);
    
    // Check first item to see if bucket exists
    match stream.next().await {
        Some(Ok(_)) | None => {
            // Bucket exists (got items or empty list)
            Ok(())
        }
        Some(Err(object_store::Error::NotFound { .. })) => {
            // Bucket doesn't exist - create it by putting a dummy object
            // LocalStack will auto-create the bucket on first PUT
            let dummy_path = ObjectPath::from("__bucket_check__");
            let dummy_data = bytes::Bytes::from("dummy");
            
            store.put(&dummy_path, dummy_data.into()).await?;
            // Clean up the dummy object
            let _ = store.delete(&dummy_path).await;
            Ok(())
        }
        Some(Err(e)) => Err(Box::new(e)),
    }
}

/// Upload a file to LocalStack S3
/// Creates the bucket if it doesn't exist, then uploads the file
async fn upload_to_localstack(
    bucket: &str,
    key: &str,
    file_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Ensure bucket exists first
    ensure_bucket_exists(bucket).await?;
    
    // Configure S3 client for LocalStack with explicit endpoint
    // Note: LocalStack uses HTTP, so we need to allow it
    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint("http://localhost:4566")
        .with_region("us-east-1")
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .with_allow_http(true)
        .build()?;
    
    let store: Arc<dyn ObjectStore> = Arc::new(s3);
    let path = ObjectPath::from(key);
    let data = std::fs::read(file_path)?;
    let bytes = bytes::Bytes::from(data);
    
    store.put(&path, bytes.into()).await?;
    
    Ok(())
}

/// Setup environment variables for LocalStack
fn setup_localstack_env() {
    std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_DISABLE_IMDSV1", "1"); // Disable metadata service
}

#[test]
#[ignore] // Ignore by default - requires LocalStack
fn test_load_nodes_from_s3() {
    if !is_localstack_available() {
        eprintln!("LocalStack is not available. Skipping S3 test.");
        eprintln!("To run this test, start LocalStack:");
        eprintln!("  docker run --rm -d --name localstack -p 4566:4566 -e SERVICES=s3 localstack/localstack");
        return;
    }
    
    // Create a runtime for async operations
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let temp_dir = TempDir::new().unwrap();
    let parquet_file = create_test_nodes_parquet(&temp_dir, "nodes.parquet");
    
    // Upload to LocalStack
    let bucket = "test-bucket";
    let key = "nodes.parquet";
    
    rt.block_on(async {
        upload_to_localstack(bucket, key, &parquet_file)
            .await
            .expect("Failed to upload to LocalStack");
    });
    
    // Set environment variables for LocalStack
    setup_localstack_env();
    
    // Test loading from S3
    let mut builder = GraphBuilder::new(None, None);
    let s3_path = format!("s3://{}/{}", bucket, key);
    let node_ids = builder
        .load_nodes_from_parquet(&s3_path, Some("id"), Some(vec!["label"]), Some(vec!["name", "age", "score", "active"]), None, None)
        .expect("Failed to load nodes from S3");
    
    assert_eq!(node_ids.len(), 5);
    assert_eq!(node_ids, vec![1, 2, 3, 4, 5]);
    
    // Verify properties were loaded
    use rustychickpeas_core::graph_snapshot::ValueId;
    let name_prop = builder.prop(1, "name");
    assert!(name_prop.is_some());
    if let Some(ValueId::Str(name_id)) = name_prop {
        // Verify the string value by checking it's not None
        assert!(name_id > 0);
    }
    assert_eq!(builder.prop(1, "age"), Some(ValueId::I64(30)));
}

#[test]
#[ignore] // Ignore by default - requires LocalStack
fn test_load_nodes_from_s3_with_deduplication() {
    if !is_localstack_available() {
        eprintln!("LocalStack is not available. Skipping S3 test.");
        return;
    }
    
    let rt = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let parquet_file = create_test_nodes_parquet(&temp_dir, "nodes.parquet");
    
    let bucket = "test-bucket-dedup";
    let key = "nodes.parquet";
    
    rt.block_on(async {
        upload_to_localstack(bucket, key, &parquet_file)
            .await
            .expect("Failed to upload to LocalStack");
    });
    
    setup_localstack_env();
    
    // Test loading with deduplication on name property
    let mut builder = GraphBuilder::new(None, None);
    let s3_path = format!("s3://{}/{}", bucket, key);
    let node_ids = builder
        .load_nodes_from_parquet(&s3_path, Some("id"), Some(vec!["label"]), Some(vec!["name"]), Some(vec!["name"]), None)
        .expect("Failed to load nodes from S3");
    
    assert_eq!(node_ids.len(), 5);
    assert_eq!(builder.node_count(), 5);
}

#[test]
#[ignore] // Ignore by default - requires LocalStack
fn test_load_relationships_from_s3() {
    if !is_localstack_available() {
        eprintln!("LocalStack is not available. Skipping S3 test.");
        return;
    }
    
    let rt = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let rels_file = create_test_relationships_parquet(&temp_dir, "relationships.parquet");
    
    let bucket = "test-bucket-rels";
    let key = "relationships.parquet";
    
    rt.block_on(async {
        upload_to_localstack(bucket, key, &rels_file)
            .await
            .expect("Failed to upload to LocalStack");
    });
    
    setup_localstack_env();
    
    // First add nodes
    let mut builder = GraphBuilder::new(None, None);
    for i in 1..=5 {
        builder.add_node(Some(i), &["Node"]).unwrap();
    }
    
    // Test loading relationships from S3
    let s3_path = format!("s3://{}/{}", bucket, key);
    let rel_ids = builder
        .load_relationships_from_parquet(
            &s3_path,
            "from",
            "to",
            Some("type"),
            Some(vec!["weight", "since"]),
            None,
            None, // deduplication
            None, // key_property_columns
        )
        .expect("Failed to load relationships from S3");
    
    assert_eq!(rel_ids.len(), 4);
    assert_eq!(rel_ids, vec![(1, 2), (2, 3), (3, 4), (4, 5)]);
    assert_eq!(builder.rel_count(), 4);
    
    // Verify relationship properties were loaded by finalizing and checking the snapshot
    let snapshot = builder.finalize(None);
    // Properties are stored internally and accessible through the snapshot
    // We verify the relationships exist and the snapshot was created successfully
    assert_eq!(snapshot.n_nodes, 5);
    assert_eq!(snapshot.n_rels, 4);
}

#[test]
#[ignore] // Ignore by default - requires LocalStack
fn test_load_relationships_from_s3_with_deduplication() {
    if !is_localstack_available() {
        eprintln!("LocalStack is not available. Skipping S3 test.");
        return;
    }
    
    let rt = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let rels_file = create_test_relationships_parquet(&temp_dir, "relationships.parquet");
    
    let bucket = "test-bucket-rels-dedup";
    let key = "relationships.parquet";
    
    rt.block_on(async {
        upload_to_localstack(bucket, key, &rels_file)
            .await
            .expect("Failed to upload to LocalStack");
    });
    
    setup_localstack_env();
    
    // First add nodes
    let mut builder = GraphBuilder::new(None, None);
    for i in 1..=5 {
        builder.add_node(Some(i), &["Node"]).unwrap();
    }
    
    // Test loading relationships with deduplication by type
    let s3_path = format!("s3://{}/{}", bucket, key);
    use rustychickpeas_core::types::RelationshipDeduplication;
    let rel_ids = builder
        .load_relationships_from_parquet(
            &s3_path,
            "from",
            "to",
            Some("type"),
            None,
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelType),
            None, // key_property_columns
        )
        .expect("Failed to load relationships from S3");
    
    assert_eq!(rel_ids.len(), 4);
    assert_eq!(builder.rel_count(), 4);
}

#[test]
#[ignore] // Ignore by default - requires LocalStack
fn test_load_from_s3_invalid_path() {
    if !is_localstack_available() {
        eprintln!("LocalStack is not available. Skipping S3 test.");
        return;
    }
    
    setup_localstack_env();
    
    let mut builder = GraphBuilder::new(None, None);
    
    // Test invalid S3 path format
    let result = builder.load_nodes_from_parquet("s3://invalid", Some("id"), None, None, None, None);
    assert!(result.is_err());
    
    // Test missing file
    let result = builder.load_nodes_from_parquet("s3://test-bucket/nonexistent.parquet", Some("id"), None, None, None, None);
    assert!(result.is_err());
}

#[test]
#[ignore] // Ignore by default - requires LocalStack
fn test_load_nodes_and_relationships_from_s3() {
    if !is_localstack_available() {
        eprintln!("LocalStack is not available. Skipping S3 test.");
        return;
    }
    
    let rt = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let nodes_file = create_test_nodes_parquet(&temp_dir, "nodes.parquet");
    let rels_file = create_test_relationships_parquet(&temp_dir, "relationships.parquet");
    
    let bucket = "test-bucket-full";
    let nodes_key = "nodes.parquet";
    let rels_key = "relationships.parquet";
    
    rt.block_on(async {
        upload_to_localstack(bucket, nodes_key, &nodes_file)
            .await
            .expect("Failed to upload nodes to LocalStack");
        upload_to_localstack(bucket, rels_key, &rels_file)
            .await
            .expect("Failed to upload relationships to LocalStack");
    });
    
    setup_localstack_env();
    
    // Load both nodes and relationships from S3
    let mut builder = GraphBuilder::new(None, None);
    
    let nodes_s3_path = format!("s3://{}/{}", bucket, nodes_key);
    let node_ids = builder
        .load_nodes_from_parquet(&nodes_s3_path, Some("id"), Some(vec!["label"]), Some(vec!["name", "age"]), None, None)
        .expect("Failed to load nodes from S3");
    
    assert_eq!(node_ids.len(), 5);
    
    let rels_s3_path = format!("s3://{}/{}", bucket, rels_key);
    let rel_ids = builder
        .load_relationships_from_parquet(
            &rels_s3_path,
            "from",
            "to",
            Some("type"),
            Some(vec!["weight"]),
            None,
            None,
            None, // key_property_columns
        )
        .expect("Failed to load relationships from S3");
    
    assert_eq!(rel_ids.len(), 4);
    assert_eq!(builder.node_count(), 5);
    assert_eq!(builder.rel_count(), 4);
}

