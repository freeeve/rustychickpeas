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

use rustychickpeas_core::builder::GraphBuilder;
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

/// Create a test Parquet file
fn create_test_parquet_file(temp_dir: &TempDir, filename: &str) -> std::path::PathBuf {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    
    let ids = Int64Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(ids), Arc::new(names)],
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
    let parquet_file = create_test_parquet_file(&temp_dir, "nodes.parquet");
    
    // Upload to LocalStack
    let bucket = "test-bucket";
    let key = "nodes.parquet";
    
    rt.block_on(async {
        upload_to_localstack(bucket, key, &parquet_file)
            .await
            .expect("Failed to upload to LocalStack");
    });
    
    // Set environment variables for LocalStack (needed for S3 client creation)
    // These must be set before the S3 client is created in load_nodes_from_parquet
    std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_DISABLE_IMDSV1", "1"); // Disable metadata service
    
    // Test loading from S3
    let mut builder = GraphBuilder::new(None, None);
    let s3_path = format!("s3://{}/{}", bucket, key);
    let node_ids = builder
        .load_nodes_from_parquet(&s3_path, Some("id"), None, None, None)
        .expect("Failed to load nodes from S3");
    
    assert_eq!(node_ids.len(), 3);
    assert_eq!(node_ids, vec![1, 2, 3]);
}

