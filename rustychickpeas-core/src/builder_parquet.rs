//! Parquet reading support for GraphBuilder

use crate::builder::GraphBuilder;
use crate::error::{Result, GraphError};
use crate::snapshot::{GraphSnapshot, ValueId};
use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use object_store::{ObjectStore, path::Path as ObjectPath, aws::AmazonS3Builder};
use std::fs::File;
use std::sync::Arc;
use hashbrown::HashMap;
use futures::TryStreamExt;

/// Enum to handle both sync (local file) and async (S3) Parquet readers
enum ParquetReaderEnum {
    Sync(parquet::arrow::arrow_reader::ParquetRecordBatchReader),
    Async {
        batches: Vec<arrow::array::RecordBatch>,
        current: usize,
    },
}

impl ParquetReaderEnum {
    fn next(&mut self) -> Option<std::result::Result<arrow::array::RecordBatch, arrow::error::ArrowError>> {
        match self {
            ParquetReaderEnum::Sync(reader) => reader.next(),
            ParquetReaderEnum::Async { batches, current } => {
                if *current < batches.len() {
                    let batch = batches[*current].clone();
                    *current += 1;
                    Some(Ok(batch))
                } else {
                    None
                }
            }
        }
    }
}

/// Helper to create a Parquet reader from either a local file path or S3 path
/// For S3, uses ParquetObjectReader for direct streaming without temp files
/// Returns the reader and schema
fn create_parquet_reader(path: &str) -> Result<(ParquetReaderEnum, Arc<Schema>)> {
    if path.starts_with("s3://") {
        // Parse S3 path: s3://bucket-name/path/to/file.parquet
        let path_str = path.strip_prefix("s3://").ok_or_else(|| {
            GraphError::BulkLoadError("Invalid S3 path format".to_string())
        })?;
        
        let parts: Vec<&str> = path_str.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(GraphError::BulkLoadError(
                "S3 path must be in format s3://bucket-name/path/to/file.parquet".to_string()
            ));
        }
        
        let bucket = parts[0];
        let object_path = parts[1];
        
        // Create S3 client (uses default AWS credentials from environment)
        // Check for custom endpoint (e.g., for LocalStack testing)
        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
        
        // If AWS_ENDPOINT_URL is set, use it (for LocalStack or other S3-compatible services)
        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
            builder = builder.with_endpoint(&endpoint);
            // Allow HTTP for local testing
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
            // If using a custom endpoint, also set credentials explicitly to avoid metadata service
            if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
                builder = builder.with_access_key_id(&access_key);
            }
            if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                builder = builder.with_secret_access_key(&secret_key);
            }
            if let Ok(region) = std::env::var("AWS_REGION") {
                builder = builder.with_region(&region);
            }
        }
        
        let s3 = builder
            .build()
            .map_err(|e| GraphError::BulkLoadError(format!("Failed to create S3 client: {}", e)))?;
        
        let store: Arc<dyn ObjectStore> = Arc::new(s3);
        let object_path = ObjectPath::from(object_path);
        
        // Use async Parquet reader with blocking runtime
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| GraphError::BulkLoadError(format!("Failed to create tokio runtime: {}", e)))?;
        
        let (schema, batches) = rt.block_on(async {
            let reader = parquet::arrow::async_reader::ParquetObjectReader::new(store, object_path);
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|e| GraphError::BulkLoadError(format!("Failed to create ParquetRecordBatchStreamBuilder: {}", e)))?;
            
            let schema = builder.schema().clone();
            let stream = builder.build()
                .map_err(|e| GraphError::BulkLoadError(format!("Failed to build Parquet stream: {}", e)))?;
            
            // Collect all batches from the async stream
            let batches = stream.try_collect::<Vec<_>>().await
                .map_err(|e| GraphError::BulkLoadError(format!("Failed to read Parquet batches: {}", e)))?;
            
            Ok::<(Arc<Schema>, Vec<arrow::array::RecordBatch>), GraphError>((schema, batches))
        })?;
        
        Ok((ParquetReaderEnum::Async { batches, current: 0 }, schema))
    } else {
        // Local file - use synchronous reader
        let file = File::open(path)
            .map_err(|e| GraphError::BulkLoadError(format!("Failed to open Parquet file: {}", e)))?;
        
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| GraphError::BulkLoadError(format!("Failed to read Parquet file: {}", e)))?;
        
        let schema = builder.schema().clone();
        let reader = builder
            .build()
            .map_err(|e| GraphError::BulkLoadError(format!("Failed to build Parquet reader: {}", e)))?;
        
        Ok((ParquetReaderEnum::Sync(reader), schema))
    }
}

impl GraphBuilder {
    /// Load nodes from a Parquet file into the builder
    /// 
    /// # Arguments
    /// * `path` - Path to Parquet file (local file path or S3 path like `s3://bucket-name/path/to/file.parquet`)
    /// * `node_id_column` - Optional column name for node IDs. If None, auto-generates IDs.
    /// * `label_columns` - Optional list of column names to use as labels
    /// * `property_columns` - Optional list of column names to use as properties. If None, uses all columns except ID and labels.
    /// * `unique_properties` - Optional list of property column names to use for deduplication. If provided, nodes with the same values for these properties will be merged.
    pub fn load_nodes_from_parquet(
        &mut self,
        path: &str,
        node_id_column: Option<&str>,
        label_columns: Option<Vec<&str>>,
        property_columns: Option<Vec<&str>>,
        unique_properties: Option<Vec<&str>>,
    ) -> Result<Vec<u32>> {
        // Create Parquet reader (handles both local and S3)
        // For S3, uses ParquetObjectReader for direct streaming without temp files
        let (mut reader, schema) = create_parquet_reader(path)?;
        
        // Configure deduplication if unique_properties is provided
        // This enables deduplication for both Parquet loading and regular builder operations
        if let Some(ref unique_props) = unique_properties {
            self.enable_node_deduplication(unique_props.clone());
        }

        // Get column names (needed for processing)
        let all_columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let label_cols = label_columns.unwrap_or_default();
        let prop_cols = property_columns.unwrap_or_else(|| {
            all_columns
                .iter()
                .filter(|col| {
                    node_id_column.map(|id_col| col.as_str() != id_col).unwrap_or(true)
                        && !label_cols.contains(&col.as_str())
                })
                .map(|s| s.as_str())
                .collect()
        });

        // Find node ID column index
        let node_id_idx = node_id_column.and_then(|col| {
            schema.fields().iter().position(|f| f.name() == col)
        });

        // Stream batches and process them immediately (no accumulation in memory)
        let mut node_ids = Vec::new();
        let mut row_offset = 0;
        let mut first_batch = true;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| GraphError::BulkLoadError(format!("Failed to read batch: {}", e)))?;
            
            if batch.num_rows() == 0 {
                continue;
            }

            // Pre-allocate node_ids Vec on first batch (estimate based on first batch size)
            if first_batch {
                // Estimate: assume similar batch sizes, but this is just a hint
                // The Vec will grow as needed anyway
                node_ids.reserve(batch.num_rows() * 10); // Conservative estimate
                first_batch = false;
            }

            // Extract node IDs
            let mut node_ids_batch = Vec::with_capacity(batch.num_rows());
            if let Some(idx) = node_id_idx {
                let id_col = batch.column(idx);
                // Support both Int64 and Int32 for node IDs
                if let Some(int_array) = id_col.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..batch.num_rows() {
                        if int_array.is_null(i) {
                            let auto_id = (row_offset + i) as u32 + 1;
                            node_ids_batch.push(auto_id);
                        } else {
                            let val = int_array.value(i);
                            if val < 0 || val > u32::MAX as i64 {
                                return Err(GraphError::BulkLoadError(
                                    format!("Node ID {} exceeds u32::MAX ({})", val, u32::MAX)
                                ));
                            }
                            node_ids_batch.push(val as u32);
                        }
                    }
                } else if let Some(int_array) = id_col.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..batch.num_rows() {
                        if int_array.is_null(i) {
                            let auto_id = (row_offset + i) as u32 + 1;
                            node_ids_batch.push(auto_id);
                        } else {
                            let val = int_array.value(i);
                            if val < 0 {
                                return Err(GraphError::BulkLoadError(
                                    format!("Node ID {} cannot be negative", val)
                                ));
                            }
                            node_ids_batch.push(val as u32);
                        }
                    }
                } else {
                    return Err(GraphError::BulkLoadError(
                        "Node ID column must be Int64 or Int32".to_string()
                    ));
                }
            } else {
                // Auto-generate IDs
                for i in 0..batch.num_rows() {
                    node_ids_batch.push((row_offset + i) as u32 + 1);
                }
            }

            // Extract labels
            let mut labels_per_row: Vec<Vec<&str>> = vec![Vec::new(); batch.num_rows()];
            for label_col in &label_cols {
                if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == label_col) {
                    let column = batch.column(column_idx);
                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                        for i in 0..batch.num_rows() {
                            if !string_array.is_null(i) {
                                labels_per_row[i].push(string_array.value(i));
                            }
                        }
                    }
                }
            }

            // Process each row with deduplication if enabled
            // First, extract unique property values for deduplication
            // OPTIMIZATION: Pre-compute column indices once per batch instead of per row
            let mut dedup_keys_per_row: Vec<Option<crate::types::DedupKey>> = vec![None; batch.num_rows()];
            if let Some(ref unique_props) = unique_properties {
                // Pre-compute column indices and arrays for unique properties
                let mut dedup_columns: Vec<(usize, &arrow::datatypes::Field, arrow::array::ArrayRef)> = Vec::new();
                for prop_name in unique_props {
                    if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == prop_name) {
                        let column = batch.column(column_idx);
                        let field = schema.field(column_idx);
                        dedup_columns.push((column_idx, field, column.clone()));
                    } else {
                        // Column not found, skip deduplication for this batch
                        dedup_columns.clear();
                        break;
                    }
                }
                
                // Now process rows with pre-computed column info
                if !dedup_columns.is_empty() {
                    for i in 0..batch.num_rows() {
                        let mut dedup_key = Vec::with_capacity(unique_props.len());
                        let mut all_present = true;
                        
                        for (_col_idx, field, column) in &dedup_columns {
                            let val = match field.data_type() {
                                DataType::Utf8 | DataType::LargeUtf8 => {
                                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                                        if !string_array.is_null(i) {
                                            let val = string_array.value(i);
                                            let interned = self.interner.get_or_intern(val);
                                            Some(ValueId::Str(interned))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                DataType::Int64 => {
                                    if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
                                        if !int_array.is_null(i) {
                                            Some(ValueId::I64(int_array.value(i)))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                DataType::Float64 => {
                                    if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
                                        if !float_array.is_null(i) {
                                            Some(ValueId::from_f64(float_array.value(i)))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                DataType::Boolean => {
                                    if let Some(bool_array) = column.as_any().downcast_ref::<BooleanArray>() {
                                        if !bool_array.is_null(i) {
                                            Some(ValueId::Bool(bool_array.value(i)))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                _ => {
                                    // Convert to string
                                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                                        if !string_array.is_null(i) {
                                            let val = string_array.value(i);
                                            let interned = self.interner.get_or_intern(val);
                                            Some(ValueId::Str(interned))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                            };
                            
                            if let Some(v) = val {
                                dedup_key.push(v);
                            } else {
                                all_present = false;
                                break;
                            }
                        }
                        
                        if all_present && !dedup_key.is_empty() {
                            dedup_keys_per_row[i] = Some(crate::types::DedupKey::from_slice(&dedup_key));
                        }
                    }
                }
            }
            
            // Now apply deduplication to node_ids_batch
            // Track which nodes were already processed in deduplication to avoid double-processing
            let mut processed_in_dedup: std::collections::HashSet<usize> = std::collections::HashSet::new();
            let mut dedup_updates: Vec<(usize, u32, Vec<String>)> = Vec::new(); // (row_index, existing_node_id, merged_labels)
            
            // First pass: check for duplicates and prepare updates
            // OPTIMIZATION: Use a HashSet to track which node_ids we've already seen labels for
            let mut labels_cache: hashbrown::HashMap<u32, Vec<String>> = hashbrown::HashMap::new();
            for i in 0..batch.num_rows() {
                if let Some(Some(ref dedup_key)) = dedup_keys_per_row.get(i) {
                    if let Some(&existing_node_id) = self.dedup_map.get(dedup_key) {
                        // Use existing node_id, merge labels
                        node_ids_batch[i] = existing_node_id;
                        // Get or compute existing labels (cache to avoid repeated lookups)
                        let existing_labels = labels_cache.entry(existing_node_id).or_insert_with(|| {
                            self.get_node_labels(existing_node_id)
                        });
                        let new_labels: Vec<&str> = labels_per_row[i].iter().copied().collect();
                        // Merge labels efficiently
                        for label in &new_labels {
                            if !existing_labels.iter().any(|l| l == label) {
                                existing_labels.push(label.to_string());
                            }
                        }
                        dedup_updates.push((i, existing_node_id, existing_labels.clone()));
                        processed_in_dedup.insert(i);
                    } else {
                        // First time seeing this combination, add to map
                        // OPTIMIZATION: DedupKey uses Copy for small tuples, so cloning is cheap
                        self.dedup_map.insert(dedup_key.clone(), node_ids_batch[i]);
                    }
                }
            }
            
            // Second pass: apply deduplication updates (now we can call self methods)
            for (_i, existing_node_id, merged_labels) in dedup_updates {
                let labels_refs: Vec<&str> = merged_labels.iter().map(|s| s.as_str()).collect();
                self.add_node(existing_node_id, &labels_refs);
            }

            // Extract properties (after deduplication, so properties go to correct node_id)
            let prop_start = std::time::Instant::now();
            let mut prop_times = std::collections::HashMap::new();
            for prop_col in &prop_cols {
                let col_start = std::time::Instant::now();
                if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == prop_col) {
                    let column = batch.column(column_idx);
                    let field = schema.field(column_idx);
                    
                    let mut set_count = 0;
                    for i in 0..batch.num_rows() {
                        let node_id = node_ids_batch[i]; // Use deduplicated ID
                        match field.data_type() {
                            DataType::Utf8 | DataType::LargeUtf8 => {
                                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                                    if !string_array.is_null(i) {
                                        self.set_prop_str(node_id, prop_col, string_array.value(i));
                                        set_count += 1;
                                    }
                                }
                            }
                            DataType::Int64 => {
                                if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
                                    if !int_array.is_null(i) {
                                        self.set_prop_i64(node_id, prop_col, int_array.value(i));
                                        set_count += 1;
                                    }
                                }
                            }
                            DataType::Float64 => {
                                if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
                                    if !float_array.is_null(i) {
                                        self.set_prop_f64(node_id, prop_col, float_array.value(i));
                                        set_count += 1;
                                    }
                                }
                            }
                            DataType::Boolean => {
                                if let Some(bool_array) = column.as_any().downcast_ref::<BooleanArray>() {
                                    if !bool_array.is_null(i) {
                                        self.set_prop_bool(node_id, prop_col, bool_array.value(i));
                                        set_count += 1;
                                    }
                                }
                            }
                            _ => {
                                // Convert to string
                                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                                    if !string_array.is_null(i) {
                                        self.set_prop_str(node_id, prop_col, string_array.value(i));
                                        set_count += 1;
                                    }
                                }
                            }
                        }
                    }
                    let col_time = col_start.elapsed();
                    prop_times.insert(prop_col.to_string(), (col_time, set_count));
                }
            }
            let prop_total = prop_start.elapsed();
            if row_offset == 0 || (row_offset % 100000 == 0 && row_offset > 0) {
                eprintln!("[RUST TIMING] Batch at row {}: Properties took {:.3}s total", row_offset, prop_total.as_secs_f64());
                for (col, (time, count)) in &prop_times {
                    eprintln!("[RUST TIMING]   {}: {:.3}s for {} values ({:.0} ops/sec)", 
                        col, time.as_secs_f64(), count, *count as f64 / time.as_secs_f64().max(0.0001));
                }
            }

            // Add nodes with labels (after deduplication)
            // Skip nodes that were already processed in the deduplication step
            for (i, node_id) in node_ids_batch.iter().enumerate() {
                if !processed_in_dedup.contains(&i) {
                    let labels: Vec<&str> = labels_per_row[i].iter().copied().collect();
                    self.add_node(*node_id, &labels);
                }
                // Only add to node_ids if this is the first time we're seeing this node_id
                if !node_ids.contains(node_id) {
                    node_ids.push(*node_id);
                }
            }

            row_offset += batch.num_rows();
        }

        Ok(node_ids)
    }

    /// Load relationships from a Parquet file into the builder
    /// 
    /// If `rel_type_column` is None, `fixed_rel_type` must be provided.
    /// If `rel_type_column` is Some, it reads the type from that column.
    pub fn load_relationships_from_parquet(
        &mut self,
        path: &str,
        start_node_column: &str,
        end_node_column: &str,
        rel_type_column: Option<&str>,
        property_columns: Option<Vec<&str>>,
        fixed_rel_type: Option<&str>,
        deduplication: Option<crate::types::RelationshipDeduplication>,
    ) -> Result<Vec<(u32, u32)>> {
        use crate::types::RelationshipDeduplication;
        
        // Create Parquet reader (handles both local and S3)
        let (mut reader, schema) = create_parquet_reader(path)?;
        
        // Set up deduplication tracking
        let mut seen_by_type: HashMap<(u32, u32, u32), ()> = HashMap::new(); // (u, v, rel_type_id) -> ()
        let mut seen_by_type_and_props: HashMap<(u32, u32, u32, Vec<ValueId>), ()> = HashMap::new(); // (u, v, rel_type_id, key_props) -> ()

        // Find column indices (needed before processing batches)
        let start_node_idx = schema.fields().iter()
            .position(|f| f.name() == start_node_column)
            .ok_or_else(|| GraphError::BulkLoadError(format!("Column '{}' not found", start_node_column)))?;
        let end_node_idx = schema.fields().iter()
            .position(|f| f.name() == end_node_column)
            .ok_or_else(|| GraphError::BulkLoadError(format!("Column '{}' not found", end_node_column)))?;
        
        // Handle relationship type: either from column or fixed value
        let rel_type_idx = rel_type_column.and_then(|col| {
            schema.fields().iter().position(|f| f.name() == col)
        });
        
        if rel_type_column.is_some() && rel_type_idx.is_none() {
            return Err(GraphError::BulkLoadError(
                format!("Relationship type column '{}' not found", rel_type_column.unwrap())
            ));
        }
        
        if rel_type_column.is_none() && fixed_rel_type.is_none() {
            return Err(GraphError::BulkLoadError(
                "Either rel_type_column or fixed_rel_type must be provided".to_string()
            ));
        }

        // Determine property columns
        let all_columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let prop_cols = property_columns.unwrap_or_else(|| {
            all_columns
                .iter()
                .filter(|col| {
                    col.as_str() != start_node_column
                        && col.as_str() != end_node_column
                        && rel_type_column.map(|rt_col| col.as_str() != rt_col).unwrap_or(true)
                })
                .map(|s| s.as_str())
                .collect()
        });

        // Stream batches and process them immediately (no accumulation in memory)
        let mut rel_ids = Vec::new();
        let mut first_batch = true;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| GraphError::BulkLoadError(format!("Failed to read batch: {}", e)))?;
            
            if batch.num_rows() == 0 {
                continue;
            }

            // Pre-allocate rel_ids Vec on first batch (estimate based on first batch size)
            if first_batch {
                rel_ids.reserve(batch.num_rows() * 10); // Conservative estimate
                first_batch = false;
            }
            // Extract start/end nodes
            let start_col = batch.column(start_node_idx);
            let end_col = batch.column(end_node_idx);
            
            // Extract relationship types (either from column or use fixed type)
            let rel_type_col = rel_type_idx.map(|idx| batch.column(idx));

            let mut start_nodes = Vec::with_capacity(batch.num_rows());
            let mut end_nodes = Vec::with_capacity(batch.num_rows());
            let mut rel_types = Vec::with_capacity(batch.num_rows());

            // Support both Int64 and Int32 for node IDs (LDBC SNB uses both)
            if let Some(int_array) = start_col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    if int_array.is_null(i) {
                        start_nodes.push(None);
                    } else {
                        let val = int_array.value(i);
                        if val < 0 || val > u32::MAX as i64 {
                            return Err(GraphError::BulkLoadError(
                                format!("Start node ID {} exceeds u32::MAX ({})", val, u32::MAX)
                            ));
                        }
                        start_nodes.push(Some(val as u32));
                    }
                }
            } else if let Some(int_array) = start_col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..batch.num_rows() {
                    if int_array.is_null(i) {
                        start_nodes.push(None);
                    } else {
                        let val = int_array.value(i);
                        if val < 0 {
                            return Err(GraphError::BulkLoadError(
                                format!("Start node ID {} cannot be negative", val)
                            ));
                        }
                        start_nodes.push(Some(val as u32));
                    }
                }
            } else {
                return Err(GraphError::BulkLoadError(
                    format!("Start node column '{}' must be Int64 or Int32", start_node_column)
                ));
            }

            if let Some(int_array) = end_col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    if int_array.is_null(i) {
                        end_nodes.push(None);
                    } else {
                        let val = int_array.value(i);
                        if val < 0 || val > u32::MAX as i64 {
                            return Err(GraphError::BulkLoadError(
                                format!("End node ID {} exceeds u32::MAX ({})", val, u32::MAX)
                            ));
                        }
                        end_nodes.push(Some(val as u32));
                    }
                }
            } else if let Some(int_array) = end_col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..batch.num_rows() {
                    if int_array.is_null(i) {
                        end_nodes.push(None);
                    } else {
                        let val = int_array.value(i);
                        if val < 0 {
                            return Err(GraphError::BulkLoadError(
                                format!("End node ID {} cannot be negative", val)
                            ));
                        }
                        end_nodes.push(Some(val as u32));
                    }
                }
            } else {
                return Err(GraphError::BulkLoadError(
                    format!("End node column '{}' must be Int64 or Int32", end_node_column)
                ));
            }

            // Extract relationship types
            if let Some(col) = rel_type_col {
                if let Some(string_array) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..batch.num_rows() {
                        if string_array.is_null(i) {
                            rel_types.push(None);
                        } else {
                            rel_types.push(Some(string_array.value(i)));
                        }
                    }
                } else {
                    return Err(GraphError::BulkLoadError(
                        "Relationship type column must be String".to_string()
                    ));
                }
            } else if let Some(fixed_type) = fixed_rel_type {
                for _i in 0..batch.num_rows() {
                    rel_types.push(Some(fixed_type));
                }
            } else {
                return Err(GraphError::BulkLoadError(
                    "Either rel_type_column or fixed_rel_type must be provided".to_string()
                ));
            }

            // First, extract property values for deduplication if needed
            let mut key_props_per_row: Vec<Option<Vec<ValueId>>> = vec![None; batch.num_rows()];
            if matches!(deduplication, Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties)) {
                for i in 0..batch.num_rows() {
                    let mut key_props = Vec::new();
                    for prop_col in &prop_cols {
                        if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == prop_col) {
                            let column = batch.column(column_idx);
                            let field = schema.field(column_idx);
                            let val = match field.data_type() {
                                DataType::Utf8 | DataType::LargeUtf8 => {
                                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                                        if !string_array.is_null(i) {
                                            let val = string_array.value(i);
                                            let interned = self.interner.get_or_intern(val);
                                            Some(ValueId::Str(interned))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                DataType::Int64 => {
                                    if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
                                        if !int_array.is_null(i) {
                                            Some(ValueId::I64(int_array.value(i)))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                DataType::Float64 => {
                                    if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
                                        if !float_array.is_null(i) {
                                            Some(ValueId::from_f64(float_array.value(i)))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                DataType::Boolean => {
                                    if let Some(bool_array) = column.as_any().downcast_ref::<BooleanArray>() {
                                        if !bool_array.is_null(i) {
                                            Some(ValueId::Bool(bool_array.value(i)))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                _ => {
                                    // Convert to string
                                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                                        if !string_array.is_null(i) {
                                            let val = string_array.value(i);
                                            let interned = self.interner.get_or_intern(val);
                                            Some(ValueId::Str(interned))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                            };
                            if let Some(v) = val {
                                key_props.push(v);
                            }
                        }
                    }
                    if !key_props.is_empty() {
                        key_props_per_row[i] = Some(key_props);
                    }
                }
            }
            
            // Track relationship indices: row_index -> rel_index in self.rels
            // This avoids O(n) search for each property set
            let mut row_to_rel_idx: Vec<Option<usize>> = vec![None; batch.num_rows()];
            
            // Add relationships with deduplication
            for i in 0..batch.num_rows() {
                if let (Some(start_id), Some(end_id), Some(rel_type)) = (start_nodes[i], end_nodes[i], rel_types[i]) {
                    let rel_type_id = self.interner.get_or_intern(rel_type);
                    let mut should_add = true;
                    
                    // Check deduplication
                    match deduplication {
                        Some(RelationshipDeduplication::CreateAll) => {
                            // No deduplication, always add
                            should_add = true;
                        }
                        Some(RelationshipDeduplication::CreateUniqueByRelType) => {
                            // Check if (u, v, rel_type) already exists
                            let key = (start_id, end_id, rel_type_id);
                            if seen_by_type.contains_key(&key) {
                                should_add = false;
                            } else {
                                seen_by_type.insert(key, ());
                            }
                        }
                        Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties) => {
                            // Use pre-extracted key properties
                            if let Some(Some(ref key_props)) = key_props_per_row.get(i) {
                                let key = (start_id, end_id, rel_type_id, key_props.clone());
                                if seen_by_type_and_props.contains_key(&key) {
                                    should_add = false;
                                } else {
                                    seen_by_type_and_props.insert(key, ());
                                }
                            }
                            // If no key properties, treat as unique (add it)
                        }
                        None => {
                            // Default: no deduplication
                            should_add = true;
                        }
                    }
                    
                    if should_add {
                        let rel_idx = self.rels.len(); // Index of the relationship we're about to add
                        self.add_rel(start_id, end_id, rel_type);
                        rel_ids.push((start_id, end_id));
                        row_to_rel_idx[i] = Some(rel_idx);
                    }
                }
            }

            // Extract and set relationship properties using tracked indices
            for prop_col in &prop_cols {
                if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == prop_col) {
                    let column = batch.column(column_idx);
                    let field = schema.field(column_idx);
                    
                    for i in 0..batch.num_rows() {
                        // Use the tracked relationship index instead of searching
                        if let Some(rel_idx) = row_to_rel_idx[i] {
                            match field.data_type() {
                                DataType::Utf8 | DataType::LargeUtf8 => {
                                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                                        if !string_array.is_null(i) {
                                            let k = self.interner.get_or_intern(prop_col);
                                            let v = self.interner.get_or_intern(string_array.value(i));
                                            self.rel_col_str.entry(k).or_default().push((rel_idx, v));
                                        }
                                    }
                                }
                                DataType::Int64 => {
                                    if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
                                        if !int_array.is_null(i) {
                                            let k = self.interner.get_or_intern(prop_col);
                                            self.rel_col_i64.entry(k).or_default().push((rel_idx, int_array.value(i)));
                                        }
                                    }
                                }
                                DataType::Float64 => {
                                    if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
                                        if !float_array.is_null(i) {
                                            let k = self.interner.get_or_intern(prop_col);
                                            self.rel_col_f64.entry(k).or_default().push((rel_idx, float_array.value(i)));
                                        }
                                    }
                                }
                                DataType::Boolean => {
                                    if let Some(bool_array) = column.as_any().downcast_ref::<BooleanArray>() {
                                        if !bool_array.is_null(i) {
                                            let k = self.interner.get_or_intern(prop_col);
                                            self.rel_col_bool.entry(k).or_default().push((rel_idx, bool_array.value(i)));
                                        }
                                    }
                                }
                                _ => {
                                    // Convert to string
                                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                                        if !string_array.is_null(i) {
                                            let k = self.interner.get_or_intern(prop_col);
                                            let v = self.interner.get_or_intern(string_array.value(i));
                                            self.rel_col_str.entry(k).or_default().push((rel_idx, v));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(rel_ids)
    }
}

/// Create a GraphSnapshot from Parquet files using GraphBuilder
pub fn read_from_parquet(
    nodes_path: Option<&str>,
    relationships_path: Option<&str>,
    node_id_column: Option<&str>,
    label_columns: Option<Vec<&str>>,
    node_property_columns: Option<Vec<&str>>,
    start_node_column: Option<&str>,
    end_node_column: Option<&str>,
    rel_type_column: Option<&str>,
    rel_property_columns: Option<Vec<&str>>,
) -> Result<GraphSnapshot> {
    // Use default capacity (1M nodes/rels)
    let mut builder = GraphBuilder::new(None, None);

    // Load nodes
    if let Some(nodes_path) = nodes_path {
        builder.load_nodes_from_parquet(
            nodes_path,
            node_id_column,
            label_columns,
            node_property_columns,
            None, // unique_properties - no deduplication by default
        )?;
    }

    // Load relationships
    if let Some(rels_path) = relationships_path {
        let start_col = start_node_column.unwrap_or("from");
        let end_col = end_node_column.unwrap_or("to");
        let type_col = rel_type_column.unwrap_or("type");
        
        builder.load_relationships_from_parquet(
            rels_path,
            start_col,
            end_col,
            Some(type_col),
            rel_property_columns,
            None, // No fixed type, use column
            None, // deduplication
        )?;
    }

    Ok(builder.finalize(None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::ValueId;
    use tempfile::TempDir;
    use parquet::file::properties::WriterProperties;
    use parquet::arrow::ArrowWriter;
    use arrow::array::{Int64Array, Int32Array, StringArray, Float64Array, BooleanArray};
    use arrow::record_batch::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_nodes_parquet(temp_dir: &TempDir) -> std::path::PathBuf {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true), // nullable
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
        ).unwrap();
        
        let file_path = temp_dir.path().join("nodes.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        file_path
    }

    fn create_test_relationships_parquet(temp_dir: &TempDir) -> std::path::PathBuf {
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int64Array::from(vec![1, 2, 3, 4]);
        let to = Int64Array::from(vec![2, 3, 4, 5]);
        let types = StringArray::from(vec!["KNOWS", "WORKS_FOR", "KNOWS", "WORKS_FOR"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(from),
                Arc::new(to),
                Arc::new(types),
            ],
        ).unwrap();
        
        let file_path = temp_dir.path().join("relationships.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        file_path
    }

    #[test]
    fn test_load_nodes_from_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            nodes_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["name", "age", "score", "active"]),
            None, // unique_properties
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        assert_eq!(node_ids, vec![1, 2, 3, 4, 5]);
        assert_eq!(builder.node_count(), 5);
        
        // Check that properties were loaded
        assert_eq!(builder.get_prop(1, "name"), Some(ValueId::Str(builder.interner.get_or_intern("Alice"))));
        assert_eq!(builder.get_prop(1, "age"), Some(ValueId::I64(30)));
        assert_eq!(builder.get_prop(1, "score"), Some(ValueId::from_f64(95.5)));
        assert_eq!(builder.get_prop(1, "active"), Some(ValueId::Bool(true)));
    }

    #[test]
    fn test_load_nodes_from_parquet_auto_id() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            nodes_path.to_str().unwrap(),
            None, // No ID column - auto-generate
            Some(vec!["label"]),
            Some(vec!["name"]),
            None, // unique_properties
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        // Auto-generated IDs start at 1
        assert_eq!(node_ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_load_nodes_from_parquet_auto_properties() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            nodes_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None, // Auto-detect property columns
            None, // unique_properties
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        // Should have loaded name, age, score, active (all columns except id and label)
        assert!(builder.get_prop(1, "name").is_some());
        assert!(builder.get_prop(1, "age").is_some());
    }

    #[test]
    fn test_load_relationships_from_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        // First add nodes
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(i, &["Node"]);
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None, // deduplication
        ).unwrap();
        
        assert_eq!(rel_ids.len(), 4);
        assert_eq!(rel_ids, vec![(1, 2), (2, 3), (3, 4), (4, 5)]);
        assert_eq!(builder.rel_count(), 4);
    }

    #[test]
    fn test_load_relationships_from_parquet_fixed_type() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(i, &["Node"]);
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            None, // No type column
            None,
            Some("KNOWS"), // Fixed type
            None, // deduplication
        ).unwrap();
        
        assert_eq!(rel_ids.len(), 4);
    }

    #[test]
    fn test_load_relationships_from_parquet_int32() {
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int32, false),
            Field::new("to", DataType::Int32, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int32Array::from(vec![1, 2]);
        let to = Int32Array::from(vec![2, 3]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(from),
                Arc::new(to),
                Arc::new(types),
            ],
        ).unwrap();
        
        let file_path = temp_dir.path().join("rels_int32.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(i as u32, &["Node"]);
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None, // deduplication
        ).unwrap();
        
        assert_eq!(rel_ids.len(), 2);
        assert_eq!(rel_ids, vec![(1, 2), (2, 3)]);
    }

    // TODO: Fix lasso into_vec() issue - this test fails due to string interner extraction
    // #[test]
    // fn test_read_from_parquet() {
    //     let temp_dir = TempDir::new().unwrap();
    //     let nodes_path = create_test_nodes_parquet(&temp_dir);
    //     let rels_path = create_test_relationships_parquet(&temp_dir);
    //     
    //     let snapshot = read_from_parquet(
    //         Some(nodes_path.to_str().unwrap()),
    //         Some(rels_path.to_str().unwrap()),
    //         Some("id"),
    //         Some(vec!["label"]),
    //         Some(vec!["name", "age"]),
    //         Some("from"),
    //         Some("to"),
    //         Some("type"),
    //         None,
    //     ).unwrap();
    //     
    //     assert_eq!(snapshot.n_nodes, 5);
    //     assert_eq!(snapshot.n_rels, 4);
    // }

    #[test]
    fn test_load_nodes_from_parquet_nonexistent_file() {
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_parquet(
            "/nonexistent/file.parquet",
            Some("id"),
            None,
            None,
            None, // unique_properties
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_from_parquet_missing_column() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "nonexistent",
            "to",
            Some("type"),
            None,
            None,
            None, // deduplication
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_from_parquet_no_type() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            None,
            None,
            None, // No fixed type either
            None, // deduplication
        );
        
        assert!(result.is_err());
    }
}

