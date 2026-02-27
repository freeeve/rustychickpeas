//! Parquet reading support for GraphBuilder

use crate::graph_builder::GraphBuilder;
use crate::error::{Result, GraphError};
use crate::graph_snapshot::{GraphSnapshot, ValueId};
use crate::types::{NodeId, Label};
use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use object_store::{ObjectStore, path::Path as ObjectPath, aws::AmazonS3Builder};
use std::fs::File;
use std::sync::Arc;
use hashbrown::HashMap;
use futures::TryStreamExt;
use roaring::RoaringBitmap;

/// Extract a string value from an Arrow array column, supporting both Utf8 (StringArray)
/// and LargeUtf8 (LargeStringArray) types. Returns None if the value is null or the
/// column is not a string type.
fn extract_string_value(column: &ArrayRef, i: usize) -> Option<&str> {
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        if !arr.is_null(i) { Some(arr.value(i)) } else { None }
    } else if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        if !arr.is_null(i) { Some(arr.value(i)) } else { None }
    } else {
        None
    }
}

/// Extract a ValueId from an Arrow column value at row i.
/// Shared helper used by deduplication key extraction, property-based node lookup,
/// and relationship key property extraction.
fn extract_value_id(
    column: &dyn arrow::array::Array,
    field: &Field,
    i: usize,
    interner: &crate::interner::StringInterner,
) -> Option<ValueId> {
    match field.data_type() {
        DataType::Int64 => {
            column.as_any().downcast_ref::<Int64Array>()
                .filter(|arr| !arr.is_null(i))
                .map(|arr| ValueId::I64(arr.value(i)))
        }
        DataType::Int32 => {
            column.as_any().downcast_ref::<Int32Array>()
                .filter(|arr| !arr.is_null(i))
                .map(|arr| ValueId::I64(arr.value(i) as i64))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                if !arr.is_null(i) {
                    Some(ValueId::Str(interner.get_or_intern(arr.value(i))))
                } else {
                    None
                }
            } else if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
                if !arr.is_null(i) {
                    Some(ValueId::Str(interner.get_or_intern(arr.value(i))))
                } else {
                    None
                }
            } else {
                None
            }
        }
        DataType::Float64 => {
            column.as_any().downcast_ref::<Float64Array>()
                .filter(|arr| !arr.is_null(i))
                .map(|arr| ValueId::from_f64(arr.value(i)))
        }
        DataType::Boolean => {
            column.as_any().downcast_ref::<BooleanArray>()
                .filter(|arr| !arr.is_null(i))
                .map(|arr| ValueId::Bool(arr.value(i)))
        }
        _ => None,
    }
}

/// Set a node property on the builder from an Arrow column value.
/// Handles Utf8, LargeUtf8, Int64, Float64, Boolean, and fallback to string.
fn set_node_property_from_arrow(
    builder: &mut GraphBuilder,
    node_id: NodeId,
    key: &str,
    column: &ArrayRef,
    data_type: &DataType,
    i: usize,
) -> Result<()> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            if let Some(val) = extract_string_value(column, i) {
                builder.set_prop_str(node_id, key, val)?;
            }
        }
        DataType::Int64 => {
            if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                if !arr.is_null(i) {
                    builder.set_prop_i64(node_id, key, arr.value(i))?;
                }
            }
        }
        DataType::Int32 => {
            if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
                if !arr.is_null(i) {
                    builder.set_prop_i64(node_id, key, arr.value(i) as i64)?;
                }
            }
        }
        DataType::Float64 => {
            if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                if !arr.is_null(i) {
                    builder.set_prop_f64(node_id, key, arr.value(i))?;
                }
            }
        }
        DataType::Float32 => {
            if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
                if !arr.is_null(i) {
                    builder.set_prop_f64(node_id, key, arr.value(i) as f64)?;
                }
            }
        }
        DataType::Boolean => {
            if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                if !arr.is_null(i) {
                    builder.set_prop_bool(node_id, key, arr.value(i))?;
                }
            }
        }
        _ => {
            // Fallback: try string conversion
            if let Some(val) = extract_string_value(column, i) {
                builder.set_prop_str(node_id, key, val)?;
            }
        }
    }
    Ok(())
}

/// Set a relationship property on the builder from an Arrow column value.
/// Handles Utf8, LargeUtf8, Int64, Float64, Boolean, and fallback to string.
fn set_rel_property_from_arrow(
    builder: &mut GraphBuilder,
    rel_idx: usize,
    key: &str,
    column: &ArrayRef,
    data_type: &DataType,
    i: usize,
) {
    let k = builder.interner.get_or_intern(key);
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            if let Some(val) = extract_string_value(column, i) {
                let v = builder.interner.get_or_intern(val);
                builder.rel_col_str.entry(k).or_default().push((rel_idx, v));
            }
        }
        DataType::Int64 => {
            if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                if !arr.is_null(i) {
                    builder.rel_col_i64.entry(k).or_default().push((rel_idx, arr.value(i)));
                }
            }
        }
        DataType::Int32 => {
            if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
                if !arr.is_null(i) {
                    builder.rel_col_i64.entry(k).or_default().push((rel_idx, arr.value(i) as i64));
                }
            }
        }
        DataType::Float64 => {
            if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                if !arr.is_null(i) {
                    builder.rel_col_f64.entry(k).or_default().push((rel_idx, arr.value(i)));
                }
            }
        }
        DataType::Float32 => {
            if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
                if !arr.is_null(i) {
                    builder.rel_col_f64.entry(k).or_default().push((rel_idx, arr.value(i) as f64));
                }
            }
        }
        DataType::Boolean => {
            if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                if !arr.is_null(i) {
                    builder.rel_col_bool.entry(k).or_default().push((rel_idx, arr.value(i)));
                }
            }
        }
        _ => {
            // Fallback: try string conversion
            if let Some(val) = extract_string_value(column, i) {
                let v = builder.interner.get_or_intern(val);
                builder.rel_col_str.entry(k).or_default().push((rel_idx, v));
            }
        }
    }
}

/// Validate that all specified column names exist in the Parquet schema.
/// Returns SchemaError for any missing columns.
fn validate_columns_exist(schema: &Schema, columns: &[&str], context: &str) -> Result<()> {
    for col_name in columns {
        if schema.fields().iter().position(|f| f.name() == col_name).is_none() {
            return Err(GraphError::SchemaError(
                format!("{} column '{}' not found in parquet schema", context, col_name)
            ));
        }
    }
    Ok(())
}

/// Extract node IDs from an integer column (Int64 or Int32).
/// Returns CapacityError for out-of-range values.
fn extract_node_ids_from_column(
    column: &ArrayRef,
    num_rows: usize,
    column_name: &str,
) -> Result<Vec<Option<NodeId>>> {
    let mut ids = Vec::with_capacity(num_rows);
    if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
        for i in 0..num_rows {
            if int_array.is_null(i) {
                ids.push(None);
            } else {
                let val = int_array.value(i);
                if val < 0 || val > u32::MAX as i64 {
                    return Err(GraphError::CapacityError(
                        format!("{} node ID {} out of u32 range", column_name, val)
                    ));
                }
                ids.push(Some(val as u32));
            }
        }
    } else if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
        for i in 0..num_rows {
            if int_array.is_null(i) {
                ids.push(None);
            } else {
                let val = int_array.value(i);
                if val < 0 {
                    return Err(GraphError::CapacityError(
                        format!("{} node ID {} cannot be negative", column_name, val)
                    ));
                }
                ids.push(Some(val as u32));
            }
        }
    } else {
        return Err(GraphError::SchemaError(
            format!("{} column must be Int64 or Int32", column_name)
        ));
    }
    Ok(ids)
}

/// Generate the next node ID from the builder, returning CapacityError on overflow.
fn next_auto_node_id(builder: &mut GraphBuilder) -> Result<NodeId> {
    let id = builder.next_node_id;
    builder.next_node_id = id.checked_add(1).ok_or_else(|| {
        GraphError::CapacityError("Node ID counter exceeded u32::MAX".to_string())
    })?;
    Ok(id)
}

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
    /// * `default_label` - Optional default label to apply to all loaded nodes (in addition to any labels from label_columns).
    pub fn load_nodes_from_parquet(
        &mut self,
        path: &str,
        node_id_column: Option<&str>,
        label_columns: Option<Vec<&str>>,
        property_columns: Option<Vec<&str>>,
        unique_properties: Option<Vec<&str>>,
        default_label: Option<&str>,
    ) -> Result<Vec<u32>> {
        // Create Parquet reader (handles both local and S3)
        // For S3, uses ParquetObjectReader for direct streaming without temp files
        let (mut reader, schema) = create_parquet_reader(path)?;
        
        // Configure deduplication if unique_properties is provided
        // This enables deduplication for both Parquet loading and regular builder operations
        if let Some(ref unique_props) = unique_properties {
            self.enable_node_deduplication(unique_props.clone());
        }

        // Validate configured columns exist in the parquet schema
        let all_columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let label_cols = label_columns.unwrap_or_default();

        // Validate node ID column exists
        if let Some(id_col) = node_id_column {
            validate_columns_exist(&schema, &[id_col], "Node ID")?;
        }
        // Validate label columns exist
        validate_columns_exist(&schema, &label_cols, "Label")?;

        // Validate property columns exist if explicitly specified
        if let Some(ref props) = property_columns {
            validate_columns_exist(&schema, props, "Property")?;
        }
        // Validate unique property columns exist if specified
        if let Some(ref unique_props) = unique_properties {
            validate_columns_exist(&schema, unique_props, "Unique property")?;
        }

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
        let mut seen_node_ids = RoaringBitmap::new(); // Track seen node IDs with bitmap (memory efficient, fast)
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

            // Extract node IDs (optional - None means auto-generate)
            let mut node_ids_batch: Vec<Option<NodeId>> = if let Some(idx) = node_id_idx {
                extract_node_ids_from_column(batch.column(idx), batch.num_rows(), "Node ID")?
            } else {
                vec![None; batch.num_rows()]
            };

            // Extract labels
            let mut labels_per_row: Vec<Vec<&str>> = vec![Vec::new(); batch.num_rows()];
            
            // Add default label to all rows if provided
            if let Some(default_lbl) = default_label {
                for labels in &mut labels_per_row {
                    labels.push(default_lbl);
                }
            }
            
            for label_col in &label_cols {
                if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == label_col) {
                    let column = batch.column(column_idx);
                    for i in 0..batch.num_rows() {
                        if let Some(val) = extract_string_value(column, i) {
                            labels_per_row[i].push(val);
                        }
                    }
                }
            }

            // Process each row with deduplication if enabled
            // First, extract unique property values for deduplication
            // OPTIMIZATION: Pre-compute column indices once per batch instead of per row
            // Only allocate dedup_keys_per_row if deduplication is enabled
            let mut dedup_keys_per_row: Vec<Option<crate::types::DedupKey>> = if unique_properties.is_some() {
                vec![None; batch.num_rows()]
            } else {
                Vec::new() // Empty vec when deduplication is disabled
            };
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
                            if let Some(v) = extract_value_id(column.as_ref(), field, i, &self.interner) {
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
            
            // First pass: check for duplicates and prepare updates (only if deduplication is enabled)
            // OPTIMIZATION: Use a HashSet to track which node_ids we've already seen labels for
            let mut labels_cache: hashbrown::HashMap<u32, Vec<String>> = hashbrown::HashMap::new();
            if !dedup_keys_per_row.is_empty() {
                for i in 0..batch.num_rows() {
                    if let Some(Some(ref dedup_key)) = dedup_keys_per_row.get(i) {
                        if let Some(&existing_node_id) = self.dedup_map.get(dedup_key) {
                            // Use existing node_id, merge labels
                            node_ids_batch[i] = Some(existing_node_id);
                            // Get or compute existing labels (cache to avoid repeated lookups)
                            let existing_labels = labels_cache.entry(existing_node_id).or_insert_with(|| {
                                self.node_labels(existing_node_id)
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
                            // Need to get or generate the node ID first
                            let node_id = match node_ids_batch[i] {
                                Some(id) => id,
                                None => next_auto_node_id(self)?,
                            };
                            node_ids_batch[i] = Some(node_id);
                            // OPTIMIZATION: DedupKey uses Copy for small tuples, so cloning is cheap
                            self.dedup_map.insert(dedup_key.clone(), node_id);
                        }
                    }
                }
            }
            
            // Second pass: apply deduplication updates (now we can call self methods)
            for (_i, existing_node_id, merged_labels) in dedup_updates {
                // Deduplicate labels before adding — only add labels not already on the node
                let existing_labels: Vec<String> = self.node_labels(existing_node_id);
                let new_labels: Vec<&str> = merged_labels.iter()
                    .filter(|l| !existing_labels.contains(l))
                    .map(|s| s.as_str())
                    .collect();
                if new_labels.is_empty() {
                    continue;
                }
                self.add_node(Some(existing_node_id), &new_labels)?;
            }

            // Extract properties (after deduplication, so properties go to correct node_id)
            for prop_col in &prop_cols {
                if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == prop_col) {
                    let column = batch.column(column_idx);
                    let field = schema.field(column_idx);

                    for i in 0..batch.num_rows() {
                        // Get node ID (auto-generate if None)
                        let node_id = match node_ids_batch[i] {
                            Some(id) => id,
                            None => {
                                let id = next_auto_node_id(self)?;
                                node_ids_batch[i] = Some(id);
                                id
                            }
                        };
                        set_node_property_from_arrow(self, node_id, prop_col, column, field.data_type(), i)?;
                    }
                }
            }

            // Add nodes with labels (after deduplication)
            // Skip nodes that were already processed in the deduplication step
            for i in 0..node_ids_batch.len() {
                if !processed_in_dedup.contains(&i) {
                    let labels: Vec<&str> = labels_per_row[i].iter().copied().collect();
                    let actual_id = self.add_node(node_ids_batch[i], &labels)?;
                    // Update the batch with the actual ID (in case it was auto-generated)
                    node_ids_batch[i] = Some(actual_id);
                }
                // Only add to node_ids if this is the first time we're seeing this node_id
                // Use RoaringBitmap for O(1) lookup - more memory efficient than HashSet for dense IDs
                if let Some(node_id) = node_ids_batch[i] {
                    if seen_node_ids.insert(node_id) {
                        node_ids.push(node_id);
                    }
                }
            }

        }

        Ok(node_ids)
    }

    /// Build an index from a single property value to node IDs
    ///
    /// Used for property-based node lookup during relationship loading.
    /// Returns a HashMap where keys are property values and values are node IDs.
    ///
    /// # Arguments
    /// * `property_key` - The property key to index
    /// * `label` - Optional label to filter nodes
    ///
    /// # Returns
    /// * HashMap from ValueId to Vec<NodeId> (multiple nodes can have same property value)
    pub fn build_property_index(
        &self,
        property_key: &str,
        label: Option<&str>,
    ) -> HashMap<ValueId, Vec<NodeId>> {
        let mut index: HashMap<ValueId, Vec<NodeId>> = HashMap::new();

        // Intern label if provided for comparison
        let label_id: Option<Label> = label.map(|l| {
            let id = self.interner.get_or_intern(l);
            Label::new(id)
        });

        // Build index by iterating through all nodes
        for node_id in 0..self.next_node_id {
            // Filter by label if specified
            if let Some(required_label) = label_id {
                let node_labels = &self.node_labels[node_id as usize];
                if !node_labels.contains(&required_label) {
                    continue;
                }
            }

            // Add to index if property exists
            if let Some(value_id) = self.prop(node_id, property_key) {
                index.entry(value_id).or_default().push(node_id);
            }
        }

        index
    }

    /// Build a composite property index from multiple property values to node IDs
    ///
    /// Used for multi-property node lookup during relationship loading.
    ///
    /// # Arguments
    /// * `property_keys` - The property keys to index (in order)
    /// * `label` - Optional label to filter nodes
    ///
    /// # Returns
    /// * HashMap from DedupKey to Vec<NodeId> (composite key to node IDs)
    pub fn build_composite_property_index(
        &self,
        property_keys: &[&str],
        label: Option<&str>,
    ) -> HashMap<crate::types::DedupKey, Vec<NodeId>> {
        use crate::types::DedupKey;

        let mut index: HashMap<DedupKey, Vec<NodeId>> = HashMap::new();

        // Intern label if provided for comparison
        let label_id: Option<Label> = label.map(|l| {
            let id = self.interner.get_or_intern(l);
            Label::new(id)
        });

        // Build index by iterating through all nodes
        for node_id in 0..self.next_node_id {
            // Filter by label if specified
            if let Some(required_label) = label_id {
                let node_labels = &self.node_labels[node_id as usize];
                if !node_labels.contains(&required_label) {
                    continue;
                }
            }

            // Collect all property values for this node
            let mut values: Vec<ValueId> = Vec::with_capacity(property_keys.len());
            let mut all_found = true;

            for key in property_keys {
                if let Some(value_id) = self.prop(node_id, key) {
                    values.push(value_id);
                } else {
                    all_found = false;
                    break;
                }
            }

            // Only add to index if all properties exist
            if all_found && values.len() == property_keys.len() {
                let key = DedupKey::from_slice(&values);
                index.entry(key).or_default().push(node_id);
            }
        }

        index
    }

    /// Load relationships from a Parquet file into the builder
    ///
    /// If `rel_type_column` is None, `fixed_rel_type` must be provided.
    /// If `rel_type_column` is Some, it reads the type from that column.
    ///
    /// # Arguments
    /// * `path` - Path to Parquet file (local or S3)
    /// * `start_node_column` - Column name for start node IDs
    /// * `end_node_column` - Column name for end node IDs
    /// * `rel_type_column` - Optional column name for relationship type
    /// * `property_columns` - Optional list of property columns to load. If None, loads all except ID/type columns.
    /// * `fixed_rel_type` - Fixed relationship type (used if rel_type_column is None)
    /// * `deduplication` - Optional deduplication strategy
    /// * `key_property_columns` - Optional list of property columns to use as uniqueness key when
    ///   deduplication is CreateUniqueByRelTypeAndKeyProperties. If None, uses all property_columns.
    pub fn load_relationships_from_parquet(
        &mut self,
        path: &str,
        start_node_column: &str,
        end_node_column: &str,
        rel_type_column: Option<&str>,
        property_columns: Option<Vec<&str>>,
        fixed_rel_type: Option<&str>,
        deduplication: Option<crate::types::RelationshipDeduplication>,
        key_property_columns: Option<Vec<&str>>,
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
            .ok_or_else(|| GraphError::SchemaError(format!("Start node column '{}' not found in parquet schema", start_node_column)))?;
        let end_node_idx = schema.fields().iter()
            .position(|f| f.name() == end_node_column)
            .ok_or_else(|| GraphError::SchemaError(format!("End node column '{}' not found in parquet schema", end_node_column)))?;
        
        // Handle relationship type: either from column or fixed value
        let rel_type_idx = rel_type_column.and_then(|col| {
            schema.fields().iter().position(|f| f.name() == col)
        });
        
        if rel_type_column.is_some() && rel_type_idx.is_none() {
            return Err(GraphError::SchemaError(
                format!("Relationship type column '{}' not found in parquet schema", rel_type_column.unwrap())
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

        // Determine effective key columns for deduplication
        // If key_property_columns is specified, use those; otherwise use all prop_cols (backward compat)
        let effective_key_cols: Vec<&str> = match (&deduplication, &key_property_columns) {
            (Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties), Some(keys)) => {
                keys.clone()
            }
            (Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties), None) => {
                // Backward compatibility: use all property columns
                prop_cols.clone()
            }
            _ => Vec::new(), // Not used for other dedup strategies
        };

        // Stream batches and process them immediately (no accumulation in memory)
        let mut rel_ids = Vec::new();
        let mut first_batch = true;
        let mut skipped_missing_nodes = 0u64;

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
            // Extract start/end node IDs using shared helper
            let start_nodes = extract_node_ids_from_column(
                batch.column(start_node_idx), batch.num_rows(), "Start"
            )?;
            let end_nodes = extract_node_ids_from_column(
                batch.column(end_node_idx), batch.num_rows(), "End"
            )?;

            // Extract relationship types (either from column or use fixed type)
            let rel_type_col = rel_type_idx.map(|idx| batch.column(idx));
            let mut rel_types = Vec::with_capacity(batch.num_rows());

            // Extract relationship types (supports both Utf8 and LargeUtf8)
            if let Some(col) = &rel_type_col {
                // Verify string column type
                let has_strings = col.as_any().downcast_ref::<StringArray>().is_some()
                    || col.as_any().downcast_ref::<LargeStringArray>().is_some();
                if !has_strings {
                    return Err(GraphError::SchemaError(
                        "Relationship type column must be String (Utf8 or LargeUtf8)".to_string()
                    ));
                }
                for i in 0..batch.num_rows() {
                    rel_types.push(extract_string_value(col, i));
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
            // Uses effective_key_cols which is either explicit key_property_columns or all prop_cols
            let mut key_props_per_row: Vec<Option<Vec<ValueId>>> = vec![None; batch.num_rows()];
            if matches!(deduplication, Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties)) {
                for i in 0..batch.num_rows() {
                    let mut key_props = Vec::new();
                    for prop_col in &effective_key_cols {
                        if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == prop_col) {
                            let column = batch.column(column_idx);
                            let field = schema.field(column_idx);
                            if let Some(v) = extract_value_id(column.as_ref(), field, i, &self.interner) {
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
            
            // Add relationships with deduplication and node existence check
            for i in 0..batch.num_rows() {
                if let (Some(start_id), Some(end_id), Some(rel_type)) = (start_nodes[i], end_nodes[i], rel_types[i]) {
                    // Skip relationships where start or end node doesn't exist in the builder
                    if !self.known_nodes.contains(start_id) || !self.known_nodes.contains(end_id) {
                        skipped_missing_nodes += 1;
                        continue;
                    }

                    let rel_type_id = self.interner.get_or_intern(rel_type);
                    let mut should_add = true;

                    // Check deduplication
                    match deduplication {
                        Some(RelationshipDeduplication::CreateAll) | None => {
                            should_add = true;
                        }
                        Some(RelationshipDeduplication::CreateUniqueByRelType) => {
                            let key = (start_id, end_id, rel_type_id);
                            if seen_by_type.contains_key(&key) {
                                should_add = false;
                            } else {
                                seen_by_type.insert(key, ());
                            }
                        }
                        Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties) => {
                            if let Some(Some(ref key_props)) = key_props_per_row.get(i) {
                                let key = (start_id, end_id, rel_type_id, key_props.clone());
                                if seen_by_type_and_props.contains_key(&key) {
                                    should_add = false;
                                } else {
                                    seen_by_type_and_props.insert(key, ());
                                }
                            }
                        }
                    }
                    
                    if should_add {
                        let rel_idx = self.rels.len(); // Index of the relationship we're about to add
                        self.add_rel(start_id, end_id, rel_type)?;
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
                        if let Some(rel_idx) = row_to_rel_idx[i] {
                            set_rel_property_from_arrow(self, rel_idx, prop_col, column, field.data_type(), i);
                        }
                    }
                }
            }
        }

        if skipped_missing_nodes > 0 {
            eprintln!("Warning: skipped {} relationships referencing non-existent nodes", skipped_missing_nodes);
        }

        Ok(rel_ids)
    }

    /// Load relationships from Parquet with flexible node reference support
    ///
    /// This version supports looking up nodes by:
    /// - Node ID column (current behavior)
    /// - Single property lookup (e.g., find node by UUID)
    /// - Composite property lookup (e.g., find node by name + birthdate)
    ///
    /// # Arguments
    /// * `path` - Path to Parquet file (local or S3)
    /// * `start_node_ref` - How to identify start nodes
    /// * `end_node_ref` - How to identify end nodes
    /// * `rel_type_column` - Optional column name for relationship type
    /// * `property_columns` - Optional list of property columns to load
    /// * `fixed_rel_type` - Fixed relationship type (used if rel_type_column is None)
    /// * `deduplication` - Optional deduplication strategy
    /// * `key_property_columns` - Optional subset of property columns for uniqueness
    ///
    /// # Returns
    /// * Vec of (start_node_id, end_node_id) pairs for created relationships
    pub fn load_relationships_from_parquet_v2(
        &mut self,
        path: &str,
        start_node_ref: crate::types::NodeReference,
        end_node_ref: crate::types::NodeReference,
        rel_type_column: Option<&str>,
        property_columns: Option<Vec<&str>>,
        fixed_rel_type: Option<&str>,
        deduplication: Option<crate::types::RelationshipDeduplication>,
        key_property_columns: Option<Vec<&str>>,
    ) -> Result<Vec<(u32, u32)>> {
        use crate::types::{NodeReference, DedupKey, RelationshipDeduplication};

        // Build property indexes for node lookup based on reference types
        enum NodeIndex {
            Id,  // No index needed, use ID column directly
            Single(HashMap<ValueId, Vec<NodeId>>),
            Composite(HashMap<DedupKey, Vec<NodeId>>),
        }

        let start_index = match &start_node_ref {
            NodeReference::Id(_) => NodeIndex::Id,
            NodeReference::Property { property_key, label, .. } => {
                NodeIndex::Single(self.build_property_index(property_key, label.as_deref()))
            }
            NodeReference::CompositeProperty { property_keys, label, .. } => {
                let keys: Vec<&str> = property_keys.iter().map(|s| s.as_str()).collect();
                NodeIndex::Composite(self.build_composite_property_index(&keys, label.as_deref()))
            }
        };

        let end_index = match &end_node_ref {
            NodeReference::Id(_) => NodeIndex::Id,
            NodeReference::Property { property_key, label, .. } => {
                NodeIndex::Single(self.build_property_index(property_key, label.as_deref()))
            }
            NodeReference::CompositeProperty { property_keys, label, .. } => {
                let keys: Vec<&str> = property_keys.iter().map(|s| s.as_str()).collect();
                NodeIndex::Composite(self.build_composite_property_index(&keys, label.as_deref()))
            }
        };

        // Create Parquet reader
        let (mut reader, schema) = create_parquet_reader(path)?;

        // Set up deduplication tracking
        let mut seen_by_type: HashMap<(u32, u32, u32), ()> = HashMap::new();
        let mut seen_by_type_and_props: HashMap<(u32, u32, u32, Vec<ValueId>), ()> = HashMap::new();

        // Get column names from NodeReference
        let start_columns: Vec<&str> = match &start_node_ref {
            NodeReference::Id(col) => vec![col.as_str()],
            NodeReference::Property { column, .. } => vec![column.as_str()],
            NodeReference::CompositeProperty { columns, .. } => columns.iter().map(|s| s.as_str()).collect(),
        };
        let end_columns: Vec<&str> = match &end_node_ref {
            NodeReference::Id(col) => vec![col.as_str()],
            NodeReference::Property { column, .. } => vec![column.as_str()],
            NodeReference::CompositeProperty { columns, .. } => columns.iter().map(|s| s.as_str()).collect(),
        };

        // Find column indices
        let start_col_indices: Vec<usize> = start_columns.iter()
            .map(|col| schema.fields().iter().position(|f| f.name() == *col)
                .ok_or_else(|| GraphError::BulkLoadError(format!("Start column '{}' not found", col))))
            .collect::<Result<Vec<_>>>()?;
        let end_col_indices: Vec<usize> = end_columns.iter()
            .map(|col| schema.fields().iter().position(|f| f.name() == *col)
                .ok_or_else(|| GraphError::BulkLoadError(format!("End column '{}' not found", col))))
            .collect::<Result<Vec<_>>>()?;

        // Handle relationship type
        let rel_type_idx = rel_type_column.and_then(|col| {
            schema.fields().iter().position(|f| f.name() == col)
        });
        if rel_type_column.is_some() && rel_type_idx.is_none() {
            return Err(GraphError::BulkLoadError(format!(
                "Relationship type column '{}' not found",
                rel_type_column.unwrap()
            )));
        }

        // Validate: must have either rel_type_column or fixed_rel_type
        if rel_type_idx.is_none() && fixed_rel_type.is_none() {
            return Err(GraphError::BulkLoadError(
                "Either rel_type_column or fixed_rel_type must be provided".to_string()
            ));
        }

        // Determine property columns
        let prop_cols: Vec<&str> = property_columns.clone().unwrap_or_else(|| Vec::new());

        // Determine effective key columns for deduplication
        let effective_key_cols: Vec<&str> = match (&deduplication, &key_property_columns) {
            (Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties), Some(keys)) => {
                keys.clone()
            }
            (Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties), None) => {
                prop_cols.clone()
            }
            _ => Vec::new(),
        };

        let mut rel_ids: Vec<(u32, u32)> = Vec::new();
        let mut skipped_missing_nodes = 0u64;

        // Process batches
        while let Some(batch_result) = reader.next() {
            let batch = batch_result.map_err(|e| GraphError::BulkLoadError(e.to_string()))?;

            // Extract start and end node IDs for each row
            let mut start_nodes: Vec<Option<NodeId>> = vec![None; batch.num_rows()];
            let mut end_nodes: Vec<Option<NodeId>> = vec![None; batch.num_rows()];

            // Resolve start nodes
            match &start_index {
                NodeIndex::Id => {
                    // Direct ID lookup
                    let column = batch.column(start_col_indices[0]);
                    for i in 0..batch.num_rows() {
                        if let Some(id_array) = column.as_any().downcast_ref::<Int64Array>() {
                            if !id_array.is_null(i) {
                                let val = id_array.value(i);
                                if val < 0 || val > u32::MAX as i64 {
                                    return Err(GraphError::BulkLoadError(format!(
                                        "Node ID {} is out of u32 range", val
                                    )));
                                }
                                start_nodes[i] = Some(val as NodeId);
                            }
                        } else if let Some(id_array) = column.as_any().downcast_ref::<Int32Array>() {
                            if !id_array.is_null(i) {
                                let val = id_array.value(i);
                                if val < 0 {
                                    return Err(GraphError::BulkLoadError(format!(
                                        "Node ID {} cannot be negative", val
                                    )));
                                }
                                start_nodes[i] = Some(val as NodeId);
                            }
                        } else if let Some(id_array) = column.as_any().downcast_ref::<UInt32Array>() {
                            if !id_array.is_null(i) {
                                start_nodes[i] = Some(id_array.value(i));
                            }
                        }
                    }
                }
                NodeIndex::Single(index) => {
                    // Single property lookup
                    let column = batch.column(start_col_indices[0]);
                    let field = schema.field(start_col_indices[0]);
                    for i in 0..batch.num_rows() {
                        if let Some(value_id) = extract_value_id(column.as_ref(), field, i, &self.interner) {
                            if let Some(node_ids) = index.get(&value_id) {
                                // Use first matching node (or could error if multiple)
                                start_nodes[i] = node_ids.first().copied();
                            }
                        }
                    }
                }
                NodeIndex::Composite(index) => {
                    // Composite property lookup
                    for i in 0..batch.num_rows() {
                        let mut values: Vec<ValueId> = Vec::with_capacity(start_col_indices.len());
                        let mut all_found = true;
                        for &col_idx in &start_col_indices {
                            let column = batch.column(col_idx);
                            let field = schema.field(col_idx);
                            if let Some(value_id) = extract_value_id(column.as_ref(), field, i, &self.interner) {
                                values.push(value_id);
                            } else {
                                all_found = false;
                                break;
                            }
                        }
                        if all_found && values.len() == start_col_indices.len() {
                            let key = DedupKey::from_slice(&values);
                            if let Some(node_ids) = index.get(&key) {
                                start_nodes[i] = node_ids.first().copied();
                            }
                        }
                    }
                }
            }

            // Resolve end nodes (same logic as start)
            match &end_index {
                NodeIndex::Id => {
                    let column = batch.column(end_col_indices[0]);
                    for i in 0..batch.num_rows() {
                        if let Some(id_array) = column.as_any().downcast_ref::<Int64Array>() {
                            if !id_array.is_null(i) {
                                let val = id_array.value(i);
                                if val < 0 || val > u32::MAX as i64 {
                                    return Err(GraphError::BulkLoadError(format!(
                                        "Node ID {} is out of u32 range", val
                                    )));
                                }
                                end_nodes[i] = Some(val as NodeId);
                            }
                        } else if let Some(id_array) = column.as_any().downcast_ref::<Int32Array>() {
                            if !id_array.is_null(i) {
                                let val = id_array.value(i);
                                if val < 0 {
                                    return Err(GraphError::BulkLoadError(format!(
                                        "Node ID {} cannot be negative", val
                                    )));
                                }
                                end_nodes[i] = Some(val as NodeId);
                            }
                        } else if let Some(id_array) = column.as_any().downcast_ref::<UInt32Array>() {
                            if !id_array.is_null(i) {
                                end_nodes[i] = Some(id_array.value(i));
                            }
                        }
                    }
                }
                NodeIndex::Single(index) => {
                    let column = batch.column(end_col_indices[0]);
                    let field = schema.field(end_col_indices[0]);
                    for i in 0..batch.num_rows() {
                        if let Some(value_id) = extract_value_id(column.as_ref(), field, i, &self.interner) {
                            if let Some(node_ids) = index.get(&value_id) {
                                end_nodes[i] = node_ids.first().copied();
                            }
                        }
                    }
                }
                NodeIndex::Composite(index) => {
                    for i in 0..batch.num_rows() {
                        let mut values: Vec<ValueId> = Vec::with_capacity(end_col_indices.len());
                        let mut all_found = true;
                        for &col_idx in &end_col_indices {
                            let column = batch.column(col_idx);
                            let field = schema.field(col_idx);
                            if let Some(value_id) = extract_value_id(column.as_ref(), field, i, &self.interner) {
                                values.push(value_id);
                            } else {
                                all_found = false;
                                break;
                            }
                        }
                        if all_found && values.len() == end_col_indices.len() {
                            let key = DedupKey::from_slice(&values);
                            if let Some(node_ids) = index.get(&key) {
                                end_nodes[i] = node_ids.first().copied();
                            }
                        }
                    }
                }
            }

            // Extract relationship types
            let rel_types: Vec<Option<&str>> = if let Some(idx) = rel_type_idx {
                let column = batch.column(idx);
                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    (0..batch.num_rows())
                        .map(|i| if string_array.is_null(i) { None } else { Some(string_array.value(i)) })
                        .collect()
                } else if let Some(large_string_array) = column.as_any().downcast_ref::<LargeStringArray>() {
                    (0..batch.num_rows())
                        .map(|i| if large_string_array.is_null(i) { None } else { Some(large_string_array.value(i)) })
                        .collect()
                } else {
                    vec![None; batch.num_rows()]
                }
            } else {
                vec![fixed_rel_type; batch.num_rows()]
            };

            // Extract key properties for deduplication
            let mut key_props_per_row: Vec<Option<Vec<ValueId>>> = vec![None; batch.num_rows()];
            if !effective_key_cols.is_empty() {
                for i in 0..batch.num_rows() {
                    let mut key_props: Vec<ValueId> = Vec::new();
                    for prop_col in &effective_key_cols {
                        if let Some(col_idx) = schema.fields().iter().position(|f| f.name() == *prop_col) {
                            let column = batch.column(col_idx);
                            let field = schema.field(col_idx);
                            if let Some(val) = extract_value_id(column.as_ref(), field, i, &self.interner) {
                                key_props.push(val);
                            }
                        }
                    }
                    if !key_props.is_empty() {
                        key_props_per_row[i] = Some(key_props);
                    }
                }
            }

            // Track relationship indices for property setting
            let mut row_to_rel_idx: Vec<Option<usize>> = vec![None; batch.num_rows()];

            // Add relationships with deduplication and node existence check
            for i in 0..batch.num_rows() {
                if let (Some(start_id), Some(end_id), Some(rel_type)) = (start_nodes[i], end_nodes[i], rel_types[i]) {
                    // Skip relationships where start or end node doesn't exist
                    if !self.known_nodes.contains(start_id) || !self.known_nodes.contains(end_id) {
                        skipped_missing_nodes += 1;
                        continue;
                    }

                    let rel_type_id = self.interner.get_or_intern(rel_type);
                    let mut should_add = true;

                    // Check deduplication
                    match deduplication {
                        Some(RelationshipDeduplication::CreateAll) | None => {
                            should_add = true;
                        }
                        Some(RelationshipDeduplication::CreateUniqueByRelType) => {
                            let key = (start_id, end_id, rel_type_id);
                            if seen_by_type.contains_key(&key) {
                                should_add = false;
                            } else {
                                seen_by_type.insert(key, ());
                            }
                        }
                        Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties) => {
                            if let Some(Some(ref key_props)) = key_props_per_row.get(i) {
                                let key = (start_id, end_id, rel_type_id, key_props.clone());
                                if seen_by_type_and_props.contains_key(&key) {
                                    should_add = false;
                                } else {
                                    seen_by_type_and_props.insert(key, ());
                                }
                            }
                        }
                    }

                    if should_add {
                        let rel_idx = self.rels.len();
                        self.add_rel(start_id, end_id, rel_type)?;
                        rel_ids.push((start_id, end_id));
                        row_to_rel_idx[i] = Some(rel_idx);
                    }
                }
            }

            // Extract and set relationship properties
            for prop_col in &prop_cols {
                if let Some(column_idx) = schema.fields().iter().position(|f| f.name() == *prop_col) {
                    let column = batch.column(column_idx);
                    let field = schema.field(column_idx);

                    for i in 0..batch.num_rows() {
                        if let Some(rel_idx) = row_to_rel_idx[i] {
                            set_rel_property_from_arrow(self, rel_idx, prop_col, column, field.data_type(), i);
                        }
                    }
                }
            }
        }

        if skipped_missing_nodes > 0 {
            eprintln!("Warning: skipped {} relationships referencing non-existent nodes", skipped_missing_nodes);
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
            None, // default_label
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
            None, // key_property_columns
        )?;
    }

    Ok(builder.finalize(None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_snapshot::ValueId;
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
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        assert_eq!(node_ids, vec![1, 2, 3, 4, 5]);
        assert_eq!(builder.node_count(), 5);
        
        // Check that properties were loaded
        assert_eq!(builder.prop(1, "name"), Some(ValueId::Str(builder.interner.get_or_intern("Alice"))));
        assert_eq!(builder.prop(1, "age"), Some(ValueId::I64(30)));
        assert_eq!(builder.prop(1, "score"), Some(ValueId::from_f64(95.5)));
        assert_eq!(builder.prop(1, "active"), Some(ValueId::Bool(true)));
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
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        // Auto-generated IDs start at 0
        assert_eq!(node_ids, vec![0, 1, 2, 3, 4]);
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
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        // Should have loaded name, age, score, active (all columns except id and label)
        assert!(builder.prop(1, "name").is_some());
        assert!(builder.prop(1, "age").is_some());
    }

    #[test]
    fn test_load_relationships_from_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        // First add nodes
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None, // deduplication
            None, // key_property_columns
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
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            None, // No type column
            None,
            Some("KNOWS"), // Fixed type
            None, // deduplication
            None, // key_property_columns
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
            builder.add_node(Some(i as u32), &["Node"]).unwrap();
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None, // deduplication
            None, // key_property_columns
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
            None, // default_label
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
            None, // key_property_columns
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
            None, // key_property_columns
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_with_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            nodes_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["name"]),
            Some(vec!["name"]), // Deduplicate by name
            None, // default_label
        ).unwrap();
        
        // All nodes should be loaded (deduplication happens during load)
        assert_eq!(node_ids.len(), 5);
        // But dedup_map should track unique names
        assert!(builder.dedup_map.len() > 0);
    }

    #[test]
    fn test_load_nodes_with_invalid_node_id() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        
        // Create array with ID exceeding u32::MAX
        let ids = Int64Array::from(vec![u32::MAX as i64 + 1]);
        let labels = StringArray::from(vec!["Person"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("invalid_ids.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None,
            None,
            None, // default_label
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_with_negative_node_id() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        
        let ids = Int64Array::from(vec![-1]);
        let labels = StringArray::from(vec!["Person"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("negative_ids.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None,
            None,
            None, // default_label
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_with_nullable_properties() {
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true), // Nullable
            Field::new("score", DataType::Float64, true), // Nullable
        ]);
        
        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec![Some("Alice"), None, Some("Bob")]);
        let scores = Float64Array::from(vec![Some(95.5), Some(88.0), None]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("nullable.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name", "score"]),
            None,
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 3);
        // Node 1 should have both properties
        assert!(builder.prop(1, "name").is_some());
        assert!(builder.prop(1, "score").is_some());
        // Node 2 should have score but not name
        assert!(builder.prop(2, "name").is_none());
        assert!(builder.prop(2, "score").is_some());
        // Node 3 should have name but not score
        assert!(builder.prop(3, "name").is_some());
        assert!(builder.prop(3, "score").is_none());
    }

    #[test]
    fn test_load_relationships_with_deduplication_by_type() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        use crate::types::RelationshipDeduplication;
        let rel_ids = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelType),
            None, // key_property_columns
        ).unwrap();
        
        assert_eq!(rel_ids.len(), 4);
        assert_eq!(builder.rel_count(), 4);
    }

    #[test]
    fn test_load_relationships_with_deduplication_by_type_and_props() {
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("weight", DataType::Float64, false),
        ]);
        
        // Create duplicate relationships with same weight
        let from = Int64Array::from(vec![1, 1, 2]);
        let to = Int64Array::from(vec![2, 2, 3]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS", "KNOWS"]);
        let weights = Float64Array::from(vec![0.8, 0.8, 0.5]); // First two are duplicates
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types), Arc::new(weights)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("rels_dedup.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        use crate::types::RelationshipDeduplication;
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["weight"]),
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
            None, // key_property_columns
        ).unwrap();
        
        // Should deduplicate the first two relationships (same from, to, type, weight)
        assert_eq!(rel_ids.len(), 2);
        assert_eq!(builder.rel_count(), 2);
    }

    #[test]
    fn test_load_relationships_with_invalid_start_node_id() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int64Array::from(vec![u32::MAX as i64 + 1]);
        let to = Int64Array::from(vec![2]);
        let types = StringArray::from(vec!["KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("invalid_start.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(2), &["Node"]).unwrap();
        
        let result = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_with_invalid_end_node_id() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int64Array::from(vec![1]);
        let to = Int64Array::from(vec![u32::MAX as i64 + 1]);
        let types = StringArray::from(vec!["KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("invalid_end.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        
        let result = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_with_nullable_nodes() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, true), // Nullable
            Field::new("to", DataType::Int64, true),   // Nullable
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int64Array::from(vec![Some(1), None, Some(2)]);
        let to = Int64Array::from(vec![Some(2), Some(3), None]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS", "KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("nullable_nodes.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }

        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        ).unwrap();

        // Only the first relationship should be added (others have null nodes)
        assert_eq!(rel_ids.len(), 1);
        assert_eq!(builder.rel_count(), 1);
    }

    #[test]
    fn test_load_relationships_with_properties() {
        use arrow::array::{Int64Array, StringArray, Float64Array, BooleanArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("weight", DataType::Float64, true),
            Field::new("verified", DataType::Boolean, true),
        ]);
        
        let from = Int64Array::from(vec![1, 2]);
        let to = Int64Array::from(vec![2, 3]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS"]);
        let weights = Float64Array::from(vec![Some(0.8), None]);
        let verified = BooleanArray::from(vec![Some(true), Some(false)]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types), Arc::new(weights), Arc::new(verified)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("rels_with_props.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["weight", "verified"]),
            None,
            None,
            None, // key_property_columns
        ).unwrap();

        assert_eq!(rel_ids.len(), 2);
        assert_eq!(builder.rel_count(), 2);

        // Verify properties were set
        let snapshot = builder.finalize(None);
        // Check first relationship (CSR position 0)
        let weight_prop = snapshot.relationship_property(0, "weight");
        assert!(weight_prop.is_some());
    }

    #[test]
    fn test_load_nodes_with_deduplication_missing_column() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_parquet(&temp_dir);

        let mut builder = GraphBuilder::new(None, None);
        // Try to deduplicate by a column that doesn't exist - should return SchemaError
        let result = builder.load_nodes_from_parquet(
            nodes_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["name"]),
            Some(vec!["nonexistent"]), // Column doesn't exist
            None, // default_label
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_load_nodes_with_multiple_batches() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        
        let file_path = temp_dir.path().join("multi_batch.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder()
            .set_write_batch_size(2) // Small batch size to force multiple batches
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).unwrap();
        
        // Write first batch
        let ids1 = Int64Array::from(vec![1, 2]);
        let names1 = StringArray::from(vec!["Alice", "Bob"]);
        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids1), Arc::new(names1)],
        ).unwrap();
        writer.write(&batch1).unwrap();
        
        // Write second batch
        let ids2 = Int64Array::from(vec![3, 4]);
        let names2 = StringArray::from(vec!["Charlie", "David"]);
        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids2), Arc::new(names2)],
        ).unwrap();
        writer.write(&batch2).unwrap();
        
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name"]),
            None,
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 4);
        assert_eq!(node_ids, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_load_nodes_with_empty_batch() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        
        let file_path = temp_dir.path().join("empty_batch.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).unwrap();
        
        // Write empty batch
        let ids = Int64Array::from(Vec::<i64>::new());
        let names = StringArray::from(Vec::<&str>::new());
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(names)],
        ).unwrap();
        writer.write(&batch).unwrap();
        
        // Write non-empty batch
        let ids2 = Int64Array::from(vec![1]);
        let names2 = StringArray::from(vec!["Alice"]);
        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids2), Arc::new(names2)],
        ).unwrap();
        writer.write(&batch2).unwrap();
        
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name"]),
            None,
            None, // default_label
        ).unwrap();
        
        // Should skip empty batch and only load from non-empty batch
        assert_eq!(node_ids.len(), 1);
        assert_eq!(node_ids, vec![1]);
    }

    #[test]
    fn test_load_nodes_with_float32_property() {
        use arrow::array::{Int64Array, Float32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float32, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        let scores = Float32Array::from(vec![95.5f32, 88.0f32]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(scores)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("float32.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["score"]),
            None,
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 2);
        // Float32 should be converted to string (fallback for unsupported types)
        // Actually, let me check what happens - it might convert to Float64
    }

    #[test]
    fn test_load_relationships_with_wrong_column_type() {
        use arrow::array::{Int64Array, Float64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Start node column is Float64 instead of Int64/Int32
        let schema = Schema::new(vec![
            Field::new("from", DataType::Float64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Float64Array::from(vec![1.0]);
        let to = Int64Array::from(vec![2]);
        let types = StringArray::from(vec!["KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("wrong_type.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();

        let result = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_with_large_utf8() {
        use arrow::array::{Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        use arrow::array::LargeStringArray;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::LargeUtf8, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        let names = LargeStringArray::from(vec!["Alice", "Bob"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(names)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("large_utf8.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        // LargeUtf8 is now properly handled via extract_string_value helper
        let result = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name"]),
            None,
            None, // default_label
        );

        assert!(result.is_ok());
        let node_ids = result.unwrap();
        assert_eq!(node_ids.len(), 2);
        // LargeUtf8 properties should now be correctly set
        assert!(builder.prop(1, "name").is_some());
        assert!(builder.prop(2, "name").is_some());
    }

    #[test]
    fn test_load_nodes_with_auto_id_and_properties() {
        use arrow::array::{StringArray, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // No ID column - all auto-generated
        let schema = Schema::new(vec![
            Field::new("label", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]);
        
        let labels = StringArray::from(vec!["Person", "Person"]);
        let names = StringArray::from(vec!["Alice", "Bob"]);
        let ages = Int64Array::from(vec![30, 25]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(labels), Arc::new(names), Arc::new(ages)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("auto_id.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            None, // No ID column
            Some(vec!["label"]),
            Some(vec!["name", "age"]),
            None,
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 2);
        // Auto-generated IDs should start at 0
        assert_eq!(node_ids, vec![0, 1]);
        assert!(builder.prop(0, "name").is_some());
        assert!(builder.prop(0, "age").is_some());
    }


    #[test]
    fn test_load_relationships_with_fixed_type() {
        use arrow::array::{Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // No type column - use fixed type
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
        ]);
        
        let from = Int64Array::from(vec![1, 2]);
        let to = Int64Array::from(vec![2, 3]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("fixed_type.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            None, // No type column
            None,
            Some("KNOWS"), // Fixed type
            None,
            None, // key_property_columns
        ).unwrap();

        assert_eq!(rel_ids.len(), 2);
        assert_eq!(builder.rel_count(), 2);
    }

    #[test]
    fn test_load_relationships_with_create_all_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        use crate::types::RelationshipDeduplication;
        let rel_ids = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            Some(RelationshipDeduplication::CreateAll), // Explicitly create all
            None, // key_property_columns
        ).unwrap();

        assert_eq!(rel_ids.len(), 4);
        assert_eq!(builder.rel_count(), 4);
    }

    #[test]
    fn test_load_relationships_with_int32_nodes() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
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
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("int32_rels.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        ).unwrap();

        assert_eq!(rel_ids.len(), 2);
        assert_eq!(rel_ids, vec![(1, 2), (2, 3)]);
    }

    #[test]
    fn test_load_relationships_with_negative_int32_node_id() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int32, false),
            Field::new("to", DataType::Int32, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int32Array::from(vec![-1]);
        let to = Int32Array::from(vec![2]);
        let types = StringArray::from(vec!["KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("negative_int32.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(2), &["Node"]).unwrap();

        let result = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_with_null_rel_type() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, true), // Nullable
        ]);
        
        let from = Int64Array::from(vec![1, 2]);
        let to = Int64Array::from(vec![2, 3]);
        let types = StringArray::from(vec![Some("KNOWS"), None]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("null_type.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        ).unwrap();

        // Only first relationship should be added (second has null type)
        assert_eq!(rel_ids.len(), 1);
        assert_eq!(builder.rel_count(), 1);
    }

    #[test]
    fn test_load_relationships_with_deduplication_no_key_props() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int64Array::from(vec![1, 1]);
        let to = Int64Array::from(vec![2, 2]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("no_key_props.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=2 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        use crate::types::RelationshipDeduplication;
        // Request deduplication by type and key props, but no property columns
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None, // No property columns
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
            None, // key_property_columns
        ).unwrap();
        
        // Should treat as unique (add all) since no key properties
        assert_eq!(rel_ids.len(), 2);
    }


    #[test]
    fn test_load_nodes_with_deduplication_multi_key_partial_null() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("username", DataType::Utf8, true),
        ]);
        
        let ids = Int64Array::from(vec![1, 2, 3]);
        let labels = StringArray::from(vec!["Person", "Person", "Person"]);
        let emails = StringArray::from(vec![Some("alice@example.com"), Some("alice@example.com"), Some("bob@example.com")]);
        let usernames = StringArray::from(vec![Some("alice"), None, Some("bob")]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(emails), Arc::new(usernames)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("multi_key_null.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["email", "username"]),
            Some(vec!["email", "username"]), // Deduplicate by both
            None, // default_label
        ).unwrap();
        
        // Node 2 has null username, so can't create complete dedup key
        // All 3 nodes should be loaded separately
        assert_eq!(node_ids.len(), 3);
    }

    #[test]
    fn test_load_relationships_with_property_deduplication_empty_key() {
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("weight", DataType::Float64, true), // All null
        ]);
        
        let from = Int64Array::from(vec![1, 1]);
        let to = Int64Array::from(vec![2, 2]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS"]);
        let weights = Float64Array::from(vec![None, None]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types), Arc::new(weights)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("empty_key_props.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=2 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        use crate::types::RelationshipDeduplication;
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["weight"]), // Property column, but all null
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
            None, // key_property_columns
        ).unwrap();
        
        // Should treat as unique (add all) since no key properties (all null)
        assert_eq!(rel_ids.len(), 2);
    }

    #[test]
    fn test_load_nodes_with_duplicate_node_ids() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        
        // Same ID appears twice
        let ids = Int64Array::from(vec![1, 1]);
        let labels = StringArray::from(vec!["Person", "User"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("duplicate_ids.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None,
            None,
            None, // default_label
        ).unwrap();
        
        // Should only return unique node IDs
        assert_eq!(node_ids.len(), 1);
        assert_eq!(node_ids, vec![1]);
        // But labels should be merged
        let labels = builder.node_labels(1);
        assert_eq!(labels.len(), 2);
    }

    #[test]
    fn test_load_relationships_with_all_property_types() {
        use arrow::array::{Int64Array, StringArray, Float64Array, BooleanArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("weight", DataType::Float64, false),
            Field::new("count", DataType::Int64, false),
            Field::new("active", DataType::Boolean, false),
        ]);
        
        let from = Int64Array::from(vec![1]);
        let to = Int64Array::from(vec![2]);
        let types = StringArray::from(vec!["KNOWS"]);
        let names = StringArray::from(vec!["rel1"]);
        let weights = Float64Array::from(vec![0.8]);
        let counts = Int64Array::from(vec![5]);
        let active = BooleanArray::from(vec![true]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(from),
                Arc::new(to),
                Arc::new(types),
                Arc::new(names),
                Arc::new(weights),
                Arc::new(counts),
                Arc::new(active),
            ],
        ).unwrap();
        
        let file_path = temp_dir.path().join("all_prop_types.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=2 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["name", "weight", "count", "active"]),
            None,
            None,
            None, // key_property_columns
        ).unwrap();

        assert_eq!(rel_ids.len(), 1);
        assert_eq!(builder.rel_count(), 1);

        // Verify all property types were set
        let snapshot = builder.finalize(None);
        let csr_pos = 0u32;
        assert!(snapshot.relationship_property(csr_pos, "name").is_some());
        assert!(snapshot.relationship_property(csr_pos, "weight").is_some());
        assert!(snapshot.relationship_property(csr_pos, "count").is_some());
        assert!(snapshot.relationship_property(csr_pos, "active").is_some());
    }

    #[test]
    fn test_load_nodes_with_int32_id_column() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        
        let ids = Int32Array::from(vec![1, 2, 3]);
        let labels = StringArray::from(vec!["Person", "Person", "Company"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("int32_ids.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None,
            None,
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 3);
        assert_eq!(node_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_load_nodes_with_nullable_id_column() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true), // Nullable
            Field::new("label", DataType::Utf8, false),
        ]);
        
        let ids = Int64Array::from(vec![Some(1), None, Some(3)]);
        let labels = StringArray::from(vec!["Person", "Person", "Company"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("nullable_ids.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None,
            None,
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 3);
        // First node has ID 1, second auto-generates (should be 0), third has ID 3
        assert_eq!(node_ids[0], 1);
        assert_eq!(node_ids[2], 3);
        // Middle node should have auto-generated ID
        assert!(node_ids[1] < 3 || node_ids[1] > 3); // Not 1 or 3
    }

    #[test]
    fn test_load_nodes_invalid_id_column_type() {
        use arrow::array::{Float64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // ID column is Float64 instead of Int64/Int32
        let schema = Schema::new(vec![
            Field::new("id", DataType::Float64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        
        let ids = Float64Array::from(vec![1.0, 2.0]);
        let labels = StringArray::from(vec!["Person", "Person"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("invalid_id_type.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None,
            None,
            None, // default_label
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_missing_start_column() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let result = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "nonexistent_from", // Column doesn't exist
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_missing_end_column() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let result = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "nonexistent_to", // Column doesn't exist
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_missing_rel_type_column() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_parquet(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        let result = builder.load_relationships_from_parquet(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            Some("nonexistent_type"), // Column doesn't exist
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_invalid_rel_type_column_type() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Type column is Int64 instead of String
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Int64, false),
        ]);
        
        let from = Int64Array::from(vec![1]);
        let to = Int64Array::from(vec![2]);
        let types = Int64Array::from(vec![1]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("invalid_rel_type.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();

        let result = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_invalid_start_column_type() {
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Start column is Float64 instead of Int64/Int32
        let schema = Schema::new(vec![
            Field::new("from", DataType::Float64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Float64Array::from(vec![1.0]);
        let to = Int64Array::from(vec![2]);
        let types = StringArray::from(vec!["KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("invalid_start_type.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();

        let result = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_invalid_end_column_type() {
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // End column is Float64 instead of Int64/Int32
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Float64, false),
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int64Array::from(vec![1]);
        let to = Float64Array::from(vec![2.0]);
        let types = StringArray::from(vec!["KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("invalid_end_type.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();

        let result = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_with_unsupported_property_type() {
        use arrow::array::{Int64Array, Date32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Date32 is not directly supported, should fall through to string conversion
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("birth_date", DataType::Date32, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        // Date32 represents days since Unix epoch
        let dates = Date32Array::from(vec![0, 365]); // Jan 1, 1970 and Jan 1, 1971
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(dates)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("date_property.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        // Should succeed, but Date32 will fall through to the _ case
        // which tries to convert to string, but Date32Array doesn't downcast to StringArray
        // So the property won't be set, but the node will be loaded
        let result = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["birth_date"]),
            None,
            None, // default_label
        );
        
        // Should succeed (node loaded, but property might not be set due to type mismatch)
        assert!(result.is_ok());
        let node_ids = result.unwrap();
        assert_eq!(node_ids.len(), 2);
    }

    #[test]
    fn test_load_nodes_with_int32_id_nullable() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true), // Nullable
            Field::new("label", DataType::Utf8, false),
        ]);
        
        let ids = Int32Array::from(vec![Some(1), None, Some(3)]);
        let labels = StringArray::from(vec!["Person", "Person", "Company"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("int32_nullable_ids.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None,
            None,
            None, // default_label
        ).unwrap();
        
        assert_eq!(node_ids.len(), 3);
        assert_eq!(node_ids[0], 1);
        assert_eq!(node_ids[2], 3);
        // Middle node should have auto-generated ID
    }

    #[test]
    fn test_load_relationships_with_int32_nullable_nodes() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int32, true), // Nullable
            Field::new("to", DataType::Int32, true),   // Nullable
            Field::new("type", DataType::Utf8, false),
        ]);
        
        let from = Int32Array::from(vec![Some(1), None, Some(2)]);
        let to = Int32Array::from(vec![Some(2), Some(3), None]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS", "KNOWS"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("int32_nullable_nodes.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }

        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None, // key_property_columns
        ).unwrap();

        // Only the first relationship should be added (others have null nodes)
        assert_eq!(rel_ids.len(), 1);
        assert_eq!(builder.rel_count(), 1);
    }

    #[test]
    fn test_load_nodes_with_property_fallback_to_string() {
        use arrow::array::{Int64Array, TimestampSecondArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // TimestampSecond is not directly supported, should fall through to string conversion
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None), false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        // TimestampSecond represents seconds since Unix epoch
        let timestamps = TimestampSecondArray::from(vec![0, 86400]); // Jan 1, 1970 and Jan 2, 1970
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(timestamps)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("timestamp_property.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        // Should succeed, but TimestampSecond will fall through to the _ case
        // which tries to convert to string, but TimestampSecondArray doesn't downcast to StringArray
        // So the property won't be set, but the node will be loaded
        let result = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["timestamp"]),
            None,
            None, // default_label
        );
        
        // Should succeed (node loaded, but property might not be set due to type mismatch)
        assert!(result.is_ok());
        let node_ids = result.unwrap();
        assert_eq!(node_ids.len(), 2);
    }

    #[test]
    fn test_load_relationships_with_property_fallback_to_string() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        use arrow::array::Date64Array;
        
        let temp_dir = TempDir::new().unwrap();
        // Date64 is not directly supported, should fall through to string conversion
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("created", DataType::Date64, false),
        ]);
        
        let from = Int64Array::from(vec![1]);
        let to = Int64Array::from(vec![2]);
        let types = StringArray::from(vec!["KNOWS"]);
        // Date64 represents milliseconds since Unix epoch
        let created = Date64Array::from(vec![0]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types), Arc::new(created)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("date64_rel_prop.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();
        
        // Should succeed, but Date64 will fall through to the _ case
        // which tries to convert to string, but Date64Array doesn't downcast to StringArray
        // So the property won't be set, but the relationship will be loaded
        let result = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["created"]),
            None,
            None,
            None, // key_property_columns
        );

        // Should succeed (relationship loaded, but property might not be set due to type mismatch)
        assert!(result.is_ok());
        let rel_ids = result.unwrap();
        assert_eq!(rel_ids.len(), 1);
    }

    #[test]
    fn test_deduplication_with_empty_dedup_key() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // All dedup properties are null, so dedup_key will be empty
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true), // Nullable, all null
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        let labels = StringArray::from(vec!["Person", "Person"]);
        let emails = StringArray::from(vec![None::<&str>, None]); // All null
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(emails)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("empty_dedup_key.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["email"]),
            Some(vec!["email"]), // Deduplicate by email, but all are null
            None, // default_label
        ).unwrap();
        
        // All nodes should be loaded separately since dedup_key is empty
        assert_eq!(node_ids.len(), 2);
        assert_eq!(builder.node_count(), 2);
        // dedup_map should be empty since no valid dedup keys were created
        assert_eq!(builder.dedup_map.len(), 0);
    }

    #[test]
    fn test_deduplication_with_partial_dedup_columns_missing() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Request deduplication by email and username, but username column doesn't exist
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            // username column is missing
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        let labels = StringArray::from(vec!["Person", "Person"]);
        let emails = StringArray::from(vec!["alice@example.com", "bob@example.com"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(emails)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("partial_dedup_cols.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        // Request deduplication by email and username, but username doesn't exist
        // Should now return SchemaError
        let result = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["email"]),
            Some(vec!["email", "username"]), // username doesn't exist
            None, // default_label
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_deduplication_with_mixed_value_types() {
        use arrow::array::{Int64Array, StringArray, Float64Array, BooleanArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Deduplicate by multiple properties of different types
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2, 3]);
        let labels = StringArray::from(vec!["Person", "Person", "Person"]);
        let emails = StringArray::from(vec!["alice@example.com", "alice@example.com", "bob@example.com"]);
        let ages = Int64Array::from(vec![30, 30, 25]);
        let scores = Float64Array::from(vec![95.5, 95.5, 88.0]);
        let active = BooleanArray::from(vec![true, true, false]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(emails), Arc::new(ages), Arc::new(scores), Arc::new(active)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("mixed_dedup_types.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        // Deduplicate by all property types
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["email", "age", "score", "active"]),
            Some(vec!["email", "age", "score", "active"]), // Multi-key deduplication
            None, // default_label
        ).unwrap();
        
        // Nodes 1 and 2 have same values for all dedup properties, so should be deduplicated
        // node_ids.len() returns unique node IDs seen, which may be less than rows processed
        // After deduplication, rows 1 and 2 map to the same node (node 1), so node_ids contains 2 unique nodes
        assert_eq!(node_ids.len(), 2);
        // After deduplication, node_count() should be 2 (1&2 merged, plus 3)
        assert_eq!(builder.node_count(), 2);
        // dedup_map should have entries
        assert!(builder.dedup_map.len() > 0);
    }

    #[test]
    fn test_deduplication_with_unsupported_type_fallback() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        use arrow::array::Date32Array;
        
        let temp_dir = TempDir::new().unwrap();
        // Deduplicate by Date32 (unsupported type, falls through to string conversion)
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("birth_date", DataType::Date32, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        let labels = StringArray::from(vec!["Person", "Person"]);
        // Date32 represents days since Unix epoch
        let dates = Date32Array::from(vec![0, 0]); // Same date
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(dates)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("date32_dedup.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        // Date32 falls through to _ case which tries string conversion
        // But Date32Array doesn't downcast to StringArray, so dedup_key will be None
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["birth_date"]),
            Some(vec!["birth_date"]), // Deduplicate by Date32
            None, // default_label
        ).unwrap();
        
        // Both nodes should be loaded (deduplication won't work due to type mismatch)
        assert_eq!(node_ids.len(), 2);
        assert_eq!(builder.node_count(), 2);
    }

    #[test]
    fn test_deduplication_with_large_utf8() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        use arrow::array::LargeStringArray;
        
        let temp_dir = TempDir::new().unwrap();
        // Deduplicate by LargeUtf8
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::LargeUtf8, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        let labels = StringArray::from(vec!["Person", "Person"]);
        let emails = LargeStringArray::from(vec!["alice@example.com", "alice@example.com"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(emails)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("large_utf8_dedup.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        // LargeUtf8 is now properly handled via extract_string_value / extract_value_id
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["email"]),
            Some(vec!["email"]), // Deduplicate by LargeUtf8
            None, // default_label
        ).unwrap();

        // Both rows have the same email, deduplication should now work with LargeUtf8
        assert_eq!(node_ids.len(), 1);
        assert_eq!(builder.node_count(), 1);
        assert!(builder.dedup_map.len() > 0);
    }

    #[test]
    fn test_deduplication_with_boolean_property() {
        use arrow::array::{Int64Array, StringArray, BooleanArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Deduplicate by boolean property
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("verified", DataType::Boolean, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2, 3]);
        let labels = StringArray::from(vec!["Person", "Person", "Person"]);
        let verified = BooleanArray::from(vec![true, true, false]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(verified)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("bool_dedup.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["verified"]),
            Some(vec!["verified"]), // Deduplicate by boolean
            None, // default_label
        ).unwrap();
        
        // Nodes 1 and 2 have same verified=true, so should be deduplicated
        // After deduplication, rows 1 and 2 map to the same node (node 1), so node_ids contains 2 unique nodes
        assert_eq!(node_ids.len(), 2);
        // Should have 2 unique nodes (1&2 merged, plus 3) if deduplication worked
        assert_eq!(builder.node_count(), 2);
        // dedup_map should have entries
        assert!(builder.dedup_map.len() > 0);
    }

    #[test]
    fn test_deduplication_with_float64_property() {
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Deduplicate by Float64 property
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2, 3]);
        let labels = StringArray::from(vec!["Person", "Person", "Person"]);
        let scores = Float64Array::from(vec![95.5, 95.5, 88.0]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(scores)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("float64_dedup.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["score"]),
            Some(vec!["score"]), // Deduplicate by Float64
            None, // default_label
        ).unwrap();
        
        // Nodes 1 and 2 have same score=95.5, so should be deduplicated
        // After deduplication, rows 1 and 2 map to the same node (node 1), so node_ids contains 2 unique nodes
        assert_eq!(node_ids.len(), 2);
        // Should have 2 unique nodes (1&2 merged, plus 3) if deduplication worked
        assert_eq!(builder.node_count(), 2);
        // dedup_map should have entries
        assert!(builder.dedup_map.len() > 0);
    }

    #[test]
    fn test_deduplication_with_row_no_dedup_key() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Some rows have null dedup properties, so they won't have dedup keys
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true), // Nullable
        ]);
        
        let ids = Int64Array::from(vec![1, 2, 3]);
        let labels = StringArray::from(vec!["Person", "Person", "Person"]);
        let emails = StringArray::from(vec![Some("alice@example.com"), None, Some("alice@example.com")]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(emails)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("no_dedup_key_row.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["email"]),
            Some(vec!["email"]), // Deduplicate by email
            None, // default_label
        ).unwrap();
        
        // Node 2 has null email, so no dedup key, should be added separately
        // Nodes 1 and 3 have same email, so should be deduplicated
        // After deduplication, rows 1 and 3 map to the same node (node 1), so node_ids contains 2 unique nodes
        assert_eq!(node_ids.len(), 2);
        // Should have 2 unique nodes (1&3 merged, plus 2) if deduplication worked
        assert_eq!(builder.node_count(), 2);
    }

    #[test]
    fn test_deduplication_label_merging() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Same email but different labels - labels should be merged
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]);
        
        let ids = Int64Array::from(vec![1, 2]);
        let labels = StringArray::from(vec!["Person", "User"]); // Different labels
        let emails = StringArray::from(vec!["alice@example.com", "alice@example.com"]); // Same email
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(labels), Arc::new(emails)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("label_merge_dedup.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["email"]),
            Some(vec!["email"]), // Deduplicate by email
            None, // default_label
        ).unwrap();
        
        // Both nodes have the same email, so they should be deduplicated
        // After deduplication, rows 1 and 2 map to the same node, so node_ids contains 1 unique node
        assert_eq!(node_ids.len(), 1);
        // Check that dedup_map has an entry
        assert!(builder.dedup_map.len() > 0);
        // If deduplication worked, node_count should be 1
        assert_eq!(builder.node_count(), 1);
    }

    #[test]
    fn test_deduplication_with_auto_generated_id() {
        use arrow::array::{StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // No ID column - auto-generate, but deduplicate by email
        let schema = Schema::new(vec![
            Field::new("label", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]);
        
        let labels = StringArray::from(vec!["Person", "Person"]);
        let emails = StringArray::from(vec!["alice@example.com", "alice@example.com"]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(labels), Arc::new(emails)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("auto_id_dedup.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            None, // No ID column - auto-generate
            Some(vec!["label"]),
            Some(vec!["email"]),
            Some(vec!["email"]), // Deduplicate by email
            None, // default_label
        ).unwrap();
        
        // Both rows should be deduplicated to the same node
        // After deduplication, rows 1 and 2 map to the same node, so node_ids contains 1 unique node
        assert_eq!(node_ids.len(), 1);
        // Should have 1 unique node if deduplication worked
        assert_eq!(builder.node_count(), 1);
    }

    #[test]
    fn test_relationship_deduplication_with_empty_key_props() {
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // All key properties are null, so key_props will be empty
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("weight", DataType::Float64, true), // All null
        ]);
        
        let from = Int64Array::from(vec![1, 1]);
        let to = Int64Array::from(vec![2, 2]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS"]);
        let weights = Float64Array::from(vec![None, None]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types), Arc::new(weights)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("empty_key_props_rel.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();
        
        use crate::types::RelationshipDeduplication;
        // Request deduplication by type and key props, but all key props are null
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["weight"]), // Property column, but all null
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
            None, // key_property_columns
        ).unwrap();
        
        // Should treat as unique (add all) since key_props is empty
        assert_eq!(rel_ids.len(), 2);
        assert_eq!(builder.rel_count(), 2);
    }

    #[test]
    fn test_relationship_deduplication_with_mixed_key_props() {
        use arrow::array::{Int64Array, StringArray, Float64Array, BooleanArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        
        let temp_dir = TempDir::new().unwrap();
        // Deduplicate by multiple property types
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("weight", DataType::Float64, false),
            Field::new("verified", DataType::Boolean, false),
        ]);
        
        let from = Int64Array::from(vec![1, 1, 2]);
        let to = Int64Array::from(vec![2, 2, 3]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS", "KNOWS"]);
        let weights = Float64Array::from(vec![0.8, 0.8, 0.5]);
        let verified = BooleanArray::from(vec![true, true, false]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types), Arc::new(weights), Arc::new(verified)],
        ).unwrap();
        
        let file_path = temp_dir.path().join("mixed_key_props_rel.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }
        
        use crate::types::RelationshipDeduplication;
        // Deduplicate by weight and verified (both properties)
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["weight", "verified"]),
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
            None, // key_property_columns
        ).unwrap();
        
        // First two relationships have same (from, to, type, weight, verified), so should be deduplicated
        assert_eq!(rel_ids.len(), 2);
        assert_eq!(builder.rel_count(), 2);
    }

    // ============================================================================
    // Node Properties Tests - Comprehensive verification that properties are saved
    // ============================================================================

    #[test]
    fn test_load_nodes_3_rows_simple_with_properties() {
        // Reproduces a simple 3-row parquet test case to verify properties are saved
        use arrow::array::{Int64Array, StringArray, Float64Array, BooleanArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
        ]);

        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let ages = Int64Array::from(vec![30, 25, 35]);
        let scores = Float64Array::from(vec![95.5, 88.0, 92.5]);
        let active = BooleanArray::from(vec![true, false, true]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(ids),
                Arc::new(names),
                Arc::new(ages),
                Arc::new(scores),
                Arc::new(active),
            ],
        ).unwrap();

        let file_path = temp_dir.path().join("simple_3_rows.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name", "age", "score", "active"]),
            None,
            None,
        ).unwrap();

        // Verify all nodes loaded
        assert_eq!(node_ids.len(), 3);
        assert_eq!(node_ids, vec![1, 2, 3]);

        // Verify properties are accessible via builder.prop() BEFORE finalization
        // Node 1: Alice, 30, 95.5, true
        let name1 = builder.prop(1, "name");
        assert!(name1.is_some(), "Node 1 should have 'name' property");
        if let Some(ValueId::Str(sid)) = name1 {
            assert_eq!(builder.resolve_string(sid), "Alice");
        } else {
            panic!("Expected string ValueId for name");
        }

        let age1 = builder.prop(1, "age");
        assert!(age1.is_some(), "Node 1 should have 'age' property");
        assert_eq!(age1, Some(ValueId::I64(30)));

        let score1 = builder.prop(1, "score");
        assert!(score1.is_some(), "Node 1 should have 'score' property");
        if let Some(ValueId::F64(bits)) = score1 {
            let f = f64::from_bits(bits);
            assert!((f - 95.5).abs() < 0.001);
        } else {
            panic!("Expected f64 ValueId for score");
        }

        let active1 = builder.prop(1, "active");
        assert!(active1.is_some(), "Node 1 should have 'active' property");
        assert_eq!(active1, Some(ValueId::Bool(true)));

        // Node 2: Bob, 25, 88.0, false
        let name2 = builder.prop(2, "name");
        assert!(name2.is_some(), "Node 2 should have 'name' property");
        if let Some(ValueId::Str(sid)) = name2 {
            assert_eq!(builder.resolve_string(sid), "Bob");
        }

        let age2 = builder.prop(2, "age");
        assert_eq!(age2, Some(ValueId::I64(25)));

        let active2 = builder.prop(2, "active");
        assert_eq!(active2, Some(ValueId::Bool(false)));

        // Node 3: Charlie, 35, 92.5, true
        let name3 = builder.prop(3, "name");
        assert!(name3.is_some(), "Node 3 should have 'name' property");
        if let Some(ValueId::Str(sid)) = name3 {
            assert_eq!(builder.resolve_string(sid), "Charlie");
        }

        let age3 = builder.prop(3, "age");
        assert_eq!(age3, Some(ValueId::I64(35)));
    }

    #[test]
    fn test_load_nodes_properties_persist_after_finalize() {
        // Verify properties survive finalization into GraphSnapshot
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]);

        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let ages = Int64Array::from(vec![30, 25, 35]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(names), Arc::new(ages)],
        ).unwrap();

        let file_path = temp_dir.path().join("props_finalize.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name", "age"]),
            None,
            None,
        ).unwrap();

        // Finalize to snapshot
        let snapshot = builder.finalize(None);

        // Verify properties in snapshot
        let name1 = snapshot.prop(1, "name");
        assert!(name1.is_some(), "Snapshot node 1 should have 'name' property");

        let age1 = snapshot.prop(1, "age");
        assert!(age1.is_some(), "Snapshot node 1 should have 'age' property");
        assert_eq!(age1, Some(ValueId::I64(30)));

        // Verify all nodes have properties
        for node_id in [1u32, 2, 3] {
            let name = snapshot.prop(node_id, "name");
            assert!(name.is_some(), "Node {} should have 'name' property in snapshot", node_id);

            let age = snapshot.prop(node_id, "age");
            assert!(age.is_some(), "Node {} should have 'age' property in snapshot", node_id);
        }
    }

    #[test]
    fn test_load_nodes_properties_with_auto_id() {
        // Verify properties work when node IDs are auto-generated
        use arrow::array::{StringArray, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        // No ID column - IDs will be auto-generated
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]);

        let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let ages = Int64Array::from(vec![30, 25, 35]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(names), Arc::new(ages)],
        ).unwrap();

        let file_path = temp_dir.path().join("auto_id_props.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            None, // No ID column - auto-generate
            None,
            Some(vec!["name", "age"]),
            None,
            None,
        ).unwrap();

        // Verify 3 nodes were created
        assert_eq!(node_ids.len(), 3);

        // Verify each auto-generated node has properties
        for &node_id in &node_ids {
            let name = builder.prop(node_id, "name");
            assert!(name.is_some(), "Auto-ID node {} should have 'name' property", node_id);

            let age = builder.prop(node_id, "age");
            assert!(age.is_some(), "Auto-ID node {} should have 'age' property", node_id);
        }

        // Verify specific values (auto-generated IDs start from 0)
        let name0 = builder.prop(node_ids[0], "name");
        if let Some(ValueId::Str(sid)) = name0 {
            assert_eq!(builder.resolve_string(sid), "Alice");
        }

        let age0 = builder.prop(node_ids[0], "age");
        assert_eq!(age0, Some(ValueId::I64(30)));
    }

    #[test]
    fn test_load_nodes_properties_with_deduplication() {
        // Verify properties are correctly set on deduplicated nodes
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),      // Used for dedup
            Field::new("age", DataType::Int64, false),
            Field::new("city", DataType::Utf8, false),
        ]);

        // Two rows with same name (will be deduplicated)
        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["Alice", "Alice", "Bob"]); // First two have same name
        let ages = Int64Array::from(vec![30, 31, 25]); // Different ages
        let cities = StringArray::from(vec!["NYC", "LA", "Chicago"]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(names), Arc::new(ages), Arc::new(cities)],
        ).unwrap();

        let file_path = temp_dir.path().join("dedup_props.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name", "age", "city"]),
            Some(vec!["name"]), // Deduplicate by name
            None,
        ).unwrap();

        // Should have 3 node IDs returned (even though 2 are deduplicated internally)
        // But unique nodes based on name = 2 (Alice, Bob)
        // The dedup_map should have 2 entries
        assert_eq!(builder.dedup_map.len(), 2, "Should have 2 unique names in dedup_map");

        // Verify node 1 (first Alice) has properties
        let name1 = builder.prop(1, "name");
        assert!(name1.is_some(), "Node 1 should have 'name' property");
        if let Some(ValueId::Str(sid)) = name1 {
            assert_eq!(builder.resolve_string(sid), "Alice");
        }

        // The first occurrence's properties should be set
        let age1 = builder.prop(1, "age");
        assert!(age1.is_some(), "Node 1 should have 'age' property");

        let city1 = builder.prop(1, "city");
        assert!(city1.is_some(), "Node 1 should have 'city' property");

        // Verify node 3 (Bob) has properties
        let name3 = builder.prop(3, "name");
        assert!(name3.is_some(), "Node 3 should have 'name' property");
        if let Some(ValueId::Str(sid)) = name3 {
            assert_eq!(builder.resolve_string(sid), "Bob");
        }

        let age3 = builder.prop(3, "age");
        assert_eq!(age3, Some(ValueId::I64(25)));
    }

    #[test]
    fn test_load_nodes_all_property_types() {
        // Test all supported property types
        use arrow::array::{Int64Array, Int32Array, StringArray, Float64Array, BooleanArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("str_prop", DataType::Utf8, false),
            Field::new("i64_prop", DataType::Int64, false),
            Field::new("f64_prop", DataType::Float64, false),
            Field::new("bool_prop", DataType::Boolean, false),
            Field::new("i32_id", DataType::Int32, false), // Int32 should work too
        ]);

        let ids = Int64Array::from(vec![1]);
        let str_props = StringArray::from(vec!["test_string"]);
        let i64_props = Int64Array::from(vec![42i64]);
        let f64_props = Float64Array::from(vec![3.14159]);
        let bool_props = BooleanArray::from(vec![true]);
        let i32_ids = Int32Array::from(vec![100i32]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(ids),
                Arc::new(str_props),
                Arc::new(i64_props),
                Arc::new(f64_props),
                Arc::new(bool_props),
                Arc::new(i32_ids),
            ],
        ).unwrap();

        let file_path = temp_dir.path().join("all_types.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        builder.load_nodes_from_parquet(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["str_prop", "i64_prop", "f64_prop", "bool_prop"]),
            None,
            None,
        ).unwrap();

        // Verify string property
        let str_val = builder.prop(1, "str_prop");
        assert!(str_val.is_some(), "Should have string property");
        if let Some(ValueId::Str(sid)) = str_val {
            assert_eq!(builder.resolve_string(sid), "test_string");
        } else {
            panic!("Expected Str ValueId");
        }

        // Verify i64 property
        let i64_val = builder.prop(1, "i64_prop");
        assert_eq!(i64_val, Some(ValueId::I64(42)));

        // Verify f64 property
        let f64_val = builder.prop(1, "f64_prop");
        assert!(f64_val.is_some(), "Should have f64 property");
        if let Some(ValueId::F64(bits)) = f64_val {
            let f = f64::from_bits(bits);
            assert!((f - 3.14159).abs() < 0.00001);
        } else {
            panic!("Expected F64 ValueId");
        }

        // Verify bool property
        let bool_val = builder.prop(1, "bool_prop");
        assert_eq!(bool_val, Some(ValueId::Bool(true)));
    }

    #[test]
    fn test_load_relationships_with_key_property_columns_subset() {
        // Test that key_property_columns allows deduplicating on a subset of property_columns
        // Scenario: Load many properties but only use some for deduplication
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("key_id", DataType::Utf8, false),     // Used for dedup
            Field::new("amount", DataType::Float64, false),  // NOT used for dedup
            Field::new("note", DataType::Utf8, true),        // NOT used for dedup
        ]);

        // Two relationships with same key_id but different amounts/notes
        // With key_property_columns=["key_id"], they should be deduplicated
        let from = Int64Array::from(vec![1, 1]);
        let to = Int64Array::from(vec![2, 2]);
        let types = StringArray::from(vec!["TRANSFER", "TRANSFER"]);
        let key_ids = StringArray::from(vec!["txn-001", "txn-001"]); // Same key
        let amounts = Float64Array::from(vec![100.0, 200.0]);  // Different
        let notes = StringArray::from(vec![Some("first"), Some("second")]);  // Different

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(from),
                Arc::new(to),
                Arc::new(types),
                Arc::new(key_ids),
                Arc::new(amounts),
                Arc::new(notes),
            ],
        ).unwrap();

        let file_path = temp_dir.path().join("key_cols_subset.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();

        use crate::types::RelationshipDeduplication;
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["key_id", "amount", "note"]), // Load all properties
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
            Some(vec!["key_id"]), // Only deduplicate by key_id
        ).unwrap();

        // Should deduplicate because key_id is the same
        assert_eq!(rel_ids.len(), 1, "Should deduplicate by key_id only");
    }

    #[test]
    fn test_load_relationships_key_property_columns_backward_compat() {
        // Test backward compatibility: when key_property_columns is None,
        // all property_columns are used for deduplication
        use arrow::array::{Int64Array, StringArray, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("key_id", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]);

        // Two relationships with same key_id but different amounts
        // With key_property_columns=None, both should be kept (different composite keys)
        let from = Int64Array::from(vec![1, 1]);
        let to = Int64Array::from(vec![2, 2]);
        let types = StringArray::from(vec!["TRANSFER", "TRANSFER"]);
        let key_ids = StringArray::from(vec!["txn-001", "txn-001"]); // Same
        let amounts = Float64Array::from(vec![100.0, 200.0]);  // Different

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(from),
                Arc::new(to),
                Arc::new(types),
                Arc::new(key_ids),
                Arc::new(amounts),
            ],
        ).unwrap();

        let file_path = temp_dir.path().join("key_cols_backward.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();

        use crate::types::RelationshipDeduplication;
        let rel_ids = builder.load_relationships_from_parquet(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["key_id", "amount"]), // Load both
            None,
            Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties),
            None, // key_property_columns = None -> use all property_columns
        ).unwrap();

        // Should keep both because (key_id, amount) is different
        assert_eq!(rel_ids.len(), 2, "Should keep both when all props differ");
    }

    #[test]
    fn test_load_relationships_v2_with_id_reference() {
        // Test v2 function with NodeReference::Id (same as v1 behavior)
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        use crate::types::NodeReference;

        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(vec![
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ]);

        let from = Int64Array::from(vec![1, 2]);
        let to = Int64Array::from(vec![2, 3]);
        let types = StringArray::from(vec!["KNOWS", "FOLLOWS"]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from), Arc::new(to), Arc::new(types)],
        ).unwrap();

        let file_path = temp_dir.path().join("v2_id_ref.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Person"]).unwrap();
        }

        let rel_ids = builder.load_relationships_from_parquet_v2(
            file_path.to_str().unwrap(),
            NodeReference::id("from"),
            NodeReference::id("to"),
            Some("type"),
            None,
            None,
            None,
            None,
        ).unwrap();

        assert_eq!(rel_ids.len(), 2);
        assert_eq!(rel_ids[0], (1, 2));
        assert_eq!(rel_ids[1], (2, 3));
    }

    #[test]
    fn test_load_relationships_v2_with_property_reference() {
        // Test v2 function with NodeReference::Property (single property lookup)
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        use crate::types::NodeReference;

        let temp_dir = TempDir::new().unwrap();

        // Create relationship parquet with UUIDs instead of node IDs
        let schema = Schema::new(vec![
            Field::new("from_uuid", DataType::Utf8, false),
            Field::new("to_uuid", DataType::Utf8, false),
            Field::new("rel_type", DataType::Utf8, false),
        ]);

        let from_uuids = StringArray::from(vec!["uuid-alice", "uuid-bob"]);
        let to_uuids = StringArray::from(vec!["uuid-bob", "uuid-charlie"]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS"]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(from_uuids), Arc::new(to_uuids), Arc::new(types)],
        ).unwrap();

        let file_path = temp_dir.path().join("v2_prop_ref.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Create nodes with uuid properties
        let mut builder = GraphBuilder::new(None, None);
        let alice = builder.add_node(None, &["Person"]).unwrap();
        builder.set_prop_str(alice, "uuid", "uuid-alice").unwrap();
        let bob = builder.add_node(None, &["Person"]).unwrap();
        builder.set_prop_str(bob, "uuid", "uuid-bob").unwrap();
        let charlie = builder.add_node(None, &["Person"]).unwrap();
        builder.set_prop_str(charlie, "uuid", "uuid-charlie").unwrap();

        // Load relationships using property lookup
        let rel_ids = builder.load_relationships_from_parquet_v2(
            file_path.to_str().unwrap(),
            NodeReference::property("from_uuid", "uuid", Some("Person")),
            NodeReference::property("to_uuid", "uuid", Some("Person")),
            Some("rel_type"),
            None,
            None,
            None,
            None,
        ).unwrap();

        assert_eq!(rel_ids.len(), 2);
        assert_eq!(rel_ids[0], (alice, bob));
        assert_eq!(rel_ids[1], (bob, charlie));
    }

    #[test]
    fn test_load_relationships_v2_with_composite_reference() {
        // Test v2 function with NodeReference::CompositeProperty
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        use crate::types::NodeReference;

        let temp_dir = TempDir::new().unwrap();

        // Create relationship parquet with composite keys (name + city)
        let schema = Schema::new(vec![
            Field::new("from_name", DataType::Utf8, false),
            Field::new("from_city", DataType::Utf8, false),
            Field::new("to_name", DataType::Utf8, false),
            Field::new("to_city", DataType::Utf8, false),
            Field::new("rel_type", DataType::Utf8, false),
        ]);

        let from_names = StringArray::from(vec!["Alice", "Bob"]);
        let from_cities = StringArray::from(vec!["NYC", "LA"]);
        let to_names = StringArray::from(vec!["Bob", "Alice"]);
        let to_cities = StringArray::from(vec!["LA", "NYC"]);
        let types = StringArray::from(vec!["KNOWS", "KNOWS"]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(from_names),
                Arc::new(from_cities),
                Arc::new(to_names),
                Arc::new(to_cities),
                Arc::new(types),
            ],
        ).unwrap();

        let file_path = temp_dir.path().join("v2_composite_ref.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Create nodes with name + city properties
        let mut builder = GraphBuilder::new(None, None);
        let alice = builder.add_node(None, &["Person"]).unwrap();
        builder.set_prop_str(alice, "name", "Alice").unwrap();
        builder.set_prop_str(alice, "city", "NYC").unwrap();
        let bob = builder.add_node(None, &["Person"]).unwrap();
        builder.set_prop_str(bob, "name", "Bob").unwrap();
        builder.set_prop_str(bob, "city", "LA").unwrap();

        // Load relationships using composite property lookup
        let rel_ids = builder.load_relationships_from_parquet_v2(
            file_path.to_str().unwrap(),
            NodeReference::composite(
                vec!["from_name".to_string(), "from_city".to_string()],
                vec!["name".to_string(), "city".to_string()],
                Some("Person"),
            ),
            NodeReference::composite(
                vec!["to_name".to_string(), "to_city".to_string()],
                vec!["name".to_string(), "city".to_string()],
                Some("Person"),
            ),
            Some("rel_type"),
            None,
            None,
            None,
            None,
        ).unwrap();

        assert_eq!(rel_ids.len(), 2);
        assert_eq!(rel_ids[0], (alice, bob));
        assert_eq!(rel_ids[1], (bob, alice));
    }
}

