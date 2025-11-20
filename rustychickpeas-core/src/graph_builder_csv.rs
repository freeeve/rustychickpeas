//! CSV reading support for GraphBuilder
//!
//! Supports both plain CSV and gzip-compressed CSV files (.csv.gz)

use crate::graph_builder::GraphBuilder;
use crate::error::{Result, GraphError};
use crate::graph_snapshot::ValueId;
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufReader, Read};
use hashbrown::HashMap;
use roaring::RoaringBitmap;

/// Helper to create a CSV reader from a file path
/// Handles both plain CSV and gzip-compressed CSV (.csv.gz)
fn create_csv_reader(path: &str) -> Result<csv::Reader<Box<dyn Read>>> {
    let file = File::open(path)
        .map_err(|e| GraphError::BulkLoadError(format!("Failed to open CSV file {}: {}", path, e)))?;
    
    let reader: Box<dyn Read> = if path.ends_with(".gz") || path.ends_with(".csv.gz") {
        Box::new(GzDecoder::new(BufReader::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    
    let csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);
    
    Ok(csv_reader)
}

/// Type hint for CSV column parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsvColumnType {
    /// Auto-detect type (try i64, f64, bool, then string)
    Auto,
    /// Force integer (i64)
    Int64,
    /// Force float (f64)
    Float64,
    /// Force boolean
    Bool,
    /// Force string
    String,
}

/// Parse a CSV string value into a ValueId
/// Uses type hint if provided, otherwise attempts heuristic parsing
fn parse_csv_value(value: &str, builder: &mut GraphBuilder, type_hint: CsvColumnType) -> ValueId {
    match type_hint {
        CsvColumnType::Int64 => {
            if let Ok(i) = value.parse::<i64>() {
                ValueId::I64(i)
            } else {
                // If parsing fails, treat as string
                ValueId::Str(builder.interner.get_or_intern(value))
            }
        }
        CsvColumnType::Float64 => {
            if let Ok(f) = value.parse::<f64>() {
                ValueId::from_f64(f)
            } else {
                // If parsing fails, treat as string
                ValueId::Str(builder.interner.get_or_intern(value))
            }
        }
        CsvColumnType::Bool => {
            if let Ok(b) = value.parse::<bool>() {
                ValueId::Bool(b)
            } else {
                // If parsing fails, treat as string
                ValueId::Str(builder.interner.get_or_intern(value))
            }
        }
        CsvColumnType::String => {
            ValueId::Str(builder.interner.get_or_intern(value))
        }
        CsvColumnType::Auto => {
            // Heuristic parsing: try i64, then f64, then bool, otherwise string
            // Note: This means "123" will be i64, not string. Use explicit String type if needed.
            if let Ok(i) = value.parse::<i64>() {
                ValueId::I64(i)
            } else if let Ok(f) = value.parse::<f64>() {
                ValueId::from_f64(f)
            } else if let Ok(b) = value.parse::<bool>() {
                ValueId::Bool(b)
            } else {
                ValueId::Str(builder.interner.get_or_intern(value))
            }
        }
    }
}

impl GraphBuilder {
    /// Load nodes from a CSV file into the builder
    /// 
    /// # Arguments
    /// * `path` - Path to CSV file (supports .csv and .csv.gz)
    /// * `node_id_column` - Optional column name for node IDs. If None, auto-generates IDs.
    /// * `label_columns` - Optional list of column names to use as labels
    /// * `property_columns` - Optional list of column names to use as properties. If None, uses all columns except ID and labels.
    /// * `unique_properties` - Optional list of property column names to use for deduplication. If provided, nodes with the same values for these properties will be merged.
    /// * `column_types` - Optional map of column names to types. If not specified, uses heuristic parsing (Auto).
    pub fn load_nodes_from_csv(
        &mut self,
        path: &str,
        node_id_column: Option<&str>,
        label_columns: Option<Vec<&str>>,
        property_columns: Option<Vec<&str>>,
        unique_properties: Option<Vec<&str>>,
        column_types: Option<HashMap<&str, CsvColumnType>>,
    ) -> Result<Vec<u32>> {
        let mut reader = create_csv_reader(path)?;
        
        // Get headers
        let headers = reader.headers()
            .map_err(|e| GraphError::BulkLoadError(format!("Failed to read CSV headers: {}", e)))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        
        // Configure deduplication if unique_properties is provided
        if let Some(ref unique_props) = unique_properties {
            self.enable_node_deduplication(unique_props.clone());
        }
        
        // Determine which columns to use
        let label_cols = label_columns.unwrap_or_default();
        let prop_cols = property_columns.unwrap_or_else(|| {
            headers
                .iter()
                .filter(|col| {
                    node_id_column.map(|id_col| col.as_str() != id_col).unwrap_or(true)
                        && !label_cols.contains(&col.as_str())
                })
                .map(|s| s.as_str())
                .collect()
        });
        
        // Find column indices
        let node_id_idx = node_id_column.and_then(|col| {
            headers.iter().position(|h| h == col)
        });
        
        let label_indices: Vec<usize> = label_cols.iter()
            .filter_map(|col| headers.iter().position(|h| h == col))
            .collect();
        
        let prop_indices: Vec<(usize, String)> = prop_cols.iter()
            .filter_map(|col| {
                headers.iter().position(|h| h == col).map(|idx| (idx, col.to_string()))
            })
            .collect();
        
        let unique_prop_indices: Vec<(usize, String)> = unique_properties.as_ref()
            .map(|props| {
                props.iter()
                    .filter_map(|col| {
                        headers.iter().position(|h| h == col).map(|idx| (idx, col.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        
        // Process rows
        let mut node_ids = Vec::new();
        let mut seen_node_ids = RoaringBitmap::new();
        let mut row_num = 0;
        
        for result in reader.records() {
            let record = result
                .map_err(|e| GraphError::BulkLoadError(format!("Failed to read CSV record at row {}: {}", row_num + 2, e)))?;
            
            row_num += 1;
            
            // Extract node ID
            let node_id = if let Some(idx) = node_id_idx {
                let id_str = record.get(idx)
                    .ok_or_else(|| GraphError::BulkLoadError(format!("Missing node ID column at row {}", row_num + 1)))?;
                if id_str.is_empty() {
                    None // Auto-generate
                } else {
                    let id_val = id_str.parse::<i64>()
                        .map_err(|e| GraphError::BulkLoadError(format!("Invalid node ID '{}' at row {}: {}", id_str, row_num + 1, e)))?;
                    if id_val < 0 || id_val > u32::MAX as i64 {
                        return Err(GraphError::BulkLoadError(
                            format!("Node ID {} exceeds u32::MAX ({})", id_val, u32::MAX)
                        ));
                    }
                    Some(id_val as u32)
                }
            } else {
                None // Auto-generate
            };
            
            // Extract labels
            let mut labels = Vec::new();
            for idx in &label_indices {
                if let Some(label_str) = record.get(*idx) {
                    if !label_str.is_empty() {
                        labels.push(self.interner.get_or_intern(label_str));
                    }
                }
            }
            
            // Extract unique properties for deduplication
            let mut dedup_key = None;
            if !unique_prop_indices.is_empty() {
                let mut dedup_values = Vec::new();
                let mut all_present = true;
                
                for (idx, prop_name) in &unique_prop_indices {
                    if let Some(val_str) = record.get(*idx) {
                        if !val_str.is_empty() {
                            let col_type = column_types.as_ref()
                                .and_then(|types| types.get(prop_name.as_str()))
                                .copied()
                                .unwrap_or(CsvColumnType::Auto);
                            let val = parse_csv_value(val_str, self, col_type);
                            dedup_values.push(val);
                        } else {
                            all_present = false;
                            break;
                        }
                    } else {
                        all_present = false;
                        break;
                    }
                }
                
                if all_present && !dedup_values.is_empty() {
                    dedup_key = Some(crate::types::DedupKey::from_slice(&dedup_values));
                }
            }
            
            // Check for existing node via deduplication
            if let Some(ref dk) = dedup_key {
                if let Some(&existing_id) = self.dedup_map.get(dk) {
                    // Use existing node, merge labels
                    if !labels.is_empty() {
                        let label_strings: Vec<String> = labels.iter()
                            .map(|&l| self.interner.resolve(l))
                            .collect();
                        let label_refs: Vec<&str> = label_strings.iter().map(|s| s.as_str()).collect();
                        if !label_refs.is_empty() {
                            self.add_node(Some(existing_id), &label_refs);
                        }
                    }
                    if !seen_node_ids.contains(existing_id) {
                        seen_node_ids.insert(existing_id);
                        node_ids.push(existing_id);
                    }
                } else {
                    // New node
                    let new_id = node_id.unwrap_or_else(|| {
                        let id = self.next_node_id;
                        self.next_node_id = id.wrapping_add(1);
                        if self.next_node_id == 0 {
                            panic!("Node ID counter wrapped around (exceeded u32::MAX)");
                        }
                        id
                    });
                    
                    // Add to dedup map
                    self.dedup_map.insert(dk.clone(), new_id);
                    
                    // Add node with labels
                    let label_strings: Vec<String> = labels.iter()
                        .map(|&l| self.interner.resolve(l))
                        .collect();
                    let label_refs: Vec<&str> = label_strings.iter().map(|s| s.as_str()).collect();
                    self.add_node(Some(new_id), &label_refs);
                    
                    // Add properties
                    for (idx, prop_name) in &prop_indices {
                        if let Some(val_str) = record.get(*idx) {
                            if !val_str.is_empty() {
                                let col_type = column_types.as_ref()
                                    .and_then(|types| types.get(prop_name.as_str()))
                                    .copied()
                                    .unwrap_or(CsvColumnType::Auto);
                                let val = parse_csv_value(val_str, self, col_type);
                                match val {
                                    ValueId::I64(v) => self.set_prop_i64(new_id, prop_name, v),
                                    ValueId::F64(bits) => {
                                        if let Some(f) = ValueId::F64(bits).to_f64() {
                                            self.set_prop_f64(new_id, prop_name, f);
                                        }
                                    }
                                    ValueId::Bool(v) => self.set_prop_bool(new_id, prop_name, v),
                                    ValueId::Str(v) => {
                                        let s = self.interner.resolve(v);
                                        self.set_prop_str(new_id, prop_name, s.as_str());
                                    }
                                }
                            }
                        }
                    }
                    
                    seen_node_ids.insert(new_id);
                    node_ids.push(new_id);
                }
            } else {
                // No deduplication - create new node
                let new_id = node_id.unwrap_or_else(|| {
                    let id = self.next_node_id;
                    self.next_node_id = id.wrapping_add(1);
                    if self.next_node_id == 0 {
                        panic!("Node ID counter wrapped around (exceeded u32::MAX)");
                    }
                    id
                });
                
                // Skip if we've already seen this ID
                if seen_node_ids.contains(new_id) {
                    // Merge labels if any
                    if !labels.is_empty() {
                        let label_strings: Vec<String> = labels.iter()
                            .map(|&l| self.interner.resolve(l))
                            .collect();
                        let label_refs: Vec<&str> = label_strings.iter().map(|s| s.as_str()).collect();
                        if !label_refs.is_empty() {
                            self.add_node(Some(new_id), &label_refs);
                        }
                    }
                    continue;
                }
                
                seen_node_ids.insert(new_id);
                
                // Add node with labels
                let label_strings: Vec<String> = labels.iter()
                    .map(|&l| self.interner.resolve(l))
                    .collect();
                let label_refs: Vec<&str> = label_strings.iter().map(|s| s.as_str()).collect();
                self.add_node(Some(new_id), &label_refs);
                
                // Add properties
                for (idx, prop_name) in &prop_indices {
                        if let Some(val_str) = record.get(*idx) {
                            if !val_str.is_empty() {
                                let col_type = column_types.as_ref()
                                    .and_then(|types| types.get(prop_name.as_str()))
                                    .copied()
                                    .unwrap_or(CsvColumnType::Auto);
                                let val = parse_csv_value(val_str, self, col_type);
                                match val {
                                ValueId::I64(v) => self.set_prop_i64(new_id, prop_name, v),
                                ValueId::F64(bits) => {
                                    if let Some(f) = ValueId::F64(bits).to_f64() {
                                        self.set_prop_f64(new_id, prop_name, f);
                                    }
                                }
                                ValueId::Bool(v) => self.set_prop_bool(new_id, prop_name, v),
                                ValueId::Str(v) => {
                                    let s = self.interner.resolve(v);
                                    self.set_prop_str(new_id, prop_name, s.as_str());
                                }
                            }
                        }
                    }
                }
                
                node_ids.push(new_id);
            }
        }
        
        Ok(node_ids)
    }
    
    /// Load relationships from a CSV file into the builder
    /// 
    /// # Arguments
    /// * `path` - Path to CSV file (supports .csv and .csv.gz)
    /// * `start_node_column` - Column name for start node IDs
    /// * `end_node_column` - Column name for end node IDs
    /// * `rel_type_column` - Optional column name for relationship type. If None, `fixed_rel_type` must be provided.
    /// * `property_columns` - Optional list of column names to use as properties. If None, uses all columns except start/end/type.
    /// * `fixed_rel_type` - Optional fixed relationship type to use for all relationships. Required if `rel_type_column` is None.
    /// * `deduplication` - Optional deduplication strategy for relationships
    /// * `column_types` - Optional map of column names to types. If not specified, uses heuristic parsing (Auto).
    pub fn load_relationships_from_csv(
        &mut self,
        path: &str,
        start_node_column: &str,
        end_node_column: &str,
        rel_type_column: Option<&str>,
        property_columns: Option<Vec<&str>>,
        fixed_rel_type: Option<&str>,
        deduplication: Option<crate::types::RelationshipDeduplication>,
        column_types: Option<HashMap<&str, CsvColumnType>>,
    ) -> Result<Vec<(u32, u32)>> {
        use crate::types::RelationshipDeduplication;
        
        let mut reader = create_csv_reader(path)?;
        
        // Get headers
        let headers = reader.headers()
            .map_err(|e| GraphError::BulkLoadError(format!("Failed to read CSV headers: {}", e)))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        
        // Find column indices
        let start_node_idx = headers.iter()
            .position(|h| h == start_node_column)
            .ok_or_else(|| GraphError::BulkLoadError(format!("Column '{}' not found", start_node_column)))?;
        let end_node_idx = headers.iter()
            .position(|h| h == end_node_column)
            .ok_or_else(|| GraphError::BulkLoadError(format!("Column '{}' not found", end_node_column)))?;
        
        let rel_type_idx = rel_type_column.and_then(|col| {
            headers.iter().position(|h| h == col)
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
        let prop_cols = property_columns.unwrap_or_else(|| {
            headers
                .iter()
                .filter(|col| {
                    col.as_str() != start_node_column
                        && col.as_str() != end_node_column
                        && rel_type_column.map(|rt_col| col.as_str() != rt_col).unwrap_or(true)
                })
                .map(|s| s.as_str())
                .collect()
        });
        
        let prop_indices: Vec<(usize, String)> = prop_cols.iter()
            .filter_map(|col| {
                headers.iter().position(|h| h == col).map(|idx| (idx, col.to_string()))
            })
            .collect();
        
        // Set up deduplication tracking
        let mut seen_by_type: HashMap<(u32, u32, u32), ()> = HashMap::new();
        let mut seen_by_type_and_props: HashMap<(u32, u32, u32, Vec<ValueId>), ()> = HashMap::new();
        
        // Process rows
        let mut rel_ids = Vec::new();
        let mut row_num = 0;
        
        for result in reader.records() {
            let record = result
                .map_err(|e| GraphError::BulkLoadError(format!("Failed to read CSV record at row {}: {}", row_num + 2, e)))?;
            
            row_num += 1;
            
            // Extract start and end node IDs
            let start_str = record.get(start_node_idx)
                .ok_or_else(|| GraphError::BulkLoadError(format!("Missing start node column at row {}", row_num + 1)))?;
            let end_str = record.get(end_node_idx)
                .ok_or_else(|| GraphError::BulkLoadError(format!("Missing end node column at row {}", row_num + 1)))?;
            
            let start_id = start_str.parse::<u32>()
                .map_err(|e| GraphError::BulkLoadError(format!("Invalid start node ID '{}' at row {}: {}", start_str, row_num + 1, e)))?;
            let end_id = end_str.parse::<u32>()
                .map_err(|e| GraphError::BulkLoadError(format!("Invalid end node ID '{}' at row {}: {}", end_str, row_num + 1, e)))?;
            
            // Extract relationship type
            let rel_type = if let Some(idx) = rel_type_idx {
                record.get(idx)
                    .ok_or_else(|| GraphError::BulkLoadError(format!("Missing relationship type column at row {}", row_num + 1)))?
                    .to_string()
            } else {
                fixed_rel_type.unwrap().to_string()
            };
            
            let rel_type_id = self.interner.get_or_intern(&rel_type);
            
            // Extract properties
            let mut props = HashMap::new();
            for (idx, prop_name) in &prop_indices {
                if let Some(val_str) = record.get(*idx) {
                    if !val_str.is_empty() {
                        let col_type = column_types.as_ref()
                            .and_then(|types| types.get(prop_name.as_str()))
                            .copied()
                            .unwrap_or(CsvColumnType::Auto);
                        let val = parse_csv_value(val_str, self, col_type);
                        let prop_key = self.interner.get_or_intern(prop_name);
                        props.insert(prop_key, val);
                    }
                }
            }
            
            // Apply deduplication
            let should_add = match deduplication {
                Some(RelationshipDeduplication::CreateUniqueByRelType) => {
                    let key = (start_id, end_id, rel_type_id);
                    if seen_by_type.contains_key(&key) {
                        false
                    } else {
                        seen_by_type.insert(key, ());
                        true
                    }
                }
                Some(RelationshipDeduplication::CreateUniqueByRelTypeAndKeyProperties) => {
                    // For CSV, we use all properties as key properties (can be refined later)
                    let key_props: Vec<ValueId> = props.values().cloned().collect();
                    let key = (start_id, end_id, rel_type_id);
                    let full_key = (key.0, key.1, key.2, key_props);
                    if seen_by_type_and_props.contains_key(&full_key) {
                        false
                    } else {
                        seen_by_type_and_props.insert(full_key, ());
                        true
                    }
                }
                Some(RelationshipDeduplication::CreateAll) | None => true,
            };
            
            if should_add {
                // Add relationship
                let rel_idx = self.rels.len();
                self.rels.push((start_id, end_id));
                self.rel_types.push(crate::types::RelationshipType::new(rel_type_id));
                
                // Add properties
                for (prop_key, prop_val) in props {
                    match prop_val {
                        ValueId::I64(v) => {
                            self.rel_col_i64.entry(prop_key).or_insert_with(Vec::new).push((rel_idx, v));
                        }
                        ValueId::F64(bits) => {
                            if let Some(f) = ValueId::F64(bits).to_f64() {
                                self.rel_col_f64.entry(prop_key).or_insert_with(Vec::new).push((rel_idx, f));
                            }
                        }
                        ValueId::Bool(v) => {
                            self.rel_col_bool.entry(prop_key).or_insert_with(Vec::new).push((rel_idx, v));
                        }
                        ValueId::Str(v) => {
                            self.rel_col_str.entry(prop_key).or_insert_with(Vec::new).push((rel_idx, v));
                        }
                    }
                }
                
                rel_ids.push((start_id, end_id));
            }
        }
        
        Ok(rel_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_snapshot::ValueId;
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::Write;
    use flate2::write::GzEncoder;
    use flate2::Compression;

    fn create_test_nodes_csv(temp_dir: &TempDir) -> std::path::PathBuf {
        let file_path = temp_dir.path().join("nodes.csv");
        let mut file = File::create(&file_path).unwrap();
        
        // Write header
        writeln!(file, "id,label,name,age,score,active").unwrap();
        // Write data
        writeln!(file, "1,Person,Alice,30,95.5,true").unwrap();
        writeln!(file, "2,Person,Bob,25,88.0,false").unwrap();
        writeln!(file, "3,Company,Acme,,,").unwrap();
        writeln!(file, "4,Person,Charlie,35,92.5,").unwrap();
        writeln!(file, "5,Company,Beta,40,90.0,false").unwrap();
        
        file_path
    }

    fn create_test_nodes_csv_gz(temp_dir: &TempDir) -> std::path::PathBuf {
        let file_path = temp_dir.path().join("nodes.csv.gz");
        let file = File::create(&file_path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        
        // Write header
        writeln!(encoder, "id,label,name,age,score,active").unwrap();
        // Write data
        writeln!(encoder, "1,Person,Alice,30,95.5,true").unwrap();
        writeln!(encoder, "2,Person,Bob,25,88.0,false").unwrap();
        writeln!(encoder, "3,Company,Acme,,,").unwrap();
        
        encoder.finish().unwrap();
        file_path
    }

    fn create_test_relationships_csv(temp_dir: &TempDir) -> std::path::PathBuf {
        let file_path = temp_dir.path().join("relationships.csv");
        let mut file = File::create(&file_path).unwrap();
        
        // Write header
        writeln!(file, "from,to,type").unwrap();
        // Write data
        writeln!(file, "1,2,KNOWS").unwrap();
        writeln!(file, "2,3,WORKS_FOR").unwrap();
        writeln!(file, "3,4,KNOWS").unwrap();
        writeln!(file, "4,5,WORKS_FOR").unwrap();
        
        file_path
    }

    fn create_test_relationships_csv_with_props(temp_dir: &TempDir) -> std::path::PathBuf {
        let file_path = temp_dir.path().join("relationships_props.csv");
        let mut file = File::create(&file_path).unwrap();
        
        // Write header
        writeln!(file, "from,to,type,weight,count,active").unwrap();
        // Write data
        writeln!(file, "1,2,KNOWS,0.8,5,true").unwrap();
        writeln!(file, "2,3,WORKS_FOR,0.9,10,false").unwrap();
        
        file_path
    }

    #[test]
    fn test_load_nodes_from_csv() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_csv(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_csv(
            nodes_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["name", "age", "score", "active"]),
            None, // unique_properties
            None, // column_types - use auto-detection
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        assert_eq!(node_ids, vec![1, 2, 3, 4, 5]);
        assert_eq!(builder.node_count(), 5);
        
        // Check that properties were loaded
        assert_eq!(builder.prop(1, "name"), Some(ValueId::Str(builder.interner.get_or_intern("Alice"))));
        assert_eq!(builder.prop(1, "age"), Some(ValueId::I64(30)));
        assert_eq!(builder.prop(1, "score"), Some(ValueId::from_f64(95.5)));
        assert_eq!(builder.prop(1, "active"), Some(ValueId::Bool(true)));
        
        // Check node 3 (has empty values)
        assert_eq!(builder.prop(3, "name"), Some(ValueId::Str(builder.interner.get_or_intern("Acme"))));
        // Empty values should not be set
        assert_eq!(builder.prop(3, "age"), None);
    }

    #[test]
    fn test_load_nodes_from_csv_gz() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_csv_gz(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_csv(
            nodes_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["name"]),
            None,
            None, // column_types
        ).unwrap();
        
        assert_eq!(node_ids.len(), 3);
        assert_eq!(node_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_load_nodes_from_csv_auto_id() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_csv(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_csv(
            nodes_path.to_str().unwrap(),
            None, // No ID column - auto-generate
            Some(vec!["label"]),
            Some(vec!["name"]),
            None,
            None, // column_types
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        // Auto-generated IDs start at 0
        assert_eq!(node_ids, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_load_nodes_from_csv_auto_properties() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_csv(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_csv(
            nodes_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            None, // Auto-detect property columns
            None,
            None, // column_types
        ).unwrap();
        
        assert_eq!(node_ids.len(), 5);
        // Should have loaded name, age, score, active (all columns except id and label)
        assert!(builder.prop(1, "name").is_some());
        assert!(builder.prop(1, "age").is_some());
        assert!(builder.prop(1, "score").is_some());
        assert!(builder.prop(1, "active").is_some());
    }

    #[test]
    fn test_load_nodes_from_csv_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nodes_dedup.csv");
        let mut file = File::create(&file_path).unwrap();
        
        writeln!(file, "id,label,name").unwrap();
        writeln!(file, "1,Person,Alice").unwrap();
        writeln!(file, "2,Person,Alice").unwrap(); // Duplicate name
        writeln!(file, "3,Person,Bob").unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["label"]),
            Some(vec!["name"]),
            Some(vec!["name"]), // Use name for deduplication
            None, // column_types
        ).unwrap();
        
        // Should only return unique node IDs (deduplication merges nodes with same name)
        assert_eq!(node_ids.len(), 2);
        // Both "Alice" entries should map to the same node
        assert!(node_ids.contains(&1) || node_ids.contains(&2));
        assert!(node_ids.contains(&3));
    }

    #[test]
    fn test_load_relationships_from_csv() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_csv(&temp_dir);
        
        // First add nodes
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]);
        }
        
        let rel_ids = builder.load_relationships_from_csv(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None, // deduplication
            None, // column_types
        ).unwrap();
        
        assert_eq!(rel_ids.len(), 4);
        assert_eq!(rel_ids, vec![(1, 2), (2, 3), (3, 4), (4, 5)]);
        assert_eq!(builder.rel_count(), 4);
    }

    #[test]
    fn test_load_relationships_from_csv_fixed_type() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_csv(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]);
        }
        
        let rel_ids = builder.load_relationships_from_csv(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            None, // No type column
            None,
            Some("KNOWS"), // Fixed type
            None,
            None, // column_types
        ).unwrap();
        
        assert_eq!(rel_ids.len(), 4);
    }

    #[test]
    fn test_load_relationships_from_csv_with_properties() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_csv_with_props(&temp_dir);
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=4 {
            builder.add_node(Some(i), &["Node"]);
        }
        
        let rel_ids = builder.load_relationships_from_csv(
            rels_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            Some(vec!["weight", "count", "active"]),
            None,
            None,
            None, // column_types
        ).unwrap();
        
        assert_eq!(rel_ids.len(), 2);
        // Properties are stored internally, we can verify the relationships were created
        assert_eq!(builder.rel_count(), 2);
    }

    #[test]
    fn test_load_nodes_from_csv_nonexistent_file() {
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_csv(
            "/nonexistent/file.csv",
            Some("id"),
            None,
            None,
            None,
            None, // column_types
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_from_csv_missing_column() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("bad_rels.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "from,to").unwrap(); // Missing "type" column
        writeln!(file, "1,2").unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]);
        builder.add_node(Some(2), &["Node"]);
        
        let result = builder.load_relationships_from_csv(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"), // Column doesn't exist
            None,
            None,
            None,
            None, // column_types
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_from_csv_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "id,name").unwrap(); // Only header, no data
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name"]),
            None,
            None, // column_types
        ).unwrap();
        
        assert_eq!(node_ids.len(), 0);
    }

    #[test]
    fn test_load_nodes_from_csv_all_property_types() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("all_types.csv");
        let mut file = File::create(&file_path).unwrap();
        
        writeln!(file, "id,name,age,score,active").unwrap();
        writeln!(file, "1,Alice,30,95.5,true").unwrap();
        writeln!(file, "2,Bob,25,88.0,false").unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name", "age", "score", "active"]),
            None,
            None, // column_types
        ).unwrap();
        
        assert_eq!(node_ids.len(), 2);
        
        // Check all property types
        assert_eq!(builder.prop(1, "name"), Some(ValueId::Str(builder.interner.get_or_intern("Alice"))));
        assert_eq!(builder.prop(1, "age"), Some(ValueId::I64(30)));
        assert_eq!(builder.prop(1, "score"), Some(ValueId::from_f64(95.5)));
        assert_eq!(builder.prop(1, "active"), Some(ValueId::Bool(true)));
    }

    #[test]
    fn test_load_relationships_from_csv_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("rels_dedup.csv");
        let mut file = File::create(&file_path).unwrap();
        
        writeln!(file, "from,to,type").unwrap();
        writeln!(file, "1,2,KNOWS").unwrap();
        writeln!(file, "1,2,KNOWS").unwrap(); // Duplicate
        writeln!(file, "1,2,KNOWS").unwrap(); // Duplicate
        writeln!(file, "2,3,WORKS_FOR").unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]);
        }
        
        let rel_ids = builder.load_relationships_from_csv(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            Some(crate::types::RelationshipDeduplication::CreateUniqueByRelType),
            None, // column_types
        ).unwrap();
        
        // Should only have 2 unique relationships (deduplication removes duplicates)
        assert_eq!(rel_ids.len(), 2);
        assert_eq!(builder.rel_count(), 2);
    }

    #[test]
    fn test_load_nodes_from_csv_invalid_id() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("invalid_id.csv");
        let mut file = File::create(&file_path).unwrap();
        
        writeln!(file, "id,name").unwrap();
        writeln!(file, "1,Alice").unwrap();
        writeln!(file, "not_a_number,Bob").unwrap(); // Invalid ID
        
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name"]),
            None,
            None, // column_types
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_from_csv_id_too_large() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("large_id.csv");
        let mut file = File::create(&file_path).unwrap();
        
        writeln!(file, "id,name").unwrap();
        writeln!(file, "1,Alice").unwrap();
        writeln!(file, "999999999999,Bob").unwrap(); // Too large for u32
        
        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name"]),
            None,
            None, // column_types
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_from_csv_explicit_types() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("explicit_types.csv");
        let mut file = File::create(&file_path).unwrap();
        
        // CSV with ambiguous values: "123" could be int or string
        writeln!(file, "id,code,value").unwrap();
        writeln!(file, "1,123,456").unwrap(); // code should be string, value should be int
        writeln!(file, "2,456,789").unwrap();
        
        let mut builder = GraphBuilder::new(None, None);
        
        // Specify types explicitly
        let mut column_types = HashMap::new();
        column_types.insert("code", CsvColumnType::String); // Force string
        column_types.insert("value", CsvColumnType::Int64); // Force int
        
        let node_ids = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["code", "value"]),
            None,
            Some(column_types),
        ).unwrap();
        
        assert_eq!(node_ids.len(), 2);
        
        // Check that "123" was treated as string, not int
        let code_prop = builder.prop(1, "code");
        assert!(code_prop.is_some());
        if let Some(ValueId::Str(code_id)) = code_prop {
            let code_str = builder.interner.resolve(code_id);
            assert_eq!(code_str, "123"); // Should be string "123", not int 123
        } else {
            panic!("code should be a string");
        }
        
        // Check that "456" was treated as int
        let value_prop = builder.prop(1, "value");
        assert_eq!(value_prop, Some(ValueId::I64(456)));
    }
}

