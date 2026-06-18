//! CSV reading support for GraphBuilder
//!
//! Supports both plain CSV and gzip-compressed CSV files (.csv.gz)

use crate::error::{GraphError, Result};
use crate::graph_builder::GraphBuilder;
use crate::graph_snapshot::ValueId;
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use hashbrown::HashMap;
use roaring::RoaringBitmap;
use std::fs::File;
use std::io::{BufReader, Read};

/// Helper to create a CSV reader from a file path
/// Handles both plain CSV and gzip-compressed CSV (.csv.gz)
fn create_csv_reader(path: &str, delimiter: u8) -> Result<csv::Reader<Box<dyn Read>>> {
    let file = File::open(path)
        .map_err(|e| GraphError::CsvError(format!("Failed to open CSV file {}: {}", path, e)))?;

    let reader: Box<dyn Read> = if path.ends_with(".gz") || path.ends_with(".csv.gz") {
        Box::new(GzDecoder::new(BufReader::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    let csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(delimiter)
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
                ValueId::Str(builder.interner.get_or_intern(value))
            }
        }
        CsvColumnType::Float64 => {
            if let Ok(f) = value.parse::<f64>() {
                ValueId::from_f64(f)
            } else {
                ValueId::Str(builder.interner.get_or_intern(value))
            }
        }
        CsvColumnType::Bool => {
            if let Ok(b) = value.parse::<bool>() {
                ValueId::Bool(b)
            } else {
                ValueId::Str(builder.interner.get_or_intern(value))
            }
        }
        CsvColumnType::String => ValueId::Str(builder.interner.get_or_intern(value)),
        CsvColumnType::Auto => {
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

/// Validate that all requested column names exist in the CSV headers.
/// Returns `SchemaError` listing any columns not found.
fn validate_columns_exist(headers: &[String], columns: &[&str], context: &str) -> Result<()> {
    let missing: Vec<&str> = columns
        .iter()
        .filter(|col| !headers.iter().any(|h| h == **col))
        .copied()
        .collect();
    if !missing.is_empty() {
        return Err(GraphError::SchemaError(format!(
            "{} column(s) not found in CSV headers: {}",
            context,
            missing.join(", ")
        )));
    }
    Ok(())
}

/// Find the index of a required column, returning `SchemaError` if not found.
fn require_column_index(headers: &[String], column: &str, context: &str) -> Result<usize> {
    headers.iter().position(|h| h == column).ok_or_else(|| {
        GraphError::SchemaError(format!(
            "{} column '{}' not found in CSV headers",
            context, column
        ))
    })
}

/// Generate the next auto-incremented node ID, returning `CapacityError` on overflow.
fn next_auto_node_id(builder: &mut GraphBuilder) -> Result<u32> {
    let id = builder.next_node_id;
    builder.next_node_id = id.checked_add(1).ok_or_else(|| {
        GraphError::CapacityError("Node ID counter exceeded u32::MAX".to_string())
    })?;
    Ok(id)
}

/// Set properties on a node from parsed CSV column values.
fn set_node_properties(
    builder: &mut GraphBuilder,
    node_id: u32,
    record: &csv::StringRecord,
    prop_indices: &[(usize, String)],
    column_types: &Option<HashMap<&str, CsvColumnType>>,
) -> Result<()> {
    for (idx, prop_name) in prop_indices {
        if let Some(val_str) = record.get(*idx) {
            if !val_str.is_empty() {
                let col_type = column_types
                    .as_ref()
                    .and_then(|types| types.get(prop_name.as_str()))
                    .copied()
                    .unwrap_or(CsvColumnType::Auto);
                let val = parse_csv_value(val_str, builder, col_type);
                match val {
                    ValueId::I64(v) => {
                        builder.set_prop_i64(node_id, prop_name, v)?;
                    }
                    ValueId::F64(bits) => {
                        builder.set_prop_f64(node_id, prop_name, f64::from_bits(bits))?;
                    }
                    ValueId::Bool(v) => {
                        builder.set_prop_bool(node_id, prop_name, v)?;
                    }
                    ValueId::Str(v) => {
                        let s = builder.interner.resolve(v);
                        builder.set_prop_str(node_id, prop_name, s.as_str())?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Resolve interned label IDs back to string refs and call add_node.
fn add_node_with_interned_labels(
    builder: &mut GraphBuilder,
    node_id: u32,
    labels: &[u32],
) -> Result<u32> {
    let label_strings: Vec<String> = labels
        .iter()
        .map(|&l| builder.interner.resolve(l))
        .collect();
    let label_refs: Vec<&str> = label_strings.iter().map(|s| s.as_str()).collect();
    builder.add_node(Some(node_id), &label_refs)
}

/// Context for processing a single node row.
struct NodeRowContext<'a> {
    node_id_idx: Option<usize>,
    label_indices: &'a [usize],
    /// Interned id of a fixed label applied to every row (e.g. the entity type
    /// when the label is the file, not a column), or `None`.
    default_label_id: Option<u32>,
    prop_indices: &'a [(usize, String)],
    unique_prop_indices: &'a [(usize, String)],
    column_types: &'a Option<HashMap<&'a str, CsvColumnType>>,
}

/// Result of processing a single node row.
enum NodeRowResult {
    /// A node was created or found with this ID; true if new to the batch.
    Node { id: u32, is_new: bool },
}

/// Process a single CSV row for node loading.
fn process_node_row(
    builder: &mut GraphBuilder,
    record: &csv::StringRecord,
    row_num: usize,
    ctx: &NodeRowContext,
    seen_node_ids: &mut RoaringBitmap,
) -> Result<NodeRowResult> {
    // Extract node ID
    let node_id = extract_node_id(record, ctx.node_id_idx, row_num)?;

    // Extract labels (column-valued labels plus any fixed default label).
    let mut labels = extract_labels(builder, record, ctx.label_indices);
    if let Some(dl) = ctx.default_label_id {
        labels.push(dl);
    }

    // Extract dedup key
    let dedup_key = extract_dedup_key(builder, record, ctx.unique_prop_indices, ctx.column_types);

    // Deduplication path
    if let Some(ref dk) = dedup_key {
        if let Some(&existing_id) = builder.dedup_map.get(dk) {
            if !labels.is_empty() {
                add_node_with_interned_labels(builder, existing_id, &labels)?;
            }
            let is_new = !seen_node_ids.contains(existing_id);
            if is_new {
                seen_node_ids.insert(existing_id);
            }
            return Ok(NodeRowResult::Node {
                id: existing_id,
                is_new,
            });
        }

        let new_id = match node_id {
            Some(id) => id,
            None => next_auto_node_id(builder)?,
        };

        builder.dedup_map.insert(dk.clone(), new_id);
        add_node_with_interned_labels(builder, new_id, &labels)?;
        set_node_properties(builder, new_id, record, ctx.prop_indices, ctx.column_types)?;
        seen_node_ids.insert(new_id);
        return Ok(NodeRowResult::Node {
            id: new_id,
            is_new: true,
        });
    }

    // No deduplication path
    let new_id = match node_id {
        Some(id) => id,
        None => next_auto_node_id(builder)?,
    };

    if seen_node_ids.contains(new_id) {
        if !labels.is_empty() {
            add_node_with_interned_labels(builder, new_id, &labels)?;
        }
        return Ok(NodeRowResult::Node {
            id: new_id,
            is_new: false,
        });
    }

    seen_node_ids.insert(new_id);
    add_node_with_interned_labels(builder, new_id, &labels)?;
    set_node_properties(builder, new_id, record, ctx.prop_indices, ctx.column_types)?;
    Ok(NodeRowResult::Node {
        id: new_id,
        is_new: true,
    })
}

/// Extract the node ID from a CSV record, parsing and validating it.
fn extract_node_id(
    record: &csv::StringRecord,
    node_id_idx: Option<usize>,
    row_num: usize,
) -> Result<Option<u32>> {
    if let Some(idx) = node_id_idx {
        let id_str = record.get(idx).ok_or_else(|| {
            GraphError::CsvError(format!("Missing node ID column at row {}", row_num + 1))
        })?;
        if id_str.is_empty() {
            Ok(None)
        } else {
            let id_val = id_str.parse::<i64>().map_err(|e| {
                GraphError::CsvError(format!(
                    "Invalid node ID '{}' at row {}: {}",
                    id_str,
                    row_num + 1,
                    e
                ))
            })?;
            if id_val < 0 || id_val > u32::MAX as i64 {
                return Err(GraphError::CapacityError(format!(
                    "Node ID {} at row {} exceeds u32 range",
                    id_val,
                    row_num + 1
                )));
            }
            Ok(Some(id_val as u32))
        }
    } else {
        Ok(None)
    }
}

/// Extract interned label IDs from the record at the given column indices.
fn extract_labels(
    builder: &mut GraphBuilder,
    record: &csv::StringRecord,
    label_indices: &[usize],
) -> Vec<u32> {
    let mut labels = Vec::new();
    for idx in label_indices {
        if let Some(label_str) = record.get(*idx) {
            if !label_str.is_empty() {
                labels.push(builder.interner.get_or_intern(label_str));
            }
        }
    }
    labels
}

/// Extract dedup key from the record if unique_prop_indices is non-empty.
fn extract_dedup_key(
    builder: &mut GraphBuilder,
    record: &csv::StringRecord,
    unique_prop_indices: &[(usize, String)],
    column_types: &Option<HashMap<&str, CsvColumnType>>,
) -> Option<crate::types::DedupKey> {
    if unique_prop_indices.is_empty() {
        return None;
    }
    let mut dedup_values = Vec::new();
    for (idx, prop_name) in unique_prop_indices {
        if let Some(val_str) = record.get(*idx) {
            if !val_str.is_empty() {
                let col_type = column_types
                    .as_ref()
                    .and_then(|types| types.get(prop_name.as_str()))
                    .copied()
                    .unwrap_or(CsvColumnType::Auto);
                let val = parse_csv_value(val_str, builder, col_type);
                dedup_values.push(val);
            } else {
                return None;
            }
        } else {
            return None;
        }
    }
    if dedup_values.is_empty() {
        None
    } else {
        Some(crate::types::DedupKey::from_slice(&dedup_values))
    }
}

/// Check whether a node ID has been registered in the builder.
fn node_exists_in_builder(builder: &GraphBuilder, node_id: u32) -> bool {
    builder.known_nodes.contains(node_id)
}

/// How to resolve a relationship endpoint to an internal node id for one CSV column set.
///
/// Mirrors the Parquet loader's `NodeReference` handling: an `Id` column is parsed directly
/// as a `u32` node id, while property references resolve through an index built over the
/// already-loaded nodes (`build_property_index` / `build_composite_property_index`). The
/// indexes are owned here so they don't borrow the builder during row processing.
enum RefIndex {
    /// Column holds the node id directly (parsed as `u32`).
    Id { col: usize },
    /// Single-property lookup: parse the column cell, look it up in the property index.
    Single {
        col: usize,
        ty: CsvColumnType,
        index: HashMap<ValueId, Vec<u32>>,
    },
    /// Composite-property lookup: parse each column cell, look up the combined key.
    Composite {
        cols: Vec<usize>,
        tys: Vec<CsvColumnType>,
        index: HashMap<crate::types::DedupKey, Vec<u32>>,
    },
}

/// Column names referenced by a `NodeReference`, used to exclude endpoint columns from
/// auto-detected relationship property columns.
fn ref_column_names(node_ref: &crate::types::NodeReference) -> Vec<&str> {
    use crate::types::NodeReference;
    match node_ref {
        NodeReference::Id(c) => vec![c.as_str()],
        NodeReference::Property { column, .. } => vec![column.as_str()],
        NodeReference::CompositeProperty { columns, .. } => {
            columns.iter().map(|s| s.as_str()).collect()
        }
    }
}

/// Resolve the endpoint columns declared by a `NodeReference` to header indices, building the
/// property index needed for property-based references. `ctx` labels the endpoint
/// ("Start node"/"End node") for not-found errors.
fn build_ref_index(
    builder: &GraphBuilder,
    headers: &[String],
    node_ref: &crate::types::NodeReference,
    ctx: &str,
    column_types: &Option<HashMap<&str, CsvColumnType>>,
) -> Result<RefIndex> {
    use crate::types::NodeReference;

    let col_type = |name: &str| {
        column_types
            .as_ref()
            .and_then(|m| m.get(name).copied())
            .unwrap_or(CsvColumnType::Auto)
    };

    match node_ref {
        NodeReference::Id(column) => {
            let col = require_column_index(headers, column, ctx)?;
            Ok(RefIndex::Id { col })
        }
        NodeReference::Property {
            column,
            property_key,
            label,
        } => {
            let col = require_column_index(headers, column, ctx)?;
            let index = builder.build_property_index(property_key, label.as_deref());
            Ok(RefIndex::Single {
                col,
                ty: col_type(column),
                index,
            })
        }
        NodeReference::CompositeProperty {
            columns,
            property_keys,
            label,
        } => {
            let cols = columns
                .iter()
                .map(|c| require_column_index(headers, c, ctx))
                .collect::<Result<Vec<_>>>()?;
            let tys = columns.iter().map(|c| col_type(c)).collect();
            let keys: Vec<&str> = property_keys.iter().map(|s| s.as_str()).collect();
            let index = builder.build_composite_property_index(&keys, label.as_deref());
            Ok(RefIndex::Composite { cols, tys, index })
        }
    }
}

/// Resolve a single relationship endpoint for one CSV row to an internal node id.
///
/// Returns `Ok(None)` when a property reference cannot be matched (empty cell or no node with
/// that property value) so the caller can skip the row. `Id` references parse the column as a
/// `u32` and error on malformed input, preserving the original loader behavior.
fn resolve_ref_node(
    builder: &mut GraphBuilder,
    record: &csv::StringRecord,
    ref_index: &RefIndex,
    side: &str,
    row_num: usize,
) -> Result<Option<u32>> {
    match ref_index {
        RefIndex::Id { col } => {
            let s = record.get(*col).ok_or_else(|| {
                GraphError::CsvError(format!(
                    "Missing {} node column at row {}",
                    side,
                    row_num + 1
                ))
            })?;
            let id = s.parse::<u32>().map_err(|e| {
                GraphError::CsvError(format!(
                    "Invalid {} node ID '{}' at row {}: {}",
                    side,
                    s,
                    row_num + 1,
                    e
                ))
            })?;
            Ok(Some(id))
        }
        RefIndex::Single { col, ty, index } => {
            let s = match record.get(*col) {
                Some(s) if !s.is_empty() => s,
                _ => return Ok(None),
            };
            let value_id = parse_csv_value(s, builder, *ty);
            Ok(index.get(&value_id).and_then(|ids| ids.first().copied()))
        }
        RefIndex::Composite { cols, tys, index } => {
            let mut values: Vec<ValueId> = Vec::with_capacity(cols.len());
            for (col, ty) in cols.iter().zip(tys.iter()) {
                let s = match record.get(*col) {
                    Some(s) if !s.is_empty() => s,
                    _ => return Ok(None),
                };
                values.push(parse_csv_value(s, builder, *ty));
            }
            let key = crate::types::DedupKey::from_slice(&values);
            Ok(index.get(&key).and_then(|ids| ids.first().copied()))
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
    #[allow(clippy::too_many_arguments)]
    pub fn load_nodes_from_csv(
        &mut self,
        path: &str,
        node_id_column: Option<&str>,
        label_columns: Option<Vec<&str>>,
        property_columns: Option<Vec<&str>>,
        unique_properties: Option<Vec<&str>>,
        column_types: Option<HashMap<&str, CsvColumnType>>,
        default_label: Option<&str>,
        delimiter: u8,
    ) -> Result<Vec<u32>> {
        let mut reader = create_csv_reader(path, delimiter)?;

        // Get headers
        let headers = reader
            .headers()
            .map_err(|e| GraphError::CsvError(format!("Failed to read CSV headers: {}", e)))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        // Validate column names against headers
        if let Some(id_col) = node_id_column {
            validate_columns_exist(&headers, &[id_col], "Node ID")?;
        }
        let label_cols = label_columns.unwrap_or_default();
        if !label_cols.is_empty() {
            validate_columns_exist(&headers, &label_cols, "Label")?;
        }
        if let Some(ref prop_cols) = property_columns {
            validate_columns_exist(&headers, prop_cols, "Property")?;
        }
        if let Some(ref unique_props) = unique_properties {
            validate_columns_exist(&headers, unique_props, "Unique property")?;
        }

        // Configure deduplication if unique_properties is provided
        if let Some(ref unique_props) = unique_properties {
            self.enable_node_deduplication(unique_props.clone());
        }

        // Determine which columns to use for properties
        let prop_cols = property_columns.unwrap_or_else(|| {
            headers
                .iter()
                .filter(|col| {
                    node_id_column
                        .map(|id_col| col.as_str() != id_col)
                        .unwrap_or(true)
                        && !label_cols.contains(&col.as_str())
                })
                .map(|s| s.as_str())
                .collect()
        });

        // Find column indices
        let node_id_idx = node_id_column.and_then(|col| headers.iter().position(|h| h == col));

        let label_indices: Vec<usize> = label_cols
            .iter()
            .filter_map(|col| headers.iter().position(|h| h == col))
            .collect();

        let prop_indices: Vec<(usize, String)> = prop_cols
            .iter()
            .filter_map(|col| {
                headers
                    .iter()
                    .position(|h| h == col)
                    .map(|idx| (idx, col.to_string()))
            })
            .collect();

        let unique_prop_indices: Vec<(usize, String)> = unique_properties
            .as_ref()
            .map(|props| {
                props
                    .iter()
                    .filter_map(|col| {
                        headers
                            .iter()
                            .position(|h| h == col)
                            .map(|idx| (idx, col.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let default_label_id = default_label.map(|l| self.interner.get_or_intern(l));

        let ctx = NodeRowContext {
            node_id_idx,
            label_indices: &label_indices,
            default_label_id,
            prop_indices: &prop_indices,
            unique_prop_indices: &unique_prop_indices,
            column_types: &column_types,
        };

        // Process rows
        let mut node_ids = Vec::new();
        let mut seen_node_ids = RoaringBitmap::new();
        let mut row_num = 0;

        for result in reader.records() {
            let record = result.map_err(|e| {
                GraphError::CsvError(format!(
                    "Failed to read CSV record at row {}: {}",
                    row_num + 2,
                    e
                ))
            })?;

            row_num += 1;

            match process_node_row(self, &record, row_num, &ctx, &mut seen_node_ids)? {
                NodeRowResult::Node { id, is_new } => {
                    if is_new {
                        node_ids.push(id);
                    }
                }
            }
        }

        Ok(node_ids)
    }

    /// Load relationships from a CSV file into the builder
    ///
    /// # Arguments
    /// * `path` - Path to CSV file (supports .csv and .csv.gz)
    /// * `start_node_ref` - How to identify start nodes: a bare column name (`&str`/`String`)
    ///   for a direct id column, or a [`crate::types::NodeReference`] for property-based lookup
    /// * `end_node_ref` - How to identify end nodes (same forms as `start_node_ref`)
    /// * `rel_type_column` - Optional column name for relationship type. If None, `fixed_rel_type` must be provided.
    /// * `property_columns` - Optional list of column names to use as properties. If None, uses all columns except the endpoint/type columns.
    /// * `fixed_rel_type` - Optional fixed relationship type to use for all relationships. Required if `rel_type_column` is None.
    /// * `deduplication` - Optional deduplication strategy for relationships
    /// * `column_types` - Optional map of column names to types. If not specified, uses heuristic parsing (Auto).
    ///
    /// Property-based references resolve through an index over the already-loaded nodes, so the
    /// nodes must be loaded before their relationships. This lets CSV relationships reference
    /// nodes whose external ids are i64 or collide across types (e.g. LDBC), where the raw id
    /// column cannot be used directly as the internal `u32` node id.
    #[allow(clippy::too_many_arguments)]
    pub fn load_relationships_from_csv(
        &mut self,
        path: &str,
        start_node_ref: impl Into<crate::types::NodeReference>,
        end_node_ref: impl Into<crate::types::NodeReference>,
        rel_type_column: Option<&str>,
        property_columns: Option<Vec<&str>>,
        fixed_rel_type: Option<&str>,
        deduplication: Option<crate::types::RelationshipDeduplication>,
        column_types: Option<HashMap<&str, CsvColumnType>>,
        delimiter: u8,
    ) -> Result<Vec<(u32, u32)>> {
        // Accept a bare column name (`From<&str>`) or an explicit reference spec.
        let start_ref = start_node_ref.into();
        let end_ref = end_node_ref.into();

        let mut reader = create_csv_reader(path, delimiter)?;

        // Get headers
        let headers = reader
            .headers()
            .map_err(|e| GraphError::CsvError(format!("Failed to read CSV headers: {}", e)))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        let rel_type_idx = match rel_type_column {
            Some(col) => Some(require_column_index(&headers, col, "Relationship type")?),
            None => None,
        };

        if rel_type_column.is_none() && fixed_rel_type.is_none() {
            return Err(GraphError::SchemaError(
                "Either rel_type_column or fixed_rel_type must be provided".to_string(),
            ));
        }

        // Validate property columns if explicitly provided
        if let Some(ref prop_cols) = property_columns {
            validate_columns_exist(&headers, prop_cols, "Relationship property")?;
        }

        // Columns consumed as endpoints or relationship type are excluded from
        // auto-detected property columns.
        let start_cols = ref_column_names(&start_ref);
        let end_cols = ref_column_names(&end_ref);
        let mut reserved: Vec<&str> = Vec::new();
        reserved.extend_from_slice(&start_cols);
        reserved.extend_from_slice(&end_cols);
        if let Some(rt_col) = rel_type_column {
            reserved.push(rt_col);
        }

        // Determine property columns
        let prop_cols = property_columns.unwrap_or_else(|| {
            headers
                .iter()
                .filter(|col| !reserved.contains(&col.as_str()))
                .map(|s| s.as_str())
                .collect()
        });

        let prop_indices: Vec<(usize, String)> = prop_cols
            .iter()
            .filter_map(|col| {
                headers
                    .iter()
                    .position(|h| h == col)
                    .map(|idx| (idx, col.to_string()))
            })
            .collect();

        // Build endpoint resolvers (and any property indexes) once, up front. This also
        // validates that the referenced columns exist (SchemaError otherwise).
        let start_index = build_ref_index(self, &headers, &start_ref, "Start node", &column_types)?;
        let end_index = build_ref_index(self, &headers, &end_ref, "End node", &column_types)?;

        // Set up deduplication tracking
        let mut seen_by_type: HashMap<(u32, u32, u32), ()> = HashMap::new();
        let mut seen_by_type_and_props: HashMap<(u32, u32, u32, Vec<ValueId>), ()> = HashMap::new();

        // Process rows
        let mut rel_ids = Vec::new();
        let mut row_num = 0;
        let mut skipped_missing_nodes = 0u64;

        for result in reader.records() {
            let record = result.map_err(|e| {
                GraphError::CsvError(format!(
                    "Failed to read CSV record at row {}: {}",
                    row_num + 2,
                    e
                ))
            })?;

            row_num += 1;

            let start_id = match resolve_ref_node(self, &record, &start_index, "start", row_num)? {
                Some(id) => id,
                None => {
                    skipped_missing_nodes += 1;
                    continue;
                }
            };
            let end_id = match resolve_ref_node(self, &record, &end_index, "end", row_num)? {
                Some(id) => id,
                None => {
                    skipped_missing_nodes += 1;
                    continue;
                }
            };

            // Validate that referenced nodes exist (relevant for id columns; property
            // lookups only ever yield existing nodes).
            if !node_exists_in_builder(self, start_id) || !node_exists_in_builder(self, end_id) {
                skipped_missing_nodes += 1;
                continue;
            }

            if let Some(pair) = process_rel_row(
                self,
                &record,
                row_num,
                start_id,
                end_id,
                rel_type_idx,
                fixed_rel_type,
                &prop_indices,
                &column_types,
                &deduplication,
                &mut seen_by_type,
                &mut seen_by_type_and_props,
            )? {
                rel_ids.push(pair);
            }
        }

        if skipped_missing_nodes > 0 {
            eprintln!(
                "Warning: skipped {} relationship(s) referencing non-existent nodes",
                skipped_missing_nodes
            );
        }

        Ok(rel_ids)
    }
}

/// Process a single CSV row for relationship loading, given already-resolved endpoint ids.
/// Returns `Some((start_id, end_id))` if the relationship was added, `None` if deduplicated away.
#[allow(clippy::too_many_arguments)]
fn process_rel_row(
    builder: &mut GraphBuilder,
    record: &csv::StringRecord,
    row_num: usize,
    start_id: u32,
    end_id: u32,
    rel_type_idx: Option<usize>,
    fixed_rel_type: Option<&str>,
    prop_indices: &[(usize, String)],
    column_types: &Option<HashMap<&str, CsvColumnType>>,
    deduplication: &Option<crate::types::RelationshipDeduplication>,
    seen_by_type: &mut HashMap<(u32, u32, u32), ()>,
    seen_by_type_and_props: &mut HashMap<(u32, u32, u32, Vec<ValueId>), ()>,
) -> Result<Option<(u32, u32)>> {
    // Extract relationship type
    let rel_type = if let Some(idx) = rel_type_idx {
        record
            .get(idx)
            .ok_or_else(|| {
                GraphError::CsvError(format!(
                    "Missing relationship type column at row {}",
                    row_num + 1
                ))
            })?
            .to_string()
    } else {
        fixed_rel_type.unwrap().to_string()
    };

    let rel_type_id = builder.interner.get_or_intern(&rel_type);

    // Extract properties
    let mut props = HashMap::new();
    for (idx, prop_name) in prop_indices {
        if let Some(val_str) = record.get(*idx) {
            if !val_str.is_empty() {
                let col_type = column_types
                    .as_ref()
                    .and_then(|types| types.get(prop_name.as_str()))
                    .copied()
                    .unwrap_or(CsvColumnType::Auto);
                let val = parse_csv_value(val_str, builder, col_type);
                let prop_key = builder.interner.get_or_intern(prop_name);
                props.insert(prop_key, val);
            }
        }
    }

    use crate::types::RelationshipDeduplication;

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
            let mut sorted_keys: Vec<u32> = props.keys().copied().collect();
            sorted_keys.sort();
            let key_props: Vec<ValueId> = sorted_keys.iter().map(|k| props[k]).collect();
            let full_key = (start_id, end_id, rel_type_id, key_props);
            if seen_by_type_and_props.contains_key(&full_key) {
                false
            } else {
                seen_by_type_and_props.insert(full_key, ());
                true
            }
        }
        Some(RelationshipDeduplication::CreateAll) | None => true,
    };

    if !should_add {
        return Ok(None);
    }

    // Add relationship via add_relationship() to ensure deg_out/deg_in and known_nodes are updated
    let rel_idx = builder.add_relationship(start_id, end_id, &rel_type)?;

    // Add properties
    for (prop_key, prop_val) in props {
        match prop_val {
            ValueId::I64(v) => {
                builder
                    .rel_col_i64
                    .entry(prop_key)
                    .or_insert_with(Vec::new)
                    .push((rel_idx, v));
            }
            ValueId::F64(bits) => {
                builder
                    .rel_col_f64
                    .entry(prop_key)
                    .or_insert_with(Vec::new)
                    .push((rel_idx, f64::from_bits(bits)));
            }
            ValueId::Bool(v) => {
                builder
                    .rel_col_bool
                    .entry(prop_key)
                    .or_insert_with(Vec::new)
                    .push((rel_idx, v));
            }
            ValueId::Str(v) => {
                builder
                    .rel_col_str
                    .entry(prop_key)
                    .or_insert_with(Vec::new)
                    .push((rel_idx, v));
            }
        }
    }

    Ok(Some((start_id, end_id)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_snapshot::ValueId;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_nodes_csv(temp_dir: &TempDir) -> std::path::PathBuf {
        let file_path = temp_dir.path().join("nodes.csv");
        let mut file = File::create(&file_path).unwrap();

        writeln!(file, "id,label,name,age,score,active").unwrap();
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

        writeln!(encoder, "id,label,name,age,score,active").unwrap();
        writeln!(encoder, "1,Person,Alice,30,95.5,true").unwrap();
        writeln!(encoder, "2,Person,Bob,25,88.0,false").unwrap();
        writeln!(encoder, "3,Company,Acme,,,").unwrap();

        encoder.finish().unwrap();
        file_path
    }

    fn create_test_relationships_csv(temp_dir: &TempDir) -> std::path::PathBuf {
        let file_path = temp_dir.path().join("relationships.csv");
        let mut file = File::create(&file_path).unwrap();

        writeln!(file, "from,to,type").unwrap();
        writeln!(file, "1,2,KNOWS").unwrap();
        writeln!(file, "2,3,WORKS_FOR").unwrap();
        writeln!(file, "3,4,KNOWS").unwrap();
        writeln!(file, "4,5,WORKS_FOR").unwrap();

        file_path
    }

    fn create_test_relationships_csv_with_props(temp_dir: &TempDir) -> std::path::PathBuf {
        let file_path = temp_dir.path().join("relationships_props.csv");
        let mut file = File::create(&file_path).unwrap();

        writeln!(file, "from,to,type,weight,count,active").unwrap();
        writeln!(file, "1,2,KNOWS,0.8,5,true").unwrap();
        writeln!(file, "2,3,WORKS_FOR,0.9,10,false").unwrap();

        file_path
    }

    #[test]
    fn test_load_nodes_from_csv() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_csv(&temp_dir);

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder
            .load_nodes_from_csv(
                nodes_path.to_str().unwrap(),
                Some("id"),
                Some(vec!["label"]),
                Some(vec!["name", "age", "score", "active"]),
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(node_ids.len(), 5);
        assert_eq!(node_ids, vec![1, 2, 3, 4, 5]);
        assert_eq!(builder.node_count(), 5);

        assert_eq!(
            builder.prop(1, "name"),
            Some(ValueId::Str(builder.interner.get_or_intern("Alice")))
        );
        assert_eq!(builder.prop(1, "age"), Some(ValueId::I64(30)));
        assert_eq!(builder.prop(1, "score"), Some(ValueId::from_f64(95.5)));
        assert_eq!(builder.prop(1, "active"), Some(ValueId::Bool(true)));

        assert_eq!(
            builder.prop(3, "name"),
            Some(ValueId::Str(builder.interner.get_or_intern("Acme")))
        );
        assert_eq!(builder.prop(3, "age"), None);
    }

    #[test]
    fn test_load_nodes_from_csv_gz() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_csv_gz(&temp_dir);

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder
            .load_nodes_from_csv(
                nodes_path.to_str().unwrap(),
                Some("id"),
                Some(vec!["label"]),
                Some(vec!["name"]),
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(node_ids.len(), 3);
        assert_eq!(node_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_load_nodes_from_csv_auto_id() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_csv(&temp_dir);

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder
            .load_nodes_from_csv(
                nodes_path.to_str().unwrap(),
                None,
                Some(vec!["label"]),
                Some(vec!["name"]),
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(node_ids.len(), 5);
        assert_eq!(node_ids, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_load_nodes_from_csv_auto_properties() {
        let temp_dir = TempDir::new().unwrap();
        let nodes_path = create_test_nodes_csv(&temp_dir);

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder
            .load_nodes_from_csv(
                nodes_path.to_str().unwrap(),
                Some("id"),
                Some(vec!["label"]),
                None,
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(node_ids.len(), 5);
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
        writeln!(file, "2,Person,Alice").unwrap();
        writeln!(file, "3,Person,Bob").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder
            .load_nodes_from_csv(
                file_path.to_str().unwrap(),
                Some("id"),
                Some(vec!["label"]),
                Some(vec!["name"]),
                Some(vec!["name"]),
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(node_ids.len(), 2);
        assert!(node_ids.contains(&1) || node_ids.contains(&2));
        assert!(node_ids.contains(&3));
    }

    #[test]
    fn test_load_relationships_from_csv() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_csv(&temp_dir);

        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }

        let rel_ids = builder
            .load_relationships_from_csv(
                rels_path.to_str().unwrap(),
                "from",
                "to",
                Some("type"),
                None,
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(rel_ids.len(), 4);
        assert_eq!(rel_ids, vec![(1, 2), (2, 3), (3, 4), (4, 5)]);
        assert_eq!(builder.relationship_count(), 4);
    }

    #[test]
    fn test_load_relationships_from_csv_fixed_type() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_csv(&temp_dir);

        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=5 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }

        let rel_ids = builder
            .load_relationships_from_csv(
                rels_path.to_str().unwrap(),
                "from",
                "to",
                None,
                None,
                Some("KNOWS"),
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(rel_ids.len(), 4);
    }

    #[test]
    fn test_load_relationships_from_csv_with_properties() {
        let temp_dir = TempDir::new().unwrap();
        let rels_path = create_test_relationships_csv_with_props(&temp_dir);

        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=4 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }

        let rel_ids = builder
            .load_relationships_from_csv(
                rels_path.to_str().unwrap(),
                "from",
                "to",
                Some("type"),
                Some(vec!["weight", "count", "active"]),
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(rel_ids.len(), 2);
        assert_eq!(builder.relationship_count(), 2);
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
            None,
            None,
            b',',
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_relationships_from_csv_missing_column() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("bad_rels.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "from,to").unwrap();
        writeln!(file, "1,2").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();

        let result = builder.load_relationships_from_csv(
            file_path.to_str().unwrap(),
            "from",
            "to",
            Some("type"),
            None,
            None,
            None,
            None,
            b',',
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_from_csv_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "id,name").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let node_ids = builder
            .load_nodes_from_csv(
                file_path.to_str().unwrap(),
                Some("id"),
                None,
                Some(vec!["name"]),
                None,
                None,
                None,
                b',',
            )
            .unwrap();

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
        let node_ids = builder
            .load_nodes_from_csv(
                file_path.to_str().unwrap(),
                Some("id"),
                None,
                Some(vec!["name", "age", "score", "active"]),
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(node_ids.len(), 2);

        assert_eq!(
            builder.prop(1, "name"),
            Some(ValueId::Str(builder.interner.get_or_intern("Alice")))
        );
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
        writeln!(file, "1,2,KNOWS").unwrap();
        writeln!(file, "1,2,KNOWS").unwrap();
        writeln!(file, "2,3,WORKS_FOR").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        for i in 1..=3 {
            builder.add_node(Some(i), &["Node"]).unwrap();
        }

        let rel_ids = builder
            .load_relationships_from_csv(
                file_path.to_str().unwrap(),
                "from",
                "to",
                Some("type"),
                None,
                None,
                Some(crate::types::RelationshipDeduplication::CreateUniqueByRelType),
                None,
                b',',
            )
            .unwrap();

        assert_eq!(rel_ids.len(), 2);
        assert_eq!(builder.relationship_count(), 2);
    }

    #[test]
    fn test_load_nodes_from_csv_invalid_id() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("invalid_id.csv");
        let mut file = File::create(&file_path).unwrap();

        writeln!(file, "id,name").unwrap();
        writeln!(file, "1,Alice").unwrap();
        writeln!(file, "not_a_number,Bob").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name"]),
            None,
            None,
            None,
            b',',
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
        writeln!(file, "999999999999,Bob").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            None,
            Some(vec!["name"]),
            None,
            None,
            None,
            b',',
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_nodes_from_csv_explicit_types() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("explicit_types.csv");
        let mut file = File::create(&file_path).unwrap();

        writeln!(file, "id,code,value").unwrap();
        writeln!(file, "1,123,456").unwrap();
        writeln!(file, "2,456,789").unwrap();

        let mut builder = GraphBuilder::new(None, None);

        let mut column_types = HashMap::new();
        column_types.insert("code", CsvColumnType::String);
        column_types.insert("value", CsvColumnType::Int64);

        let node_ids = builder
            .load_nodes_from_csv(
                file_path.to_str().unwrap(),
                Some("id"),
                None,
                Some(vec!["code", "value"]),
                None,
                Some(column_types),
                None,
                b',',
            )
            .unwrap();

        assert_eq!(node_ids.len(), 2);

        let code_prop = builder.prop(1, "code");
        assert!(code_prop.is_some());
        if let Some(ValueId::Str(code_id)) = code_prop {
            let code_str = builder.interner.resolve(code_id);
            assert_eq!(code_str, "123");
        } else {
            panic!("code should be a string");
        }

        let value_prop = builder.prop(1, "value");
        assert_eq!(value_prop, Some(ValueId::I64(456)));
    }

    #[test]
    fn test_load_nodes_from_csv_schema_error_bad_id_column() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("schema.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "id,name").unwrap();
        writeln!(file, "1,Alice").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("nonexistent_id"),
            None,
            None,
            None,
            None,
            None,
            b',',
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found"),
            "Expected schema error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_nodes_from_csv_schema_error_bad_label_column() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("schema_label.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "id,name").unwrap();
        writeln!(file, "1,Alice").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_nodes_from_csv(
            file_path.to_str().unwrap(),
            Some("id"),
            Some(vec!["missing_label"]),
            None,
            None,
            None,
            None,
            b',',
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found"),
            "Expected schema error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_relationships_from_csv_missing_node() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("rels_missing_node.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "from,to,type").unwrap();
        writeln!(file, "1,2,KNOWS").unwrap();
        writeln!(file, "1,999,KNOWS").unwrap(); // node 999 does not exist

        let mut builder = GraphBuilder::new(None, None);
        builder.add_node(Some(1), &["Node"]).unwrap();
        builder.add_node(Some(2), &["Node"]).unwrap();

        let rel_ids = builder
            .load_relationships_from_csv(
                file_path.to_str().unwrap(),
                "from",
                "to",
                Some("type"),
                None,
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        // Only the first relationship should be added; the second references non-existent node 999
        assert_eq!(rel_ids.len(), 1);
        assert_eq!(rel_ids[0], (1, 2));
    }

    #[test]
    fn test_load_relationships_from_csv_schema_error_bad_start_column() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("schema_rels.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "from,to,type").unwrap();
        writeln!(file, "1,2,KNOWS").unwrap();

        let mut builder = GraphBuilder::new(None, None);
        let result = builder.load_relationships_from_csv(
            file_path.to_str().unwrap(),
            "source", // doesn't exist
            "to",
            Some("type"),
            None,
            None,
            None,
            None,
            b',',
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found"),
            "Expected schema error, got: {}",
            err_msg
        );
    }

    /// Resolve relationship endpoints by a property whose external ids overflow `u32` and
    /// collide across labels (the LDBC shape). Auto-assigned internal ids are 0,1,2 in row
    /// order; the `Person` label filter must disambiguate the colliding external id.
    #[test]
    fn test_load_relationships_from_csv_property_ref() {
        use crate::types::NodeReference;
        let temp_dir = TempDir::new().unwrap();

        let nodes_path = temp_dir.path().join("nodes.csv");
        let mut nf = File::create(&nodes_path).unwrap();
        // internal 0 = Comment ext 1000000000001, 1 = Person ext 1000000000001,
        // 2 = Person ext 1000000000002. Without the label filter, `.first()` would pick
        // the Comment for ext 1000000000001 because it is loaded first.
        writeln!(nf, "ext_id|label").unwrap();
        writeln!(nf, "1000000000001|Comment").unwrap();
        writeln!(nf, "1000000000001|Person").unwrap();
        writeln!(nf, "1000000000002|Person").unwrap();
        drop(nf);

        let rels_path = temp_dir.path().join("rels.csv");
        let mut rf = File::create(&rels_path).unwrap();
        writeln!(rf, "from|to|type").unwrap();
        writeln!(rf, "1000000000001|1000000000002|KNOWS").unwrap();
        drop(rf);

        let mut builder = GraphBuilder::new(None, None);
        builder
            .load_nodes_from_csv(
                nodes_path.to_str().unwrap(),
                None, // auto-assign internal node ids
                Some(vec!["label"]),
                Some(vec!["ext_id"]), // store external id as a property
                None,
                None,
                None,
                b'|',
            )
            .unwrap();

        let rel_ids = builder
            .load_relationships_from_csv(
                rels_path.to_str().unwrap(),
                NodeReference::property("from", "ext_id", Some("Person")),
                NodeReference::property("to", "ext_id", Some("Person")),
                Some("type"),
                None,
                None,
                None,
                None,
                b'|',
            )
            .unwrap();

        assert_eq!(rel_ids, vec![(1, 2)]);
        assert_eq!(builder.relationship_count(), 1);
        // Endpoints are the two Person nodes, not the colliding Comment node.
        assert_eq!(builder.prop(1, "ext_id"), Some(ValueId::I64(1000000000001)));
        assert_eq!(builder.prop(2, "ext_id"), Some(ValueId::I64(1000000000002)));
    }

    /// Relationship rows whose property reference matches no node are skipped, not errored.
    #[test]
    fn test_load_relationships_from_csv_property_ref_unmatched_skipped() {
        use crate::types::NodeReference;
        let temp_dir = TempDir::new().unwrap();

        let nodes_path = temp_dir.path().join("nodes.csv");
        let mut nf = File::create(&nodes_path).unwrap();
        writeln!(nf, "ext_id|label").unwrap();
        writeln!(nf, "10|Person").unwrap();
        writeln!(nf, "20|Person").unwrap();
        drop(nf);

        let rels_path = temp_dir.path().join("rels.csv");
        let mut rf = File::create(&rels_path).unwrap();
        writeln!(rf, "from|to|type").unwrap();
        writeln!(rf, "10|20|KNOWS").unwrap();
        writeln!(rf, "10|999|KNOWS").unwrap(); // 999 matches no node
        drop(rf);

        let mut builder = GraphBuilder::new(None, None);
        builder
            .load_nodes_from_csv(
                nodes_path.to_str().unwrap(),
                None,
                Some(vec!["label"]),
                Some(vec!["ext_id"]),
                None,
                None,
                None,
                b'|',
            )
            .unwrap();

        let rel_ids = builder
            .load_relationships_from_csv(
                rels_path.to_str().unwrap(),
                NodeReference::property("from", "ext_id", Some("Person")),
                NodeReference::property("to", "ext_id", Some("Person")),
                Some("type"),
                None,
                None,
                None,
                None,
                b'|',
            )
            .unwrap();

        assert_eq!(rel_ids, vec![(0, 1)]);
        assert_eq!(builder.relationship_count(), 1);
    }

    /// Resolve endpoints by a composite (multi-column) property reference.
    #[test]
    fn test_load_relationships_from_csv_composite_ref() {
        use crate::types::NodeReference;
        let temp_dir = TempDir::new().unwrap();

        let nodes_path = temp_dir.path().join("nodes.csv");
        let mut nf = File::create(&nodes_path).unwrap();
        writeln!(nf, "first,last,label").unwrap();
        writeln!(nf, "Alice,Smith,Person").unwrap();
        writeln!(nf, "Bob,Jones,Person").unwrap();
        drop(nf);

        let rels_path = temp_dir.path().join("rels.csv");
        let mut rf = File::create(&rels_path).unwrap();
        writeln!(rf, "f1,l1,f2,l2,type").unwrap();
        writeln!(rf, "Alice,Smith,Bob,Jones,KNOWS").unwrap();
        drop(rf);

        let mut builder = GraphBuilder::new(None, None);
        builder
            .load_nodes_from_csv(
                nodes_path.to_str().unwrap(),
                None,
                Some(vec!["label"]),
                Some(vec!["first", "last"]),
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        let rel_ids = builder
            .load_relationships_from_csv(
                rels_path.to_str().unwrap(),
                NodeReference::composite(
                    vec!["f1".to_string(), "l1".to_string()],
                    vec!["first".to_string(), "last".to_string()],
                    Some("Person"),
                ),
                NodeReference::composite(
                    vec!["f2".to_string(), "l2".to_string()],
                    vec!["first".to_string(), "last".to_string()],
                    Some("Person"),
                ),
                Some("type"),
                None,
                None,
                None,
                None,
                b',',
            )
            .unwrap();

        assert_eq!(rel_ids, vec![(0, 1)]);
        assert_eq!(builder.relationship_count(), 1);
    }
}
