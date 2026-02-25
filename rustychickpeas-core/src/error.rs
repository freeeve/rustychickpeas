//! Error types for the graph API

use crate::types::NodeId;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GraphError {
    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),
    #[error("Bulk load error: {0}")]
    BulkLoadError(String),
    #[error("Property not indexed: {0}")]
    PropertyNotIndexed(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Schema error: {0}")]
    SchemaError(String),
    #[error("Capacity error: {0}")]
    CapacityError(String),
    #[error("Relationship error: {0}")]
    RelationshipError(String),
    #[error("Parquet error: {0}")]
    ParquetError(String),
    #[error("CSV error: {0}")]
    CsvError(String),
    #[error("Arrow error: {0}")]
    ArrowError(String),
}

pub type Result<T> = std::result::Result<T, GraphError>;

