//! Error types for the graph API

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GraphError {
    #[error("Bulk load error: {0}")]
    BulkLoadError(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Schema error: {0}")]
    SchemaError(String),
    #[error("Capacity error: {0}")]
    CapacityError(String),
    #[error("CSV error: {0}")]
    CsvError(String),
}

pub type Result<T> = std::result::Result<T, GraphError>;

