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
}

pub type Result<T> = std::result::Result<T, GraphError>;

