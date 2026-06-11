//! RustyChickpeas Core - In-memory graph API using RoaringBitmaps
//!
//! This library provides a high-performance graph API with a familiar
//! interface for easy porting of graph database stored procedures.

pub mod bitmap;
pub mod error;
pub mod graph_builder;
pub mod graph_builder_csv;
pub mod graph_builder_parquet;
pub mod graph_snapshot;
pub mod interner;
pub mod rusty_chickpeas;
pub mod types;

// Re-export main types
pub use error::{GraphError, Result};
pub use graph_builder::GraphBuilder;
pub use graph_builder_csv::CsvColumnType;
pub use graph_snapshot::{Column, GraphSnapshot, ValueId};
pub use rusty_chickpeas::RustyChickpeas;
pub use types::{
    Direction, Label, NodeId, PropertyKey, PropertyValue, RelationshipDeduplication,
    RelationshipType,
};
