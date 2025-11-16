//! RustyChickpeas Core - In-memory graph API using RoaringBitmaps
//!
//! This library provides a high-performance graph API with a familiar
//! interface for easy porting of graph database stored procedures.

pub mod bitmap;
pub mod builder;
pub mod builder_parquet;
pub mod error;
pub mod interner;
pub mod rusty_chickpeas;
pub mod snapshot;
pub mod types;

// Re-export main types
pub use builder::GraphBuilder;
pub use error::{GraphError, Result};
pub use rusty_chickpeas::RustyChickpeas;
pub use snapshot::{Column, GraphSnapshot, ValueId};
pub use types::{Direction, Label, NodeId, PropertyKey, PropertyValue, RelationshipType};

