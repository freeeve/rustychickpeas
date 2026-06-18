//! RustyChickpeas Core - In-memory graph API using RoaringBitmaps
//!
//! This library provides a high-performance graph API with a familiar
//! interface for easy porting of graph database stored procedures.

pub mod bitmap;
pub mod error;
pub mod fulltext;
pub mod geo;
pub mod graph_builder;
pub mod graph_builder_csv;
pub mod graph_builder_parquet;
pub mod graph_snapshot;
pub mod interner;
pub mod rusty_chickpeas;
pub mod serialize;
pub mod types;

/// On-disk formats (RCPG graph file, RRSR record store), re-exported for
/// direct access to the codec and record-store range planning.
pub use rustychickpeas_format as format;

// Re-export main types
pub use error::{GraphError, Result};
pub use fulltext::FullTextField;
pub use geo::{haversine_km, GeoIndex};
pub use graph_builder::GraphBuilder;
pub use graph_builder_csv::CsvColumnType;
pub use graph_snapshot::{
    BoolCol, Col, Column, GraphSnapshot, I64Col, NeighborsByType, Prop, RelMatch, RelTypeFilter,
    RelationshipRef, RelationshipsByType, ShortestPaths, ValueId,
};
pub use rusty_chickpeas::RustyChickpeas;
pub use types::{
    Direction, Label, NodeId, PropertyKey, PropertyValue, RelationshipDeduplication,
    RelationshipType,
};
