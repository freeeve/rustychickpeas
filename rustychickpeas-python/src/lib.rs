//! RustyChickpeas Python Bindings
//!
//! PyO3 bindings for the RustyChickpeas graph API

#![allow(non_local_definitions)] // PyO3 requires impl blocks for pymethods
#![allow(deprecated)] // Allow deprecated methods for backward compatibility

use pyo3::prelude::*;

// Module declarations
mod direction;
mod utils;
mod node;
mod relationship;
mod graph_snapshot;
mod graph_snapshot_builder;
mod rusty_chickpeas;

// Re-export types for use in other modules
pub(crate) use direction::Direction;
pub(crate) use node::Node;
pub(crate) use relationship::Relationship;
pub(crate) use graph_snapshot::GraphSnapshot;
pub(crate) use graph_snapshot_builder::GraphSnapshotBuilder;
pub(crate) use rusty_chickpeas::RustyChickpeas;

#[pymodule]
fn rustychickpeas(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Direction>()?;
    m.add_class::<Node>()?;
    m.add_class::<Relationship>()?;
    m.add_class::<GraphSnapshot>()?;
    m.add_class::<GraphSnapshotBuilder>()?;
    m.add_class::<RustyChickpeas>()?;
    Ok(())
}
