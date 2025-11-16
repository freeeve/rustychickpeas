//! RustyChickpeas manager Python wrapper

use pyo3::prelude::*;
use rustychickpeas_core::RustyChickpeas as CoreRustyChickpeas;
use crate::graph_snapshot::GraphSnapshot;
use crate::graph_snapshot_builder::GraphSnapshotBuilder;

/// Python wrapper for RustyChickpeas manager
#[pyclass(name = "RustyChickpeas")]
pub struct RustyChickpeas {
    pub(crate) manager: CoreRustyChickpeas,
}

#[pymethods]
impl RustyChickpeas {
    #[new]
    fn new() -> Self {
        Self {
            manager: CoreRustyChickpeas::new(),
        }
    }

    /// Create a new GraphBuilder
    /// 
    /// # Arguments
    /// * `version` - Optional version string for the snapshot (e.g., "v1.0")
    /// * `capacity_nodes` - Optional capacity hint for nodes (defaults to 2^20 = 1,048,576, auto-grows as needed)
    /// * `capacity_rels` - Optional capacity hint for relationships (defaults to 2^20 = 1,048,576, auto-grows as needed)
    /// 
    /// Capacity is just a performance hint. The builder automatically grows as needed up to
    /// the maximum limits (4.3B nodes, 18.4 quintillion relationships).
    fn create_builder(
        &self,
        version: Option<String>,
        capacity_nodes: Option<usize>,
        capacity_rels: Option<usize>,
    ) -> GraphSnapshotBuilder {
        let builder = self.manager.create_builder(
            version.as_deref(),
            capacity_nodes,
            capacity_rels,
        );
        GraphSnapshotBuilder { builder }
    }

    /// Get a graph snapshot by version
    fn get_graph_snapshot(&self, version: &str) -> PyResult<Option<GraphSnapshot>> {
        if let Some(snapshot) = self.manager.get_graph_snapshot(version) {
            // Arc::clone is cheap, just cloning the pointer
            Ok(Some(GraphSnapshot::from_arc(snapshot)))
        } else {
            Ok(None)
        }
    }

    /// Get all available versions
    fn versions(&self) -> Vec<String> {
        self.manager.versions()
    }

    /// Remove a snapshot by version
    fn remove_snapshot(&self, version: &str) -> bool {
        self.manager.remove_snapshot(version)
    }

    /// Get the number of snapshots stored
    fn len(&self) -> usize {
        self.manager.len()
    }

    /// Check if the manager is empty
    fn is_empty(&self) -> bool {
        self.manager.is_empty()
    }

    /// Clear all snapshots
    fn clear(&self) {
        self.manager.clear()
    }
}

