//! High-level graph API for managing multiple snapshots by version

use crate::graph_builder::GraphBuilder;
use crate::graph_snapshot::GraphSnapshot;
use hashbrown::HashMap;
use std::sync::{Arc, RwLock};

/// High-level graph API for storing and retrieving multiple graph snapshots by version
/// 
/// This is the main entry point for RustyChickpeas. It allows you to maintain multiple 
/// versions of a graph (e.g., "v0.1", "v0.2", "v1.0") and retrieve them by version string.
/// 
/// The manager creates builders, and when you finalize a builder, it automatically
/// adds the snapshot to the manager.
#[derive(Debug, Clone)]
pub struct RustyChickpeas {
    snapshots: Arc<RwLock<HashMap<String, Arc<GraphSnapshot>>>>,
}

impl RustyChickpeas {
    /// Create a new empty manager
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new GraphBuilder
    /// 
    /// The builder is owned by the caller and can be used to add nodes and relationships.
    /// After finalizing with `finalize()`, add the snapshot to this manager using `add_snapshot()`.
    /// 
    /// # Arguments
    /// * `version` - Optional version string for the snapshot (e.g., "v1.0")
    /// * `capacity_nodes` - Optional capacity hint for nodes (defaults to 2^20 = 1,048,576, auto-grows as needed)
    /// * `capacity_rels` - Optional capacity hint for relationships (defaults to 2^20 = 1,048,576, auto-grows as needed)
    /// 
    /// Capacity is just a performance hint for pre-allocation. The builder will automatically
    /// grow as needed (doubling each time) up to the maximum limits (2^32 - 1 nodes, 2^64 - 1 relationships).
    pub fn create_builder(&self, version: Option<&str>, capacity_nodes: Option<usize>, capacity_rels: Option<usize>) -> GraphBuilder {
        if let Some(v) = version {
            GraphBuilder::with_version(v, capacity_nodes, capacity_rels)
        } else {
            GraphBuilder::new(capacity_nodes, capacity_rels)
        }
    }

    /// Add a snapshot to the manager
    /// 
    /// If the snapshot has a version, it will be stored under that version.
    /// If no version is set, it will be stored under the key "latest".
    /// 
    /// If a snapshot with the same version already exists, it will be replaced.
    pub fn add_snapshot(&self, snapshot: GraphSnapshot) {
        let version = snapshot.version()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "latest".to_string());
        
        let mut snapshots = self.snapshots.write().unwrap();
        snapshots.insert(version, Arc::new(snapshot));
    }

    /// Add a snapshot with an explicit version key
    /// 
    /// This allows you to override the snapshot's internal version or assign
    /// a version to a snapshot that doesn't have one.
    pub fn add_snapshot_with_version(&self, version: &str, snapshot: GraphSnapshot) {
        let mut snapshots = self.snapshots.write().unwrap();
        snapshots.insert(version.to_string(), Arc::new(snapshot));
    }

    /// Get a graph snapshot by version
    /// 
    /// Returns `None` if no snapshot with that version exists.
    pub fn graph_snapshot(&self, version: &str) -> Option<Arc<GraphSnapshot>> {
        let snapshots = self.snapshots.read().unwrap();
        snapshots.get(version).cloned()
    }

    /// Get all available versions
    pub fn versions(&self) -> Vec<String> {
        let snapshots = self.snapshots.read().unwrap();
        snapshots.keys().cloned().collect()
    }

    /// Remove a snapshot by version
    pub fn remove_snapshot(&self, version: &str) -> bool {
        let mut snapshots = self.snapshots.write().unwrap();
        snapshots.remove(version).is_some()
    }

    /// Get the number of snapshots stored
    pub fn len(&self) -> usize {
        let snapshots = self.snapshots.read().unwrap();
        snapshots.len()
    }

    /// Check if the manager is empty
    pub fn is_empty(&self) -> bool {
        let snapshots = self.snapshots.read().unwrap();
        snapshots.is_empty()
    }

    /// Clear all snapshots
    pub fn clear(&self) {
        let mut snapshots = self.snapshots.write().unwrap();
        snapshots.clear();
    }
}

impl Default for RustyChickpeas {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rusty_chickpeas_new() {
        let manager = RustyChickpeas::new();
        assert!(manager.is_empty());
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_rusty_chickpeas_default() {
        let manager = RustyChickpeas::default();
        assert!(manager.is_empty());
    }

    #[test]
    fn test_create_builder() {
        let manager = RustyChickpeas::new();
        let builder = manager.create_builder(None, Some(10), Some(10));
        assert_eq!(builder.node_count(), 0);
    }

    #[test]
    fn test_create_builder_with_version() {
        let manager = RustyChickpeas::new();
        let builder = manager.create_builder(Some("v1.0"), Some(10), Some(10));
        assert_eq!(builder.version, Some("v1.0".to_string()));
    }

    #[test]
    fn test_add_snapshot() {
        let manager = RustyChickpeas::new();
        let snapshot = GraphSnapshot::new();
        manager.add_snapshot(snapshot);
        assert_eq!(manager.len(), 1);
        assert!(!manager.is_empty());
        assert_eq!(manager.versions(), vec!["latest"]);
    }

    #[test]
    fn test_add_snapshot_with_version() {
        let manager = RustyChickpeas::new();
        let snapshot = GraphSnapshot::new();
        manager.add_snapshot_with_version("v1.0", snapshot);
        assert_eq!(manager.len(), 1);
        assert_eq!(manager.versions(), vec!["v1.0"]);
    }

    #[test]
    fn test_get_graph_snapshot() {
        let manager = RustyChickpeas::new();
        let snapshot = GraphSnapshot::new();
        manager.add_snapshot_with_version("v1.0", snapshot);
        
        let retrieved = manager.graph_snapshot("v1.0");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().n_nodes, 0);
    }

    #[test]
    fn test_get_graph_snapshot_nonexistent() {
        let manager = RustyChickpeas::new();
        let retrieved = manager.graph_snapshot("v1.0");
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_versions() {
        let manager = RustyChickpeas::new();
        manager.add_snapshot_with_version("v1.0", GraphSnapshot::new());
        manager.add_snapshot_with_version("v2.0", GraphSnapshot::new());
        
        let versions = manager.versions();
        assert_eq!(versions.len(), 2);
        assert!(versions.contains(&"v1.0".to_string()));
        assert!(versions.contains(&"v2.0".to_string()));
    }

    #[test]
    fn test_remove_snapshot() {
        let manager = RustyChickpeas::new();
        manager.add_snapshot_with_version("v1.0", GraphSnapshot::new());
        assert_eq!(manager.len(), 1);
        
        assert!(manager.remove_snapshot("v1.0"));
        assert_eq!(manager.len(), 0);
        assert!(manager.is_empty());
        
        assert!(!manager.remove_snapshot("v1.0")); // Already removed
    }

    #[test]
    fn test_clear() {
        let manager = RustyChickpeas::new();
        manager.add_snapshot_with_version("v1.0", GraphSnapshot::new());
        manager.add_snapshot_with_version("v2.0", GraphSnapshot::new());
        assert_eq!(manager.len(), 2);
        
        manager.clear();
        assert_eq!(manager.len(), 0);
        assert!(manager.is_empty());
    }

    #[test]
    fn test_replace_snapshot() {
        let manager = RustyChickpeas::new();
        let snapshot1 = GraphSnapshot::new();
        manager.add_snapshot_with_version("v1.0", snapshot1);
        
        // Replace with new snapshot
        let snapshot2 = GraphSnapshot::new();
        manager.add_snapshot_with_version("v1.0", snapshot2);
        
        assert_eq!(manager.len(), 1);
    }
}
