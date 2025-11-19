"""Comprehensive tests for RustyChickpeas manager class"""

import pytest
from rustychickpeas import RustyChickpeas, GraphSnapshotBuilder, Direction


class TestRustyChickpeasBasic:
    """Test basic RustyChickpeas functionality"""
    
    def test_create_manager(self):
        """Test creating a new manager"""
        manager = RustyChickpeas()
        assert manager is not None
    
    def test_manager_is_empty(self):
        """Test that new manager is empty"""
        manager = RustyChickpeas()
        assert manager.is_empty() is True
        assert manager.len() == 0
    
    def test_manager_versions_empty(self):
        """Test getting versions from empty manager"""
        manager = RustyChickpeas()
        versions = manager.versions()
        assert versions == []


class TestRustyChickpeasBuilder:
    """Test RustyChickpeas builder creation"""
    
    def test_create_builder_default(self):
        """Test creating builder with default parameters"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        assert builder is not None
    
    def test_create_builder_with_version(self):
        """Test creating builder with version"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        assert builder is not None
    
    def test_create_builder_with_capacity(self):
        """Test creating builder with capacity hints"""
        manager = RustyChickpeas()
        builder = manager.create_builder(capacity_nodes=1000, capacity_rels=5000)
        assert builder is not None
    
    def test_create_builder_with_all_params(self):
        """Test creating builder with all parameters"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v2.0", capacity_nodes=500, capacity_rels=2000)
        assert builder is not None


class TestRustyChickpeasSnapshotManagement:
    """Test RustyChickpeas snapshot management"""
    
    def test_add_snapshot(self):
        """Test adding a snapshot to manager"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        builder.add_node(["Person"], node_id=0)
        builder.set_version("v1.0")
        builder.finalize_into(manager)
        
        assert manager.len() == 1
        assert manager.is_empty() is False
        assert "v1.0" in manager.versions()
    
    def test_get_snapshot(self):
        """Test getting a snapshot by version"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        builder.add_node(["Person"], node_id=0)
        builder.set_version("v1.0")
        builder.finalize_into(manager)
        
        snapshot = manager.graph_snapshot("v1.0")
        assert snapshot is not None
        assert snapshot.node_count() >= 1
    
    def test_get_snapshot_nonexistent(self):
        """Test getting nonexistent snapshot"""
        manager = RustyChickpeas()
        snapshot = manager.graph_snapshot("nonexistent")
        assert snapshot is None
    
    def test_multiple_snapshots(self):
        """Test managing multiple snapshots"""
        manager = RustyChickpeas()
        
        # Create first snapshot
        builder1 = manager.create_builder()
        builder1.add_node(["Person"], node_id=0)
        builder1.set_version("v1.0")
        builder1.finalize_into(manager)
        
        # Create second snapshot
        builder2 = manager.create_builder()
        builder2.add_node(["Person"], node_id=0)
        builder2.add_node(["Person"], node_id=1)
        builder2.set_version("v2.0")
        builder2.finalize_into(manager)
        
        assert manager.len() == 2
        assert set(manager.versions()) == {"v1.0", "v2.0"}
        
        snapshot1 = manager.graph_snapshot("v1.0")
        snapshot2 = manager.graph_snapshot("v2.0")
        
        assert snapshot1.node_count() == 1
        assert snapshot2.node_count() == 2
    
    def test_remove_snapshot(self):
        """Test removing a snapshot"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        builder.add_node(["Person"], node_id=0)
        builder.set_version("v1.0")
        builder.finalize_into(manager)
        
        assert manager.len() == 1
        assert manager.remove_snapshot("v1.0") is True
        assert manager.len() == 0
        assert manager.graph_snapshot("v1.0") is None
    
    def test_remove_nonexistent_snapshot(self):
        """Test removing nonexistent snapshot"""
        manager = RustyChickpeas()
        assert manager.remove_snapshot("nonexistent") is False
    
    def test_clear_all_snapshots(self):
        """Test clearing all snapshots"""
        manager = RustyChickpeas()
        
        # Add multiple snapshots
        for i in range(3):
            builder = manager.create_builder()
            builder.add_node(["Person"], node_id=0)
            builder.set_version(f"v{i}.0")
            builder.finalize_into(manager)
        
        assert manager.len() == 3
        manager.clear()
        assert manager.len() == 0
        assert manager.is_empty() is True
        assert manager.versions() == []


class TestRustyChickpeasWorkflow:
    """Test complete workflows with RustyChickpeas"""
    
    def test_build_and_query_workflow(self):
        """Test complete build and query workflow"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        # Build graph
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        builder.set_prop(0, "name", "Alice")
        builder.set_prop(1, "name", "Bob")
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "WORKS_FOR")
        
        builder.set_version("workflow_test")
        builder.finalize_into(manager)
        
        # Query graph
        snapshot = manager.graph_snapshot("workflow_test")
        assert snapshot.node_count() == 3
        assert snapshot.relationship_count() == 2
        
        node0 = snapshot.node(0)
        assert node0.get_property("name") == "Alice"
        assert node0.degree(Direction.Outgoing) == 2
    
    def test_multiple_versions_workflow(self):
        """Test workflow with multiple versions"""
        manager = RustyChickpeas()
        
        # Version 1: Simple graph
        builder1 = manager.create_builder()
        builder1.add_node(["Person"], node_id=0)
        builder1.set_version("v1.0")
        builder1.finalize_into(manager)
        
        # Version 2: Extended graph
        builder2 = manager.create_builder()
        builder2.add_node(["Person"], node_id=0)
        builder2.add_node(["Person"], node_id=1)
        builder2.add_rel(0, 1, "KNOWS")
        builder2.set_version("v2.0")
        builder2.finalize_into(manager)
        
        # Query both versions
        snapshot1 = manager.graph_snapshot("v1.0")
        snapshot2 = manager.graph_snapshot("v2.0")
        
        assert snapshot1.node_count() == 1
        assert snapshot1.relationship_count() == 0
        assert snapshot2.node_count() == 2
        assert snapshot2.relationship_count() == 1
    
    def test_update_workflow(self):
        """Test updating a graph version"""
        manager = RustyChickpeas()
        
        # Create initial version
        builder1 = manager.create_builder()
        builder1.add_node(["Person"], node_id=0)
        builder1.set_prop(0, "name", "Alice")
        builder1.set_version("v1.0")
        builder1.finalize_into(manager)
        
        # Create updated version
        builder2 = manager.create_builder()
        builder2.add_node(["Person"], node_id=0)
        builder2.set_prop(0, "name", "Alice")
        builder2.set_prop(0, "age", 30)  # Added property
        builder2.set_version("v1.1")
        builder2.finalize_into(manager)
        
        snapshot1 = manager.graph_snapshot("v1.0")
        snapshot2 = manager.graph_snapshot("v1.1")
        
        # v1.0 doesn't have age property, should raise ValueError
        with pytest.raises(ValueError, match="Property key 'age' not found"):
            snapshot1.node_property(0, "age")
        assert snapshot2.node_property(0, "age") == 30


class TestRustyChickpeasEdgeCases:
    """Test RustyChickpeas edge cases"""
    
    def test_empty_snapshot(self):
        """Test creating and managing empty snapshot"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        builder.set_version("empty")
        builder.finalize_into(manager)
        
        snapshot = manager.graph_snapshot("empty")
        assert snapshot is not None
        assert snapshot.node_count() == 0
        assert snapshot.relationship_count() == 0
    
    def test_single_node_snapshot(self):
        """Test snapshot with single node"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        builder.add_node(["Person"], node_id=0)
        builder.set_version("single")
        builder.finalize_into(manager)
        
        snapshot = manager.graph_snapshot("single")
        assert snapshot.node_count() == 1
        assert snapshot.relationship_count() == 0
    
    def test_large_number_of_snapshots(self):
        """Test managing many snapshots"""
        manager = RustyChickpeas()
        
        # Create 10 snapshots
        for i in range(10):
            builder = manager.create_builder()
            builder.add_node(["Person"], node_id=0)
            builder.set_version(f"v{i}.0")
            builder.finalize_into(manager)
        
        assert manager.len() == 10
        assert len(manager.versions()) == 10
        
        # Verify all snapshots are accessible
        for i in range(10):
            snapshot = manager.graph_snapshot(f"v{i}.0")
            assert snapshot is not None
    
    def test_snapshot_isolation(self):
        """Test that snapshots are isolated from each other"""
        manager = RustyChickpeas()
        
        # Create two snapshots with same node IDs but different data
        builder1 = manager.create_builder()
        builder1.add_node(["Person"], node_id=0)
        builder1.set_prop(0, "name", "Alice")
        builder1.set_version("v1")
        builder1.finalize_into(manager)
        
        builder2 = manager.create_builder()
        builder2.add_node(["Person"], node_id=0)
        builder2.set_prop(0, "name", "Bob")
        builder2.set_version("v2")
        builder2.finalize_into(manager)
        
        snapshot1 = manager.graph_snapshot("v1")
        snapshot2 = manager.graph_snapshot("v2")
        
        assert snapshot1.node_property(0, "name") == "Alice"
        assert snapshot2.node_property(0, "name") == "Bob"
    
    def test_remove_and_recreate(self):
        """Test removing and recreating snapshot with same version"""
        manager = RustyChickpeas()
        
        # Create and remove
        builder1 = manager.create_builder()
        builder1.add_node(["Person"], node_id=0)
        builder1.set_version("test")
        builder1.finalize_into(manager)
        manager.remove_snapshot("test")
        
        # Recreate with same version
        builder2 = manager.create_builder()
        builder2.add_node(["Person"], node_id=0)
        builder2.add_node(["Person"], node_id=1)
        builder2.set_version("test")
        builder2.finalize_into(manager)
        
        snapshot = manager.graph_snapshot("test")
        assert snapshot.node_count() == 2


class TestRustyChickpeasConcurrency:
    """Test RustyChickpeas with concurrent operations"""
    
    def test_concurrent_builders(self):
        """Test creating multiple builders concurrently"""
        manager = RustyChickpeas()
        
        # Create multiple builders
        builders = []
        for i in range(5):
            builder = manager.create_builder()
            builder.add_node(["Person"], node_id=i)
            builders.append((builder, f"v{i}.0"))
        
        # Finalize all
        for builder, version in builders:
            builder.set_version(version)
            builder.finalize_into(manager)
        
        assert manager.len() == 5
        
        # Verify all snapshots
        for i in range(5):
            snapshot = manager.graph_snapshot(f"v{i}.0")
            assert snapshot is not None
            assert snapshot.node_count() >= 1

