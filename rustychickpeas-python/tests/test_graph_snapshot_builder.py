"""Comprehensive tests for GraphSnapshotBuilder class"""

import pytest
import tempfile
import os
from rustychickpeas import GraphSnapshotBuilder, RustyChickpeas, Direction
import pyarrow as pa
import pyarrow.parquet as pq


class TestGraphSnapshotBuilderBasic:
    """Test basic GraphSnapshotBuilder functionality"""
    
    def test_create_builder_default(self):
        """Test creating builder with default parameters"""
        builder = GraphSnapshotBuilder()
        assert builder is not None
    
    def test_create_builder_with_version(self):
        """Test creating builder with version"""
        builder = GraphSnapshotBuilder(version="v1.0")
        assert builder is not None
    
    def test_create_builder_with_capacity(self):
        """Test creating builder with capacity hints"""
        builder = GraphSnapshotBuilder(capacity_nodes=1000, capacity_rels=5000)
        assert builder is not None
    
    def test_set_version(self):
        """Test setting version"""
        builder = GraphSnapshotBuilder()
        builder.set_version("v2.0")
        # Version is stored internally, verify by finalizing
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("v2.0")
        assert snapshot is not None


class TestGraphSnapshotBuilderNodes:
    """Test GraphSnapshotBuilder node operations"""
    
    @pytest.fixture
    def builder(self):
        """Create a fresh builder for each test"""
        return GraphSnapshotBuilder()
    
    def test_add_node_single_label(self, builder):
        """Test adding node with single label"""
        builder.add_node(["Person"], node_id=0)
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_count() >= 1
    
    def test_add_node_multiple_labels(self, builder):
        """Test adding node with multiple labels"""
        builder.add_node(["Person", "User"], node_id=0)
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        node = snapshot.node(0)
        labels = node.labels()
        assert set(labels) == {"Person", "User"}
    
    def test_add_multiple_nodes(self, builder):
        """Test adding multiple nodes"""
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_count() == 3


class TestGraphSnapshotBuilderRelationships:
    """Test GraphSnapshotBuilder relationship operations"""
    
    @pytest.fixture
    def builder(self):
        """Create a fresh builder for each test"""
        builder = GraphSnapshotBuilder()
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        return builder
    
    def test_add_rel(self, builder):
        """Test adding a relationship"""
        builder.add_rel(0, 1, "KNOWS")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.relationship_count() == 1
    
    def test_add_multiple_rels(self, builder):
        """Test adding multiple relationships"""
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "WORKS_FOR")
        builder.add_rel(1, 2, "WORKS_FOR")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.relationship_count() == 3
    
    def test_add_rel_self_loop(self, builder):
        """Test adding self-loop relationship"""
        builder.add_rel(0, 0, "SELF_REF")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.relationship_count() == 1


class TestGraphSnapshotBuilderProperties:
    """Test GraphSnapshotBuilder property operations"""
    
    @pytest.fixture
    def builder(self):
        """Create a fresh builder for each test"""
        builder = GraphSnapshotBuilder()
        builder.add_node(["Person"], node_id=0)
        return builder
    
    def test_set_prop_string(self, builder):
        """Test setting string property"""
        builder.set_prop(0, "name", "Alice")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "name") == "Alice"
    
    def test_set_prop_integer(self, builder):
        """Test setting integer property"""
        builder.set_prop(0, "age", 30)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "age") == 30
    
    def test_set_prop_float(self, builder):
        """Test setting float property"""
        builder.set_prop(0, "score", 95.5)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "score") == pytest.approx(95.5)
    
    def test_set_prop_bool(self, builder):
        """Test setting boolean property"""
        builder.set_prop(0, "active", True)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "active") is True
    
    def test_set_props_batch(self, builder):
        """Test setting multiple properties at once"""
        builder.set_props(0, {"name": "Alice", "age": 30, "active": True})
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "name") == "Alice"
        assert snapshot.node_property(0, "age") == 30
        assert snapshot.node_property(0, "active") is True
    
    def test_set_prop_str(self, builder):
        """Test set_prop_str method"""
        builder.set_prop_str(0, "name", "Alice")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "name") == "Alice"
    
    def test_set_prop_i64(self, builder):
        """Test set_prop_i64 method"""
        builder.set_prop_i64(0, "age", 30)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "age") == 30
    
    def test_set_prop_f64(self, builder):
        """Test set_prop_f64 method"""
        builder.set_prop_f64(0, "score", 95.5)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "score") == pytest.approx(95.5)
    
    def test_set_prop_bool_method(self, builder):
        """Test set_prop_bool method"""
        builder.set_prop_bool(0, "active", True)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "active") is True
    
    def test_get_property(self, builder):
        """Test getting property from builder"""
        builder.set_prop(0, "name", "Alice")
        
        name = builder.get_property(0, "name")
        assert name == "Alice"
    
    def test_update_prop(self, builder):
        """Test updating property"""
        builder.set_prop(0, "name", "Alice")
        builder.update_prop(0, "name", "Bob")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "name") == "Bob"
    
    def test_update_property_str(self, builder):
        """Test update_property_str method"""
        builder.set_prop(0, "name", "Alice")
        builder.update_prop_str(0, "name", "Bob")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "name") == "Bob"
    
    def test_update_property_i64(self, builder):
        """Test update_property_i64 method"""
        builder.set_prop(0, "age", 30)
        builder.update_prop_i64(0, "age", 31)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "age") == 31
    
    def test_update_property_f64(self, builder):
        """Test update_property_f64 method"""
        builder.set_prop(0, "score", 95.5)
        builder.update_prop_f64(0, "score", 96.0)
        
        manager = RustyChickpeas()
        snapshot = builder.finalize()
        assert snapshot.node_property(0, "score") == pytest.approx(96.0)
    
    def test_update_property_bool(self, builder):
        """Test update_property_bool method"""
        builder.set_prop(0, "active", True)
        builder.update_prop_bool(0, "active", False)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        assert snapshot.node_property(0, "active") is False


class TestGraphSnapshotBuilderRelationshipProperties:
    """Test GraphSnapshotBuilder relationship property operations"""
    
    @pytest.fixture
    def builder(self):
        """Create a fresh builder for each test"""
        builder = GraphSnapshotBuilder()
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_rel(0, 1, "KNOWS")
        return builder
    
    def test_set_rel_prop_string(self, builder):
        """Test setting string property on relationship"""
        builder.set_rel_prop(0, 1, "KNOWS", "since", "2020")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        node0 = snapshot.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        assert len(rels) == 1
        assert rels[0].get_property("since") == "2020"
    
    def test_set_rel_prop_integer(self, builder):
        """Test setting integer property on relationship"""
        builder.set_rel_prop(0, 1, "KNOWS", "strength", 8)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        node0 = snapshot.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        assert len(rels) == 1
        assert rels[0].get_property("strength") == 8
    
    def test_set_rel_prop_str(self, builder):
        """Test set_rel_prop_str method"""
        builder.set_rel_prop_str(0, 1, "KNOWS", "since", "2020")
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        node0 = snapshot.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        assert rels[0].get_property("since") == "2020"
    
    def test_set_rel_prop_i64(self, builder):
        """Test set_rel_prop_i64 method"""
        builder.set_rel_prop_i64(0, 1, "KNOWS", "strength", 8)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        node0 = snapshot.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        assert rels[0].get_property("strength") == 8
    
    def test_set_rel_prop_f64(self, builder):
        """Test set_rel_prop_f64 method"""
        builder.set_rel_prop_f64(0, 1, "KNOWS", "weight", 0.85)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        node0 = snapshot.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        assert rels[0].get_property("weight") == pytest.approx(0.85)
    
    def test_set_rel_prop_bool(self, builder):
        """Test set_rel_prop_bool method"""
        builder.set_rel_prop_bool(0, 1, "KNOWS", "verified", True)
        
        manager = RustyChickpeas()
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot(manager.versions()[0])
        node0 = snapshot.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        assert rels[0].get_property("verified") is True


class TestGraphSnapshotBuilderQueries:
    """Test GraphSnapshotBuilder query operations"""
    
    @pytest.fixture
    def builder(self):
        """Create a fresh builder for each test"""
        builder = GraphSnapshotBuilder()
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.set_prop(0, "age", 30)
        builder.set_prop(1, "age", 30)
        builder.set_prop(2, "age", 25)
        return builder
    
    def test_get_node_labels(self, builder):
        """Test getting node labels from builder"""
        labels = builder.node_labels(0)
        assert labels == ["Person"]
    
    def test_get_neighbor_ids(self, builder):
        """Test getting neighbor IDs from builder"""
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "KNOWS")
        
        outgoing, incoming = builder.neighbor_ids(0)
        assert set(outgoing) == {1, 2}
        assert incoming == []
    
    def test_get_nodes_with_property(self, builder):
        """Test getting nodes with property (label-scoped)"""
        # Note: This now requires a label parameter
        nodes = builder.nodes_with_property("Person", "age", 30)
        assert set(nodes) == {0, 1}


class TestGraphSnapshotBuilderParquet:
    """Test GraphSnapshotBuilder Parquet loading"""
    
    def test_load_nodes_from_parquet(self):
        """Test loading nodes from Parquet file"""
        # Create temporary Parquet file
        temp_dir = tempfile.mkdtemp()
        nodes_file = os.path.join(temp_dir, "nodes.parquet")
        
        try:
            nodes_data = {
                "node_id": [0, 1, 2],
                "label": ["Person", "Person", "Company"],
                "name": ["Alice", "Bob", "Acme"],
                "age": [30, 25, None],
            }
            nodes_table = pa.table(nodes_data)
            pq.write_table(nodes_table, nodes_file)
            
            builder = GraphSnapshotBuilder()
            node_ids = builder.load_nodes_from_parquet(
                nodes_file,
                node_id_column="node_id",
                label_columns=["label"],
                property_columns=["name", "age"],
            )
            
            assert len(node_ids) == 3
            assert set(node_ids) == {0, 1, 2}
            
        finally:
            import shutil
            shutil.rmtree(temp_dir)
    
    def test_load_relationships_from_parquet(self):
        """Test loading relationships from Parquet file"""
        # Create temporary Parquet file
        temp_dir = tempfile.mkdtemp()
        rels_file = os.path.join(temp_dir, "rels.parquet")
        
        try:
            # First create nodes
            builder = GraphSnapshotBuilder()
            builder.add_node(["Person"], node_id=0)
            builder.add_node(["Person"], node_id=1)
            builder.add_node(["Company"], node_id=2)
            
            rels_data = {
                "start_node": [0, 1],
                "end_node": [1, 2],
                "rel_type": ["KNOWS", "WORKS_FOR"],
            }
            rels_table = pa.table(rels_data)
            pq.write_table(rels_table, rels_file)
            
            rel_pairs = builder.load_relationships_from_parquet(
                rels_file,
                start_node_column="start_node",
                end_node_column="end_node",
                rel_type_column="rel_type",
            )
            
            assert len(rel_pairs) == 2
            assert (0, 1) in rel_pairs
            assert (1, 2) in rel_pairs
            
        finally:
            import shutil
            shutil.rmtree(temp_dir)


class TestGraphSnapshotBuilderFinalization:
    """Test GraphSnapshotBuilder finalization"""
    
    def test_finalize(self):
        """Test finalizing builder to snapshot"""
        builder = GraphSnapshotBuilder()
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_rel(0, 1, "KNOWS")
        
        snapshot = builder.finalize()
        assert snapshot.node_count() >= 2
        assert snapshot.relationship_count() == 1
    
    def test_finalize_into(self):
        """Test finalizing builder into manager"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        builder.add_node(["Person"], node_id=0)
        builder.set_version("test")
        
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        assert snapshot is not None
        assert snapshot.node_count() >= 1
    
    def test_finalize_with_index_properties(self):
        """Test finalizing with property indexes"""
        builder = GraphSnapshotBuilder()
        builder.add_node(["Person"], node_id=0)
        builder.set_prop(0, "name", "Alice")
        builder.set_prop(0, "age", 30)
        
        # Finalize with index on "name" property
        snapshot = builder.finalize(index_properties=["name"])
        assert snapshot is not None
        
        # Query should be fast with index
        nodes = snapshot.nodes_with_property("Person", "name", "Alice")
        assert nodes == [0]

