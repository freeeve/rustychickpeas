"""Comprehensive tests for Node class"""

import pytest
import json
from rustychickpeas import Direction, GraphSnapshotBuilder, RustyChickpeas


class TestNodeBasic:
    """Test basic Node functionality"""
    
    @pytest.fixture
    def sample_graph(self):
        """Create a sample graph for testing"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        # Create nodes with labels and properties
        builder.add_node(["Person", "User"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        
        # Add properties
        builder.set_prop(0, "name", "Alice")
        builder.set_prop(0, "age", 30)
        builder.set_prop(0, "active", True)
        builder.set_prop(0, "score", 95.5)
        
        builder.set_prop(1, "name", "Bob")
        builder.set_prop(1, "age", 25)
        
        builder.set_prop(2, "name", "Acme Corp")
        builder.set_prop(2, "founded", 1990)
        
        # Add relationships
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "WORKS_FOR")
        builder.add_rel(1, 2, "WORKS_FOR")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_node(self, sample_graph):
        """Test getting a node by ID"""
        node = sample_graph.node(0)
        assert node is not None
        assert node.id() == 0
        
        node = sample_graph.node(1)
        assert node.id() == 1
    
    def test_get_node_invalid_id(self, sample_graph):
        """Test getting a node with invalid ID"""
        with pytest.raises(Exception):  # Should raise ValueError
            sample_graph.node(999)
    
    def test_node_id(self, sample_graph):
        """Test getting node ID"""
        node = sample_graph.node(0)
        assert node.id() == 0
        
        node = sample_graph.node(1)
        assert node.id() == 1


class TestNodeLabels:
    """Test Node label functionality"""
    
    @pytest.fixture
    def labeled_graph(self):
        """Create a graph with labeled nodes"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person", "User"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company", "Organization"], node_id=2)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_labels_single(self, labeled_graph):
        """Test getting labels for a node with single label"""
        node = labeled_graph.node(1)
        labels = node.labels()
        assert labels == ["Person"]
    
    def test_get_labels_multiple(self, labeled_graph):
        """Test getting labels for a node with multiple labels"""
        node = labeled_graph.node(0)
        labels = node.labels()
        assert set(labels) == {"Person", "User"}
        
        node = labeled_graph.node(2)
        labels = node.labels()
        assert set(labels) == {"Company", "Organization"}
    
    def test_get_labels_no_labels(self):
        """Test getting labels for a node with no labels"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        # Note: nodes must have at least one label in the current API
        builder.add_node(["Person"], node_id=0)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node = snapshot.node(0)
        labels = node.labels()
        assert len(labels) >= 1  # At least one label


class TestNodeProperties:
    """Test Node property functionality"""
    
    @pytest.fixture
    def property_graph(self):
        """Create a graph with node properties"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.set_prop(0, "name", "Alice")
        builder.set_prop(0, "age", 30)
        builder.set_prop(0, "active", True)
        builder.set_prop(0, "score", 95.5)
        
        builder.add_node(["Person"], node_id=1)
        builder.set_prop(1, "name", "Bob")
        builder.set_prop(1, "age", 25)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_property_string(self, property_graph):
        """Test getting string property"""
        node = property_graph.node(0)
        name = node.get_property("name")
        assert name == "Alice"
    
    def test_get_property_integer(self, property_graph):
        """Test getting integer property"""
        node = property_graph.node(0)
        age = node.get_property("age")
        assert age == 30
    
    def test_get_property_float(self, property_graph):
        """Test getting float property"""
        node = property_graph.node(0)
        score = node.get_property("score")
        assert score == pytest.approx(95.5)
    
    def test_get_property_bool(self, property_graph):
        """Test getting boolean property"""
        node = property_graph.node(0)
        active = node.get_property("active")
        assert active is True
    
    def test_get_property_nonexistent(self, property_graph):
        """Test getting nonexistent property"""
        node = property_graph.node(0)
        with pytest.raises(ValueError, match="Property key 'nonexistent' not found"):
            node.get_property("nonexistent")
    
    def test_get_property_different_nodes(self, property_graph):
        """Test getting properties from different nodes"""
        node0 = property_graph.node(0)
        node1 = property_graph.node(1)
        
        assert node0.get_property("name") == "Alice"
        assert node1.get_property("name") == "Bob"
        
        assert node0.get_property("age") == 30
        assert node1.get_property("age") == 25


class TestNodeRelationships:
    """Test Node relationship functionality"""
    
    @pytest.fixture
    def relationship_graph(self):
        """Create a graph with relationships"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_node(["Company"], node_id=3)
        
        # 0 -> 1, 0 -> 2, 1 -> 2, 3 -> 0
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        builder.add_rel(3, 0, "EMPLOYS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_rels_outgoing(self, relationship_graph):
        """Test getting outgoing relationships"""
        node = relationship_graph.node(0)
        rels = node.relationships(Direction.Outgoing)
        assert len(rels) == 2
        
        end_ids = {rel.end_node().id() for rel in rels}
        assert end_ids == {1, 2}
    
    def test_get_rels_incoming(self, relationship_graph):
        """Test getting incoming relationships"""
        node = relationship_graph.node(0)
        rels = node.relationships(Direction.Incoming)
        assert len(rels) == 1
        assert rels[0].start_node().id() == 3
    
    def test_get_rels_both(self, relationship_graph):
        """Test getting relationships in both directions"""
        node = relationship_graph.node(0)
        rels = node.relationships(Direction.Both)
        assert len(rels) == 3  # 2 outgoing + 1 incoming
    
    def test_get_rels_with_type_filter(self, relationship_graph):
        """Test getting relationships with type filter"""
        node = relationship_graph.node(0)
        knows_rels = node.relationships(Direction.Outgoing, ["KNOWS"])
        assert len(knows_rels) == 2
        
        for rel in knows_rels:
            assert rel.reltype() == "KNOWS"
    
    def test_get_rel_ids_outgoing(self, relationship_graph):
        """Test getting outgoing relationship IDs"""
        node = relationship_graph.node(0)
        rel_ids = node.relationship_ids(Direction.Outgoing)
        assert set(rel_ids) == {1, 2}
    
    def test_get_rel_ids_incoming(self, relationship_graph):
        """Test getting incoming relationship IDs"""
        node = relationship_graph.node(0)
        rel_ids = node.relationship_ids(Direction.Incoming)
        assert rel_ids == [3]
    
    def test_get_rel_ids_both(self, relationship_graph):
        """Test getting relationship IDs in both directions"""
        node = relationship_graph.node(0)
        rel_ids = node.relationship_ids(Direction.Both)
        assert set(rel_ids) == {1, 2, 3}
    
    def test_get_rel_ids_with_type_filter(self, relationship_graph):
        """Test getting relationship IDs with type filter"""
        node = relationship_graph.node(0)
        knows_ids = node.relationship_ids(Direction.Outgoing, ["KNOWS"])
        assert set(knows_ids) == {1, 2}
        
        employs_ids = node.relationship_ids(Direction.Incoming, ["EMPLOYS"])
        assert employs_ids == [3]
    
    def test_get_degree_outgoing(self, relationship_graph):
        """Test getting outgoing degree"""
        node = relationship_graph.node(0)
        assert node.degree(Direction.Outgoing) == 2
    
    def test_get_degree_incoming(self, relationship_graph):
        """Test getting incoming degree"""
        node = relationship_graph.node(0)
        assert node.degree(Direction.Incoming) == 1
    
    def test_get_degree_both(self, relationship_graph):
        """Test getting total degree"""
        node = relationship_graph.node(0)
        assert node.degree(Direction.Both) == 3
    
    def test_get_degree_isolated_node(self):
        """Test degree for isolated node"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node = snapshot.node(0)
        assert node.degree(Direction.Outgoing) == 0
        assert node.degree(Direction.Incoming) == 0
        assert node.degree(Direction.Both) == 0


class TestNodeSerialization:
    """Test Node serialization methods"""
    
    @pytest.fixture
    def serialization_graph(self):
        """Create a graph for serialization tests"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person", "User"], node_id=0)
        builder.set_prop(0, "name", "Alice")
        builder.set_prop(0, "age", 30)
        builder.set_prop(0, "active", True)
        builder.set_prop(0, "score", 95.5)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_to_dict(self, serialization_graph):
        """Test converting node to dictionary"""
        node = serialization_graph.node(0)
        node_dict = node.to_dict()
        
        assert isinstance(node_dict, dict)
        assert node_dict["id"] == 0
        assert set(node_dict["labels"]) == {"Person", "User"}
        assert node_dict["properties"]["name"] == "Alice"
        assert node_dict["properties"]["age"] == 30
        assert node_dict["properties"]["active"] is True
        assert node_dict["properties"]["score"] == pytest.approx(95.5)
    
    def test_to_json(self, serialization_graph):
        """Test converting node to JSON string"""
        node = serialization_graph.node(0)
        json_str = node.to_json()
        
        assert isinstance(json_str, str)
        node_dict = json.loads(json_str)
        
        assert node_dict["id"] == 0
        assert set(node_dict["labels"]) == {"Person", "User"}
        assert node_dict["properties"]["name"] == "Alice"
        assert node_dict["properties"]["age"] == 30
    
    def test_to_dict_node_without_properties(self):
        """Test to_dict for node without properties"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node = snapshot.node(0)
        node_dict = node.to_dict()
        
        assert node_dict["id"] == 0
        assert node_dict["labels"] == ["Person"]
        assert isinstance(node_dict["properties"], dict)
        # Properties dict may be empty or contain default values

