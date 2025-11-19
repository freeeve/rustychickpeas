"""Comprehensive tests for Relationship class"""

import pytest
import json
from rustychickpeas import Direction, GraphSnapshotBuilder, RustyChickpeas


class TestRelationshipBasic:
    """Test basic Relationship functionality"""
    
    @pytest.fixture
    def relationship_graph(self):
        """Create a graph with relationships"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "WORKS_FOR")
        builder.add_rel(1, 2, "WORKS_FOR")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_relationship_from_node(self, relationship_graph):
        """Test getting a relationship from a node"""
        node = relationship_graph.node(0)
        rels = node.relationships(Direction.Outgoing)
        
        assert len(rels) == 2
        assert all(rel.reltype() in ["KNOWS", "WORKS_FOR"] for rel in rels)
    
    def test_relationship_id(self, relationship_graph):
        """Test getting relationship ID"""
        node = relationship_graph.node(0)
        rels = node.relationships(Direction.Outgoing)
        
        for rel in rels:
            assert isinstance(rel.id(), int)
            assert rel.id() >= 0


class TestRelationshipNodes:
    """Test Relationship node access"""
    
    @pytest.fixture
    def node_graph(self):
        """Create a graph for node access tests"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "WORKS_FOR")
        builder.add_rel(1, 2, "WORKS_FOR")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_start_node(self, node_graph):
        """Test getting start node of relationship"""
        node0 = node_graph.node(0)
        rels = node0.relationships(Direction.Outgoing)
        
        for rel in rels:
            start_node = rel.start_node()
            assert start_node.id() == 0
    
    def test_get_end_node(self, node_graph):
        """Test getting end node of relationship"""
        node0 = node_graph.node(0)
        rels = node0.relationships(Direction.Outgoing)
        
        end_ids = {rel.end_node().id() for rel in rels}
        assert end_ids == {1, 2}
    
    def test_start_end_consistency(self, node_graph):
        """Test that start and end nodes are consistent"""
        node0 = node_graph.node(0)
        node1 = node_graph.node(1)
        
        # Get relationship from node 0 to node 1
        rels = node0.relationships(Direction.Outgoing)
        knows_rel = next(rel for rel in rels if rel.end_node().id() == 1)
        
        assert knows_rel.start_node().id() == 0
        assert knows_rel.end_node().id() == 1
    
    def test_incoming_relationship_nodes(self, node_graph):
        """Test start/end nodes for incoming relationships"""
        node1 = node_graph.node(1)
        rels = node1.relationships(Direction.Incoming)
        
        assert len(rels) == 1
        rel = rels[0]
        assert rel.start_node().id() == 0
        assert rel.end_node().id() == 1


class TestRelationshipType:
    """Test Relationship type functionality"""
    
    @pytest.fixture
    def typed_graph(self):
        """Create a graph with different relationship types"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "WORKS_FOR")
        builder.add_rel(1, 2, "WORKS_FOR")
        builder.add_rel(1, 0, "KNOWS")  # Bidirectional
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_type(self, typed_graph):
        """Test getting relationship type"""
        node0 = typed_graph.node(0)
        rels = node0.relationships(Direction.Outgoing)
        
        types = {rel.reltype() for rel in rels}
        assert types == {"KNOWS", "WORKS_FOR"}
    
    def test_type_filtering(self, typed_graph):
        """Test filtering relationships by type"""
        node0 = typed_graph.node(0)
        knows_rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        
        assert len(knows_rels) == 1
        assert knows_rels[0].reltype() == "KNOWS"
        assert knows_rels[0].end_node().id() == 1
    
    def test_multiple_type_filtering(self, typed_graph):
        """Test filtering by multiple relationship types"""
        node0 = typed_graph.node(0)
        rels = node0.relationships(Direction.Both, ["KNOWS"])
        
        # Should get both outgoing and incoming KNOWS relationships
        assert len(rels) >= 1
        assert all(rel.reltype() == "KNOWS" for rel in rels)


class TestRelationshipProperties:
    """Test Relationship property functionality"""
    
    @pytest.fixture
    def property_graph(self):
        """Create a graph with relationship properties"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "WORKS_FOR")
        
        # Set relationship properties
        builder.set_rel_prop(0, 1, "KNOWS", "since", "2020")
        builder.set_rel_prop(0, 1, "KNOWS", "strength", 8)
        builder.set_rel_prop(0, 2, "WORKS_FOR", "since", "2018")
        builder.set_rel_prop(0, 2, "WORKS_FOR", "salary", 100000)
        builder.set_rel_prop(0, 2, "WORKS_FOR", "active", True)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_property_string(self, property_graph):
        """Test getting string property from relationship"""
        node0 = property_graph.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        
        assert len(rels) == 1
        rel = rels[0]
        assert rel.get_property("since") == "2020"
    
    def test_get_property_integer(self, property_graph):
        """Test getting integer property from relationship"""
        node0 = property_graph.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        
        assert len(rels) == 1
        rel = rels[0]
        assert rel.get_property("strength") == 8
    
    def test_get_property_float(self, property_graph):
        """Test getting float property from relationship"""
        node0 = property_graph.node(0)
        rels = node0.relationships(Direction.Outgoing, ["WORKS_FOR"])
        
        assert len(rels) == 1
        rel = rels[0]
        # Note: salary was set as int, but might be stored differently
        salary = rel.get_property("salary")
        assert salary == 100000 or salary == 100000.0
    
    def test_get_property_bool(self, property_graph):
        """Test getting boolean property from relationship"""
        node0 = property_graph.node(0)
        rels = node0.relationships(Direction.Outgoing, ["WORKS_FOR"])
        
        assert len(rels) == 1
        rel = rels[0]
        assert rel.get_property("active") is True
    
    def test_get_property_nonexistent(self, property_graph):
        """Test getting nonexistent property from relationship"""
        node0 = property_graph.node(0)
        rels = node0.relationships(Direction.Outgoing)
        
        assert len(rels) > 0
        rel = rels[0]
        assert rel.get_property("nonexistent") is None
    
    def test_multiple_properties(self, property_graph):
        """Test relationship with multiple properties"""
        node0 = property_graph.node(0)
        rels = node0.relationships(Direction.Outgoing, ["WORKS_FOR"])
        
        assert len(rels) == 1
        rel = rels[0]
        
        assert rel.get_property("since") == "2018"
        assert rel.get_property("salary") in [100000, 100000.0]
        assert rel.get_property("active") is True


class TestRelationshipSerialization:
    """Test Relationship serialization methods"""
    
    @pytest.fixture
    def serialization_graph(self):
        """Create a graph for serialization tests"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.set_rel_prop(0, 1, "KNOWS", "since", "2020")
        builder.set_rel_prop(0, 1, "KNOWS", "strength", 8)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_to_dict(self, serialization_graph):
        """Test converting relationship to dictionary"""
        node0 = serialization_graph.node(0)
        rels = node0.relationships(Direction.Outgoing)
        
        assert len(rels) == 1
        rel = rels[0]
        rel_dict = rel.to_dict()
        
        assert isinstance(rel_dict, dict)
        assert rel_dict["type"] == "KNOWS"
        assert rel_dict["start_node"] == 0
        assert rel_dict["end_node"] == 1
        assert rel_dict["properties"]["since"] == "2020"
        assert rel_dict["properties"]["strength"] == 8
    
    def test_to_json(self, serialization_graph):
        """Test converting relationship to JSON string"""
        node0 = serialization_graph.node(0)
        rels = node0.relationships(Direction.Outgoing)
        
        assert len(rels) == 1
        rel = rels[0]
        json_str = rel.to_json()
        
        assert isinstance(json_str, str)
        rel_dict = json.loads(json_str)
        
        assert rel_dict["type"] == "KNOWS"
        assert rel_dict["start_node"] == 0
        assert rel_dict["end_node"] == 1
        assert rel_dict["properties"]["since"] == "2020"
    
    def test_to_dict_no_properties(self):
        """Test to_dict for relationship without properties"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_rel(0, 1, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node0 = snapshot.node(0)
        rels = node0.relationships(Direction.Outgoing)
        
        assert len(rels) == 1
        rel = rels[0]
        rel_dict = rel.to_dict()
        
        assert rel_dict["type"] == "KNOWS"
        assert isinstance(rel_dict["properties"], dict)


class TestRelationshipEdgeCases:
    """Test Relationship edge cases and error conditions"""
    
    def test_self_loop(self):
        """Test relationship from node to itself"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_rel(0, 0, "SELF_REF")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node = snapshot.node(0)
        rels = node.relationships(Direction.Outgoing)
        
        assert len(rels) == 1
        rel = rels[0]
        assert rel.start_node().id() == 0
        assert rel.end_node().id() == 0
    
    def test_multiple_relationships_same_type(self):
        """Test multiple relationships of same type between nodes"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        
        # Add multiple KNOWS relationships (if deduplication allows)
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 1, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node0 = snapshot.node(0)
        rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        
        # Depending on deduplication, might have 1 or 2 relationships
        assert len(rels) >= 1
    
    def test_bidirectional_relationships(self):
        """Test bidirectional relationships"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(1, 0, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node0 = snapshot.node(0)
        node1 = snapshot.node(1)
        
        # Both should have outgoing KNOWS relationships
        rels0 = node0.relationships(Direction.Outgoing, ["KNOWS"])
        rels1 = node1.relationships(Direction.Outgoing, ["KNOWS"])
        
        assert len(rels0) == 1
        assert len(rels1) == 1
        assert rels0[0].end_node().id() == 1
        assert rels1[0].end_node().id() == 0

