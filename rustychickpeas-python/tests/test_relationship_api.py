"""Tests for Relationship and Node API functionality

Tests the new Relationship class and updated Node.get_rels() method
that returns Relationship objects instead of Node objects.
"""

import pytest
import json
from rustychickpeas import Direction, RustyChickpeas


@pytest.fixture
def sample_graph():
    """Create a sample graph for testing"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Create nodes (using 0-indexed IDs to match test expectations)
    builder.add_node(0, ["Person"])
    builder.add_node(1, ["Person"])
    builder.add_node(2, ["Company"])
    builder.add_node(3, ["Person"])
    
    # Create relationships
    builder.add_rel(0, 1, "KNOWS")
    builder.add_rel(0, 2, "WORKS_FOR")
    builder.add_rel(1, 2, "WORKS_FOR")
    builder.add_rel(3, 0, "KNOWS")
    
    # Add properties
    builder.set_prop(0, "name", "Alice")
    builder.set_prop(0, "age", 30)
    builder.set_prop(1, "name", "Bob")
    builder.set_prop(2, "name", "Acme Corp")
    
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    return snapshot, manager


class TestNodeGetRels:
    """Test Node.get_rels() returning Relationship objects"""
    
    def test_get_rels_outgoing(self, sample_graph):
        """Test getting outgoing relationships as Relationship objects"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)  # Node 1 (Alice)
        
        rels = node.get_rels(Direction.Outgoing)
        assert len(rels) == 2
        
        # Check relationship types
        rel_types = [rel.get_type() for rel in rels]
        assert "KNOWS" in rel_types
        assert "WORKS_FOR" in rel_types
        
        # Check start/end nodes
        for rel in rels:
            assert rel.get_start_node().id() == 0  # All start from node 0
            assert rel.get_end_node().id() in [1, 2]  # End at node 1 or 2
    
    def test_get_rels_incoming(self, sample_graph):
        """Test getting incoming relationships as Relationship objects"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(1)  # Node 2 (Bob)
        
        rels = node.get_rels(Direction.Incoming)
        assert len(rels) == 1
        
        rel = rels[0]
        assert rel.get_type() == "KNOWS"
        assert rel.get_start_node().id() == 0  # From Alice
        assert rel.get_end_node().id() == 1  # To Bob
    
    def test_get_rels_both(self, sample_graph):
        """Test getting both directions of relationships"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)  # Node 1 (Alice)
        
        rels = node.get_rels(Direction.Both)
        assert len(rels) == 3  # 2 outgoing + 1 incoming
        
        # Check we have both directions
        outgoing_count = sum(1 for rel in rels if rel.get_start_node().id() == 0)
        incoming_count = sum(1 for rel in rels if rel.get_end_node().id() == 0)
        
        assert outgoing_count == 2
        assert incoming_count == 1
    
    def test_get_rels_type_filter(self, sample_graph):
        """Test filtering relationships by type"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)  # Node 1 (Alice)
        
        # Filter by KNOWS
        knows_rels = node.get_rels(Direction.Outgoing, rel_types=["KNOWS"])
        assert len(knows_rels) == 1
        assert knows_rels[0].get_type() == "KNOWS"
        
        # Filter by WORKS_FOR
        works_for_rels = node.get_rels(Direction.Outgoing, rel_types=["WORKS_FOR"])
        assert len(works_for_rels) == 1
        assert works_for_rels[0].get_type() == "WORKS_FOR"
        
        # Filter by multiple types
        both_types = node.get_rels(Direction.Outgoing, rel_types=["KNOWS", "WORKS_FOR"])
        assert len(both_types) == 2
        
        # Filter by non-existent type
        no_rels = node.get_rels(Direction.Outgoing, rel_types=["NONEXISTENT"])
        assert len(no_rels) == 0
    
    def test_get_rels_type_filter_incoming(self, sample_graph):
        """Test type filtering on incoming relationships"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(2)  # Node 3 (Company)
        
        # Should have 2 incoming WORKS_FOR relationships
        works_for_rels = node.get_rels(Direction.Incoming, rel_types=["WORKS_FOR"])
        assert len(works_for_rels) == 2
        
        # Should have no KNOWS relationships
        knows_rels = node.get_rels(Direction.Incoming, rel_types=["KNOWS"])
        assert len(knows_rels) == 0


class TestNodeGetRelIds:
    """Test Node.get_rel_ids() returning neighbor node IDs"""
    
    def test_get_rel_ids_outgoing(self, sample_graph):
        """Test getting outgoing neighbor IDs"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)  # Node 1 (Alice)
        
        neighbor_ids = node.get_rel_ids(Direction.Outgoing)
        assert len(neighbor_ids) == 2
        assert 1 in neighbor_ids  # Bob
        assert 2 in neighbor_ids  # Company
    
    def test_get_rel_ids_incoming(self, sample_graph):
        """Test getting incoming neighbor IDs"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(1)  # Node 2 (Bob)
        
        neighbor_ids = node.get_rel_ids(Direction.Incoming)
        assert len(neighbor_ids) == 1
        assert 0 in neighbor_ids  # Alice
    
    def test_get_rel_ids_both(self, sample_graph):
        """Test getting both directions of neighbor IDs"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)  # Node 1 (Alice)
        
        neighbor_ids = node.get_rel_ids(Direction.Both)
        assert len(neighbor_ids) == 3  # 2 outgoing + 1 incoming
        assert 1 in neighbor_ids  # Bob (outgoing)
        assert 2 in neighbor_ids  # Company (outgoing)
        assert 3 in neighbor_ids  # Person 4 (incoming)
    
    def test_get_rel_ids_type_filter(self, sample_graph):
        """Test filtering neighbor IDs by relationship type"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)  # Node 1 (Alice)
        
        # Filter by KNOWS
        knows_ids = node.get_rel_ids(Direction.Outgoing, rel_types=["KNOWS"])
        assert len(knows_ids) == 1
        assert 1 in knows_ids  # Bob
        
        # Filter by WORKS_FOR
        works_for_ids = node.get_rel_ids(Direction.Outgoing, rel_types=["WORKS_FOR"])
        assert len(works_for_ids) == 1
        assert 2 in works_for_ids  # Company


class TestRelationshipClass:
    """Test Relationship class methods"""
    
    def test_relationship_get_start_node(self, sample_graph):
        """Test Relationship.get_start_node()"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)
        rels = node.get_rels(Direction.Outgoing)
        
        for rel in rels:
            start_node = rel.get_start_node()
            assert start_node.id() == 0
            assert isinstance(start_node, snapshot.get_node(0).__class__)
    
    def test_relationship_get_end_node(self, sample_graph):
        """Test Relationship.get_end_node()"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)
        rels = node.get_rels(Direction.Outgoing)
        
        end_ids = [rel.get_end_node().id() for rel in rels]
        assert 1 in end_ids  # Bob
        assert 2 in end_ids  # Company
    
    def test_relationship_get_type(self, sample_graph):
        """Test Relationship.get_type()"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)
        rels = node.get_rels(Direction.Outgoing)
        
        types = [rel.get_type() for rel in rels]
        assert "KNOWS" in types
        assert "WORKS_FOR" in types
    
    def test_relationship_id(self, sample_graph):
        """Test Relationship.id() returns relationship index"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)
        rels = node.get_rels(Direction.Outgoing)
        
        # All relationships should have valid IDs (indices)
        for rel in rels:
            rel_id = rel.id()
            assert isinstance(rel_id, int)
            assert rel_id >= 0
    
    def test_relationship_to_dict(self, sample_graph):
        """Test Relationship.to_dict()"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)
        rels = node.get_rels(Direction.Outgoing)
        
        for rel in rels:
            rel_dict = rel.to_dict()
            
            # Check structure
            assert "id" in rel_dict
            assert "type" in rel_dict
            assert "start_node" in rel_dict
            assert "end_node" in rel_dict
            assert "properties" in rel_dict
            
            # Check types
            assert isinstance(rel_dict["id"], int)
            assert isinstance(rel_dict["type"], str)
            assert isinstance(rel_dict["start_node"], int)
            assert isinstance(rel_dict["end_node"], int)
            assert isinstance(rel_dict["properties"], dict)
            
            # Check values match
            assert rel_dict["type"] == rel.get_type()
            assert rel_dict["start_node"] == rel.get_start_node().id()
            assert rel_dict["end_node"] == rel.get_end_node().id()
    
    def test_relationship_to_json(self, sample_graph):
        """Test Relationship.to_json()"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)
        rels = node.get_rels(Direction.Outgoing)
        
        for rel in rels:
            json_str = rel.to_json()
            assert isinstance(json_str, str)
            
            # Parse JSON to verify it's valid
            rel_dict = json.loads(json_str)
            assert "id" in rel_dict
            assert "type" in rel_dict
            assert "start_node" in rel_dict
            assert "end_node" in rel_dict
            assert "properties" in rel_dict


class TestGraphSnapshotRelationshipMethods:
    """Test GraphSnapshot methods for relationships"""
    
    def test_get_all_relationships(self, sample_graph):
        """Test GraphSnapshot.get_all_relationships()"""
        snapshot, _ = sample_graph
        
        all_rels = snapshot.get_all_relationships()
        assert len(all_rels) == 4  # 4 relationships total
        
        # Check all are Relationship objects
        for rel in all_rels:
            assert hasattr(rel, "get_start_node")
            assert hasattr(rel, "get_end_node")
            assert hasattr(rel, "get_type")
    
    def test_get_relationships_by_type(self, sample_graph):
        """Test GraphSnapshot.get_relationships_by_type()"""
        snapshot, _ = sample_graph
        
        # Get all KNOWS relationships
        knows_rels = snapshot.get_relationships_by_type("KNOWS")
        assert len(knows_rels) == 2
        
        for rel in knows_rels:
            assert rel.get_type() == "KNOWS"
        
        # Get all WORKS_FOR relationships
        works_for_rels = snapshot.get_relationships_by_type("WORKS_FOR")
        assert len(works_for_rels) == 2
        
        for rel in works_for_rels:
            assert rel.get_type() == "WORKS_FOR"
        
        # Non-existent type should raise ValueError
        with pytest.raises(ValueError, match="Relationship type 'NONEXISTENT' not found"):
            snapshot.get_relationships_by_type("NONEXISTENT")
    
    def test_get_relationship_by_index(self, sample_graph):
        """Test GraphSnapshot.get_relationship() by index"""
        snapshot, _ = sample_graph
        
        # Get first relationship
        rel0 = snapshot.get_relationship(0)
        assert rel0 is not None
        assert rel0.id() == 0
        
        # Get second relationship
        rel1 = snapshot.get_relationship(1)
        assert rel1 is not None
        assert rel1.id() == 1
        
        # Invalid index should raise error
        with pytest.raises(Exception):  # ValueError or similar
            snapshot.get_relationship(9999)
    
    def test_get_relationship_by_nodes(self, sample_graph):
        """Test GraphSnapshot.get_relationship_by_nodes()"""
        snapshot, _ = sample_graph
        
        # Find relationship from node 0 to node 1
        rel = snapshot.get_relationship_by_nodes(0, 1)
        assert rel is not None
        assert rel.get_start_node().id() == 0
        assert rel.get_end_node().id() == 1
        assert rel.get_type() == "KNOWS"
        
        # Find relationship from node 0 to node 2
        rel = snapshot.get_relationship_by_nodes(0, 2)
        assert rel is not None
        assert rel.get_start_node().id() == 0
        assert rel.get_end_node().id() == 2
        assert rel.get_type() == "WORKS_FOR"
        
        # Non-existent relationship
        rel = snapshot.get_relationship_by_nodes(0, 999)
        assert rel is None


class TestNodeToDict:
    """Test Node.to_dict() and Node.to_json()"""
    
    def test_node_to_dict(self, sample_graph):
        """Test Node.to_dict() structure"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)  # Alice
        
        node_dict = node.to_dict()
        
        # Check structure
        assert "id" in node_dict
        assert "labels" in node_dict
        assert "properties" in node_dict
        
        # Check types
        assert isinstance(node_dict["id"], int)
        assert isinstance(node_dict["labels"], list)
        assert isinstance(node_dict["properties"], dict)
        
        # Check values
        assert node_dict["id"] == 0
        assert "Person" in node_dict["labels"]
        assert node_dict["properties"]["name"] == "Alice"
        assert node_dict["properties"]["age"] == 30
    
    def test_node_to_json(self, sample_graph):
        """Test Node.to_json()"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)  # Alice
        
        json_str = node.to_json()
        assert isinstance(json_str, str)
        
        # Parse JSON to verify it's valid
        node_dict = json.loads(json_str)
        assert "id" in node_dict
        assert "labels" in node_dict
        assert "properties" in node_dict
        assert node_dict["properties"]["name"] == "Alice"


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_node_with_no_relationships(self, sample_graph):
        """Test node with no relationships"""
        snapshot, _ = sample_graph
        # Node 3 (Company) has no outgoing relationships
        node = snapshot.get_node(2)
        
        outgoing = node.get_rels(Direction.Outgoing)
        assert len(outgoing) == 0
        
        outgoing_ids = node.get_rel_ids(Direction.Outgoing)
        assert len(outgoing_ids) == 0
    
    def test_empty_type_filter(self, sample_graph):
        """Test empty type filter behavior"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(0)
        
        # Empty list is treated as "filter provided but no types match" -> returns empty
        rels = node.get_rels(Direction.Outgoing, rel_types=[])
        assert len(rels) == 0
        
        # None should return all (no filter)
        rels = node.get_rels(Direction.Outgoing, rel_types=None)
        assert len(rels) == 2
    
    def test_multiple_relationships_same_type(self, sample_graph):
        """Test multiple relationships of the same type"""
        snapshot, _ = sample_graph
        node = snapshot.get_node(2)  # Company
        
        # Company has 2 incoming WORKS_FOR relationships
        incoming = node.get_rels(Direction.Incoming, rel_types=["WORKS_FOR"])
        assert len(incoming) == 2
        
        # All should be WORKS_FOR
        for rel in incoming:
            assert rel.get_type() == "WORKS_FOR"
            assert rel.get_end_node().id() == 2  # All end at company


class TestRelationshipProperties:
    """Test relationship property functionality"""
    
    def test_set_rel_property_str(self):
        """Test setting string property on relationship"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        builder.set_rel_prop_str(1, 2, "KNOWS", "since", "2020")
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        assert len(rels) == 1
        rel = rels[0]
        assert rel.get_property("since") == "2020"
    
    def test_set_rel_property_i64(self):
        """Test setting integer property on relationship"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        builder.set_rel_prop_i64(1, 2, "KNOWS", "strength", 95)
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        rel = rels[0]
        assert rel.get_property("strength") == 95
    
    def test_set_rel_property_f64(self):
        """Test setting float property on relationship"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        builder.set_rel_prop_f64(1, 2, "KNOWS", "weight", 0.85)
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        rel = rels[0]
        assert rel.get_property("weight") == pytest.approx(0.85)
    
    def test_set_rel_property_bool(self):
        """Test setting boolean property on relationship"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        builder.set_rel_prop_bool(1, 2, "KNOWS", "verified", True)
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        rel = rels[0]
        assert rel.get_property("verified") is True
    
    def test_set_rel_prop_auto_type_detection(self):
        """Test set_rel_prop with automatic type detection"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        
        # Test all types with automatic detection
        builder.set_rel_prop(1, 2, "KNOWS", "since", "2020")
        builder.set_rel_prop(1, 2, "KNOWS", "strength", 95)
        builder.set_rel_prop(1, 2, "KNOWS", "weight", 0.85)
        builder.set_rel_prop(1, 2, "KNOWS", "verified", True)
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        rel = rels[0]
        assert rel.get_property("since") == "2020"
        assert rel.get_property("strength") == 95
        assert rel.get_property("weight") == pytest.approx(0.85)
        assert rel.get_property("verified") is True
    
    def test_multiple_relationship_properties(self):
        """Test multiple relationships with different properties"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_node(3, ["Company"])
        
        builder.add_rel(1, 2, "KNOWS")
        builder.add_rel(1, 3, "WORKS_FOR")
        
        builder.set_rel_prop(1, 2, "KNOWS", "since", "2020")
        builder.set_rel_prop(1, 2, "KNOWS", "strength", 95)
        builder.set_rel_prop(1, 3, "WORKS_FOR", "since", "2018")
        builder.set_rel_prop(1, 3, "WORKS_FOR", "role", "Engineer")
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        assert len(rels) == 2
        
        # Find KNOWS relationship
        knows_rel = next(r for r in rels if r.get_type() == "KNOWS")
        assert knows_rel.get_property("since") == "2020"
        assert knows_rel.get_property("strength") == 95
        # Note: For dense columns, unset properties may return default values (empty string/0)
        # For sparse columns, unset properties return None
        role_val = knows_rel.get_property("role")
        assert role_val is None or role_val == ""  # Not set on this relationship
        
        # Find WORKS_FOR relationship
        works_rel = next(r for r in rels if r.get_type() == "WORKS_FOR")
        assert works_rel.get_property("since") == "2018"
        assert works_rel.get_property("role") == "Engineer"
        # Note: For dense columns, unset properties may return default values (empty string/0)
        strength_val = works_rel.get_property("strength")
        assert strength_val is None or strength_val == 0  # Not set on this relationship
    
    def test_relationship_property_none(self):
        """Test getting property that doesn't exist returns None"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        # Don't set any properties
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        rel = rels[0]
        assert rel.get_property("nonexistent") is None
    
    def test_relationship_properties_in_to_dict(self):
        """Test that relationship properties appear in to_dict()"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        builder.set_rel_prop(1, 2, "KNOWS", "since", "2020")
        builder.set_rel_prop(1, 2, "KNOWS", "strength", 95)
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        rel = rels[0]
        rel_dict = rel.to_dict()
        
        assert "properties" in rel_dict
        assert rel_dict["properties"]["since"] == "2020"
        assert rel_dict["properties"]["strength"] == 95
    
    def test_relationship_properties_in_to_json(self):
        """Test that relationship properties appear in to_json()"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        builder.set_rel_prop(1, 2, "KNOWS", "since", "2020")
        builder.set_rel_prop(1, 2, "KNOWS", "strength", 95)
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        rel = rels[0]
        json_str = rel.to_json()
        rel_dict = json.loads(json_str)
        
        assert "properties" in rel_dict
        assert rel_dict["properties"]["since"] == "2020"
        assert rel_dict["properties"]["strength"] == 95
    
    def test_relationship_properties_empty_dict(self):
        """Test that relationships without properties have empty properties dict"""
        manager = RustyChickpeas()
        builder = manager.create_builder(version="v1.0")
        
        builder.add_node(1, ["Person"])
        builder.add_node(2, ["Person"])
        builder.add_rel(1, 2, "KNOWS")
        # No properties set
        
        builder.finalize_into(manager)
        snapshot = manager.get_graph_snapshot("v1.0")
        
        rels = snapshot.get_rels(1, Direction.Outgoing)
        rel = rels[0]
        rel_dict = rel.to_dict()
        
        assert "properties" in rel_dict
        assert isinstance(rel_dict["properties"], dict)
        assert len(rel_dict["properties"]) == 0

