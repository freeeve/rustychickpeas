"""Comprehensive tests for Direction enum"""

import pytest
from rustychickpeas import Direction, GraphSnapshotBuilder, RustyChickpeas


class TestDirection:
    """Test Direction enum values and usage"""
    
    def test_direction_values(self):
        """Test that Direction enum has the expected values"""
        assert Direction.Outgoing is not None
        assert Direction.Incoming is not None
        assert Direction.Both is not None
    
    def test_direction_equality(self):
        """Test Direction enum equality"""
        assert Direction.Outgoing == Direction.Outgoing
        assert Direction.Incoming == Direction.Incoming
        assert Direction.Both == Direction.Both
        assert Direction.Outgoing != Direction.Incoming
        assert Direction.Outgoing != Direction.Both
        assert Direction.Incoming != Direction.Both
    
    def test_direction_in_get_rels(self):
        """Test Direction usage in get_rels()"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        # Create a simple graph: 0 -> 1 -> 2
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        assert snapshot is not None
        
        node0 = snapshot.node(0)
        node1 = snapshot.node(1)
        node2 = snapshot.node(2)
        
        # Test outgoing relationships
        outgoing_rels = node0.relationships(Direction.Outgoing)
        assert len(outgoing_rels) == 1
        assert outgoing_rels[0].end_node().id() == 1
        
        # Test incoming relationships
        incoming_rels = node1.relationships(Direction.Incoming)
        assert len(incoming_rels) == 1
        assert incoming_rels[0].start_node().id() == 0
        
        # Test both directions
        both_rels = node1.relationships(Direction.Both)
        assert len(both_rels) == 2  # One incoming, one outgoing
    
    def test_direction_in_get_rel_ids(self):
        """Test Direction usage in get_rel_ids()"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node0 = snapshot.node(0)
        node1 = snapshot.node(1)
        
        # Test outgoing
        outgoing_ids = node0.relationship_ids(Direction.Outgoing)
        assert outgoing_ids == [1]
        
        # Test incoming
        incoming_ids = node1.relationship_ids(Direction.Incoming)
        assert incoming_ids == [0]
        
        # Test both
        both_ids = node1.relationship_ids(Direction.Both)
        assert set(both_ids) == {0, 2}
    
    def test_direction_in_get_degree(self):
        """Test Direction usage in get_degree()"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node0 = snapshot.node(0)
        node1 = snapshot.node(1)
        node2 = snapshot.node(2)
        
        # Node 0: 1 outgoing, 0 incoming
        assert node0.degree(Direction.Outgoing) == 1
        assert node0.degree(Direction.Incoming) == 0
        assert node0.degree(Direction.Both) == 1
        
        # Node 1: 1 outgoing, 1 incoming
        assert node1.degree(Direction.Outgoing) == 1
        assert node1.degree(Direction.Incoming) == 1
        assert node1.degree(Direction.Both) == 2
        
        # Node 2: 0 outgoing, 1 incoming
        assert node2.degree(Direction.Outgoing) == 0
        assert node2.degree(Direction.Incoming) == 1
        assert node2.degree(Direction.Both) == 1
    
    def test_direction_with_type_filter(self):
        """Test Direction with relationship type filtering"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "WORKS_WITH")
        builder.add_rel(1, 2, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        node0 = snapshot.node(0)
        
        # Test outgoing with type filter
        knows_rels = node0.relationships(Direction.Outgoing, ["KNOWS"])
        assert len(knows_rels) == 1
        assert knows_rels[0].reltype() == "KNOWS"
        
        # Test both with type filter
        all_knows = node0.relationships(Direction.Both, ["KNOWS"])
        assert len(all_knows) == 1  # Only outgoing KNOWS
        
        # Test incoming with type filter
        node2 = snapshot.node(2)
        incoming_knows = node2.relationships(Direction.Incoming, ["KNOWS"])
        assert len(incoming_knows) == 1
        assert incoming_knows[0].reltype() == "KNOWS"

