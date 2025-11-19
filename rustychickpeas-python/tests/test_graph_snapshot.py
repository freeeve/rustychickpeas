"""Comprehensive tests for GraphSnapshot class"""

import pytest
import tempfile
import os
from rustychickpeas import Direction, GraphSnapshot, GraphSnapshotBuilder, RustyChickpeas
import pyarrow as pa
import pyarrow.parquet as pq


class TestGraphSnapshotBasic:
    """Test basic GraphSnapshot functionality"""
    
    @pytest.fixture
    def sample_snapshot(self):
        """Create a sample snapshot for testing"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_node_count(self, sample_snapshot):
        """Test getting number of nodes"""
        assert sample_snapshot.node_count() == 3
    
    def test_relationship_count(self, sample_snapshot):
        """Test getting number of relationships"""
        assert sample_snapshot.relationship_count() == 0
    
    def test_relationship_count_with_relationships(self):
        """Test relationship_count with relationships"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_rel(0, 1, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        assert snapshot.relationship_count() == 1


class TestGraphSnapshotLabels:
    """Test GraphSnapshot label functionality"""
    
    @pytest.fixture
    def labeled_snapshot(self):
        """Create a snapshot with labeled nodes"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person", "User"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Company"], node_id=2)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_node_labels(self, labeled_snapshot):
        """Test getting labels for a node"""
        labels = labeled_snapshot.node_labels(0)
        assert set(labels) == {"Person", "User"}
        
        labels = labeled_snapshot.node_labels(1)
        assert labels == ["Person"]
    
    def test_get_nodes_with_label(self, labeled_snapshot):
        """Test getting nodes with a specific label"""
        person_nodes = labeled_snapshot.nodes_with_label("Person")
        assert set(person_nodes) == {0, 1}
        
        company_nodes = labeled_snapshot.nodes_with_label("Company")
        assert company_nodes == [2]
        
        user_nodes = labeled_snapshot.nodes_with_label("User")
        assert user_nodes == [0]
    
    def test_get_nodes_with_label_nonexistent(self, labeled_snapshot):
        """Test getting nodes with nonexistent label"""
        with pytest.raises(Exception):  # Should raise ValueError
            labeled_snapshot.nodes_with_label("Nonexistent")


class TestGraphSnapshotNodes:
    """Test GraphSnapshot node access"""
    
    @pytest.fixture
    def node_snapshot(self):
        """Create a snapshot for node tests"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_node(self, node_snapshot):
        """Test getting a node by ID"""
        node = node_snapshot.node(0)
        assert node.id() == 0
        
        node = node_snapshot.node(1)
        assert node.id() == 1
    
    def test_get_node_invalid_id(self, node_snapshot):
        """Test getting a node with invalid ID"""
        with pytest.raises(Exception):  # Should raise ValueError
            node_snapshot.node(999)
    
    def test_get_all_nodes(self, node_snapshot):
        """Test getting all nodes"""
        all_nodes = node_snapshot.all_nodes()
        assert set(all_nodes) == {0, 1}


class TestGraphSnapshotProperties:
    """Test GraphSnapshot property functionality"""
    
    @pytest.fixture
    def property_snapshot(self):
        """Create a snapshot with properties"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.set_prop(0, "name", "Alice")
        builder.set_prop(0, "age", 30)
        builder.set_prop(1, "name", "Bob")
        builder.set_prop(1, "age", 25)
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_node_property(self, property_snapshot):
        """Test getting a node property"""
        name = property_snapshot.node_property(0, "name")
        assert name == "Alice"
        
        age = property_snapshot.node_property(0, "age")
        assert age == 30
    
    def test_get_node_property_nonexistent(self, property_snapshot):
        """Test getting nonexistent property"""
        with pytest.raises(ValueError, match="Property key 'nonexistent' not found"):
            property_snapshot.node_property(0, "nonexistent")
    
    def test_get_nodes_with_property(self, property_snapshot):
        """Test getting nodes with a specific property value (label-scoped)"""
        # Note: This now requires a label parameter
        alice_nodes = property_snapshot.nodes_with_property("Person", "name", "Alice")
        assert alice_nodes == [0]
        
        bob_nodes = property_snapshot.nodes_with_property("Person", "name", "Bob")
        assert bob_nodes == [1]
        
        age_30_nodes = property_snapshot.nodes_with_property("Person", "age", 30)
        assert age_30_nodes == [0]
    
    def test_get_nodes_with_property_nonexistent_label(self, property_snapshot):
        """Test getting nodes with property for nonexistent label"""
        with pytest.raises(Exception):  # Should raise ValueError
            property_snapshot.nodes_with_property("Nonexistent", "name", "Alice")
    
    def test_get_nodes_with_property_nonexistent_key(self, property_snapshot):
        """Test getting nodes with nonexistent property key"""
        with pytest.raises(ValueError, match="Property key 'nonexistent' not found"):
            property_snapshot.nodes_with_property("Person", "nonexistent", "value")


class TestGraphSnapshotRelationships:
    """Test GraphSnapshot relationship functionality"""
    
    @pytest.fixture
    def relationship_snapshot(self):
        """Create a snapshot with relationships"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(0, 2, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        
        builder.set_version("test")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("test")
        
        return snapshot
    
    def test_get_rels(self, relationship_snapshot):
        """Test getting relationships"""
        rels = relationship_snapshot.relationships(0, Direction.Outgoing)
        assert len(rels) == 2
        
        end_ids = {rel.end_node().id() for rel in rels}
        assert end_ids == {1, 2}
    
    def test_get_rels_with_type_filter(self, relationship_snapshot):
        """Test getting relationships with type filter"""
        rels = relationship_snapshot.relationships(0, Direction.Outgoing, ["KNOWS"])
        assert len(rels) == 2
        assert all(rel.reltype() == "KNOWS" for rel in rels)
    
    def test_get_neighbor_ids(self, relationship_snapshot):
        """Test getting neighbor IDs"""
        neighbors = relationship_snapshot.neighbor_ids(0, Direction.Outgoing)
        assert set(neighbors) == {1, 2}
    
    def test_get_neighbors(self, relationship_snapshot):
        """Test getting neighbors as Node objects"""
        neighbors = relationship_snapshot.neighbors(0, Direction.Outgoing)
        assert len(neighbors) == 2
        
        neighbor_ids = {node.id() for node in neighbors}
        assert neighbor_ids == {1, 2}
    
    def test_get_degree(self, relationship_snapshot):
        """Test getting node degree"""
        assert relationship_snapshot.degree(0, Direction.Outgoing) == 2
        assert relationship_snapshot.degree(0, Direction.Incoming) == 0
        assert relationship_snapshot.degree(0, Direction.Both) == 2
    
    def test_get_relationships(self, relationship_snapshot):
        """Test getting relationships (returns neighbor IDs)"""
        rels = relationship_snapshot.neighbor_ids(0, Direction.Outgoing)
        assert set(rels) == {1, 2}
    
    def test_get_relationships_by_type(self, relationship_snapshot):
        """Test getting all relationships of a specific type"""
        knows_rels = relationship_snapshot.relationships_by_type("KNOWS")
        assert len(knows_rels) == 3  # All three relationships are KNOWS
        
        for rel in knows_rels:
            assert rel.reltype() == "KNOWS"
    
    def test_get_all_relationships(self, relationship_snapshot):
        """Test getting all relationships in the graph"""
        all_rels = relationship_snapshot.all_relationships()
        assert len(all_rels) == 3  # 3 relationships total
        
        # Check all are Relationship objects
        for rel in all_rels:
            assert hasattr(rel, "start_node")
            assert hasattr(rel, "end_node")
            assert hasattr(rel, "reltype")
    
    def test_get_relationship_by_index(self, relationship_snapshot):
        """Test getting a relationship by index"""
        # Get first relationship
        rel0 = relationship_snapshot.relationship(0)
        assert rel0 is not None
        assert rel0.id() == 0
        
        # Get second relationship
        rel1 = relationship_snapshot.relationship(1)
        assert rel1 is not None
        assert rel1.id() == 1
        
        # Invalid index should raise error
        with pytest.raises(Exception):  # ValueError
            relationship_snapshot.relationship(9999)
    
    def test_get_relationship_by_nodes(self, relationship_snapshot):
        """Test getting a relationship by node pair"""
        # Find relationship from node 0 to node 1
        rel = relationship_snapshot.relationship_by_nodes(0, 1)
        assert rel is not None
        assert rel.start_node().id() == 0
        assert rel.end_node().id() == 1
        assert rel.reltype() == "KNOWS"
        
        # Find relationship from node 0 to node 2
        rel = relationship_snapshot.relationship_by_nodes(0, 2)
        assert rel is not None
        assert rel.start_node().id() == 0
        assert rel.end_node().id() == 2
        assert rel.reltype() == "KNOWS"
        
        # Non-existent relationship
        rel = relationship_snapshot.relationship_by_nodes(0, 999)
        assert rel is None


class TestGraphSnapshotVersion:
    """Test GraphSnapshot version functionality"""
    
    def test_version(self):
        """Test getting snapshot version"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.set_version("v1.0")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("v1.0")
        
        assert snapshot.version() == "v1.0"
    
    def test_version_none(self):
        """Test snapshot without version"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.finalize_into(manager)
        # Get snapshot by checking versions
        versions = manager.versions()
        assert len(versions) > 0
        snapshot = manager.graph_snapshot(versions[0])
        
        # Version might be None if not set
        version = snapshot.version()
        # Either None or some default version
        assert version is None or isinstance(version, str)


class TestGraphSnapshotFromParquet:
    """Test creating GraphSnapshot from Parquet files"""
    
    def test_read_from_parquet(self):
        """Test reading snapshot from Parquet files"""
        # Create temporary Parquet files
        temp_dir = tempfile.mkdtemp()
        nodes_file = os.path.join(temp_dir, "nodes.parquet")
        rels_file = os.path.join(temp_dir, "rels.parquet")
        
        try:
            # Create nodes Parquet file
            nodes_data = {
                "node_id": [0, 1, 2],
                "label": ["Person", "Person", "Company"],
                "name": ["Alice", "Bob", "Acme"],
                "age": [30, 25, None],
            }
            nodes_table = pa.table(nodes_data)
            pq.write_table(nodes_table, nodes_file)
            
            # Create relationships Parquet file
            rels_data = {
                "start_node": [0, 1],
                "end_node": [1, 2],
                "rel_type": ["KNOWS", "WORKS_FOR"],
            }
            rels_table = pa.table(rels_data)
            pq.write_table(rels_table, rels_file)
            
            # Read from Parquet
            snapshot = GraphSnapshot.read_from_parquet(
                nodes_file,
                rels_file,
                node_id_column="node_id",
                label_columns=["label"],
                node_property_columns=["name", "age"],
                start_node_column="start_node",
                end_node_column="end_node",
                rel_type_column="rel_type",
            )
            
            assert snapshot.node_count() >= 2
            assert snapshot.relationship_count() >= 1
            
        finally:
            # Cleanup
            import shutil
            shutil.rmtree(temp_dir)


class TestGraphSnapshotEdgeCases:
    """Test GraphSnapshot edge cases"""
    
    def test_empty_snapshot(self):
        """Test empty snapshot"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.set_version("empty")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("empty")
        
        assert snapshot.node_count() == 0
        assert snapshot.relationship_count() == 0
        assert snapshot.all_nodes() == []
    
    def test_single_node(self):
        """Test snapshot with single node"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        
        builder.set_version("single")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("single")
        
        assert snapshot.node_count() == 1
        assert snapshot.relationship_count() == 0
        assert snapshot.all_nodes() == [0]
    
    def test_isolated_nodes(self):
        """Test snapshot with isolated nodes (no relationships)"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        
        builder.set_version("isolated")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("isolated")
        
        assert snapshot.node_count() == 3
        assert snapshot.relationship_count() == 0
        
        for node_id in [0, 1, 2]:
            assert snapshot.degree(node_id, Direction.Both) == 0
    
    def test_self_loop(self):
        """Test snapshot with self-loop relationship"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_rel(0, 0, "SELF_REF")
        
        builder.set_version("self_loop")
        builder.finalize_into(manager)
        snapshot = manager.graph_snapshot("self_loop")
        
        assert snapshot.relationship_count() == 1
        node = snapshot.node(0)
        rels = node.relationships(Direction.Outgoing)
        assert len(rels) == 1
        assert rels[0].end_node().id() == 0


class TestBidirectionalBFS:
    """Test bidirectional BFS functionality"""
    
    @pytest.fixture
    def path_snapshot(self):
        """Create a snapshot with paths: 0 -> 1 -> 2 -> 3, and 0 -> 4 -> 3"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_node(["Person"], node_id=3)
        builder.add_node(["Person"], node_id=4)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        builder.add_rel(2, 3, "KNOWS")
        builder.add_rel(0, 4, "KNOWS")
        builder.add_rel(4, 3, "KNOWS")
        
        builder.set_version("path_test")
        builder.finalize_into(manager)
        return manager.graph_snapshot("path_test")
    
    def test_bidirectional_bfs_basic(self, path_snapshot):
        """Test basic bidirectional BFS"""
        nodes, rels = path_snapshot.bidirectional_bfs([0], [3], Direction.Outgoing)
        
        # Should find nodes on paths from 0 to 3
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        assert len(rels) > 0


class TestBFS:
    """Test BFS traversal functionality"""
    
    @pytest.fixture
    def bfs_snapshot(self):
        """Create a snapshot with paths: 0 -> 1 -> 2 -> 3, 0 -> 4 -> 3, and 0 -> 5 (WORKS_FOR)"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_node(["Person"], node_id=3)
        builder.add_node(["Person"], node_id=4)
        builder.add_node(["Company"], node_id=5)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        builder.add_rel(2, 3, "KNOWS")
        builder.add_rel(0, 4, "KNOWS")
        builder.add_rel(4, 3, "KNOWS")
        builder.add_rel(0, 5, "WORKS_FOR")
        
        builder.set_version("bfs_test")
        builder.finalize_into(manager)
        return manager.graph_snapshot("bfs_test")
    
    def test_bfs_basic(self, bfs_snapshot):
        """Test basic BFS from a single node"""
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing)
        
        # Should find all nodes reachable from 0
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        assert 5 in nodes
        assert len(rels) > 0
    
    def test_bfs_with_rel_types(self, bfs_snapshot):
        """Test BFS with relationship type filter"""
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, rel_types=["KNOWS"])
        
        # Should find nodes via KNOWS relationships only
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        # Should NOT find node 5 (connected via WORKS_FOR)
        assert 5 not in nodes
    
    def test_bfs_with_node_filter(self, bfs_snapshot):
        """Test BFS with node filter"""
        def node_filter(node_id):
            return "Person" in bfs_snapshot.node_labels(node_id)
        
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, node_filter=node_filter)
        
        # Should find Person nodes only
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        # Should NOT find Company node
        assert 5 not in nodes
    
    def test_bfs_with_max_depth(self, bfs_snapshot):
        """Test BFS with max depth limit"""
        # With depth 1, should only reach direct neighbors
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, max_depth=1)
        
        assert 0 in nodes
        assert 1 in nodes
        assert 4 in nodes
        assert 5 in nodes
        # Should NOT find nodes at depth 2+
        assert 2 not in nodes
        assert 3 not in nodes
        
        # With depth 2, should reach nodes 2 and 3
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, max_depth=2)
        
        assert 2 in nodes
        assert 3 in nodes
    
    def test_bfs_with_direction_incoming(self, bfs_snapshot):
        """Test BFS with incoming direction (reverse traversal)"""
        nodes, rels = bfs_snapshot.bfs([3], Direction.Incoming)
        
        # Should find nodes reachable by following incoming edges
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
    
    def test_bfs_with_direction_both(self, bfs_snapshot):
        """Test BFS with both directions"""
        nodes, rels = bfs_snapshot.bfs([2], Direction.Both)
        
        # Should find nodes reachable in both directions
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
    
    def test_bfs_multiple_start_nodes(self, bfs_snapshot):
        """Test BFS from multiple starting nodes"""
        nodes, rels = bfs_snapshot.bfs([0, 2], Direction.Outgoing)
        
        # Should find nodes reachable from either start node
        assert 0 in nodes
        assert 2 in nodes
        assert 3 in nodes  # Reachable from both
        assert 1 in nodes  # Reachable from 0
    
    def test_bidirectional_bfs_no_path(self, path_snapshot):
        """Test bidirectional BFS when no path exists"""
        # No path from 3 to 0 (reverse direction)
        nodes, rels = path_snapshot.bidirectional_bfs([3], [0], Direction.Outgoing)
        
        # Should not find a path
        assert len(nodes) == 0 or 0 not in nodes or 3 not in nodes
    
    def test_bidirectional_bfs_with_rel_types(self, path_snapshot):
        """Test bidirectional BFS with relationship type filter"""
        nodes, rels = path_snapshot.bidirectional_bfs(
            [0], [3], Direction.Outgoing,
            rel_types=["KNOWS"]
        )
        
        # Should still find the path
        assert 0 in nodes
        assert 3 in nodes
    
    def test_bidirectional_bfs_with_node_filter(self, path_snapshot):
        """Test bidirectional BFS with node filter"""
        def node_filter(node_id):
            # Only allow Person nodes (all nodes in this test)
            return "Person" in path_snapshot.node_labels(node_id)
        
        nodes, rels = path_snapshot.bidirectional_bfs(
            [0], [3], Direction.Outgoing,
            node_filter=node_filter
        )
        
        # Should find path since all nodes have "Person" label
        assert 0 in nodes
        assert 3 in nodes
    
    def test_bidirectional_bfs_with_rel_filter(self, path_snapshot):
        """Test bidirectional BFS with relationship filter"""
        def rel_filter(from_node, to_node, rel_type, csr_pos):
            # Only allow KNOWS relationships
            return rel_type == "KNOWS"
        
        nodes, rels = path_snapshot.bidirectional_bfs(
            [0], [3],
            rel_filter=rel_filter
        )
        
        # Should find path
        assert 0 in nodes
        assert 3 in nodes
    
    def test_bidirectional_bfs_with_max_depth(self, path_snapshot):
        """Test bidirectional BFS with max depth limit"""
        # With depth 1, should not reach node 3
        nodes, _ = path_snapshot.bidirectional_bfs(
            [0], [3], Direction.Outgoing,
            max_depth=1
        )
        
        # Should not find node 3 with depth 1
        assert 3 not in nodes
        
        # With depth 3, should reach node 3
        nodes, _ = path_snapshot.bidirectional_bfs(
            [0], [3], Direction.Outgoing,
            max_depth=3
        )
        
        # Should find node 3 with depth 3
        assert 3 in nodes
    
    def test_bidirectional_bfs_multiple_sources_targets(self, path_snapshot):
        """Test bidirectional BFS with multiple source and target nodes"""
        nodes, rels = path_snapshot.bidirectional_bfs(
            [0, 1], [2, 3], Direction.Outgoing
        )
        
        # Should find intersection nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert len(rels) > 0


class TestBFS:
    """Test BFS traversal functionality"""
    
    @pytest.fixture
    def bfs_snapshot(self):
        """Create a snapshot with paths: 0 -> 1 -> 2 -> 3, 0 -> 4 -> 3, and 0 -> 5 (WORKS_FOR)"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_node(["Person"], node_id=3)
        builder.add_node(["Person"], node_id=4)
        builder.add_node(["Company"], node_id=5)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        builder.add_rel(2, 3, "KNOWS")
        builder.add_rel(0, 4, "KNOWS")
        builder.add_rel(4, 3, "KNOWS")
        builder.add_rel(0, 5, "WORKS_FOR")
        
        builder.set_version("bfs_test")
        builder.finalize_into(manager)
        return manager.graph_snapshot("bfs_test")
    
    def test_bfs_basic(self, bfs_snapshot):
        """Test basic BFS from a single node"""
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing)
        
        # Should find all nodes reachable from 0
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        assert 5 in nodes
        assert len(rels) > 0
    
    def test_bfs_with_rel_types(self, bfs_snapshot):
        """Test BFS with relationship type filter"""
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, rel_types=["KNOWS"])
        
        # Should find nodes via KNOWS relationships only
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        # Should NOT find node 5 (connected via WORKS_FOR)
        assert 5 not in nodes
    
    def test_bfs_with_node_filter(self, bfs_snapshot):
        """Test BFS with node filter"""
        def node_filter(node_id):
            return "Person" in bfs_snapshot.node_labels(node_id)
        
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, node_filter=node_filter)
        
        # Should find Person nodes only
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        # Should NOT find Company node
        assert 5 not in nodes
    
    def test_bfs_with_max_depth(self, bfs_snapshot):
        """Test BFS with max depth limit"""
        # With depth 1, should only reach direct neighbors
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, max_depth=1)
        
        assert 0 in nodes
        assert 1 in nodes
        assert 4 in nodes
        assert 5 in nodes
        # Should NOT find nodes at depth 2+
        assert 2 not in nodes
        assert 3 not in nodes
        
        # With depth 2, should reach nodes 2 and 3
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, max_depth=2)
        
        assert 2 in nodes
        assert 3 in nodes
    
    def test_bfs_with_direction_incoming(self, bfs_snapshot):
        """Test BFS with incoming direction (reverse traversal)"""
        nodes, rels = bfs_snapshot.bfs([3], Direction.Incoming)
        
        # Should find nodes reachable by following incoming edges
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
    
    def test_bfs_with_direction_both(self, bfs_snapshot):
        """Test BFS with both directions"""
        nodes, rels = bfs_snapshot.bfs([2], Direction.Both)
        
        # Should find nodes reachable in both directions
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
    
    def test_bfs_multiple_start_nodes(self, bfs_snapshot):
        """Test BFS from multiple starting nodes"""
        nodes, rels = bfs_snapshot.bfs([0, 2], Direction.Outgoing)
        
        # Should find nodes reachable from either start node
        assert 0 in nodes
        assert 2 in nodes
        assert 3 in nodes  # Reachable from both
        assert 1 in nodes  # Reachable from 0
    
    def test_bidirectional_bfs_overlapping_sources_targets(self, path_snapshot):
        """Test bidirectional BFS when source and target sets overlap"""
        # If source and target overlap, should return those nodes immediately
        nodes, rels = path_snapshot.bidirectional_bfs(
            [0, 1], [1, 2], Direction.Outgoing
        )
        
        # Should find overlapping nodes
        assert 1 in nodes
        assert 2 in nodes


class TestBFS:
    """Test BFS traversal functionality"""
    
    @pytest.fixture
    def bfs_snapshot(self):
        """Create a snapshot with paths: 0 -> 1 -> 2 -> 3, 0 -> 4 -> 3, and 0 -> 5 (WORKS_FOR)"""
        manager = RustyChickpeas()
        builder = manager.create_builder()
        
        builder.add_node(["Person"], node_id=0)
        builder.add_node(["Person"], node_id=1)
        builder.add_node(["Person"], node_id=2)
        builder.add_node(["Person"], node_id=3)
        builder.add_node(["Person"], node_id=4)
        builder.add_node(["Company"], node_id=5)
        
        builder.add_rel(0, 1, "KNOWS")
        builder.add_rel(1, 2, "KNOWS")
        builder.add_rel(2, 3, "KNOWS")
        builder.add_rel(0, 4, "KNOWS")
        builder.add_rel(4, 3, "KNOWS")
        builder.add_rel(0, 5, "WORKS_FOR")
        
        builder.set_version("bfs_test")
        builder.finalize_into(manager)
        return manager.graph_snapshot("bfs_test")
    
    def test_bfs_basic(self, bfs_snapshot):
        """Test basic BFS from a single node"""
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing)
        
        # Should find all nodes reachable from 0
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        assert 5 in nodes
        assert len(rels) > 0
    
    def test_bfs_with_rel_types(self, bfs_snapshot):
        """Test BFS with relationship type filter"""
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, rel_types=["KNOWS"])
        
        # Should find nodes via KNOWS relationships only
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        # Should NOT find node 5 (connected via WORKS_FOR)
        assert 5 not in nodes
    
    def test_bfs_with_node_filter(self, bfs_snapshot):
        """Test BFS with node filter"""
        def node_filter(node_id):
            return "Person" in bfs_snapshot.node_labels(node_id)
        
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, node_filter=node_filter)
        
        # Should find Person nodes only
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
        # Should NOT find Company node
        assert 5 not in nodes
    
    def test_bfs_with_max_depth(self, bfs_snapshot):
        """Test BFS with max depth limit"""
        # With depth 1, should only reach direct neighbors
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, max_depth=1)
        
        assert 0 in nodes
        assert 1 in nodes
        assert 4 in nodes
        assert 5 in nodes
        # Should NOT find nodes at depth 2+
        assert 2 not in nodes
        assert 3 not in nodes
        
        # With depth 2, should reach nodes 2 and 3
        nodes, rels = bfs_snapshot.bfs([0], Direction.Outgoing, max_depth=2)
        
        assert 2 in nodes
        assert 3 in nodes
    
    def test_bfs_with_direction_incoming(self, bfs_snapshot):
        """Test BFS with incoming direction (reverse traversal)"""
        nodes, rels = bfs_snapshot.bfs([3], Direction.Incoming)
        
        # Should find nodes reachable by following incoming edges
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
    
    def test_bfs_with_direction_both(self, bfs_snapshot):
        """Test BFS with both directions"""
        nodes, rels = bfs_snapshot.bfs([2], Direction.Both)
        
        # Should find nodes reachable in both directions
        assert 0 in nodes
        assert 1 in nodes
        assert 2 in nodes
        assert 3 in nodes
        assert 4 in nodes
    
    def test_bfs_multiple_start_nodes(self, bfs_snapshot):
        """Test BFS from multiple starting nodes"""
        nodes, rels = bfs_snapshot.bfs([0, 2], Direction.Outgoing)
        
        # Should find nodes reachable from either start node
        assert 0 in nodes
        assert 2 in nodes
        assert 3 in nodes  # Reachable from both
        assert 1 in nodes  # Reachable from 0

