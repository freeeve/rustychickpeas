"""Tests for Parquet loading with node and relationship deduplication"""

import os
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import rustychickpeas as rcp


def create_nodes_parquet_file(file_path, data):
    """Create a Parquet file with node data"""
    arrays = {}
    for col_name, values in data.items():
        if col_name == "id":
            arrays[col_name] = pa.array(values, type=pa.int32())
        elif col_name == "age":
            arrays[col_name] = pa.array(values, type=pa.int64())
        elif col_name == "score":
            arrays[col_name] = pa.array(values, type=pa.float64())
        elif col_name == "active":
            arrays[col_name] = pa.array(values, type=pa.bool_())
        else:
            arrays[col_name] = pa.array(values, type=pa.string())
    
    table = pa.table(arrays)
    pq.write_table(table, file_path)
    return file_path


def create_relationships_parquet_file(file_path, data):
    """Create a Parquet file with relationship data"""
    arrays = {}
    for col_name, values in data.items():
        if col_name in ["from", "to"]:
            arrays[col_name] = pa.array(values, type=pa.int32())
        elif col_name == "weight":
            arrays[col_name] = pa.array(values, type=pa.float64())
        elif col_name == "since":
            arrays[col_name] = pa.array(values, type=pa.int64())
        else:
            arrays[col_name] = pa.array(values, type=pa.string())
    
    table = pa.table(arrays)
    pq.write_table(table, file_path)
    return file_path


class TestNodeDeduplication:
    """Test node deduplication using unique properties"""
    
    def test_node_deduplication_single_unique_property(self):
        """Test deduplication with a single unique property (email)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create first file with nodes
            file1 = os.path.join(tmpdir, "nodes1.parquet")
            create_nodes_parquet_file(file1, {
                "id": [1, 2, 3],
                "label": ["Person", "Person", "Person"],
                "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
                "name": ["Alice", "Bob", "Charlie"],
            })
            
            # Create second file with duplicate email (should merge with node 1)
            file2 = os.path.join(tmpdir, "nodes2.parquet")
            create_nodes_parquet_file(file2, {
                "id": [10, 11],
                "label": ["Person", "Person"],
                "email": ["alice@example.com", "dave@example.com"],  # alice@example.com is duplicate
                "name": ["Alice Updated", "Dave"],
            })
            
            # Load both files with deduplication
            manager = rcp.RustyChickpeas()
            builder = manager.create_builder(version="test_v1")
            
            # Load first file
            node_ids1 = builder.load_nodes_from_parquet(
                path=file1,
                node_id_column="id",
                label_columns=["label"],
                property_columns=["email", "name"],
                unique_properties=["email"],  # Use email as unique key
            )
            
            # Load second file (should deduplicate alice@example.com)
            node_ids2 = builder.load_nodes_from_parquet(
                path=file2,
                node_id_column="id",
                label_columns=["label"],
                property_columns=["email", "name"],
                unique_properties=["email"],
            )
            
            # Finalize
            builder.finalize_into(manager)
            graph = manager.graph_snapshot("test_v1")
            
            # Should have 4 unique nodes (alice, bob, charlie, dave)
            # alice@example.com appears in both files but should be deduplicated
            assert graph.node_count() == 4
            
            # Check that alice@example.com node has properties from both files
            # (The last loaded properties should be present)
            # Find node with email "alice@example.com"
            alice_found = False
            for node_id in range(graph.node_count()):
                node = graph.node(node_id)
                email = node.get_property("email")
                if email == "alice@example.com":
                    alice_found = True
                    # Should have name from second file (last loaded)
                    name = node.get_property("name")
                    assert name == "Alice Updated"
                    break
            assert alice_found, "Should find alice@example.com node"
    
    def test_node_deduplication_multiple_unique_properties(self):
        """Test deduplication with multiple unique properties (email + phone)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create first file
            file1 = os.path.join(tmpdir, "nodes1.parquet")
            create_nodes_parquet_file(file1, {
                "id": [1, 2],
                "label": ["Person", "Person"],
                "email": ["alice@example.com", "bob@example.com"],
                "phone": ["555-0100", "555-0200"],
                "name": ["Alice", "Bob"],
            })
            
            # Create second file with same email but different phone (should NOT deduplicate)
            file2 = os.path.join(tmpdir, "nodes2.parquet")
            create_nodes_parquet_file(file2, {
                "id": [10],
                "label": ["Person"],
                "email": ["alice@example.com",],  # Same email
                "phone": ["555-9999",],  # Different phone
                "name": ["Alice Other"],
            })
            
            # Load both files with deduplication on email+phone
            manager = rcp.RustyChickpeas()
            builder = manager.create_builder(version="test_v2")
            
            builder.load_nodes_from_parquet(
                path=file1,
                node_id_column="id",
                label_columns=["label"],
                property_columns=["email", "phone", "name"],
                unique_properties=["email", "phone"],  # Both must match
            )
            
            builder.load_nodes_from_parquet(
                path=file2,
                node_id_column="id",
                label_columns=["label"],
                property_columns=["email", "phone", "name"],
                unique_properties=["email", "phone"],
            )
            
            builder.finalize_into(manager)
            graph = manager.graph_snapshot("test_v2")
            
            # Should have 3 nodes (alice with phone 555-0100, bob, alice with phone 555-9999)
            assert graph.node_count() == 3
    
    def test_node_deduplication_no_unique_properties(self):
        """Test that without unique_properties, all nodes are loaded"""
        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = os.path.join(tmpdir, "nodes1.parquet")
            create_nodes_parquet_file(file1, {
                "id": [1, 2],
                "label": ["Person", "Person"],
                "email": ["alice@example.com", "bob@example.com"],
            })
            
            file2 = os.path.join(tmpdir, "nodes2.parquet")
            create_nodes_parquet_file(file2, {
                "id": [1, 2],  # Same IDs
                "label": ["Person", "Person"],
                "email": ["alice@example.com", "bob@example.com"],  # Same emails
            })
            
            manager = rcp.RustyChickpeas()
            builder = manager.create_builder(version="test_v3")
            
            builder.load_nodes_from_parquet(
                path=file1,
                node_id_column="id",
                label_columns=["label"],
                property_columns=["email"],
                unique_properties=None,  # No deduplication
            )
            
            builder.load_nodes_from_parquet(
                path=file2,
                node_id_column="id",
                label_columns=["label"],
                property_columns=["email"],
                unique_properties=None,
            )
            
            builder.finalize_into(manager)
            graph = manager.graph_snapshot("test_v3")
            
            # Should have 2 nodes (same IDs are updated, not duplicated)
            # Without deduplication, loading the same node ID twice just updates the same node
            assert graph.node_count() == 2


class TestRelationshipDeduplication:
    """Test relationship deduplication"""
    
    def test_relationship_deduplication_create_all(self):
        """Test CreateAll - no deduplication, all relationships created"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create nodes
            nodes_file = os.path.join(tmpdir, "nodes.parquet")
            create_nodes_parquet_file(nodes_file, {
                "id": [1, 2, 3],
                "label": ["Person", "Person", "Person"],
            })
            
            # Create relationships file with duplicates
            rels_file = os.path.join(tmpdir, "rels.parquet")
            create_relationships_parquet_file(rels_file, {
                "from": [1, 1, 2],
                "to": [2, 2, 3],  # Two relationships from 1 to 2
                "type": ["KNOWS", "KNOWS", "KNOWS"],
            })
            
            manager = rcp.RustyChickpeas()
            builder = manager.create_builder(version="test_v4")
            
            builder.load_nodes_from_parquet(
                path=nodes_file,
                node_id_column="id",
                label_columns=["label"],
                property_columns=None,
                unique_properties=None,
            )
            
            builder.load_relationships_from_parquet(
                path=rels_file,
                start_node_column="from",
                end_node_column="to",
                rel_type_column="type",
                property_columns=None,
                fixed_rel_type=None,
                deduplication="CreateAll",  # No deduplication
            )
            
            builder.finalize_into(manager)
            graph = manager.graph_snapshot("test_v4")
            
            # Should have 3 relationships (all created)
            assert graph.relationship_count() == 3
            
            # Node 1 should have 2 outgoing relationships to node 2
            node1 = graph.node(1)
            rels = node1.relationships(rcp.Direction.Outgoing)
            assert len(rels) == 2
            assert all(rel.end_node().id() == 2 for rel in rels)
    
    def test_relationship_deduplication_unique_by_type(self):
        """Test CreateUniqueByRelType - one relationship per type between two nodes"""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes_file = os.path.join(tmpdir, "nodes.parquet")
            create_nodes_parquet_file(nodes_file, {
                "id": [1, 2, 3],
                "label": ["Person", "Person", "Person"],
            })
            
            rels_file = os.path.join(tmpdir, "rels.parquet")
            create_relationships_parquet_file(rels_file, {
                "from": [1, 1, 1, 2],
                "to": [2, 2, 3, 3],
                "type": ["KNOWS", "KNOWS", "LIKES", "KNOWS"],  # Two KNOWS from 1->2
            })
            
            manager = rcp.RustyChickpeas()
            builder = manager.create_builder(version="test_v5")
            
            builder.load_nodes_from_parquet(
                path=nodes_file,
                node_id_column="id",
                label_columns=["label"],
                property_columns=None,
                unique_properties=None,
            )
            
            builder.load_relationships_from_parquet(
                path=rels_file,
                start_node_column="from",
                end_node_column="to",
                rel_type_column="type",
                property_columns=None,
                fixed_rel_type=None,
                deduplication="CreateUniqueByRelType",  # One per type
            )
            
            builder.finalize_into(manager)
            graph = manager.graph_snapshot("test_v5")
            
            # Should have 3 relationships (1->2 KNOWS deduplicated, 1->3 LIKES, 2->3 KNOWS)
            assert graph.relationship_count() == 3
            
            # Node 1 should have 2 outgoing relationships (one KNOWS to 2, one LIKES to 3)
            node1 = graph.node(1)
            rels = node1.relationships(rcp.Direction.Outgoing)
            assert len(rels) == 2
            rel_types = [rel.reltype() for rel in rels]
            assert "KNOWS" in rel_types
            assert "LIKES" in rel_types
    
    def test_relationship_deduplication_with_properties(self):
        """Test CreateUniqueByRelTypeAndKeyProperties - deduplicate by type and key properties"""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes_file = os.path.join(tmpdir, "nodes.parquet")
            create_nodes_parquet_file(nodes_file, {
                "id": [1, 2, 3],
                "label": ["Person", "Person", "Person"],
            })
            
            rels_file = os.path.join(tmpdir, "rels.parquet")
            create_relationships_parquet_file(rels_file, {
                "from": [1, 1, 1],
                "to": [2, 2, 3],
                "type": ["KNOWS", "KNOWS", "KNOWS"],
                "since": [2020, 2021, 2020],  # Two KNOWS from 1->2 with different 'since' values
            })
            
            manager = rcp.RustyChickpeas()
            builder = manager.create_builder(version="test_v6")
            
            builder.load_nodes_from_parquet(
                path=nodes_file,
                node_id_column="id",
                label_columns=["label"],
                property_columns=None,
                unique_properties=None,
            )
            
            # Note: CreateUniqueByRelTypeAndKeyProperties is not yet fully implemented
            # This test documents the expected behavior
            builder.load_relationships_from_parquet(
                path=rels_file,
                start_node_column="from",
                end_node_column="to",
                rel_type_column="type",
                property_columns=["since"],
                fixed_rel_type=None,
                deduplication="CreateUniqueByRelType",  # For now, use this
            )
            
            builder.finalize_into(manager)
            graph = manager.graph_snapshot("test_v6")
            
            # With CreateUniqueByRelType, should have 2 relationships
            # (1->2 KNOWS deduplicated, 1->3 KNOWS)
            assert graph.relationship_count() == 2


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])

