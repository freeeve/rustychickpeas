#!/usr/bin/env python3
"""Test bulk loading from Parquet files using the new RustyChickpeas API"""

import time
import tempfile
import os
import rustychickpeas as rcp
import pyarrow as pa
import pyarrow.parquet as pq


def create_test_parquet_files():
    """Create test Parquet files for nodes and relationships"""
    # Create nodes Parquet file
    nodes_data = {
        "id": list(range(1, 10001)),  # 10K nodes
        "type": ["Person"] * 5000 + ["Company"] * 5000,
        "name": [f"Node_{i}" for i in range(1, 10001)],
        "age": [20 + (i % 50) for i in range(1, 10001)],
        "city": ["New York"] * 3333 + ["San Francisco"] * 3333 + ["Chicago"] * 3334,
        "active": [True] * 7000 + [False] * 3000,
    }
    nodes_table = pa.table(nodes_data)
    
    # Create relationships Parquet file
    relationships_data = {
        "from": list(range(1, 5001)),  # 5K relationships
        "to": list(range(5001, 10001)),
        "type": ["KNOWS"] * 2500 + ["WORKS_FOR"] * 2500,
        "weight": [0.5 + (i % 10) * 0.1 for i in range(1, 5001)],
        "since": [2020 + (i % 5) for i in range(1, 5001)],
    }
    rels_table = pa.table(relationships_data)
    
    # Write to temporary files
    nodes_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    rels_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    
    pq.write_table(nodes_table, nodes_file.name)
    pq.write_table(rels_table, rels_file.name)
    
    return nodes_file.name, rels_file.name


def test_bulk_load_nodes():
    """Test loading nodes from Parquet using GraphSnapshotBuilder"""
    nodes_file, _ = create_test_parquet_files()
    
    try:
        print("=" * 70)
        print("Testing Bulk Load: Nodes from Parquet (New API)")
        print("=" * 70)
        
        # Create manager and builder
        manager = rcp.RustyChickpeas()
        builder = manager.create_builder(version="test_v1", capacity_nodes=10000, capacity_rels=5000)
        
        start = time.time()
        node_ids = builder.load_nodes_from_parquet(
            path=nodes_file,
            node_id_column="id",
            label_columns=["type"],
            property_columns=["name", "age", "city", "active"],
        )
        load_time = time.time() - start
        
        print(f"Loaded {len(node_ids):,} nodes in {load_time:.3f}s")
        print(f"Rate: {len(node_ids) / load_time:,.0f} nodes/sec")
        
        # Finalize and add to manager
        builder.set_version("test_v1")
        builder.finalize_into(manager)
        
        # Retrieve snapshot
        snapshot = manager.get_graph_snapshot("test_v1")
        assert snapshot is not None, "Snapshot should be stored in manager"
        
        print(f"\nSnapshot stored with version: test_v1")
        print(f"Total nodes in snapshot: {snapshot.n_nodes():,}")
        
        # Verify some nodes
        if node_ids:
            test_node = node_ids[0]
            props = snapshot.get_node_properties(test_node)
            print(f"\nSample node {test_node} properties:")
            for key, value in props.items():
                print(f"  {key}: {value}")
        
        # Clean up
        os.unlink(nodes_file)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if os.path.exists(nodes_file):
            os.unlink(nodes_file)


def test_bulk_load_relationships():
    """Test loading relationships from Parquet using GraphSnapshotBuilder"""
    nodes_file, rels_file = create_test_parquet_files()
    
    try:
        print("\n" + "=" * 70)
        print("Testing Bulk Load: Relationships from Parquet (New API)")
        print("=" * 70)
        
        # Create manager and builder
        manager = rcp.RustyChickpeas()
        builder = manager.create_builder(version="test_v1", capacity_nodes=10000, capacity_rels=5000)
        
        # First load nodes
        print("Loading nodes...")
        node_ids = builder.load_nodes_from_parquet(
            path=nodes_file,
            node_id_column="id",
            label_columns=["type"],
            property_columns=["name", "age", "city", "active"],
        )
        print(f"Loaded {len(node_ids):,} nodes")
        
        # Then load relationships
        print("Loading relationships...")
        start = time.time()
        rel_ids = builder.load_relationships_from_parquet(
            path=rels_file,
            start_node_column="from",
            end_node_column="to",
            rel_type_column="type",
            property_columns=["weight", "since"],
        )
        load_time = time.time() - start
        
        print(f"Loaded {len(rel_ids):,} relationships in {load_time:.3f}s")
        print(f"Rate: {len(rel_ids) / load_time:,.0f} rels/sec")
        
        # Finalize and add to manager
        builder.set_version("test_v2")
        builder.finalize_into(manager)
        
        # Retrieve snapshot
        snapshot = manager.get_graph_snapshot("test_v2")
        assert snapshot is not None, "Snapshot should be stored in manager"
        
        print(f"\nSnapshot stored with version: test_v2")
        print(f"Total nodes: {snapshot.n_nodes():,}")
        print(f"Total relationships: {snapshot.n_rels():,}")
        
        # Clean up
        os.unlink(nodes_file)
        os.unlink(rels_file)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if os.path.exists(nodes_file):
            os.unlink(nodes_file)
        if os.path.exists(rels_file):
            os.unlink(rels_file)


def test_read_from_parquet():
    """Test the convenience read_from_parquet method"""
    nodes_file, rels_file = create_test_parquet_files()
    
    try:
        print("\n" + "=" * 70)
        print("Testing read_from_parquet convenience method")
        print("=" * 70)
        
        start = time.time()
        snapshot = rcp.GraphSnapshot.read_from_parquet(
            nodes_path=nodes_file,
            relationships_path=rels_file,
            node_id_column="id",
            label_columns=["type"],
            node_property_columns=["name", "age", "city", "active"],
            start_node_column="from",
            end_node_column="to",
            rel_type_column="type",
            rel_property_columns=["weight", "since"],
        )
        load_time = time.time() - start
        
        print(f"Loaded snapshot in {load_time:.3f}s")
        print(f"Nodes: {snapshot.n_nodes():,}")
        print(f"Relationships: {snapshot.n_rels():,}")
        print(f"Overall rate: {(snapshot.n_nodes() + snapshot.n_rels()) / load_time:,.0f} entities/sec")
        
        # Clean up
        os.unlink(nodes_file)
        os.unlink(rels_file)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if os.path.exists(nodes_file):
            os.unlink(nodes_file)
        if os.path.exists(rels_file):
            os.unlink(rels_file)


def main():
    test_bulk_load_nodes()
    test_bulk_load_relationships()
    test_read_from_parquet()


if __name__ == "__main__":
    main()
