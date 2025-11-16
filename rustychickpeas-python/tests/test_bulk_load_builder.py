#!/usr/bin/env python3
"""Test bulk load using GraphSnapshotBuilder with RustyChickpeas manager"""

import time
import tempfile
import os
import rustychickpeas as rcp
import pyarrow as pa
import pyarrow.parquet as pq


def create_large_parquet_files(num_nodes=1000000, num_relationships=1000000):
    """Create large Parquet files for nodes and relationships"""
    print(f"Creating Parquet files: {num_nodes:,} nodes, {num_relationships:,} relationships...")
    
    # Create nodes Parquet file
    nodes_data = {
        "id": list(range(1, num_nodes + 1)),
        "type": ["Person"] * (num_nodes // 2) + ["Company"] * (num_nodes // 2),
        "name": [f"Node_{i}" for i in range(1, num_nodes + 1)],
        "age": [20 + (i % 50) for i in range(1, num_nodes + 1)],
        "city": ["New York"] * (num_nodes // 3) + ["San Francisco"] * (num_nodes // 3) + ["Chicago"] * (num_nodes - 2 * (num_nodes // 3)),
        "active": [True] * int(num_nodes * 0.7) + [False] * (num_nodes - int(num_nodes * 0.7)),
        "email": [f"user_{i}@example.com" for i in range(1, num_nodes + 1)],
    }
    nodes_table = pa.table(nodes_data)
    
    # Create relationships Parquet file
    type_col = ["KNOWS"] * (num_relationships // 2) + ["WORKS_FOR"] * (num_relationships // 2)
    relationships_data = {
        "from": list(range(1, num_relationships + 1)),
        "to": list(range(2, num_relationships + 2)),
        "type": type_col,
        "weight": [0.5 + (i % 10) * 0.1 for i in range(1, num_relationships + 1)],
        "since": [2020 + (i % 5) for i in range(1, num_relationships + 1)],
    }
    rels_table = pa.table(relationships_data)
    
    # Write to temporary files
    nodes_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    rels_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    
    print("Writing nodes Parquet file...")
    pq.write_table(nodes_table, nodes_file.name)
    print("Writing relationships Parquet file...")
    pq.write_table(rels_table, rels_file.name)
    
    return nodes_file.name, rels_file.name


def test_builder_bulk_load(num_nodes=1000000, num_relationships=1000000):
    """Test bulk load using GraphSnapshotBuilder with RustyChickpeas manager"""
    nodes_file, rels_file = create_large_parquet_files(num_nodes, num_relationships)
    
    try:
        print("\n" + "=" * 70)
        print(f"GraphSnapshotBuilder Bulk Load Performance Test")
        print(f"{num_nodes:,} nodes, {num_relationships:,} relationships")
        print("=" * 70)
        
        # Test GraphSnapshotBuilder approach with manager
        print("\n--- GraphSnapshotBuilder with RustyChickpeas Manager ---")
        manager = rcp.RustyChickpeas()
        builder = manager.create_builder(version="v1.0", capacity_nodes=num_nodes, capacity_rels=num_relationships)
        
        start = time.time()
        node_ids = builder.load_nodes_from_parquet(
            path=nodes_file,
            node_id_column="id",
            label_columns=["type"],
            property_columns=["name", "age", "city", "active", "email"],
        )
        node_load_time = time.time() - start
        
        start = time.time()
        rel_ids = builder.load_relationships_from_parquet(
            path=rels_file,
            start_node_column="from",
            end_node_column="to",
            rel_type_column="type",
            property_columns=["weight", "since"],
        )
        rel_load_time = time.time() - start
        
        builder.set_version("v1.0")
        start = time.time()
        builder.finalize_into(manager)
        finalize_time = time.time() - start
        
        builder_total_time = node_load_time + rel_load_time + finalize_time
        
        snapshot = manager.get_graph_snapshot("v1.0")
        assert snapshot is not None, "Snapshot should be in manager"
        
        print(f"Nodes loaded: {len(node_ids):,} in {node_load_time:.3f}s")
        print(f"Relationships loaded: {len(rel_ids):,} in {rel_load_time:.3f}s")
        print(f"Finalization: {finalize_time:.3f}s")
        print(f"Total time: {builder_total_time:.3f}s")
        print(f"Nodes: {snapshot.n_nodes():,}")
        print(f"Relationships: {snapshot.n_rels():,}")
        print(f"Overall rate: {(snapshot.n_nodes() + snapshot.n_rels()) / builder_total_time:,.0f} entities/sec")
        
        # Test read_from_parquet convenience method
        print("\n--- read_from_parquet convenience method ---")
        start = time.time()
        snapshot2 = rcp.GraphSnapshot.read_from_parquet(
            nodes_path=nodes_file,
            relationships_path=rels_file,
            node_id_column="id",
            label_columns=["type"],
            node_property_columns=["name", "age", "city", "active", "email"],
            start_node_column="from",
            end_node_column="to",
            rel_type_column="type",
            rel_property_columns=["weight", "since"],
        )
        convenience_time = time.time() - start
        
        print(f"Snapshot created in {convenience_time:.3f}s")
        print(f"Nodes: {snapshot2.n_nodes():,}")
        print(f"Relationships: {snapshot2.n_rels():,}")
        print(f"Overall rate: {(snapshot2.n_nodes() + snapshot2.n_rels()) / convenience_time:,.0f} entities/sec")
        
        # Comparison
        print("\n" + "=" * 70)
        print("PERFORMANCE COMPARISON")
        print("=" * 70)
        speedup = builder_total_time / convenience_time if convenience_time > 0 else 0
        savings = ((builder_total_time - convenience_time) / builder_total_time * 100) if builder_total_time > 0 else 0
        
        print(f"{'Metric':<40} {'Builder+Manager':<20} {'read_from_parquet':<20} {'Difference':<15}")
        print("-" * 70)
        print(f"{'Total time (s)':<40} {builder_total_time:>19.3f} {convenience_time:>19.3f} {speedup:>14.2f}x")
        print(f"{'Time difference':<40} {'':<20} {'':<20} {savings:>13.1f}%")
        print()
        
        # Verify manager has the snapshot
        versions = manager.versions()
        print(f"Manager versions: {versions}")
        assert "v1.0" in versions, "v1.0 should be in manager"
        assert manager.len() == 1, "Manager should have 1 snapshot"
        
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
    import argparse
    parser = argparse.ArgumentParser(description="GraphSnapshotBuilder bulk load performance test")
    parser.add_argument("--nodes", type=int, default=1000000, help="Number of nodes")
    parser.add_argument("--relationships", type=int, default=1000000, help="Number of relationships")
    args = parser.parse_args()
    
    test_builder_bulk_load(args.nodes, args.relationships)


if __name__ == "__main__":
    main()
