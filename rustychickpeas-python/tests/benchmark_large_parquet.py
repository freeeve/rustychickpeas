#!/usr/bin/env python3
"""Benchmark bulk loading from large Parquet files (25M nodes, 25M rels)"""

import time
import tempfile
import os
import rustychickpeas as rcp
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import psutil
import gc

def get_memory_mb():
    """Get current process memory usage in MB"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return mem_info.rss / (1024 * 1024)  # Convert to MB
    except:
        return 0

def log_memory(stage):
    """Log memory usage at a specific stage"""
    mem_mb = get_memory_mb()
    print(f"[MEMORY] {stage}: {mem_mb:,.0f} MB ({mem_mb/1024:.2f} GB)")
    sys.stdout.flush()

def create_large_parquet_files(num_nodes=25_000_000, num_relationships=25_000_000):
    """Create large Parquet files for benchmarking"""
    print(f"\n{'='*70}")
    print(f"Creating Large Parquet Files")
    print(f"{'='*70}")
    print(f"Nodes: {num_nodes:,}")
    print(f"Relationships: {num_relationships:,}")
    print(f"{'='*70}\n")
    
    log_memory("Start - Before file creation")
    temp_dir = tempfile.mkdtemp()
    nodes_file = os.path.join(temp_dir, "nodes_large.parquet")
    rels_file = os.path.join(temp_dir, "relationships_large.parquet")
    
    # Create nodes file in chunks to avoid memory issues
    print("Creating nodes file...")
    log_memory("Before nodes file creation")
    start = time.time()
    
    batch_size = 100_000  # Process in batches
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("label", pa.string()),
        pa.field("name", pa.string()),
        pa.field("active", pa.bool_()),
    ])
    
    writer = pq.ParquetWriter(nodes_file, schema)
    
    for batch_start in range(0, num_nodes, batch_size):
        batch_end = min(batch_start + batch_size, num_nodes)
        batch_size_actual = batch_end - batch_start
        
        ids = list(range(batch_start + 1, batch_end + 1))
        labels = ["Person" if i % 2 == 0 else "Company" for i in range(batch_size_actual)]
        names = [f"Entity{i}" for i in range(batch_start + 1, batch_end + 1)]
        active = [i % 2 == 0 for i in range(batch_size_actual)]
        
        arrays = [
            pa.array(ids, type=pa.int64()),
            pa.array(labels, type=pa.string()),
            pa.array(names, type=pa.string()),
            pa.array(active, type=pa.bool_()),
        ]
        
        batch = pa.record_batch(arrays, schema=schema)
        writer.write_batch(batch)
        
        if (batch_start // batch_size) % 100 == 0:
            elapsed = time.time() - start
            rate = batch_end / elapsed if elapsed > 0 else 0
            print(f"  Progress: {batch_end:,}/{num_nodes:,} nodes ({batch_end*100/num_nodes:.1f}%) - {rate:,.0f} nodes/sec", end='\r')
    
    writer.close()
    nodes_time = time.time() - start
    log_memory("After nodes file creation")
    print(f"\n  Nodes file created in {nodes_time:.2f}s ({num_nodes/nodes_time:,.0f} nodes/sec)")
    
    # Force garbage collection
    gc.collect()
    log_memory("After nodes file GC")
    
    # Create relationships file
    print("Creating relationships file...")
    log_memory("Before relationships file creation")
    start = time.time()
    
    rel_schema = pa.schema([
        pa.field("from", pa.int64()),
        pa.field("to", pa.int64()),
        pa.field("type", pa.string()),
    ])
    
    writer = pq.ParquetWriter(rels_file, rel_schema)
    
    for batch_start in range(0, num_relationships, batch_size):
        batch_end = min(batch_start + batch_size, num_relationships)
        batch_size_actual = batch_end - batch_start
        
        from_ids = list(range(1, batch_size_actual + 1))
        to_ids = [(i % num_nodes) + 1 for i in range(batch_start, batch_end)]
        types = ["KNOWS" if i % 2 == 0 else "WORKS_FOR" for i in range(batch_size_actual)]
        
        arrays = [
            pa.array(from_ids, type=pa.int64()),
            pa.array(to_ids, type=pa.int64()),
            pa.array(types, type=pa.string()),
        ]
        
        batch = pa.record_batch(arrays, schema=rel_schema)
        writer.write_batch(batch)
        
        if (batch_start // batch_size) % 100 == 0:
            elapsed = time.time() - start
            rate = batch_end / elapsed if elapsed > 0 else 0
            print(f"  Progress: {batch_end:,}/{num_relationships:,} rels ({batch_end*100/num_relationships:.1f}%) - {rate:,.0f} rels/sec", end='\r')
    
    writer.close()
    rels_time = time.time() - start
    log_memory("After relationships file creation")
    print(f"\n  Relationships file created in {rels_time:.2f}s ({num_relationships/rels_time:,.0f} rels/sec)")
    
    # Force garbage collection
    gc.collect()
    log_memory("After relationships file GC")
    
    file_size_nodes = os.path.getsize(nodes_file) / (1024 * 1024)  # MB
    file_size_rels = os.path.getsize(rels_file) / (1024 * 1024)  # MB
    
    print(f"\nFile sizes:")
    print(f"  Nodes: {file_size_nodes:,.1f} MB")
    print(f"  Relationships: {file_size_rels:,.1f} MB")
    print(f"  Total: {file_size_nodes + file_size_rels:,.1f} MB")
    
    log_memory("After file creation complete")
    
    return nodes_file, rels_file, temp_dir


def benchmark_large_load(nodes_file, rels_file):
    """Benchmark loading large Parquet files"""
    print(f"\n{'='*70}")
    print(f"Benchmarking Large Parquet Load (Streaming)")
    print(f"{'='*70}\n")
    
    log_memory("Before manager creation")
    manager = rcp.RustyChickpeas()
    log_memory("After manager creation")
    
    # Estimate sizes from file metadata (without loading full table)
    import pyarrow.parquet as pq
    nodes_meta = pq.read_metadata(nodes_file)
    num_nodes_est = nodes_meta.num_rows
    rels_meta = pq.read_metadata(rels_file)
    num_rels_est = rels_meta.num_rows
    log_memory("After reading table metadata (metadata only)")
    
    # Load nodes
    print("Loading nodes...")
    log_memory("Before builder creation")
    start = time.time()
    builder = manager.create_builder(version="large_test_v1", capacity_nodes=num_nodes_est, capacity_rels=num_rels_est)
    log_memory("After builder creation")
    
    log_memory("Before node loading")
    node_ids = builder.load_nodes_from_parquet(
        path=nodes_file,
        node_id_column="id",
        label_columns=["label"],
        property_columns=["name", "active"],
    )
    nodes_load_time = time.time() - start
    log_memory("After node loading")
    
    print(f"  Loaded {len(node_ids):,} nodes in {nodes_load_time:.2f}s")
    print(f"  Rate: {len(node_ids) / nodes_load_time:,.0f} nodes/sec ({len(node_ids) / nodes_load_time / 1e6:.2f}M nodes/sec)")
    
    # Force GC
    gc.collect()
    log_memory("After node loading GC")
    
    # Load relationships
    print("\nLoading relationships...")
    log_memory("Before relationship loading")
    start = time.time()
    rel_ids = builder.load_relationships_from_parquet(
        path=rels_file,
        start_node_column="from",
        end_node_column="to",
        rel_type_column="type",
        property_columns=None,
    )
    rels_load_time = time.time() - start
    log_memory("After relationship loading")
    
    print(f"  Loaded {len(rel_ids):,} relationships in {rels_load_time:.2f}s")
    print(f"  Rate: {len(rel_ids) / rels_load_time:,.0f} rels/sec ({len(rel_ids) / rels_load_time / 1e6:.2f}M rels/sec)")
    
    # Force GC
    gc.collect()
    log_memory("After relationship loading GC")
    
    # Finalize
    print("\nFinalizing snapshot...")
    log_memory("Before finalization")
    start = time.time()
    builder.finalize_into(manager)
    finalize_time = time.time() - start
    log_memory("After finalization")
    
    # Force GC after finalization
    gc.collect()
    log_memory("After finalization GC")
    
    snapshot_opt = manager.get_graph_snapshot("large_test_v1")
    if snapshot_opt is None:
        raise RuntimeError("Failed to retrieve snapshot")
    snapshot = snapshot_opt  # Already unwrapped, no .unwrap() needed
    
    total_time = nodes_load_time + rels_load_time + finalize_time
    total_entities = len(node_ids) + len(rel_ids)
    
    print(f"\n{'='*70}")
    print(f"Summary")
    print(f"{'='*70}")
    print(f"Total nodes: {snapshot.n_nodes():,}")
    print(f"Total relationships: {snapshot.n_rels():,}")
    print(f"Total entities: {total_entities:,}")
    print(f"\nTiming:")
    print(f"  Node loading: {nodes_load_time:.2f}s")
    print(f"  Relationship loading: {rels_load_time:.2f}s")
    print(f"  Finalization: {finalize_time:.2f}s")
    print(f"  Total: {total_time:.2f}s")
    print(f"\nThroughput:")
    print(f"  Nodes: {len(node_ids) / nodes_load_time:,.0f} nodes/sec ({len(node_ids) / nodes_load_time / 1e6:.2f}M/sec)")
    print(f"  Relationships: {len(rel_ids) / rels_load_time:,.0f} rels/sec ({len(rel_ids) / rels_load_time / 1e6:.2f}M/sec)")
    print(f"  Combined: {total_entities / total_time:,.0f} entities/sec ({total_entities / total_time / 1e6:.2f}M/sec)")
    print(f"{'='*70}\n")
    
    return snapshot, total_time


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Benchmark large Parquet file loading")
    parser.add_argument("--nodes", type=int, default=25_000_000, help="Number of nodes")
    parser.add_argument("--relationships", type=int, default=25_000_000, help="Number of relationships")
    parser.add_argument("--skip-create", action="store_true", help="Skip file creation (use existing files)")
    parser.add_argument("--nodes-file", type=str, help="Path to existing nodes file")
    parser.add_argument("--rels-file", type=str, help="Path to existing relationships file")
    
    args = parser.parse_args()
    
    if args.skip_create and args.nodes_file and args.rels_file:
        nodes_file = args.nodes_file
        rels_file = args.rels_file
        temp_dir = None
    else:
        nodes_file, rels_file, temp_dir = create_large_parquet_files(args.nodes, args.relationships)
    
    try:
        snapshot, total_time = benchmark_large_load(nodes_file, rels_file)
        
        # Cleanup
        if temp_dir:
            import shutil
            print(f"Cleaning up temporary files in {temp_dir}...")
            shutil.rmtree(temp_dir)
            print("Done!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if temp_dir:
            import shutil
            print(f"\nTemporary files preserved in: {temp_dir}")
            print("You can use --nodes-file and --rels-file to reuse them")


if __name__ == "__main__":
    main()

