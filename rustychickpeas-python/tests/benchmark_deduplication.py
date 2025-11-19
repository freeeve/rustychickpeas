#!/usr/bin/env python3
"""Benchmark loading nodes/relationships with properties and deduplication logic"""

import time
import tempfile
import os
import rustychickpeas as rcp
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import psutil
import gc
import random

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

def create_parquet_with_deduplication(
    num_nodes=1_000_000,
    num_relationships=1_000_000,
    duplicate_rate=0.1,  # 10% of nodes will be duplicates
    num_properties=3,  # Number of properties per node
):
    """Create Parquet files with properties and intentional duplicates for deduplication testing"""
    print(f"\n{'='*70}")
    print(f"Creating Parquet Files with Deduplication Test Data")
    print(f"{'='*70}")
    print(f"Nodes: {num_nodes:,}")
    print(f"Relationships: {num_relationships:,}")
    print(f"Duplicate rate: {duplicate_rate*100:.1f}%")
    print(f"Properties per node: {num_properties}")
    print(f"{'='*70}\n")
    
    log_memory("Start - Before file creation")
    temp_dir = tempfile.mkdtemp()
    nodes_file = os.path.join(temp_dir, "nodes_dedup.parquet")
    rels_file = os.path.join(temp_dir, "relationships_dedup.parquet")
    
    # Generate unique keys for deduplication
    # We'll use email as the unique property
    num_unique_nodes = int(num_nodes * (1 - duplicate_rate))
    unique_emails = [f"user{i}@example.com" for i in range(num_unique_nodes)]
    
    # Create nodes file
    print("Creating nodes file with duplicates...")
    log_memory("Before nodes file creation")
    start = time.time()
    
    batch_size = 100_000
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("label", pa.string()),
        pa.field("email", pa.string()),  # Unique property for deduplication
        pa.field("name", pa.string()),
        pa.field("age", pa.int64()),
        pa.field("score", pa.float64()),
        pa.field("active", pa.bool_()),
    ])
    
    writer = pq.ParquetWriter(nodes_file, schema)
    
    node_id = 1
    for batch_start in range(0, num_nodes, batch_size):
        batch_end = min(batch_start + batch_size, num_nodes)
        batch_size_actual = batch_end - batch_start
        
        ids = []
        labels = []
        emails = []
        names = []
        ages = []
        scores = []
        active_flags = []
        
        for i in range(batch_size_actual):
            ids.append(node_id)
            labels.append("Person" if i % 2 == 0 else "Company")
            
            # Create duplicates: reuse emails from earlier nodes
            if random.random() < duplicate_rate and len(unique_emails) > 0:
                # This is a duplicate - reuse an existing email
                email = random.choice(unique_emails)
            else:
                # New unique email
                email = f"user{node_id}@example.com"
                unique_emails.append(email)
            
            emails.append(email)
            names.append(f"Entity{node_id}")
            ages.append(20 + (node_id % 50))
            scores.append(50.0 + (node_id % 50))
            active_flags.append(node_id % 2 == 0)
            
            node_id += 1
        
        arrays = [
            pa.array(ids, type=pa.int64()),
            pa.array(labels, type=pa.string()),
            pa.array(emails, type=pa.string()),
            pa.array(names, type=pa.string()),
            pa.array(ages, type=pa.int64()),
            pa.array(scores, type=pa.float64()),
            pa.array(active_flags, type=pa.bool_()),
        ]
        
        batch = pa.record_batch(arrays, schema=schema)
        writer.write_batch(batch)
        
        if (batch_start // batch_size) % 10 == 0:
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
        pa.field("weight", pa.float64()),
        pa.field("since", pa.int64()),
    ])
    
    writer = pq.ParquetWriter(rels_file, rel_schema)
    
    for batch_start in range(0, num_relationships, batch_size):
        batch_end = min(batch_start + batch_size, num_relationships)
        batch_size_actual = batch_end - batch_start
        
        from_ids = []
        to_ids = []
        types = []
        weights = []
        since_years = []
        
        for i in range(batch_size_actual):
            from_ids.append((i % num_unique_nodes) + 1)
            to_ids.append(((i + 1) % num_unique_nodes) + 1)
            types.append("KNOWS" if i % 2 == 0 else "WORKS_FOR")
            weights.append(0.5 + (i % 100) / 100.0)
            since_years.append(2020 + (i % 5))
        
        arrays = [
            pa.array(from_ids, type=pa.int64()),
            pa.array(to_ids, type=pa.int64()),
            pa.array(types, type=pa.string()),
            pa.array(weights, type=pa.float64()),
            pa.array(since_years, type=pa.int64()),
        ]
        
        batch = pa.record_batch(arrays, schema=rel_schema)
        writer.write_batch(batch)
        
        if (batch_start // batch_size) % 10 == 0:
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
    
    return nodes_file, rels_file, temp_dir, num_unique_nodes


def benchmark_with_deduplication(nodes_file, rels_file, expected_unique_nodes):
    """Benchmark loading with node deduplication and relationship deduplication"""
    print(f"\n{'='*70}")
    print(f"Benchmarking Load with Deduplication")
    print(f"{'='*70}\n")
    
    log_memory("Before manager creation")
    manager = rcp.RustyChickpeas()
    log_memory("After manager creation")
    
    # Estimate sizes from file metadata
    nodes_meta = pq.read_metadata(nodes_file)
    num_nodes_est = nodes_meta.num_rows
    rels_meta = pq.read_metadata(rels_file)
    num_rels_est = rels_meta.num_rows
    log_memory("After reading table metadata")
    
    # Load nodes WITH deduplication
    print("Loading nodes WITH deduplication (unique_properties=['email'])...")
    log_memory("Before builder creation")
    start = time.time()
    builder = manager.create_builder(version="dedup_test_v1", capacity_nodes=expected_unique_nodes, capacity_rels=num_rels_est)
    log_memory("After builder creation")
    
    log_memory("Before node loading with dedup")
    node_ids = builder.load_nodes_from_parquet(
        path=nodes_file,
        node_id_column="id",
        label_columns=["label"],
        property_columns=["email", "name", "age", "score", "active"],
        unique_properties=["email"],  # Deduplicate by email
    )
    nodes_load_time = time.time() - start
    log_memory("After node loading with dedup")
    
    print(f"  Loaded {len(node_ids):,} node IDs from file")
    print(f"  Expected unique nodes: ~{expected_unique_nodes:,}")
    print(f"  Load time: {nodes_load_time:.2f}s")
    print(f"  Rate: {len(node_ids) / nodes_load_time:,.0f} nodes/sec ({len(node_ids) / nodes_load_time / 1e6:.2f}M nodes/sec)")
    
    # Force GC
    gc.collect()
    log_memory("After node loading GC")
    
    # Load relationships WITH deduplication
    print("\nLoading relationships WITH deduplication (CreateUniqueByRelType)...")
    log_memory("Before relationship loading with dedup")
    start = time.time()
    rel_ids = builder.load_relationships_from_parquet(
        path=rels_file,
        start_node_column="from",
        end_node_column="to",
        rel_type_column="type",
        property_columns=["weight", "since"],
        fixed_rel_type=None,
        deduplication="CreateUniqueByRelType",  # One relationship per type between two nodes
    )
    rels_load_time = time.time() - start
    log_memory("After relationship loading with dedup")
    
    print(f"  Loaded {len(rel_ids):,} relationship pairs from file")
    print(f"  Load time: {rels_load_time:.2f}s")
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
    
    snapshot_opt = manager.graph_snapshot("dedup_test_v1")
    if snapshot_opt is None:
        raise RuntimeError("Failed to retrieve snapshot")
    snapshot = snapshot_opt
    
    total_time = nodes_load_time + rels_load_time + finalize_time
    total_entities = len(node_ids) + len(rel_ids)
    
    print(f"\n{'='*70}")
    print(f"Summary (WITH Deduplication)")
    print(f"{'='*70}")
    print(f"Nodes in file: {len(node_ids):,}")
    print(f"Unique nodes in graph: {snapshot.node_count():,}")
    print(f"Expected unique nodes: ~{expected_unique_nodes:,}")
    print(f"Relationships in file: {len(rel_ids):,}")
    print(f"Unique relationships in graph: {snapshot.relationship_count():,}")
    print(f"\nTiming:")
    print(f"  Node loading (with dedup): {nodes_load_time:.2f}s")
    print(f"  Relationship loading (with dedup): {rels_load_time:.2f}s")
    print(f"  Finalization: {finalize_time:.2f}s")
    print(f"  Total: {total_time:.2f}s")
    print(f"\nThroughput:")
    print(f"  Nodes: {len(node_ids) / nodes_load_time:,.0f} nodes/sec ({len(node_ids) / nodes_load_time / 1e6:.2f}M/sec)")
    print(f"  Relationships: {len(rel_ids) / rels_load_time:,.0f} rels/sec ({len(rel_ids) / rels_load_time / 1e6:.2f}M/sec)")
    print(f"  Combined: {total_entities / total_time:,.0f} entities/sec ({total_entities / total_time / 1e6:.2f}M/sec)")
    print(f"{'='*70}\n")
    
    return snapshot, total_time


def benchmark_without_deduplication(nodes_file, rels_file):
    """Benchmark loading WITHOUT deduplication for comparison"""
    print(f"\n{'='*70}")
    print(f"Benchmarking Load WITHOUT Deduplication (Baseline)")
    print(f"{'='*70}\n")
    
    log_memory("Before manager creation (baseline)")
    manager = rcp.RustyChickpeas()
    log_memory("After manager creation (baseline)")
    
    # Estimate sizes from file metadata
    nodes_meta = pq.read_metadata(nodes_file)
    num_nodes_est = nodes_meta.num_rows
    rels_meta = pq.read_metadata(rels_file)
    num_rels_est = rels_meta.num_rows
    log_memory("After reading table metadata (baseline)")
    
    # Load nodes WITHOUT deduplication
    print("Loading nodes WITHOUT deduplication...")
    log_memory("Before builder creation (baseline)")
    start = time.time()
    builder = manager.create_builder(version="baseline_test_v1", capacity_nodes=num_nodes_est, capacity_rels=num_rels_est)
    log_memory("After builder creation (baseline)")
    
    log_memory("Before node loading (baseline)")
    node_ids = builder.load_nodes_from_parquet(
        path=nodes_file,
        node_id_column="id",
        label_columns=["label"],
        property_columns=["email", "name", "age", "score", "active"],
        unique_properties=None,  # No deduplication
    )
    nodes_load_time = time.time() - start
    log_memory("After node loading (baseline)")
    
    print(f"  Loaded {len(node_ids):,} node IDs")
    print(f"  Load time: {nodes_load_time:.2f}s")
    print(f"  Rate: {len(node_ids) / nodes_load_time:,.0f} nodes/sec ({len(node_ids) / nodes_load_time / 1e6:.2f}M nodes/sec)")
    
    # Force GC
    gc.collect()
    log_memory("After node loading GC (baseline)")
    
    # Load relationships WITHOUT deduplication
    print("\nLoading relationships WITHOUT deduplication (CreateAll)...")
    log_memory("Before relationship loading (baseline)")
    start = time.time()
    rel_ids = builder.load_relationships_from_parquet(
        path=rels_file,
        start_node_column="from",
        end_node_column="to",
        rel_type_column="type",
        property_columns=["weight", "since"],
        fixed_rel_type=None,
        deduplication="CreateAll",  # No deduplication
    )
    rels_load_time = time.time() - start
    log_memory("After relationship loading (baseline)")
    
    print(f"  Loaded {len(rel_ids):,} relationship pairs")
    print(f"  Load time: {rels_load_time:.2f}s")
    print(f"  Rate: {len(rel_ids) / rels_load_time:,.0f} rels/sec ({len(rel_ids) / rels_load_time / 1e6:.2f}M rels/sec)")
    
    # Force GC
    gc.collect()
    log_memory("After relationship loading GC (baseline)")
    
    # Finalize
    print("\nFinalizing snapshot...")
    log_memory("Before finalization (baseline)")
    start = time.time()
    builder.finalize_into(manager)
    finalize_time = time.time() - start
    log_memory("After finalization (baseline)")
    
    # Force GC after finalization
    gc.collect()
    log_memory("After finalization GC (baseline)")
    
    snapshot_opt = manager.graph_snapshot("baseline_test_v1")
    if snapshot_opt is None:
        raise RuntimeError("Failed to retrieve snapshot")
    snapshot = snapshot_opt
    
    total_time = nodes_load_time + rels_load_time + finalize_time
    total_entities = len(node_ids) + len(rel_ids)
    
    print(f"\n{'='*70}")
    print(f"Summary (WITHOUT Deduplication - Baseline)")
    print(f"{'='*70}")
    print(f"Nodes in file: {len(node_ids):,}")
    print(f"Nodes in graph: {snapshot.node_count():,}")
    print(f"Relationships in file: {len(rel_ids):,}")
    print(f"Relationships in graph: {snapshot.relationship_count():,}")
    print(f"\nTiming:")
    print(f"  Node loading (no dedup): {nodes_load_time:.2f}s")
    print(f"  Relationship loading (no dedup): {rels_load_time:.2f}s")
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
    parser = argparse.ArgumentParser(description="Benchmark loading with properties and deduplication")
    parser.add_argument("--nodes", type=int, default=1_000_000, help="Number of nodes")
    parser.add_argument("--relationships", type=int, default=1_000_000, help="Number of relationships")
    parser.add_argument("--duplicate-rate", type=float, default=0.1, help="Rate of duplicate nodes (0.0-1.0)")
    parser.add_argument("--skip-create", action="store_true", help="Skip file creation (use existing files)")
    parser.add_argument("--nodes-file", type=str, help="Path to existing nodes file")
    parser.add_argument("--rels-file", type=str, help="Path to existing relationships file")
    parser.add_argument("--baseline-only", action="store_true", help="Only run baseline (no dedup) benchmark")
    parser.add_argument("--dedup-only", action="store_true", help="Only run deduplication benchmark")
    
    args = parser.parse_args()
    
    if args.skip_create and args.nodes_file and args.rels_file:
        nodes_file = args.nodes_file
        rels_file = args.rels_file
        temp_dir = None
        # Estimate unique nodes (rough estimate)
        nodes_meta = pq.read_metadata(nodes_file)
        expected_unique_nodes = int(nodes_meta.num_rows * (1 - args.duplicate_rate))
    else:
        nodes_file, rels_file, temp_dir, expected_unique_nodes = create_parquet_with_deduplication(
            args.nodes, args.relationships, args.duplicate_rate
        )
    
    try:
        if not args.dedup_only:
            print("\n" + "="*70)
            print("RUNNING BASELINE (NO DEDUPLICATION)")
            print("="*70)
            baseline_snapshot, baseline_time = benchmark_without_deduplication(nodes_file, rels_file)
        
        if not args.baseline_only:
            print("\n" + "="*70)
            print("RUNNING WITH DEDUPLICATION")
            print("="*70)
            dedup_snapshot, dedup_time = benchmark_with_deduplication(nodes_file, rels_file, expected_unique_nodes)
        
        # Comparison
        if not args.baseline_only and not args.dedup_only:
            print("\n" + "="*70)
            print("COMPARISON")
            print("="*70)
            print(f"Baseline (no dedup):")
            print(f"  Nodes: {baseline_snapshot.node_count():,}")
            print(f"  Relationships: {baseline_snapshot.relationship_count():,}")
            print(f"  Total time: {baseline_time:.2f}s")
            print(f"\nWith deduplication:")
            print(f"  Nodes: {dedup_snapshot.node_count():,}")
            print(f"  Relationships: {dedup_snapshot.relationship_count():,}")
            print(f"  Total time: {dedup_time:.2f}s")
            print(f"\nDeduplication impact:")
            print(f"  Node reduction: {baseline_snapshot.node_count() - dedup_snapshot.node_count():,} nodes ({((baseline_snapshot.node_count() - dedup_snapshot.node_count()) / baseline_snapshot.node_count() * 100):.1f}%)")
            print(f"  Relationship reduction: {baseline_snapshot.relationship_count() - dedup_snapshot.relationship_count():,} rels ({((baseline_snapshot.relationship_count() - dedup_snapshot.relationship_count()) / baseline_snapshot.relationship_count() * 100):.1f}%)")
            print(f"  Time overhead: {dedup_time - baseline_time:.2f}s ({((dedup_time - baseline_time) / baseline_time * 100):.1f}%)")
            print("="*70)
        
        # Cleanup
        if temp_dir:
            import shutil
            print(f"\nCleaning up temporary files in {temp_dir}...")
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

