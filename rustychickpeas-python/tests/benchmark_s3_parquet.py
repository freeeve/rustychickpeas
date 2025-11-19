#!/usr/bin/env python3
"""Benchmark loading 1M nodes and 1M relationships from S3 Parquet files using Python API

Usage:
    # With LocalStack (local testing):
    docker run --rm -d --name localstack -p 4566:4566 -e SERVICES=s3 localstack/localstack
    export AWS_ENDPOINT_URL=http://localhost:4566
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    export AWS_REGION=us-east-1
    pip install boto3  # Optional, only needed for uploading
    python benchmark_s3_parquet.py

    # With real AWS S3:
    export AWS_ACCESS_KEY_ID=your_key
    export AWS_SECRET_ACCESS_KEY=your_secret
    export AWS_REGION=us-east-1
    python benchmark_s3_parquet.py --bucket your-bucket-name

    # Skip upload (files already in S3):
    python benchmark_s3_parquet.py --skip-upload --nodes-s3 s3://bucket/nodes.parquet --rels-s3 s3://bucket/rels.parquet
"""

import time
import tempfile
import os
import rustychickpeas as rcp
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import psutil
import gc

# boto3 is optional - only needed for uploading files to S3
try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False
    print("Warning: boto3 not installed. Install with: pip install boto3")
    print("  This is only needed for uploading files to S3.")
    print("  If files are already in S3, you can use --skip-upload and provide --nodes-s3 and --rels-s3")

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

def check_localstack():
    """Check if LocalStack is available"""
    try:
        import urllib.request
        import json
        req = urllib.request.Request("http://localhost:4566/_localstack/health")
        with urllib.request.urlopen(req, timeout=2) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode())
                s3_status = data.get("services", {}).get("s3", "")
                return s3_status in ["running", "available"]
    except:
        pass
    return False

def get_s3_client():
    """Get S3 client (LocalStack or real AWS)"""
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 operations. Install with: pip install boto3")
    
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    region = os.environ.get("AWS_REGION", "us-east-1")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    return s3_client

def ensure_bucket_exists(bucket_name):
    """Ensure S3 bucket exists, create if it doesn't"""
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"  Bucket '{bucket_name}' already exists")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"  Created bucket '{bucket_name}'")
            except ClientError as create_error:
                # For LocalStack, bucket might auto-create on first PUT
                print(f"  Note: Bucket creation may have failed, but LocalStack may auto-create on PUT")
        else:
            raise

def upload_to_s3(local_file, bucket_name, s3_key):
    """Upload a file to S3"""
    s3_client = get_s3_client()
    file_size_mb = os.path.getsize(local_file) / (1024 * 1024)
    print(f"  Uploading {os.path.basename(local_file)} ({file_size_mb:.1f} MB) to s3://{bucket_name}/{s3_key}...")
    start = time.time()
    
    s3_client.upload_file(local_file, bucket_name, s3_key)
    
    upload_time = time.time() - start
    print(f"  Uploaded in {upload_time:.2f}s ({file_size_mb/upload_time:.1f} MB/s)")
    return f"s3://{bucket_name}/{s3_key}"

def create_parquet_files(num_nodes=1_000_000, num_relationships=1_000_000):
    """Create Parquet files for nodes and relationships"""
    print(f"\n{'='*70}")
    print(f"Creating Parquet Files")
    print(f"{'='*70}")
    print(f"Nodes: {num_nodes:,}")
    print(f"Relationships: {num_relationships:,}")
    print(f"{'='*70}\n")
    
    log_memory("Start - Before file creation")
    temp_dir = tempfile.mkdtemp()
    nodes_file = os.path.join(temp_dir, "nodes_1m.parquet")
    rels_file = os.path.join(temp_dir, "relationships_1m.parquet")
    
    # Create nodes file
    print("Creating nodes file...")
    log_memory("Before nodes file creation")
    start = time.time()
    
    batch_size = 100_000
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("label", pa.string()),
        pa.field("name", pa.string()),
        pa.field("age", pa.int64()),
        pa.field("score", pa.float64()),
        pa.field("active", pa.bool_()),
    ])
    
    writer = pq.ParquetWriter(nodes_file, schema)
    
    for batch_start in range(0, num_nodes, batch_size):
        batch_end = min(batch_start + batch_size, num_nodes)
        batch_size_actual = batch_end - batch_start
        
        ids = list(range(batch_start + 1, batch_end + 1))
        labels = ["Person" if i % 2 == 0 else "Company" for i in range(batch_size_actual)]
        names = [f"Entity{i}" for i in range(batch_start + 1, batch_end + 1)]
        ages = [20 + (i % 50) for i in range(batch_start + 1, batch_end + 1)]
        scores = [50.0 + (i % 50) for i in range(batch_start + 1, batch_end + 1)]
        active = [i % 2 == 0 for i in range(batch_start + 1, batch_end + 1)]
        
        arrays = [
            pa.array(ids, type=pa.int64()),
            pa.array(labels, type=pa.string()),
            pa.array(names, type=pa.string()),
            pa.array(ages, type=pa.int64()),
            pa.array(scores, type=pa.float64()),
            pa.array(active, type=pa.bool_()),
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
        
        from_ids = list(range(1, batch_size_actual + 1))
        to_ids = [(i % num_nodes) + 1 for i in range(batch_start, batch_end)]
        types = ["KNOWS" if i % 2 == 0 else "WORKS_FOR" for i in range(batch_size_actual)]
        weights = [0.5 + (i % 100) / 100.0 for i in range(batch_size_actual)]
        since_years = [2020 + (i % 5) for i in range(batch_size_actual)]
        
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
    
    return nodes_file, rels_file, temp_dir


def benchmark_s3_load(nodes_s3_path, rels_s3_path):
    """Benchmark loading from S3 using Python API"""
    print(f"\n{'='*70}")
    print(f"Benchmarking S3 Parquet Load (Python API)")
    print(f"{'='*70}\n")
    
    log_memory("Before manager creation")
    manager = rcp.RustyChickpeas()
    log_memory("After manager creation")
    
    # Get file metadata (estimate sizes)
    # Note: For S3, we can't easily read metadata without downloading, so we'll estimate
    print(f"Loading from S3:")
    print(f"  Nodes: {nodes_s3_path}")
    print(f"  Relationships: {rels_s3_path}")
    
    # Load nodes from S3
    print("\nLoading nodes from S3...")
    log_memory("Before builder creation")
    start = time.time()
    builder = manager.create_builder(version="s3_test_v1", capacity_nodes=1_000_000, capacity_rels=1_000_000)
    log_memory("After builder creation")
    
    log_memory("Before node loading from S3")
    node_ids = builder.load_nodes_from_parquet(
        path=nodes_s3_path,
        node_id_column="id",
        label_columns=["label"],
        property_columns=["name", "age", "score", "active"],
    )
    nodes_load_time = time.time() - start
    log_memory("After node loading from S3")
    
    print(f"  Loaded {len(node_ids):,} nodes in {nodes_load_time:.2f}s")
    print(f"  Rate: {len(node_ids) / nodes_load_time:,.0f} nodes/sec ({len(node_ids) / nodes_load_time / 1e6:.2f}M nodes/sec)")
    
    # Force GC
    gc.collect()
    log_memory("After node loading GC")
    
    # Load relationships from S3
    print("\nLoading relationships from S3...")
    log_memory("Before relationship loading from S3")
    start = time.time()
    rel_ids = builder.load_relationships_from_parquet(
        path=rels_s3_path,
        start_node_column="from",
        end_node_column="to",
        rel_type_column="type",
        property_columns=["weight", "since"],
    )
    rels_load_time = time.time() - start
    log_memory("After relationship loading from S3")
    
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
    
    snapshot_opt = manager.graph_snapshot("s3_test_v1")
    if snapshot_opt is None:
        raise RuntimeError("Failed to retrieve snapshot")
    snapshot = snapshot_opt
    
    total_time = nodes_load_time + rels_load_time + finalize_time
    total_entities = len(node_ids) + len(rel_ids)
    
    print(f"\n{'='*70}")
    print(f"Summary (S3 Load)")
    print(f"{'='*70}")
    print(f"Total nodes: {snapshot.node_count():,}")
    print(f"Total relationships: {snapshot.relationship_count():,}")
    print(f"Total entities loaded: {total_entities:,}")
    print(f"\nTiming:")
    print(f"  Node loading (S3): {nodes_load_time:.2f}s")
    print(f"  Relationship loading (S3): {rels_load_time:.2f}s")
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
    parser = argparse.ArgumentParser(description="Benchmark loading 1M nodes/rels from S3 Parquet files")
    parser.add_argument("--nodes", type=int, default=1_000_000, help="Number of nodes")
    parser.add_argument("--relationships", type=int, default=1_000_000, help="Number of relationships")
    parser.add_argument("--bucket", type=str, default="rustychickpeas-test", help="S3 bucket name")
    parser.add_argument("--skip-upload", action="store_true", help="Skip upload (use existing S3 files)")
    parser.add_argument("--nodes-s3", type=str, help="S3 path to existing nodes file (e.g., s3://bucket/path)")
    parser.add_argument("--rels-s3", type=str, help="S3 path to existing relationships file")
    parser.add_argument("--skip-create", action="store_true", help="Skip file creation (use existing local files)")
    parser.add_argument("--nodes-file", type=str, help="Path to existing local nodes file")
    parser.add_argument("--rels-file", type=str, help="Path to existing local relationships file")
    
    args = parser.parse_args()
    
    # Check if using LocalStack
    is_localstack = check_localstack()
    if is_localstack:
        print("✓ LocalStack detected")
        if not os.environ.get("AWS_ENDPOINT_URL"):
            os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            os.environ["AWS_ACCESS_KEY_ID"] = "test"
        if not os.environ.get("AWS_SECRET_ACCESS_KEY"):
            os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        if not os.environ.get("AWS_REGION"):
            os.environ["AWS_REGION"] = "us-east-1"
    else:
        print("⚠ LocalStack not detected - using real AWS S3")
        print("  Make sure AWS credentials are configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc.)")
    
    # Set environment variables for Rust code (needed for S3 client)
    # These must be set before the Python API calls load_nodes_from_parquet
    if not os.environ.get("AWS_ENDPOINT_URL") and is_localstack:
        os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    if not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    if not os.environ.get("AWS_REGION"):
        os.environ["AWS_REGION"] = os.environ.get("AWS_REGION", "us-east-1")
    os.environ["AWS_DISABLE_IMDSV1"] = "1"  # Disable metadata service
    
    # Create or use existing local files
    if args.skip_create and args.nodes_file and args.rels_file:
        nodes_file = args.nodes_file
        rels_file = args.rels_file
        temp_dir = None
    else:
        nodes_file, rels_file, temp_dir = create_parquet_files(args.nodes, args.relationships)
    
    # Upload to S3 or use existing S3 paths
    if args.skip_upload and args.nodes_s3 and args.rels_s3:
        nodes_s3_path = args.nodes_s3
        rels_s3_path = args.rels_s3
    else:
        # Ensure bucket exists
        print(f"\nEnsuring S3 bucket '{args.bucket}' exists...")
        ensure_bucket_exists(args.bucket)
        
        # Upload files
        print(f"\nUploading files to S3...")
        nodes_s3_path = upload_to_s3(nodes_file, args.bucket, "nodes_1m.parquet")
        rels_s3_path = upload_to_s3(rels_file, args.bucket, "relationships_1m.parquet")
    
    try:
        # Benchmark loading from S3
        snapshot, total_time = benchmark_s3_load(nodes_s3_path, rels_s3_path)
        
        # Cleanup local files
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

