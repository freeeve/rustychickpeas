#!/usr/bin/env python3
"""
Collect benchmark results from Criterion and append to CSV.

This script parses Criterion's estimates.json files and appends results
to a CSV file for historical tracking. Tracks both time (ns) and allocations.

Usage:
    python bench/collect.py [--machine github] [--results-dir bench/results]
"""

import json
import os
import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any


def get_commit_sha() -> str:
    """Get current commit SHA from git or CI environment."""
    # Try CI environment first
    sha = os.environ.get("GITHUB_SHA")
    if sha:
        return sha[:12]  # Short SHA
    
    # Try git
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()[:12]
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "local"


def get_git_version_tag() -> str:
    """Get current git version tag if available, otherwise use Cargo.toml version."""
    try:
        # Try to get the tag for the current commit
        result = subprocess.run(
            ["git", "describe", "--tags", "--exact-match", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        # No exact tag match - prefer Cargo.toml version over nearest tag
        # (since we might be on a commit after bumping version but before tagging)
        try:
            cargo_toml = Path("Cargo.toml")
            if cargo_toml.exists():
                with open(cargo_toml) as f:
                    for line in f:
                        # Match both workspace.package version and regular version
                        if line.strip().startswith("version ="):
                            version = line.split("=")[1].strip().strip('"').strip("'")
                            return f"v{version}"
        except:
            pass
        
        # Fallback to nearest tag if Cargo.toml not found
        try:
            result = subprocess.run(
                ["git", "describe", "--tags", "--abbrev=0"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            return ""


def get_rust_version() -> str:
    """Get Rust compiler version."""
    try:
        result = subprocess.run(
            ["rustc", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def get_machine_id() -> str:
    """Get machine identifier from environment or default."""
    return (
        os.environ.get("BENCH_MACHINE")
        or os.environ.get("GITHUB_RUN_ID", "local")
    )


def extract_dhat_data(bench_dir: Path) -> Optional[Dict[str, Any]]:
    """Extract allocation data from dhat output files if available."""
    # dhat writes to files like dhat-heap.json in the benchmark directory
    dhat_files = list(bench_dir.glob("**/dhat-heap.json"))
    if not dhat_files:
        return None
    
    # Use the most recent dhat file
    dhat_file = max(dhat_files, key=lambda p: p.stat().st_mtime)
    
    try:
        with open(dhat_file) as f:
            dhat_data = json.load(f)
        
        # dhat format: {"dhatFileVersion": 2, "mode": "rust", "verbatim": false, "bk": {...}}
        # The "bk" field contains block data
        if "bk" in dhat_data:
            blocks = dhat_data["bk"]
            # Sum up all allocations
            total_blocks = 0
            total_bytes = 0
            for block_data in blocks.values():
                if isinstance(block_data, dict):
                    # Count allocations (each block is an allocation)
                    total_blocks += 1
                    if "sb" in block_data:  # Size bytes
                        total_bytes += block_data["sb"]
            
            return {
                "allocs": total_blocks,
                "allocs_bytes": total_bytes,
            }
    except (json.JSONDecodeError, FileNotFoundError, KeyError):
        pass
    
    return None


def extract_estimates(estimates_path: Path, bench_dir: Path) -> Optional[Dict[str, Any]]:
    """Extract time and allocation data from Criterion estimates.json."""
    if not estimates_path.exists():
        return None
    
    try:
        with open(estimates_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return None
    
    result = {}
    
    # Extract time metrics (mean, median)
    for metric in ["mean", "median"]:
        if metric in data:
            metric_data = data[metric]
            if "point_estimate" in metric_data:
                result[f"{metric}_ns"] = metric_data["point_estimate"]
                
                # Confidence interval
                if "confidence_interval" in metric_data:
                    ci = metric_data["confidence_interval"]
                    result[f"{metric}_ci_low_ns"] = ci.get("lower_bound", 0)
                    result[f"{metric}_ci_high_ns"] = ci.get("upper_bound", 0)
    
    # Try to extract allocation data from dhat output
    dhat_data = extract_dhat_data(bench_dir)
    if dhat_data:
        result.update(dhat_data)
    
    # Also check for allocation data in estimates.json (if custom measurement was used)
    if "allocation" in data:
        alloc_data = data["allocation"]
        if "point_estimate" in alloc_data:
            result["allocs"] = alloc_data["point_estimate"]
            if "confidence_interval" in alloc_data:
                ci = alloc_data["confidence_interval"]
                result["allocs_ci_low"] = ci.get("lower_bound", 0)
                result["allocs_ci_high"] = ci.get("upper_bound", 0)
    
    # Also check for memory-related metrics
    if "memory" in data:
        mem_data = data["memory"]
        if "point_estimate" in mem_data:
            result["memory_bytes"] = mem_data["point_estimate"]
    
    return result if result else None


def find_benchmark_directories(criterion_dir: Path):
    """Find all benchmark directories and their estimates.json files."""
    benchmarks = []
    
    if not criterion_dir.exists():
        return benchmarks
    
    for entry in criterion_dir.iterdir():
        if not entry.is_dir():
            continue
        
        bench_name = entry.name
        
        # Check for direct estimates.json (for benchmarks without size parameters)
        direct_estimates = entry / "new" / "estimates.json"
        if direct_estimates.exists():
            benchmarks.append((bench_name, None, direct_estimates, entry))
        
        # Check for size-specific benchmarks (e.g., builder_add_nodes/10000/new/estimates.json)
        for size_entry in entry.iterdir():
            if not size_entry.is_dir():
                continue
            
            # Try to parse as size
            try:
                size = int(size_entry.name)
                size_estimates = size_entry / "new" / "estimates.json"
                if size_estimates.exists():
                    benchmarks.append((bench_name, size, size_estimates, entry))
            except ValueError:
                # Not a size directory, skip
                continue
    
    return benchmarks


def collect_benchmarks(criterion_dir: Path, machine: str) -> list:
    """Collect all benchmark results and format as CSV rows."""
    rows = []
    
    sha = get_commit_sha()
    date = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    rustc = get_rust_version()
    version_tag = get_git_version_tag()
    
    benchmarks = find_benchmark_directories(criterion_dir)
    
    if not benchmarks:
        print(f"Warning: No benchmark results found in {criterion_dir}")
        return rows
    
    for bench_name, size, estimates_path, bench_dir in benchmarks:
        estimates = extract_estimates(estimates_path, bench_dir)
        if not estimates:
            continue
        
        # Format benchmark name with size if applicable
        full_bench_name = f"{bench_name}/{size}" if size is not None else bench_name
        
        # Extract time metrics (mean and median)
        for metric in ["mean", "median"]:
            metric_key = f"{metric}_ns"
            if metric_key in estimates:
                value_ns = estimates[metric_key]
                ci_low = estimates.get(f"{metric}_ci_low_ns", 0)
                ci_hi = estimates.get(f"{metric}_ci_high_ns", 0)
                
                row = {
                    "commit": sha,
                    "version": version_tag,
                    "date": date,
                    "bench": full_bench_name,
                    "metric": metric,
                    "unit": "ns",
                    "value": value_ns,
                    "ci_low": ci_low,
                    "ci_high": ci_hi,
                    "rustc": rustc,
                    "machine": machine,
                }
                
                # Add allocations if available
                if "allocs" in estimates:
                    row["allocs"] = estimates["allocs"]
                    row["allocs_ci_low"] = estimates.get("allocs_ci_low", 0)
                    row["allocs_ci_high"] = estimates.get("allocs_ci_high", 0)
                else:
                    row["allocs"] = ""
                    row["allocs_ci_low"] = ""
                    row["allocs_ci_high"] = ""
                
                rows.append(row)
    
    return rows


def write_csv(rows: list, output_file: Path):
    """Write rows to CSV file, creating header if needed."""
    import csv
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = [
        "commit",
        "version",
        "date",
        "bench",
        "metric",
        "unit",
        "value",
        "ci_low",
        "ci_high",
        "allocs",
        "allocs_ci_low",
        "allocs_ci_high",
        "rustc",
        "machine",
    ]
    
    # Check if file exists and has header
    file_exists = output_file.exists()
    has_header = False
    needs_version_column = False
    
    if file_exists:
        with open(output_file) as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                has_header = "commit" in reader.fieldnames
                needs_version_column = "version" not in reader.fieldnames
    
    # If file exists but missing version column, we need to add it
    if file_exists and needs_version_column:
        # Read all existing rows
        existing_rows = []
        with open(output_file) as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)
        
        # Rewrite file with version column
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for old_row in existing_rows:
                old_row["version"] = ""  # Add empty version for old rows
                writer.writerow(old_row)
    
    # Write new rows
    with open(output_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not has_header:
            writer.writeheader()
        
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Collect benchmark results from Criterion and append to CSV"
    )
    parser.add_argument(
        "--machine",
        default=None,
        help="Machine identifier (default: from BENCH_MACHINE env or 'local')",
    )
    parser.add_argument(
        "--results-dir",
        default="bench/results",
        help="Output directory for CSV files (default: bench/results)",
    )
    parser.add_argument(
        "--criterion-dir",
        default="target/criterion",
        help="Criterion output directory (default: target/criterion)",
    )
    args = parser.parse_args()
    
    criterion_dir = Path(args.criterion_dir)
    results_dir = Path(args.results_dir)
    
    if not criterion_dir.exists():
        print(f"Error: Criterion directory not found: {criterion_dir}")
        print("Run benchmarks first: cargo bench")
        sys.exit(1)
    
    machine = args.machine or get_machine_id()
    
    print(f"Collecting benchmarks from: {criterion_dir}")
    print(f"Machine: {machine}")
    
    rows = collect_benchmarks(criterion_dir, machine)
    
    if not rows:
        print("No benchmark data collected")
        sys.exit(1)
    
    output_file = results_dir / f"{machine}.csv"
    write_csv(rows, output_file)
    
    print(f"Collected {len(rows)} benchmark results")
    print(f"Appended to: {output_file}")
    
    # Show summary
    benches = set(row["bench"] for row in rows)
    print(f"Benchmarks: {len(benches)}")
    for bench in sorted(benches):
        bench_rows = [r for r in rows if r["bench"] == bench]
        metrics = set(r["metric"] for r in bench_rows)
        print(f"  {bench}: {', '.join(metrics)}")


if __name__ == "__main__":
    main()

