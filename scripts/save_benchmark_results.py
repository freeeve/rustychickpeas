#!/usr/bin/env python3
"""
Extract benchmark results from Criterion and save to versioned JSON files.

This script extracts performance metrics from Criterion's JSON files
and saves them to a centralized JSON file for historical tracking.

Usage:
    ./scripts/save_benchmark_results.py [--version v0.4.0] [--output-dir benchmarks/]
"""

import json
import os
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

# Benchmark groups to track
BENCHMARK_GROUPS = [
    "builder_add_nodes",
    "builder_add_relationships",
    "builder_finalize",
    "bulk_load_nodes",
    "bulk_load_complete_graph",
    "snapshot_get_neighbors",
    "snapshot_get_property",
    "snapshot_get_nodes_with_property",
]


def get_git_version() -> str:
    """Get current git version (tag or commit hash)."""
    try:
        # Try to get tag
        result = subprocess.run(
            ["git", "describe", "--tags", "--exact-match", "HEAD"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        
        # Try to get version from Cargo.toml
        cargo_toml = Path(__file__).parent.parent / "Cargo.toml"
        if cargo_toml.exists():
            with open(cargo_toml) as f:
                for line in f:
                    if line.startswith("version ="):
                        version = line.split("=")[1].strip().strip('"')
                        return f"v{version}"
        
        # Fall back to commit hash
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return f"commit-{result.stdout.strip()}"
    except subprocess.CalledProcessError:
        return "unknown"


def get_system_info() -> Dict[str, str]:
    """Get system information for the benchmark run."""
    info = {
        "date": datetime.now().isoformat(),
        "platform": sys.platform,
    }
    
    # Try to get Rust version
    try:
        result = subprocess.run(
            ["rustc", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
        info["rust_version"] = result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    # Try to get CPU info (Unix-like systems)
    if sys.platform != "win32":
        try:
            if sys.platform == "darwin":
                result = subprocess.run(
                    ["sysctl", "-n", "machdep.cpu.brand_string"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                info["cpu"] = result.stdout.strip()
            else:
                with open("/proc/cpuinfo") as f:
                    for line in f:
                        if "model name" in line:
                            info["cpu"] = line.split(":")[1].strip()
                            break
        except:
            pass
    
    return info


def extract_benchmark_data(
    criterion_dir: Path, benchmark_group: str, size: int, baseline_name: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Extract benchmark data from Criterion JSON file.
    
    Returns a dict with mean, median, and other statistics in milliseconds.
    """
    search_paths = []
    if baseline_name:
        # Look for baseline-specific directory
        search_paths.append(criterion_dir / benchmark_group / str(size) / baseline_name / "estimates.json")
        search_paths.append(criterion_dir / benchmark_group / f"{baseline_name}" / str(size) / "new" / "estimates.json")
    else:
        # Default: check new (current run) and base (baseline comparison)
        search_paths.append(criterion_dir / benchmark_group / str(size) / "new" / "estimates.json")
        search_paths.append(criterion_dir / benchmark_group / str(size) / "base" / "estimates.json")
    
    for json_path in search_paths:
        if json_path.exists():
            try:
                with open(json_path) as f:
                    data = json.load(f)
                    
                    # Extract statistics
                    mean = data.get("mean", {})
                    median = data.get("median", {})
                    std_dev = data.get("std_dev", {})
                    
                    result = {}
                    
                    if "point_estimate" in mean:
                        result["mean_ms"] = mean["point_estimate"] / 1_000_000
                        if "confidence_interval" in mean:
                            ci = mean["confidence_interval"]
                            result["mean_ci_lower_ms"] = ci.get("lower_bound", 0) / 1_000_000
                            result["mean_ci_upper_ms"] = ci.get("upper_bound", 0) / 1_000_000
                    
                    if "point_estimate" in median:
                        result["median_ms"] = median["point_estimate"] / 1_000_000
                    
                    if "point_estimate" in std_dev:
                        result["std_dev_ms"] = std_dev["point_estimate"] / 1_000_000
                    
                    return result if result else None
            except (json.JSONDecodeError, KeyError) as e:
                continue
    
    return None


def collect_all_benchmark_data(criterion_dir: Path) -> Dict[str, Dict[int, Dict[str, Any]]]:
    """Collect all benchmark data from Criterion directory."""
    data = {}
    
    for benchmark_group in BENCHMARK_GROUPS:
        group_dir = criterion_dir / benchmark_group
        if not group_dir.exists():
            continue
        
        group_data = {}
        
        # Look for size directories
        for size_dir in group_dir.iterdir():
            if not size_dir.is_dir():
                continue
            
            try:
                size = int(size_dir.name)
            except ValueError:
                continue
            
            # Try to extract data (prefer "new" over "base")
            benchmark_data = extract_benchmark_data(criterion_dir, benchmark_group, size, None)
            if benchmark_data:
                group_data[size] = benchmark_data
        
        if group_data:
            data[benchmark_group] = group_data
    
    return data


def save_benchmark_results(
    version: str,
    criterion_dir: Path,
    output_dir: Path,
    system_info: Dict[str, str],
) -> Path:
    """Save benchmark results to a versioned JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Collect all benchmark data
    benchmark_data = collect_all_benchmark_data(criterion_dir)
    
    if not benchmark_data:
        print("Warning: No benchmark data found in Criterion directory")
        return None
    
    # Create result structure
    result = {
        "version": version,
        "system": system_info,
        "benchmarks": benchmark_data,
    }
    
    # Save to JSON file
    output_file = output_dir / f"benchmarks_{version}.json"
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)
    
    print(f"Saved benchmark results to: {output_file}")
    print(f"  Benchmarks: {len(benchmark_data)}")
    print(f"  Total data points: {sum(len(sizes) for sizes in benchmark_data.values())}")
    
    return output_file


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract and save benchmark results from Criterion"
    )
    parser.add_argument(
        "--version",
        help="Version tag/identifier (default: auto-detect from git)",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmarks",
        help="Output directory for JSON files (default: benchmarks/)",
    )
    parser.add_argument(
        "--criterion-dir",
        default="target/criterion",
        help="Criterion output directory (default: target/criterion)",
    )
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    criterion_dir = repo_root / args.criterion_dir
    output_dir = repo_root / args.output_dir
    
    if not criterion_dir.exists():
        print(f"Error: Criterion directory not found: {criterion_dir}")
        print("Run benchmarks first: cargo bench")
        sys.exit(1)
    
    # Get version
    version = args.version or get_git_version()
    print(f"Extracting benchmarks for version: {version}")
    
    # Get system info
    system_info = get_system_info()
    
    # Save results
    output_file = save_benchmark_results(version, criterion_dir, output_dir, system_info)
    
    if output_file:
        print(f"\nTo generate charts, run:")
        print(f"  ./scripts/generate_benchmark_charts.py --version {version}")
        print(f"\nOr to compare across versions:")
        print(f"  ./scripts/generate_benchmark_charts.py --compare")


if __name__ == "__main__":
    main()

