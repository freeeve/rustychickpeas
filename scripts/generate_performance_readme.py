#!/usr/bin/env python3
"""
Generate a performance tracking README from Criterion benchmark data.

This script extracts performance metrics from Criterion's JSON files
and generates a markdown file with performance tables and charts.

Usage:
    ./scripts/generate_performance_readme.py [--extract-from-tags]

NOTE: This is the original script. For a more robust system with JSON storage
and better chart generation, see:
  - scripts/save_benchmark_results.py
  - scripts/generate_benchmark_charts.py
  - scripts/BENCHMARK_TRACKING.md

The new system is recommended for long-term benchmark tracking.
"""

import json
import os
import sys
import argparse
import subprocess
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

# Criterion's per-benchmark statistics file name
ESTIMATES_JSON = "estimates.json"

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

# Sizes to track for each benchmark (use the most representative sizes)
BENCHMARK_SIZES = {
    "builder_add_nodes": [10000, 100000],
    "builder_add_relationships": [10000, 100000],
    "builder_finalize": [10000, 100000],
    "bulk_load_nodes": [100000, 1000000],
    "bulk_load_complete_graph": [100000, 1000000],
    "snapshot_get_neighbors": [10000, 100000],
    "snapshot_get_property": [10000, 100000],
    "snapshot_get_nodes_with_property": [10000, 100000],
}


def get_git_tags() -> List[str]:
    """Get all git tags sorted by version."""
    try:
        result = subprocess.run(
            ["git", "tag", "--sort=-version:refname"],
            capture_output=True,
            text=True,
            check=True,
        )
        tags = [tag.strip() for tag in result.stdout.strip().split("\n") if tag.strip()]
        return [tag for tag in tags if tag.startswith("v")]  # Only version tags
    except subprocess.CalledProcessError:
        return []


def extract_benchmark_data(
    criterion_dir: Path, benchmark_group: str, size: int, baseline_name: Optional[str] = None
) -> Optional[float]:
    """
    Extract mean time in milliseconds from Criterion JSON file.
    
    If baseline_name is provided, looks for that baseline. Otherwise looks for 'new' or 'base'.
    """
    # Criterion stores baselines in the same directory structure
    # When save_baseline(name) is used, it stores in a subdirectory named after the baseline
    # But the actual structure might vary - we'll check multiple locations
    
    search_paths = []
    if baseline_name:
        # Look for baseline-specific directory
        search_paths.append(criterion_dir / benchmark_group / str(size) / baseline_name / ESTIMATES_JSON)
        # Also check if baseline is stored differently
        search_paths.append(criterion_dir / benchmark_group / f"{baseline_name}" / str(size) / "new" / ESTIMATES_JSON)
    else:
        # Default: check new (current run) and base (baseline comparison)
        search_paths.append(criterion_dir / benchmark_group / str(size) / "new" / ESTIMATES_JSON)
        search_paths.append(criterion_dir / benchmark_group / str(size) / "base" / ESTIMATES_JSON)

    for json_path in search_paths:
        if json_path.exists():
            try:
                with open(json_path) as f:
                    data = json.load(f)
                    # Point estimate is in nanoseconds
                    point_estimate = data.get("mean", {}).get("point_estimate")
                    if point_estimate:
                        # Convert to milliseconds
                        return point_estimate / 1_000_000
            except (json.JSONDecodeError, KeyError):
                continue

    return None


def collect_criterion_data(
    criterion_dir: Path, baseline_name: Optional[str]
) -> Dict[str, Dict[int, float]]:
    """Collect mean times for all tracked benchmark groups and sizes from a Criterion directory."""
    data = {}
    for benchmark_group in BENCHMARK_GROUPS:
        sizes = BENCHMARK_SIZES.get(benchmark_group, [])
        group_data = {}
        for size in sizes:
            time_ms = extract_benchmark_data(criterion_dir, benchmark_group, size, baseline_name)
            if time_ms:
                group_data[size] = time_ms
        if group_data:
            data[benchmark_group] = group_data
    return data


def _collect_data_from_checkout(tag: str) -> Dict[str, Dict[int, float]]:
    """Check out a tag, collect data from its Criterion directory, and restore the original branch."""
    current_branch = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
    ).stdout.strip()

    data = {}
    try:
        subprocess.run(
            ["git", "checkout", tag],
            capture_output=True,
            check=True,
        )
        data = collect_criterion_data(Path("target") / "criterion", None)
        subprocess.run(
            ["git", "checkout", current_branch],
            capture_output=True,
        )
    except subprocess.CalledProcessError:
        # Try to return to original branch
        try:
            subprocess.run(["git", "checkout", current_branch], capture_output=True)
        except (subprocess.SubprocessError, OSError):
            pass
    return data


def collect_data_for_tag(
    criterion_dir: Path, tag: str, extract_from_tag: bool = False
) -> Dict[str, Dict[int, float]]:
    """
    Collect benchmark data for a specific tag.

    If extract_from_tag is True, will checkout the tag and look for existing data,
    or optionally run benchmarks (not implemented yet).
    """
    if extract_from_tag:
        return _collect_data_from_checkout(tag)
    # Extract from current criterion directory using tag as baseline name
    return collect_criterion_data(criterion_dir, tag)


def format_time_ms(time_ms: float) -> str:
    """Format time in milliseconds with appropriate units."""
    if time_ms < 1:
        return f"{time_ms * 1000:.2f} μs"
    elif time_ms < 1000:
        return f"{time_ms:.2f} ms"
    else:
        return f"{time_ms / 1000:.2f} s"


def generate_markdown_table(
    benchmark_group: str,
    sizes: List[int],
    data_by_tag: Dict[str, Dict[int, float]],
) -> str:
    """Generate a markdown table for a benchmark group."""
    lines = [f"### {benchmark_group}", ""]
    
    if not data_by_tag:
        lines.append("*No data available*")
        lines.append("")
        return "\n".join(lines)
    
    # Header
    header = "| Size"
    sorted_tags = sorted(data_by_tag.keys(), reverse=True)
    for tag in sorted_tags:
        header += f" | {tag}"
    header += " |"
    lines.append(header)
    
    # Separator
    sep = "|" + "---|" * (len(data_by_tag) + 1)
    lines.append(sep)
    
    # Rows
    for size in sizes:
        row = f"| {size:,}"
        for tag in sorted_tags:
            time_ms = data_by_tag[tag].get(size)
            if time_ms is not None:
                row += f" | {format_time_ms(time_ms)}"
            else:
                row += " | —"
        row += " |"
        lines.append(row)
    
    lines.append("")
    return "\n".join(lines)


def generate_mermaid_chart(
    benchmark_group: str,
    sizes: List[int],
    data_by_tag: Dict[str, Dict[int, float]],
) -> str:
    """Generate a Mermaid line chart for a benchmark group."""
    if not data_by_tag or len(data_by_tag) < 2:
        return ""
    
    # Find max time for y-axis
    max_time = 0
    for times in data_by_tag.values():
        if times:
            max_time = max(max_time, max(times.values()))
    
    if max_time == 0:
        return ""
    
    lines = [
        "```mermaid",
        "xychart-beta",
        f'    title "{benchmark_group} Performance Over Time"',
        f'    x-axis ["{", ".join(str(s) for s in sizes)}"]',
        f'    y-axis "Time (ms)" 0 --> {max_time * 1.1:.0f}',
    ]
    
    sorted_tags = sorted(data_by_tag.keys())
    for tag in sorted_tags:
        times = [data_by_tag[tag].get(size, 0) for size in sizes]
        if any(t > 0 for t in times):
            line_data = ", ".join(f"{t:.2f}" for t in times)
            lines.append(f'    line "{tag}" [{line_data}]')
    
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _collect_all_data(
    criterion_dir: Path, tags: List[str], extract_from_tags: bool
) -> Dict[str, Dict[str, Dict[int, float]]]:
    """Collect benchmark data for the current run and the most recent tags."""
    all_data: Dict[str, Dict[str, Dict[int, float]]] = {}

    # First, try to get current data (from "new" directories)
    current_data = collect_criterion_data(criterion_dir, None)
    if current_data:
        all_data["current"] = current_data

    # Collect data for each tag
    # When benchmarks are run with BENCHMARK_BASELINE=tag, Criterion saves
    # the baseline data. We'll try to extract it using the tag name.
    print(f"Extracting data for {len(tags)} tags...")
    for tag in tags[:10]:  # Limit to last 10 tags to avoid too much data
        tag_data = collect_data_for_tag(criterion_dir, tag, extract_from_tags)
        if tag_data:
            all_data[tag] = tag_data
            print(f"  Found data for {tag}")

    return all_data


def _generate_benchmark_sections(all_data: Dict[str, Dict[str, Dict[int, float]]]) -> List[str]:
    """Generate table and chart markdown sections for each benchmark group."""
    lines = []
    for benchmark_group in BENCHMARK_GROUPS:
        sizes = BENCHMARK_SIZES.get(benchmark_group, [])
        if not sizes:
            continue

        # Collect data for this group across all tags
        data_by_tag: Dict[str, Dict[int, float]] = {}
        for tag, tag_data in all_data.items():
            if benchmark_group in tag_data:
                data_by_tag[tag] = tag_data[benchmark_group]

        if data_by_tag:
            lines.append(generate_markdown_table(benchmark_group, sizes, data_by_tag))
            # Add chart if we have multiple data points
            if len(data_by_tag) > 1:
                lines.append(generate_mermaid_chart(benchmark_group, sizes, data_by_tag))
    return lines


def generate_performance_readme(
    criterion_dir: Path, output_path: Path, tags: List[str], extract_from_tags: bool = False
) -> None:
    """Generate the performance README file."""
    lines = [
        "# Performance Benchmarks",
        "",
        "This document tracks performance metrics across different versions of RustyChickpeas.",
        "",
        "> **Note**: This file is auto-generated. To update it, run:",
        "> ```bash",
        "> ./scripts/generate_performance_readme.py",
        "> ```",
        "",
        "## How to Update",
        "",
        "1. Run benchmarks for a specific tag:",
        "   ```bash",
        "   ./scripts/benchmark_tag.sh v0.4.0",
        "   ```",
        "",
        "2. Generate this README:",
        "   ```bash",
        "   ./scripts/generate_performance_readme.py",
        "   ```",
        "",
        "3. Commit the updated README.",
        "",
        "---",
        "",
    ]

    all_data = _collect_all_data(criterion_dir, tags, extract_from_tags)

    if not all_data:
        lines.extend([
            "## No Benchmark Data Available",
            "",
            "Run benchmarks first using:",
            "```bash",
            "cargo bench",
            "```",
            "",
            "Or run benchmarks for a specific tag:",
            "```bash",
            "./scripts/benchmark_tag.sh v0.4.0",
            "```",
            "",
        ])
    else:
        lines.extend(_generate_benchmark_sections(all_data))

    output_path.write_text("\n".join(lines))
    print(f"Generated performance README: {output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate performance tracking README from Criterion benchmark data"
    )
    parser.add_argument(
        "--extract-from-tags",
        action="store_true",
        help="Extract data by checking out each tag (slower but more accurate)",
    )
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    criterion_dir = repo_root / "target" / "criterion"
    output_path = repo_root / "rustychickpeas-core" / "benches" / "PERFORMANCE.md"
    
    tags = get_git_tags()
    print(f"Found {len(tags)} version tags")
    
    generate_performance_readme(criterion_dir, output_path, tags, args.extract_from_tags)


if __name__ == "__main__":
    main()
