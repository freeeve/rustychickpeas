#!/usr/bin/env python3
"""
Generate comparison charts from versioned benchmark JSON files.

This script reads benchmark JSON files and generates:
1. Comparison charts (PNG/SVG) showing performance over time
2. Updated markdown with tables and Mermaid charts

Usage:
    # Generate charts for a specific version
    ./scripts/generate_benchmark_charts.py --version v0.4.0
    
    # Generate comparison charts for all versions
    ./scripts/generate_benchmark_charts.py --compare
    
    # Update performance README
    ./scripts/generate_benchmark_charts.py --update-readme
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import subprocess

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from datetime import datetime
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Warning: matplotlib not available. PNG charts will not be generated.")
    print("Install with: pip install matplotlib")

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

# Sizes to track for each benchmark
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


def load_benchmark_file(file_path: Path) -> Optional[Dict]:
    """Load a benchmark JSON file."""
    try:
        with open(file_path) as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return None


def find_all_benchmark_files(benchmarks_dir: Path) -> List[Tuple[str, Path]]:
    """Find all benchmark JSON files and return (version, path) tuples."""
    files = []
    for file_path in benchmarks_dir.glob("benchmarks_*.json"):
        # Extract version from filename: benchmarks_v0.4.0.json -> v0.4.0
        version = file_path.stem.replace("benchmarks_", "")
        files.append((version, file_path))
    
    # Sort by version (simple string sort, may need improvement for complex versions)
    files.sort(key=lambda x: x[0])
    return files


def parse_version(version: str) -> Tuple:
    """Parse version string for sorting (e.g., 'v0.4.0' -> (0, 4, 0))."""
    # Remove 'v' prefix and split by '.'
    version = version.lstrip('v')
    try:
        parts = [int(p) for p in version.split('.')]
        # Pad to 3 parts for consistent sorting
        while len(parts) < 3:
            parts.append(0)
        return tuple(parts[:3])
    except ValueError:
        # Fall back to string comparison
        return (999, 999, 999)


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
    data_by_version: Dict[str, Dict[int, float]],
) -> str:
    """Generate a markdown table for a benchmark group."""
    lines = [f"### {benchmark_group}", ""]
    
    if not data_by_version:
        lines.append("*No data available*")
        lines.append("")
        return "\n".join(lines)
    
    # Sort versions
    sorted_versions = sorted(data_by_version.keys(), key=parse_version, reverse=True)
    
    # Header
    header = "| Size"
    for version in sorted_versions:
        header += f" | {version}"
    header += " |"
    lines.append(header)
    
    # Separator
    sep = "|" + "---|" * (len(data_by_version) + 1)
    lines.append(sep)
    
    # Rows
    for size in sizes:
        row = f"| {size:,}"
        for version in sorted_versions:
            time_ms = data_by_version[version].get(size)
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
    data_by_version: Dict[str, Dict[int, float]],
) -> str:
    """Generate a Mermaid line chart for a benchmark group."""
    if not data_by_version or len(data_by_version) < 2:
        return ""
    
    # Find max time for y-axis
    max_time = 0
    for times in data_by_version.values():
        if times:
            max_time = max(max_time, max(times.values()))
    
    if max_time == 0:
        return ""
    
    lines = [
        f"```mermaid",
        f"xychart-beta",
        f'    title "{benchmark_group} Performance Over Time"',
        f'    x-axis ["{", ".join(str(s) for s in sizes)}"]',
        f'    y-axis "Time (ms)" 0 --> {max_time * 1.1:.0f}',
    ]
    
    sorted_versions = sorted(data_by_version.keys(), key=parse_version)
    for version in sorted_versions:
        times = [data_by_version[version].get(size, 0) for size in sizes]
        if any(t > 0 for t in times):
            line_data = ", ".join(f"{t:.2f}" for t in times)
            lines.append(f'    line "{version}" [{line_data}]')
    
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def create_comparison_chart_png(
    benchmark_group: str,
    sizes: List[int],
    data_by_version: Dict[str, Dict[int, float]],
    output_dir: Path,
) -> Optional[Path]:
    """Create a PNG comparison chart using matplotlib."""
    if not HAS_MATPLOTLIB or not data_by_version or len(data_by_version) < 2:
        return None
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    sorted_versions = sorted(data_by_version.keys(), key=parse_version)
    
    # Create line for each version
    for version in sorted_versions:
        times = [data_by_version[version].get(size) for size in sizes]
        if any(t is not None for t in times):
            # Filter out None values for plotting
            plot_sizes = [s for s, t in zip(sizes, times) if t is not None]
            plot_times = [t for t in times if t is not None]
            ax.plot(plot_sizes, plot_times, marker='o', label=version, linewidth=2, markersize=8)
    
    ax.set_xlabel('Size', fontsize=12)
    ax.set_ylabel('Time (ms)', fontsize=12)
    ax.set_title(f'{benchmark_group} Performance Comparison', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log')
    ax.set_yscale('log')
    
    plt.tight_layout()
    
    # Save chart
    safe_name = benchmark_group.replace('/', '_')
    output_path = output_dir / f"{safe_name}_comparison.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    return output_path


def collect_data_from_files(benchmark_files: List[Tuple[str, Path]]) -> Dict[str, Dict[str, Dict[int, float]]]:
    """Collect benchmark data from all JSON files."""
    all_data = {}
    
    for version, file_path in benchmark_files:
        data = load_benchmark_file(file_path)
        if not data or "benchmarks" not in data:
            continue
        
        version_data = {}
        benchmarks = data["benchmarks"]
        
        for benchmark_group in BENCHMARK_GROUPS:
            if benchmark_group not in benchmarks:
                continue
            
            group_data = {}
            for size_str, metrics in benchmarks[benchmark_group].items():
                try:
                    size = int(size_str)
                    # Use mean_ms if available, fall back to median_ms
                    time_ms = metrics.get("mean_ms") or metrics.get("median_ms")
                    if time_ms:
                        group_data[size] = time_ms
                except (ValueError, KeyError):
                    continue
            
            if group_data:
                version_data[benchmark_group] = group_data
        
        if version_data:
            all_data[version] = version_data
    
    return all_data


def generate_performance_readme(
    all_data: Dict[str, Dict[str, Dict[int, float]]],
    output_path: Path,
) -> None:
    """Generate the performance README file."""
    lines = [
        "# Performance Benchmarks",
        "",
        "This document tracks performance metrics across different versions of RustyChickpeas.",
        "",
        "> **Note**: This file is auto-generated. To update it, run:",
        "> ```bash",
        "> ./scripts/generate_benchmark_charts.py --update-readme",
        "> ```",
        "",
        "## How to Update",
        "",
        "1. Run benchmarks:",
        "   ```bash",
        "   cargo bench",
        "   ```",
        "",
        "2. Save results to JSON:",
        "   ```bash",
        "   ./scripts/save_benchmark_results.py",
        "   ```",
        "",
        "3. Generate this README and charts:",
        "   ```bash",
        "   ./scripts/generate_benchmark_charts.py --update-readme",
        "   ```",
        "",
        "---",
        "",
    ]
    
    if not all_data:
        lines.extend([
            "## No Benchmark Data Available",
            "",
            "Run benchmarks and save results first:",
            "```bash",
            "cargo bench",
            "./scripts/save_benchmark_results.py",
            "```",
            "",
        ])
    else:
        # Generate tables and charts for each benchmark group
        for benchmark_group in BENCHMARK_GROUPS:
            sizes = BENCHMARK_SIZES.get(benchmark_group, [])
            if not sizes:
                continue
            
            # Collect data for this group across all versions
            data_by_version: Dict[str, Dict[int, float]] = {}
            for version, version_data in all_data.items():
                if benchmark_group in version_data:
                    data_by_version[version] = version_data[benchmark_group]
            
            if data_by_version:
                lines.append(generate_markdown_table(benchmark_group, sizes, data_by_version))
                # Add chart if we have multiple data points
                if len(data_by_version) > 1:
                    lines.append(generate_mermaid_chart(benchmark_group, sizes, data_by_version))
    
    # Write output
    output_path.write_text("\n".join(lines))
    print(f"Generated performance README: {output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate benchmark comparison charts and documentation"
    )
    parser.add_argument(
        "--version",
        help="Generate charts for a specific version",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Generate comparison charts for all versions",
    )
    parser.add_argument(
        "--update-readme",
        action="store_true",
        help="Update the performance README file",
    )
    parser.add_argument(
        "--benchmarks-dir",
        default="benchmarks",
        help="Directory containing benchmark JSON files (default: benchmarks/)",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmarks/charts",
        help="Output directory for PNG charts (default: benchmarks/charts/)",
    )
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    benchmarks_dir = repo_root / args.benchmarks_dir
    output_dir = repo_root / args.output_dir
    
    if not benchmarks_dir.exists():
        print(f"Error: Benchmarks directory not found: {benchmarks_dir}")
        print("Run save_benchmark_results.py first to create benchmark JSON files.")
        return
    
    # Find all benchmark files
    benchmark_files = find_all_benchmark_files(benchmarks_dir)
    
    if not benchmark_files:
        print(f"No benchmark JSON files found in {benchmarks_dir}")
        print("Run: ./scripts/save_benchmark_results.py")
        return
    
    print(f"Found {len(benchmark_files)} benchmark files")
    
    # Collect data
    all_data = collect_data_from_files(benchmark_files)
    
    if args.update_readme or (not args.version and not args.compare):
        # Update README by default
        readme_path = repo_root / "rustychickpeas-core" / "benches" / "PERFORMANCE.md"
        generate_performance_readme(all_data, readme_path)
    
    if args.compare or args.update_readme:
        # Generate PNG charts for all versions
        if HAS_MATPLOTLIB:
            output_dir.mkdir(parents=True, exist_ok=True)
            print(f"\nGenerating comparison charts...")
            
            for benchmark_group in BENCHMARK_GROUPS:
                sizes = BENCHMARK_SIZES.get(benchmark_group, [])
                if not sizes:
                    continue
                
                # Collect data for this group
                data_by_version: Dict[str, Dict[int, float]] = {}
                for version, version_data in all_data.items():
                    if benchmark_group in version_data:
                        data_by_version[version] = version_data[benchmark_group]
                
                if len(data_by_version) > 1:
                    chart_path = create_comparison_chart_png(
                        benchmark_group, sizes, data_by_version, output_dir
                    )
                    if chart_path:
                        print(f"  Created: {chart_path}")
        else:
            print("\nSkipping PNG chart generation (matplotlib not available)")


if __name__ == "__main__":
    main()

