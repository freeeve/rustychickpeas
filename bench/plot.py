#!/usr/bin/env python3
"""
Generate SVG sparklines and badges from benchmark CSV data.

This script reads benchmark results from CSV files and generates:
- Sparkline SVGs showing performance trends
- Badge SVGs showing current value and % change

Usage:
    python bench/plot.py [--machine github] [--results-dir bench/results] [--output-dir bench/img]
"""

import csv
import os
import sys
import argparse
from collections import defaultdict
from pathlib import Path


def format_ops_per_sec(ops_per_sec):
    """Format operations per second with appropriate units."""
    if ops_per_sec >= 1_000_000_000:
        return f"{ops_per_sec/1_000_000_000:.2f}B"
    elif ops_per_sec >= 1_000_000:
        return f"{ops_per_sec/1_000_000:.2f}M"
    elif ops_per_sec >= 1_000:
        return f"{ops_per_sec/1_000:.2f}K"
    else:
        return f"{ops_per_sec:.1f}"


def sparkline_svg(xs, w=500, h=120, pad=8, versions=None, line_color="#3b82f6", is_time=True):
    """Generate an enhanced sparkline SVG with version markers and scale.
    
    Args:
        xs: List of values to plot (time in ns if is_time=True, else direct values)
        w: Width in pixels
        h: Height in pixels
        pad: Padding around edges
        versions: List of (index, version_tag) tuples for version markers
        line_color: Color for the line (default: blue, visible in dark mode)
        is_time: If True, convert time (ns) to ops/sec (higher is better)
    """
    if len(xs) < 2:
        xs = xs + xs if xs else [0, 0]
    
    # Convert time to ops/sec if needed (higher is better)
    if is_time:
        # Convert nanoseconds to ops/sec: 1e9 / time_ns
        ops_per_sec = [1_000_000_000 / x if x > 0 else 0 for x in xs]
    else:
        # For allocations, use as-is (lower is better, but we'll invert for display)
        ops_per_sec = xs
    
    lo, hi = min(ops_per_sec), max(ops_per_sec)
    rng = (hi - lo) or 1.0
    
    # Calculate if we need extra space for last point label
    extra_right_pad = 80 if versions and any(v[0] == len(ops_per_sec) - 1 for v in versions) else 0
    svg_width = w + extra_right_pad  # Extend SVG width to accommodate label
    
    # Build the SVG
    label_height = 15  # Space for labels at top
    chart_h = h - label_height - pad
    
    svg_parts = [
        f'<svg width="{svg_width}" height="{h}" viewBox="0 0 {svg_width} {h}" xmlns="http://www.w3.org/2000/svg">',
        # Background (transparent, but can be styled with CSS)
        f'<rect width="{svg_width}" height="{h}" fill="transparent"/>',
    ]
    
    # Draw scale label at top (show max ops/sec)
    if hi > 0:
        max_label = format_ops_per_sec(hi)
        svg_parts.append(
            f'<text x="{w - pad}" y="{label_height - 2}" '
            f'font-size="10" fill="{line_color}" text-anchor="end" '
            f'font-family="system-ui, -apple-system, sans-serif" '
            f'opacity="0.9" font-weight="500">{max_label} ops/s</text>'
        )
    
    # Draw subtle grid line at midpoint
    effective_w = w  # Chart area (not including extra padding for label)
    
    if rng > 0:
        mid_y = label_height + pad + (chart_h - 2 * pad) / 2
        svg_parts.append(
            f'<line x1="{pad}" y1="{mid_y:.1f}" x2="{effective_w-pad}" y2="{mid_y:.1f}" '
            f'stroke="currentColor" stroke-width="0.5" opacity="0.2" stroke-dasharray="2,2"/>'
        )
    
    # Draw the main line (inverted Y so higher ops/sec is at top)
    # Calculate point coordinates - store them for reuse by version markers
    # All points use the same calculation (last point doesn't move)
    point_coords = []
    pts = []
    for i, ops in enumerate(ops_per_sec):
        t = i / (len(ops_per_sec) - 1) if len(ops_per_sec) > 1 else 0
        # All points calculated normally - last point stays at its original position
        X = pad + t * (w - 2 * pad)
        # Invert Y: higher ops/sec = higher on chart
        Y = label_height + pad + chart_h - pad - ((ops - lo) / rng) * (chart_h - 2 * pad)
        point_coords.append((X, Y))
        pts.append(f"{X:.1f},{Y:.1f}")
    
    svg_parts.append(
        f'<polyline fill="none" stroke="{line_color}" stroke-width="2" '
        f'points="{" ".join(pts)}" stroke-linecap="round" stroke-linejoin="round"/>'
    )
    
    # Add version markers if provided
    if versions:
        # Find min and max ops/s values among version markers
        version_ops_list = []
        for idx, version in versions:
            if 0 <= idx < len(ops_per_sec):
                version_ops_list.append(ops_per_sec[idx])
        
        if version_ops_list:
            min_ops = min(version_ops_list)
            max_ops = max(version_ops_list)
        else:
            min_ops = max_ops = 0
        
        # Track which min/max we've already shown
        # For max, show on first occurrence
        # For min, show on last occurrence (most recent)
        max_shown = False
        min_last_idx = None
        
        # Find the last index with min value
        for idx, version in versions:
            if 0 <= idx < len(ops_per_sec):
                ops = ops_per_sec[idx]
                if abs(ops - min_ops) < 0.001:
                    min_last_idx = idx
        
        for idx, version in versions:
            if 0 <= idx < len(ops_per_sec):
                ops = ops_per_sec[idx]
                
                # Use the exact same coordinates as the polyline points
                X, Y = point_coords[idx]
                
                # Draw marker circle
                svg_parts.append(
                    f'<circle cx="{X:.1f}" cy="{Y:.1f}" r="3" fill="{line_color}" stroke="white" stroke-width="1"/>'
                )
                
                # Draw version label (above the marker)
                # Only show ops/s for first occurrence of max and last occurrence of min
                is_min = abs(ops - min_ops) < 0.001
                is_max = abs(ops - max_ops) < 0.001
                
                show_ops = False
                if is_max and not max_shown:
                    show_ops = True
                    max_shown = True
                elif is_min and idx == min_last_idx:
                    show_ops = True
                
                if show_ops:
                    ops_str = format_ops_per_sec(ops)
                    label = f"{version} ({ops_str} ops/s)"
                else:
                    label = version
                
                is_last = idx == len(ops_per_sec) - 1
                if is_last:
                    # Center label with the point
                    label_x = X
                    anchor = "middle"
                else:
                    label_x = min(X, w - pad - 5)  # Keep 5px from right edge
                    anchor = "middle" if X < w - pad - 15 else "end"
                
                label_y = max(Y - 8, label_height + 2)
                svg_parts.append(
                    f'<text x="{label_x:.1f}" y="{label_y:.1f}" '
                    f'font-size="9" fill="{line_color}" text-anchor="{anchor}" '
                    f'font-family="system-ui, -apple-system, sans-serif" '
                    f'font-weight="600" opacity="0.9">{label}</text>'
                )
    
    # Draw data points as small circles for better visibility
    for i, (X, Y) in enumerate(point_coords):
        # Only draw point if it's not a version marker (to avoid overlap)
        is_version_marker = versions and any(v[0] == i for v in versions)
        if not is_version_marker:
            svg_parts.append(
                f'<circle cx="{X:.1f}" cy="{Y:.1f}" r="1.5" fill="{line_color}" opacity="0.6"/>'
            )
    
    svg_parts.append('</svg>')
    return '\n'.join(svg_parts)


def badge_svg(text, color="#4c1"):
    """Generate a badge SVG with label and value."""
    # Wider badge to prevent text overlap
    left, right = "perf", text
    left_width = 55
    right_width = 120  # Increased for wider badges
    total_width = left_width + right_width
    return f'''<svg xmlns="http://www.w3.org/2000/svg" height="20" width="{total_width}">
<g shape-rendering="crispEdges">
  <rect width="{left_width}" height="20" fill="#555"/>
  <rect x="{left_width}" width="{right_width}" height="20" fill="{color}"/>
</g>
<g fill="#fff" font-family="system-ui, -apple-system, sans-serif" font-size="11">
  <text x="{left_width // 2}" y="14" text-anchor="middle" font-weight="500">{left}</text>
  <text x="{left_width + right_width // 2}" y="14" text-anchor="middle" font-weight="500">{right}</text>
</g>
</svg>'''


def format_time_ns(ns):
    """Format nanoseconds to appropriate unit."""
    if ns < 1_000:
        return f"{ns:.1f} ns"
    elif ns < 1_000_000:
        return f"{ns/1_000:.2f} μs"
    elif ns < 1_000_000_000:
        return f"{ns/1_000_000:.2f} ms"
    else:
        return f"{ns/1_000_000_000:.2f} s"


def format_allocs(allocs):
    """Format allocation count."""
    if allocs < 1_000:
        return f"{allocs:.0f}"
    elif allocs < 1_000_000:
        return f"{allocs/1_000:.1f}K"
    elif allocs < 1_000_000_000:
        return f"{allocs/1_000_000:.1f}M"
    else:
        return f"{allocs/1_000_000_000:.1f}B"


def main():
    parser = argparse.ArgumentParser(
        description="Generate SVG sparklines and badges from benchmark CSV"
    )
    parser.add_argument(
        "--machine",
        default="github",
        help="Machine identifier (default: github)",
    )
    parser.add_argument(
        "--results-dir",
        default="bench/results",
        help="Directory containing CSV files (default: bench/results)",
    )
    parser.add_argument(
        "--output-dir",
        default="bench/img",
        help="Output directory for SVG files (default: bench/img)",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=50,
        help="Maximum number of points in sparkline (default: 50)",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    output_dir = Path(args.output_dir)
    
    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}")
        print("Run the collector first: cargo run --package bench-collector --bin collect")
        sys.exit(1)

    # Find CSV file for this machine
    csv_file = results_dir / f"{args.machine}.csv"
    
    if not csv_file.exists():
        print(f"Error: CSV file not found: {csv_file}")
        print(f"Available files: {list(results_dir.glob('*.csv'))}")
        sys.exit(1)

    # Collect data by benchmark name and metric type
    time_series = defaultdict(list)  # For time (ns)
    alloc_series = defaultdict(list)   # For allocations
    time_versions = defaultdict(list)  # Version tags for each data point
    alloc_versions = defaultdict(list)  # Version tags for allocations
    
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        row_index = 0
        for row in reader:
            if row.get("metric") != "mean":
                continue
            
            bench = row["bench"]
            version = row.get("version", "").strip()
            
            # Collect time data
            try:
                value_ns = float(row["value"])
                time_series[bench].append(value_ns)
                if version:
                    time_versions[bench].append((len(time_series[bench]) - 1, version))
            except (ValueError, KeyError):
                pass
            
            # Collect allocation data (if available)
            if row.get("allocs") and row["allocs"].strip():
                try:
                    allocs = float(row["allocs"])
                    alloc_series[bench].append(allocs)
                    if version:
                        alloc_versions[bench].append((len(alloc_series[bench]) - 1, version))
                except (ValueError, KeyError):
                    pass
            
            row_index += 1

    if not time_series and not alloc_series:
        print(f"Warning: No benchmark data found in {csv_file}")
        return

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    all_benches = set(time_series.keys()) | set(alloc_series.keys())
    print(f"Generating charts for {len(all_benches)} benchmarks...")

    for bench in all_benches:
        # Sanitize benchmark name for filename
        safe_name = bench.replace("/", "_").replace("\\", "_")
        
        # Generate time sparkline if available
        if bench in time_series:
            vals = time_series[bench]
            recent_vals = vals[-args.max_points:]
            # Adjust version indices for recent values
            versions = None
            if bench in time_versions and time_versions[bench]:
                start_idx = max(0, len(vals) - args.max_points)
                versions = [
                    (idx - start_idx, v) 
                    for idx, v in time_versions[bench] 
                    if start_idx <= idx < len(vals)
                ]
            svg = sparkline_svg(recent_vals, versions=versions, line_color="#3b82f6", is_time=True)
            
            sparkline_path = output_dir / f"{safe_name}_time.svg"
            with open(sparkline_path, "w") as f:
                f.write(svg)
            print(f"  Created: {sparkline_path}")
            
            # Generate time badge
            if len(vals) >= 2:
                last, prev = vals[-1], vals[-2]
                pct = (last - prev) / prev * 100.0
                
                # Format value
                text = format_time_ns(last)
                text += f" ({pct:+.1f}%)"
                
                # Color based on change (for time, lower is better, so reverse the logic)
                if pct > +5:
                    color = "#e05d44"  # Red (regression - slower)
                elif pct < -5:
                    color = "#4c1"  # Green (improvement - faster)
                else:
                    color = "#dfb317"  # Yellow (neutral)
            else:
                text = format_time_ns(vals[-1])
                color = "#4c1"
            
            badge_path = output_dir / f"{safe_name}_time_badge.svg"
            with open(badge_path, "w") as f:
                f.write(badge_svg(text, color))
            print(f"  Created: {badge_path}")
        
        # Generate allocation sparkline if available
        if bench in alloc_series:
            vals = alloc_series[bench]
            recent_vals = vals[-args.max_points:]
            # Adjust version indices for recent values
            versions = None
            if bench in alloc_versions and alloc_versions[bench]:
                start_idx = max(0, len(vals) - args.max_points)
                versions = [
                    (idx - start_idx, v) 
                    for idx, v in alloc_versions[bench] 
                    if start_idx <= idx < len(vals)
                ]
            # For allocations, lower is better, so we'll show inverse (higher on chart = better)
            # Convert to "efficiency" metric: 1 / (allocs / 1000) to get a meaningful scale
            svg = sparkline_svg(recent_vals, versions=versions, line_color="#10b981", is_time=False)
            
            sparkline_path = output_dir / f"{safe_name}_allocs.svg"
            with open(sparkline_path, "w") as f:
                f.write(svg)
            print(f"  Created: {sparkline_path}")
            
            # Generate allocation badge
            if len(vals) >= 2:
                last, prev = vals[-1], vals[-2]
                pct = (last - prev) / prev * 100.0
                
                # Format value
                text = format_allocs(last)
                text += f" ({pct:+.1f}%)"
                
                # Color based on change (for allocations, lower is better, so reverse the logic)
                if pct > +5:
                    color = "#e05d44"  # Red (regression - more allocations)
                elif pct < -5:
                    color = "#4c1"  # Green (improvement - fewer allocations)
                else:
                    color = "#dfb317"  # Yellow (neutral)
            else:
                text = format_allocs(vals[-1])
                color = "#4c1"
            
            badge_path = output_dir / f"{safe_name}_allocs_badge.svg"
            with open(badge_path, "w") as f:
                f.write(badge_svg(text, color))
            print(f"  Created: {badge_path}")

    print(f"\nAll charts saved to: {output_dir}")


if __name__ == "__main__":
    main()

