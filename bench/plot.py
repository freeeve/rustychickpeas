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


def _edge_label_pad(idx, ops_per_sec, version_ops, extreme_ops, pad_wide, pad_narrow):
    """Return padding for an edge version label, wider when the label will show ops/s."""
    ops = ops_per_sec[idx] if idx < len(ops_per_sec) else 0
    is_extreme = abs(ops - extreme_ops) < 0.001 if version_ops else False
    if is_extreme:
        return pad_wide
    return pad_narrow


def _label_padding(versions, ops_per_sec, w):
    """Compute extra left/right padding for first/last version labels, capping total width at ~800px."""
    extra_left_pad = 0
    extra_right_pad = 0

    if versions:
        version_ops = [ops_per_sec[i] for i, _ in versions if 0 <= i < len(ops_per_sec)]
        min_ops = min(version_ops) if version_ops else 0
        max_ops = max(version_ops) if version_ops else 0

        for idx, _ in versions:
            if idx == 0:
                # First point shows ops/s when it holds the max; point is shifted 15px left
                extra_left_pad = _edge_label_pad(idx, ops_per_sec, version_ops, max_ops, 125, 55)
            elif idx == len(ops_per_sec) - 1:
                # Last point shows ops/s when it holds the min; label is centered on the point
                extra_right_pad = _edge_label_pad(idx, ops_per_sec, version_ops, min_ops, 200, 60)

    total_width = w + extra_left_pad + extra_right_pad
    if total_width > 800:
        scale = 800 / total_width
        extra_left_pad = int(extra_left_pad * scale)
        extra_right_pad = int(extra_right_pad * scale)

    return extra_left_pad, extra_right_pad


def _point_coords(ops_per_sec, lo, rng, w, pad, extra_left_pad, label_height, available_height):
    """Compute pixel coordinates per data point, inverting Y so higher ops/sec is at the top."""
    coords = []
    for i, ops in enumerate(ops_per_sec):
        t = i / (len(ops_per_sec) - 1) if len(ops_per_sec) > 1 else 0

        if i == 0 and extra_left_pad > 0:
            x = pad + extra_left_pad - 15
        else:
            x = pad + extra_left_pad + t * (w - 2 * pad)

        y = label_height + pad + available_height - ((ops - lo) / rng) * available_height
        coords.append((x, y))
    return coords


def _version_extremes(versions, ops_per_sec):
    """Find min/max ops among version markers and the last marker index holding the min."""
    version_ops = [ops_per_sec[idx] for idx, _ in versions if 0 <= idx < len(ops_per_sec)]
    if not version_ops:
        return 0, 0, None

    min_ops = min(version_ops)
    max_ops = max(version_ops)

    min_last_idx = None
    for idx, _ in versions:
        if 0 <= idx < len(ops_per_sec) and abs(ops_per_sec[idx] - min_ops) < 0.001:
            min_last_idx = idx

    return min_ops, max_ops, min_last_idx


def _marker_label_anchor(idx, x, n_points, w, pad, extra_left_pad):
    """Choose label x position and text anchor for a version marker."""
    if idx == 0 or idx == n_points - 1:
        # First and last points: center label with the point
        return x, "middle"
    label_x = min(x, w - pad - 5 + extra_left_pad)  # Keep 5px from right edge
    anchor = "middle" if x < w - pad - 15 + extra_left_pad else "end"
    return label_x, anchor


def _version_marker_parts(versions, ops_per_sec, point_coords, w, pad, extra_left_pad, label_height, line_color):
    """Render marker circles and labels, showing ops/s on the first max and last min occurrence."""
    parts = []
    min_ops, max_ops, min_last_idx = _version_extremes(versions, ops_per_sec)
    max_shown = False

    for idx, version in versions:
        if not (0 <= idx < len(ops_per_sec)):
            continue

        ops = ops_per_sec[idx]
        x, y = point_coords[idx]

        parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="{line_color}" stroke="white" stroke-width="1"/>'
        )

        show_ops = False
        if abs(ops - max_ops) < 0.001 and not max_shown:
            show_ops = True
            max_shown = True
        elif abs(ops - min_ops) < 0.001 and idx == min_last_idx:
            show_ops = True

        if show_ops:
            label = f"{version} ({format_ops_per_sec(ops)} ops/s)"
        else:
            label = version

        label_x, anchor = _marker_label_anchor(idx, x, len(ops_per_sec), w, pad, extra_left_pad)
        label_y = max(y - 8, label_height + 2)
        parts.append(
            f'<text x="{label_x:.1f}" y="{label_y:.1f}" '
            f'font-size="9" fill="{line_color}" text-anchor="{anchor}" '
            f'font-family="system-ui, -apple-system, sans-serif" '
            f'font-weight="600" opacity="0.9">{label}</text>'
        )

    return parts


def _plain_point_parts(point_coords, versions, line_color):
    """Render small circles for data points that are not version markers."""
    parts = []
    for i, (x, y) in enumerate(point_coords):
        is_version_marker = versions and any(v[0] == i for v in versions)
        if not is_version_marker:
            parts.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="1.5" fill="{line_color}" opacity="0.6"/>'
            )
    return parts


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

    if is_time:
        # Convert nanoseconds to ops/sec: 1e9 / time_ns (higher is better)
        ops_per_sec = [1_000_000_000 / x if x > 0 else 0 for x in xs]
    else:
        ops_per_sec = xs

    lo, hi = min(ops_per_sec), max(ops_per_sec)
    rng = (hi - lo) or 1.0

    extra_left_pad, extra_right_pad = _label_padding(versions, ops_per_sec, w)
    svg_width = w + extra_left_pad + extra_right_pad

    label_height = 15  # Space for labels at top
    chart_h = h - label_height - pad
    available_height = chart_h - pad

    svg_parts = [
        f'<svg width="{svg_width}" height="{h}" viewBox="0 0 {svg_width} {h}" xmlns="http://www.w3.org/2000/svg">',
        f'<rect width="{svg_width}" height="{h}" fill="transparent"/>',
    ]

    # Draw subtle grid line at midpoint
    if rng > 0:
        mid_y = label_height + pad + available_height / 2
        svg_parts.append(
            f'<line x1="{pad + extra_left_pad}" y1="{mid_y:.1f}" x2="{w - pad + extra_left_pad}" y2="{mid_y:.1f}" '
            f'stroke="currentColor" stroke-width="0.5" opacity="0.2" stroke-dasharray="2,2"/>'
        )

    point_coords = _point_coords(
        ops_per_sec, lo, rng, w, pad, extra_left_pad, label_height, available_height
    )
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in point_coords)
    svg_parts.append(
        f'<polyline fill="none" stroke="{line_color}" stroke-width="2" '
        f'points="{pts}" stroke-linecap="round" stroke-linejoin="round"/>'
    )

    if versions:
        svg_parts.extend(
            _version_marker_parts(
                versions, ops_per_sec, point_coords, w, pad, extra_left_pad, label_height, line_color
            )
        )

    svg_parts.extend(_plain_point_parts(point_coords, versions, line_color))

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


def is_exact_tag(version: str) -> bool:
    """Check if version is an exact git tag (starts with 'v' and looks like a version)."""
    if not version:
        return False
    # Exact tags typically start with 'v' followed by numbers/dots, e.g. "v0.4.1", "v1.0.0"
    return version.startswith("v") and any(c.isdigit() for c in version)


def format_version_label(version: str, commit: str) -> str:
    """Format version label: use exact tag if available, otherwise use commit hash."""
    if is_exact_tag(version):
        return version
    elif commit:
        # Use first 8 characters of commit hash
        return commit[:8] if len(commit) >= 8 else commit
    else:
        return version if version else ""


def _append_sample(series, versions, bench, row, key, version_label):
    """Parse row[key] as float and append it (with its version label) to the bench series."""
    try:
        value = float(row[key])
    except (ValueError, KeyError):
        return
    series[bench].append(value)
    if version_label:
        versions[bench].append((len(series[bench]) - 1, version_label))


def load_benchmark_series(csv_file):
    """Read mean-metric rows from the CSV into time/alloc series keyed by benchmark name."""
    time_series = defaultdict(list)
    alloc_series = defaultdict(list)
    time_versions = defaultdict(list)
    alloc_versions = defaultdict(list)

    with open(csv_file) as f:
        for row in csv.DictReader(f):
            if row.get("metric") != "mean":
                continue

            bench = row["bench"]
            version_label = format_version_label(
                row.get("version", "").strip(), row.get("commit", "").strip()
            )

            _append_sample(time_series, time_versions, bench, row, "value", version_label)

            if row.get("allocs") and row["allocs"].strip():
                _append_sample(alloc_series, alloc_versions, bench, row, "allocs", version_label)

    return time_series, alloc_series, time_versions, alloc_versions


def _recent_versions(vals, version_list, max_points):
    """Shift version marker indices into the window of the most recent max_points values."""
    if not version_list:
        return None
    start_idx = max(0, len(vals) - max_points)
    return [
        (idx - start_idx, v)
        for idx, v in version_list
        if start_idx <= idx < len(vals)
    ]


def _badge_label(vals, formatter):
    """Build badge text and color from the last two values (lower is better, so growth is red)."""
    if len(vals) >= 2:
        last, prev = vals[-1], vals[-2]
        pct = (last - prev) / prev * 100.0

        text = formatter(last)
        text += f" ({pct:+.1f}%)"

        if pct > +5:
            color = "#e05d44"  # Red (regression)
        elif pct < -5:
            color = "#4c1"  # Green (improvement)
        else:
            color = "#dfb317"  # Yellow (neutral)
    else:
        text = formatter(vals[-1])
        color = "#4c1"
    return text, color


def _write_svg(path, content):
    """Write SVG content to path and report the created file."""
    with open(path, "w") as f:
        f.write(content)
    print(f"  Created: {path}")


def _generate_series_charts(
    safe_name, vals, version_list, max_points, output_dir, suffix, line_color, is_time, formatter
):
    """Generate the sparkline and badge SVGs for one benchmark series."""
    recent_vals = vals[-max_points:]
    versions = _recent_versions(vals, version_list, max_points)

    svg = sparkline_svg(recent_vals, versions=versions, line_color=line_color, is_time=is_time)
    _write_svg(output_dir / f"{safe_name}{suffix}.svg", svg)

    text, color = _badge_label(vals, formatter)
    _write_svg(output_dir / f"{safe_name}{suffix}_badge.svg", badge_svg(text, color))


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

    time_series, alloc_series, time_versions, alloc_versions = load_benchmark_series(csv_file)

    if not time_series and not alloc_series:
        print(f"Warning: No benchmark data found in {csv_file}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    all_benches = set(time_series.keys()) | set(alloc_series.keys())
    print(f"Generating charts for {len(all_benches)} benchmarks...")

    for bench in all_benches:
        # Sanitize benchmark name for filename
        safe_name = bench.replace("/", "_").replace("\\", "_")

        if bench in time_series:
            _generate_series_charts(
                safe_name, time_series[bench], time_versions.get(bench), args.max_points,
                output_dir, "_time", "#3b82f6", True, format_time_ns,
            )

        if bench in alloc_series:
            _generate_series_charts(
                safe_name, alloc_series[bench], alloc_versions.get(bench), args.max_points,
                output_dir, "_allocs", "#10b981", False, format_allocs,
            )

    print(f"\nAll charts saved to: {output_dir}")


if __name__ == "__main__":
    main()
