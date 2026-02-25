"""
BleepStore Cross-Implementation Benchmark Report Generator

Reads JSON result files from benchmark runs and generates a markdown comparison
report with side-by-side tables for all implementations.

Usage:
    python compare_report.py --results-dir ./results [--output BENCHMARK_RESULTS.md] [--stdout]
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone


# Canonical ordering for implementations
IMPL_ORDER = ["python", "go", "rust", "zig"]
IMPL_DISPLAY = {"python": "Python", "go": "Go", "rust": "Rust", "zig": "Zig"}


def load_results(results_dir):
    """Load all JSON result files from a directory.

    Expects filenames matching the pattern *_*.json (e.g., python_latency.json).

    Returns:
        Dict mapping (implementation, benchmark) -> parsed JSON dict.
    """
    results = {}
    pattern = os.path.join(results_dir, "*_*.json")
    for filepath in sorted(glob.glob(pattern)):
        filename = os.path.basename(filepath)
        name_part = filename.rsplit(".", 1)[0]  # strip .json
        parts = name_part.split("_", 1)
        if len(parts) != 2:
            continue
        impl, benchmark = parts
        try:
            with open(filepath) as f:
                data = json.load(f)
            results[(impl, benchmark)] = data
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: could not load {filepath}: {e}", file=sys.stderr)
    return results


def get_implementations(results):
    """Return sorted list of implementations found in results."""
    impls = sorted(
        {impl for impl, _ in results.keys()},
        key=lambda x: IMPL_ORDER.index(x) if x in IMPL_ORDER else 999,
    )
    return impls


def get_benchmarks(results):
    """Return sorted list of benchmark types found in results."""
    return sorted({bench for _, bench in results.keys()})


def fmt_value(value, precision=2):
    """Format a numeric value for display, returning '-' for None/missing."""
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{precision}f}"
    return str(value)


def find_winner_lower(row_values):
    """Find the implementation with the lowest value (lower is better).

    Args:
        row_values: Dict mapping impl -> numeric value (or None).

    Returns:
        Name of the winning implementation, or '-' if no valid values.
    """
    valid = {k: v for k, v in row_values.items() if v is not None}
    if not valid:
        return "-"
    winner = min(valid, key=lambda k: valid[k])
    return IMPL_DISPLAY.get(winner, winner)


def find_winner_higher(row_values):
    """Find the implementation with the highest value (higher is better).

    Args:
        row_values: Dict mapping impl -> numeric value (or None).

    Returns:
        Name of the winning implementation, or '-' if no valid values.
    """
    valid = {k: v for k, v in row_values.items() if v is not None}
    if not valid:
        return "-"
    winner = max(valid, key=lambda k: valid[k])
    return IMPL_DISPLAY.get(winner, winner)


def generate_latency_table(results, impls):
    """Generate markdown table for latency benchmark results.

    Returns list of markdown lines.
    """
    lines = []
    lines.append("## Latency (p50 ms) -- Lower is Better")
    lines.append("")

    # Gather all operation names from all implementations
    all_ops = []
    for impl in impls:
        data = results.get((impl, "latency"))
        if data and "results" in data:
            for entry in data["results"]:
                name = entry.get("name", "")
                if name and name not in all_ops:
                    all_ops.append(name)

    if not all_ops:
        lines.append("_No latency results found._")
        lines.append("")
        return lines

    # Header
    impl_headers = " | ".join(IMPL_DISPLAY.get(i, i) for i in impls)
    lines.append(f"| Operation | {impl_headers} | Winner |")
    separator = "|-----------|" + "|".join("------" for _ in impls) + "|--------|"
    lines.append(separator)

    # Rows
    for op_name in all_ops:
        row_values = {}
        for impl in impls:
            data = results.get((impl, "latency"))
            value = None
            if data and "results" in data:
                for entry in data["results"]:
                    if entry.get("name") == op_name:
                        value = entry.get("p50_ms")
                        break
            row_values[impl] = value

        cells = " | ".join(fmt_value(row_values.get(i)) for i in impls)
        winner = find_winner_lower(row_values)
        lines.append(f"| {op_name} | {cells} | {winner} |")

    lines.append("")

    # Also generate p95 and p99 tables
    for pct, label in [("p95_ms", "p95"), ("p99_ms", "p99")]:
        lines.append(f"## Latency ({label} ms) -- Lower is Better")
        lines.append("")
        lines.append(f"| Operation | {impl_headers} | Winner |")
        lines.append(separator)

        for op_name in all_ops:
            row_values = {}
            for impl in impls:
                data = results.get((impl, "latency"))
                value = None
                if data and "results" in data:
                    for entry in data["results"]:
                        if entry.get("name") == op_name:
                            value = entry.get(pct)
                            break
                row_values[impl] = value

            cells = " | ".join(fmt_value(row_values.get(i)) for i in impls)
            winner = find_winner_lower(row_values)
            lines.append(f"| {op_name} | {cells} | {winner} |")

        lines.append("")

    return lines


def generate_throughput_table(results, impls):
    """Generate markdown table for throughput benchmark results.

    Returns list of markdown lines.
    """
    lines = []
    lines.append("## Throughput (ops/sec) -- Higher is Better")
    lines.append("")

    # Gather all operation names
    all_ops = []
    for impl in impls:
        data = results.get((impl, "throughput"))
        if data and "results" in data:
            for entry in data["results"]:
                name = entry.get("name", "")
                if name and name not in all_ops:
                    all_ops.append(name)

    if not all_ops:
        lines.append("_No throughput results found._")
        lines.append("")
        return lines

    # Header
    impl_headers = " | ".join(IMPL_DISPLAY.get(i, i) for i in impls)
    lines.append(f"| Operation | {impl_headers} | Winner |")
    separator = "|-----------|" + "|".join("------" for _ in impls) + "|--------|"
    lines.append(separator)

    # Rows
    for op_name in all_ops:
        row_values = {}
        for impl in impls:
            data = results.get((impl, "throughput"))
            value = None
            if data and "results" in data:
                for entry in data["results"]:
                    if entry.get("name") == op_name:
                        value = entry.get("ops_per_sec")
                        break
            row_values[impl] = value

        cells = " | ".join(fmt_value(row_values.get(i), precision=1) for i in impls)
        winner = find_winner_higher(row_values)
        lines.append(f"| {op_name} | {cells} | {winner} |")

    lines.append("")

    # Throughput in MB/s
    lines.append("## Throughput (MB/s) -- Higher is Better")
    lines.append("")
    lines.append(f"| Operation | {impl_headers} | Winner |")
    lines.append(separator)

    for op_name in all_ops:
        row_values = {}
        for impl in impls:
            data = results.get((impl, "throughput"))
            value = None
            if data and "results" in data:
                for entry in data["results"]:
                    if entry.get("name") == op_name:
                        value = entry.get("throughput_mbps")
                        break
            row_values[impl] = value

        cells = " | ".join(fmt_value(row_values.get(i)) for i in impls)
        winner = find_winner_higher(row_values)
        lines.append(f"| {op_name} | {cells} | {winner} |")

    lines.append("")

    return lines


def generate_scaling_table(results, impls):
    """Generate markdown table for scaling benchmark results.

    Returns list of markdown lines.
    """
    lines = []
    lines.append("## Scaling Efficiency (ops/sec at concurrency N)")
    lines.append("")

    # Gather all result entries across implementations
    # Scaling entries have names like "PUT 1KB c=1", "PUT 1KB c=2", etc.
    # Group by operation prefix (PUT 1KB, GET 1KB) and concurrency level

    # First, collect all unique operations and concurrency levels
    operations = {}  # op_prefix -> {concurrency -> {impl -> ops_per_sec}}
    for impl in impls:
        data = results.get((impl, "scaling"))
        if not data or "results" not in data:
            continue
        for entry in data["results"]:
            name = entry.get("name", "")
            # Skip saturation analysis entries
            if "Saturation" in name:
                continue
            # Parse "PUT 1KB c=4" -> op_prefix="PUT 1KB", concurrency=4
            if " c=" in name:
                parts = name.rsplit(" c=", 1)
                op_prefix = parts[0]
                try:
                    concurrency = int(parts[1])
                except (ValueError, IndexError):
                    continue
            else:
                # Fallback: use concurrency field
                op_prefix = name
                concurrency = entry.get("concurrency", 0)

            if op_prefix not in operations:
                operations[op_prefix] = {}
            if concurrency not in operations[op_prefix]:
                operations[op_prefix][concurrency] = {}
            operations[op_prefix][concurrency][impl] = entry.get("ops_per_sec")

    if not operations:
        lines.append("_No scaling results found._")
        lines.append("")
        return lines

    impl_headers = " | ".join(IMPL_DISPLAY.get(i, i) for i in impls)

    for op_prefix in sorted(operations.keys()):
        concurrency_data = operations[op_prefix]
        concurrency_levels = sorted(concurrency_data.keys())

        lines.append(f"### {op_prefix}")
        lines.append("")
        lines.append(f"| Concurrency | {impl_headers} |")
        separator = "|-------------|" + "|".join("------" for _ in impls) + "|"
        lines.append(separator)

        for conc in concurrency_levels:
            impl_values = concurrency_data[conc]
            cells = " | ".join(
                fmt_value(impl_values.get(i), precision=1) for i in impls
            )
            lines.append(f"| {conc} | {cells} |")

        lines.append("")

    # Saturation analysis summary
    lines.append("### Saturation Points")
    lines.append("")
    lines.append(f"| Metric | {impl_headers} |")
    separator = "|--------|" + "|".join("------" for _ in impls) + "|"
    lines.append(separator)

    for metric in ["put_saturation_concurrency", "get_saturation_concurrency"]:
        label = metric.replace("_", " ").replace("concurrency", "").strip().title()
        row_values = {}
        for impl in impls:
            data = results.get((impl, "scaling"))
            if not data or "results" not in data:
                row_values[impl] = None
                continue
            value = None
            for entry in data["results"]:
                if entry.get("name") == "Saturation Analysis":
                    value = entry.get(metric)
                    break
            row_values[impl] = value

        cells = " | ".join(
            fmt_value(row_values.get(i), precision=0) if row_values.get(i) is not None else "none"
            for i in impls
        )
        lines.append(f"| {label} | {cells} |")

    lines.append("")

    return lines


def generate_report(results):
    """Generate the full markdown comparison report.

    Args:
        results: Dict mapping (implementation, benchmark) -> parsed JSON data.

    Returns:
        List of markdown lines.
    """
    impls = get_implementations(results)
    benchmarks = get_benchmarks(results)

    lines = []
    lines.append("# BleepStore Cross-Implementation Benchmark Results")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")
    lines.append(f"Implementations tested: {', '.join(IMPL_DISPLAY.get(i, i) or i for i in impls)}")
    lines.append(f"Benchmarks run: {', '.join(benchmarks)}")
    lines.append("")
    lines.append("---")
    lines.append("")

    if "latency" in benchmarks:
        lines.extend(generate_latency_table(results, impls))

    if "throughput" in benchmarks:
        lines.extend(generate_throughput_table(results, impls))

    if "scaling" in benchmarks:
        lines.extend(generate_scaling_table(results, impls))

    # Summary: overall winners per benchmark
    lines.append("---")
    lines.append("")
    lines.append("## Summary")
    lines.append("")

    if "latency" in benchmarks:
        # Count wins per implementation across all latency operations
        win_counts = {impl: 0 for impl in impls}
        for impl_key in impls:
            data = results.get((impl_key, "latency"))
            if not data or "results" not in data:
                continue
        # Recount across all ops
        all_ops = []
        for impl in impls:
            data = results.get((impl, "latency"))
            if data and "results" in data:
                for entry in data["results"]:
                    name = entry.get("name", "")
                    if name and name not in all_ops:
                        all_ops.append(name)
        for op_name in all_ops:
            row_values = {}
            for impl in impls:
                data = results.get((impl, "latency"))
                if data and "results" in data:
                    for entry in data["results"]:
                        if entry.get("name") == op_name:
                            row_values[impl] = entry.get("p50_ms")
                            break
            valid = {k: v for k, v in row_values.items() if v is not None}
            if valid:
                winner = min(valid, key=lambda k: valid[k])
                win_counts[winner] = win_counts.get(winner, 0) + 1

        if any(win_counts.values()):
            best = max(win_counts, key=lambda k: win_counts[k])
            lines.append(
                f"- **Latency leader:** {IMPL_DISPLAY.get(best, best)} "
                f"({win_counts[best]}/{len(all_ops)} operations)"
            )

    if "throughput" in benchmarks:
        win_counts = {impl: 0 for impl in impls}
        all_ops = []
        for impl in impls:
            data = results.get((impl, "throughput"))
            if data and "results" in data:
                for entry in data["results"]:
                    name = entry.get("name", "")
                    if name and name not in all_ops:
                        all_ops.append(name)
        for op_name in all_ops:
            row_values = {}
            for impl in impls:
                data = results.get((impl, "throughput"))
                if data and "results" in data:
                    for entry in data["results"]:
                        if entry.get("name") == op_name:
                            row_values[impl] = entry.get("ops_per_sec")
                            break
            valid = {k: v for k, v in row_values.items() if v is not None}
            if valid:
                winner = max(valid, key=lambda k: valid[k])
                win_counts[winner] = win_counts.get(winner, 0) + 1

        if any(win_counts.values()):
            best = max(win_counts, key=lambda k: win_counts[k])
            lines.append(
                f"- **Throughput leader:** {IMPL_DISPLAY.get(best, best)} "
                f"({win_counts[best]}/{len(all_ops)} operations)"
            )

    lines.append("")

    return lines


def main():
    parser = argparse.ArgumentParser(
        description="BleepStore Cross-Implementation Benchmark Report Generator"
    )
    parser.add_argument(
        "--results-dir",
        required=True,
        help="Directory containing JSON result files",
    )
    parser.add_argument(
        "--output",
        default="BENCHMARK_RESULTS.md",
        help="Output file path (default: BENCHMARK_RESULTS.md)",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print report to stdout instead of file",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.results_dir):
        print(f"Error: results directory '{args.results_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)

    results = load_results(args.results_dir)

    if not results:
        print(f"Error: no JSON result files found in '{args.results_dir}'.", file=sys.stderr)
        print("Expected files matching pattern: *_*.json (e.g., python_latency.json)", file=sys.stderr)
        sys.exit(1)

    report_lines = generate_report(results)
    report_text = "\n".join(report_lines)

    if args.stdout:
        print(report_text)
    else:
        with open(args.output, "w") as f:
            f.write(report_text)
            f.write("\n")
        print(f"Report written to {args.output}")
        print(f"  Implementations: {', '.join(get_implementations(results))}")
        print(f"  Benchmarks: {', '.join(get_benchmarks(results))}")


if __name__ == "__main__":
    main()
