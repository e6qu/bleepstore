"""
BleepStore Benchmark JSON Output Utilities

Provides standardized JSON output support for all benchmark scripts.
"""

import json
import statistics
from datetime import datetime, timezone
from urllib.parse import urlparse


PORT_TO_IMPLEMENTATION = {
    9010: "python",
    9011: "go",
    9012: "rust",
    9013: "zig",
}


def add_json_args(parser):
    """Add --json and --json-file arguments to an argparse parser."""
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Print results as JSON to stdout",
    )
    parser.add_argument(
        "--json-file",
        type=str,
        default=None,
        help="Write results as JSON to the given file path",
    )
    parser.add_argument(
        "--implementation",
        type=str,
        default=None,
        help="Override auto-detected implementation name",
    )


def auto_detect_implementation(endpoint):
    """Detect implementation from endpoint port number.

    Returns 'unknown' if the port does not match a known implementation.
    """
    try:
        parsed = urlparse(endpoint)
        port = parsed.port
        if port is not None:
            return PORT_TO_IMPLEMENTATION.get(port, "unknown")
    except Exception:
        pass
    return "unknown"


def make_result_entry(name, iterations, timings_ms=None, ops_per_sec=None, **extra):
    """Build a single result entry dict with auto-computed percentiles.

    Args:
        name: Human-readable name for this benchmark entry.
        iterations: Number of iterations run.
        timings_ms: Optional list of timing values in milliseconds.
            If provided, percentiles (p50, p95, p99), min, max, mean, and
            stdev are computed automatically.
        ops_per_sec: Optional operations-per-second value.
        **extra: Any additional key-value pairs to include in the entry.

    Returns:
        A dict suitable for inclusion in a BenchmarkResult's results list.
    """
    entry = {"name": name, "iterations": iterations}

    if timings_ms is not None and len(timings_ms) > 0:
        sorted_t = sorted(timings_ms)
        n = len(sorted_t)
        entry["min_ms"] = round(sorted_t[0], 2)
        entry["p50_ms"] = round(sorted_t[int(n * 0.50)], 2) if n > 1 else round(sorted_t[0], 2)
        entry["p95_ms"] = round(sorted_t[min(int(n * 0.95), n - 1)], 2)
        entry["p99_ms"] = round(sorted_t[min(int(n * 0.99), n - 1)], 2)
        entry["max_ms"] = round(sorted_t[-1], 2)
        entry["mean_ms"] = round(statistics.mean(sorted_t), 2)
        entry["stdev_ms"] = round(statistics.stdev(sorted_t), 2) if n > 1 else 0

    if ops_per_sec is not None:
        entry["ops_per_sec"] = round(ops_per_sec, 1)

    entry.update(extra)
    return entry


def build_benchmark_result(endpoint, benchmark, results, implementation=None):
    """Build the top-level benchmark result dict.

    Args:
        endpoint: The endpoint URL used for the benchmark.
        benchmark: Name of the benchmark (e.g. 'latency', 'throughput').
        results: List of result entry dicts.
        implementation: Optional implementation name override.

    Returns:
        A dict matching the BleepStore benchmark JSON schema.
    """
    if implementation is None:
        implementation = auto_detect_implementation(endpoint)

    return {
        "implementation": implementation,
        "endpoint": endpoint,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "benchmark": benchmark,
        "results": results,
    }


def write_json_output(args, result_dict):
    """Write JSON output based on CLI arguments.

    If --json was given, prints JSON to stdout.
    If --json-file was given, writes JSON to the specified file.
    If neither was given, does nothing.
    """
    if not getattr(args, "json_output", False) and not getattr(args, "json_file", None):
        return

    json_str = json.dumps(result_dict, indent=2)

    if getattr(args, "json_output", False):
        print(json_str)

    json_file = getattr(args, "json_file", None)
    if json_file:
        with open(json_file, "w") as f:
            f.write(json_str)
            f.write("\n")
