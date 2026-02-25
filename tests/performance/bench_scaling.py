"""
BleepStore Concurrency Scaling Benchmarks

Sweeps concurrency levels to measure throughput scaling curves.
For each concurrency level, runs PUT and GET workloads for a fixed duration
and reports ops/sec, latency percentiles, and error rates.

Usage:
    python bench_scaling.py [--endpoint URL] [--duration SECONDS] [--levels 1,2,4,8,16,32,64]
"""

import argparse
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

try:
    from output_utils import (
        add_json_args,
        auto_detect_implementation,
        build_benchmark_result,
        make_result_entry,
        write_json_output,
    )

    HAS_OUTPUT_UTILS = True
except ImportError:
    HAS_OUTPUT_UTILS = False

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


def create_client(max_pool=100):
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            max_pool_connections=max_pool,
        ),
    )


def cleanup_bucket(client, bucket):
    """Delete all objects and the bucket itself."""
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                client.delete_object(Bucket=bucket, Key=obj["Key"])
        uploads = client.list_multipart_uploads(Bucket=bucket)
        for u in uploads.get("Uploads", []):
            client.abort_multipart_upload(
                Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"]
            )
        client.delete_bucket(Bucket=bucket)
    except Exception:
        pass


def percentile(data, pct):
    """Calculate the pct-th percentile of sorted data."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * pct / 100)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


def run_workers(operation_func, concurrency, duration):
    """Run `concurrency` workers each calling operation_func in a tight loop
    for `duration` seconds.

    Args:
        operation_func: Callable that takes a worker_id and returns latency in
            seconds for a single operation, or raises on error.
        concurrency: Number of concurrent worker threads.
        duration: Duration in seconds to run each worker.

    Returns:
        Tuple of (latencies_ms, error_count) where latencies_ms is a list of
        individual operation latencies in milliseconds.
    """
    stop_event = threading.Event()
    all_latencies = []
    latency_lock = threading.Lock()
    error_count = [0]
    error_lock = threading.Lock()

    def worker(worker_id):
        local_latencies = []
        local_errors = 0
        while not stop_event.is_set():
            try:
                latency_s = operation_func(worker_id)
                local_latencies.append(latency_s * 1000)  # convert to ms
            except Exception:
                local_errors += 1
        with latency_lock:
            all_latencies.extend(local_latencies)
        with error_lock:
            error_count[0] += local_errors

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, i) for i in range(concurrency)]
        time.sleep(duration)
        stop_event.set()
        for f in futures:
            f.result()

    return all_latencies, error_count[0]


def bench_scaling_put(client, bucket, data, concurrency, duration):
    """Run PUT benchmark at a given concurrency level for `duration` seconds."""
    counter = [0]
    counter_lock = threading.Lock()

    def put_op(worker_id):
        with counter_lock:
            counter[0] += 1
            seq = counter[0]
        key = f"scale-put-{worker_id}-{seq}"
        start = time.monotonic()
        client.put_object(Bucket=bucket, Key=key, Body=data)
        return time.monotonic() - start

    return run_workers(put_op, concurrency, duration)


def bench_scaling_get(client, bucket, get_keys, concurrency, duration):
    """Run GET benchmark at a given concurrency level for `duration` seconds."""
    num_keys = len(get_keys)
    counter = [0]
    counter_lock = threading.Lock()

    def get_op(worker_id):
        with counter_lock:
            counter[0] += 1
            idx = counter[0] % num_keys
        key = get_keys[idx]
        start = time.monotonic()
        resp = client.get_object(Bucket=bucket, Key=key)
        resp["Body"].read()
        return time.monotonic() - start

    return run_workers(get_op, concurrency, duration)


def compute_stats(latencies_ms, errors, duration):
    """Compute stats from a list of latencies in ms."""
    total_ops = len(latencies_ms)
    ops_per_sec = total_ops / duration if duration > 0 else 0

    if total_ops == 0:
        return {
            "ops_per_sec": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "errors": errors,
            "total_ops": 0,
        }

    return {
        "ops_per_sec": round(ops_per_sec, 1),
        "p50_ms": round(percentile(latencies_ms, 50), 2),
        "p95_ms": round(percentile(latencies_ms, 95), 2),
        "p99_ms": round(percentile(latencies_ms, 99), 2),
        "errors": errors,
        "total_ops": total_ops,
    }


def find_saturation_point(results):
    """Identify the concurrency level where throughput stops scaling linearly.

    Saturation is detected when the throughput increase ratio drops below 0.5
    of ideal linear scaling (i.e., doubling concurrency yields less than 50%
    more throughput).

    Returns the concurrency level at which saturation occurs, or None if
    throughput keeps scaling linearly.
    """
    if len(results) < 2:
        return None

    for i in range(1, len(results)):
        prev_conc = results[i - 1]["concurrency"]
        curr_conc = results[i]["concurrency"]
        prev_ops = results[i - 1]["ops_per_sec"]
        curr_ops = results[i]["ops_per_sec"]

        if prev_ops == 0:
            continue

        # How much concurrency grew (e.g., 2x)
        conc_ratio = curr_conc / prev_conc
        # How much throughput grew
        ops_ratio = curr_ops / prev_ops
        # Efficiency: how much of the ideal scaling was achieved
        efficiency = ops_ratio / conc_ratio

        if efficiency < 0.5:
            return curr_conc

    return None


def print_table(title, results):
    """Print a formatted results table."""
    print(f"\n{title}")
    print(
        f"{'Concurrency':>11} | {'Ops/sec':>9} | {'p50 ms':>8} | "
        f"{'p95 ms':>8} | {'p99 ms':>8} | {'Errors':>6}"
    )
    print("-" * 11 + "-+-" + "-" * 9 + "-+-" + "-" * 8 + "-+-"
          + "-" * 8 + "-+-" + "-" * 8 + "-+-" + "-" * 6)
    for r in results:
        print(
            f"{r['concurrency']:>11} | {r['ops_per_sec']:>9.1f} | "
            f"{r['p50_ms']:>8.2f} | {r['p95_ms']:>8.2f} | "
            f"{r['p99_ms']:>8.2f} | {r['errors']:>6}"
        )


def print_saturation(title, results):
    """Print saturation point analysis."""
    sat = find_saturation_point(results)
    if sat is not None:
        print(f"\n  Saturation detected at concurrency={sat} "
              f"(throughput scaling dropped below 50% of ideal)")
    else:
        print(f"\n  No saturation detected — throughput scaled well across all levels")


def main():
    global ENDPOINT, ACCESS_KEY, SECRET_KEY

    parser = argparse.ArgumentParser(description="BleepStore Concurrency Scaling Benchmark")
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument("--duration", type=int, default=10,
                        help="Duration in seconds per concurrency level (default: 10)")
    parser.add_argument("--levels", type=str, default="1,2,4,8,16,32,64",
                        help="Comma-separated concurrency levels (default: 1,2,4,8,16,32,64)")
    parser.add_argument("--object-size", type=int, default=1024,
                        help="Object size in bytes (default: 1024)")
    parser.add_argument("--access-key", default=ACCESS_KEY)
    parser.add_argument("--secret-key", default=SECRET_KEY)

    if HAS_OUTPUT_UTILS:
        add_json_args(parser)

    args = parser.parse_args()

    ENDPOINT = args.endpoint
    ACCESS_KEY = args.access_key
    SECRET_KEY = args.secret_key

    levels = [int(x.strip()) for x in args.levels.split(",")]
    object_size = args.object_size
    duration = args.duration

    # Size label for display
    if object_size >= 1024 * 1024:
        size_label = f"{object_size // (1024 * 1024)}MB"
    elif object_size >= 1024:
        size_label = f"{object_size // 1024}KB"
    else:
        size_label = f"{object_size}B"

    # Determine max concurrency for connection pool sizing
    max_conc = max(levels)
    pool_size = max(max_conc + 10, 100)

    client = create_client(max_pool=pool_size)
    bucket = f"bench-scale-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)

    data = os.urandom(object_size)

    print(f"BleepStore Concurrency Scaling Benchmark")
    print(f"  Endpoint:    {ENDPOINT}")
    print(f"  Object size: {size_label} ({object_size} bytes)")
    print(f"  Duration:    {duration}s per level")
    print(f"  Levels:      {levels}")
    print("=" * 70)

    put_results = []
    get_results = []

    try:
        # --- PUT scaling ---
        print(f"\nRunning PUT {size_label} scaling benchmarks...")
        for level in levels:
            sys.stdout.write(f"  concurrency={level:>3}  ")
            sys.stdout.flush()
            latencies_ms, errors = bench_scaling_put(
                client, bucket, data, level, duration
            )
            stats = compute_stats(latencies_ms, errors, duration)
            stats["concurrency"] = level
            put_results.append(stats)
            sys.stdout.write(
                f"-> {stats['ops_per_sec']:>8.1f} ops/s, "
                f"p50={stats['p50_ms']:.2f}ms, "
                f"p99={stats['p99_ms']:.2f}ms, "
                f"errors={stats['errors']}\n"
            )
            sys.stdout.flush()

        # Seed objects for GET test — put enough objects so we don't bottleneck
        # on re-reading the same key from cache
        num_seed = max(max_conc * 10, 200)
        print(f"\nSeeding {num_seed} objects for GET benchmarks...")
        get_keys = []
        for i in range(num_seed):
            key = f"scale-get-seed-{i:06d}"
            client.put_object(Bucket=bucket, Key=key, Body=data)
            get_keys.append(key)

        # --- GET scaling ---
        print(f"\nRunning GET {size_label} scaling benchmarks...")
        for level in levels:
            sys.stdout.write(f"  concurrency={level:>3}  ")
            sys.stdout.flush()
            latencies_ms, errors = bench_scaling_get(
                client, bucket, get_keys, level, duration
            )
            stats = compute_stats(latencies_ms, errors, duration)
            stats["concurrency"] = level
            get_results.append(stats)
            sys.stdout.write(
                f"-> {stats['ops_per_sec']:>8.1f} ops/s, "
                f"p50={stats['p50_ms']:.2f}ms, "
                f"p99={stats['p99_ms']:.2f}ms, "
                f"errors={stats['errors']}\n"
            )
            sys.stdout.flush()

        # --- Print summary tables ---
        print_table(f"Concurrency Scaling — PUT {size_label}", put_results)
        print_saturation(f"PUT {size_label}", put_results)

        print_table(f"Concurrency Scaling — GET {size_label}", get_results)
        print_saturation(f"GET {size_label}", get_results)

        # --- JSON output ---
        if HAS_OUTPUT_UTILS:
            impl = getattr(args, "implementation", None) or auto_detect_implementation(ENDPOINT)
            json_results = []

            for r in put_results:
                entry = make_result_entry(
                    name=f"PUT {size_label} c={r['concurrency']}",
                    iterations=r["total_ops"],
                    ops_per_sec=r["ops_per_sec"],
                    concurrency=r["concurrency"],
                    p50_ms=r["p50_ms"],
                    p95_ms=r["p95_ms"],
                    p99_ms=r["p99_ms"],
                    errors=r["errors"],
                    object_size=object_size,
                    duration=duration,
                )
                json_results.append(entry)

            for r in get_results:
                entry = make_result_entry(
                    name=f"GET {size_label} c={r['concurrency']}",
                    iterations=r["total_ops"],
                    ops_per_sec=r["ops_per_sec"],
                    concurrency=r["concurrency"],
                    p50_ms=r["p50_ms"],
                    p95_ms=r["p95_ms"],
                    p99_ms=r["p99_ms"],
                    errors=r["errors"],
                    object_size=object_size,
                    duration=duration,
                )
                json_results.append(entry)

            # Add saturation analysis
            put_sat = find_saturation_point(put_results)
            get_sat = find_saturation_point(get_results)
            json_results.append(
                make_result_entry(
                    name="Saturation Analysis",
                    iterations=0,
                    put_saturation_concurrency=put_sat,
                    get_saturation_concurrency=get_sat,
                )
            )

            result_dict = build_benchmark_result(
                endpoint=ENDPOINT,
                benchmark="scaling",
                results=json_results,
                implementation=impl,
            )
            write_json_output(args, result_dict)

    finally:
        # Cleanup
        print(f"\nCleaning up bucket {bucket}...")
        cleanup_bucket(client, bucket)
        print("Done.")


if __name__ == "__main__":
    main()
