#!/usr/bin/env python3
"""
BleepStore Workload Profile Benchmarks

Simulates realistic workload patterns with different object size distributions
and read/write ratios. Each profile models a real-world usage pattern.

Profiles:
    web         80/20 read-heavy, 1-100KB (web assets, thumbnails)
    data-lake   40/60 write-heavy, 10MB-100MB (analytics, data pipelines)
    backup      5/95 sequential writes, 10-100MB (backup/archive)
    mixed       50/50, 1KB-10MB uniform (general purpose)
    all         Run all profiles sequentially

Usage:
    python bench_profiles.py --profile web [--endpoint URL] [--duration 30] [--concurrency 8]
    python bench_profiles.py --profile all --json
    python bench_profiles.py --profile data-lake --large --duration 60
"""

import argparse
import math
import os
import random
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

from output_utils import add_json_args, build_benchmark_result, write_json_output

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Workload profile definitions
# ---------------------------------------------------------------------------
# Each profile: (read_pct, write_pct, min_bytes, max_bytes, description)
PROFILES = {
    "web": {
        "read_pct": 80,
        "write_pct": 20,
        "min_bytes": 1 * 1024,           # 1KB
        "max_bytes": 100 * 1024,          # 100KB
        "description": "Web assets, thumbnails, small files",
    },
    "data-lake": {
        "read_pct": 40,
        "write_pct": 60,
        "min_bytes": 10 * 1024 * 1024,    # 10MB
        "max_bytes": 100 * 1024 * 1024,   # 100MB (capped; --large raises to 1GB)
        "description": "Analytics, data pipeline ingestion",
    },
    "backup": {
        "read_pct": 5,
        "write_pct": 95,
        "min_bytes": 10 * 1024 * 1024,    # 10MB
        "max_bytes": 100 * 1024 * 1024,   # 100MB
        "description": "Backup/archive workload",
    },
    "mixed": {
        "read_pct": 50,
        "write_pct": 50,
        "min_bytes": 1 * 1024,            # 1KB
        "max_bytes": 10 * 1024 * 1024,    # 10MB
        "description": "General purpose mixed workload",
    },
}


def create_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            max_pool_connections=150,
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


def log_uniform_size(min_bytes, max_bytes):
    """Generate a random size using log-uniform distribution.

    Log-uniform produces more realistic file size distributions where
    smaller files are more common than very large ones within the range.
    """
    log_min = math.log(max(min_bytes, 1))
    log_max = math.log(max(max_bytes, 1))
    return int(math.exp(random.uniform(log_min, log_max)))


def format_bytes(n):
    """Format a byte count as a human-readable string."""
    if n >= 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024 * 1024):.1f} GB"
    if n >= 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


def percentile(data, pct):
    """Calculate the pct-th percentile of data."""
    if not data:
        return 0.0
    s = sorted(data)
    idx = min(int(len(s) * pct / 100), len(s) - 1)
    return s[idx]


# ---------------------------------------------------------------------------
# Seeding: pre-create objects for GET operations
# ---------------------------------------------------------------------------
def seed_objects(client, bucket, profile, count, large_mode):
    """Pre-create objects that GET operations will read from.

    Returns a list of (key, size_bytes) tuples for the seeded objects.
    """
    min_bytes = profile["min_bytes"]
    max_bytes = profile["max_bytes"]
    if not large_mode and max_bytes > 100 * 1024 * 1024:
        max_bytes = 100 * 1024 * 1024

    seeds = []
    for i in range(count):
        size = log_uniform_size(min_bytes, max_bytes)
        key = f"seed-{i:06d}"
        body = os.urandom(size)
        client.put_object(Bucket=bucket, Key=key, Body=body)
        seeds.append((key, size))
    return seeds


# ---------------------------------------------------------------------------
# Worker logic
# ---------------------------------------------------------------------------
def run_profile(profile_name, profile, duration, concurrency, large_mode):
    """Execute a single workload profile and return results.

    Workers randomly choose PUT or GET per the profile's ratio, generate
    appropriately-sized objects, and record per-operation latencies.
    """
    client = create_client()
    bucket = f"prof-{profile_name[:6]}-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)

    min_bytes = profile["min_bytes"]
    max_bytes = profile["max_bytes"]
    if not large_mode and max_bytes > 100 * 1024 * 1024:
        max_bytes = 100 * 1024 * 1024

    read_pct = profile["read_pct"]

    # Seed objects for GET operations
    seed_count = max(concurrency * 5, 20)
    print(f"  Seeding {seed_count} objects for read operations...")
    seeds = seed_objects(client, bucket, profile, seed_count, large_mode)

    # Shared mutable state protected by a lock
    lock = threading.Lock()
    put_latencies = []
    get_latencies = []
    put_errors = 0
    get_errors = 0
    all_sizes = []
    stop_event = threading.Event()
    put_counter = [0]
    # Track keys written during the benchmark (available for GET)
    written_keys = list(seeds)  # start with seed keys

    def worker(worker_id):
        nonlocal put_errors, get_errors
        c = create_client()

        while not stop_event.is_set():
            is_read = random.randint(1, 100) <= read_pct

            if is_read:
                # GET: pick a random seeded or previously-written object
                with lock:
                    if not written_keys:
                        continue
                    key, size = random.choice(written_keys)

                start = time.monotonic()
                try:
                    resp = c.get_object(Bucket=bucket, Key=key)
                    resp["Body"].read()
                    elapsed_ms = (time.monotonic() - start) * 1000
                    with lock:
                        get_latencies.append(elapsed_ms)
                        all_sizes.append(size)
                except Exception:
                    with lock:
                        get_errors += 1
            else:
                # PUT: generate a random-sized object
                size = log_uniform_size(min_bytes, max_bytes)
                body = os.urandom(size)

                with lock:
                    put_counter[0] += 1
                    key = f"prof-{worker_id}-{put_counter[0]:06d}"

                start = time.monotonic()
                try:
                    c.put_object(Bucket=bucket, Key=key, Body=body)
                    elapsed_ms = (time.monotonic() - start) * 1000
                    with lock:
                        put_latencies.append(elapsed_ms)
                        all_sizes.append(size)
                        written_keys.append((key, size))
                except Exception:
                    with lock:
                        put_errors += 1

    try:
        # Launch workers
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(worker, i) for i in range(concurrency)]

            # Run for the specified duration
            time.sleep(duration)
            stop_event.set()

            # Collect any exceptions
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception:
                    pass

        # Build results
        total_put = len(put_latencies)
        total_get = len(get_latencies)
        total_ops = total_put + total_get
        total_errors = put_errors + get_errors

        all_latencies = put_latencies + get_latencies

        result = {
            "profile": profile_name,
            "read_pct": read_pct,
            "write_pct": profile["write_pct"],
            "duration_s": duration,
            "concurrency": concurrency,
            "put": {
                "count": total_put,
                "ops_per_sec": round(total_put / duration, 1) if duration > 0 else 0,
                "p50_ms": round(percentile(put_latencies, 50), 1) if put_latencies else 0,
                "p95_ms": round(percentile(put_latencies, 95), 1) if put_latencies else 0,
                "p99_ms": round(percentile(put_latencies, 99), 1) if put_latencies else 0,
                "errors": put_errors,
            },
            "get": {
                "count": total_get,
                "ops_per_sec": round(total_get / duration, 1) if duration > 0 else 0,
                "p50_ms": round(percentile(get_latencies, 50), 1) if get_latencies else 0,
                "p95_ms": round(percentile(get_latencies, 95), 1) if get_latencies else 0,
                "p99_ms": round(percentile(get_latencies, 99), 1) if get_latencies else 0,
                "errors": get_errors,
            },
            "overall": {
                "count": total_ops,
                "ops_per_sec": round(total_ops / duration, 1) if duration > 0 else 0,
                "p50_ms": round(percentile(all_latencies, 50), 1) if all_latencies else 0,
                "p95_ms": round(percentile(all_latencies, 95), 1) if all_latencies else 0,
                "p99_ms": round(percentile(all_latencies, 99), 1) if all_latencies else 0,
                "errors": total_errors,
            },
            "size_distribution": {},
        }

        if all_sizes:
            total_bytes = sum(all_sizes)
            result["size_distribution"] = {
                "min": format_bytes(min(all_sizes)),
                "max": format_bytes(max(all_sizes)),
                "mean": format_bytes(int(statistics.mean(all_sizes))),
                "total_transferred": format_bytes(total_bytes),
                "total_bytes": total_bytes,
            }

        return result

    finally:
        print(f"  Cleaning up bucket {bucket}...")
        cleanup_bucket(client, bucket)


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------
def print_profile_results(result):
    """Print formatted results for a single profile run."""
    profile = result["profile"]
    read_pct = result["read_pct"]
    write_pct = result["write_pct"]
    duration = result["duration_s"]
    concurrency = result["concurrency"]

    prof_def = PROFILES[profile]
    min_size = format_bytes(prof_def["min_bytes"])
    max_size = format_bytes(prof_def["max_bytes"])

    print()
    print(f"Workload Profile: {profile} ({read_pct}% read, {write_pct}% write, {min_size}-{max_size})")
    print(f"Duration: {duration}s, Concurrency: {concurrency}")
    print()

    header = f"{'Operation':<10} {'Count':<8} {'Ops/sec':<10} {'p50 ms':<10} {'p95 ms':<10} {'p99 ms':<10} {'Errors':<8}"
    print(header)
    print("-" * len(header))

    for op_name, op_key in [("PUT", "put"), ("GET", "get"), ("Overall", "overall")]:
        op = result[op_key]
        print(
            f"{op_name:<10} {op['count']:<8} {op['ops_per_sec']:<10} "
            f"{op['p50_ms']:<10} {op['p95_ms']:<10} {op['p99_ms']:<10} {op['errors']:<8}"
        )

    size_dist = result.get("size_distribution", {})
    if size_dist:
        print()
        print("Size Distribution:")
        print(f"  Min: {size_dist['min']}, Max: {size_dist['max']}, Mean: {size_dist['mean']}")
        print(f"  Total transferred: {size_dist['total_transferred']}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global ENDPOINT, ACCESS_KEY, SECRET_KEY

    parser = argparse.ArgumentParser(
        description="BleepStore Workload Profile Benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Profiles:
  web         80/20 read-heavy, 1-100KB (web assets, thumbnails)
  data-lake   40/60 write-heavy, 10MB-100MB (analytics, data pipelines)
  backup      5/95 sequential writes, 10-100MB (backup/archive)
  mixed       50/50, 1KB-10MB (general purpose)
  all         Run all profiles sequentially
""",
    )
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument(
        "--profile",
        required=True,
        choices=list(PROFILES.keys()) + ["all"],
        help="Workload profile to run",
    )
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds per profile (default: 30)")
    parser.add_argument("--concurrency", type=int, default=8, help="Number of concurrent workers (default: 8)")
    parser.add_argument("--access-key", default=ACCESS_KEY, help="S3 access key")
    parser.add_argument("--secret-key", default=SECRET_KEY, help="S3 secret key")
    parser.add_argument(
        "--large",
        action="store_true",
        help="Allow data-lake profile to use full 1GB max size (default caps at 100MB)",
    )
    add_json_args(parser)
    args = parser.parse_args()

    ENDPOINT = args.endpoint
    ACCESS_KEY = args.access_key
    SECRET_KEY = args.secret_key

    if args.profile == "all":
        profiles_to_run = list(PROFILES.keys())
    else:
        profiles_to_run = [args.profile]

    print("BleepStore Workload Profile Benchmark")
    print(f"  Endpoint:    {ENDPOINT}")
    print(f"  Profiles:    {', '.join(profiles_to_run)}")
    print(f"  Duration:    {args.duration}s per profile")
    print(f"  Concurrency: {args.concurrency}")
    print(f"  Large mode:  {'yes' if args.large else 'no'}")
    print("=" * 60)

    all_results = []

    for profile_name in profiles_to_run:
        profile = PROFILES[profile_name]
        print()
        print(f"--- Running profile: {profile_name} ({profile['description']}) ---")
        print(f"  Read/Write ratio: {profile['read_pct']}/{profile['write_pct']}")
        print(f"  Size range: {format_bytes(profile['min_bytes'])} - {format_bytes(profile['max_bytes'])}")

        result = run_profile(
            profile_name,
            profile,
            duration=args.duration,
            concurrency=args.concurrency,
            large_mode=args.large,
        )

        print_profile_results(result)
        all_results.append(result)

    # Summary across all profiles
    if len(all_results) > 1:
        print()
        print("=" * 60)
        print("Summary (all profiles)")
        print()
        header = f"{'Profile':<12} {'Ops':<8} {'Ops/sec':<10} {'p50 ms':<10} {'p99 ms':<10} {'Errors':<8}"
        print(header)
        print("-" * len(header))
        for r in all_results:
            o = r["overall"]
            print(
                f"{r['profile']:<12} {o['count']:<8} {o['ops_per_sec']:<10} "
                f"{o['p50_ms']:<10} {o['p99_ms']:<10} {o['errors']:<8}"
            )

    # JSON output
    json_results = []
    for r in all_results:
        json_results.append({
            "name": r["profile"],
            "iterations": r["overall"]["count"],
            "duration_s": r["duration_s"],
            "concurrency": r["concurrency"],
            "read_pct": r["read_pct"],
            "write_pct": r["write_pct"],
            "put_count": r["put"]["count"],
            "put_ops_per_sec": r["put"]["ops_per_sec"],
            "put_p50_ms": r["put"]["p50_ms"],
            "put_p95_ms": r["put"]["p95_ms"],
            "put_p99_ms": r["put"]["p99_ms"],
            "put_errors": r["put"]["errors"],
            "get_count": r["get"]["count"],
            "get_ops_per_sec": r["get"]["ops_per_sec"],
            "get_p50_ms": r["get"]["p50_ms"],
            "get_p95_ms": r["get"]["p95_ms"],
            "get_p99_ms": r["get"]["p99_ms"],
            "get_errors": r["get"]["errors"],
            "overall_ops_per_sec": r["overall"]["ops_per_sec"],
            "overall_p50_ms": r["overall"]["p50_ms"],
            "overall_p95_ms": r["overall"]["p95_ms"],
            "overall_p99_ms": r["overall"]["p99_ms"],
            "overall_errors": r["overall"]["errors"],
            "size_min": r["size_distribution"].get("min", ""),
            "size_max": r["size_distribution"].get("max", ""),
            "size_mean": r["size_distribution"].get("mean", ""),
            "total_bytes": r["size_distribution"].get("total_bytes", 0),
        })

    json_result = build_benchmark_result(
        endpoint=ENDPOINT,
        benchmark="profiles",
        results=json_results,
        implementation=args.implementation,
    )
    write_json_output(args, json_result)

    # Exit with error if any profile had errors
    total_errors = sum(r["overall"]["errors"] for r in all_results)
    if total_errors > 0:
        print(f"\nWarning: {total_errors} total errors across all profiles")
        sys.exit(1)


if __name__ == "__main__":
    main()
