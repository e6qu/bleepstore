"""
BleepStore Throughput Benchmarks

Measures objects/second for various object sizes under different concurrency levels.

Usage:
    python bench_throughput.py [--endpoint URL] [--concurrency N] [--iterations N]
"""

import argparse
import hashlib
import os
import statistics
import sys
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

OBJECT_SIZES = {
    "1KB": 1024,
    "10KB": 10 * 1024,
    "100KB": 100 * 1024,
    "1MB": 1024 * 1024,
    "10MB": 10 * 1024 * 1024,
    "100MB": 100 * 1024 * 1024,
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
            max_pool_connections=100,
        ),
    )


def bench_put(client, bucket, key, data):
    """Benchmark a single PUT operation. Returns elapsed seconds."""
    start = time.monotonic()
    client.put_object(Bucket=bucket, Key=key, Body=data)
    return time.monotonic() - start


def bench_get(client, bucket, key):
    """Benchmark a single GET operation. Returns elapsed seconds."""
    start = time.monotonic()
    resp = client.get_object(Bucket=bucket, Key=key)
    resp["Body"].read()
    return time.monotonic() - start


def bench_delete(client, bucket, key):
    """Benchmark a single DELETE operation. Returns elapsed seconds."""
    start = time.monotonic()
    client.delete_object(Bucket=bucket, Key=key)
    return time.monotonic() - start


def run_throughput_test(operation, size_name, size_bytes, concurrency, iterations):
    """Run a throughput test and return results."""
    bucket = f"bench-{uuid.uuid4().hex[:8]}"
    client = create_client()
    client.create_bucket(Bucket=bucket)

    data = os.urandom(size_bytes)
    latencies = []

    try:
        if operation == "put":
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = []
                for i in range(iterations):
                    key = f"bench-{i:06d}"
                    futures.append(pool.submit(bench_put, client, bucket, key, data))
                for f in as_completed(futures):
                    latencies.append(f.result())

        elif operation == "get":
            # Pre-populate objects
            for i in range(iterations):
                client.put_object(Bucket=bucket, Key=f"bench-{i:06d}", Body=data)

            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = []
                for i in range(iterations):
                    key = f"bench-{i:06d}"
                    futures.append(pool.submit(bench_get, client, bucket, key))
                for f in as_completed(futures):
                    latencies.append(f.result())

        elif operation == "delete":
            # Pre-populate objects
            for i in range(iterations):
                client.put_object(Bucket=bucket, Key=f"bench-{i:06d}", Body=data)

            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = []
                for i in range(iterations):
                    key = f"bench-{i:06d}"
                    futures.append(pool.submit(bench_delete, client, bucket, key))
                for f in as_completed(futures):
                    latencies.append(f.result())

    finally:
        # Cleanup
        try:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
            client.delete_bucket(Bucket=bucket)
        except Exception:
            pass

    total_time = sum(latencies)
    ops_per_sec = len(latencies) / total_time if total_time > 0 else 0
    throughput_mbps = (size_bytes * len(latencies) / total_time) / (1024 * 1024) if total_time > 0 else 0

    return {
        "operation": operation.upper(),
        "size": size_name,
        "concurrency": concurrency,
        "iterations": len(latencies),
        "total_time_s": round(total_time, 3),
        "ops_per_sec": round(ops_per_sec, 1),
        "throughput_mbps": round(throughput_mbps, 2),
        "p50_ms": round(statistics.median(latencies) * 1000, 2),
        "p95_ms": round(sorted(latencies)[int(len(latencies) * 0.95)] * 1000, 2),
        "p99_ms": round(sorted(latencies)[int(len(latencies) * 0.99)] * 1000, 2),
    }


def main():
    parser = argparse.ArgumentParser(description="BleepStore Throughput Benchmark")
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument(
        "--sizes",
        nargs="+",
        default=["1KB", "100KB", "1MB"],
        choices=list(OBJECT_SIZES.keys()),
    )
    parser.add_argument(
        "--operations",
        nargs="+",
        default=["put", "get"],
        choices=["put", "get", "delete"],
    )
    add_json_args(parser)
    args = parser.parse_args()

    global ENDPOINT
    ENDPOINT = args.endpoint

    print(f"BleepStore Throughput Benchmark")
    print(f"Endpoint: {ENDPOINT}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Iterations: {args.iterations}")
    print()
    print(f"{'OP':<8} {'SIZE':<8} {'OPS/s':<10} {'MB/s':<10} {'p50ms':<10} {'p95ms':<10} {'p99ms':<10}")
    print("-" * 66)

    all_results = []
    for op in args.operations:
        for size_name in args.sizes:
            size_bytes = OBJECT_SIZES[size_name]
            result = run_throughput_test(
                op, size_name, size_bytes, args.concurrency, args.iterations
            )
            print(
                f"{result['operation']:<8} {result['size']:<8} "
                f"{result['ops_per_sec']:<10} {result['throughput_mbps']:<10} "
                f"{result['p50_ms']:<10} {result['p95_ms']:<10} {result['p99_ms']:<10}"
            )
            all_results.append({
                "name": f"{result['operation']} {result['size']}",
                "iterations": result["iterations"],
                "concurrency": result["concurrency"],
                "ops_per_sec": result["ops_per_sec"],
                "throughput_mbps": result["throughput_mbps"],
                "p50_ms": result["p50_ms"],
                "p95_ms": result["p95_ms"],
                "p99_ms": result["p99_ms"],
                "total_time_s": result["total_time_s"],
            })

    # JSON output
    json_result = build_benchmark_result(
        endpoint=ENDPOINT,
        benchmark="throughput",
        results=all_results,
        implementation=args.implementation,
    )
    write_json_output(args, json_result)


if __name__ == "__main__":
    main()
