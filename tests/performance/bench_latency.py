"""
BleepStore Latency Benchmarks

Measures p50, p95, p99 latency for individual operations in serial (no concurrency).

Usage:
    python bench_latency.py [--endpoint URL] [--iterations N]
"""

import argparse
import os
import statistics
import time
import uuid

import boto3
from botocore.config import Config

from output_utils import add_json_args, build_benchmark_result, write_json_output

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


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
        ),
    )


def percentile(data, pct):
    """Calculate the pct-th percentile of sorted data."""
    idx = int(len(data) * pct / 100)
    idx = min(idx, len(data) - 1)
    return sorted(data)[idx]


def bench_operation(name, func, iterations):
    """Run a function `iterations` times and report latency stats."""
    latencies = []
    for _ in range(iterations):
        start = time.monotonic()
        func()
        latencies.append((time.monotonic() - start) * 1000)  # ms

    return {
        "name": name,
        "iterations": iterations,
        "min_ms": round(min(latencies), 2),
        "p50_ms": round(percentile(latencies, 50), 2),
        "p95_ms": round(percentile(latencies, 95), 2),
        "p99_ms": round(percentile(latencies, 99), 2),
        "max_ms": round(max(latencies), 2),
        "mean_ms": round(statistics.mean(latencies), 2),
        "stdev_ms": round(statistics.stdev(latencies), 2) if len(latencies) > 1 else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="BleepStore Latency Benchmark")
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument("--iterations", type=int, default=100)
    add_json_args(parser)
    args = parser.parse_args()

    global ENDPOINT
    ENDPOINT = args.endpoint

    client = create_client()
    bucket = f"bench-lat-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)

    small_body = os.urandom(1024)        # 1KB
    medium_body = os.urandom(1024 * 100)  # 100KB

    # Pre-populate test objects
    client.put_object(Bucket=bucket, Key="latency-small.bin", Body=small_body)
    client.put_object(Bucket=bucket, Key="latency-medium.bin", Body=medium_body)
    for i in range(20):
        client.put_object(Bucket=bucket, Key=f"list-{i:04d}.txt", Body=b"x")

    try:
        results = []

        # PUT 1KB
        counter = [0]
        def put_small():
            counter[0] += 1
            client.put_object(Bucket=bucket, Key=f"put-{counter[0]}", Body=small_body)
        results.append(bench_operation("PUT 1KB", put_small, args.iterations))

        # PUT 100KB
        counter2 = [0]
        def put_medium():
            counter2[0] += 1
            client.put_object(Bucket=bucket, Key=f"putm-{counter2[0]}", Body=medium_body)
        results.append(bench_operation("PUT 100KB", put_medium, args.iterations))

        # GET 1KB
        def get_small():
            resp = client.get_object(Bucket=bucket, Key="latency-small.bin")
            resp["Body"].read()
        results.append(bench_operation("GET 1KB", get_small, args.iterations))

        # GET 100KB
        def get_medium():
            resp = client.get_object(Bucket=bucket, Key="latency-medium.bin")
            resp["Body"].read()
        results.append(bench_operation("GET 100KB", get_medium, args.iterations))

        # HEAD
        def head_obj():
            client.head_object(Bucket=bucket, Key="latency-small.bin")
        results.append(bench_operation("HEAD", head_obj, args.iterations))

        # DELETE (recreate each time)
        del_counter = [0]
        for i in range(args.iterations):
            client.put_object(Bucket=bucket, Key=f"del-{i}", Body=b"x")
        def delete_obj():
            del_counter[0] += 1
            client.delete_object(Bucket=bucket, Key=f"del-{del_counter[0] - 1}")
        results.append(bench_operation("DELETE", delete_obj, args.iterations))

        # LIST (20 objects)
        def list_objs():
            client.list_objects_v2(Bucket=bucket, MaxKeys=20)
        results.append(bench_operation("LIST (20)", list_objs, args.iterations))

        # HEAD BUCKET
        def head_bucket():
            client.head_bucket(Bucket=bucket)
        results.append(bench_operation("HEAD BUCKET", head_bucket, args.iterations))

        # Print results
        print(f"BleepStore Latency Benchmark")
        print(f"Endpoint: {ENDPOINT}")
        print(f"Iterations: {args.iterations}")
        print()
        print(f"{'Operation':<16} {'min':<10} {'p50':<10} {'p95':<10} {'p99':<10} {'max':<10} {'mean':<10}")
        print("-" * 76)
        for r in results:
            print(
                f"{r['name']:<16} {r['min_ms']:<10} {r['p50_ms']:<10} "
                f"{r['p95_ms']:<10} {r['p99_ms']:<10} {r['max_ms']:<10} {r['mean_ms']:<10}"
            )

        # JSON output
        json_result = build_benchmark_result(
            endpoint=ENDPOINT,
            benchmark="latency",
            results=results,
            implementation=args.implementation,
        )
        write_json_output(args, json_result)

    finally:
        # Cleanup
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


if __name__ == "__main__":
    main()
