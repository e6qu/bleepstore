#!/usr/bin/env python3
"""
BleepStore Benchmark Suite

Runs latency, throughput, and memory benchmarks against one or two S3-compatible
endpoints and prints a side-by-side comparison table.

Usage:
    python bench.py --endpoint http://localhost:9010 --label BleepStore
    python bench.py --endpoint http://localhost:9010 --label BleepStore \
                    --baseline http://localhost:9099 --baseline-label MinIO
    python bench.py --endpoint http://localhost:9010 --label BleepStore --json
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
import uuid

import boto3
from botocore.config import Config

ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


def create_client(endpoint):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
        ),
    )


def percentile(data, pct):
    idx = min(int(len(data) * pct / 100), len(data) - 1)
    return sorted(data)[idx]


def bench_latency(client, bucket, name, fn, n=100, warmup=5):
    for _ in range(warmup):
        fn()
    lats = []
    for _ in range(n):
        t0 = time.monotonic()
        fn()
        lats.append((time.monotonic() - t0) * 1000)
    lats.sort()
    return {
        "name": name,
        "n": n,
        "min": round(min(lats), 2),
        "p50": round(percentile(lats, 50), 2),
        "p95": round(percentile(lats, 95), 2),
        "p99": round(percentile(lats, 99), 2),
        "max": round(max(lats), 2),
        "mean": round(statistics.mean(lats), 2),
    }


def bench_throughput(client, bucket, body, label, reps):
    """Returns (put_mbps, get_mbps)."""
    sz = len(body)

    # PUT
    t0 = time.monotonic()
    for i in range(reps):
        client.put_object(Bucket=bucket, Key=f"tp-{label}-{i}", Body=body)
    elapsed = time.monotonic() - t0
    put_mbps = round((sz * reps / elapsed) / (1024 * 1024), 1)

    # GET
    t0 = time.monotonic()
    for i in range(reps):
        r = client.get_object(Bucket=bucket, Key=f"tp-{label}-{i}")
        r["Body"].read()
    elapsed = time.monotonic() - t0
    get_mbps = round((sz * reps / elapsed) / (1024 * 1024), 1)

    return put_mbps, get_mbps


def get_rss_kb(pid):
    """Get RSS in KB for a given PID."""
    try:
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)]).decode().strip()
        return int(out)
    except Exception:
        return 0


def run_suite(endpoint, label, n_latency=100, pid=None):
    """Run the full benchmark suite against an endpoint. Returns a results dict."""
    client = create_client(endpoint)
    bucket = f"bench-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)

    small = os.urandom(1024)  # 1KB
    medium = os.urandom(100 * 1024)  # 100KB
    large = os.urandom(1024 * 1024)  # 1MB
    xlarge = os.urandom(10 * 1024 * 1024)  # 10MB

    # Pre-populate
    client.put_object(Bucket=bucket, Key="small.bin", Body=small)
    client.put_object(Bucket=bucket, Key="medium.bin", Body=medium)
    client.put_object(Bucket=bucket, Key="large.bin", Body=large)
    client.put_object(Bucket=bucket, Key="xlarge.bin", Body=xlarge)
    for i in range(20):
        client.put_object(Bucket=bucket, Key=f"list-{i:04d}", Body=b"x")

    # --- Memory before ---
    rss_before = get_rss_kb(pid) if pid else 0

    # --- Latency ---
    latency = []

    c = [0]
    def put_1k():
        c[0] += 1
        client.put_object(Bucket=bucket, Key=f"p1k-{c[0]}", Body=small)
    latency.append(bench_latency(client, bucket, "PUT 1KB", put_1k, n_latency))

    c2 = [0]
    def put_100k():
        c2[0] += 1
        client.put_object(Bucket=bucket, Key=f"p100k-{c2[0]}", Body=medium)
    latency.append(bench_latency(client, bucket, "PUT 100KB", put_100k, n_latency))

    c3 = [0]
    def put_1m():
        c3[0] += 1
        client.put_object(Bucket=bucket, Key=f"p1m-{c3[0]}", Body=large)
    latency.append(bench_latency(client, bucket, "PUT 1MB", put_1m, n_latency))

    def get_1k():
        r = client.get_object(Bucket=bucket, Key="small.bin")
        r["Body"].read()
    latency.append(bench_latency(client, bucket, "GET 1KB", get_1k, n_latency))

    def get_100k():
        r = client.get_object(Bucket=bucket, Key="medium.bin")
        r["Body"].read()
    latency.append(bench_latency(client, bucket, "GET 100KB", get_100k, n_latency))

    def get_1m():
        r = client.get_object(Bucket=bucket, Key="large.bin")
        r["Body"].read()
    latency.append(bench_latency(client, bucket, "GET 1MB", get_1m, n_latency))

    def head_obj():
        client.head_object(Bucket=bucket, Key="small.bin")
    latency.append(bench_latency(client, bucket, "HEAD", head_obj, n_latency))

    def list_20():
        client.list_objects_v2(Bucket=bucket, MaxKeys=20)
    latency.append(bench_latency(client, bucket, "LIST (20)", list_20, n_latency))

    def head_bkt():
        client.head_bucket(Bucket=bucket)
    latency.append(bench_latency(client, bucket, "HEAD BUCKET", head_bkt, n_latency))

    # --- Throughput ---
    throughput = []
    for body, sz_label, reps in [
        (small, "1KB", 20),
        (medium, "100KB", 20),
        (large, "1MB", 20),
        (xlarge, "10MB", 5),
    ]:
        put_mbps, get_mbps = bench_throughput(client, bucket, body, sz_label, reps)
        throughput.append({"size": sz_label, "put_mbps": put_mbps, "get_mbps": get_mbps})

    # --- Memory: 100MB upload ---
    rss_before_big = get_rss_kb(pid) if pid else 0
    big = os.urandom(100 * 1024 * 1024)
    client.put_object(Bucket=bucket, Key="big100mb.bin", Body=big)
    r = client.get_object(Bucket=bucket, Key="big100mb.bin")
    r["Body"].read()
    del big
    time.sleep(0.5)
    rss_after_big = get_rss_kb(pid) if pid else 0

    # --- Cleanup ---
    try:
        pag = client.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                client.delete_object(Bucket=bucket, Key=obj["Key"])
        client.delete_bucket(Bucket=bucket)
    except Exception:
        pass

    return {
        "label": label,
        "endpoint": endpoint,
        "latency": latency,
        "throughput": throughput,
        "memory": {
            "idle_rss_kb": rss_before,
            "after_100mb_rss_kb": rss_after_big,
            "rss_delta_kb": max(0, rss_after_big - rss_before_big),
        },
    }


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def print_single(results):
    """Print results for a single endpoint."""
    label = results["label"]
    print(f"\n{'=' * 70}")
    print(f"  {label} Benchmark Results")
    print(f"  Endpoint: {results['endpoint']}")
    print(f"{'=' * 70}\n")

    print("LATENCY (ms)")
    print(f"{'Operation':<16} {'p50':>8} {'p95':>8} {'p99':>8} {'mean':>8}")
    print("-" * 48)
    for r in results["latency"]:
        print(f"{r['name']:<16} {r['p50']:>8.2f} {r['p95']:>8.2f} {r['p99']:>8.2f} {r['mean']:>8.2f}")

    print(f"\nTHROUGHPUT (MB/s)")
    print(f"{'Size':<10} {'PUT':>12} {'GET':>12}")
    print("-" * 34)
    for t in results["throughput"]:
        print(f"{t['size']:<10} {t['put_mbps']:>10.1f}   {t['get_mbps']:>10.1f}")

    mem = results["memory"]
    if mem["idle_rss_kb"] > 0:
        print(f"\nMEMORY")
        print(f"  Idle RSS:            {mem['idle_rss_kb'] // 1024} MB")
        print(f"  After 100MB upload:  {mem['after_100mb_rss_kb'] // 1024} MB")
        print(f"  Delta:               {mem['rss_delta_kb'] // 1024} MB")


def print_comparison(target, baseline):
    """Print side-by-side comparison of two benchmark runs."""
    tl = target["label"]
    bl = baseline["label"]

    print(f"\n{'=' * 90}")
    print(f"  {tl} vs {bl} — Side-by-Side Comparison")
    print(f"{'=' * 90}\n")

    # Latency p50
    print("LATENCY p50 (ms) — lower is better")
    print(f"{'Operation':<16} {tl:>12} {bl:>12} {'Ratio':>10}  {'':>10}")
    print("-" * 70)
    for t, b in zip(target["latency"], baseline["latency"]):
        ratio = t["p50"] / b["p50"] if b["p50"] > 0 else 0
        tag = "OK" if ratio <= 2.0 else "> 2x"
        # Show which is faster
        if ratio < 1.0:
            tag = "FASTER"
        print(f"{t['name']:<16} {t['p50']:>10.2f}ms {b['p50']:>10.2f}ms {ratio:>9.2f}x  {tag}")

    # Latency p95
    print(f"\nLATENCY p95 (ms)")
    print(f"{'Operation':<16} {tl:>12} {bl:>12} {'Ratio':>10}")
    print("-" * 56)
    for t, b in zip(target["latency"], baseline["latency"]):
        ratio = t["p95"] / b["p95"] if b["p95"] > 0 else 0
        print(f"{t['name']:<16} {t['p95']:>10.2f}ms {b['p95']:>10.2f}ms {ratio:>9.2f}x")

    # Throughput
    print(f"\nTHROUGHPUT (MB/s) — higher is better")
    print(f"{'Size':<8} {'Op':<5} {tl:>14} {bl:>14} {'Ratio':>10}  {'':>8}")
    print("-" * 70)
    for t, b in zip(target["throughput"], baseline["throughput"]):
        pr = t["put_mbps"] / b["put_mbps"] if b["put_mbps"] > 0 else 0
        gr = t["get_mbps"] / b["get_mbps"] if b["get_mbps"] > 0 else 0
        pt = "OK" if pr >= 0.5 else "< 0.5x"
        gt = "OK" if gr >= 0.5 else "< 0.5x"
        if pr >= 1.0:
            pt = "FASTER"
        if gr >= 1.0:
            gt = "FASTER"
        print(f"{t['size']:<8} PUT  {t['put_mbps']:>11.1f}    {b['put_mbps']:>11.1f}    {pr:>9.2f}x  {pt}")
        print(f"{'':8} GET  {t['get_mbps']:>11.1f}    {b['get_mbps']:>11.1f}    {gr:>9.2f}x  {gt}")

    # Memory
    tm = target["memory"]
    bm = baseline["memory"]
    if tm["idle_rss_kb"] > 0 and bm["idle_rss_kb"] > 0:
        print(f"\nMEMORY")
        print(f"{'Metric':<24} {tl:>14} {bl:>14}")
        print("-" * 56)
        print(f"{'Idle RSS':<24} {tm['idle_rss_kb'] // 1024:>12} MB {bm['idle_rss_kb'] // 1024:>12} MB")
        print(f"{'After 100MB upload':<24} {tm['after_100mb_rss_kb'] // 1024:>12} MB {bm['after_100mb_rss_kb'] // 1024:>12} MB")
        print(f"{'RSS delta (100MB op)':<24} {tm['rss_delta_kb'] // 1024:>12} MB {bm['rss_delta_kb'] // 1024:>12} MB")


def main():
    parser = argparse.ArgumentParser(description="BleepStore Benchmark Suite")
    parser.add_argument("--endpoint", required=True, help="Target S3 endpoint URL")
    parser.add_argument("--label", default="Target", help="Label for the target")
    parser.add_argument("--pid", type=int, default=None, help="PID of target server (for RSS measurement)")
    parser.add_argument("--baseline", default=None, help="Baseline S3 endpoint URL (e.g. MinIO)")
    parser.add_argument("--baseline-label", default="MinIO", help="Label for the baseline")
    parser.add_argument("--baseline-pid", type=int, default=None, help="PID of baseline server")
    parser.add_argument("-n", type=int, default=100, help="Iterations per latency test (default: 100)")
    parser.add_argument("--json", action="store_true", help="Output raw JSON instead of tables")

    args = parser.parse_args()

    print(f"Running {args.label} benchmarks (N={args.n})...")
    target = run_suite(args.endpoint, args.label, n_latency=args.n, pid=args.pid)

    baseline = None
    if args.baseline:
        print(f"Running {args.baseline_label} benchmarks (N={args.n})...")
        baseline = run_suite(args.baseline, args.baseline_label, n_latency=args.n, pid=args.baseline_pid)

    if args.json:
        out = {"target": target}
        if baseline:
            out["baseline"] = baseline
        print(json.dumps(out, indent=2))
        return

    if baseline:
        print_comparison(target, baseline)
    else:
        print_single(target)


if __name__ == "__main__":
    main()
