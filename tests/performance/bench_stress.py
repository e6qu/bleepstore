"""
BleepStore Stress Test Scenarios

Exercises failure modes and edge cases beyond simple throughput benchmarks:
connection storms, large objects, rapid create/delete, key explosion, multipart stress.

Usage:
    python bench_stress.py [--endpoint URL] [--scenario NAME] [--iterations N]

Scenarios:
    connection_storm    100 concurrent connections doing rapid PUT/GET
    large_objects       Upload/download 50MB, 100MB, 250MB objects
    bucket_churn        Rapidly create and delete 50 buckets
    key_explosion       10,000 small objects in one bucket, then list/delete
    multipart_stress    100MB multipart upload with 5MB parts, 8 uploaders
    all                 Run all scenarios (default)
"""

import argparse
import os
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


def report(name, passed, elapsed, detail=""):
    status = "PASS" if passed else "FAIL"
    msg = f"  [{status}] {name:<30} {elapsed:>8.2f}s"
    if detail:
        msg += f"  ({detail})"
    print(msg)
    return passed


# ---------------------------------------------------------------------------
# Scenario 1: Connection Storm
# ---------------------------------------------------------------------------
def scenario_connection_storm(iterations=1):
    """100 concurrent connections doing rapid PUT/GET."""
    client = create_client()
    bucket = f"stress-storm-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)
    body = os.urandom(1024)  # 1KB

    concurrency = 100
    ops_per_worker = 10 * iterations

    def worker(worker_id):
        c = create_client()
        for i in range(ops_per_worker):
            key = f"storm-{worker_id}-{i}"
            c.put_object(Bucket=bucket, Key=key, Body=body)
            resp = c.get_object(Bucket=bucket, Key=key)
            resp["Body"].read()

    start = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(worker, i) for i in range(concurrency)]
            errors = 0
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception:
                    errors += 1
        elapsed = time.monotonic() - start
        total_ops = concurrency * ops_per_worker * 2  # PUT + GET
        detail = f"{total_ops} ops, {concurrency} concurrent, {errors} errors"
        return report("Connection storm", errors == 0, elapsed, detail)
    finally:
        cleanup_bucket(client, bucket)


# ---------------------------------------------------------------------------
# Scenario 2: Large Objects
# ---------------------------------------------------------------------------
def scenario_large_objects(iterations=1):
    """Upload and download 50MB, 100MB, 250MB single objects."""
    client = create_client()
    bucket = f"stress-large-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)

    sizes_mb = [50, 100, 250]
    all_passed = True

    try:
        for size_mb in sizes_mb:
            for _ in range(iterations):
                key = f"large-{size_mb}mb-{uuid.uuid4().hex[:8]}"
                body = os.urandom(size_mb * 1024 * 1024)

                t0 = time.monotonic()
                client.put_object(Bucket=bucket, Key=key, Body=body)
                put_time = time.monotonic() - t0

                t0 = time.monotonic()
                resp = client.get_object(Bucket=bucket, Key=key)
                data = resp["Body"].read()
                get_time = time.monotonic() - t0

                passed = len(data) == len(body)
                if not passed:
                    all_passed = False
                detail = f"PUT {put_time:.1f}s, GET {get_time:.1f}s"
                report(f"Large object {size_mb}MB", passed, put_time + get_time, detail)

        return all_passed
    finally:
        cleanup_bucket(client, bucket)


# ---------------------------------------------------------------------------
# Scenario 3: Bucket Churn
# ---------------------------------------------------------------------------
def scenario_bucket_churn(iterations=1):
    """Rapidly create and delete 50 buckets."""
    client = create_client()
    prefix = f"stress-churn-{uuid.uuid4().hex[:6]}"
    count = 50 * iterations
    buckets = []

    start = time.monotonic()
    try:
        # Create
        for i in range(count):
            name = f"{prefix}-{i:04d}"
            client.create_bucket(Bucket=name)
            buckets.append(name)

        # Verify they exist
        response = client.list_buckets()
        existing = {b["Name"] for b in response["Buckets"]}
        created_ok = all(b in existing for b in buckets)

        # Delete
        for b in buckets:
            client.delete_bucket(Bucket=b)

        elapsed = time.monotonic() - start
        detail = f"{count} buckets created+deleted"
        return report("Bucket churn", created_ok, elapsed, detail)
    finally:
        for b in buckets:
            try:
                client.delete_bucket(Bucket=b)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Scenario 4: Key Explosion
# ---------------------------------------------------------------------------
def scenario_key_explosion(iterations=1):
    """10,000 small objects in one bucket, then list and delete all."""
    client = create_client()
    bucket = f"stress-keys-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)

    num_keys = 10000 * iterations
    body = b"x"

    start = time.monotonic()
    try:
        # Bulk PUT with concurrency
        def put_batch(start_idx, count):
            c = create_client()
            for i in range(start_idx, start_idx + count):
                c.put_object(Bucket=bucket, Key=f"key-{i:06d}", Body=body)

        batch_size = 500
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = []
            for batch_start in range(0, num_keys, batch_size):
                actual = min(batch_size, num_keys - batch_start)
                futures.append(pool.submit(put_batch, batch_start, actual))
            for f in as_completed(futures):
                f.result()

        # List all
        listed = 0
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            listed += len(page.get("Contents", []))

        list_ok = listed == num_keys

        # Delete all
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                client.delete_object(Bucket=bucket, Key=obj["Key"])

        client.delete_bucket(Bucket=bucket)

        elapsed = time.monotonic() - start
        detail = f"{num_keys} keys, listed {listed}"
        return report("Key explosion", list_ok, elapsed, detail)
    except Exception as e:
        elapsed = time.monotonic() - start
        report("Key explosion", False, elapsed, str(e))
        cleanup_bucket(client, bucket)
        return False


# ---------------------------------------------------------------------------
# Scenario 5: Multipart Stress
# ---------------------------------------------------------------------------
def scenario_multipart_stress(iterations=1):
    """100MB multipart upload with 5MB parts, 8 concurrent uploaders."""
    client = create_client()
    bucket = f"stress-mp-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)

    part_size = 5 * 1024 * 1024   # 5MB
    total_size = 100 * 1024 * 1024  # 100MB
    num_parts = total_size // part_size
    uploaders = 8 * iterations

    def do_multipart(uploader_id):
        c = create_client()
        key = f"multipart-{uploader_id}"
        upload = c.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = upload["UploadId"]

        parts = []
        for part_num in range(1, num_parts + 1):
            data = os.urandom(part_size)
            resp = c.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=data,
            )
            parts.append({"ETag": resp["ETag"], "PartNumber": part_num})

        c.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        # Verify size
        head = c.head_object(Bucket=bucket, Key=key)
        return head["ContentLength"] == total_size

    start = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=uploaders) as pool:
            futures = [pool.submit(do_multipart, i) for i in range(uploaders)]
            results = []
            for f in as_completed(futures):
                try:
                    results.append(f.result())
                except Exception:
                    results.append(False)

        elapsed = time.monotonic() - start
        all_ok = all(results)
        detail = f"{uploaders} uploaders, {num_parts}x{part_size // (1024*1024)}MB parts each"
        return report("Multipart stress", all_ok, elapsed, detail)
    finally:
        cleanup_bucket(client, bucket)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
SCENARIOS = {
    "connection_storm": scenario_connection_storm,
    "large_objects": scenario_large_objects,
    "bucket_churn": scenario_bucket_churn,
    "key_explosion": scenario_key_explosion,
    "multipart_stress": scenario_multipart_stress,
}


def main():
    global ENDPOINT

    parser = argparse.ArgumentParser(description="BleepStore Stress Tests")
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()) + ["all"],
        default="all",
    )
    parser.add_argument("--iterations", type=int, default=1)
    add_json_args(parser)
    args = parser.parse_args()

    ENDPOINT = args.endpoint

    print("BleepStore Stress Tests")
    print(f"  Endpoint:   {ENDPOINT}")
    print(f"  Scenario:   {args.scenario}")
    print(f"  Iterations: {args.iterations}")
    print("==============================================")
    print()

    if args.scenario == "all":
        to_run = list(SCENARIOS.items())
    else:
        to_run = [(args.scenario, SCENARIOS[args.scenario])]

    results = []
    for name, func in to_run:
        print(f"--- {name} ---")
        scenario_start = time.monotonic()
        try:
            passed = func(iterations=args.iterations)
        except Exception as e:
            print(f"  [FAIL] {name:<30} (exception: {e})")
            passed = False
        elapsed_s = time.monotonic() - scenario_start
        results.append((name, passed, elapsed_s))
        print()

    # Summary
    print("==============================================")
    passed_count = sum(1 for _, p, _ in results if p)
    total = len(results)
    print(f"Results: {passed_count}/{total} scenarios passed")

    if passed_count < total:
        failed = [name for name, p, _ in results if not p]
        print(f"Failed:  {', '.join(failed)}")

    # JSON output
    json_results = []
    for name, passed, elapsed_s in results:
        json_results.append({
            "name": name,
            "iterations": args.iterations,
            "passed": passed,
            "elapsed_s": round(elapsed_s, 3),
        })
    json_result = build_benchmark_result(
        endpoint=ENDPOINT,
        benchmark="stress",
        results=json_results,
        implementation=args.implementation,
    )
    write_json_output(args, json_result)

    if passed_count < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
