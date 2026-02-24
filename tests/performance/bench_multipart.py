"""
BleepStore Multipart Upload Benchmark

Measures upload performance for large files via multipart upload.

Usage:
    python bench_multipart.py [--endpoint URL] [--size-mb N] [--part-size-mb N]
"""

import argparse
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

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
            max_pool_connections=50,
        ),
    )


def upload_part(client, bucket, key, upload_id, part_number, data):
    """Upload a single part and return (part_number, etag, elapsed_ms)."""
    start = time.monotonic()
    resp = client.upload_part(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        PartNumber=part_number,
        Body=data,
    )
    elapsed = (time.monotonic() - start) * 1000
    return part_number, resp["ETag"], elapsed


def main():
    parser = argparse.ArgumentParser(description="BleepStore Multipart Upload Benchmark")
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument("--size-mb", type=int, default=100, help="Total file size in MB")
    parser.add_argument("--part-size-mb", type=int, default=10, help="Part size in MB")
    parser.add_argument("--concurrency", type=int, default=4, help="Concurrent part uploads")
    parser.add_argument("--iterations", type=int, default=3, help="Number of uploads")
    args = parser.parse_args()

    global ENDPOINT
    ENDPOINT = args.endpoint

    client = create_client()
    bucket = f"bench-mp-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket)

    total_bytes = args.size_mb * 1024 * 1024
    part_bytes = args.part_size_mb * 1024 * 1024
    num_parts = (total_bytes + part_bytes - 1) // part_bytes

    print(f"BleepStore Multipart Upload Benchmark")
    print(f"Endpoint: {ENDPOINT}")
    print(f"File size: {args.size_mb} MB")
    print(f"Part size: {args.part_size_mb} MB")
    print(f"Parts: {num_parts}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Iterations: {args.iterations}")
    print()

    # Generate part data once
    part_data = os.urandom(part_bytes)
    last_part_size = total_bytes - (num_parts - 1) * part_bytes
    last_part_data = os.urandom(last_part_size) if last_part_size != part_bytes else part_data

    upload_times = []

    try:
        for iteration in range(args.iterations):
            key = f"multipart-bench-{iteration}.bin"
            total_start = time.monotonic()

            # Create multipart upload
            create_resp = client.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = create_resp["UploadId"]

            try:
                # Upload parts concurrently
                parts = []
                part_latencies = []

                with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                    futures = {}
                    for i in range(1, num_parts + 1):
                        data = last_part_data if i == num_parts else part_data
                        f = pool.submit(
                            upload_part, client, bucket, key, upload_id, i, data
                        )
                        futures[f] = i

                    for f in as_completed(futures):
                        part_num, etag, elapsed = f.result()
                        parts.append({"PartNumber": part_num, "ETag": etag})
                        part_latencies.append(elapsed)

                # Sort parts by number
                parts.sort(key=lambda p: p["PartNumber"])

                # Complete
                complete_start = time.monotonic()
                client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
                complete_ms = (time.monotonic() - complete_start) * 1000

                total_ms = (time.monotonic() - total_start) * 1000
                throughput_mbps = (total_bytes / (total_ms / 1000)) / (1024 * 1024)
                upload_times.append(total_ms)

                avg_part_ms = sum(part_latencies) / len(part_latencies)
                print(
                    f"  Iteration {iteration + 1}: "
                    f"total={total_ms:.0f}ms, "
                    f"throughput={throughput_mbps:.1f} MB/s, "
                    f"avg_part={avg_part_ms:.0f}ms, "
                    f"complete={complete_ms:.0f}ms"
                )

            except Exception:
                client.abort_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=upload_id
                )
                raise

        # Summary
        print()
        avg_total = sum(upload_times) / len(upload_times)
        avg_throughput = (total_bytes / (avg_total / 1000)) / (1024 * 1024)
        print(f"Average: {avg_total:.0f}ms ({avg_throughput:.1f} MB/s)")

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


if __name__ == "__main__":
    main()
