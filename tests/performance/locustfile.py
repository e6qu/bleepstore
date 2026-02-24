"""
BleepStore Locust Load Test

Runs S3-compatible load tests using Locust with full SigV4 authentication via boto3.

Prerequisites:
    pip install locust boto3  (or: uv pip install locust boto3)

Usage:
    # Web UI mode (default: http://localhost:8089)
    locust -f locustfile.py

    # Headless mode
    locust -f locustfile.py --headless -u 50 -r 10 --run-time 60s

    # Custom endpoint
    BLEEPSTORE_ENDPOINT=http://localhost:9000 locust -f locustfile.py --headless -u 20 -r 5 -t 30s

Environment variables:
    BLEEPSTORE_ENDPOINT   - BleepStore endpoint (default: http://localhost:9000)
    BLEEPSTORE_ACCESS_KEY - Access key (default: bleepstore)
    BLEEPSTORE_SECRET_KEY - Secret key (default: bleepstore-secret)
    BLEEPSTORE_REGION     - Region (default: us-east-1)
"""

import hashlib
import os
import random
import string
import time
import uuid

import boto3
from botocore.config import Config
from locust import HttpUser, between, events, task

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")

BUCKET = "locust-load-test"

# Object size distributions (weighted)
OBJECT_SIZES = {
    "1KB": (1024, 0.4),
    "10KB": (10 * 1024, 0.3),
    "100KB": (100 * 1024, 0.2),
    "1MB": (1024 * 1024, 0.1),
}


def _random_data(size: int) -> bytes:
    return os.urandom(size)


def _pick_size() -> tuple[str, int]:
    r = random.random()
    cumulative = 0.0
    for label, (size, weight) in OBJECT_SIZES.items():
        cumulative += weight
        if r < cumulative:
            return label, size
    return "1KB", 1024


class S3User(HttpUser):
    """Simulates an S3 client performing mixed operations against BleepStore."""

    wait_time = between(0.01, 0.1)

    def on_start(self):
        """Create boto3 client and ensure test bucket exists."""
        self.s3 = boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name=REGION,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 1, "mode": "standard"},
            ),
        )

        # Ensure bucket exists
        try:
            self.s3.create_bucket(Bucket=BUCKET)
        except self.s3.exceptions.BucketAlreadyOwnedByYou:
            pass
        except Exception:
            pass  # Bucket may already exist

        # Pre-populate seed objects
        self._seed_keys = []
        for i in range(50):
            key = f"seed/{i:04d}.bin"
            self.s3.put_object(Bucket=BUCKET, Key=key, Body=_random_data(1024))
            self._seed_keys.append(key)

        self._written_keys = []

    @task(20)
    def put_object(self):
        """PUT a new object (20% of traffic)."""
        label, size = _pick_size()
        key = f"load/{uuid.uuid4().hex[:12]}-{label}"
        data = _random_data(size)

        start = time.perf_counter()
        try:
            self.s3.put_object(Bucket=BUCKET, Key=key, Body=data)
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name=f"PUT {label}",
                response_time=elapsed_ms,
                response_length=size,
                exception=None,
                context={},
            )
            self._written_keys.append(key)
            # Cap stored keys to avoid memory issues
            if len(self._written_keys) > 500:
                self._written_keys = self._written_keys[-250:]
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name=f"PUT {label}",
                response_time=elapsed_ms,
                response_length=0,
                exception=e,
                context={},
            )

    @task(60)
    def get_object(self):
        """GET an existing object (60% of traffic)."""
        keys = self._seed_keys + self._written_keys[-50:]
        if not keys:
            return
        key = random.choice(keys)

        start = time.perf_counter()
        try:
            resp = self.s3.get_object(Bucket=BUCKET, Key=key)
            body = resp["Body"].read()
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name="GET",
                response_time=elapsed_ms,
                response_length=len(body),
                exception=None,
                context={},
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name="GET",
                response_time=elapsed_ms,
                response_length=0,
                exception=e,
                context={},
            )

    @task(10)
    def list_objects(self):
        """LIST objects in the bucket (10% of traffic)."""
        start = time.perf_counter()
        try:
            resp = self.s3.list_objects_v2(Bucket=BUCKET, MaxKeys=100)
            count = resp.get("KeyCount", 0)
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name="LIST",
                response_time=elapsed_ms,
                response_length=count,
                exception=None,
                context={},
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name="LIST",
                response_time=elapsed_ms,
                response_length=0,
                exception=e,
                context={},
            )

    @task(5)
    def head_object(self):
        """HEAD an existing object (5% of traffic)."""
        keys = self._seed_keys + self._written_keys[-50:]
        if not keys:
            return
        key = random.choice(keys)

        start = time.perf_counter()
        try:
            self.s3.head_object(Bucket=BUCKET, Key=key)
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name="HEAD",
                response_time=elapsed_ms,
                response_length=0,
                exception=None,
                context={},
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name="HEAD",
                response_time=elapsed_ms,
                response_length=0,
                exception=e,
                context={},
            )

    @task(5)
    def delete_object(self):
        """DELETE a previously written object (5% of traffic)."""
        if not self._written_keys:
            return
        key = self._written_keys.pop(0)

        start = time.perf_counter()
        try:
            self.s3.delete_object(Bucket=BUCKET, Key=key)
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name="DELETE",
                response_time=elapsed_ms,
                response_length=0,
                exception=None,
                context={},
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            events.request.fire(
                request_type="S3",
                name="DELETE",
                response_time=elapsed_ms,
                response_length=0,
                exception=e,
                context={},
            )
