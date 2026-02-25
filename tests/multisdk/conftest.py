"""Multi-SDK test fixtures.

Tests run against any BleepStore implementation. Configure via environment:

    BLEEPSTORE_ENDPOINT=http://localhost:9013
    BLEEPSTORE_ACCESS_KEY=bleepstore
    BLEEPSTORE_SECRET_KEY=bleepstore-secret
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from botocore.config import Config

from clients import discover_clients

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


def pytest_configure(config):
    config.addinivalue_line("markers", "multisdk: Multi-SDK S3 tests")
    config.addinivalue_line("markers", "bucket_ops: Bucket operations")
    config.addinivalue_line("markers", "object_ops: Object operations")
    config.addinivalue_line("markers", "listing_ops: Listing operations")
    config.addinivalue_line("markers", "bulk_ops: Bulk operations")
    config.addinivalue_line("markers", "large_file: Large file / multipart operations")
    config.addinivalue_line("markers", "data_integrity: Data integrity tests")
    config.addinivalue_line("markers", "cross_client: Cross-client interop tests")


# Discover clients once at import time (session scope)
_CLIENTS = discover_clients()


@pytest.fixture(params=_CLIENTS, ids=lambda c: c.name, scope="session")
def s3(request):
    """Yield each available S3Client implementation."""
    return request.param


@pytest.fixture(scope="session")
def s3_client_boto3():
    """Raw boto3 client for bucket setup/teardown (avoids circular dependency)."""
    return boto3.client(
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


@pytest.fixture()
def bucket(s3_client_boto3):
    """Create bucket via boto3, yield name, cleanup via boto3."""
    name = f"msdk-{uuid.uuid4().hex[:8]}"
    s3_client_boto3.create_bucket(Bucket=name)
    yield name
    _empty_and_delete_bucket(s3_client_boto3, name)


@pytest.fixture()
def bucket_name():
    """Generate a unique bucket name (no creation)."""
    return f"msdk-{uuid.uuid4().hex[:8]}"


def _empty_and_delete_bucket(client, bucket_name):
    """Delete all objects in a bucket, then delete the bucket."""
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            objects = page.get("Contents", [])
            if objects:
                client.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
                )
        client.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass  # Best-effort cleanup
