"""
BleepStore E2E Test Configuration

Tests run against any BleepStore implementation via its S3-compatible HTTP endpoint.
Configure via environment variables:

    BLEEPSTORE_ENDPOINT=http://localhost:9000
    BLEEPSTORE_ACCESS_KEY=bleepstore
    BLEEPSTORE_SECRET_KEY=bleepstore-secret
    BLEEPSTORE_REGION=us-east-1
"""

import os
import uuid

import boto3
import pytest
from botocore.config import Config


def pytest_configure(config):
    config.addinivalue_line("markers", "bucket_ops: Bucket operations")
    config.addinivalue_line("markers", "object_ops: Object operations")
    config.addinivalue_line("markers", "multipart_ops: Multipart upload operations")
    config.addinivalue_line("markers", "presigned: Presigned URL operations")
    config.addinivalue_line("markers", "acl_ops: ACL operations")
    config.addinivalue_line("markers", "error_handling: Error response handling")
    config.addinivalue_line("markers", "auth: Authentication tests")

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


@pytest.fixture(scope="session")
def s3_client():
    """Create a boto3 S3 client configured for BleepStore."""
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


@pytest.fixture(scope="session")
def s3_resource():
    """Create a boto3 S3 resource configured for BleepStore."""
    return boto3.resource(
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


@pytest.fixture()
def bucket_name():
    """Generate a unique bucket name for a test."""
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture()
def created_bucket(s3_client, bucket_name):
    """Create a bucket, yield its name, then clean up."""
    s3_client.create_bucket(Bucket=bucket_name)
    yield bucket_name
    # Cleanup: delete all objects then the bucket
    _empty_and_delete_bucket(s3_client, bucket_name)


@pytest.fixture()
def created_bucket_with_objects(s3_client, created_bucket):
    """Create a bucket with sample objects for listing tests."""
    objects = [
        "file1.txt",
        "file2.txt",
        "photos/2024/jan/photo1.jpg",
        "photos/2024/jan/photo2.jpg",
        "photos/2024/feb/photo3.jpg",
        "photos/2025/mar/photo4.jpg",
        "docs/readme.md",
        "docs/guide.md",
    ]
    for key in objects:
        s3_client.put_object(
            Bucket=created_bucket,
            Key=key,
            Body=f"content of {key}".encode(),
        )
    yield created_bucket, objects


def _empty_and_delete_bucket(client, bucket_name):
    """Delete all objects in a bucket, then delete the bucket."""
    try:
        # List and delete all objects
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            objects = page.get("Contents", [])
            if objects:
                client.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
                )
        # Abort any incomplete multipart uploads
        uploads = client.list_multipart_uploads(Bucket=bucket_name)
        for upload in uploads.get("Uploads", []):
            client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=upload["Key"],
                UploadId=upload["UploadId"],
            )
        # Delete the bucket
        client.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass  # Best-effort cleanup
