"""Cross-implementation consistency tests for BleepStore.

Sends identical S3 requests to all running BleepStore implementations and
verifies that responses are consistent across them. Skips automatically
if fewer than 2 implementations are running.

Usage:
    pytest test_cross_impl.py -m cross_impl

Requires all implementations to be running on their standard ports:
    Python: 9010, Go: 9011, Rust: 9012, Zig: 9013
"""

import hashlib
import os
import uuid

import boto3
import pytest
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

# Implementation definitions: name -> (port, endpoint)
IMPLEMENTATIONS = {
    "python": {"port": 9010, "endpoint": "http://localhost:9010"},
    "go": {"port": 9011, "endpoint": "http://localhost:9011"},
    "rust": {"port": 9012, "endpoint": "http://localhost:9012"},
    "zig": {"port": 9013, "endpoint": "http://localhost:9013"},
}

ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")

# Headers to ignore when comparing responses across implementations
IGNORED_HEADERS = {"x-amz-request-id", "x-amz-id-2", "date", "server"}


def _create_client(endpoint):
    """Create a boto3 S3 client for a given endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1, "mode": "standard"},
        ),
    )


def _check_health(endpoint):
    """Check if an implementation is running by hitting /health."""
    try:
        resp = requests.get(f"{endpoint}/health", timeout=2)
        return resp.status_code == 200
    except (requests.ConnectionError, requests.Timeout):
        return False


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
        uploads = client.list_multipart_uploads(Bucket=bucket_name)
        for upload in uploads.get("Uploads", []):
            client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=upload["Key"],
                UploadId=upload["UploadId"],
            )
        client.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass


@pytest.fixture(scope="session")
def impl_clients():
    """Create boto3 S3 clients for all running implementations.

    Auto-skips implementations that are not running (health check fails).
    Skips all tests if fewer than 2 implementations are running.

    Returns:
        Dict mapping implementation name -> boto3 S3 client.
    """
    clients = {}
    for name, info in IMPLEMENTATIONS.items():
        if _check_health(info["endpoint"]):
            clients[name] = _create_client(info["endpoint"])

    if len(clients) < 2:
        pytest.skip(
            f"Cross-implementation tests require at least 2 running implementations, "
            f"found {len(clients)}: {list(clients.keys())}"
        )

    return clients


@pytest.fixture()
def cross_bucket(impl_clients):
    """Create a uniquely named bucket on all running implementations, yield the
    name, then clean up.
    """
    bucket_name = f"cross-{uuid.uuid4().hex[:12]}"
    for name, client in impl_clients.items():
        client.create_bucket(Bucket=bucket_name)
    yield bucket_name
    for name, client in impl_clients.items():
        _empty_and_delete_bucket(client, bucket_name)


# 5 MiB minimum part size for multipart
MIN_PART_SIZE = 5 * 1024 * 1024


@pytest.mark.cross_impl
class TestCrossCreateDeleteBucket:
    def test_cross_create_delete_bucket(self, impl_clients):
        """Bucket create and delete operations produce consistent results."""
        bucket_name = f"cross-cd-{uuid.uuid4().hex[:12]}"

        # Create bucket on all implementations
        create_statuses = {}
        for name, client in impl_clients.items():
            resp = client.create_bucket(Bucket=bucket_name)
            create_statuses[name] = resp["ResponseMetadata"]["HTTPStatusCode"]

        # All should return 200
        status_values = list(create_statuses.values())
        assert all(
            s == status_values[0] for s in status_values
        ), f"Create status codes differ: {create_statuses}"

        # Verify bucket exists via HEAD on all implementations
        for name, client in impl_clients.items():
            resp = client.head_bucket(Bucket=bucket_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Delete bucket on all implementations
        delete_statuses = {}
        for name, client in impl_clients.items():
            resp = client.delete_bucket(Bucket=bucket_name)
            delete_statuses[name] = resp["ResponseMetadata"]["HTTPStatusCode"]

        status_values = list(delete_statuses.values())
        assert all(
            s == status_values[0] for s in status_values
        ), f"Delete status codes differ: {delete_statuses}"


@pytest.mark.cross_impl
class TestCrossPutGetObject:
    def test_cross_put_get_object(self, impl_clients, cross_bucket):
        """PUT and GET object operations produce consistent ETags and sizes."""
        key = "cross-test-obj.bin"
        body = os.urandom(2048)
        expected_etag = hashlib.md5(body).hexdigest()

        # PUT on all implementations
        put_etags = {}
        for name, client in impl_clients.items():
            resp = client.put_object(Bucket=cross_bucket, Key=key, Body=body)
            put_etags[name] = resp["ETag"].strip('"')

        # All ETags should match expected MD5
        for name, etag in put_etags.items():
            assert etag == expected_etag, (
                f"{name}: PUT ETag {etag} != expected {expected_etag}"
            )

        # GET on all implementations
        get_results = {}
        for name, client in impl_clients.items():
            resp = client.get_object(Bucket=cross_bucket, Key=key)
            data = resp["Body"].read()
            get_results[name] = {
                "status": resp["ResponseMetadata"]["HTTPStatusCode"],
                "size": resp["ContentLength"],
                "etag": resp["ETag"].strip('"'),
                "data_len": len(data),
                "data_match": data == body,
            }

        # Compare all GET results
        for name, result in get_results.items():
            assert result["status"] == 200, f"{name}: GET status {result['status']}"
            assert result["size"] == len(body), (
                f"{name}: size {result['size']} != {len(body)}"
            )
            assert result["etag"] == expected_etag, (
                f"{name}: GET ETag {result['etag']} != {expected_etag}"
            )
            assert result["data_match"], f"{name}: GET body does not match"


@pytest.mark.cross_impl
class TestCrossListObjects:
    def test_cross_list_objects(self, impl_clients, cross_bucket):
        """List operations return consistent key names and counts."""
        # Create identical objects on all implementations
        keys = ["alpha.txt", "beta.txt", "gamma/one.txt", "gamma/two.txt", "delta.txt"]
        for key in keys:
            body = f"content-of-{key}".encode()
            for name, client in impl_clients.items():
                client.put_object(Bucket=cross_bucket, Key=key, Body=body)

        # List objects on all implementations
        list_results = {}
        for name, client in impl_clients.items():
            resp = client.list_objects_v2(Bucket=cross_bucket)
            returned_keys = sorted(obj["Key"] for obj in resp.get("Contents", []))
            list_results[name] = {
                "status": resp["ResponseMetadata"]["HTTPStatusCode"],
                "count": resp["KeyCount"],
                "keys": returned_keys,
            }

        # All should return 200
        for name, result in list_results.items():
            assert result["status"] == 200, f"{name}: LIST status {result['status']}"

        # Compare counts
        counts = {name: result["count"] for name, result in list_results.items()}
        count_values = list(counts.values())
        assert all(
            c == count_values[0] for c in count_values
        ), f"Key counts differ: {counts}"

        # Compare key names
        reference_keys = list(list_results.values())[0]["keys"]
        for name, result in list_results.items():
            assert result["keys"] == reference_keys, (
                f"{name}: keys {result['keys']} != reference {reference_keys}"
            )


@pytest.mark.cross_impl
class TestCrossMultipartUpload:
    def test_cross_multipart_upload(self, impl_clients, cross_bucket):
        """Multipart upload lifecycle produces consistent results."""
        key = "cross-multipart.bin"
        part1_data = b"A" * MIN_PART_SIZE
        part2_data = b"B" * 1024  # Last part can be smaller

        total_size = len(part1_data) + len(part2_data)

        complete_results = {}
        for name, client in impl_clients.items():
            # Initiate
            create_resp = client.create_multipart_upload(
                Bucket=cross_bucket, Key=key, ContentType="application/octet-stream"
            )
            upload_id = create_resp["UploadId"]
            assert upload_id, f"{name}: no UploadId returned"

            # Upload parts
            part1 = client.upload_part(
                Bucket=cross_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=part1_data,
            )
            part2 = client.upload_part(
                Bucket=cross_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=2,
                Body=part2_data,
            )

            # Complete
            resp = client.complete_multipart_upload(
                Bucket=cross_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"ETag": part1["ETag"], "PartNumber": 1},
                        {"ETag": part2["ETag"], "PartNumber": 2},
                    ]
                },
            )

            complete_results[name] = {
                "status": resp["ResponseMetadata"]["HTTPStatusCode"],
                "etag": resp.get("ETag", "").strip('"'),
            }

        # All status codes should match
        statuses = {n: r["status"] for n, r in complete_results.items()}
        status_values = list(statuses.values())
        assert all(
            s == status_values[0] for s in status_values
        ), f"Complete status codes differ: {statuses}"

        # Verify final object size is consistent
        sizes = {}
        for name, client in impl_clients.items():
            head = client.head_object(Bucket=cross_bucket, Key=key)
            sizes[name] = head["ContentLength"]

        for name, size in sizes.items():
            assert size == total_size, (
                f"{name}: multipart object size {size} != expected {total_size}"
            )


@pytest.mark.cross_impl
class TestCrossHeadObject:
    def test_cross_head_object(self, impl_clients, cross_bucket):
        """HEAD object returns consistent metadata across implementations."""
        key = "cross-head-test.txt"
        body = b"head object cross-impl test content"
        expected_etag = hashlib.md5(body).hexdigest()

        # PUT on all implementations
        for name, client in impl_clients.items():
            client.put_object(
                Bucket=cross_bucket,
                Key=key,
                Body=body,
                ContentType="text/plain",
                Metadata={"test-key": "test-value"},
            )

        # HEAD on all implementations
        head_results = {}
        for name, client in impl_clients.items():
            resp = client.head_object(Bucket=cross_bucket, Key=key)
            head_results[name] = {
                "status": resp["ResponseMetadata"]["HTTPStatusCode"],
                "content_length": resp["ContentLength"],
                "content_type": resp["ContentType"],
                "etag": resp["ETag"].strip('"'),
                "metadata": resp.get("Metadata", {}),
            }

        # All should return 200
        for name, result in head_results.items():
            assert result["status"] == 200, f"{name}: HEAD status {result['status']}"

        # Compare content length
        for name, result in head_results.items():
            assert result["content_length"] == len(body), (
                f"{name}: ContentLength {result['content_length']} != {len(body)}"
            )

        # Compare ETags
        for name, result in head_results.items():
            assert result["etag"] == expected_etag, (
                f"{name}: ETag {result['etag']} != {expected_etag}"
            )

        # Compare content type
        for name, result in head_results.items():
            assert result["content_type"] == "text/plain", (
                f"{name}: ContentType {result['content_type']} != text/plain"
            )

        # Compare user metadata
        for name, result in head_results.items():
            assert result["metadata"].get("test-key") == "test-value", (
                f"{name}: metadata {result['metadata']} missing test-key"
            )


@pytest.mark.cross_impl
class TestCrossDeleteObject:
    def test_cross_delete_object(self, impl_clients, cross_bucket):
        """DELETE object returns consistent status across implementations."""
        key = "cross-delete-test.txt"

        # PUT on all implementations
        for name, client in impl_clients.items():
            client.put_object(Bucket=cross_bucket, Key=key, Body=b"delete me")

        # DELETE on all implementations
        delete_statuses = {}
        for name, client in impl_clients.items():
            resp = client.delete_object(Bucket=cross_bucket, Key=key)
            delete_statuses[name] = resp["ResponseMetadata"]["HTTPStatusCode"]

        # All should return 204
        for name, status in delete_statuses.items():
            assert status == 204, f"{name}: DELETE status {status} != 204"

        # Verify deleted: GET should return NoSuchKey on all implementations
        for name, client in impl_clients.items():
            with pytest.raises(ClientError) as exc_info:
                client.get_object(Bucket=cross_bucket, Key=key)
            assert exc_info.value.response["Error"]["Code"] == "NoSuchKey", (
                f"{name}: expected NoSuchKey after DELETE"
            )


@pytest.mark.cross_impl
class TestCrossErrorResponses:
    def test_cross_no_such_bucket(self, impl_clients):
        """NoSuchBucket error code is consistent across implementations."""
        fake_bucket = f"nonexistent-{uuid.uuid4().hex[:12]}"

        error_codes = {}
        for name, client in impl_clients.items():
            with pytest.raises(ClientError) as exc_info:
                client.get_object(Bucket=fake_bucket, Key="any-key.txt")
            error_codes[name] = exc_info.value.response["Error"]["Code"]

        # All should return NoSuchBucket
        for name, code in error_codes.items():
            assert code == "NoSuchBucket", (
                f"{name}: error code {code} != NoSuchBucket"
            )

    def test_cross_no_such_key(self, impl_clients, cross_bucket):
        """NoSuchKey error code is consistent across implementations."""
        error_codes = {}
        for name, client in impl_clients.items():
            with pytest.raises(ClientError) as exc_info:
                client.get_object(Bucket=cross_bucket, Key="nonexistent-key.txt")
            error_codes[name] = exc_info.value.response["Error"]["Code"]

        # All should return NoSuchKey
        for name, code in error_codes.items():
            assert code == "NoSuchKey", (
                f"{name}: error code {code} != NoSuchKey"
            )

    def test_cross_bucket_already_exists(self, impl_clients, cross_bucket):
        """BucketAlreadyOwnedByYou error is consistent when re-creating a bucket."""
        # cross_bucket already exists on all implementations.
        # Re-creating should either succeed (200) or return BucketAlreadyOwnedByYou.
        # The key is consistency: all implementations should behave the same way.
        results = {}
        for name, client in impl_clients.items():
            try:
                resp = client.create_bucket(Bucket=cross_bucket)
                results[name] = ("ok", resp["ResponseMetadata"]["HTTPStatusCode"])
            except ClientError as e:
                results[name] = ("error", e.response["Error"]["Code"])

        # All should produce the same outcome type
        outcome_types = {name: r[0] for name, r in results.items()}
        outcome_values = list(outcome_types.values())
        assert all(
            o == outcome_values[0] for o in outcome_values
        ), f"Bucket re-create behavior differs: {results}"

    def test_cross_head_nonexistent_object_404(self, impl_clients, cross_bucket):
        """HEAD on non-existent object returns 404 across all implementations."""
        statuses = {}
        for name, client in impl_clients.items():
            with pytest.raises(ClientError) as exc_info:
                client.head_object(Bucket=cross_bucket, Key="no-such-key.txt")
            statuses[name] = exc_info.value.response["ResponseMetadata"][
                "HTTPStatusCode"
            ]

        for name, status in statuses.items():
            assert status == 404, f"{name}: HEAD nonexistent returned {status} != 404"


@pytest.mark.cross_impl
class TestCrossPutBucketAcl:
    def test_cross_put_bucket_acl(self, impl_clients, cross_bucket):
        """ACL operations produce consistent results across implementations."""
        # Set a canned ACL on all implementations
        put_statuses = {}
        for name, client in impl_clients.items():
            resp = client.put_bucket_acl(Bucket=cross_bucket, ACL="public-read")
            put_statuses[name] = resp["ResponseMetadata"]["HTTPStatusCode"]

        # All should return 200
        for name, status in put_statuses.items():
            assert status == 200, f"{name}: PutBucketAcl status {status} != 200"

        # Get ACL and compare structure
        acl_results = {}
        for name, client in impl_clients.items():
            resp = client.get_bucket_acl(Bucket=cross_bucket)
            acl_results[name] = {
                "status": resp["ResponseMetadata"]["HTTPStatusCode"],
                "has_owner": "Owner" in resp,
                "has_grants": "Grants" in resp,
                "grant_count": len(resp.get("Grants", [])),
                "permissions": sorted(
                    g["Permission"] for g in resp.get("Grants", [])
                ),
            }

        # All should return 200
        for name, result in acl_results.items():
            assert result["status"] == 200, (
                f"{name}: GetBucketAcl status {result['status']}"
            )
            assert result["has_owner"], f"{name}: GetBucketAcl missing Owner"
            assert result["has_grants"], f"{name}: GetBucketAcl missing Grants"

        # Grant counts should match across implementations
        counts = {name: result["grant_count"] for name, result in acl_results.items()}
        count_values = list(counts.values())
        assert all(
            c == count_values[0] for c in count_values
        ), f"ACL grant counts differ: {counts}"

        # Permission sets should match
        permissions = {
            name: result["permissions"] for name, result in acl_results.items()
        }
        perm_values = list(permissions.values())
        assert all(
            p == perm_values[0] for p in perm_values
        ), f"ACL permissions differ: {permissions}"
