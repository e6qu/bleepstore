"""Multi-SDK S3 operations test suite.

Every test uses the `s3` fixture and runs once per available client.
Bucket setup/teardown uses boto3 directly via the `bucket` fixture.
"""

from __future__ import annotations

import os

import pytest

from clients.base import S3ClientError

# =============================================================================
# Bucket operations
# =============================================================================


@pytest.mark.bucket_ops
class TestBucketOperations:
    def test_create_bucket(self, s3, bucket_name):
        s3.create_bucket(bucket_name)
        assert s3.head_bucket(bucket_name) == 200
        # Cleanup
        s3.delete_bucket(bucket_name)

    def test_list_buckets(self, s3, bucket):
        names = s3.list_buckets()
        assert bucket in names

    def test_head_bucket_exists(self, s3, bucket):
        assert s3.head_bucket(bucket) == 200

    def test_head_bucket_not_found(self, s3):
        assert s3.head_bucket("nonexistent-bucket-xyz-999") == 404

    def test_delete_empty_bucket(self, s3, bucket_name):
        # Create via the SDK under test, delete via the SDK under test
        s3.create_bucket(bucket_name)
        s3.delete_bucket(bucket_name)
        assert s3.head_bucket(bucket_name) == 404


# =============================================================================
# Object CRUD
# =============================================================================


@pytest.mark.object_ops
class TestObjectCRUD:
    def test_put_and_get_roundtrip(self, s3, bucket):
        body = b"hello world"
        s3.put_object(bucket, "test.txt", body)
        result = s3.get_object(bucket, "test.txt")
        assert result == body

    def test_put_object_returns_etag(self, s3, bucket):
        etag = s3.put_object(bucket, "etag-test.txt", b"data")
        # etag should be non-empty (format varies by client)
        assert etag is not None

    def test_put_empty_object(self, s3, bucket):
        s3.put_object(bucket, "empty.txt", b"")
        result = s3.get_object(bucket, "empty.txt")
        assert result == b""

    def test_get_nonexistent_object(self, s3, bucket):
        with pytest.raises((S3ClientError, Exception)):
            s3.get_object(bucket, "does-not-exist.txt")

    def test_head_object(self, s3, bucket):
        body = b"hello"
        s3.put_object(bucket, "head-test.txt", body)
        info = s3.head_object(bucket, "head-test.txt")
        assert info["size"] == len(body)
        assert info["etag"]

    def test_delete_object(self, s3, bucket):
        s3.put_object(bucket, "to-delete.txt", b"bye")
        s3.delete_object(bucket, "to-delete.txt")
        with pytest.raises((S3ClientError, Exception)):
            s3.get_object(bucket, "to-delete.txt")

    def test_delete_object_idempotent(self, s3, bucket):
        # Deleting a nonexistent object should not raise
        s3.delete_object(bucket, "never-existed.txt")

    def test_copy_object(self, s3, bucket):
        body = b"original content"
        s3.put_object(bucket, "src.txt", body)
        s3.copy_object(bucket, "src.txt", "dst.txt")
        result = s3.get_object(bucket, "dst.txt")
        assert result == body

    def test_put_nested_key(self, s3, bucket):
        body = b"nested"
        key = "a/b/c/file.txt"
        s3.put_object(bucket, key, body)
        result = s3.get_object(bucket, key)
        assert result == body

    def test_overwrite_object(self, s3, bucket):
        s3.put_object(bucket, "overwrite.txt", b"version1")
        s3.put_object(bucket, "overwrite.txt", b"version2")
        result = s3.get_object(bucket, "overwrite.txt")
        assert result == b"version2"


# =============================================================================
# Listing
# =============================================================================


@pytest.mark.listing_ops
class TestListing:
    def test_list_objects_basic(self, s3, bucket):
        s3.put_object(bucket, "a.txt", b"a")
        s3.put_object(bucket, "b.txt", b"b")
        result = s3.list_objects(bucket)
        assert "a.txt" in result["keys"]
        assert "b.txt" in result["keys"]

    def test_list_objects_with_prefix(self, s3, bucket):
        s3.put_object(bucket, "logs/app.log", b"log1")
        s3.put_object(bucket, "logs/error.log", b"log2")
        s3.put_object(bucket, "data/file.csv", b"csv")
        result = s3.list_objects(bucket, prefix="logs/")
        assert "logs/app.log" in result["keys"]
        assert "logs/error.log" in result["keys"]
        assert "data/file.csv" not in result["keys"]

    def test_list_objects_with_delimiter(self, s3, bucket):
        s3.put_object(bucket, "photos/jan/a.jpg", b"a")
        s3.put_object(bucket, "photos/feb/b.jpg", b"b")
        s3.put_object(bucket, "root.txt", b"r")
        result = s3.list_objects(bucket, delimiter="/")
        assert "root.txt" in result["keys"]
        assert "photos/" in result["prefixes"]

    def test_list_objects_empty_bucket(self, s3, bucket):
        result = s3.list_objects(bucket)
        assert result["keys"] == []
        assert result["prefixes"] == []


# =============================================================================
# Bulk delete
# =============================================================================


@pytest.mark.bulk_ops
class TestBulkDelete:
    def test_delete_objects_multiple(self, s3, bucket):
        keys = ["del1.txt", "del2.txt", "del3.txt"]
        for k in keys:
            s3.put_object(bucket, k, b"data")
        s3.delete_objects(bucket, keys)
        result = s3.list_objects(bucket)
        for k in keys:
            assert k not in result["keys"]

    def test_delete_objects_with_missing(self, s3, bucket):
        s3.put_object(bucket, "exists.txt", b"data")
        # Should not raise — missing keys are silently ignored
        s3.delete_objects(bucket, ["exists.txt", "ghost1.txt", "ghost2.txt"])


# =============================================================================
# Large file / multipart
# =============================================================================


@pytest.mark.large_file
class TestLargeFile:
    def test_upload_file_small(self, s3, bucket, tmp_path):
        filepath = tmp_path / "small.bin"
        filepath.write_bytes(b"x" * 1024)
        s3.upload_file(bucket, "small.bin", str(filepath))
        result = s3.get_object(bucket, "small.bin")
        assert len(result) == 1024

    def test_upload_file_large(self, s3, bucket, tmp_path):
        """6MB file — triggers multipart in most clients."""
        filepath = tmp_path / "large.bin"
        data = os.urandom(6 * 1024 * 1024)
        filepath.write_bytes(data)
        s3.upload_file(bucket, "large.bin", str(filepath))
        result = s3.get_object(bucket, "large.bin")
        assert len(result) == len(data)

    def test_download_file_roundtrip(self, s3, bucket, tmp_path):
        data = os.urandom(2048)
        s3.put_object(bucket, "download-me.bin", data)
        outpath = tmp_path / "downloaded.bin"
        s3.download_file(bucket, "download-me.bin", str(outpath))
        assert outpath.read_bytes() == data


# =============================================================================
# Data integrity
# =============================================================================


@pytest.mark.data_integrity
class TestDataIntegrity:
    def test_binary_data_roundtrip(self, s3, bucket):
        data = os.urandom(4096)
        s3.put_object(bucket, "binary.bin", data)
        result = s3.get_object(bucket, "binary.bin")
        assert result == data

    def test_unicode_key(self, s3, bucket):
        key = "café/naïve/résumé.txt"
        body = b"unicode key test"
        s3.put_object(bucket, key, body)
        result = s3.get_object(bucket, key)
        assert result == body

    def test_long_key(self, s3, bucket):
        """Key at 200 chars — long but within safe limits."""
        key = "a/" + "k" * 196 + ".txt"
        body = b"long key"
        s3.put_object(bucket, key, body)
        result = s3.get_object(bucket, key)
        assert result == body


# =============================================================================
# Cross-client interop
# =============================================================================


@pytest.mark.cross_client
class TestCrossClient:
    def test_boto3_writes_cli_reads(self, s3, bucket, s3_client_boto3):
        """Write via raw boto3, read via the current client."""
        body = b"written by boto3"
        s3_client_boto3.put_object(Bucket=bucket, Key="cross-read.txt", Body=body)
        result = s3.get_object(bucket, "cross-read.txt")
        assert result == body

    def test_cli_writes_boto3_reads(self, s3, bucket, s3_client_boto3):
        """Write via the current client, read via raw boto3."""
        body = b"written by cli"
        s3.put_object(bucket, "cross-write.txt", body)
        resp = s3_client_boto3.get_object(Bucket=bucket, Key="cross-write.txt")
        assert resp["Body"].read() == body
