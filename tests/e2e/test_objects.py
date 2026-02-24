"""E2E tests for S3 object operations."""

import hashlib
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError


@pytest.mark.object_ops
class TestPutAndGetObject:
    def test_put_and_get_small_object(self, s3_client, created_bucket):
        """PUT then GET a small object."""
        body = b"Hello, BleepStore!"
        s3_client.put_object(
            Bucket=created_bucket,
            Key="hello.txt",
            Body=body,
            ContentType="text/plain",
        )

        resp = s3_client.get_object(Bucket=created_bucket, Key="hello.txt")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["Body"].read() == body
        assert resp["ContentType"] == "text/plain"
        assert resp["ContentLength"] == len(body)

    def test_put_object_etag(self, s3_client, created_bucket):
        """PUT object should return MD5-based ETag."""
        body = b"test content"
        expected_md5 = hashlib.md5(body).hexdigest()

        resp = s3_client.put_object(
            Bucket=created_bucket, Key="etag-test.txt", Body=body
        )
        etag = resp["ETag"].strip('"')
        assert etag == expected_md5

    def test_put_object_with_metadata(self, s3_client, created_bucket):
        """PUT object with user-defined metadata."""
        s3_client.put_object(
            Bucket=created_bucket,
            Key="meta.txt",
            Body=b"data",
            Metadata={"author": "tester", "version": "1.0"},
        )

        resp = s3_client.head_object(Bucket=created_bucket, Key="meta.txt")
        assert resp["Metadata"]["author"] == "tester"
        assert resp["Metadata"]["version"] == "1.0"

    def test_put_object_overwrite(self, s3_client, created_bucket):
        """PUT to existing key overwrites the object."""
        s3_client.put_object(
            Bucket=created_bucket, Key="file.txt", Body=b"version 1"
        )
        s3_client.put_object(
            Bucket=created_bucket, Key="file.txt", Body=b"version 2"
        )

        resp = s3_client.get_object(Bucket=created_bucket, Key="file.txt")
        assert resp["Body"].read() == b"version 2"

    def test_put_object_with_slash_in_key(self, s3_client, created_bucket):
        """Keys with / are treated as opaque strings."""
        body = b"nested content"
        s3_client.put_object(
            Bucket=created_bucket, Key="a/b/c/file.txt", Body=body
        )

        resp = s3_client.get_object(
            Bucket=created_bucket, Key="a/b/c/file.txt"
        )
        assert resp["Body"].read() == body

    def test_put_empty_object(self, s3_client, created_bucket):
        """PUT a zero-byte object."""
        s3_client.put_object(
            Bucket=created_bucket, Key="empty.txt", Body=b""
        )

        resp = s3_client.head_object(Bucket=created_bucket, Key="empty.txt")
        assert resp["ContentLength"] == 0

    def test_get_nonexistent_object(self, s3_client, created_bucket):
        """GET a non-existent key returns NoSuchKey."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(Bucket=created_bucket, Key="nonexistent.txt")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    def test_get_object_in_nonexistent_bucket(self, s3_client):
        """GET from a non-existent bucket returns NoSuchBucket."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket="nonexistent-bucket-xyz123", Key="file.txt"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"


@pytest.mark.object_ops
class TestGetObjectRange:
    def test_range_request(self, s3_client, created_bucket):
        """GET with Range header returns partial content."""
        body = b"0123456789ABCDEF"
        s3_client.put_object(
            Bucket=created_bucket, Key="range.txt", Body=body
        )

        resp = s3_client.get_object(
            Bucket=created_bucket, Key="range.txt", Range="bytes=0-4"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert resp["Body"].read() == b"01234"
        assert "ContentRange" in resp

    def test_range_request_suffix(self, s3_client, created_bucket):
        """Range with suffix: bytes=-5 returns last 5 bytes."""
        body = b"0123456789ABCDEF"
        s3_client.put_object(
            Bucket=created_bucket, Key="range2.txt", Body=body
        )

        resp = s3_client.get_object(
            Bucket=created_bucket, Key="range2.txt", Range="bytes=-5"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert resp["Body"].read() == b"BCDEF"

    def test_invalid_range(self, s3_client, created_bucket):
        """Invalid range returns 416."""
        s3_client.put_object(
            Bucket=created_bucket, Key="range3.txt", Body=b"short"
        )

        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket=created_bucket, Key="range3.txt", Range="bytes=100-200"
            )
        assert (
            exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
            == 416
        )


@pytest.mark.object_ops
class TestHeadObject:
    def test_head_object(self, s3_client, created_bucket):
        """HEAD returns metadata without body."""
        body = b"head test content"
        s3_client.put_object(
            Bucket=created_bucket,
            Key="head.txt",
            Body=body,
            ContentType="text/plain",
        )

        resp = s3_client.head_object(Bucket=created_bucket, Key="head.txt")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["ContentLength"] == len(body)
        assert resp["ContentType"] == "text/plain"
        assert "ETag" in resp
        assert "LastModified" in resp

    def test_head_nonexistent_object(self, s3_client, created_bucket):
        """HEAD on non-existent key returns 404."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.head_object(
                Bucket=created_bucket, Key="nonexistent.txt"
            )
        assert (
            exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
            == 404
        )


@pytest.mark.object_ops
class TestConditionalRequests:
    def test_if_match_success(self, s3_client, created_bucket):
        """If-Match with correct ETag returns 200."""
        body = b"conditional test"
        put_resp = s3_client.put_object(
            Bucket=created_bucket, Key="cond.txt", Body=body
        )
        etag = put_resp["ETag"]

        resp = s3_client.get_object(
            Bucket=created_bucket, Key="cond.txt", IfMatch=etag
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_if_match_failure(self, s3_client, created_bucket):
        """If-Match with wrong ETag returns 412."""
        s3_client.put_object(
            Bucket=created_bucket, Key="cond2.txt", Body=b"data"
        )

        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket=created_bucket,
                Key="cond2.txt",
                IfMatch='"wrong-etag"',
            )
        assert (
            exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
            == 412
        )

    def test_if_none_match_returns_304(self, s3_client, created_bucket):
        """If-None-Match with matching ETag returns 304."""
        body = b"not modified test"
        put_resp = s3_client.put_object(
            Bucket=created_bucket, Key="cond3.txt", Body=body
        )
        etag = put_resp["ETag"]

        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket=created_bucket, Key="cond3.txt", IfNoneMatch=etag
            )
        assert (
            exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
            == 304
        )

    def test_if_modified_since_returns_304(self, s3_client, created_bucket):
        """Object not modified since timestamp returns 304."""
        body = b"not modified since test"
        s3_client.put_object(
            Bucket=created_bucket, Key="cond-mod.txt", Body=body
        )

        # Get the object's LastModified timestamp
        head_resp = s3_client.head_object(
            Bucket=created_bucket, Key="cond-mod.txt"
        )
        last_modified = head_resp["LastModified"]

        # Request with a timestamp in the future - object has not been
        # modified since then, so we should get 304
        future_time = last_modified + timedelta(seconds=60)
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket=created_bucket,
                Key="cond-mod.txt",
                IfModifiedSince=future_time,
            )
        assert (
            exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
            == 304
        )

    def test_if_unmodified_since_precondition_failed(
        self, s3_client, created_bucket
    ):
        """Object modified since timestamp returns 412 Precondition Failed."""
        body = b"unmodified since test"
        s3_client.put_object(
            Bucket=created_bucket, Key="cond-unmod.txt", Body=body
        )

        # Request with a timestamp far in the past - object was modified
        # after this time, so If-Unmodified-Since should fail with 412
        past_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket=created_bucket,
                Key="cond-unmod.txt",
                IfUnmodifiedSince=past_time,
            )
        assert (
            exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
            == 412
        )


@pytest.mark.object_ops
class TestDeleteObject:
    def test_delete_object(self, s3_client, created_bucket):
        """Delete an existing object."""
        s3_client.put_object(
            Bucket=created_bucket, Key="delete-me.txt", Body=b"bye"
        )
        resp = s3_client.delete_object(
            Bucket=created_bucket, Key="delete-me.txt"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204

        # Verify deleted
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket=created_bucket, Key="delete-me.txt"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    def test_delete_nonexistent_object(self, s3_client, created_bucket):
        """Delete of non-existent object returns 204 (idempotent)."""
        resp = s3_client.delete_object(
            Bucket=created_bucket, Key="never-existed.txt"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


@pytest.mark.object_ops
class TestDeleteObjects:
    def test_delete_multiple_objects(self, s3_client, created_bucket):
        """Delete multiple objects in one request."""
        keys = ["a.txt", "b.txt", "c.txt"]
        for key in keys:
            s3_client.put_object(
                Bucket=created_bucket, Key=key, Body=b"data"
            )

        resp = s3_client.delete_objects(
            Bucket=created_bucket,
            Delete={"Objects": [{"Key": k} for k in keys]},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        deleted_keys = [d["Key"] for d in resp.get("Deleted", [])]
        for key in keys:
            assert key in deleted_keys

    def test_delete_objects_quiet_mode(self, s3_client, created_bucket):
        """Quiet mode only returns errors, not successes."""
        s3_client.put_object(
            Bucket=created_bucket, Key="quiet.txt", Body=b"data"
        )

        resp = s3_client.delete_objects(
            Bucket=created_bucket,
            Delete={"Objects": [{"Key": "quiet.txt"}], "Quiet": True},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # In quiet mode, no Deleted entries for successful deletes
        assert len(resp.get("Errors", [])) == 0


@pytest.mark.object_ops
class TestCopyObject:
    def test_copy_object(self, s3_client, created_bucket):
        """Copy an object within the same bucket."""
        body = b"copy me"
        s3_client.put_object(
            Bucket=created_bucket, Key="original.txt", Body=body
        )

        resp = s3_client.copy_object(
            Bucket=created_bucket,
            Key="copy.txt",
            CopySource=f"{created_bucket}/original.txt",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "CopyObjectResult" in resp

        # Verify copy
        get_resp = s3_client.get_object(
            Bucket=created_bucket, Key="copy.txt"
        )
        assert get_resp["Body"].read() == body

    def test_copy_object_with_replace_metadata(self, s3_client, created_bucket):
        """Copy with REPLACE metadata directive."""
        s3_client.put_object(
            Bucket=created_bucket,
            Key="src.txt",
            Body=b"data",
            Metadata={"original": "true"},
        )

        s3_client.copy_object(
            Bucket=created_bucket,
            Key="dst.txt",
            CopySource=f"{created_bucket}/src.txt",
            MetadataDirective="REPLACE",
            Metadata={"copied": "true"},
            ContentType="text/csv",
        )

        resp = s3_client.head_object(Bucket=created_bucket, Key="dst.txt")
        assert resp["Metadata"].get("copied") == "true"
        assert "original" not in resp["Metadata"]

    def test_copy_nonexistent_source(self, s3_client, created_bucket):
        """Copy from non-existent source returns NoSuchKey."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.copy_object(
                Bucket=created_bucket,
                Key="dst.txt",
                CopySource=f"{created_bucket}/nonexistent.txt",
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"


@pytest.mark.object_ops
class TestListObjectsV2:
    def test_list_objects(self, s3_client, created_bucket_with_objects):
        """ListObjectsV2 returns all objects."""
        bucket, objects = created_bucket_with_objects
        resp = s3_client.list_objects_v2(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["KeyCount"] == len(objects)
        returned_keys = [obj["Key"] for obj in resp["Contents"]]
        for key in objects:
            assert key in returned_keys

    def test_list_objects_with_prefix(self, s3_client, created_bucket_with_objects):
        """ListObjectsV2 with prefix filter."""
        bucket, _ = created_bucket_with_objects
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix="photos/")
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert all(k.startswith("photos/") for k in keys)
        assert resp["KeyCount"] == 4

    def test_list_objects_with_delimiter(self, s3_client, created_bucket_with_objects):
        """ListObjectsV2 with delimiter returns CommonPrefixes."""
        bucket, _ = created_bucket_with_objects
        resp = s3_client.list_objects_v2(Bucket=bucket, Delimiter="/")
        # Top-level files
        file_keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert "file1.txt" in file_keys
        assert "file2.txt" in file_keys
        # Common prefixes
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert "photos/" in prefixes
        assert "docs/" in prefixes

    def test_list_objects_pagination(self, s3_client, created_bucket):
        """ListObjectsV2 pagination with MaxKeys."""
        # Create 5 objects
        for i in range(5):
            s3_client.put_object(
                Bucket=created_bucket, Key=f"page-{i:03d}.txt", Body=b"x"
            )

        # List with max 2
        resp = s3_client.list_objects_v2(Bucket=created_bucket, MaxKeys=2)
        assert resp["MaxKeys"] == 2
        assert resp["KeyCount"] == 2
        assert resp["IsTruncated"] is True
        assert "NextContinuationToken" in resp

        # Get next page
        resp2 = s3_client.list_objects_v2(
            Bucket=created_bucket,
            MaxKeys=2,
            ContinuationToken=resp["NextContinuationToken"],
        )
        assert resp2["KeyCount"] == 2

    def test_list_objects_empty_bucket(self, s3_client, created_bucket):
        """ListObjectsV2 on empty bucket returns empty result."""
        resp = s3_client.list_objects_v2(Bucket=created_bucket)
        assert resp["KeyCount"] == 0
        assert "Contents" not in resp

    def test_list_objects_start_after(self, s3_client, created_bucket_with_objects):
        """ListObjectsV2 with StartAfter."""
        bucket, _ = created_bucket_with_objects
        resp = s3_client.list_objects_v2(Bucket=bucket, StartAfter="file2.txt")
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert "file1.txt" not in keys
        assert "file2.txt" not in keys

    def test_list_objects_content_fields(self, s3_client, created_bucket):
        """Each object in listing has required fields."""
        s3_client.put_object(
            Bucket=created_bucket, Key="fields.txt", Body=b"test"
        )
        resp = s3_client.list_objects_v2(Bucket=created_bucket)
        obj = resp["Contents"][0]
        assert "Key" in obj
        assert "LastModified" in obj
        assert "ETag" in obj
        assert "Size" in obj
        assert "StorageClass" in obj


@pytest.mark.object_ops
class TestListObjectsV1:
    def test_list_objects_v1(self, s3_client, created_bucket_with_objects):
        """Legacy ListObjects (v1) returns objects."""
        bucket, objects = created_bucket_with_objects
        resp = s3_client.list_objects(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        returned_keys = [obj["Key"] for obj in resp["Contents"]]
        for key in objects:
            assert key in returned_keys

    def test_list_objects_v1_with_marker(self, s3_client, created_bucket_with_objects):
        """V1 pagination uses Marker."""
        bucket, _ = created_bucket_with_objects
        resp = s3_client.list_objects(Bucket=bucket, MaxKeys=2)
        assert resp["IsTruncated"] is True
        assert resp["MaxKeys"] == 2
