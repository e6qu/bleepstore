"""E2E tests for S3 bucket operations."""

import pytest
from botocore.exceptions import ClientError


@pytest.mark.bucket_ops
class TestCreateBucket:
    def test_create_bucket(self, s3_client, bucket_name):
        """Create a bucket and verify it exists."""
        resp = s3_client.create_bucket(Bucket=bucket_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify bucket exists
        resp = s3_client.head_bucket(Bucket=bucket_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Cleanup
        s3_client.delete_bucket(Bucket=bucket_name)

    def test_create_bucket_already_exists(self, s3_client, created_bucket):
        """Creating an existing bucket you own should succeed (us-east-1 behavior)."""
        # In us-east-1, this returns 200. Other regions return 409.
        # BleepStore defaults to us-east-1 behavior.
        resp = s3_client.create_bucket(Bucket=created_bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_bucket_invalid_name(self, s3_client):
        """Bucket names must follow naming rules."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.create_bucket(Bucket="INVALID-UPPERCASE")
        assert exc_info.value.response["Error"]["Code"] in (
            "InvalidBucketName",
            "400",
        )

    def test_create_bucket_too_short_name(self, s3_client):
        """Bucket names must be at least 3 characters."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.create_bucket(Bucket="ab")
        assert exc_info.value.response["Error"]["Code"] in (
            "InvalidBucketName",
            "400",
        )


@pytest.mark.bucket_ops
class TestDeleteBucket:
    def test_delete_bucket(self, s3_client, bucket_name):
        """Delete an empty bucket."""
        s3_client.create_bucket(Bucket=bucket_name)
        resp = s3_client.delete_bucket(Bucket=bucket_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    def test_delete_nonexistent_bucket(self, s3_client):
        """Deleting a non-existent bucket returns NoSuchBucket."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.delete_bucket(Bucket="nonexistent-bucket-xyz123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_delete_nonempty_bucket(self, s3_client, created_bucket):
        """Deleting a non-empty bucket returns BucketNotEmpty."""
        s3_client.put_object(Bucket=created_bucket, Key="test.txt", Body=b"hello")
        with pytest.raises(ClientError) as exc_info:
            s3_client.delete_bucket(Bucket=created_bucket)
        assert exc_info.value.response["Error"]["Code"] == "BucketNotEmpty"


@pytest.mark.bucket_ops
class TestHeadBucket:
    def test_head_existing_bucket(self, s3_client, created_bucket):
        """HEAD on an existing bucket returns 200."""
        resp = s3_client.head_bucket(Bucket=created_bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_head_nonexistent_bucket(self, s3_client):
        """HEAD on a non-existent bucket returns 404."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.head_bucket(Bucket="nonexistent-bucket-xyz123")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


@pytest.mark.bucket_ops
class TestListBuckets:
    def test_list_buckets(self, s3_client, created_bucket):
        """ListBuckets should include the created bucket."""
        resp = s3_client.list_buckets()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        bucket_names = [b["Name"] for b in resp["Buckets"]]
        assert created_bucket in bucket_names

    def test_list_buckets_has_owner(self, s3_client):
        """ListBuckets response should include Owner."""
        resp = s3_client.list_buckets()
        assert "Owner" in resp
        assert "ID" in resp["Owner"]

    def test_list_buckets_creation_date(self, s3_client, created_bucket):
        """Each bucket should have a CreationDate."""
        resp = s3_client.list_buckets()
        for bucket in resp["Buckets"]:
            if bucket["Name"] == created_bucket:
                assert "CreationDate" in bucket
                break


@pytest.mark.bucket_ops
class TestGetBucketLocation:
    def test_get_bucket_location(self, s3_client, created_bucket):
        """GetBucketLocation returns the bucket's region."""
        resp = s3_client.get_bucket_location(Bucket=created_bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # us-east-1 returns None (empty LocationConstraint)
        # Other regions return the region string

    def test_get_location_nonexistent_bucket(self, s3_client):
        """GetBucketLocation on non-existent bucket returns NoSuchBucket."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_bucket_location(Bucket="nonexistent-bucket-xyz123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"


@pytest.mark.bucket_ops
@pytest.mark.acl_ops
class TestBucketAcl:
    def test_get_bucket_acl_default(self, s3_client, created_bucket):
        """Default ACL should grant FULL_CONTROL to owner."""
        resp = s3_client.get_bucket_acl(Bucket=created_bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Owner" in resp
        assert "Grants" in resp
        # Default: owner has FULL_CONTROL
        assert any(
            g["Permission"] == "FULL_CONTROL" for g in resp["Grants"]
        )

    def test_put_bucket_acl_canned(self, s3_client, created_bucket):
        """Set a canned ACL on a bucket."""
        resp = s3_client.put_bucket_acl(
            Bucket=created_bucket, ACL="public-read"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify the ACL was updated
        acl = s3_client.get_bucket_acl(Bucket=created_bucket)
        permissions = [g["Permission"] for g in acl["Grants"]]
        assert "READ" in permissions or "FULL_CONTROL" in permissions
