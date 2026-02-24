"""E2E tests for S3 ACL operations."""

import pytest
from botocore.exceptions import ClientError


@pytest.mark.acl_ops
class TestObjectAcl:
    def test_get_object_acl_default(self, s3_client, created_bucket):
        """Default ACL grants FULL_CONTROL to owner."""
        s3_client.put_object(
            Bucket=created_bucket, Key="acl-test.txt", Body=b"data"
        )
        resp = s3_client.get_object_acl(
            Bucket=created_bucket, Key="acl-test.txt"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Owner" in resp
        assert "Grants" in resp
        assert any(
            g["Permission"] == "FULL_CONTROL" for g in resp["Grants"]
        )

    def test_put_object_acl_canned(self, s3_client, created_bucket):
        """Set a canned ACL on an object."""
        s3_client.put_object(
            Bucket=created_bucket, Key="acl-canned.txt", Body=b"data"
        )
        resp = s3_client.put_object_acl(
            Bucket=created_bucket, Key="acl-canned.txt", ACL="public-read"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_object_with_canned_acl(self, s3_client, created_bucket):
        """Create an object with a canned ACL."""
        s3_client.put_object(
            Bucket=created_bucket,
            Key="acl-on-put.txt",
            Body=b"data",
            ACL="public-read",
        )
        resp = s3_client.get_object_acl(
            Bucket=created_bucket, Key="acl-on-put.txt"
        )
        permissions = [g["Permission"] for g in resp["Grants"]]
        assert "READ" in permissions or "FULL_CONTROL" in permissions

    def test_get_acl_nonexistent_object(self, s3_client, created_bucket):
        """Get ACL of non-existent object returns NoSuchKey."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object_acl(
                Bucket=created_bucket, Key="nonexistent.txt"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"
