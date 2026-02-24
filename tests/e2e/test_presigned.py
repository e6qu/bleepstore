"""E2E tests for S3 presigned URLs."""

import time

import requests
import pytest


@pytest.mark.presigned
class TestPresignedGetUrl:
    def test_presigned_get(self, s3_client, created_bucket):
        """Generate a presigned GET URL and download the object."""
        body = b"presigned download content"
        s3_client.put_object(
            Bucket=created_bucket, Key="presigned-get.txt", Body=body
        )

        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": created_bucket, "Key": "presigned-get.txt"},
            ExpiresIn=300,
        )

        # Download without AWS credentials (plain HTTP GET)
        resp = requests.get(url)
        assert resp.status_code == 200
        assert resp.content == body

    def test_presigned_get_nonexistent_key(self, s3_client, created_bucket):
        """Presigned URL for non-existent key returns 404 when accessed."""
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": created_bucket, "Key": "nonexistent.txt"},
            ExpiresIn=300,
        )

        resp = requests.get(url)
        assert resp.status_code in (403, 404)


@pytest.mark.presigned
class TestPresignedPutUrl:
    def test_presigned_put(self, s3_client, created_bucket):
        """Generate a presigned PUT URL and upload an object."""
        url = s3_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": created_bucket, "Key": "presigned-put.txt"},
            ExpiresIn=300,
        )

        # Upload without AWS credentials (plain HTTP PUT)
        body = b"uploaded via presigned URL"
        resp = requests.put(url, data=body)
        assert resp.status_code == 200

        # Verify upload
        get_resp = s3_client.get_object(
            Bucket=created_bucket, Key="presigned-put.txt"
        )
        assert get_resp["Body"].read() == body


@pytest.mark.presigned
class TestPresignedUrlExpiration:
    def test_presigned_url_contains_expected_params(self, s3_client, created_bucket):
        """Presigned URL contains all required SigV4 query parameters."""
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": created_bucket, "Key": "test.txt"},
            ExpiresIn=3600,
        )

        assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
        assert "X-Amz-Credential=" in url
        assert "X-Amz-Date=" in url
        assert "X-Amz-Expires=" in url
        assert "X-Amz-SignedHeaders=" in url
        assert "X-Amz-Signature=" in url

    @pytest.mark.slow
    def test_presigned_url_expired(self, s3_client, created_bucket):
        """Generate URL with 1-second expiry, wait, verify 403."""
        body = b"expires soon"
        s3_client.put_object(
            Bucket=created_bucket, Key="presigned-expire.txt", Body=body
        )

        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": created_bucket, "Key": "presigned-expire.txt"},
            ExpiresIn=1,
        )

        # Wait for the URL to expire
        time.sleep(2)

        resp = requests.get(url)
        assert resp.status_code == 403

    def test_presigned_put_and_verify(self, s3_client, created_bucket):
        """PUT via presigned URL, then GET to verify content."""
        key = "presigned-roundtrip.txt"
        body = b"roundtrip presigned content"

        # Generate presigned PUT URL
        put_url = s3_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": created_bucket, "Key": key},
            ExpiresIn=300,
        )

        # Upload via presigned URL (plain HTTP, no AWS credentials)
        put_resp = requests.put(put_url, data=body)
        assert put_resp.status_code == 200

        # Generate presigned GET URL
        get_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": created_bucket, "Key": key},
            ExpiresIn=300,
        )

        # Download via presigned URL and verify content
        get_resp = requests.get(get_url)
        assert get_resp.status_code == 200
        assert get_resp.content == body
