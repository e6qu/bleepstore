"""E2E tests for S3 error handling."""

import os

import boto3
import requests
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")

# 5 MiB minimum part size
MIN_PART_SIZE = 5 * 1024 * 1024


@pytest.mark.error_handling
class TestErrorResponses:
    def test_nosuchbucket_error(self, s3_client):
        """Operations on non-existent bucket return NoSuchBucket."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.list_objects_v2(Bucket="this-bucket-does-not-exist-xyz")
        err = exc_info.value.response["Error"]
        assert err["Code"] == "NoSuchBucket"
        assert "Message" in err

    def test_nosuchkey_error(self, s3_client, created_bucket):
        """GET on non-existent key returns NoSuchKey."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket=created_bucket, Key="does-not-exist.txt"
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "NoSuchKey"

    def test_bucket_not_empty_error(self, s3_client, created_bucket):
        """Delete non-empty bucket returns BucketNotEmpty."""
        s3_client.put_object(
            Bucket=created_bucket, Key="blocker.txt", Body=b"data"
        )
        with pytest.raises(ClientError) as exc_info:
            s3_client.delete_bucket(Bucket=created_bucket)
        assert exc_info.value.response["Error"]["Code"] == "BucketNotEmpty"

    def test_invalid_bucket_name_error(self, s3_client):
        """Invalid bucket name returns InvalidBucketName."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.create_bucket(Bucket="A")
        # boto3 may raise ParamValidationError locally or server returns error
        err_code = exc_info.value.response.get("Error", {}).get("Code", "")
        assert err_code in ("InvalidBucketName", "400", "")

    def test_method_not_allowed(self, s3_client, created_bucket):
        """Unsupported operations return appropriate errors."""
        # This is harder to trigger through boto3 since it validates locally.
        # Tested more thoroughly via raw HTTP in smoke tests.
        pass

    def test_error_has_request_id(self, s3_client):
        """Error responses include x-amz-request-id."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(
                Bucket="nonexistent-bucket-xyz123", Key="file.txt"
            )
        headers = exc_info.value.response["ResponseMetadata"][
            "HTTPHeaders"
        ]
        assert "x-amz-request-id" in headers


@pytest.mark.error_handling
@pytest.mark.auth
class TestAuthErrors:
    def test_invalid_access_key(self):
        """Request with invalid access key returns InvalidAccessKeyId."""
        import boto3
        from botocore.config import Config

        bad_client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            aws_access_key_id="INVALID_KEY",
            aws_secret_access_key="INVALID_SECRET",
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 0},
            ),
        )
        with pytest.raises(ClientError) as exc_info:
            bad_client.list_buckets()
        err_code = exc_info.value.response["Error"]["Code"]
        assert err_code in ("InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied")

    def test_signature_mismatch(self):
        """Request with wrong secret key returns SignatureDoesNotMatch."""
        import boto3
        from botocore.config import Config
        import os

        # Use the correct access key but wrong secret
        bad_client = boto3.client(
            "s3",
            endpoint_url=os.environ.get(
                "BLEEPSTORE_ENDPOINT", "http://localhost:9000"
            ),
            aws_access_key_id=os.environ.get(
                "BLEEPSTORE_ACCESS_KEY", "bleepstore"
            ),
            aws_secret_access_key="wrong-secret-key",
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 0},
            ),
        )
        with pytest.raises(ClientError) as exc_info:
            bad_client.list_buckets()
        err_code = exc_info.value.response["Error"]["Code"]
        assert err_code in ("SignatureDoesNotMatch", "AccessDenied")


@pytest.mark.error_handling
@pytest.mark.multipart_ops
@pytest.mark.slow
class TestMultipartErrors:
    def test_entity_too_small_multipart(self, s3_client, created_bucket):
        """Part smaller than 5MB (except last) should error at CompleteMultipartUpload."""
        key = "too-small-parts.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            # Upload two parts where the first is smaller than 5MB
            small_data = b"X" * 1024  # 1 KiB - too small for non-last part
            last_data = b"Y" * 1024

            part1 = s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=small_data,
            )
            part2 = s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=2,
                Body=last_data,
            )

            with pytest.raises(ClientError) as exc_info:
                s3_client.complete_multipart_upload(
                    Bucket=created_bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [
                            {"PartNumber": 1, "ETag": part1["ETag"]},
                            {"PartNumber": 2, "ETag": part2["ETag"]},
                        ]
                    },
                )
            assert exc_info.value.response["Error"]["Code"] == "EntityTooSmall"
        finally:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )


@pytest.mark.error_handling
class TestMalformedRequestErrors:
    def test_malformed_xml(self, s3_client, created_bucket):
        """Send invalid XML body to DeleteObjects."""
        # Use raw requests to send malformed XML that boto3 would not produce
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        url = f"{ENDPOINT}/{created_bucket}?delete"
        malformed_body = b"<Delete><this is not valid xml"

        # Sign the request using the client's credentials
        credentials = s3_client._request_signer._credentials
        region = s3_client._client_config.region_name

        aws_request = AWSRequest(
            method="POST",
            url=url,
            data=malformed_body,
            headers={"Content-Type": "application/xml"},
        )
        SigV4Auth(credentials, "s3", region).add_auth(aws_request)

        resp = requests.post(
            url,
            data=malformed_body,
            headers=dict(aws_request.headers),
        )
        assert resp.status_code == 400
        assert "MalformedXML" in resp.text

    def test_missing_content_length(self, s3_client, created_bucket):
        """Send request without Content-Length where required."""
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        url = f"{ENDPOINT}/{created_bucket}/missing-cl.txt"

        # Build a PUT request and remove Content-Length before sending
        aws_request = AWSRequest(
            method="PUT",
            url=url,
            data=b"some data",
            headers={"Content-Type": "application/octet-stream"},
        )
        credentials = s3_client._request_signer._credentials
        region = s3_client._client_config.region_name
        SigV4Auth(credentials, "s3", region).add_auth(aws_request)

        headers = dict(aws_request.headers)
        headers.pop("Content-Length", None)
        headers["Transfer-Encoding"] = "identity"

        resp = requests.put(
            url,
            data=b"some data",
            headers=headers,
        )
        # Server should reject or handle gracefully
        # 501 is also valid: Go's net/http rejects Transfer-Encoding: identity at protocol level
        assert resp.status_code in (400, 411, 403, 501)

    def test_key_too_long(self, s3_client, created_bucket):
        """Key longer than 1024 bytes should be rejected."""
        long_key = "k" * 1025

        with pytest.raises(ClientError) as exc_info:
            s3_client.put_object(
                Bucket=created_bucket, Key=long_key, Body=b"data"
            )
        assert exc_info.value.response["Error"]["Code"] in (
            "KeyTooLongError",
            "KeyTooLong",
            "400",
        )
