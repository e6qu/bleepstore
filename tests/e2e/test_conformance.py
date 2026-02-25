"""E2E conformance tests for BleepStore S3-compatible features.

Tests Content-MD5 validation, If-None-Match conditional PUTs,
ACL grant headers, and canned/grant exclusivity.
"""

import base64
import hashlib
import os
import uuid
import xml.etree.ElementTree as ET

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


def _signed_request(method, url, data=None, headers=None):
    """Build and send a SigV4-signed raw HTTP request."""
    creds = Credentials(ACCESS_KEY, SECRET_KEY)
    if headers is None:
        headers = {}
    aws_request = AWSRequest(
        method=method,
        url=url,
        data=data or b"",
        headers=headers,
    )
    SigV4Auth(creds, "s3", REGION).add_auth(aws_request)
    return getattr(requests, method.lower())(
        url,
        data=data or b"",
        headers=dict(aws_request.headers),
    )


def _create_bucket_raw(bucket_name):
    """Create a bucket via signed raw HTTP PUT."""
    url = f"{ENDPOINT}/{bucket_name}"
    resp = _signed_request("PUT", url)
    assert resp.status_code == 200, f"Failed to create bucket: {resp.status_code} {resp.text}"


def _delete_object_raw(bucket_name, key):
    """Delete an object via signed raw HTTP DELETE."""
    url = f"{ENDPOINT}/{bucket_name}/{key}"
    _signed_request("DELETE", url)


def _delete_bucket_raw(bucket_name):
    """Delete a bucket via signed raw HTTP DELETE."""
    url = f"{ENDPOINT}/{bucket_name}"
    _signed_request("DELETE", url)


def _parse_error_code(response_text):
    """Extract the error Code from an S3 XML error response."""
    try:
        root = ET.fromstring(response_text)
        # Handle both namespaced and non-namespaced XML
        code_elem = root.find("Code")
        if code_elem is None:
            code_elem = root.find("{http://s3.amazonaws.com/doc/2006-03-01/}Code")
        return code_elem.text if code_elem is not None else None
    except ET.ParseError:
        return None


# ---------------------------------------------------------------------------
# Content-MD5 validation tests
# ---------------------------------------------------------------------------


@pytest.mark.object_ops
class TestContentMD5:
    def test_put_object_valid_content_md5(self, s3_client, created_bucket):
        """PUT an object with a correct Content-MD5 header succeeds."""
        body = b"Hello, Content-MD5 validation!"
        md5_digest = base64.b64encode(hashlib.md5(body).digest()).decode()

        resp = s3_client.put_object(
            Bucket=created_bucket,
            Key="valid-md5.txt",
            Body=body,
            ContentMD5=md5_digest,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_object_bad_digest(self, s3_client, created_bucket):
        """PUT with wrong Content-MD5 (valid base64, wrong hash) returns BadDigest."""
        body = b"Hello, bad digest test!"
        # Compute MD5 of different content to get a valid but wrong hash
        wrong_md5 = base64.b64encode(hashlib.md5(b"wrong content").digest()).decode()

        with pytest.raises(ClientError) as exc_info:
            s3_client.put_object(
                Bucket=created_bucket,
                Key="bad-digest.txt",
                Body=body,
                ContentMD5=wrong_md5,
            )
        assert exc_info.value.response["Error"]["Code"] == "BadDigest"

    def test_put_object_invalid_digest(self, s3_client, created_bucket):
        """PUT with invalid Content-MD5 (not valid base64) returns InvalidDigest."""
        body = b"Hello, invalid digest test!"

        # Use raw HTTP since boto3 may validate Content-MD5 client-side
        url = f"{ENDPOINT}/{created_bucket}/invalid-digest.txt"
        resp = _signed_request(
            "PUT",
            url,
            data=body,
            headers={"Content-MD5": "not-base64!!!"},
        )
        assert resp.status_code == 400
        error_code = _parse_error_code(resp.text)
        assert error_code == "InvalidDigest"


# ---------------------------------------------------------------------------
# If-None-Match conditional PUT tests
# ---------------------------------------------------------------------------


@pytest.mark.object_ops
class TestIfNoneMatch:
    def test_put_object_if_none_match_new(self, s3_client, created_bucket):
        """PUT with If-None-Match: * on a new key succeeds."""
        key = f"if-none-match-new-{uuid.uuid4().hex[:8]}.txt"
        body = b"conditional create"
        url = f"{ENDPOINT}/{created_bucket}/{key}"

        resp = _signed_request(
            "PUT",
            url,
            data=body,
            headers={"If-None-Match": "*"},
        )
        assert resp.status_code == 200

        # Cleanup
        _delete_object_raw(created_bucket, key)

    def test_put_object_if_none_match_existing(self, s3_client, created_bucket):
        """PUT with If-None-Match: * on an existing key returns 412."""
        key = f"if-none-match-exists-{uuid.uuid4().hex[:8]}.txt"
        body = b"original content"

        # First, create the object normally
        s3_client.put_object(
            Bucket=created_bucket, Key=key, Body=body
        )

        # Now attempt conditional PUT with If-None-Match: *
        url = f"{ENDPOINT}/{created_bucket}/{key}"
        resp = _signed_request(
            "PUT",
            url,
            data=b"should not overwrite",
            headers={"If-None-Match": "*"},
        )
        assert resp.status_code == 412
        error_code = _parse_error_code(resp.text)
        assert error_code == "PreconditionFailed"


# ---------------------------------------------------------------------------
# ACL grant header tests
# ---------------------------------------------------------------------------


@pytest.mark.bucket_ops
class TestAclGrantHeaders:
    def test_create_bucket_grant_headers(self, s3_client):
        """Create a bucket with x-amz-grant-read header and verify via GetBucketAcl."""
        bucket_name = f"test-grant-create-{uuid.uuid4().hex[:12]}"
        url = f"{ENDPOINT}/{bucket_name}"

        try:
            resp = _signed_request(
                "PUT",
                url,
                headers={
                    "x-amz-grant-read": 'uri="http://acs.amazonaws.com/groups/global/AllUsers"',
                },
            )
            assert resp.status_code == 200

            # Verify the grant via GetBucketAcl using boto3
            acl_resp = s3_client.get_bucket_acl(Bucket=bucket_name)
            grants = acl_resp["Grants"]

            # Find a READ grant for AllUsers
            has_read_grant = False
            for grant in grants:
                grantee = grant.get("Grantee", {})
                permission = grant.get("Permission", "")
                uri = grantee.get("URI", "")
                if (
                    permission == "READ"
                    and "AllUsers" in uri
                ):
                    has_read_grant = True
                    break
            assert has_read_grant, f"Expected READ grant for AllUsers, got grants: {grants}"
        finally:
            _delete_bucket_raw(bucket_name)

    def test_put_bucket_acl_grant_headers(self, s3_client, created_bucket):
        """PutBucketAcl with x-amz-grant-full-control header and verify."""
        url = f"{ENDPOINT}/{created_bucket}?acl"

        resp = _signed_request(
            "PUT",
            url,
            headers={
                "x-amz-grant-full-control": 'id="testuser"',
            },
        )
        assert resp.status_code == 200

        # Verify the grant via GetBucketAcl
        acl_resp = s3_client.get_bucket_acl(Bucket=created_bucket)
        grants = acl_resp["Grants"]

        # Find a FULL_CONTROL grant for testuser
        has_fc_grant = False
        for grant in grants:
            grantee = grant.get("Grantee", {})
            permission = grant.get("Permission", "")
            grantee_id = grantee.get("ID", "")
            if permission == "FULL_CONTROL" and grantee_id == "testuser":
                has_fc_grant = True
                break
        assert has_fc_grant, f"Expected FULL_CONTROL grant for testuser, got grants: {grants}"


# ---------------------------------------------------------------------------
# Canned ACL and grant header exclusivity test
# ---------------------------------------------------------------------------


@pytest.mark.error_handling
class TestAclExclusivity:
    def test_acl_canned_and_grant_exclusive(self, s3_client, created_bucket):
        """Sending both x-amz-acl and x-amz-grant-* headers returns InvalidArgument."""
        url = f"{ENDPOINT}/{created_bucket}?acl"

        resp = _signed_request(
            "PUT",
            url,
            headers={
                "x-amz-acl": "private",
                "x-amz-grant-read": 'uri="http://acs.amazonaws.com/groups/global/AllUsers"',
            },
        )
        assert resp.status_code == 400
        error_code = _parse_error_code(resp.text)
        assert error_code == "InvalidArgument"
