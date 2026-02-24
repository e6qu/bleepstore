"""Integration tests for bucket handlers.

These tests use a real SQLite metadata store (in-memory) and exercise
the full request path via httpx AsyncClient. Each test class creates
its own metadata store and app to avoid state leaks between tests.
"""

import hashlib

import pytest
from httpx import ASGITransport, AsyncClient
from bleepstore.metadata.sqlite import SQLiteMetadataStore
from bleepstore.storage.local import LocalStorageBackend
from bleepstore.server import create_app


# Use the session-scoped app from conftest but with function-scoped
# metadata store resets to avoid inter-test pollution.


@pytest.fixture
async def bucket_client(app, config, tmp_path):
    """Create a client with a fresh metadata store for bucket tests.

    Each test gets a fresh in-memory SQLite database to avoid state leaks.
    """
    metadata = SQLiteMetadataStore(":memory:")
    await metadata.init_db()

    # Seed credentials
    access_key = config.auth.access_key
    secret_key = config.auth.secret_key
    owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
    await metadata.put_credential(
        access_key_id=access_key,
        secret_key=secret_key,
        owner_id=owner_id,
        display_name=access_key,
    )

    # Replace the metadata store on app.state
    old_metadata = getattr(app.state, "metadata", None)
    app.state.metadata = metadata

    # Initialize a fresh storage backend
    old_storage = getattr(app.state, "storage", None)
    storage = LocalStorageBackend(str(tmp_path / "objects"))
    await storage.init()
    app.state.storage = storage

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac

    # Restore old state and close new ones
    app.state.metadata = old_metadata
    app.state.storage = old_storage
    await metadata.close()


class TestListBuckets:
    """Tests for GET / (ListBuckets)."""

    async def test_list_buckets_empty(self, bucket_client):
        """ListBuckets on empty store returns 200 with empty Buckets element."""
        resp = await bucket_client.get("/")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")
        body = resp.text
        assert "ListAllMyBucketsResult" in body
        assert "<Buckets>" in body
        assert "</Buckets>" in body
        assert "<Owner>" in body

    async def test_list_buckets_has_owner(self, bucket_client):
        """ListBuckets response includes Owner with ID and DisplayName."""
        resp = await bucket_client.get("/")
        body = resp.text
        assert "<Owner>" in body
        assert "<ID>" in body
        assert "<DisplayName>" in body

    async def test_list_buckets_with_buckets(self, bucket_client):
        """ListBuckets includes created buckets."""
        await bucket_client.put("/list-bucket-a")
        await bucket_client.put("/list-bucket-b")

        resp = await bucket_client.get("/")
        assert resp.status_code == 200
        body = resp.text
        assert "<Name>list-bucket-a</Name>" in body
        assert "<Name>list-bucket-b</Name>" in body
        assert "<CreationDate>" in body

    async def test_list_buckets_has_namespace(self, bucket_client):
        """ListBuckets XML includes proper S3 namespace."""
        resp = await bucket_client.get("/")
        body = resp.text
        assert 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"' in body

    async def test_list_buckets_common_headers(self, bucket_client):
        """ListBuckets has common S3 response headers."""
        resp = await bucket_client.get("/")
        assert "x-amz-request-id" in resp.headers
        assert "x-amz-id-2" in resp.headers
        assert resp.headers["server"] == "BleepStore"


class TestCreateBucket:
    """Tests for PUT /{bucket} (CreateBucket)."""

    async def test_create_bucket_success(self, bucket_client):
        """Create a bucket returns 200 with Location header."""
        resp = await bucket_client.put("/my-new-bucket")
        assert resp.status_code == 200
        assert resp.headers.get("location") == "/my-new-bucket"

    async def test_create_bucket_exists_in_head(self, bucket_client):
        """Created bucket is visible via HeadBucket."""
        await bucket_client.put("/visible-bucket")
        resp = await bucket_client.head("/visible-bucket")
        assert resp.status_code == 200

    async def test_create_bucket_idempotent(self, bucket_client):
        """Creating the same bucket twice returns 200 (us-east-1 behavior)."""
        resp1 = await bucket_client.put("/idempotent-bucket")
        assert resp1.status_code == 200

        resp2 = await bucket_client.put("/idempotent-bucket")
        assert resp2.status_code == 200

    async def test_create_bucket_invalid_name_uppercase(self, bucket_client):
        """Bucket names with uppercase characters are rejected."""
        resp = await bucket_client.put("/INVALID-UPPERCASE")
        assert resp.status_code == 400
        assert "InvalidBucketName" in resp.text

    async def test_create_bucket_invalid_name_too_short(self, bucket_client):
        """Bucket names shorter than 3 characters are rejected."""
        resp = await bucket_client.put("/ab")
        assert resp.status_code == 400
        assert "InvalidBucketName" in resp.text

    async def test_create_bucket_invalid_name_ip_address(self, bucket_client):
        """Bucket names that look like IP addresses are rejected."""
        resp = await bucket_client.put("/192.168.1.1")
        assert resp.status_code == 400
        assert "InvalidBucketName" in resp.text

    async def test_create_bucket_with_location_constraint(self, bucket_client):
        """CreateBucket with LocationConstraint body sets the region."""
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<LocationConstraint>us-west-2</LocationConstraint>"
            "</CreateBucketConfiguration>"
        )
        resp = await bucket_client.put(
            "/regional-bucket",
            content=body.encode(),
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 200

        # Verify location
        loc_resp = await bucket_client.get("/regional-bucket?location")
        assert "us-west-2" in loc_resp.text

    async def test_create_bucket_with_canned_acl(self, bucket_client):
        """CreateBucket with x-amz-acl header sets the ACL."""
        resp = await bucket_client.put(
            "/acl-bucket",
            headers={"x-amz-acl": "public-read"},
        )
        assert resp.status_code == 200

        # Verify ACL
        acl_resp = await bucket_client.get("/acl-bucket?acl")
        assert acl_resp.status_code == 200
        assert "READ" in acl_resp.text

    async def test_create_bucket_malformed_xml(self, bucket_client):
        """CreateBucket with malformed XML body returns MalformedXML."""
        resp = await bucket_client.put(
            "/bad-xml-bucket",
            content=b"<not-valid-xml",
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 400
        assert "MalformedXML" in resp.text


class TestDeleteBucket:
    """Tests for DELETE /{bucket} (DeleteBucket)."""

    async def test_delete_bucket_success(self, bucket_client):
        """Delete an empty bucket returns 204."""
        await bucket_client.put("/delete-me-bucket")
        resp = await bucket_client.delete("/delete-me-bucket")
        assert resp.status_code == 204

    async def test_delete_nonexistent_bucket(self, bucket_client):
        """Deleting a non-existent bucket returns 404 NoSuchBucket."""
        resp = await bucket_client.delete("/nonexistent-bucket-xyz")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_delete_bucket_not_empty(self, bucket_client):
        """Deleting a non-empty bucket returns 409 BucketNotEmpty."""
        await bucket_client.put("/notempty-bucket")

        # Add an object to the bucket via metadata store directly
        metadata = bucket_client._transport.app.state.metadata
        await metadata.put_object(
            bucket="notempty-bucket",
            key="test.txt",
            size=5,
            etag='"abc123"',
        )

        resp = await bucket_client.delete("/notempty-bucket")
        assert resp.status_code == 409
        assert "BucketNotEmpty" in resp.text

    async def test_delete_bucket_then_head_404(self, bucket_client):
        """After deleting a bucket, HEAD returns 404."""
        await bucket_client.put("/delete-check-bucket")
        await bucket_client.delete("/delete-check-bucket")
        resp = await bucket_client.head("/delete-check-bucket")
        assert resp.status_code == 404


class TestHeadBucket:
    """Tests for HEAD /{bucket} (HeadBucket)."""

    async def test_head_existing_bucket(self, bucket_client):
        """HEAD on existing bucket returns 200."""
        await bucket_client.put("/head-exists-bucket")
        resp = await bucket_client.head("/head-exists-bucket")
        assert resp.status_code == 200

    async def test_head_bucket_has_region_header(self, bucket_client):
        """HEAD includes x-amz-bucket-region header."""
        await bucket_client.put("/head-region-bucket")
        resp = await bucket_client.head("/head-region-bucket")
        assert resp.status_code == 200
        assert resp.headers.get("x-amz-bucket-region") == "us-east-1"

    async def test_head_nonexistent_bucket(self, bucket_client):
        """HEAD on non-existent bucket returns 404."""
        resp = await bucket_client.head("/nonexistent-head-bucket")
        assert resp.status_code == 404

    async def test_head_bucket_no_body(self, bucket_client):
        """HEAD responses have no body."""
        await bucket_client.put("/head-nobody-bucket")
        resp = await bucket_client.head("/head-nobody-bucket")
        assert resp.content == b""


class TestGetBucketLocation:
    """Tests for GET /{bucket}?location (GetBucketLocation)."""

    async def test_get_location_default_region(self, bucket_client):
        """us-east-1 returns empty LocationConstraint."""
        await bucket_client.put("/loc-default-bucket")
        resp = await bucket_client.get("/loc-default-bucket?location")
        assert resp.status_code == 200
        body = resp.text
        assert "LocationConstraint" in body
        # us-east-1 quirk: should be self-closing or empty
        assert "<LocationConstraint" in body

    async def test_get_location_custom_region(self, bucket_client):
        """Custom region returns region in LocationConstraint."""
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<LocationConstraint>eu-west-1</LocationConstraint>"
            "</CreateBucketConfiguration>"
        )
        await bucket_client.put(
            "/loc-custom-bucket",
            content=body.encode(),
            headers={"Content-Type": "application/xml"},
        )
        resp = await bucket_client.get("/loc-custom-bucket?location")
        assert resp.status_code == 200
        assert "eu-west-1" in resp.text

    async def test_get_location_nonexistent_bucket(self, bucket_client):
        """GetBucketLocation on non-existent bucket returns NoSuchBucket."""
        resp = await bucket_client.get("/nonexistent-loc-bucket?location")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_get_location_content_type(self, bucket_client):
        """GetBucketLocation returns application/xml."""
        await bucket_client.put("/loc-ct-bucket")
        resp = await bucket_client.get("/loc-ct-bucket?location")
        assert "application/xml" in resp.headers.get("content-type", "")

    async def test_get_location_has_xml_declaration(self, bucket_client):
        """GetBucketLocation has XML declaration."""
        await bucket_client.put("/loc-xmldecl-bucket")
        resp = await bucket_client.get("/loc-xmldecl-bucket?location")
        assert '<?xml version="1.0" encoding="UTF-8"?>' in resp.text


class TestGetBucketAcl:
    """Tests for GET /{bucket}?acl (GetBucketAcl)."""

    async def test_get_acl_default(self, bucket_client):
        """Default ACL has FULL_CONTROL grant for owner."""
        await bucket_client.put("/acl-default-bucket")
        resp = await bucket_client.get("/acl-default-bucket?acl")
        assert resp.status_code == 200
        body = resp.text
        assert "AccessControlPolicy" in body
        assert "FULL_CONTROL" in body

    async def test_get_acl_has_owner(self, bucket_client):
        """ACL response includes Owner element."""
        await bucket_client.put("/acl-owner-bucket")
        resp = await bucket_client.get("/acl-owner-bucket?acl")
        body = resp.text
        assert "<Owner>" in body
        assert "<ID>" in body
        assert "<DisplayName>" in body

    async def test_get_acl_has_grantee(self, bucket_client):
        """ACL response includes Grantee element."""
        await bucket_client.put("/acl-grantee-bucket")
        resp = await bucket_client.get("/acl-grantee-bucket?acl")
        body = resp.text
        assert "<Grantee" in body
        assert "<Grant>" in body
        assert "<Permission>" in body

    async def test_get_acl_nonexistent_bucket(self, bucket_client):
        """GetBucketAcl on non-existent bucket returns NoSuchBucket."""
        resp = await bucket_client.get("/nonexistent-acl-bucket?acl")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_get_acl_has_namespace(self, bucket_client):
        """ACL XML includes proper S3 namespace."""
        await bucket_client.put("/acl-ns-bucket")
        resp = await bucket_client.get("/acl-ns-bucket?acl")
        assert 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"' in resp.text

    async def test_get_acl_content_type(self, bucket_client):
        """ACL response has content-type application/xml."""
        await bucket_client.put("/acl-ct-bucket")
        resp = await bucket_client.get("/acl-ct-bucket?acl")
        assert "application/xml" in resp.headers.get("content-type", "")


class TestPutBucketAcl:
    """Tests for PUT /{bucket}?acl (PutBucketAcl)."""

    async def test_put_acl_canned_private(self, bucket_client):
        """Set canned ACL 'private' on bucket."""
        await bucket_client.put("/put-acl-private")
        resp = await bucket_client.put(
            "/put-acl-private?acl",
            headers={"x-amz-acl": "private"},
        )
        assert resp.status_code == 200

    async def test_put_acl_canned_public_read(self, bucket_client):
        """Set canned ACL 'public-read' and verify READ grant."""
        await bucket_client.put("/put-acl-public")
        resp = await bucket_client.put(
            "/put-acl-public?acl",
            headers={"x-amz-acl": "public-read"},
        )
        assert resp.status_code == 200

        # Verify
        acl_resp = await bucket_client.get("/put-acl-public?acl")
        body = acl_resp.text
        assert "READ" in body
        assert "AllUsers" in body

    async def test_put_acl_nonexistent_bucket(self, bucket_client):
        """PutBucketAcl on non-existent bucket returns NoSuchBucket."""
        resp = await bucket_client.put(
            "/nonexistent-put-acl?acl",
            headers={"x-amz-acl": "private"},
        )
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_put_acl_xml_body(self, bucket_client):
        """Set ACL via XML body."""
        await bucket_client.put("/put-acl-xml")
        acl_xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<Owner><ID>testowner</ID><DisplayName>testdisplay</DisplayName></Owner>"
            "<AccessControlList>"
            "<Grant>"
            '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">'
            "<ID>testowner</ID><DisplayName>testdisplay</DisplayName>"
            "</Grantee>"
            "<Permission>FULL_CONTROL</Permission>"
            "</Grant>"
            "</AccessControlList>"
            "</AccessControlPolicy>"
        )
        resp = await bucket_client.put(
            "/put-acl-xml?acl",
            content=acl_xml.encode(),
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 200

    async def test_put_acl_replaces_existing(self, bucket_client):
        """PutBucketAcl replaces the existing ACL."""
        await bucket_client.put("/put-acl-replace")

        # Set public-read
        await bucket_client.put(
            "/put-acl-replace?acl",
            headers={"x-amz-acl": "public-read"},
        )
        acl1 = await bucket_client.get("/put-acl-replace?acl")
        assert "AllUsers" in acl1.text

        # Replace with private
        await bucket_client.put(
            "/put-acl-replace?acl",
            headers={"x-amz-acl": "private"},
        )
        acl2 = await bucket_client.get("/put-acl-replace?acl")
        assert "AllUsers" not in acl2.text
        assert "FULL_CONTROL" in acl2.text
