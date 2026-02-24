"""Tests for the BleepStore FastAPI server."""


class TestHealthCheck:
    """Tests for the /health endpoint."""

    async def test_health_returns_200(self, client):
        """GET /health returns 200."""
        resp = await client.get("/health")
        assert resp.status_code == 200

    async def test_health_returns_json(self, client):
        """GET /health returns JSON with status ok."""
        resp = await client.get("/health")
        data = resp.json()
        assert data == {"status": "ok"}

    async def test_health_has_common_headers(self, client):
        """GET /health includes common S3 headers."""
        resp = await client.get("/health")
        assert "x-amz-request-id" in resp.headers
        assert len(resp.headers["x-amz-request-id"]) == 16
        assert "x-amz-id-2" in resp.headers
        assert "date" in resp.headers
        assert resp.headers["server"] == "BleepStore"


class TestCommonHeaders:
    """Tests that common headers are present on all responses."""

    async def test_request_id_is_hex(self, client):
        """x-amz-request-id is a 16-char uppercase hex string."""
        resp = await client.get("/health")
        req_id = resp.headers["x-amz-request-id"]
        assert len(req_id) == 16
        # Should be valid hex
        int(req_id, 16)

    async def test_headers_on_error_response(self, client):
        """Common headers present on error responses too."""
        # Object GET on non-existent bucket returns NoSuchBucket (404)
        resp = await client.get("/nonexistent-svr-bucket/some-key")
        assert resp.status_code == 404
        assert "x-amz-request-id" in resp.headers
        assert "x-amz-id-2" in resp.headers
        assert "date" in resp.headers
        assert resp.headers["server"] == "BleepStore"


class TestStubRoutes:
    """Tests that non-implemented S3 routes still return 501 with proper error XML.

    Bucket operations (GET /, PUT/DELETE/HEAD/GET /{bucket}) are implemented
    in Stage 3. Object CRUD (PUT/GET/HEAD/DELETE /{bucket}/{key}) is
    implemented in Stage 4. Remaining operations return 501.
    """

    async def test_put_object_nosuchbucket(self, client):
        """PUT /{bucket}/{key} on non-existent bucket returns 404 NoSuchBucket."""
        resp = await client.put("/nonexistent-stub-bucket/test-key")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_get_object_nosuchbucket(self, client):
        """GET /{bucket}/{key} on non-existent bucket returns 404 NoSuchBucket."""
        resp = await client.get("/nonexistent-stub-bucket/test-key")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_head_object_nosuchbucket(self, client):
        """HEAD /{bucket}/{key} on non-existent bucket returns 404 with no body."""
        resp = await client.head("/nonexistent-stub-bucket/test-key")
        assert resp.status_code == 404
        assert resp.content == b""

    async def test_delete_object_nosuchbucket(self, client):
        """DELETE /{bucket}/{key} on non-existent bucket returns 404 NoSuchBucket."""
        resp = await client.delete("/nonexistent-stub-bucket/test-key")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_post_bucket_delete_nosuchbucket(self, client):
        """POST /{bucket}?delete on non-existent bucket returns 404."""
        resp = await client.post("/test-bucket?delete")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_post_object_uploads_nosuchbucket(self, client):
        """POST /{bucket}/{key}?uploads on non-existent bucket returns 404."""
        resp = await client.post("/nonexistent-mp-bucket/test-key?uploads")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_post_object_upload_id_nosuchbucket(self, client):
        """POST /{bucket}/{key}?uploadId=abc on non-existent bucket returns 404."""
        resp = await client.post("/nonexistent-cmp-bucket/test-key?uploadId=abc")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_put_object_upload_part_nosuchbucket(self, client):
        """PUT /{bucket}/{key}?partNumber=1&uploadId=abc on non-existent bucket returns 404."""
        resp = await client.put("/nonexistent-mp-bucket/test-key?partNumber=1&uploadId=abc")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_delete_object_abort_upload_nosuchbucket(self, client):
        """DELETE /{bucket}/{key}?uploadId=abc on non-existent bucket returns 404."""
        resp = await client.delete("/nonexistent-mp-bucket/test-key?uploadId=abc")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_get_object_acl_nosuchbucket(self, client):
        """GET /{bucket}/{key}?acl on non-existent bucket returns 404."""
        resp = await client.get("/nonexistent-acl-svr-bucket/test-key?acl")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_put_object_acl_nosuchbucket(self, client):
        """PUT /{bucket}/{key}?acl on non-existent bucket returns 404."""
        resp = await client.put("/nonexistent-acl-svr-bucket/test-key?acl")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_get_object_list_parts_nosuchbucket(self, client):
        """GET /{bucket}/{key}?uploadId=abc on non-existent bucket returns 404."""
        resp = await client.get("/nonexistent-mp-bucket/test-key?uploadId=abc")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_list_uploads_returns_xml(self, client):
        """GET /{bucket}?uploads returns 200 with ListMultipartUploadsResult XML."""
        # First create the bucket
        await client.put("/test-stub-mp-bucket")
        resp = await client.get("/test-stub-mp-bucket?uploads")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")
        assert "ListMultipartUploadsResult" in resp.text

    async def test_list_objects_returns_xml(self, client):
        """GET /{bucket} returns 200 with ListBucketResult XML."""
        # First create the bucket
        await client.put("/test-listobj-bucket")
        resp = await client.get("/test-listobj-bucket")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")
        assert "ListBucketResult" in resp.text


class TestBucketRoutesWired:
    """Tests that bucket routes are now wired and return proper responses."""

    async def test_list_buckets_returns_200(self, client):
        """GET / returns 200 with XML."""
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")
        assert "ListAllMyBucketsResult" in resp.text

    async def test_create_bucket_returns_200(self, client):
        """PUT /{bucket} returns 200 for valid bucket name."""
        resp = await client.put("/my-test-bucket")
        assert resp.status_code == 200

    async def test_head_bucket_returns_200_when_exists(self, client):
        """HEAD /{bucket} returns 200 when bucket exists."""
        await client.put("/head-test-bucket")
        resp = await client.head("/head-test-bucket")
        assert resp.status_code == 200

    async def test_head_bucket_returns_404_when_missing(self, client):
        """HEAD /{bucket} returns 404 when bucket does not exist."""
        resp = await client.head("/nonexistent-bucket-xyz123")
        assert resp.status_code == 404

    async def test_get_bucket_location_returns_xml(self, client):
        """GET /{bucket}?location returns XML."""
        await client.put("/loc-test-bucket")
        resp = await client.get("/loc-test-bucket?location")
        assert resp.status_code == 200
        assert "LocationConstraint" in resp.text

    async def test_get_bucket_acl_returns_xml(self, client):
        """GET /{bucket}?acl returns XML."""
        await client.put("/acl-test-bucket")
        resp = await client.get("/acl-test-bucket?acl")
        assert resp.status_code == 200
        assert "AccessControlPolicy" in resp.text

    async def test_delete_bucket_returns_204(self, client):
        """DELETE /{bucket} returns 204 for empty bucket."""
        await client.put("/del-test-bucket")
        resp = await client.delete("/del-test-bucket")
        assert resp.status_code == 204


class TestObjectRoutesWired:
    """Tests that basic object CRUD routes are wired (Stage 4)."""

    async def test_put_get_object_round_trip(self, client):
        """PUT then GET object returns the same data."""
        await client.put("/obj-rt-bucket")
        put_resp = await client.put(
            "/obj-rt-bucket/test.txt",
            content=b"hello world",
            headers={"Content-Type": "text/plain"},
        )
        assert put_resp.status_code == 200
        assert "etag" in put_resp.headers

        get_resp = await client.get("/obj-rt-bucket/test.txt")
        assert get_resp.status_code == 200
        assert get_resp.content == b"hello world"

    async def test_head_object(self, client):
        """HEAD returns metadata with no body."""
        await client.put("/obj-head-bucket")
        await client.put(
            "/obj-head-bucket/test.txt",
            content=b"data",
        )
        resp = await client.head("/obj-head-bucket/test.txt")
        assert resp.status_code == 200
        assert resp.content == b""
        assert "etag" in resp.headers

    async def test_delete_object(self, client):
        """DELETE returns 204."""
        await client.put("/obj-del-bucket")
        await client.put("/obj-del-bucket/test.txt", content=b"data")
        resp = await client.delete("/obj-del-bucket/test.txt")
        assert resp.status_code == 204

    async def test_get_nonexistent_object(self, client):
        """GET non-existent object returns 404 NoSuchKey."""
        await client.put("/obj-nokey-bucket")
        resp = await client.get("/obj-nokey-bucket/nonexistent.txt")
        assert resp.status_code == 404
        assert "NoSuchKey" in resp.text


class TestErrorXMLFormat:
    """Tests for the S3 error XML format."""

    async def test_error_xml_structure(self, client):
        """Error XML has correct structure."""
        # Use a still-501 route (POST /{bucket} without ?delete)
        await client.put("/errxml-bucket")
        resp = await client.post("/errxml-bucket")
        body = resp.text
        assert '<?xml version="1.0" encoding="UTF-8"?>' in body
        assert "<Error>" in body
        assert "</Error>" in body
        assert "<Code>NotImplemented</Code>" in body
        assert "<Message>" in body

    async def test_error_xml_has_request_id(self, client):
        """Error XML RequestId matches the x-amz-request-id header."""
        await client.put("/errxml-reqid-bucket")
        resp = await client.post("/errxml-reqid-bucket")
        body = resp.text
        req_id = resp.headers["x-amz-request-id"]
        assert f"<RequestId>{req_id}</RequestId>" in body

    async def test_error_content_type_is_xml(self, client):
        """Error responses have content-type application/xml."""
        await client.put("/errxml-ct-bucket")
        resp = await client.post("/errxml-ct-bucket")
        assert "application/xml" in resp.headers.get("content-type", "")

    async def test_nested_key_path(self, client):
        """Object keys with slashes route correctly."""
        # Non-existent bucket -> 404 NoSuchBucket
        resp = await client.get("/nonexistent-nested/path/to/key.txt")
        assert resp.status_code == 404
        body = resp.text
        assert "<Code>NoSuchBucket</Code>" in body
