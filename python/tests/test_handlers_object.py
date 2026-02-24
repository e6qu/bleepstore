"""Integration tests for object handlers (Stage 4: Basic Object CRUD).

These tests use a real SQLite metadata store (in-memory) and real
local storage backend (in tmp_path) and exercise the full request path
via httpx AsyncClient. Each test class creates its own fresh stores
to avoid state leaks between tests.
"""

import hashlib

import pytest
from httpx import ASGITransport, AsyncClient

from bleepstore.metadata.sqlite import SQLiteMetadataStore
from bleepstore.storage.local import LocalStorageBackend


@pytest.fixture
async def obj_client(app, config, tmp_path):
    """Create a client with fresh metadata and storage for object tests.

    Each test gets a fresh in-memory SQLite database and a fresh storage
    directory to avoid state leaks.
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


class TestPutObject:
    """Tests for PUT /{bucket}/{key} (PutObject)."""

    async def test_put_object_success(self, obj_client):
        """PutObject returns 200 with ETag header."""
        await obj_client.put("/put-bucket")
        resp = await obj_client.put(
            "/put-bucket/test.txt",
            content=b"hello world",
            headers={"Content-Type": "text/plain"},
        )
        assert resp.status_code == 200
        assert "etag" in resp.headers

    async def test_put_object_etag_is_quoted_md5(self, obj_client):
        """PutObject ETag is a quoted MD5 hex string."""
        data = b"hello world"
        expected_md5 = hashlib.md5(data).hexdigest()

        await obj_client.put("/etag-bucket")
        resp = await obj_client.put(
            "/etag-bucket/test.txt",
            content=data,
        )
        etag = resp.headers["etag"]
        assert etag == f'"{expected_md5}"'

    async def test_put_object_empty_body(self, obj_client):
        """PutObject with empty body returns 200."""
        await obj_client.put("/empty-bucket")
        resp = await obj_client.put(
            "/empty-bucket/empty.txt",
            content=b"",
        )
        assert resp.status_code == 200

    async def test_put_object_nosuchbucket(self, obj_client):
        """PutObject on non-existent bucket returns 404 NoSuchBucket."""
        resp = await obj_client.put(
            "/nonexistent-bucket/test.txt",
            content=b"data",
        )
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_put_object_overwrite(self, obj_client):
        """PutObject overwrites an existing object at the same key."""
        await obj_client.put("/overwrite-bucket")
        await obj_client.put("/overwrite-bucket/file.txt", content=b"original")

        resp = await obj_client.put("/overwrite-bucket/file.txt", content=b"updated")
        assert resp.status_code == 200

        get_resp = await obj_client.get("/overwrite-bucket/file.txt")
        assert get_resp.content == b"updated"

    async def test_put_object_with_nested_key(self, obj_client):
        """PutObject with slashes in key works correctly."""
        await obj_client.put("/nested-bucket")
        resp = await obj_client.put(
            "/nested-bucket/path/to/file.txt",
            content=b"nested",
        )
        assert resp.status_code == 200

        get_resp = await obj_client.get("/nested-bucket/path/to/file.txt")
        assert get_resp.content == b"nested"

    async def test_put_object_preserves_content_type(self, obj_client):
        """PutObject stores and returns the Content-Type header."""
        await obj_client.put("/ct-bucket")
        await obj_client.put(
            "/ct-bucket/image.png",
            content=b"\x89PNG...",
            headers={"Content-Type": "image/png"},
        )

        resp = await obj_client.get("/ct-bucket/image.png")
        assert resp.headers["content-type"] == "image/png"

    async def test_put_object_default_content_type(self, obj_client):
        """PutObject with no Content-Type defaults to application/octet-stream."""
        await obj_client.put("/default-ct-bucket")
        await obj_client.put(
            "/default-ct-bucket/blob",
            content=b"binary",
        )

        resp = await obj_client.head("/default-ct-bucket/blob")
        ct = resp.headers.get("content-type", "")
        assert "application/octet-stream" in ct

    async def test_put_object_user_metadata(self, obj_client):
        """PutObject stores and returns x-amz-meta-* headers."""
        await obj_client.put("/meta-bucket")
        await obj_client.put(
            "/meta-bucket/with-meta.txt",
            content=b"data",
            headers={
                "x-amz-meta-color": "blue",
                "x-amz-meta-count": "42",
            },
        )

        resp = await obj_client.head("/meta-bucket/with-meta.txt")
        assert resp.headers.get("x-amz-meta-color") == "blue"
        assert resp.headers.get("x-amz-meta-count") == "42"

    async def test_put_object_common_headers(self, obj_client):
        """PutObject response has common S3 headers."""
        await obj_client.put("/common-bucket")
        resp = await obj_client.put(
            "/common-bucket/test.txt",
            content=b"data",
        )
        assert "x-amz-request-id" in resp.headers
        assert "x-amz-id-2" in resp.headers
        assert resp.headers["server"] == "BleepStore"


class TestGetObject:
    """Tests for GET /{bucket}/{key} (GetObject)."""

    async def test_get_object_returns_body(self, obj_client):
        """GetObject returns the stored object body."""
        data = b"hello world"
        await obj_client.put("/get-bucket")
        await obj_client.put("/get-bucket/test.txt", content=data)

        resp = await obj_client.get("/get-bucket/test.txt")
        assert resp.status_code == 200
        assert resp.content == data

    async def test_get_object_has_etag(self, obj_client):
        """GetObject response includes the ETag header."""
        data = b"etag test"
        expected_md5 = hashlib.md5(data).hexdigest()

        await obj_client.put("/get-etag-bucket")
        await obj_client.put("/get-etag-bucket/test.txt", content=data)

        resp = await obj_client.get("/get-etag-bucket/test.txt")
        assert resp.headers["etag"] == f'"{expected_md5}"'

    async def test_get_object_has_content_length(self, obj_client):
        """GetObject response includes Content-Length header."""
        data = b"length test"
        await obj_client.put("/get-len-bucket")
        await obj_client.put("/get-len-bucket/test.txt", content=data)

        resp = await obj_client.get("/get-len-bucket/test.txt")
        assert resp.headers.get("content-length") == str(len(data))

    async def test_get_object_has_last_modified(self, obj_client):
        """GetObject response includes Last-Modified header."""
        await obj_client.put("/get-lm-bucket")
        await obj_client.put("/get-lm-bucket/test.txt", content=b"data")

        resp = await obj_client.get("/get-lm-bucket/test.txt")
        assert "last-modified" in resp.headers

    async def test_get_object_has_accept_ranges(self, obj_client):
        """GetObject response includes Accept-Ranges: bytes header."""
        await obj_client.put("/get-ar-bucket")
        await obj_client.put("/get-ar-bucket/test.txt", content=b"data")

        resp = await obj_client.get("/get-ar-bucket/test.txt")
        assert resp.headers.get("accept-ranges") == "bytes"

    async def test_get_object_nosuchkey(self, obj_client):
        """GetObject on non-existent key returns 404 NoSuchKey."""
        await obj_client.put("/get-nokey-bucket")
        resp = await obj_client.get("/get-nokey-bucket/nonexistent.txt")
        assert resp.status_code == 404
        assert "NoSuchKey" in resp.text

    async def test_get_object_nosuchbucket(self, obj_client):
        """GetObject on non-existent bucket returns 404 NoSuchBucket."""
        resp = await obj_client.get("/nonexistent-get-bucket/test.txt")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_get_object_empty(self, obj_client):
        """GetObject on zero-length object returns empty body."""
        await obj_client.put("/get-empty-bucket")
        await obj_client.put("/get-empty-bucket/empty.txt", content=b"")

        resp = await obj_client.get("/get-empty-bucket/empty.txt")
        assert resp.status_code == 200
        assert resp.content == b""

    async def test_get_object_with_content_type(self, obj_client):
        """GetObject returns the correct Content-Type."""
        await obj_client.put("/get-ct-bucket")
        await obj_client.put(
            "/get-ct-bucket/page.html",
            content=b"<html>test</html>",
            headers={"Content-Type": "text/html"},
        )

        resp = await obj_client.get("/get-ct-bucket/page.html")
        assert "text/html" in resp.headers.get("content-type", "")

    async def test_get_object_user_metadata(self, obj_client):
        """GetObject returns x-amz-meta-* headers."""
        await obj_client.put("/get-meta-bucket")
        await obj_client.put(
            "/get-meta-bucket/with-meta.txt",
            content=b"data",
            headers={"x-amz-meta-author": "alice"},
        )

        resp = await obj_client.get("/get-meta-bucket/with-meta.txt")
        assert resp.headers.get("x-amz-meta-author") == "alice"


class TestHeadObject:
    """Tests for HEAD /{bucket}/{key} (HeadObject)."""

    async def test_head_object_returns_200(self, obj_client):
        """HeadObject returns 200 for existing object."""
        await obj_client.put("/head-bucket")
        await obj_client.put("/head-bucket/test.txt", content=b"data")

        resp = await obj_client.head("/head-bucket/test.txt")
        assert resp.status_code == 200

    async def test_head_object_no_body(self, obj_client):
        """HeadObject response has no body."""
        await obj_client.put("/head-nb-bucket")
        await obj_client.put("/head-nb-bucket/test.txt", content=b"data")

        resp = await obj_client.head("/head-nb-bucket/test.txt")
        assert resp.content == b""

    async def test_head_object_has_etag(self, obj_client):
        """HeadObject response includes ETag header."""
        data = b"head etag"
        expected_md5 = hashlib.md5(data).hexdigest()

        await obj_client.put("/head-etag-bucket")
        await obj_client.put("/head-etag-bucket/test.txt", content=data)

        resp = await obj_client.head("/head-etag-bucket/test.txt")
        assert resp.headers["etag"] == f'"{expected_md5}"'

    async def test_head_object_has_content_length(self, obj_client):
        """HeadObject response includes Content-Length header."""
        data = b"head length"
        await obj_client.put("/head-len-bucket")
        await obj_client.put("/head-len-bucket/test.txt", content=data)

        resp = await obj_client.head("/head-len-bucket/test.txt")
        assert resp.headers.get("content-length") == str(len(data))

    async def test_head_object_has_content_type(self, obj_client):
        """HeadObject response includes Content-Type header."""
        await obj_client.put("/head-ct-bucket")
        await obj_client.put(
            "/head-ct-bucket/test.json",
            content=b"{}",
            headers={"Content-Type": "application/json"},
        )

        resp = await obj_client.head("/head-ct-bucket/test.json")
        assert "application/json" in resp.headers.get("content-type", "")

    async def test_head_object_has_accept_ranges(self, obj_client):
        """HeadObject response includes Accept-Ranges header."""
        await obj_client.put("/head-ar-bucket")
        await obj_client.put("/head-ar-bucket/test.txt", content=b"data")

        resp = await obj_client.head("/head-ar-bucket/test.txt")
        assert resp.headers.get("accept-ranges") == "bytes"

    async def test_head_object_nosuchkey(self, obj_client):
        """HeadObject on non-existent key returns 404 with no body."""
        await obj_client.put("/head-nokey-bucket")
        resp = await obj_client.head("/head-nokey-bucket/nonexistent.txt")
        assert resp.status_code == 404
        assert resp.content == b""

    async def test_head_object_nosuchbucket(self, obj_client):
        """HeadObject on non-existent bucket returns 404 with no body."""
        resp = await obj_client.head("/nonexistent-head-obj-bucket/test.txt")
        assert resp.status_code == 404
        assert resp.content == b""

    async def test_head_object_user_metadata(self, obj_client):
        """HeadObject returns x-amz-meta-* headers."""
        await obj_client.put("/head-meta-bucket")
        await obj_client.put(
            "/head-meta-bucket/test.txt",
            content=b"data",
            headers={"x-amz-meta-project": "bleepstore"},
        )

        resp = await obj_client.head("/head-meta-bucket/test.txt")
        assert resp.headers.get("x-amz-meta-project") == "bleepstore"


class TestDeleteObject:
    """Tests for DELETE /{bucket}/{key} (DeleteObject)."""

    async def test_delete_object_returns_204(self, obj_client):
        """DeleteObject returns 204."""
        await obj_client.put("/del-bucket")
        await obj_client.put("/del-bucket/test.txt", content=b"data")

        resp = await obj_client.delete("/del-bucket/test.txt")
        assert resp.status_code == 204

    async def test_delete_object_removes_from_get(self, obj_client):
        """After DeleteObject, GetObject returns 404 NoSuchKey."""
        await obj_client.put("/del-verify-bucket")
        await obj_client.put("/del-verify-bucket/test.txt", content=b"data")
        await obj_client.delete("/del-verify-bucket/test.txt")

        resp = await obj_client.get("/del-verify-bucket/test.txt")
        assert resp.status_code == 404
        assert "NoSuchKey" in resp.text

    async def test_delete_nonexistent_returns_204(self, obj_client):
        """DeleteObject on non-existent key returns 204 (idempotent)."""
        await obj_client.put("/del-idempotent-bucket")
        resp = await obj_client.delete("/del-idempotent-bucket/nonexistent.txt")
        assert resp.status_code == 204

    async def test_delete_nosuchbucket(self, obj_client):
        """DeleteObject on non-existent bucket returns 404 NoSuchBucket."""
        resp = await obj_client.delete("/nonexistent-del-bucket/test.txt")
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_delete_twice_idempotent(self, obj_client):
        """Deleting the same object twice is safe (both return 204)."""
        await obj_client.put("/del-twice-bucket")
        await obj_client.put("/del-twice-bucket/test.txt", content=b"data")

        resp1 = await obj_client.delete("/del-twice-bucket/test.txt")
        assert resp1.status_code == 204

        resp2 = await obj_client.delete("/del-twice-bucket/test.txt")
        assert resp2.status_code == 204


class TestObjectRoundTrip:
    """End-to-end round-trip tests combining multiple operations."""

    async def test_put_head_get_delete_round_trip(self, obj_client):
        """Full CRUD lifecycle: put, head, get, delete, verify 404."""
        data = b"round trip test data"
        await obj_client.put("/rt-bucket")

        # PUT
        put_resp = await obj_client.put("/rt-bucket/lifecycle.txt", content=data)
        assert put_resp.status_code == 200
        etag = put_resp.headers["etag"]

        # HEAD
        head_resp = await obj_client.head("/rt-bucket/lifecycle.txt")
        assert head_resp.status_code == 200
        assert head_resp.headers["etag"] == etag
        assert head_resp.headers.get("content-length") == str(len(data))

        # GET
        get_resp = await obj_client.get("/rt-bucket/lifecycle.txt")
        assert get_resp.status_code == 200
        assert get_resp.content == data
        assert get_resp.headers["etag"] == etag

        # DELETE
        del_resp = await obj_client.delete("/rt-bucket/lifecycle.txt")
        assert del_resp.status_code == 204

        # Verify 404
        get_resp2 = await obj_client.get("/rt-bucket/lifecycle.txt")
        assert get_resp2.status_code == 404

    async def test_multiple_objects_in_bucket(self, obj_client):
        """Multiple objects can coexist in the same bucket."""
        await obj_client.put("/multi-bucket")

        await obj_client.put("/multi-bucket/a.txt", content=b"aaa")
        await obj_client.put("/multi-bucket/b.txt", content=b"bbb")
        await obj_client.put("/multi-bucket/c.txt", content=b"ccc")

        a_resp = await obj_client.get("/multi-bucket/a.txt")
        b_resp = await obj_client.get("/multi-bucket/b.txt")
        c_resp = await obj_client.get("/multi-bucket/c.txt")

        assert a_resp.content == b"aaa"
        assert b_resp.content == b"bbb"
        assert c_resp.content == b"ccc"

    async def test_objects_in_different_buckets(self, obj_client):
        """Same key in different buckets holds different data."""
        await obj_client.put("/bucket-one")
        await obj_client.put("/bucket-two")

        await obj_client.put("/bucket-one/key.txt", content=b"from one")
        await obj_client.put("/bucket-two/key.txt", content=b"from two")

        resp1 = await obj_client.get("/bucket-one/key.txt")
        resp2 = await obj_client.get("/bucket-two/key.txt")

        assert resp1.content == b"from one"
        assert resp2.content == b"from two"

    async def test_delete_one_object_does_not_affect_others(self, obj_client):
        """Deleting one object leaves other objects untouched."""
        await obj_client.put("/del-partial-bucket")
        await obj_client.put("/del-partial-bucket/keep.txt", content=b"keep")
        await obj_client.put("/del-partial-bucket/remove.txt", content=b"remove")

        await obj_client.delete("/del-partial-bucket/remove.txt")

        resp = await obj_client.get("/del-partial-bucket/keep.txt")
        assert resp.status_code == 200
        assert resp.content == b"keep"

        resp2 = await obj_client.get("/del-partial-bucket/remove.txt")
        assert resp2.status_code == 404
