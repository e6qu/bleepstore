"""Tests for multipart upload operations (Stage 7).

Tests the core multipart upload lifecycle:
    - CreateMultipartUpload
    - UploadPart
    - ListParts
    - ListMultipartUploads
    - AbortMultipartUpload

Uses a fresh metadata store and storage backend per test to avoid state
leaks between tests.
"""

import hashlib
import xml.etree.ElementTree as ET

import pytest
from httpx import ASGITransport, AsyncClient

from bleepstore.metadata.sqlite import SQLiteMetadataStore
from bleepstore.storage.local import LocalStorageBackend


@pytest.fixture
async def mp_client(app, config, tmp_path) -> AsyncClient:
    """Create an async test client with fresh metadata and storage per test.

    Each test gets a clean in-memory SQLite database and a fresh temp
    directory for storage, so multipart upload state does not leak between
    tests.
    """
    # Create fresh metadata store
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

    # Create fresh storage backend
    storage = LocalStorageBackend(str(tmp_path / "objects"))
    await storage.init()

    # Swap in fresh instances
    old_metadata = getattr(app.state, "metadata", None)
    old_storage = getattr(app.state, "storage", None)
    app.state.metadata = metadata
    app.state.storage = storage

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac

    # Restore previous state
    app.state.metadata = old_metadata
    app.state.storage = old_storage

    # Cleanup
    await metadata.close()


async def _create_bucket(client: AsyncClient, bucket: str = "test-bucket") -> None:
    """Helper to create a test bucket."""
    resp = await client.put(f"/{bucket}")
    assert resp.status_code == 200


async def _initiate_upload(
    client: AsyncClient, bucket: str, key: str
) -> str:
    """Helper to initiate a multipart upload and return the upload ID."""
    resp = await client.post(f"/{bucket}/{key}?uploads")
    assert resp.status_code == 200
    root = ET.fromstring(resp.text)
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    upload_id_elem = root.find(f"{ns}UploadId")
    assert upload_id_elem is not None
    upload_id = upload_id_elem.text
    assert upload_id is not None and len(upload_id) > 0
    return upload_id


# ---------------------------------------------------------------------------
# CreateMultipartUpload tests
# ---------------------------------------------------------------------------


class TestCreateMultipartUpload:
    """Tests for POST /{bucket}/{key}?uploads (CreateMultipartUpload)."""

    async def test_returns_200_with_xml(self, mp_client: AsyncClient) -> None:
        """CreateMultipartUpload returns 200 with InitiateMultipartUploadResult XML."""
        await _create_bucket(mp_client)
        resp = await mp_client.post("/test-bucket/my-key?uploads")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")

    async def test_xml_has_bucket_and_key(self, mp_client: AsyncClient) -> None:
        """The XML response contains Bucket, Key, and UploadId elements."""
        await _create_bucket(mp_client)
        resp = await mp_client.post("/test-bucket/my-key?uploads")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Bucket").text == "test-bucket"
        assert root.find(f"{ns}Key").text == "my-key"
        assert root.find(f"{ns}UploadId") is not None
        assert len(root.find(f"{ns}UploadId").text) > 0

    async def test_upload_id_is_uuid(self, mp_client: AsyncClient) -> None:
        """The returned UploadId looks like a UUID."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        # UUID format: 8-4-4-4-12 hex chars
        parts = upload_id.split("-")
        assert len(parts) == 5
        assert len(parts[0]) == 8
        assert len(parts[1]) == 4

    async def test_nosuchbucket(self, mp_client: AsyncClient) -> None:
        """CreateMultipartUpload on non-existent bucket returns 404."""
        resp = await mp_client.post("/no-such-bucket/my-key?uploads")
        assert resp.status_code == 404

    async def test_multiple_uploads_different_ids(self, mp_client: AsyncClient) -> None:
        """Multiple uploads for the same key get different upload IDs."""
        await _create_bucket(mp_client)
        id1 = await _initiate_upload(mp_client, "test-bucket", "my-key")
        id2 = await _initiate_upload(mp_client, "test-bucket", "my-key")
        assert id1 != id2

    async def test_has_namespace(self, mp_client: AsyncClient) -> None:
        """The XML root element has the S3 namespace."""
        await _create_bucket(mp_client)
        resp = await mp_client.post("/test-bucket/my-key?uploads")
        assert "http://s3.amazonaws.com/doc/2006-03-01/" in resp.text

    async def test_nested_key(self, mp_client: AsyncClient) -> None:
        """CreateMultipartUpload works with nested key paths."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "a/b/c/file.txt")
        assert len(upload_id) > 0


# ---------------------------------------------------------------------------
# UploadPart tests
# ---------------------------------------------------------------------------


class TestUploadPart:
    """Tests for PUT /{bucket}/{key}?partNumber&uploadId (UploadPart)."""

    async def test_returns_200_with_etag(self, mp_client: AsyncClient) -> None:
        """UploadPart returns 200 with an ETag header."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        data = b"hello world part 1"
        resp = await mp_client.put(
            f"/test-bucket/my-key?partNumber=1&uploadId={upload_id}",
            content=data,
        )
        assert resp.status_code == 200
        assert "etag" in resp.headers
        etag = resp.headers["etag"]
        assert etag.startswith('"') and etag.endswith('"')

    async def test_etag_is_md5(self, mp_client: AsyncClient) -> None:
        """The ETag is the quoted MD5 hex digest of the part data."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        data = b"test data for md5"
        expected_md5 = hashlib.md5(data).hexdigest()
        resp = await mp_client.put(
            f"/test-bucket/my-key?partNumber=1&uploadId={upload_id}",
            content=data,
        )
        assert resp.headers["etag"] == f'"{expected_md5}"'

    async def test_nosuchupload(self, mp_client: AsyncClient) -> None:
        """UploadPart with non-existent upload_id returns 404 NoSuchUpload."""
        await _create_bucket(mp_client)
        resp = await mp_client.put(
            "/test-bucket/my-key?partNumber=1&uploadId=fake-upload-id",
            content=b"data",
        )
        assert resp.status_code == 404
        assert "NoSuchUpload" in resp.text

    async def test_nosuchbucket(self, mp_client: AsyncClient) -> None:
        """UploadPart with non-existent bucket returns 404."""
        resp = await mp_client.put(
            "/no-such-bucket/my-key?partNumber=1&uploadId=fake-id",
            content=b"data",
        )
        assert resp.status_code == 404

    async def test_part_overwrite(self, mp_client: AsyncClient) -> None:
        """Uploading the same part number twice replaces the previous part."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Upload part 1 with data1
        data1 = b"first version"
        resp1 = await mp_client.put(
            f"/test-bucket/my-key?partNumber=1&uploadId={upload_id}",
            content=data1,
        )
        etag1 = resp1.headers["etag"]

        # Upload part 1 with data2 (overwrite)
        data2 = b"second version"
        resp2 = await mp_client.put(
            f"/test-bucket/my-key?partNumber=1&uploadId={upload_id}",
            content=data2,
        )
        etag2 = resp2.headers["etag"]

        # ETags should differ because data differs
        assert etag1 != etag2
        assert etag2 == f'"{hashlib.md5(data2).hexdigest()}"'

    async def test_multiple_parts(self, mp_client: AsyncClient) -> None:
        """Multiple parts can be uploaded for the same upload."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        for i in range(1, 4):
            data = f"part {i} data".encode()
            resp = await mp_client.put(
                f"/test-bucket/my-key?partNumber={i}&uploadId={upload_id}",
                content=data,
            )
            assert resp.status_code == 200

    async def test_invalid_part_number_zero(self, mp_client: AsyncClient) -> None:
        """UploadPart with part number 0 returns 400."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        resp = await mp_client.put(
            f"/test-bucket/my-key?partNumber=0&uploadId={upload_id}",
            content=b"data",
        )
        assert resp.status_code == 400

    async def test_invalid_part_number_too_large(self, mp_client: AsyncClient) -> None:
        """UploadPart with part number > 10000 returns 400."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        resp = await mp_client.put(
            f"/test-bucket/my-key?partNumber=10001&uploadId={upload_id}",
            content=b"data",
        )
        assert resp.status_code == 400

    async def test_part_stored_on_disk(self, mp_client: AsyncClient, tmp_path) -> None:
        """Uploaded part data exists on disk in .parts/{upload_id}/{part_number}."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        data = b"stored data"
        await mp_client.put(
            f"/test-bucket/my-key?partNumber=1&uploadId={upload_id}",
            content=data,
        )
        part_path = tmp_path / "objects" / ".parts" / upload_id / "1"
        assert part_path.exists()
        assert part_path.read_bytes() == data


# ---------------------------------------------------------------------------
# AbortMultipartUpload tests
# ---------------------------------------------------------------------------


class TestAbortMultipartUpload:
    """Tests for DELETE /{bucket}/{key}?uploadId (AbortMultipartUpload)."""

    async def test_returns_204(self, mp_client: AsyncClient) -> None:
        """AbortMultipartUpload returns 204 No Content."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        resp = await mp_client.delete(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        assert resp.status_code == 204

    async def test_nosuchupload(self, mp_client: AsyncClient) -> None:
        """Aborting a non-existent upload returns 404 NoSuchUpload."""
        await _create_bucket(mp_client)
        resp = await mp_client.delete(
            "/test-bucket/my-key?uploadId=fake-upload-id"
        )
        assert resp.status_code == 404
        assert "NoSuchUpload" in resp.text

    async def test_cleans_up_parts(self, mp_client: AsyncClient, tmp_path) -> None:
        """Aborting an upload removes part files from disk."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Upload a part
        await mp_client.put(
            f"/test-bucket/my-key?partNumber=1&uploadId={upload_id}",
            content=b"part data",
        )

        # Verify part exists on disk
        parts_dir = tmp_path / "objects" / ".parts" / upload_id
        assert parts_dir.exists()

        # Abort
        resp = await mp_client.delete(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        assert resp.status_code == 204

        # Part directory should be cleaned up
        assert not parts_dir.exists()

    async def test_cleans_up_metadata(self, mp_client: AsyncClient) -> None:
        """Aborting removes the upload and part records from metadata."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Upload parts
        for i in range(1, 3):
            await mp_client.put(
                f"/test-bucket/my-key?partNumber={i}&uploadId={upload_id}",
                content=f"part {i}".encode(),
            )

        # Abort
        await mp_client.delete(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )

        # Trying to list parts should fail with NoSuchUpload
        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        assert resp.status_code == 404
        assert "NoSuchUpload" in resp.text

    async def test_nosuchbucket(self, mp_client: AsyncClient) -> None:
        """Aborting an upload on a non-existent bucket returns 404."""
        resp = await mp_client.delete(
            "/no-such-bucket/my-key?uploadId=fake-id"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# ListParts tests
# ---------------------------------------------------------------------------


class TestListParts:
    """Tests for GET /{bucket}/{key}?uploadId (ListParts)."""

    async def test_returns_200_with_xml(self, mp_client: AsyncClient) -> None:
        """ListParts returns 200 with ListPartsResult XML."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")
        assert "ListPartsResult" in resp.text

    async def test_lists_uploaded_parts(self, mp_client: AsyncClient) -> None:
        """ListParts returns metadata for all uploaded parts."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Upload 3 parts
        for i in range(1, 4):
            data = f"part {i} content".encode()
            await mp_client.put(
                f"/test-bucket/my-key?partNumber={i}&uploadId={upload_id}",
                content=data,
            )

        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

        parts_elems = root.findall(f"{ns}Part")
        assert len(parts_elems) == 3

        # Check part numbers are 1, 2, 3
        part_nums = [int(p.find(f"{ns}PartNumber").text) for p in parts_elems]
        assert part_nums == [1, 2, 3]

    async def test_parts_have_etag_and_size(self, mp_client: AsyncClient) -> None:
        """Each Part element has ETag, Size, and LastModified."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        data = b"test data for size and etag"
        await mp_client.put(
            f"/test-bucket/my-key?partNumber=1&uploadId={upload_id}",
            content=data,
        )

        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

        part = root.find(f"{ns}Part")
        assert part is not None

        etag = part.find(f"{ns}ETag").text
        assert etag.startswith('"') and etag.endswith('"')

        size = int(part.find(f"{ns}Size").text)
        assert size == len(data)

        last_modified = part.find(f"{ns}LastModified").text
        assert last_modified is not None and "T" in last_modified

    async def test_nosuchupload(self, mp_client: AsyncClient) -> None:
        """ListParts for non-existent upload returns 404 NoSuchUpload."""
        await _create_bucket(mp_client)
        resp = await mp_client.get(
            "/test-bucket/my-key?uploadId=fake-upload-id"
        )
        assert resp.status_code == 404
        assert "NoSuchUpload" in resp.text

    async def test_has_bucket_key_upload_id(self, mp_client: AsyncClient) -> None:
        """ListParts XML contains Bucket, Key, and UploadId elements."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Bucket").text == "test-bucket"
        assert root.find(f"{ns}Key").text == "my-key"
        assert root.find(f"{ns}UploadId").text == upload_id

    async def test_empty_parts(self, mp_client: AsyncClient) -> None:
        """ListParts with no uploaded parts returns an empty list."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        parts_elems = root.findall(f"{ns}Part")
        assert len(parts_elems) == 0

    async def test_pagination_max_parts(self, mp_client: AsyncClient) -> None:
        """ListParts respects max-parts for pagination."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Upload 5 parts
        for i in range(1, 6):
            await mp_client.put(
                f"/test-bucket/my-key?partNumber={i}&uploadId={upload_id}",
                content=f"part {i}".encode(),
            )

        # Request only 2 parts
        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}&max-parts=2"
        )
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

        parts_elems = root.findall(f"{ns}Part")
        assert len(parts_elems) == 2
        assert root.find(f"{ns}IsTruncated").text == "true"

    async def test_has_namespace(self, mp_client: AsyncClient) -> None:
        """The ListParts XML has the S3 namespace."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        assert "http://s3.amazonaws.com/doc/2006-03-01/" in resp.text


# ---------------------------------------------------------------------------
# ListMultipartUploads tests
# ---------------------------------------------------------------------------


class TestListMultipartUploads:
    """Tests for GET /{bucket}?uploads (ListMultipartUploads)."""

    async def test_returns_200_with_xml(self, mp_client: AsyncClient) -> None:
        """ListMultipartUploads returns 200 with XML."""
        await _create_bucket(mp_client)
        resp = await mp_client.get("/test-bucket?uploads")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")
        assert "ListMultipartUploadsResult" in resp.text

    async def test_empty_uploads(self, mp_client: AsyncClient) -> None:
        """ListMultipartUploads with no uploads returns an empty list."""
        await _create_bucket(mp_client)
        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        uploads = root.findall(f"{ns}Upload")
        assert len(uploads) == 0

    async def test_lists_active_uploads(self, mp_client: AsyncClient) -> None:
        """ListMultipartUploads returns all in-progress uploads."""
        await _create_bucket(mp_client)

        # Create 3 uploads
        ids = []
        for i in range(3):
            upload_id = await _initiate_upload(mp_client, "test-bucket", f"key-{i}")
            ids.append(upload_id)

        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

        uploads = root.findall(f"{ns}Upload")
        assert len(uploads) == 3

        # Verify upload IDs are present
        found_ids = {u.find(f"{ns}UploadId").text for u in uploads}
        assert found_ids == set(ids)

    async def test_upload_has_key_and_initiated(self, mp_client: AsyncClient) -> None:
        """Each Upload element has Key, UploadId, and Initiated."""
        await _create_bucket(mp_client)
        await _initiate_upload(mp_client, "test-bucket", "my-key")

        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

        upload = root.find(f"{ns}Upload")
        assert upload is not None
        assert upload.find(f"{ns}Key").text == "my-key"
        assert upload.find(f"{ns}UploadId") is not None
        assert upload.find(f"{ns}Initiated") is not None

    async def test_aborted_upload_not_listed(self, mp_client: AsyncClient) -> None:
        """An aborted upload does not appear in the list."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Abort it
        await mp_client.delete(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )

        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        uploads = root.findall(f"{ns}Upload")
        assert len(uploads) == 0

    async def test_prefix_filter(self, mp_client: AsyncClient) -> None:
        """ListMultipartUploads with prefix filters by key prefix."""
        await _create_bucket(mp_client)
        await _initiate_upload(mp_client, "test-bucket", "logs/a.txt")
        await _initiate_upload(mp_client, "test-bucket", "logs/b.txt")
        await _initiate_upload(mp_client, "test-bucket", "data/c.txt")

        resp = await mp_client.get("/test-bucket?uploads&prefix=logs/")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        uploads = root.findall(f"{ns}Upload")
        assert len(uploads) == 2

    async def test_nosuchbucket(self, mp_client: AsyncClient) -> None:
        """ListMultipartUploads on non-existent bucket returns 404."""
        resp = await mp_client.get("/no-such-bucket?uploads")
        assert resp.status_code == 404

    async def test_has_bucket(self, mp_client: AsyncClient) -> None:
        """The XML contains the Bucket element."""
        await _create_bucket(mp_client)
        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Bucket").text == "test-bucket"

    async def test_has_namespace(self, mp_client: AsyncClient) -> None:
        """The ListMultipartUploadsResult XML has the S3 namespace."""
        await _create_bucket(mp_client)
        resp = await mp_client.get("/test-bucket?uploads")
        assert "http://s3.amazonaws.com/doc/2006-03-01/" in resp.text


# ---------------------------------------------------------------------------
# Full lifecycle tests
# ---------------------------------------------------------------------------


class TestMultipartLifecycle:
    """End-to-end lifecycle tests for multipart uploads."""

    async def test_create_upload_parts_list_abort(self, mp_client: AsyncClient, tmp_path) -> None:
        """Full lifecycle: create -> upload parts -> list parts -> abort."""
        await _create_bucket(mp_client)

        # Create
        upload_id = await _initiate_upload(mp_client, "test-bucket", "big-file.bin")

        # Upload 3 parts
        etags = []
        for i in range(1, 4):
            data = f"part {i} data content".encode()
            resp = await mp_client.put(
                f"/test-bucket/big-file.bin?partNumber={i}&uploadId={upload_id}",
                content=data,
            )
            assert resp.status_code == 200
            etags.append(resp.headers["etag"])

        # List parts
        resp = await mp_client.get(
            f"/test-bucket/big-file.bin?uploadId={upload_id}"
        )
        assert resp.status_code == 200
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        parts_elems = root.findall(f"{ns}Part")
        assert len(parts_elems) == 3

        # Verify ETags match
        for i, part_elem in enumerate(parts_elems):
            assert part_elem.find(f"{ns}ETag").text == etags[i]

        # Verify upload appears in list
        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        uploads = root.findall(f"{ns}Upload")
        assert len(uploads) == 1

        # Abort
        resp = await mp_client.delete(
            f"/test-bucket/big-file.bin?uploadId={upload_id}"
        )
        assert resp.status_code == 204

        # Verify parts cleaned up from disk
        parts_dir = tmp_path / "objects" / ".parts" / upload_id
        assert not parts_dir.exists()

        # Verify upload no longer listed
        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        uploads = root.findall(f"{ns}Upload")
        assert len(uploads) == 0

    async def test_parallel_uploads_for_same_key(self, mp_client: AsyncClient) -> None:
        """Multiple concurrent uploads for the same key can coexist."""
        await _create_bucket(mp_client)

        # Create two uploads for the same key
        id1 = await _initiate_upload(mp_client, "test-bucket", "same-key")
        id2 = await _initiate_upload(mp_client, "test-bucket", "same-key")

        # Upload parts to both
        await mp_client.put(
            f"/test-bucket/same-key?partNumber=1&uploadId={id1}",
            content=b"upload1-part1",
        )
        await mp_client.put(
            f"/test-bucket/same-key?partNumber=1&uploadId={id2}",
            content=b"upload2-part1",
        )

        # Both should appear in list
        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        uploads = root.findall(f"{ns}Upload")
        assert len(uploads) == 2

        # Abort one, the other should still be listed
        await mp_client.delete(
            f"/test-bucket/same-key?uploadId={id1}"
        )
        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        uploads = root.findall(f"{ns}Upload")
        assert len(uploads) == 1
        assert uploads[0].find(f"{ns}UploadId").text == id2

    async def test_common_headers_present(self, mp_client: AsyncClient) -> None:
        """All multipart responses include common S3 response headers."""
        await _create_bucket(mp_client)

        # Check headers on CreateMultipartUpload
        resp = await mp_client.post("/test-bucket/my-key?uploads")
        assert "x-amz-request-id" in resp.headers
        assert "x-amz-id-2" in resp.headers
        assert "date" in resp.headers
        assert resp.headers.get("server") == "BleepStore"
