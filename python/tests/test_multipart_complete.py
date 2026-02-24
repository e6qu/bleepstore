"""Tests for multipart upload completion (Stage 8).

Tests the CompleteMultipartUpload handler including:
    - Full lifecycle: create -> upload parts -> complete -> verify object
    - Composite ETag format validation
    - Error cases: InvalidPartOrder, InvalidPart, EntityTooSmall, MalformedXML
    - Part file cleanup after completion
    - UploadPartCopy functionality

Uses a fresh metadata store and storage backend per test to avoid state
leaks between tests.
"""

import binascii
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


NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

# Minimum part size for non-last parts (5 MiB)
MIN_PART_SIZE = 5 * 1024 * 1024


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
    upload_id_elem = root.find(f"{NS}UploadId")
    assert upload_id_elem is not None
    upload_id = upload_id_elem.text
    assert upload_id is not None and len(upload_id) > 0
    return upload_id


async def _upload_part(
    client: AsyncClient, bucket: str, key: str,
    upload_id: str, part_number: int, data: bytes,
) -> str:
    """Helper to upload a part and return the quoted ETag."""
    resp = await client.put(
        f"/{bucket}/{key}?partNumber={part_number}&uploadId={upload_id}",
        content=data,
    )
    assert resp.status_code == 200
    return resp.headers["etag"]


def _build_complete_xml(parts: list[tuple[int, str]]) -> str:
    """Build the CompleteMultipartUpload XML body.

    Args:
        parts: List of (part_number, etag) tuples.

    Returns:
        XML string for the request body.
    """
    xml_parts = ['<CompleteMultipartUpload>']
    for pn, etag in parts:
        xml_parts.append(f"<Part><PartNumber>{pn}</PartNumber><ETag>{etag}</ETag></Part>")
    xml_parts.append("</CompleteMultipartUpload>")
    return "".join(xml_parts)


def _compute_expected_composite_etag(etags: list[str]) -> str:
    """Compute the expected composite ETag from part ETags.

    Args:
        etags: List of quoted ETag strings.

    Returns:
        The expected composite ETag string.
    """
    binary_md5s = b""
    for etag in etags:
        clean = etag.strip('"')
        binary_md5s += binascii.unhexlify(clean)
    final_md5 = hashlib.md5(binary_md5s).hexdigest()
    return f'"{final_md5}-{len(etags)}"'


# ---------------------------------------------------------------------------
# CompleteMultipartUpload basic tests
# ---------------------------------------------------------------------------


class TestCompleteMultipartUpload:
    """Tests for POST /{bucket}/{key}?uploadId (CompleteMultipartUpload)."""

    async def test_returns_200_with_xml(self, mp_client: AsyncClient) -> None:
        """CompleteMultipartUpload returns 200 with CompleteMultipartUploadResult XML."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, b"hello world")

        xml_body = _build_complete_xml([(1, etag)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")
        assert "CompleteMultipartUploadResult" in resp.text

    async def test_xml_has_bucket_key_etag(self, mp_client: AsyncClient) -> None:
        """The XML response contains Bucket, Key, ETag, and Location elements."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, b"data")

        xml_body = _build_complete_xml([(1, etag)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        root = ET.fromstring(resp.text)
        assert root.find(f"{NS}Bucket").text == "test-bucket"
        assert root.find(f"{NS}Key").text == "my-key"
        assert root.find(f"{NS}ETag") is not None
        assert root.find(f"{NS}Location") is not None

    async def test_has_namespace(self, mp_client: AsyncClient) -> None:
        """The XML root element has the S3 namespace."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, b"data")

        xml_body = _build_complete_xml([(1, etag)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert "http://s3.amazonaws.com/doc/2006-03-01/" in resp.text

    async def test_nosuchbucket(self, mp_client: AsyncClient) -> None:
        """CompleteMultipartUpload on non-existent bucket returns 404."""
        resp = await mp_client.post(
            "/no-such-bucket/my-key?uploadId=fake-id",
            content=b"<CompleteMultipartUpload></CompleteMultipartUpload>",
        )
        assert resp.status_code == 404
        assert "NoSuchBucket" in resp.text

    async def test_nosuchupload(self, mp_client: AsyncClient) -> None:
        """CompleteMultipartUpload with non-existent upload returns 404."""
        await _create_bucket(mp_client)
        resp = await mp_client.post(
            "/test-bucket/my-key?uploadId=fake-upload-id",
            content=b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>abc</ETag></Part></CompleteMultipartUpload>",
        )
        assert resp.status_code == 404
        assert "NoSuchUpload" in resp.text


# ---------------------------------------------------------------------------
# Composite ETag tests
# ---------------------------------------------------------------------------


class TestCompositeETag:
    """Tests for composite ETag format and computation."""

    async def test_single_part_composite_etag(self, mp_client: AsyncClient) -> None:
        """Composite ETag for a single part has the correct format."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        data = b"single part data"
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, data)

        xml_body = _build_complete_xml([(1, etag)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        root = ET.fromstring(resp.text)
        result_etag = root.find(f"{NS}ETag").text

        # Composite ETag format: "md5hex-1"
        assert result_etag.startswith('"')
        assert result_etag.endswith('"')
        inner = result_etag.strip('"')
        assert "-1" in inner
        parts = inner.split("-")
        assert len(parts) == 2
        assert len(parts[0]) == 32  # MD5 hex is 32 chars
        assert parts[1] == "1"

    async def test_multi_part_composite_etag(self, mp_client: AsyncClient) -> None:
        """Composite ETag for multiple parts matches expected computation."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        etags = []
        for i in range(1, 4):
            # Non-last parts must be >= 5 MiB
            if i < 3:
                data = (f"part {i} ").encode().ljust(MIN_PART_SIZE, b"x")
            else:
                data = f"part {i} last".encode()
            etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, i, data)
            etags.append(etag)

        expected = _compute_expected_composite_etag(etags)

        xml_body = _build_complete_xml([(i + 1, etags[i]) for i in range(3)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        root = ET.fromstring(resp.text)
        result_etag = root.find(f"{NS}ETag").text
        assert result_etag == expected

    async def test_composite_etag_has_part_count(self, mp_client: AsyncClient) -> None:
        """The composite ETag suffix is the number of parts."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        etags = []
        for i in range(1, 6):
            # Non-last parts must be >= 5 MiB
            if i < 5:
                data = (f"part {i} ").encode().ljust(MIN_PART_SIZE, b"x")
            else:
                data = f"part {i} last".encode()
            etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, i, data)
            etags.append(etag)

        xml_body = _build_complete_xml([(i + 1, etags[i]) for i in range(5)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        root = ET.fromstring(resp.text)
        result_etag = root.find(f"{NS}ETag").text
        assert result_etag.strip('"').endswith("-5")


# ---------------------------------------------------------------------------
# Object verification after completion
# ---------------------------------------------------------------------------


class TestCompletedObject:
    """Tests that verify the assembled object is accessible after completion."""

    async def test_object_accessible_after_completion(self, mp_client: AsyncClient) -> None:
        """The assembled object can be retrieved via GET after completion."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Part 1 must be >= 5 MiB (non-last)
        data1 = b"part one data".ljust(MIN_PART_SIZE, b"A")
        data2 = b"part two data"
        etag1 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, data1)
        etag2 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 2, data2)

        xml_body = _build_complete_xml([(1, etag1), (2, etag2)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 200

        # GET the assembled object
        get_resp = await mp_client.get("/test-bucket/my-key")
        assert get_resp.status_code == 200
        assert get_resp.content == data1 + data2

    async def test_object_size_correct(self, mp_client: AsyncClient) -> None:
        """HeadObject returns the correct total size after completion."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Part 1 must be >= 5 MiB (non-last)
        data1 = b"A" * MIN_PART_SIZE
        data2 = b"B" * 200
        etag1 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, data1)
        etag2 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 2, data2)

        xml_body = _build_complete_xml([(1, etag1), (2, etag2)])
        await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )

        head_resp = await mp_client.head("/test-bucket/my-key")
        assert head_resp.status_code == 200
        assert int(head_resp.headers["content-length"]) == MIN_PART_SIZE + 200

    async def test_object_etag_is_composite(self, mp_client: AsyncClient) -> None:
        """HeadObject returns the composite ETag after multipart completion."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        data = b"some data"
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, data)

        xml_body = _build_complete_xml([(1, etag)])
        await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )

        head_resp = await mp_client.head("/test-bucket/my-key")
        head_etag = head_resp.headers["etag"]
        # Composite ETag has a dash
        assert "-" in head_etag.strip('"')

    async def test_content_type_preserved(self, mp_client: AsyncClient) -> None:
        """Content-Type from CreateMultipartUpload is preserved on the object."""
        await _create_bucket(mp_client)

        # Create upload with custom content type
        resp = await mp_client.post(
            "/test-bucket/my-key?uploads",
            headers={"Content-Type": "image/png"},
        )
        root = ET.fromstring(resp.text)
        upload_id = root.find(f"{NS}UploadId").text

        data = b"\x89PNG\r\n\x1a\n"
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, data)

        xml_body = _build_complete_xml([(1, etag)])
        await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )

        head_resp = await mp_client.head("/test-bucket/my-key")
        assert head_resp.headers.get("content-type") == "image/png"

    async def test_nested_key_after_completion(self, mp_client: AsyncClient) -> None:
        """Completion works for nested key paths."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "a/b/c/file.txt")

        data = b"nested file data"
        etag = await _upload_part(mp_client, "test-bucket", "a/b/c/file.txt", upload_id, 1, data)

        xml_body = _build_complete_xml([(1, etag)])
        resp = await mp_client.post(
            f"/test-bucket/a/b/c/file.txt?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 200

        get_resp = await mp_client.get("/test-bucket/a/b/c/file.txt")
        assert get_resp.status_code == 200
        assert get_resp.content == data


# ---------------------------------------------------------------------------
# Part cleanup tests
# ---------------------------------------------------------------------------


class TestPartCleanup:
    """Tests that part files are cleaned up after completion."""

    async def test_part_files_removed_after_completion(self, mp_client: AsyncClient, tmp_path) -> None:
        """Part files are deleted from disk after successful completion."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        data = b"part data"
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, data)

        # Verify part exists before completion
        parts_dir = tmp_path / "objects" / ".parts" / upload_id
        assert parts_dir.exists()
        assert (parts_dir / "1").exists()

        # Complete
        xml_body = _build_complete_xml([(1, etag)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 200

        # Parts directory should be cleaned up
        assert not parts_dir.exists()

    async def test_upload_metadata_removed_after_completion(self, mp_client: AsyncClient) -> None:
        """Upload and part metadata records are deleted after completion."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        data = b"part data"
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, data)

        # Complete
        xml_body = _build_complete_xml([(1, etag)])
        await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )

        # Upload should no longer be listed
        resp = await mp_client.get("/test-bucket?uploads")
        root = ET.fromstring(resp.text)
        uploads = root.findall(f"{NS}Upload")
        assert len(uploads) == 0

        # Trying to list parts should fail
        resp = await mp_client.get(
            f"/test-bucket/my-key?uploadId={upload_id}"
        )
        assert resp.status_code == 404
        assert "NoSuchUpload" in resp.text


# ---------------------------------------------------------------------------
# Error case tests
# ---------------------------------------------------------------------------


class TestCompleteMultipartErrors:
    """Tests for error conditions in CompleteMultipartUpload."""

    async def test_invalid_part_order(self, mp_client: AsyncClient) -> None:
        """Parts listed in non-ascending order return InvalidPartOrder."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        etag1 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, b"part1")
        etag2 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 2, b"part2")

        # Submit parts in wrong order (2, 1)
        xml_body = _build_complete_xml([(2, etag2), (1, etag1)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 400
        assert "InvalidPartOrder" in resp.text

    async def test_duplicate_part_number(self, mp_client: AsyncClient) -> None:
        """Duplicate part numbers return InvalidPartOrder."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        etag1 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, b"part1")

        # Submit same part number twice
        xml_body = _build_complete_xml([(1, etag1), (1, etag1)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 400
        assert "InvalidPartOrder" in resp.text

    async def test_invalid_part_etag_mismatch(self, mp_client: AsyncClient) -> None:
        """An ETag that doesn't match the stored part returns InvalidPart."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, b"real data")

        # Submit with a fake ETag
        xml_body = _build_complete_xml([(1, '"0000000000000000000000000000dead"')])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 400
        assert "InvalidPart" in resp.text

    async def test_invalid_part_not_uploaded(self, mp_client: AsyncClient) -> None:
        """Referencing a part that was never uploaded returns InvalidPart."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        etag1 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, b"part1")

        # Reference part 2 which was never uploaded
        xml_body = _build_complete_xml([(1, etag1), (2, '"fakeetag1234567890abcdef12345678"')])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 400
        assert "InvalidPart" in resp.text

    async def test_entity_too_small(self, mp_client: AsyncClient) -> None:
        """A non-last part smaller than 5 MiB returns EntityTooSmall."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Upload two small parts (both < 5 MiB)
        small_data = b"x" * 100  # 100 bytes, way under 5 MiB
        etag1 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, small_data)
        etag2 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 2, small_data)

        # Part 1 (not last) is too small
        xml_body = _build_complete_xml([(1, etag1), (2, etag2)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 400
        assert "EntityTooSmall" in resp.text

    async def test_single_small_part_ok(self, mp_client: AsyncClient) -> None:
        """A single small part (the only and last part) is allowed."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        small_data = b"tiny"
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, small_data)

        xml_body = _build_complete_xml([(1, etag)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 200

    async def test_last_part_can_be_small(self, mp_client: AsyncClient) -> None:
        """The last part can be smaller than 5 MiB when previous parts are >= 5 MiB."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Part 1: exactly 5 MiB
        big_data = b"x" * (5 * 1024 * 1024)
        etag1 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, big_data)

        # Part 2: tiny (last part)
        small_data = b"tiny last part"
        etag2 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 2, small_data)

        xml_body = _build_complete_xml([(1, etag1), (2, etag2)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 200

        # Verify assembled object
        get_resp = await mp_client.get("/test-bucket/my-key")
        assert get_resp.status_code == 200
        assert get_resp.content == big_data + small_data

    async def test_malformed_xml(self, mp_client: AsyncClient) -> None:
        """Non-XML body returns MalformedXML."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=b"not xml at all",
        )
        assert resp.status_code == 400
        assert "MalformedXML" in resp.text

    async def test_empty_parts_list(self, mp_client: AsyncClient) -> None:
        """XML with no Part elements returns MalformedXML."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=b"<CompleteMultipartUpload></CompleteMultipartUpload>",
        )
        assert resp.status_code == 400
        assert "MalformedXML" in resp.text

    async def test_subset_of_uploaded_parts(self, mp_client: AsyncClient) -> None:
        """Completing with a subset of uploaded parts succeeds (only listed parts are assembled)."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")

        # Part 1 is not the last in the completion list, so must be >= 5 MiB
        data1 = b"part one".ljust(MIN_PART_SIZE, b"X")
        data2 = b"part two"
        data3 = b"part three"
        etag1 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, data1)
        await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 2, data2)
        etag3 = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 3, data3)

        # Complete with only parts 1 and 3 (skip part 2)
        xml_body = _build_complete_xml([(1, etag1), (3, etag3)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 200

        # Object should be part1 + part3 (no part2)
        get_resp = await mp_client.get("/test-bucket/my-key")
        assert get_resp.content == data1 + data3


# ---------------------------------------------------------------------------
# Full lifecycle test
# ---------------------------------------------------------------------------


class TestMultipartCompleteLifecycle:
    """Full lifecycle tests including completion."""

    async def test_create_upload_complete_get(self, mp_client: AsyncClient, tmp_path) -> None:
        """Full lifecycle: create -> upload 3 parts -> complete -> GET assembled."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "big-file.bin")

        # Upload 3 parts (non-last parts must be >= 5 MiB)
        parts_data = [
            b"part 1 data content".ljust(MIN_PART_SIZE, b"A"),
            b"part 2 data content".ljust(MIN_PART_SIZE, b"B"),
            b"part 3 data content",  # last part can be small
        ]
        etags = []
        for i, data in enumerate(parts_data, start=1):
            etag = await _upload_part(mp_client, "test-bucket", "big-file.bin", upload_id, i, data)
            etags.append(etag)

        # Verify parts exist on disk
        parts_dir = tmp_path / "objects" / ".parts" / upload_id
        assert parts_dir.exists()

        # Complete
        xml_body = _build_complete_xml([(i + 1, etags[i]) for i in range(3)])
        resp = await mp_client.post(
            f"/test-bucket/big-file.bin?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert resp.status_code == 200

        # Verify composite ETag
        root = ET.fromstring(resp.text)
        result_etag = root.find(f"{NS}ETag").text
        expected_etag = _compute_expected_composite_etag(etags)
        assert result_etag == expected_etag

        # Verify assembled object
        get_resp = await mp_client.get("/test-bucket/big-file.bin")
        assert get_resp.status_code == 200
        assert get_resp.content == b"".join(parts_data)

        # Verify parts cleaned up from disk
        assert not parts_dir.exists()

        # Verify upload no longer listed
        list_resp = await mp_client.get("/test-bucket?uploads")
        list_root = ET.fromstring(list_resp.text)
        uploads = list_root.findall(f"{NS}Upload")
        assert len(uploads) == 0

    async def test_common_headers_on_complete_response(self, mp_client: AsyncClient) -> None:
        """CompleteMultipartUpload response has common S3 headers."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "my-key")
        etag = await _upload_part(mp_client, "test-bucket", "my-key", upload_id, 1, b"data")

        xml_body = _build_complete_xml([(1, etag)])
        resp = await mp_client.post(
            f"/test-bucket/my-key?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert "x-amz-request-id" in resp.headers
        assert "x-amz-id-2" in resp.headers
        assert "date" in resp.headers
        assert resp.headers.get("server") == "BleepStore"


# ---------------------------------------------------------------------------
# UploadPartCopy tests
# ---------------------------------------------------------------------------


class TestUploadPartCopy:
    """Tests for PUT /{bucket}/{key}?partNumber&uploadId with x-amz-copy-source."""

    async def test_copy_part_from_existing_object(self, mp_client: AsyncClient) -> None:
        """UploadPartCopy copies data from an existing object into a part."""
        await _create_bucket(mp_client)

        # Create source object
        source_data = b"source object data for copy"
        await mp_client.put("/test-bucket/source.txt", content=source_data)

        # Initiate multipart upload
        upload_id = await _initiate_upload(mp_client, "test-bucket", "dest.txt")

        # Copy part from source
        resp = await mp_client.put(
            f"/test-bucket/dest.txt?partNumber=1&uploadId={upload_id}",
            headers={"x-amz-copy-source": "/test-bucket/source.txt"},
        )
        assert resp.status_code == 200
        assert "CopyPartResult" in resp.text

        # Parse CopyPartResult for ETag
        root = ET.fromstring(resp.text)
        copy_etag = root.find(f"{NS}ETag").text
        assert copy_etag is not None

        # Complete the upload
        xml_body = _build_complete_xml([(1, copy_etag)])
        complete_resp = await mp_client.post(
            f"/test-bucket/dest.txt?uploadId={upload_id}",
            content=xml_body.encode(),
        )
        assert complete_resp.status_code == 200

        # Verify the assembled object has the source data
        get_resp = await mp_client.get("/test-bucket/dest.txt")
        assert get_resp.content == source_data

    async def test_copy_part_with_range(self, mp_client: AsyncClient) -> None:
        """UploadPartCopy with x-amz-copy-source-range copies a byte range."""
        await _create_bucket(mp_client)

        # Create source object
        source_data = b"0123456789ABCDEFGHIJ"
        await mp_client.put("/test-bucket/source.txt", content=source_data)

        # Initiate multipart upload
        upload_id = await _initiate_upload(mp_client, "test-bucket", "dest.txt")

        # Copy bytes 5-14 from source
        resp = await mp_client.put(
            f"/test-bucket/dest.txt?partNumber=1&uploadId={upload_id}",
            headers={
                "x-amz-copy-source": "/test-bucket/source.txt",
                "x-amz-copy-source-range": "bytes=5-14",
            },
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        copy_etag = root.find(f"{NS}ETag").text

        # Complete
        xml_body = _build_complete_xml([(1, copy_etag)])
        await mp_client.post(
            f"/test-bucket/dest.txt?uploadId={upload_id}",
            content=xml_body.encode(),
        )

        # Verify - should be bytes 5-14: "56789ABCDE"
        get_resp = await mp_client.get("/test-bucket/dest.txt")
        assert get_resp.content == source_data[5:15]

    async def test_copy_part_source_not_found(self, mp_client: AsyncClient) -> None:
        """UploadPartCopy with non-existent source returns 404."""
        await _create_bucket(mp_client)
        upload_id = await _initiate_upload(mp_client, "test-bucket", "dest.txt")

        resp = await mp_client.put(
            f"/test-bucket/dest.txt?partNumber=1&uploadId={upload_id}",
            headers={"x-amz-copy-source": "/test-bucket/nonexistent.txt"},
        )
        assert resp.status_code == 404
        assert "NoSuchKey" in resp.text

    async def test_copy_part_cross_bucket(self, mp_client: AsyncClient) -> None:
        """UploadPartCopy can copy from a different bucket."""
        await _create_bucket(mp_client, "source-bucket")
        await _create_bucket(mp_client, "dest-bucket")

        # Create source object
        source_data = b"cross bucket data"
        await mp_client.put("/source-bucket/file.txt", content=source_data)

        # Initiate multipart upload in dest bucket
        upload_id = await _initiate_upload(mp_client, "dest-bucket", "file.txt")

        # Copy part from source bucket
        resp = await mp_client.put(
            f"/dest-bucket/file.txt?partNumber=1&uploadId={upload_id}",
            headers={"x-amz-copy-source": "/source-bucket/file.txt"},
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        copy_etag = root.find(f"{NS}ETag").text

        # Complete
        xml_body = _build_complete_xml([(1, copy_etag)])
        await mp_client.post(
            f"/dest-bucket/file.txt?uploadId={upload_id}",
            content=xml_body.encode(),
        )

        get_resp = await mp_client.get("/dest-bucket/file.txt")
        assert get_resp.content == source_data
