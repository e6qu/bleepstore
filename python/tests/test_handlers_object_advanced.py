"""Integration tests for advanced object handlers (Stage 5a + 5b).

Stage 5a: List, Copy & Batch Delete
Stage 5b: Range Requests, Conditional Requests & Object ACLs

These tests use a real SQLite metadata store (in-memory) and real
local storage backend (in tmp_path) and exercise the full request path
via httpx AsyncClient. Each test class creates its own fresh stores
to avoid state leaks between tests.
"""

import email.utils
import hashlib
import time
import xml.etree.ElementTree as ET

import pytest
from httpx import ASGITransport, AsyncClient

from bleepstore.metadata.sqlite import SQLiteMetadataStore
from bleepstore.storage.local import LocalStorageBackend


@pytest.fixture
async def adv_client(app, config, tmp_path):
    """Create a client with fresh metadata and storage for advanced object tests.

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


async def _create_bucket(client, bucket: str):
    """Helper: create a bucket."""
    resp = await client.put(f"/{bucket}")
    assert resp.status_code == 200


async def _put_object(client, bucket: str, key: str, data: bytes = b"data"):
    """Helper: put an object."""
    resp = await client.put(f"/{bucket}/{key}", content=data)
    assert resp.status_code == 200
    return resp


class TestCopyObject:
    """Tests for PUT /{bucket}/{key} with x-amz-copy-source (CopyObject)."""

    async def test_copy_object_same_bucket(self, adv_client):
        """CopyObject within the same bucket returns 200 with CopyObjectResult XML."""
        await _create_bucket(adv_client, "copy-bucket")
        await _put_object(adv_client, "copy-bucket", "source.txt", b"hello")

        resp = await adv_client.put(
            "/copy-bucket/dest.txt",
            headers={"x-amz-copy-source": "/copy-bucket/source.txt"},
        )
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")

        # Parse the XML response
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        etag_elem = root.find(f"{ns}ETag")
        assert etag_elem is not None
        assert etag_elem.text is not None
        assert etag_elem.text.startswith('"')

        last_mod_elem = root.find(f"{ns}LastModified")
        assert last_mod_elem is not None
        assert last_mod_elem.text is not None

    async def test_copy_object_cross_bucket(self, adv_client):
        """CopyObject across buckets copies data correctly."""
        await _create_bucket(adv_client, "src-bucket")
        await _create_bucket(adv_client, "dst-bucket")
        await _put_object(adv_client, "src-bucket", "file.txt", b"cross-bucket data")

        resp = await adv_client.put(
            "/dst-bucket/file-copy.txt",
            headers={"x-amz-copy-source": "/src-bucket/file.txt"},
        )
        assert resp.status_code == 200

        # Verify the copy has the same content
        get_resp = await adv_client.get("/dst-bucket/file-copy.txt")
        assert get_resp.content == b"cross-bucket data"

    async def test_copy_object_preserves_metadata_by_default(self, adv_client):
        """CopyObject with COPY directive preserves source metadata."""
        await _create_bucket(adv_client, "meta-copy-bucket")
        await adv_client.put(
            "/meta-copy-bucket/original.txt",
            content=b"data",
            headers={
                "Content-Type": "text/plain",
                "x-amz-meta-color": "blue",
            },
        )

        resp = await adv_client.put(
            "/meta-copy-bucket/copy.txt",
            headers={"x-amz-copy-source": "/meta-copy-bucket/original.txt"},
        )
        assert resp.status_code == 200

        # Verify metadata is preserved
        head_resp = await adv_client.head("/meta-copy-bucket/copy.txt")
        assert head_resp.headers.get("content-type") == "text/plain"
        assert head_resp.headers.get("x-amz-meta-color") == "blue"

    async def test_copy_object_replace_metadata(self, adv_client):
        """CopyObject with REPLACE directive uses new metadata."""
        await _create_bucket(adv_client, "replace-bucket")
        await adv_client.put(
            "/replace-bucket/original.txt",
            content=b"data",
            headers={
                "Content-Type": "text/plain",
                "x-amz-meta-color": "blue",
            },
        )

        resp = await adv_client.put(
            "/replace-bucket/replaced.txt",
            headers={
                "x-amz-copy-source": "/replace-bucket/original.txt",
                "x-amz-metadata-directive": "REPLACE",
                "Content-Type": "application/json",
                "x-amz-meta-color": "red",
            },
        )
        assert resp.status_code == 200

        # Verify new metadata
        head_resp = await adv_client.head("/replace-bucket/replaced.txt")
        assert "application/json" in head_resp.headers.get("content-type", "")
        assert head_resp.headers.get("x-amz-meta-color") == "red"

    async def test_copy_object_source_not_found(self, adv_client):
        """CopyObject with non-existent source returns 404."""
        await _create_bucket(adv_client, "copy-err-bucket")
        resp = await adv_client.put(
            "/copy-err-bucket/dest.txt",
            headers={"x-amz-copy-source": "/copy-err-bucket/nonexistent.txt"},
        )
        assert resp.status_code == 404

    async def test_copy_object_url_decoded_source(self, adv_client):
        """CopyObject with URL-encoded source works correctly."""
        await _create_bucket(adv_client, "encoded-bucket")
        await _put_object(adv_client, "encoded-bucket", "path/to/file.txt", b"encoded")

        resp = await adv_client.put(
            "/encoded-bucket/copy.txt",
            headers={
                "x-amz-copy-source": "/encoded-bucket/path%2Fto%2Ffile.txt",
            },
        )
        assert resp.status_code == 200

        get_resp = await adv_client.get("/encoded-bucket/copy.txt")
        assert get_resp.content == b"encoded"

    async def test_copy_object_no_leading_slash(self, adv_client):
        """CopyObject source without leading slash works."""
        await _create_bucket(adv_client, "no-slash-bucket")
        await _put_object(adv_client, "no-slash-bucket", "src.txt", b"data")

        resp = await adv_client.put(
            "/no-slash-bucket/dst.txt",
            headers={"x-amz-copy-source": "no-slash-bucket/src.txt"},
        )
        assert resp.status_code == 200


class TestDeleteObjects:
    """Tests for POST /{bucket}?delete (DeleteObjects / batch delete)."""

    async def test_delete_objects_basic(self, adv_client):
        """DeleteObjects removes multiple objects and returns XML result."""
        await _create_bucket(adv_client, "batch-del-bucket")
        await _put_object(adv_client, "batch-del-bucket", "a.txt", b"aaa")
        await _put_object(adv_client, "batch-del-bucket", "b.txt", b"bbb")

        delete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete>
            <Object><Key>a.txt</Key></Object>
            <Object><Key>b.txt</Key></Object>
        </Delete>"""

        resp = await adv_client.post(
            "/batch-del-bucket?delete",
            content=delete_xml.encode(),
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")

        # Verify deleted
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        deleted_elems = root.findall(f"{ns}Deleted")
        assert len(deleted_elems) == 2

        # Verify objects are gone
        assert (await adv_client.get("/batch-del-bucket/a.txt")).status_code == 404
        assert (await adv_client.get("/batch-del-bucket/b.txt")).status_code == 404

    async def test_delete_objects_quiet_mode(self, adv_client):
        """DeleteObjects in quiet mode omits Deleted elements."""
        await _create_bucket(adv_client, "quiet-del-bucket")
        await _put_object(adv_client, "quiet-del-bucket", "x.txt", b"xxx")

        delete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete>
            <Quiet>true</Quiet>
            <Object><Key>x.txt</Key></Object>
        </Delete>"""

        resp = await adv_client.post(
            "/quiet-del-bucket?delete",
            content=delete_xml.encode(),
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        deleted_elems = root.findall(f"{ns}Deleted")
        # In quiet mode, no Deleted elements
        assert len(deleted_elems) == 0

        # But object is still deleted
        assert (await adv_client.get("/quiet-del-bucket/x.txt")).status_code == 404

    async def test_delete_objects_nonexistent_keys(self, adv_client):
        """DeleteObjects with non-existent keys still returns 200."""
        await _create_bucket(adv_client, "del-nonexist-bucket")

        delete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete>
            <Object><Key>ghost.txt</Key></Object>
        </Delete>"""

        resp = await adv_client.post(
            "/del-nonexist-bucket?delete",
            content=delete_xml.encode(),
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 200

    async def test_delete_objects_no_such_bucket(self, adv_client):
        """DeleteObjects on non-existent bucket returns 404."""
        delete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete>
            <Object><Key>a.txt</Key></Object>
        </Delete>"""

        resp = await adv_client.post(
            "/nonexistent-batch-bucket?delete",
            content=delete_xml.encode(),
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 404

    async def test_delete_objects_malformed_xml(self, adv_client):
        """DeleteObjects with malformed XML returns 400."""
        await _create_bucket(adv_client, "bad-xml-bucket")

        resp = await adv_client.post(
            "/bad-xml-bucket?delete",
            content=b"not xml at all",
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 400

    async def test_delete_objects_with_namespace(self, adv_client):
        """DeleteObjects with S3 namespace in XML body works."""
        await _create_bucket(adv_client, "ns-del-bucket")
        await _put_object(adv_client, "ns-del-bucket", "f1.txt", b"data")

        delete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Object><Key>f1.txt</Key></Object>
        </Delete>"""

        resp = await adv_client.post(
            "/ns-del-bucket?delete",
            content=delete_xml.encode(),
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 200


class TestListObjectsV2:
    """Tests for GET /{bucket}?list-type=2 (ListObjectsV2)."""

    async def test_list_objects_v2_empty_bucket(self, adv_client):
        """ListObjectsV2 on empty bucket returns valid XML with no Contents."""
        await _create_bucket(adv_client, "list-empty-bucket")

        resp = await adv_client.get("/list-empty-bucket?list-type=2")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Name").text == "list-empty-bucket"
        assert root.find(f"{ns}KeyCount").text == "0"
        assert root.find(f"{ns}IsTruncated").text == "false"

    async def test_list_objects_v2_with_objects(self, adv_client):
        """ListObjectsV2 returns Contents elements for all objects."""
        await _create_bucket(adv_client, "list-objs-bucket")
        await _put_object(adv_client, "list-objs-bucket", "a.txt", b"aaa")
        await _put_object(adv_client, "list-objs-bucket", "b.txt", b"bbb")
        await _put_object(adv_client, "list-objs-bucket", "c.txt", b"ccc")

        resp = await adv_client.get("/list-objs-bucket?list-type=2")
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        contents = root.findall(f"{ns}Contents")
        assert len(contents) == 3

        keys = [c.find(f"{ns}Key").text for c in contents]
        assert keys == ["a.txt", "b.txt", "c.txt"]

    async def test_list_objects_v2_prefix(self, adv_client):
        """ListObjectsV2 with prefix filters results."""
        await _create_bucket(adv_client, "list-prefix-bucket")
        await _put_object(adv_client, "list-prefix-bucket", "images/a.jpg", b"img1")
        await _put_object(adv_client, "list-prefix-bucket", "images/b.jpg", b"img2")
        await _put_object(adv_client, "list-prefix-bucket", "docs/readme.md", b"doc")

        resp = await adv_client.get(
            "/list-prefix-bucket?list-type=2&prefix=images/"
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        contents = root.findall(f"{ns}Contents")
        assert len(contents) == 2

        keys = [c.find(f"{ns}Key").text for c in contents]
        assert all(k.startswith("images/") for k in keys)

    async def test_list_objects_v2_delimiter(self, adv_client):
        """ListObjectsV2 with delimiter returns CommonPrefixes."""
        await _create_bucket(adv_client, "list-delim-bucket")
        await _put_object(adv_client, "list-delim-bucket", "photos/cat.jpg", b"cat")
        await _put_object(adv_client, "list-delim-bucket", "photos/dog.jpg", b"dog")
        await _put_object(adv_client, "list-delim-bucket", "videos/clip.mp4", b"clip")
        await _put_object(adv_client, "list-delim-bucket", "readme.txt", b"readme")

        resp = await adv_client.get(
            "/list-delim-bucket?list-type=2&delimiter=/"
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

        # readme.txt is a direct object (no delimiter in its name after prefix)
        contents = root.findall(f"{ns}Contents")
        assert len(contents) == 1
        assert contents[0].find(f"{ns}Key").text == "readme.txt"

        # photos/ and videos/ should be CommonPrefixes
        cps = root.findall(f"{ns}CommonPrefixes")
        cp_values = sorted([cp.find(f"{ns}Prefix").text for cp in cps])
        assert "photos/" in cp_values
        assert "videos/" in cp_values

    async def test_list_objects_v2_max_keys(self, adv_client):
        """ListObjectsV2 with max-keys limits results."""
        await _create_bucket(adv_client, "list-maxkeys-bucket")
        for i in range(5):
            await _put_object(
                adv_client, "list-maxkeys-bucket", f"obj{i}.txt", b"data"
            )

        resp = await adv_client.get(
            "/list-maxkeys-bucket?list-type=2&max-keys=2"
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        contents = root.findall(f"{ns}Contents")
        assert len(contents) == 2
        assert root.find(f"{ns}IsTruncated").text == "true"
        assert root.find(f"{ns}MaxKeys").text == "2"

    async def test_list_objects_v2_pagination(self, adv_client):
        """ListObjectsV2 pagination with continuation-token."""
        await _create_bucket(adv_client, "list-page-bucket")
        for i in range(5):
            await _put_object(
                adv_client, "list-page-bucket", f"key{i:02d}.txt", b"data"
            )

        # Page 1
        resp1 = await adv_client.get(
            "/list-page-bucket?list-type=2&max-keys=2"
        )
        root1 = ET.fromstring(resp1.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root1.find(f"{ns}IsTruncated").text == "true"
        token = root1.find(f"{ns}NextContinuationToken").text
        assert token is not None

        # Page 2
        resp2 = await adv_client.get(
            f"/list-page-bucket?list-type=2&max-keys=2&continuation-token={token}"
        )
        root2 = ET.fromstring(resp2.text)
        contents2 = root2.findall(f"{ns}Contents")
        assert len(contents2) == 2

        # Page 3 (last page)
        token2 = root2.find(f"{ns}NextContinuationToken").text
        resp3 = await adv_client.get(
            f"/list-page-bucket?list-type=2&max-keys=2&continuation-token={token2}"
        )
        root3 = ET.fromstring(resp3.text)
        contents3 = root3.findall(f"{ns}Contents")
        assert len(contents3) == 1
        assert root3.find(f"{ns}IsTruncated").text == "false"

    async def test_list_objects_v2_start_after(self, adv_client):
        """ListObjectsV2 with start-after skips keys."""
        await _create_bucket(adv_client, "list-start-bucket")
        await _put_object(adv_client, "list-start-bucket", "a.txt", b"aaa")
        await _put_object(adv_client, "list-start-bucket", "b.txt", b"bbb")
        await _put_object(adv_client, "list-start-bucket", "c.txt", b"ccc")

        resp = await adv_client.get(
            "/list-start-bucket?list-type=2&start-after=a.txt"
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        contents = root.findall(f"{ns}Contents")
        keys = [c.find(f"{ns}Key").text for c in contents]
        assert "a.txt" not in keys
        assert "b.txt" in keys
        assert "c.txt" in keys

    async def test_list_objects_v2_has_storage_class(self, adv_client):
        """ListObjectsV2 Contents elements include StorageClass."""
        await _create_bucket(adv_client, "list-sc-bucket")
        await _put_object(adv_client, "list-sc-bucket", "obj.txt", b"data")

        resp = await adv_client.get("/list-sc-bucket?list-type=2")
        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        sc = root.find(f"{ns}Contents/{ns}StorageClass")
        assert sc is not None
        assert sc.text == "STANDARD"

    async def test_list_objects_v2_nosuchbucket(self, adv_client):
        """ListObjectsV2 on non-existent bucket returns 404."""
        resp = await adv_client.get("/no-such-list-bucket?list-type=2")
        assert resp.status_code == 404


class TestListObjectsV1:
    """Tests for GET /{bucket} (ListObjects v1)."""

    async def test_list_objects_v1_basic(self, adv_client):
        """ListObjectsV1 returns objects in XML."""
        await _create_bucket(adv_client, "list-v1-bucket")
        await _put_object(adv_client, "list-v1-bucket", "file1.txt", b"f1")
        await _put_object(adv_client, "list-v1-bucket", "file2.txt", b"f2")

        resp = await adv_client.get("/list-v1-bucket")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        contents = root.findall(f"{ns}Contents")
        assert len(contents) == 2

    async def test_list_objects_v1_marker(self, adv_client):
        """ListObjectsV1 with marker pagination."""
        await _create_bucket(adv_client, "list-v1-marker-bucket")
        await _put_object(adv_client, "list-v1-marker-bucket", "a.txt", b"a")
        await _put_object(adv_client, "list-v1-marker-bucket", "b.txt", b"b")
        await _put_object(adv_client, "list-v1-marker-bucket", "c.txt", b"c")

        resp = await adv_client.get(
            "/list-v1-marker-bucket?marker=a.txt"
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        contents = root.findall(f"{ns}Contents")
        keys = [c.find(f"{ns}Key").text for c in contents]
        assert "a.txt" not in keys
        assert "b.txt" in keys
        assert "c.txt" in keys

    async def test_list_objects_v1_has_marker_in_xml(self, adv_client):
        """ListObjectsV1 includes Marker element in response."""
        await _create_bucket(adv_client, "list-v1-mkr-xml-bucket")

        resp = await adv_client.get(
            "/list-v1-mkr-xml-bucket?marker=some-key"
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        marker_elem = root.find(f"{ns}Marker")
        assert marker_elem is not None
        assert marker_elem.text == "some-key"

    async def test_list_objects_v1_delimiter(self, adv_client):
        """ListObjectsV1 with delimiter returns CommonPrefixes."""
        await _create_bucket(adv_client, "list-v1-delim-bucket")
        await _put_object(adv_client, "list-v1-delim-bucket", "dir1/a.txt", b"a")
        await _put_object(adv_client, "list-v1-delim-bucket", "dir2/b.txt", b"b")
        await _put_object(adv_client, "list-v1-delim-bucket", "root.txt", b"r")

        resp = await adv_client.get(
            "/list-v1-delim-bucket?delimiter=/"
        )
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

        contents = root.findall(f"{ns}Contents")
        assert len(contents) == 1
        assert contents[0].find(f"{ns}Key").text == "root.txt"

        cps = root.findall(f"{ns}CommonPrefixes")
        cp_values = sorted([cp.find(f"{ns}Prefix").text for cp in cps])
        assert "dir1/" in cp_values
        assert "dir2/" in cp_values

    async def test_list_objects_v1_pagination_with_max_keys(self, adv_client):
        """ListObjectsV1 with max-keys returns truncated results with NextMarker."""
        await _create_bucket(adv_client, "list-v1-page-bucket")
        for i in range(4):
            await _put_object(
                adv_client, "list-v1-page-bucket", f"key{i:02d}.txt", b"data"
            )

        # Page 1
        resp1 = await adv_client.get(
            "/list-v1-page-bucket?max-keys=2"
        )
        root1 = ET.fromstring(resp1.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root1.find(f"{ns}IsTruncated").text == "true"
        contents1 = root1.findall(f"{ns}Contents")
        assert len(contents1) == 2

        next_marker = root1.find(f"{ns}NextMarker")
        assert next_marker is not None
        assert next_marker.text is not None

        # Page 2
        resp2 = await adv_client.get(
            f"/list-v1-page-bucket?max-keys=2&marker={next_marker.text}"
        )
        root2 = ET.fromstring(resp2.text)
        contents2 = root2.findall(f"{ns}Contents")
        assert len(contents2) == 2
        assert root2.find(f"{ns}IsTruncated").text == "false"


class TestListObjectsDispatch:
    """Tests for GET /{bucket} dispatch (list-type routing)."""

    async def test_default_dispatches_to_v1(self, adv_client):
        """GET /{bucket} without list-type dispatches to v1."""
        await _create_bucket(adv_client, "dispatch-v1-bucket")
        await _put_object(adv_client, "dispatch-v1-bucket", "test.txt", b"data")

        resp = await adv_client.get("/dispatch-v1-bucket")
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        # V1 should have Marker, no KeyCount
        marker = root.find(f"{ns}Marker")
        assert marker is not None

    async def test_list_type_2_dispatches_to_v2(self, adv_client):
        """GET /{bucket}?list-type=2 dispatches to v2."""
        await _create_bucket(adv_client, "dispatch-v2-bucket")
        await _put_object(adv_client, "dispatch-v2-bucket", "test.txt", b"data")

        resp = await adv_client.get("/dispatch-v2-bucket?list-type=2")
        assert resp.status_code == 200

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        # V2 should have KeyCount, no Marker
        key_count = root.find(f"{ns}KeyCount")
        assert key_count is not None


# ===========================================================================
# Stage 5b: Range Requests, Conditional Requests & Object ACLs
# ===========================================================================


class TestGetObjectRange:
    """Tests for GET /{bucket}/{key} with Range header."""

    async def test_range_basic_206(self, adv_client):
        """Range bytes=0-4 returns 206 with first 5 bytes and Content-Range header."""
        await _create_bucket(adv_client, "range-basic-bucket")
        await _put_object(adv_client, "range-basic-bucket", "hello.txt", b"hello world")

        resp = await adv_client.get(
            "/range-basic-bucket/hello.txt",
            headers={"Range": "bytes=0-4"},
        )
        assert resp.status_code == 206
        assert resp.content == b"hello"
        assert resp.headers.get("content-range") == "bytes 0-4/11"
        assert resp.headers.get("content-length") == "5"

    async def test_range_suffix(self, adv_client):
        """Range bytes=-5 returns last 5 bytes."""
        await _create_bucket(adv_client, "range-suffix-bucket")
        await _put_object(adv_client, "range-suffix-bucket", "hello.txt", b"hello world")

        resp = await adv_client.get(
            "/range-suffix-bucket/hello.txt",
            headers={"Range": "bytes=-5"},
        )
        assert resp.status_code == 206
        assert resp.content == b"world"
        assert resp.headers.get("content-range") == "bytes 6-10/11"

    async def test_range_open_ended(self, adv_client):
        """Range bytes=6- returns from byte 6 to end."""
        await _create_bucket(adv_client, "range-open-bucket")
        await _put_object(adv_client, "range-open-bucket", "hello.txt", b"hello world")

        resp = await adv_client.get(
            "/range-open-bucket/hello.txt",
            headers={"Range": "bytes=6-"},
        )
        assert resp.status_code == 206
        assert resp.content == b"world"
        assert resp.headers.get("content-range") == "bytes 6-10/11"

    async def test_range_full_file(self, adv_client):
        """Range bytes=0-10 returns entire 11-byte file."""
        await _create_bucket(adv_client, "range-full-bucket")
        await _put_object(adv_client, "range-full-bucket", "hello.txt", b"hello world")

        resp = await adv_client.get(
            "/range-full-bucket/hello.txt",
            headers={"Range": "bytes=0-10"},
        )
        assert resp.status_code == 206
        assert resp.content == b"hello world"
        assert resp.headers.get("content-range") == "bytes 0-10/11"

    async def test_range_single_byte(self, adv_client):
        """Range bytes=5-5 returns single byte."""
        await _create_bucket(adv_client, "range-single-bucket")
        await _put_object(adv_client, "range-single-bucket", "hello.txt", b"hello world")

        resp = await adv_client.get(
            "/range-single-bucket/hello.txt",
            headers={"Range": "bytes=5-5"},
        )
        assert resp.status_code == 206
        assert resp.content == b" "
        assert resp.headers.get("content-range") == "bytes 5-5/11"

    async def test_range_invalid_416(self, adv_client):
        """Range past end of file returns 416 Range Not Satisfiable."""
        await _create_bucket(adv_client, "range-invalid-bucket")
        await _put_object(adv_client, "range-invalid-bucket", "hello.txt", b"hello world")

        resp = await adv_client.get(
            "/range-invalid-bucket/hello.txt",
            headers={"Range": "bytes=100-200"},
        )
        assert resp.status_code == 416

    async def test_range_clamps_end(self, adv_client):
        """Range with end past file size clamps to file size - 1."""
        await _create_bucket(adv_client, "range-clamp-bucket")
        await _put_object(adv_client, "range-clamp-bucket", "hello.txt", b"hello world")

        resp = await adv_client.get(
            "/range-clamp-bucket/hello.txt",
            headers={"Range": "bytes=6-999"},
        )
        assert resp.status_code == 206
        assert resp.content == b"world"
        assert resp.headers.get("content-range") == "bytes 6-10/11"

    async def test_range_has_etag(self, adv_client):
        """206 response still includes ETag header."""
        await _create_bucket(adv_client, "range-etag-bucket")
        data = b"hello world"
        await _put_object(adv_client, "range-etag-bucket", "hello.txt", data)

        resp = await adv_client.get(
            "/range-etag-bucket/hello.txt",
            headers={"Range": "bytes=0-4"},
        )
        assert resp.status_code == 206
        expected_md5 = hashlib.md5(data).hexdigest()
        assert resp.headers.get("etag") == f'"{expected_md5}"'

    async def test_no_range_returns_200(self, adv_client):
        """GET without Range header returns 200 with full body."""
        await _create_bucket(adv_client, "range-none-bucket")
        await _put_object(adv_client, "range-none-bucket", "hello.txt", b"hello world")

        resp = await adv_client.get("/range-none-bucket/hello.txt")
        assert resp.status_code == 200
        assert resp.content == b"hello world"
        assert "content-range" not in resp.headers


class TestConditionalRequests:
    """Tests for conditional request headers (If-Match, If-None-Match, etc.)."""

    async def test_if_none_match_304(self, adv_client):
        """If-None-Match with matching ETag returns 304."""
        await _create_bucket(adv_client, "cond-inm-bucket")
        data = b"conditional data"
        put_resp = await _put_object(adv_client, "cond-inm-bucket", "doc.txt", data)
        etag = put_resp.headers["etag"]

        resp = await adv_client.get(
            "/cond-inm-bucket/doc.txt",
            headers={"If-None-Match": etag},
        )
        assert resp.status_code == 304
        assert resp.content == b""

    async def test_if_none_match_200(self, adv_client):
        """If-None-Match with non-matching ETag returns 200."""
        await _create_bucket(adv_client, "cond-inm-ok-bucket")
        await _put_object(adv_client, "cond-inm-ok-bucket", "doc.txt", b"data")

        resp = await adv_client.get(
            "/cond-inm-ok-bucket/doc.txt",
            headers={"If-None-Match": '"nonmatchingetag"'},
        )
        assert resp.status_code == 200

    async def test_if_match_412(self, adv_client):
        """If-Match with non-matching ETag returns 412."""
        await _create_bucket(adv_client, "cond-im-bucket")
        await _put_object(adv_client, "cond-im-bucket", "doc.txt", b"data")

        resp = await adv_client.get(
            "/cond-im-bucket/doc.txt",
            headers={"If-Match": '"wrongetag"'},
        )
        assert resp.status_code == 412

    async def test_if_match_200(self, adv_client):
        """If-Match with correct ETag returns 200."""
        await _create_bucket(adv_client, "cond-im-ok-bucket")
        data = b"match data"
        put_resp = await _put_object(adv_client, "cond-im-ok-bucket", "doc.txt", data)
        etag = put_resp.headers["etag"]

        resp = await adv_client.get(
            "/cond-im-ok-bucket/doc.txt",
            headers={"If-Match": etag},
        )
        assert resp.status_code == 200
        assert resp.content == data

    async def test_if_match_star(self, adv_client):
        """If-Match: * always succeeds if object exists."""
        await _create_bucket(adv_client, "cond-im-star-bucket")
        await _put_object(adv_client, "cond-im-star-bucket", "doc.txt", b"data")

        resp = await adv_client.get(
            "/cond-im-star-bucket/doc.txt",
            headers={"If-Match": "*"},
        )
        assert resp.status_code == 200

    async def test_if_none_match_star_304(self, adv_client):
        """If-None-Match: * returns 304 on GET if object exists."""
        await _create_bucket(adv_client, "cond-inm-star-bucket")
        await _put_object(adv_client, "cond-inm-star-bucket", "doc.txt", b"data")

        resp = await adv_client.get(
            "/cond-inm-star-bucket/doc.txt",
            headers={"If-None-Match": "*"},
        )
        assert resp.status_code == 304

    async def test_if_none_match_head_304(self, adv_client):
        """If-None-Match on HEAD returns 304 with matching ETag."""
        await _create_bucket(adv_client, "cond-head-bucket")
        data = b"head conditional"
        put_resp = await _put_object(adv_client, "cond-head-bucket", "doc.txt", data)
        etag = put_resp.headers["etag"]

        resp = await adv_client.head(
            "/cond-head-bucket/doc.txt",
            headers={"If-None-Match": etag},
        )
        assert resp.status_code == 304

    async def test_304_includes_etag(self, adv_client):
        """304 response includes ETag header."""
        await _create_bucket(adv_client, "cond-304etag-bucket")
        data = b"etag check"
        put_resp = await _put_object(adv_client, "cond-304etag-bucket", "doc.txt", data)
        etag = put_resp.headers["etag"]

        resp = await adv_client.get(
            "/cond-304etag-bucket/doc.txt",
            headers={"If-None-Match": etag},
        )
        assert resp.status_code == 304
        assert resp.headers.get("etag") == etag

    async def test_if_none_match_multiple_etags(self, adv_client):
        """If-None-Match with comma-separated ETags matches correctly."""
        await _create_bucket(adv_client, "cond-multi-bucket")
        data = b"multi match"
        put_resp = await _put_object(adv_client, "cond-multi-bucket", "doc.txt", data)
        etag = put_resp.headers["etag"]

        resp = await adv_client.get(
            "/cond-multi-bucket/doc.txt",
            headers={"If-None-Match": f'"fake1", {etag}, "fake2"'},
        )
        assert resp.status_code == 304


class TestObjectAcl:
    """Tests for GET/PUT /{bucket}/{key}?acl (Object ACLs)."""

    async def test_get_object_acl_default(self, adv_client):
        """GetObjectAcl returns default private ACL with FULL_CONTROL."""
        await _create_bucket(adv_client, "acl-default-bucket")
        await _put_object(adv_client, "acl-default-bucket", "file.txt", b"data")

        resp = await adv_client.get("/acl-default-bucket/file.txt?acl")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")

        root = ET.fromstring(resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

        # Owner should be present
        owner = root.find(f"{ns}Owner")
        assert owner is not None
        owner_id = owner.find(f"{ns}ID")
        assert owner_id is not None
        assert owner_id.text is not None and len(owner_id.text) > 0

        # Should have at least one grant with FULL_CONTROL
        acl_list = root.find(f"{ns}AccessControlList")
        assert acl_list is not None
        grants = acl_list.findall(f"{ns}Grant")
        assert len(grants) >= 1

        permissions = [
            g.find(f"{ns}Permission").text for g in grants
        ]
        assert "FULL_CONTROL" in permissions

    async def test_put_object_acl_canned(self, adv_client):
        """PutObjectAcl with canned ACL header updates the ACL."""
        await _create_bucket(adv_client, "acl-put-bucket")
        await _put_object(adv_client, "acl-put-bucket", "file.txt", b"data")

        # Set public-read ACL
        resp = await adv_client.put(
            "/acl-put-bucket/file.txt?acl",
            headers={"x-amz-acl": "public-read"},
        )
        assert resp.status_code == 200

        # Verify the ACL was updated
        get_resp = await adv_client.get("/acl-put-bucket/file.txt?acl")
        assert get_resp.status_code == 200

        root = ET.fromstring(get_resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        acl_list = root.find(f"{ns}AccessControlList")
        grants = acl_list.findall(f"{ns}Grant")
        assert len(grants) == 2  # FULL_CONTROL + READ

        permissions = [
            g.find(f"{ns}Permission").text for g in grants
        ]
        assert "FULL_CONTROL" in permissions
        assert "READ" in permissions

    async def test_get_object_acl_nosuchkey(self, adv_client):
        """GetObjectAcl on non-existent key returns 404."""
        await _create_bucket(adv_client, "acl-nokey-bucket")
        resp = await adv_client.get("/acl-nokey-bucket/nonexistent.txt?acl")
        assert resp.status_code == 404

    async def test_get_object_acl_nosuchbucket(self, adv_client):
        """GetObjectAcl on non-existent bucket returns 404."""
        resp = await adv_client.get("/no-such-acl-bucket/file.txt?acl")
        assert resp.status_code == 404

    async def test_put_object_acl_nosuchkey(self, adv_client):
        """PutObjectAcl on non-existent key returns 404."""
        await _create_bucket(adv_client, "acl-put-nokey-bucket")
        resp = await adv_client.put(
            "/acl-put-nokey-bucket/ghost.txt?acl",
            headers={"x-amz-acl": "private"},
        )
        assert resp.status_code == 404

    async def test_put_object_acl_private(self, adv_client):
        """PutObjectAcl with private canned ACL sets owner-only access."""
        await _create_bucket(adv_client, "acl-private-bucket")
        await _put_object(adv_client, "acl-private-bucket", "file.txt", b"data")

        # First set to public-read
        await adv_client.put(
            "/acl-private-bucket/file.txt?acl",
            headers={"x-amz-acl": "public-read"},
        )

        # Then set back to private
        resp = await adv_client.put(
            "/acl-private-bucket/file.txt?acl",
            headers={"x-amz-acl": "private"},
        )
        assert resp.status_code == 200

        # Verify only 1 grant (FULL_CONTROL for owner)
        get_resp = await adv_client.get("/acl-private-bucket/file.txt?acl")
        root = ET.fromstring(get_resp.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        acl_list = root.find(f"{ns}AccessControlList")
        grants = acl_list.findall(f"{ns}Grant")
        assert len(grants) == 1
        assert grants[0].find(f"{ns}Permission").text == "FULL_CONTROL"

    async def test_get_object_acl_has_namespace(self, adv_client):
        """GetObjectAcl response has correct S3 XML namespace."""
        await _create_bucket(adv_client, "acl-ns-bucket")
        await _put_object(adv_client, "acl-ns-bucket", "file.txt", b"data")

        resp = await adv_client.get("/acl-ns-bucket/file.txt?acl")
        assert resp.status_code == 200
        assert "http://s3.amazonaws.com/doc/2006-03-01/" in resp.text
