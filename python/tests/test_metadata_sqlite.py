"""Tests for the SQLite metadata store.

Uses pytest-asyncio with tmp_path fixture for isolated test databases.
Each test gets its own fresh SQLiteMetadataStore instance.
"""

import json

import pytest

from bleepstore.metadata.sqlite import SQLiteMetadataStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def store(tmp_path):
    """Create a fresh SQLiteMetadataStore for each test."""
    db_path = str(tmp_path / "test.db")
    s = SQLiteMetadataStore(db_path)
    await s.init_db()
    yield s
    await s.close()


@pytest.fixture
async def store_with_bucket(store):
    """A store with a pre-created bucket named 'test-bucket'."""
    await store.create_bucket(
        "test-bucket", "us-east-1", owner_id="owner1", owner_display="Owner One"
    )
    return store


# ---------------------------------------------------------------------------
# Schema idempotency
# ---------------------------------------------------------------------------


class TestSchemaIdempotency:
    """Test that init_db can be called multiple times without error."""

    async def test_init_db_twice(self, tmp_path):
        """Calling init_db twice on the same DB does not raise."""
        db_path = str(tmp_path / "idempotent.db")
        s = SQLiteMetadataStore(db_path)
        await s.init_db()
        # Call again -- should not raise
        await s.init_db()
        await s.close()

    async def test_schema_version_exists(self, store):
        """Schema version table has version 1."""
        assert store._db is not None
        async with store._db.execute("SELECT version FROM schema_version") as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == 1

    async def test_reopen_after_close(self, tmp_path):
        """Re-opening a previously created database works."""
        db_path = str(tmp_path / "reopen.db")

        s1 = SQLiteMetadataStore(db_path)
        await s1.init_db()
        await s1.create_bucket("my-bucket", "us-west-2", owner_id="o1")
        await s1.close()

        s2 = SQLiteMetadataStore(db_path)
        await s2.init_db()
        b = await s2.get_bucket("my-bucket")
        assert b is not None
        assert b["name"] == "my-bucket"
        assert b["region"] == "us-west-2"
        await s2.close()


# ---------------------------------------------------------------------------
# Bucket CRUD
# ---------------------------------------------------------------------------


class TestBucketCRUD:
    """Tests for bucket create, read, list, and delete operations."""

    async def test_create_and_get_bucket(self, store):
        """Creating a bucket and retrieving it returns correct data."""
        await store.create_bucket("my-bucket", "us-east-1", owner_id="owner1", owner_display="Test")
        b = await store.get_bucket("my-bucket")
        assert b is not None
        assert b["name"] == "my-bucket"
        assert b["region"] == "us-east-1"
        assert b["owner_id"] == "owner1"
        assert b["owner_display"] == "Test"
        assert b["created_at"] != ""

    async def test_get_nonexistent_bucket(self, store):
        """Getting a bucket that doesn't exist returns None."""
        b = await store.get_bucket("no-such-bucket")
        assert b is None

    async def test_bucket_exists_true(self, store_with_bucket):
        """bucket_exists returns True for an existing bucket."""
        assert await store_with_bucket.bucket_exists("test-bucket") is True

    async def test_bucket_exists_false(self, store):
        """bucket_exists returns False for a non-existent bucket."""
        assert await store.bucket_exists("no-such-bucket") is False

    async def test_delete_bucket(self, store_with_bucket):
        """Deleting a bucket removes it from the store."""
        await store_with_bucket.delete_bucket("test-bucket")
        b = await store_with_bucket.get_bucket("test-bucket")
        assert b is None

    async def test_delete_nonexistent_bucket(self, store):
        """Deleting a non-existent bucket does not raise."""
        await store.delete_bucket("no-such-bucket")  # Should not raise

    async def test_list_buckets_empty(self, store):
        """Listing buckets when none exist returns empty list."""
        result = await store.list_buckets()
        assert result == []

    async def test_list_buckets_multiple(self, store):
        """Listing buckets returns all created buckets in name order."""
        await store.create_bucket("bucket-b", "us-west-1", owner_id="o1")
        await store.create_bucket("bucket-a", "us-east-1", owner_id="o1")
        await store.create_bucket("bucket-c", "eu-west-1", owner_id="o1")
        result = await store.list_buckets()
        names = [b["name"] for b in result]
        assert names == ["bucket-a", "bucket-b", "bucket-c"]

    async def test_list_buckets_by_owner(self, store):
        """Listing buckets with owner_id filter only returns that owner's buckets."""
        await store.create_bucket("bucket-1", "us-east-1", owner_id="alice")
        await store.create_bucket("bucket-2", "us-east-1", owner_id="bob")
        await store.create_bucket("bucket-3", "us-east-1", owner_id="alice")
        result = await store.list_buckets(owner_id="alice")
        names = [b["name"] for b in result]
        assert names == ["bucket-1", "bucket-3"]

    async def test_create_bucket_with_acl(self, store):
        """Creating a bucket with a custom ACL stores it."""
        acl = json.dumps({"owner": {"id": "o1"}, "grants": []})
        await store.create_bucket("acl-bucket", "us-east-1", owner_id="o1", acl=acl)
        b = await store.get_bucket("acl-bucket")
        assert b is not None
        assert json.loads(b["acl"]) == {"owner": {"id": "o1"}, "grants": []}

    async def test_update_bucket_acl(self, store_with_bucket):
        """Updating a bucket's ACL changes it."""
        new_acl = json.dumps({"owner": {"id": "owner1"}, "grants": [{"permission": "READ"}]})
        await store_with_bucket.update_bucket_acl("test-bucket", new_acl)
        b = await store_with_bucket.get_bucket("test-bucket")
        assert b is not None
        parsed = json.loads(b["acl"])
        assert parsed["grants"][0]["permission"] == "READ"

    async def test_create_bucket_default_region(self, store):
        """Creating a bucket without specifying region uses us-east-1."""
        await store.create_bucket("default-region")
        b = await store.get_bucket("default-region")
        assert b is not None
        assert b["region"] == "us-east-1"

    async def test_create_duplicate_bucket_raises(self, store_with_bucket):
        """Creating a bucket with the same name raises IntegrityError."""
        import aiosqlite

        with pytest.raises(aiosqlite.IntegrityError):
            await store_with_bucket.create_bucket("test-bucket", "us-west-2")


# ---------------------------------------------------------------------------
# Object CRUD
# ---------------------------------------------------------------------------


class TestObjectCRUD:
    """Tests for object create, read, update, and delete operations."""

    async def test_put_and_get_object(self, store_with_bucket):
        """Putting an object and retrieving it returns correct data."""
        await store_with_bucket.put_object(
            bucket="test-bucket",
            key="hello.txt",
            size=12,
            etag='"abc123"',
            content_type="text/plain",
        )
        obj = await store_with_bucket.get_object("test-bucket", "hello.txt")
        assert obj is not None
        assert obj["bucket"] == "test-bucket"
        assert obj["key"] == "hello.txt"
        assert obj["size"] == 12
        assert obj["etag"] == '"abc123"'
        assert obj["content_type"] == "text/plain"
        assert obj["last_modified"] != ""
        assert obj["storage_class"] == "STANDARD"

    async def test_get_nonexistent_object(self, store_with_bucket):
        """Getting an object that doesn't exist returns None."""
        obj = await store_with_bucket.get_object("test-bucket", "no-such-key")
        assert obj is None

    async def test_object_exists_true(self, store_with_bucket):
        """object_exists returns True for an existing object."""
        await store_with_bucket.put_object("test-bucket", "key1", 10, '"e1"', "text/plain")
        assert await store_with_bucket.object_exists("test-bucket", "key1") is True

    async def test_object_exists_false(self, store_with_bucket):
        """object_exists returns False for a non-existent object."""
        assert await store_with_bucket.object_exists("test-bucket", "no-such-key") is False

    async def test_put_object_upsert(self, store_with_bucket):
        """Putting the same object again updates it (upsert)."""
        await store_with_bucket.put_object("test-bucket", "key1", 10, '"e1"', "text/plain")
        await store_with_bucket.put_object("test-bucket", "key1", 20, '"e2"', "application/json")
        obj = await store_with_bucket.get_object("test-bucket", "key1")
        assert obj is not None
        assert obj["size"] == 20
        assert obj["etag"] == '"e2"'
        assert obj["content_type"] == "application/json"

    async def test_delete_object(self, store_with_bucket):
        """Deleting an object removes it."""
        await store_with_bucket.put_object("test-bucket", "key1", 10, '"e1"', "text/plain")
        await store_with_bucket.delete_object("test-bucket", "key1")
        obj = await store_with_bucket.get_object("test-bucket", "key1")
        assert obj is None

    async def test_delete_nonexistent_object(self, store_with_bucket):
        """Deleting a non-existent object does not raise."""
        await store_with_bucket.delete_object("test-bucket", "no-such-key")

    async def test_delete_objects_meta_batch(self, store_with_bucket):
        """Batch delete returns list of actually deleted keys."""
        await store_with_bucket.put_object("test-bucket", "a", 1, '"a"', "text/plain")
        await store_with_bucket.put_object("test-bucket", "b", 2, '"b"', "text/plain")
        await store_with_bucket.put_object("test-bucket", "c", 3, '"c"', "text/plain")
        deleted = await store_with_bucket.delete_objects_meta(
            "test-bucket", ["a", "c", "nonexistent"]
        )
        assert sorted(deleted) == ["a", "c"]
        # b should still exist
        assert await store_with_bucket.object_exists("test-bucket", "b") is True
        assert await store_with_bucket.object_exists("test-bucket", "a") is False

    async def test_put_object_with_all_fields(self, store_with_bucket):
        """Putting an object with all optional fields stores them."""
        meta = json.dumps({"x-amz-meta-author": "Test"})
        acl = json.dumps({"owner": {"id": "o1"}, "grants": []})
        await store_with_bucket.put_object(
            bucket="test-bucket",
            key="full.txt",
            size=100,
            etag='"full"',
            content_type="text/html",
            content_encoding="gzip",
            content_language="en",
            content_disposition="attachment",
            cache_control="max-age=3600",
            expires="Thu, 01 Jan 2099 00:00:00 GMT",
            storage_class="STANDARD",
            acl=acl,
            user_metadata=meta,
        )
        obj = await store_with_bucket.get_object("test-bucket", "full.txt")
        assert obj is not None
        assert obj["content_encoding"] == "gzip"
        assert obj["content_language"] == "en"
        assert obj["content_disposition"] == "attachment"
        assert obj["cache_control"] == "max-age=3600"
        assert obj["expires"] == "Thu, 01 Jan 2099 00:00:00 GMT"
        assert json.loads(obj["user_metadata"]) == {"x-amz-meta-author": "Test"}
        assert json.loads(obj["acl"])["owner"]["id"] == "o1"

    async def test_update_object_acl(self, store_with_bucket):
        """Updating an object's ACL changes it."""
        await store_with_bucket.put_object("test-bucket", "key1", 10, '"e1"', "text/plain")
        new_acl = json.dumps({"grants": [{"permission": "WRITE"}]})
        await store_with_bucket.update_object_acl("test-bucket", "key1", new_acl)
        obj = await store_with_bucket.get_object("test-bucket", "key1")
        assert obj is not None
        parsed = json.loads(obj["acl"])
        assert parsed["grants"][0]["permission"] == "WRITE"

    async def test_count_objects(self, store_with_bucket):
        """count_objects returns the correct count."""
        assert await store_with_bucket.count_objects("test-bucket") == 0
        await store_with_bucket.put_object("test-bucket", "a", 1, '"a"', "text/plain")
        await store_with_bucket.put_object("test-bucket", "b", 2, '"b"', "text/plain")
        assert await store_with_bucket.count_objects("test-bucket") == 2

    async def test_cascade_delete_bucket_removes_objects(self, store_with_bucket):
        """Deleting a bucket cascades to its objects (foreign key)."""
        await store_with_bucket.put_object("test-bucket", "key1", 10, '"e1"', "text/plain")
        await store_with_bucket.delete_bucket("test-bucket")
        obj = await store_with_bucket.get_object("test-bucket", "key1")
        assert obj is None


# ---------------------------------------------------------------------------
# List objects with prefix/delimiter/pagination
# ---------------------------------------------------------------------------


class TestListObjects:
    """Tests for listing objects with prefix, delimiter, and pagination."""

    async def _seed_objects(self, store):
        """Seed objects for list testing."""
        keys = [
            "photos/2023/january/photo1.jpg",
            "photos/2023/january/photo2.jpg",
            "photos/2023/february/photo3.jpg",
            "photos/2024/march/photo4.jpg",
            "documents/readme.txt",
            "documents/notes.txt",
            "root.txt",
        ]
        for key in keys:
            await store.put_object("test-bucket", key, 100, f'"{key}"', "image/jpeg")

    async def test_list_all_objects(self, store_with_bucket):
        """List all objects without filters."""
        await self._seed_objects(store_with_bucket)
        result = await store_with_bucket.list_objects("test-bucket")
        assert len(result["contents"]) == 7
        assert result["is_truncated"] is False
        # Should be sorted by key
        keys = [c["key"] for c in result["contents"]]
        assert keys == sorted(keys)

    async def test_list_with_prefix(self, store_with_bucket):
        """List objects filtered by prefix."""
        await self._seed_objects(store_with_bucket)
        result = await store_with_bucket.list_objects("test-bucket", prefix="photos/")
        keys = [c["key"] for c in result["contents"]]
        assert len(keys) == 4
        assert all(k.startswith("photos/") for k in keys)

    async def test_list_with_delimiter(self, store_with_bucket):
        """List objects with delimiter groups into CommonPrefixes."""
        await self._seed_objects(store_with_bucket)
        result = await store_with_bucket.list_objects("test-bucket", delimiter="/")
        keys = [c["key"] for c in result["contents"]]
        assert keys == ["root.txt"]
        assert sorted(result["common_prefixes"]) == ["documents/", "photos/"]

    async def test_list_with_prefix_and_delimiter(self, store_with_bucket):
        """Prefix + delimiter simulates folder listing."""
        await self._seed_objects(store_with_bucket)
        result = await store_with_bucket.list_objects(
            "test-bucket", prefix="photos/", delimiter="/"
        )
        assert result["contents"] == []
        assert sorted(result["common_prefixes"]) == ["photos/2023/", "photos/2024/"]

    async def test_list_with_prefix_delimiter_deeper(self, store_with_bucket):
        """Deeper prefix + delimiter listing."""
        await self._seed_objects(store_with_bucket)
        result = await store_with_bucket.list_objects(
            "test-bucket", prefix="photos/2023/", delimiter="/"
        )
        assert result["contents"] == []
        assert sorted(result["common_prefixes"]) == [
            "photos/2023/february/",
            "photos/2023/january/",
        ]

    async def test_list_with_max_keys(self, store_with_bucket):
        """MaxKeys limits the number of results and sets is_truncated."""
        await self._seed_objects(store_with_bucket)
        result = await store_with_bucket.list_objects("test-bucket", max_keys=3)
        assert len(result["contents"]) == 3
        assert result["is_truncated"] is True
        assert result["next_continuation_token"] is not None

    async def test_list_pagination_with_continuation_token(self, store_with_bucket):
        """Pagination using continuation_token."""
        await self._seed_objects(store_with_bucket)

        # Page 1
        page1 = await store_with_bucket.list_objects("test-bucket", max_keys=3)
        assert len(page1["contents"]) == 3
        assert page1["is_truncated"] is True
        token = page1["next_continuation_token"]

        # Page 2
        page2 = await store_with_bucket.list_objects(
            "test-bucket", max_keys=3, continuation_token=token
        )
        assert len(page2["contents"]) == 3
        assert page2["is_truncated"] is True

        # Page 3 (last)
        token2 = page2["next_continuation_token"]
        page3 = await store_with_bucket.list_objects(
            "test-bucket", max_keys=3, continuation_token=token2
        )
        assert len(page3["contents"]) == 1
        assert page3["is_truncated"] is False

        # All keys collected
        all_keys = (
            [c["key"] for c in page1["contents"]]
            + [c["key"] for c in page2["contents"]]
            + [c["key"] for c in page3["contents"]]
        )
        assert len(all_keys) == 7
        assert all_keys == sorted(all_keys)

    async def test_list_pagination_with_marker(self, store_with_bucket):
        """Pagination using marker (v1 style)."""
        await self._seed_objects(store_with_bucket)

        page1 = await store_with_bucket.list_objects("test-bucket", max_keys=3)
        marker = page1["next_marker"]

        page2 = await store_with_bucket.list_objects("test-bucket", max_keys=3, marker=marker)
        assert len(page2["contents"]) == 3
        # All keys in page2 should be after the marker
        for c in page2["contents"]:
            assert c["key"] > marker

    async def test_list_empty_bucket(self, store_with_bucket):
        """Listing an empty bucket returns empty contents."""
        result = await store_with_bucket.list_objects("test-bucket")
        assert result["contents"] == []
        assert result["common_prefixes"] == []
        assert result["is_truncated"] is False
        assert result["key_count"] == 0

    async def test_list_key_count(self, store_with_bucket):
        """key_count reflects total returned items (contents + common_prefixes)."""
        await self._seed_objects(store_with_bucket)
        result = await store_with_bucket.list_objects("test-bucket", delimiter="/")
        # root.txt + 2 common prefixes = 3
        assert result["key_count"] == 3

    async def test_list_objects_have_required_fields(self, store_with_bucket):
        """Listed objects contain key, size, etag, last_modified, storage_class."""
        await store_with_bucket.put_object("test-bucket", "file.txt", 42, '"abc"', "text/plain")
        result = await store_with_bucket.list_objects("test-bucket")
        assert len(result["contents"]) == 1
        obj = result["contents"][0]
        assert "key" in obj
        assert "size" in obj
        assert "etag" in obj
        assert "last_modified" in obj
        assert "storage_class" in obj


# ---------------------------------------------------------------------------
# Multipart upload lifecycle
# ---------------------------------------------------------------------------


class TestMultipartLifecycle:
    """Tests for multipart upload create, part upload, complete, and abort."""

    async def test_create_and_get_upload(self, store_with_bucket):
        """Creating a multipart upload and retrieving it works."""
        await store_with_bucket.create_multipart_upload(
            bucket="test-bucket",
            key="large-file.bin",
            upload_id="upload-001",
            content_type="application/octet-stream",
            owner_id="owner1",
            owner_display="Owner One",
        )
        upload = await store_with_bucket.get_multipart_upload(
            "test-bucket", "large-file.bin", "upload-001"
        )
        assert upload is not None
        assert upload["upload_id"] == "upload-001"
        assert upload["bucket"] == "test-bucket"
        assert upload["key"] == "large-file.bin"
        assert upload["owner_id"] == "owner1"
        assert upload["initiated_at"] != ""

    async def test_get_nonexistent_upload(self, store_with_bucket):
        """Getting a non-existent upload returns None."""
        upload = await store_with_bucket.get_multipart_upload(
            "test-bucket", "key", "no-such-upload"
        )
        assert upload is None

    async def test_put_and_list_parts(self, store_with_bucket):
        """Uploading parts and listing them returns correct data."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "key", "upload-002", owner_id="o1"
        )
        await store_with_bucket.put_part("upload-002", 1, 5 * 1024 * 1024, '"p1"')
        await store_with_bucket.put_part("upload-002", 2, 5 * 1024 * 1024, '"p2"')
        await store_with_bucket.put_part("upload-002", 3, 1000, '"p3"')

        result = await store_with_bucket.list_parts("upload-002")
        assert len(result["parts"]) == 3
        assert result["parts"][0]["part_number"] == 1
        assert result["parts"][1]["part_number"] == 2
        assert result["parts"][2]["part_number"] == 3
        assert result["is_truncated"] is False

    async def test_put_part_upsert(self, store_with_bucket):
        """Re-uploading the same part number replaces it."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "key", "upload-003", owner_id="o1"
        )
        await store_with_bucket.put_part("upload-003", 1, 1000, '"old"')
        await store_with_bucket.put_part("upload-003", 1, 2000, '"new"')

        result = await store_with_bucket.list_parts("upload-003")
        assert len(result["parts"]) == 1
        assert result["parts"][0]["size"] == 2000
        assert result["parts"][0]["etag"] == '"new"'

    async def test_get_parts_for_completion(self, store_with_bucket):
        """get_parts_for_completion returns parts ordered by part_number."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "key", "upload-004", owner_id="o1"
        )
        await store_with_bucket.put_part("upload-004", 3, 1000, '"p3"')
        await store_with_bucket.put_part("upload-004", 1, 5000, '"p1"')
        await store_with_bucket.put_part("upload-004", 2, 3000, '"p2"')

        parts = await store_with_bucket.get_parts_for_completion("upload-004")
        assert len(parts) == 3
        assert [p["part_number"] for p in parts] == [1, 2, 3]

    async def test_complete_multipart_upload(self, store_with_bucket):
        """Completing a multipart upload creates the object and removes the upload."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket",
            "key",
            "upload-005",
            content_type="application/pdf",
            user_metadata='{"x-amz-meta-doc": "yes"}',
            owner_id="o1",
        )
        await store_with_bucket.put_part("upload-005", 1, 5000, '"p1"')
        await store_with_bucket.put_part("upload-005", 2, 3000, '"p2"')

        await store_with_bucket.complete_multipart_upload(
            bucket="test-bucket",
            key="key",
            upload_id="upload-005",
            size=8000,
            etag='"composite-etag-2"',
            content_type="application/pdf",
            user_metadata='{"x-amz-meta-doc": "yes"}',
        )

        # Object should exist
        obj = await store_with_bucket.get_object("test-bucket", "key")
        assert obj is not None
        assert obj["size"] == 8000
        assert obj["etag"] == '"composite-etag-2"'
        assert obj["content_type"] == "application/pdf"

        # Upload should be gone
        upload = await store_with_bucket.get_multipart_upload("test-bucket", "key", "upload-005")
        assert upload is None

        # Parts should be gone
        parts = await store_with_bucket.get_parts_for_completion("upload-005")
        assert parts == []

    async def test_abort_multipart_upload(self, store_with_bucket):
        """Aborting a multipart upload removes the upload and its parts."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "key", "upload-006", owner_id="o1"
        )
        await store_with_bucket.put_part("upload-006", 1, 5000, '"p1"')
        await store_with_bucket.put_part("upload-006", 2, 3000, '"p2"')

        await store_with_bucket.abort_multipart_upload("test-bucket", "key", "upload-006")

        # Upload should be gone
        upload = await store_with_bucket.get_multipart_upload("test-bucket", "key", "upload-006")
        assert upload is None

        # Parts should be gone
        parts = await store_with_bucket.get_parts_for_completion("upload-006")
        assert parts == []

    async def test_list_parts_pagination(self, store_with_bucket):
        """list_parts pagination with part_number_marker and max_parts."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "key", "upload-007", owner_id="o1"
        )
        for i in range(1, 6):
            await store_with_bucket.put_part("upload-007", i, 1000 * i, f'"p{i}"')

        # Page 1: parts 1-2
        page1 = await store_with_bucket.list_parts("upload-007", max_parts=2)
        assert len(page1["parts"]) == 2
        assert page1["is_truncated"] is True
        assert page1["next_part_number_marker"] == 2

        # Page 2: parts 3-4
        page2 = await store_with_bucket.list_parts("upload-007", part_number_marker=2, max_parts=2)
        assert len(page2["parts"]) == 2
        assert page2["parts"][0]["part_number"] == 3
        assert page2["is_truncated"] is True

        # Page 3: part 5 (last)
        page3 = await store_with_bucket.list_parts("upload-007", part_number_marker=4, max_parts=2)
        assert len(page3["parts"]) == 1
        assert page3["is_truncated"] is False

    async def test_list_multipart_uploads(self, store_with_bucket):
        """Listing multipart uploads returns all active uploads."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "key-a", "upload-a", owner_id="o1"
        )
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "key-b", "upload-b", owner_id="o1"
        )
        result = await store_with_bucket.list_multipart_uploads("test-bucket")
        assert len(result["uploads"]) == 2
        keys = [u["key"] for u in result["uploads"]]
        assert keys == ["key-a", "key-b"]

    async def test_list_multipart_uploads_with_prefix(self, store_with_bucket):
        """Listing uploads with prefix filters correctly."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "docs/a.txt", "u1", owner_id="o1"
        )
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "docs/b.txt", "u2", owner_id="o1"
        )
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "images/c.jpg", "u3", owner_id="o1"
        )
        result = await store_with_bucket.list_multipart_uploads("test-bucket", prefix="docs/")
        assert len(result["uploads"]) == 2
        keys = [u["key"] for u in result["uploads"]]
        assert all(k.startswith("docs/") for k in keys)

    async def test_list_multipart_uploads_empty(self, store_with_bucket):
        """Listing uploads when none exist returns empty."""
        result = await store_with_bucket.list_multipart_uploads("test-bucket")
        assert result["uploads"] == []
        assert result["is_truncated"] is False

    async def test_create_upload_with_all_fields(self, store_with_bucket):
        """Creating an upload with all optional fields stores them."""
        await store_with_bucket.create_multipart_upload(
            bucket="test-bucket",
            key="full-upload",
            upload_id="upload-full",
            content_type="video/mp4",
            content_encoding="gzip",
            content_language="en",
            content_disposition="inline",
            cache_control="no-cache",
            expires="Fri, 01 Jan 2100 00:00:00 GMT",
            storage_class="STANDARD",
            acl='{"owner":{"id":"o1"}}',
            user_metadata='{"x-amz-meta-type":"video"}',
            owner_id="o1",
            owner_display="Owner",
        )
        upload = await store_with_bucket.get_multipart_upload(
            "test-bucket", "full-upload", "upload-full"
        )
        assert upload is not None
        assert upload["content_type"] == "video/mp4"
        assert upload["content_encoding"] == "gzip"
        assert upload["content_language"] == "en"
        assert upload["content_disposition"] == "inline"
        assert upload["cache_control"] == "no-cache"
        assert upload["storage_class"] == "STANDARD"
        assert upload["owner_id"] == "o1"

    async def test_cascade_delete_bucket_removes_uploads(self, store_with_bucket):
        """Deleting a bucket cascades to its multipart uploads."""
        await store_with_bucket.create_multipart_upload(
            "test-bucket", "key", "upload-cascade", owner_id="o1"
        )
        await store_with_bucket.put_part("upload-cascade", 1, 1000, '"p1"')
        await store_with_bucket.delete_bucket("test-bucket")

        upload = await store_with_bucket.get_multipart_upload(
            "test-bucket", "key", "upload-cascade"
        )
        assert upload is None


# ---------------------------------------------------------------------------
# Credential operations
# ---------------------------------------------------------------------------


class TestCredentials:
    """Tests for credential CRUD operations."""

    async def test_put_and_get_credential(self, store):
        """Creating a credential and retrieving it works."""
        await store.put_credential(
            access_key_id="AKID123",
            secret_key="secret-abc",
            owner_id="owner1",
            display_name="Test User",
        )
        cred = await store.get_credential("AKID123")
        assert cred is not None
        assert cred["access_key_id"] == "AKID123"
        assert cred["secret_key"] == "secret-abc"
        assert cred["owner_id"] == "owner1"
        assert cred["display_name"] == "Test User"
        assert cred["active"] == 1
        assert cred["created_at"] != ""

    async def test_get_nonexistent_credential(self, store):
        """Getting a credential that doesn't exist returns None."""
        cred = await store.get_credential("NO-SUCH-KEY")
        assert cred is None

    async def test_put_credential_upsert(self, store):
        """Updating an existing credential replaces it."""
        await store.put_credential("AKID123", "secret1", "o1", "User1")
        await store.put_credential("AKID123", "secret2", "o1", "User1-Updated")
        cred = await store.get_credential("AKID123")
        assert cred is not None
        assert cred["secret_key"] == "secret2"
        assert cred["display_name"] == "User1-Updated"

    async def test_inactive_credential_not_returned(self, store):
        """Deactivated credentials are not returned by get_credential."""
        await store.put_credential("AKID456", "secret", "o1", "User")
        # Manually deactivate
        assert store._db is not None
        await store._db.execute(
            "UPDATE credentials SET active = 0 WHERE access_key_id = ?",
            ("AKID456",),
        )
        await store._db.commit()
        cred = await store.get_credential("AKID456")
        assert cred is None

    async def test_multiple_credentials(self, store):
        """Multiple credentials can coexist."""
        await store.put_credential("KEY1", "s1", "o1", "User1")
        await store.put_credential("KEY2", "s2", "o2", "User2")
        c1 = await store.get_credential("KEY1")
        c2 = await store.get_credential("KEY2")
        assert c1 is not None and c1["owner_id"] == "o1"
        assert c2 is not None and c2["owner_id"] == "o2"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge case and boundary tests."""

    async def test_object_key_with_slashes(self, store_with_bucket):
        """Object keys containing forward slashes work correctly."""
        await store_with_bucket.put_object(
            "test-bucket", "path/to/deep/file.txt", 10, '"e"', "text/plain"
        )
        obj = await store_with_bucket.get_object("test-bucket", "path/to/deep/file.txt")
        assert obj is not None
        assert obj["key"] == "path/to/deep/file.txt"

    async def test_object_key_with_special_chars(self, store_with_bucket):
        """Object keys with special characters work correctly."""
        key = "files/report (2024).pdf"
        await store_with_bucket.put_object("test-bucket", key, 10, '"e"', "application/pdf")
        obj = await store_with_bucket.get_object("test-bucket", key)
        assert obj is not None
        assert obj["key"] == key

    async def test_empty_prefix_list(self, store_with_bucket):
        """Listing with empty prefix returns all objects."""
        await store_with_bucket.put_object("test-bucket", "a", 1, '"a"', "text/plain")
        await store_with_bucket.put_object("test-bucket", "b", 2, '"b"', "text/plain")
        result = await store_with_bucket.list_objects("test-bucket", prefix="")
        assert len(result["contents"]) == 2

    async def test_list_with_nonexistent_prefix(self, store_with_bucket):
        """Listing with a prefix that matches no objects returns empty."""
        await store_with_bucket.put_object("test-bucket", "a", 1, '"a"', "text/plain")
        result = await store_with_bucket.list_objects("test-bucket", prefix="zzz/")
        assert result["contents"] == []
        assert result["common_prefixes"] == []

    async def test_max_keys_zero(self, store_with_bucket):
        """max_keys=0 returns no objects."""
        await store_with_bucket.put_object("test-bucket", "a", 1, '"a"', "text/plain")
        result = await store_with_bucket.list_objects("test-bucket", max_keys=0)
        assert result["contents"] == []

    async def test_user_metadata_roundtrip(self, store_with_bucket):
        """JSON user metadata survives a put/get round-trip."""
        meta = {"x-amz-meta-key1": "value1", "x-amz-meta-key2": "value2"}
        await store_with_bucket.put_object(
            "test-bucket",
            "meta-test",
            1,
            '"e"',
            "text/plain",
            user_metadata=json.dumps(meta),
        )
        obj = await store_with_bucket.get_object("test-bucket", "meta-test")
        assert obj is not None
        assert json.loads(obj["user_metadata"]) == meta

    async def test_acl_json_roundtrip(self, store_with_bucket):
        """Complex ACL JSON survives a round-trip."""
        acl = {
            "owner": {"id": "abc123", "display_name": "owner@example.com"},
            "grants": [
                {
                    "grantee": {
                        "type": "CanonicalUser",
                        "id": "abc123",
                        "display_name": "owner@example.com",
                    },
                    "permission": "FULL_CONTROL",
                },
                {
                    "grantee": {
                        "type": "Group",
                        "uri": "http://acs.amazonaws.com/groups/global/AllUsers",
                    },
                    "permission": "READ",
                },
            ],
        }
        await store_with_bucket.put_object(
            "test-bucket",
            "acl-test",
            1,
            '"e"',
            "text/plain",
            acl=json.dumps(acl),
        )
        obj = await store_with_bucket.get_object("test-bucket", "acl-test")
        assert obj is not None
        assert json.loads(obj["acl"]) == acl
