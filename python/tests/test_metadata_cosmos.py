"""Unit tests for CosmosMetadataStore.

NOTE: These tests require a real Cosmos DB account or Cosmos DB Emulator running.
To run these tests:
1. Start Cosmos DB Emulator or use a real Cosmos DB account
2. Set environment:
   export COSMOS_TEST_ENDPOINT="https://localhost:8081"  (emulator)
   export COSMOS_TEST_KEY="C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
3. Run: uv run pytest tests/test_metadata_cosmos.py -v

For CI, these tests are skipped by default.
"""

import os
import uuid

import pytest

from bleepstore.config import CosmosConfig

pytestmark = pytest.mark.skipif(
    not os.environ.get("COSMOS_TEST_ENDPOINT"),
    reason="Set COSMOS_TEST_ENDPOINT to run Cosmos DB tests",
)


@pytest.fixture
def cosmos_config():
    unique_id = uuid.uuid4().hex[:8]
    return CosmosConfig(
        database=f"test-bleepstore-{unique_id}",
        container="metadata",
        endpoint=os.environ.get("COSMOS_TEST_ENDPOINT"),
        connection_string=os.environ.get("COSMOS_TEST_CONNECTION_STRING"),
    )


@pytest.fixture
async def cosmos_store(cosmos_config):
    from bleepstore.metadata.cosmos import CosmosMetadataStore

    store = CosmosMetadataStore(cosmos_config)
    await store.init_db()
    yield store
    await store.close()


class TestBucketOperations:
    """Tests for bucket CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_bucket(self, cosmos_store):
        await cosmos_store.create_bucket("test-bucket", "us-west-2", "owner1", "Owner One")
        bucket = await cosmos_store.get_bucket("test-bucket")
        assert bucket is not None
        assert bucket["name"] == "test-bucket"
        assert bucket["region"] == "us-west-2"
        assert bucket["owner_id"] == "owner1"

    @pytest.mark.asyncio
    async def test_bucket_exists(self, cosmos_store):
        await cosmos_store.create_bucket("exists-bucket")
        assert await cosmos_store.bucket_exists("exists-bucket") is True
        assert await cosmos_store.bucket_exists("no-such-bucket") is False

    @pytest.mark.asyncio
    async def test_delete_bucket(self, cosmos_store):
        await cosmos_store.create_bucket("delete-me")
        assert await cosmos_store.bucket_exists("delete-me") is True

        await cosmos_store.delete_bucket("delete-me")
        assert await cosmos_store.bucket_exists("delete-me") is False

    @pytest.mark.asyncio
    async def test_list_buckets(self, cosmos_store):
        await cosmos_store.create_bucket("list-bucket-1", owner_id="owner1")
        await cosmos_store.create_bucket("list-bucket-2", owner_id="owner1")
        await cosmos_store.create_bucket("list-bucket-3", owner_id="owner2")

        all_buckets = await cosmos_store.list_buckets()
        assert len(all_buckets) >= 3

        owner1_buckets = await cosmos_store.list_buckets(owner_id="owner1")
        assert len(owner1_buckets) >= 2

    @pytest.mark.asyncio
    async def test_update_bucket_acl(self, cosmos_store):
        await cosmos_store.create_bucket("acl-bucket")
        await cosmos_store.update_bucket_acl("acl-bucket", '{"private": true}')

        bucket = await cosmos_store.get_bucket("acl-bucket")
        assert bucket["acl"] == '{"private": true}'


class TestObjectOperations:
    """Tests for object CRUD operations."""

    @pytest.mark.asyncio
    async def test_put_and_get_object(self, cosmos_store):
        await cosmos_store.create_bucket("obj-bucket")
        await cosmos_store.put_object(
            bucket="obj-bucket",
            key="test/key.txt",
            size=1024,
            etag='"abc123"',
            content_type="text/plain",
        )

        obj = await cosmos_store.get_object("obj-bucket", "test/key.txt")
        assert obj is not None
        assert obj["key"] == "test/key.txt"
        assert obj["size"] == 1024
        assert obj["etag"] == '"abc123"'

    @pytest.mark.asyncio
    async def test_object_exists(self, cosmos_store):
        await cosmos_store.create_bucket("exists-obj-bucket")
        await cosmos_store.put_object("exists-obj-bucket", "exists.txt", 100, '"etag"')

        assert await cosmos_store.object_exists("exists-obj-bucket", "exists.txt") is True
        assert await cosmos_store.object_exists("exists-obj-bucket", "nope.txt") is False

    @pytest.mark.asyncio
    async def test_delete_object(self, cosmos_store):
        await cosmos_store.create_bucket("del-obj-bucket")
        await cosmos_store.put_object("del-obj-bucket", "delete-me.txt", 100, '"etag"')
        assert await cosmos_store.object_exists("del-obj-bucket", "delete-me.txt") is True

        await cosmos_store.delete_object("del-obj-bucket", "delete-me.txt")
        assert await cosmos_store.object_exists("del-obj-bucket", "delete-me.txt") is False

    @pytest.mark.asyncio
    async def test_delete_objects_meta(self, cosmos_store):
        await cosmos_store.create_bucket("batch-del-bucket")
        await cosmos_store.put_object("batch-del-bucket", "file1.txt", 100, '"e1"')
        await cosmos_store.put_object("batch-del-bucket", "file2.txt", 100, '"e2"')
        await cosmos_store.put_object("batch-del-bucket", "file3.txt", 100, '"e3"')

        deleted = await cosmos_store.delete_objects_meta(
            "batch-del-bucket", ["file1.txt", "file2.txt", "nope.txt"]
        )
        assert set(deleted) == {"file1.txt", "file2.txt"}

    @pytest.mark.asyncio
    async def test_list_objects(self, cosmos_store):
        await cosmos_store.create_bucket("list-obj-bucket")
        await cosmos_store.put_object("list-obj-bucket", "a/1.txt", 100, '"e1"')
        await cosmos_store.put_object("list-obj-bucket", "a/2.txt", 100, '"e2"')
        await cosmos_store.put_object("list-obj-bucket", "b/1.txt", 100, '"e3"')

        result = await cosmos_store.list_objects("list-obj-bucket", prefix="a/")
        assert len(result["contents"]) == 2
        assert result["is_truncated"] is False

    @pytest.mark.asyncio
    async def test_list_objects_pagination(self, cosmos_store):
        await cosmos_store.create_bucket("page-bucket")
        for i in range(5):
            await cosmos_store.put_object("page-bucket", f"file{i:03d}.txt", 100, f'"e{i}"')

        page1 = await cosmos_store.list_objects("page-bucket", max_keys=2)
        assert len(page1["contents"]) == 2
        assert page1["is_truncated"] is True

        page2 = await cosmos_store.list_objects(
            "page-bucket", max_keys=2, continuation_token=page1["next_continuation_token"]
        )
        assert len(page2["contents"]) == 2


class TestMultipartOperations:
    """Tests for multipart upload operations."""

    @pytest.mark.asyncio
    async def test_create_and_get_multipart_upload(self, cosmos_store):
        await cosmos_store.create_bucket("mp-bucket")
        upload_id = uuid.uuid4().hex

        await cosmos_store.create_multipart_upload(
            bucket="mp-bucket",
            key="multipart.dat",
            upload_id=upload_id,
            content_type="application/octet-stream",
        )

        upload = await cosmos_store.get_multipart_upload("mp-bucket", "multipart.dat", upload_id)
        assert upload is not None
        assert upload["upload_id"] == upload_id
        assert upload["key"] == "multipart.dat"

    @pytest.mark.asyncio
    async def test_put_and_list_parts(self, cosmos_store):
        await cosmos_store.create_bucket("parts-bucket")
        upload_id = uuid.uuid4().hex

        await cosmos_store.create_multipart_upload(
            "parts-bucket", "parts.dat", upload_id, content_type="application/octet-stream"
        )

        await cosmos_store.put_part(upload_id, 1, 1024, '"part1"')
        await cosmos_store.put_part(upload_id, 2, 2048, '"part2"')

        parts = await cosmos_store.get_parts_for_completion(upload_id)
        assert len(parts) == 2
        assert parts[0]["part_number"] == 1
        assert parts[1]["part_number"] == 2

    @pytest.mark.asyncio
    async def test_complete_multipart_upload(self, cosmos_store):
        await cosmos_store.create_bucket("complete-bucket")
        upload_id = uuid.uuid4().hex

        await cosmos_store.create_multipart_upload("complete-bucket", "complete.dat", upload_id)
        await cosmos_store.put_part(upload_id, 1, 1024, '"part1"')
        await cosmos_store.put_part(upload_id, 2, 2048, '"part2"')

        await cosmos_store.complete_multipart_upload(
            bucket="complete-bucket",
            key="complete.dat",
            upload_id=upload_id,
            size=3072,
            etag='"composite"',
        )

        obj = await cosmos_store.get_object("complete-bucket", "complete.dat")
        assert obj is not None
        assert obj["size"] == 3072

        upload = await cosmos_store.get_multipart_upload(
            "complete-bucket", "complete.dat", upload_id
        )
        assert upload is None

    @pytest.mark.asyncio
    async def test_abort_multipart_upload(self, cosmos_store):
        await cosmos_store.create_bucket("abort-bucket")
        upload_id = uuid.uuid4().hex

        await cosmos_store.create_multipart_upload("abort-bucket", "abort.dat", upload_id)
        await cosmos_store.put_part(upload_id, 1, 1024, '"part1"')

        await cosmos_store.abort_multipart_upload("abort-bucket", "abort.dat", upload_id)

        upload = await cosmos_store.get_multipart_upload("abort-bucket", "abort.dat", upload_id)
        assert upload is None

        parts = await cosmos_store.get_parts_for_completion(upload_id)
        assert len(parts) == 0

    @pytest.mark.asyncio
    async def test_list_multipart_uploads(self, cosmos_store):
        await cosmos_store.create_bucket("list-mp-bucket")
        upload_id1 = uuid.uuid4().hex
        upload_id2 = uuid.uuid4().hex

        await cosmos_store.create_multipart_upload("list-mp-bucket", "file1.dat", upload_id1)
        await cosmos_store.create_multipart_upload("list-mp-bucket", "file2.dat", upload_id2)

        result = await cosmos_store.list_multipart_uploads("list-mp-bucket")
        assert len(result["uploads"]) >= 2


class TestCredentialOperations:
    """Tests for credential CRUD operations."""

    @pytest.mark.asyncio
    async def test_put_and_get_credential(self, cosmos_store):
        await cosmos_store.put_credential(
            access_key_id="test-key-id",
            secret_key="test-secret",
            owner_id="owner1",
            display_name="Test User",
        )

        cred = await cosmos_store.get_credential("test-key-id")
        assert cred is not None
        assert cred["secret_key"] == "test-secret"
        assert cred["owner_id"] == "owner1"

    @pytest.mark.asyncio
    async def test_get_nonexistent_credential(self, cosmos_store):
        cred = await cosmos_store.get_credential("no-such-key")
        assert cred is None


class TestCountAndReap:
    """Tests for count_objects and reap_expired_uploads."""

    @pytest.mark.asyncio
    async def test_count_objects(self, cosmos_store):
        await cosmos_store.create_bucket("count-bucket")
        await cosmos_store.put_object("count-bucket", "file1.txt", 100, '"e1"')
        await cosmos_store.put_object("count-bucket", "file2.txt", 100, '"e2"')
        await cosmos_store.put_object("count-bucket", "file3.txt", 100, '"e3"')

        count = await cosmos_store.count_objects("count-bucket")
        assert count == 3

    @pytest.mark.asyncio
    async def test_reap_expired_uploads(self, cosmos_store):
        await cosmos_store.create_bucket("reap-bucket")
        upload_id = uuid.uuid4().hex

        await cosmos_store.create_multipart_upload("reap-bucket", "reap.dat", upload_id)
        await cosmos_store.put_part(upload_id, 1, 1024, '"part1"')

        reaped = await cosmos_store.reap_expired_uploads(ttl_seconds=0)
        reap_ids = [r["upload_id"] for r in reaped]

        assert upload_id in reap_ids
