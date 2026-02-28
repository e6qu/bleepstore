"""Unit tests for DynamoDBMetadataStore.

NOTE: These tests require a real DynamoDB table or DynamoDB Local running.
The moto mock library does not properly support aiobotocore's async client.

To run these tests:
1. Start DynamoDB Local: docker run -p 8000:8000 amazon/dynamodb-local
2. Set environment: export DYNAMODB_TEST_ENDPOINT=http://localhost:8000
3. Run: uv run pytest tests/test_metadata_dynamodb.py -v

For CI, these tests are skipped by default.
"""

import os

import pytest

from bleepstore.config import DynamoDBConfig

# Skip all tests in this file unless explicitly enabled
pytestmark = pytest.mark.skipif(
    not os.environ.get("DYNAMODB_TEST_ENDPOINT"),
    reason="Set DYNAMODB_TEST_ENDPOINT to run DynamoDB tests",
)


@pytest.fixture
def dynamodb_config():
    return DynamoDBConfig(
        table="test-bleepstore-metadata",
        region="us-east-1",
        endpoint_url=os.environ.get("DYNAMODB_TEST_ENDPOINT"),
    )


async def _create_table(dynamodb_config):
    """Create the test DynamoDB table."""
    import boto3

    client = boto3.client(
        "dynamodb",
        region_name=dynamodb_config.region,
        endpoint_url=dynamodb_config.endpoint_url,
    )
    try:
        client.create_table(
            TableName=dynamodb_config.table,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    except client.exceptions.ResourceInUseException:
        pass


class TestBucketOperations:
    """Tests for bucket CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_bucket(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("test-bucket", "us-west-2", "owner1", "Owner One")
        bucket = await store.get_bucket("test-bucket")
        assert bucket is not None
        assert bucket["name"] == "test-bucket"
        assert bucket["region"] == "us-west-2"
        assert bucket["owner_id"] == "owner1"

        await store.close()

    @pytest.mark.asyncio
    async def test_bucket_exists(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("exists-bucket")
        assert await store.bucket_exists("exists-bucket") is True
        assert await store.bucket_exists("no-such-bucket") is False

        await store.close()

    @pytest.mark.asyncio
    async def test_delete_bucket(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("delete-me")
        assert await store.bucket_exists("delete-me") is True

        await store.delete_bucket("delete-me")
        assert await store.bucket_exists("delete-me") is False

        await store.close()

    @pytest.mark.asyncio
    async def test_list_buckets(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("bucket-a")
        await store.create_bucket("bucket-b")
        await store.create_bucket("bucket-c")

        buckets = await store.list_buckets()
        names = [b["name"] for b in buckets]
        assert "bucket-a" in names
        assert "bucket-b" in names
        assert "bucket-c" in names

        await store.close()

    @pytest.mark.asyncio
    async def test_update_bucket_acl(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("acl-bucket")
        await store.update_bucket_acl("acl-bucket", '{"private": true}')

        bucket = await store.get_bucket("acl-bucket")
        assert bucket["acl"] == '{"private": true}'

        await store.close()


class TestObjectOperations:
    """Tests for object CRUD operations."""

    @pytest.mark.asyncio
    async def test_put_and_get_object(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("obj-bucket")
        await store.put_object(
            bucket="obj-bucket",
            key="test.txt",
            size=100,
            etag='"abc123"',
            content_type="text/plain",
        )

        obj = await store.get_object("obj-bucket", "test.txt")
        assert obj is not None
        assert obj["bucket"] == "obj-bucket"
        assert obj["key"] == "test.txt"
        assert obj["size"] == 100
        assert obj["etag"] == '"abc123"'
        assert obj["content_type"] == "text/plain"

        await store.close()

    @pytest.mark.asyncio
    async def test_object_exists(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("exists-obj-bucket")
        await store.put_object("exists-obj-bucket", "exists.txt", 10, '"x"')

        assert await store.object_exists("exists-obj-bucket", "exists.txt") is True
        assert await store.object_exists("exists-obj-bucket", "no-such.txt") is False

        await store.close()

    @pytest.mark.asyncio
    async def test_delete_object(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("del-obj-bucket")
        await store.put_object("del-obj-bucket", "delete.txt", 10, '"x"')

        await store.delete_object("del-obj-bucket", "delete.txt")
        assert await store.object_exists("del-obj-bucket", "delete.txt") is False

        await store.close()

    @pytest.mark.asyncio
    async def test_list_objects(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("list-obj-bucket")
        await store.put_object("list-obj-bucket", "a.txt", 10, '"a"')
        await store.put_object("list-obj-bucket", "b.txt", 20, '"b"')
        await store.put_object("list-obj-bucket", "c.txt", 30, '"c"')

        result = await store.list_objects("list-obj-bucket")
        assert len(result["contents"]) == 3
        assert result["is_truncated"] is False
        assert result["key_count"] == 3

        await store.close()

    @pytest.mark.asyncio
    async def test_list_objects_with_prefix(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("prefix-bucket")
        await store.put_object("prefix-bucket", "photos/2024/1.jpg", 10, '"x"')
        await store.put_object("prefix-bucket", "photos/2024/2.jpg", 10, '"x"')
        await store.put_object("prefix-bucket", "docs/readme.md", 10, '"x"')

        result = await store.list_objects("prefix-bucket", prefix="photos/")
        assert len(result["contents"]) == 2
        keys = [o["key"] for o in result["contents"]]
        assert "photos/2024/1.jpg" in keys
        assert "photos/2024/2.jpg" in keys

        await store.close()


class TestMultipartOperations:
    """Tests for multipart upload operations."""

    @pytest.mark.asyncio
    async def test_create_multipart_upload(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("mp-bucket")
        await store.create_multipart_upload(
            bucket="mp-bucket",
            key="big-file.dat",
            upload_id="upload-123",
            content_type="application/octet-stream",
        )

        upload = await store.get_multipart_upload("mp-bucket", "big-file.dat", "upload-123")
        assert upload is not None
        assert upload["upload_id"] == "upload-123"
        assert upload["bucket"] == "mp-bucket"
        assert upload["key"] == "big-file.dat"

        await store.close()

    @pytest.mark.asyncio
    async def test_put_part_and_get_parts(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("part-bucket")
        await store.create_multipart_upload(
            bucket="part-bucket", key="file.dat", upload_id="upload-456"
        )

        await store.put_part("upload-456", 1, 5242880, '"part1-etag"')
        await store.put_part("upload-456", 2, 5242880, '"part2-etag"')

        parts = await store.get_parts_for_completion("upload-456")
        assert len(parts) == 2
        assert parts[0]["part_number"] == 1
        assert parts[1]["part_number"] == 2

        await store.close()

    @pytest.mark.asyncio
    async def test_complete_multipart_upload(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("complete-bucket")
        await store.create_multipart_upload(
            bucket="complete-bucket", key="file.dat", upload_id="upload-complete"
        )

        await store.put_part("upload-complete", 1, 1000, '"etag1"')
        await store.put_part("upload-complete", 2, 2000, '"etag2"')

        await store.complete_multipart_upload(
            bucket="complete-bucket",
            key="file.dat",
            upload_id="upload-complete",
            size=3000,
            etag='"composite-etag"',
        )

        obj = await store.get_object("complete-bucket", "file.dat")
        assert obj is not None
        assert obj["size"] == 3000
        assert obj["etag"] == '"composite-etag"'

        parts = await store.get_parts_for_completion("upload-complete")
        assert len(parts) == 0

        await store.close()

    @pytest.mark.asyncio
    async def test_abort_multipart_upload(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("abort-bucket")
        await store.create_multipart_upload(
            bucket="abort-bucket", key="file.dat", upload_id="upload-abort"
        )

        await store.put_part("upload-abort", 1, 1000, '"etag1"')

        await store.abort_multipart_upload("abort-bucket", "file.dat", "upload-abort")

        upload = await store.get_multipart_upload("abort-bucket", "file.dat", "upload-abort")
        assert upload is None

        parts = await store.get_parts_for_completion("upload-abort")
        assert len(parts) == 0

        await store.close()


class TestCredentialOperations:
    """Tests for credential operations."""

    @pytest.mark.asyncio
    async def test_put_and_get_credential(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.put_credential(
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            owner_id="owner1",
            display_name="Owner One",
        )

        cred = await store.get_credential("AKIAIOSFODNN7EXAMPLE")
        assert cred is not None
        assert cred["secret_key"] == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert cred["owner_id"] == "owner1"

        await store.close()

    @pytest.mark.asyncio
    async def test_get_credential_not_found(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        cred = await store.get_credential("NO-SUCH-KEY")
        assert cred is None

        await store.close()


class TestUtilityOperations:
    """Tests for utility operations."""

    @pytest.mark.asyncio
    async def test_count_objects(self, dynamodb_config):
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        await _create_table(dynamodb_config)
        store = DynamoDBMetadataStore(dynamodb_config)
        await store.init_db()

        await store.create_bucket("count-bucket")
        await store.put_object("count-bucket", "a.txt", 10, '"a"')
        await store.put_object("count-bucket", "b.txt", 20, '"b"')
        await store.put_object("count-bucket", "c.txt", 30, '"c"')

        count = await store.count_objects("count-bucket")
        assert count == 3

        await store.close()
