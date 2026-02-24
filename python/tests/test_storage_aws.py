"""Unit tests for the AWS S3 gateway storage backend.

All tests use mocked aiobotocore — no real AWS credentials or network
access required. The mock S3 client is injected directly onto
backend._client to bypass session creation.
"""

import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from bleepstore.storage.aws import AWSGatewayBackend


def _client_error(code: str, message: str = "error") -> ClientError:
    """Create a botocore ClientError with the given error code."""
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "TestOperation",
    )


def _make_backend(bucket="test-bucket", region="us-east-1", prefix=""):
    """Create an AWSGatewayBackend with a mock client (skip init)."""
    backend = AWSGatewayBackend(bucket_name=bucket, region=region, prefix=prefix)
    backend._client = AsyncMock()
    backend._client_ctx = AsyncMock()
    return backend


class TestKeyMapping:
    """Tests for internal key mapping helpers."""

    def test_s3_key_no_prefix(self):
        backend = _make_backend(prefix="")
        assert backend._s3_key("mybucket", "mykey") == "mybucket/mykey"

    def test_s3_key_with_prefix(self):
        backend = _make_backend(prefix="prod/")
        assert backend._s3_key("mybucket", "mykey") == "prod/mybucket/mykey"

    def test_s3_key_nested_key(self):
        backend = _make_backend(prefix="")
        assert backend._s3_key("b", "a/b/c.txt") == "b/a/b/c.txt"

    def test_part_key_no_prefix(self):
        backend = _make_backend(prefix="")
        assert backend._part_key("uid123", 1) == ".parts/uid123/1"

    def test_part_key_with_prefix(self):
        backend = _make_backend(prefix="dev/")
        assert backend._part_key("uid123", 5) == "dev/.parts/uid123/5"


class TestInit:
    """Tests for init() and close()."""

    async def test_init_verifies_bucket(self):
        """init() calls head_bucket to verify the upstream bucket exists."""
        with patch("bleepstore.storage.aws.AioSession") as mock_session_cls:
            mock_client = AsyncMock()
            mock_client.head_bucket = AsyncMock()
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
            mock_ctx.__aexit__ = AsyncMock(return_value=False)
            mock_session_cls.return_value.create_client.return_value = mock_ctx

            backend = AWSGatewayBackend(bucket_name="my-bucket", region="us-west-2")
            await backend.init()

            mock_client.head_bucket.assert_awaited_once_with(Bucket="my-bucket")
            await backend.close()

    async def test_init_raises_on_missing_bucket(self):
        """init() raises ValueError if the upstream bucket doesn't exist."""
        with patch("bleepstore.storage.aws.AioSession") as mock_session_cls:
            mock_client = AsyncMock()
            mock_client.head_bucket = AsyncMock(side_effect=_client_error("404", "Not Found"))
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
            mock_ctx.__aexit__ = AsyncMock(return_value=False)
            mock_session_cls.return_value.create_client.return_value = mock_ctx

            backend = AWSGatewayBackend(bucket_name="no-such-bucket")
            with pytest.raises(ValueError, match="Cannot access upstream S3 bucket"):
                await backend.init()

    async def test_close_exits_context(self):
        """close() exits the client context manager."""
        backend = _make_backend()
        ctx_ref = backend._client_ctx
        await backend.close()
        ctx_ref.__aexit__.assert_awaited_once()
        assert backend._client is None
        assert backend._client_ctx is None

    async def test_close_noop_when_not_initialized(self):
        """close() is safe to call when not initialized."""
        backend = AWSGatewayBackend(bucket_name="b")
        await backend.close()  # Should not raise


class TestPut:
    """Tests for put()."""

    async def test_put_returns_md5(self):
        backend = _make_backend()
        data = b"hello world"
        expected_md5 = hashlib.md5(data).hexdigest()

        result = await backend.put("bucket", "key", data)

        assert result == expected_md5
        backend._client.put_object.assert_awaited_once_with(
            Bucket="test-bucket", Key="bucket/key", Body=data
        )

    async def test_put_with_prefix(self):
        backend = _make_backend(prefix="pfx/")
        await backend.put("b", "k", b"data")
        backend._client.put_object.assert_awaited_once_with(
            Bucket="test-bucket", Key="pfx/b/k", Body=b"data"
        )

    async def test_put_empty_data(self):
        backend = _make_backend()
        result = await backend.put("b", "k", b"")
        assert result == hashlib.md5(b"").hexdigest()


class TestGet:
    """Tests for get()."""

    async def test_get_returns_bytes(self):
        backend = _make_backend()
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(return_value=b"content")
        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
        mock_body.__aexit__ = AsyncMock(return_value=False)
        backend._client.get_object = AsyncMock(return_value={"Body": mock_body})

        result = await backend.get("bucket", "key")
        assert result == b"content"

    async def test_get_not_found_raises_file_not_found(self):
        backend = _make_backend()
        backend._client.get_object = AsyncMock(side_effect=_client_error("NoSuchKey"))

        with pytest.raises(FileNotFoundError, match="Object not found"):
            await backend.get("bucket", "key")

    async def test_get_404_raises_file_not_found(self):
        backend = _make_backend()
        backend._client.get_object = AsyncMock(side_effect=_client_error("404"))

        with pytest.raises(FileNotFoundError):
            await backend.get("bucket", "key")

    async def test_get_other_error_propagates(self):
        backend = _make_backend()
        backend._client.get_object = AsyncMock(side_effect=_client_error("AccessDenied"))

        with pytest.raises(ClientError):
            await backend.get("bucket", "key")


class TestGetStream:
    """Tests for get_stream()."""

    async def test_get_stream_yields_chunks(self):
        backend = _make_backend()
        chunks = [b"chunk1", b"chunk2", b""]
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(side_effect=chunks)
        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
        mock_body.__aexit__ = AsyncMock(return_value=False)
        backend._client.get_object = AsyncMock(return_value={"Body": mock_body})

        result = []
        async for chunk in backend.get_stream("b", "k"):
            result.append(chunk)

        assert result == [b"chunk1", b"chunk2"]

    async def test_get_stream_with_offset(self):
        backend = _make_backend()
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(side_effect=[b"data", b""])
        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
        mock_body.__aexit__ = AsyncMock(return_value=False)
        backend._client.get_object = AsyncMock(return_value={"Body": mock_body})

        result = []
        async for chunk in backend.get_stream("b", "k", offset=100):
            result.append(chunk)

        call_kwargs = backend._client.get_object.call_args[1]
        assert call_kwargs["Range"] == "bytes=100-"

    async def test_get_stream_with_offset_and_length(self):
        backend = _make_backend()
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(side_effect=[b"data", b""])
        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
        mock_body.__aexit__ = AsyncMock(return_value=False)
        backend._client.get_object = AsyncMock(return_value={"Body": mock_body})

        result = []
        async for chunk in backend.get_stream("b", "k", offset=10, length=50):
            result.append(chunk)

        call_kwargs = backend._client.get_object.call_args[1]
        assert call_kwargs["Range"] == "bytes=10-59"

    async def test_get_stream_not_found(self):
        backend = _make_backend()
        backend._client.get_object = AsyncMock(side_effect=_client_error("NoSuchKey"))

        with pytest.raises(FileNotFoundError):
            async for _ in backend.get_stream("b", "k"):
                pass


class TestDelete:
    """Tests for delete()."""

    async def test_delete_calls_delete_object(self):
        backend = _make_backend()
        await backend.delete("bucket", "key")
        backend._client.delete_object.assert_awaited_once_with(
            Bucket="test-bucket", Key="bucket/key"
        )


class TestExists:
    """Tests for exists()."""

    async def test_exists_true(self):
        backend = _make_backend()
        backend._client.head_object = AsyncMock()
        assert await backend.exists("b", "k") is True

    async def test_exists_false_on_404(self):
        backend = _make_backend()
        backend._client.head_object = AsyncMock(side_effect=_client_error("404"))
        assert await backend.exists("b", "k") is False

    async def test_exists_false_on_no_such_key(self):
        backend = _make_backend()
        backend._client.head_object = AsyncMock(side_effect=_client_error("NoSuchKey"))
        assert await backend.exists("b", "k") is False

    async def test_exists_other_error_propagates(self):
        backend = _make_backend()
        backend._client.head_object = AsyncMock(side_effect=_client_error("AccessDenied"))
        with pytest.raises(ClientError):
            await backend.exists("b", "k")


class TestCopyObject:
    """Tests for copy_object()."""

    async def test_copy_object_server_side(self):
        backend = _make_backend()
        backend._client.copy_object = AsyncMock(
            return_value={"CopyObjectResult": {"ETag": '"abc123"'}}
        )

        result = await backend.copy_object("src-b", "src-k", "dst-b", "dst-k")

        assert result == "abc123"
        backend._client.copy_object.assert_awaited_once_with(
            Bucket="test-bucket",
            Key="dst-b/dst-k",
            CopySource={"Bucket": "test-bucket", "Key": "src-b/src-k"},
        )


class TestPutPart:
    """Tests for put_part()."""

    async def test_put_part_returns_md5(self):
        backend = _make_backend()
        data = b"part data"
        expected_md5 = hashlib.md5(data).hexdigest()

        result = await backend.put_part("b", "k", "uid", 1, data)

        assert result == expected_md5
        backend._client.put_object.assert_awaited_once_with(
            Bucket="test-bucket",
            Key=".parts/uid/1",
            Body=data,
        )


class TestAssembleParts:
    """Tests for assemble_parts()."""

    async def test_single_part_uses_copy(self):
        backend = _make_backend()
        backend._client.copy_object = AsyncMock(
            return_value={"CopyObjectResult": {"ETag": '"singlemd5"'}}
        )

        result = await backend.assemble_parts("b", "k", "uid", [1])

        assert result == "singlemd5"
        backend._client.copy_object.assert_awaited_once()
        # Should NOT create a multipart upload
        backend._client.create_multipart_upload.assert_not_awaited()

    async def test_multi_part_uses_multipart_upload(self):
        backend = _make_backend()
        backend._client.create_multipart_upload = AsyncMock(return_value={"UploadId": "aws-uid"})
        backend._client.upload_part_copy = AsyncMock(
            return_value={"CopyPartResult": {"ETag": '"partmd5"'}}
        )
        backend._client.complete_multipart_upload = AsyncMock(return_value={"ETag": '"final-etag"'})

        result = await backend.assemble_parts("b", "k", "uid", [1, 2, 3])

        assert result == "final-etag"
        assert backend._client.upload_part_copy.await_count == 3
        backend._client.complete_multipart_upload.assert_awaited_once()

    async def test_multi_part_entity_too_small_fallback(self):
        """When upload_part_copy fails with EntityTooSmall, falls back to download+reupload."""
        backend = _make_backend()
        backend._client.create_multipart_upload = AsyncMock(return_value={"UploadId": "aws-uid"})

        # upload_part_copy fails with EntityTooSmall for all parts
        def _make_body():
            body = AsyncMock()
            body.read = AsyncMock(return_value=b"small-data")
            body.__aenter__ = AsyncMock(return_value=body)
            body.__aexit__ = AsyncMock(return_value=False)
            return body

        backend._client.upload_part_copy = AsyncMock(side_effect=_client_error("EntityTooSmall"))
        backend._client.get_object = AsyncMock(side_effect=lambda **kw: {"Body": _make_body()})
        backend._client.upload_part = AsyncMock(return_value={"ETag": '"fallback-etag"'})
        backend._client.complete_multipart_upload = AsyncMock(return_value={"ETag": '"done"'})

        # Must use 2+ parts to trigger the multipart code path
        result = await backend.assemble_parts("b", "k", "uid", [1, 2])

        assert result == "done"
        assert backend._client.upload_part.await_count == 2

    async def test_multi_part_aborts_on_failure(self):
        """Assembly aborts the AWS multipart upload on unexpected errors."""
        backend = _make_backend()
        backend._client.create_multipart_upload = AsyncMock(return_value={"UploadId": "aws-uid"})
        backend._client.upload_part_copy = AsyncMock(side_effect=_client_error("InternalError"))
        backend._client.abort_multipart_upload = AsyncMock()

        with pytest.raises(ClientError):
            await backend.assemble_parts("b", "k", "uid", [1, 2])

        backend._client.abort_multipart_upload.assert_awaited_once()


class TestDeleteParts:
    """Tests for delete_parts()."""

    async def test_delete_parts_batch_deletes(self):
        backend = _make_backend()

        # Mock paginator
        mock_paginator = AsyncMock()
        page = {
            "Contents": [
                {"Key": ".parts/uid/1"},
                {"Key": ".parts/uid/2"},
            ]
        }

        async def _pages(**kwargs):
            yield page

        mock_paginator.paginate = MagicMock(return_value=_pages())
        backend._client.get_paginator = MagicMock(return_value=mock_paginator)

        await backend.delete_parts("b", "k", "uid")

        backend._client.delete_objects.assert_awaited_once_with(
            Bucket="test-bucket",
            Delete={
                "Objects": [{"Key": ".parts/uid/1"}, {"Key": ".parts/uid/2"}],
                "Quiet": True,
            },
        )

    async def test_delete_parts_empty(self):
        """delete_parts is a no-op when no parts exist."""
        backend = _make_backend()

        mock_paginator = AsyncMock()

        async def _pages(**kwargs):
            yield {"Contents": []}

        mock_paginator.paginate = MagicMock(return_value=_pages())
        backend._client.get_paginator = MagicMock(return_value=mock_paginator)

        await backend.delete_parts("b", "k", "uid")
        backend._client.delete_objects.assert_not_awaited()


class TestServerFactory:
    """Tests for the server.py backend factory with AWS backend."""

    def test_aws_backend_requires_bucket(self):
        """Factory raises ValueError when aws_bucket is not set."""
        from bleepstore.config import BleepStoreConfig, StorageConfig

        config = BleepStoreConfig(storage=StorageConfig(backend="aws", aws_bucket=""))
        from bleepstore.server import _create_storage_backend

        with pytest.raises(ValueError, match="aws.bucket.*required"):
            _create_storage_backend(config)

    def test_aws_backend_creates_instance(self):
        """Factory creates AWSGatewayBackend with correct config."""
        from bleepstore.config import BleepStoreConfig, StorageConfig

        config = BleepStoreConfig(
            storage=StorageConfig(
                backend="aws",
                aws_bucket="my-bucket",
                aws_region="eu-west-1",
                aws_prefix="test/",
            )
        )
        from bleepstore.server import _create_storage_backend

        backend = _create_storage_backend(config)
        assert isinstance(backend, AWSGatewayBackend)
        assert backend.bucket_name == "my-bucket"
        assert backend.region == "eu-west-1"
        assert backend.prefix == "test/"
