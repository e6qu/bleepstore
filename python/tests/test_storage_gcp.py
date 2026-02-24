"""Unit tests for the GCP Cloud Storage gateway backend.

All tests use mocked gcloud-aio-storage — no real GCP credentials or network
access required. The mock Storage client is injected directly onto
backend._client to bypass session creation.
"""

import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bleepstore.storage.gcp import GCPGatewayBackend, _is_not_found


def _not_found_error(message: str = "Not Found") -> Exception:
    """Create a mock 404 error mimicking aiohttp.ClientResponseError."""
    exc = Exception(message)
    exc.status = 404  # type: ignore[attr-defined]
    return exc


def _other_error(status: int = 403, message: str = "Forbidden") -> Exception:
    """Create a mock non-404 error."""
    exc = Exception(message)
    exc.status = status  # type: ignore[attr-defined]
    return exc


def _make_backend(bucket="test-bucket", project="test-project", prefix=""):
    """Create a GCPGatewayBackend with a mock client (skip init)."""
    backend = GCPGatewayBackend(
        bucket_name=bucket, project=project, prefix=prefix
    )
    backend._client = AsyncMock()
    return backend


class TestKeyMapping:
    """Tests for internal key mapping helpers."""

    def test_gcs_name_no_prefix(self):
        backend = _make_backend(prefix="")
        assert backend._gcs_name("mybucket", "mykey") == "mybucket/mykey"

    def test_gcs_name_with_prefix(self):
        backend = _make_backend(prefix="prod/")
        assert backend._gcs_name("mybucket", "mykey") == "prod/mybucket/mykey"

    def test_gcs_name_nested_key(self):
        backend = _make_backend(prefix="")
        assert backend._gcs_name("b", "a/b/c.txt") == "b/a/b/c.txt"

    def test_part_name_no_prefix(self):
        backend = _make_backend(prefix="")
        assert backend._part_name("uid123", 1) == ".parts/uid123/1"

    def test_part_name_with_prefix(self):
        backend = _make_backend(prefix="dev/")
        assert backend._part_name("uid123", 5) == "dev/.parts/uid123/5"


class TestInit:
    """Tests for init() and close()."""

    async def test_init_verifies_bucket(self):
        """init() calls list_objects to verify the upstream bucket exists."""
        with patch("bleepstore.storage.gcp.Storage") as mock_storage_cls:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(return_value={"items": []})
            mock_storage_cls.return_value = mock_client

            backend = GCPGatewayBackend(bucket_name="my-bucket", project="proj")
            await backend.init()

            mock_client.list_objects.assert_awaited_once_with(
                "my-bucket",
                params={"maxResults": "1"},
            )
            await backend.close()

    async def test_init_raises_on_missing_bucket(self):
        """init() raises ValueError if the upstream bucket doesn't exist."""
        with patch("bleepstore.storage.gcp.Storage") as mock_storage_cls:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(
                side_effect=_not_found_error("Bucket not found")
            )
            mock_storage_cls.return_value = mock_client

            backend = GCPGatewayBackend(bucket_name="no-such-bucket")
            with pytest.raises(ValueError, match="Cannot access upstream GCS bucket"):
                await backend.init()

            # Client should be closed after failure
            mock_client.close.assert_awaited_once()

    async def test_close_closes_client(self):
        """close() closes the underlying client."""
        backend = _make_backend()
        client_ref = backend._client
        await backend.close()
        client_ref.close.assert_awaited_once()
        assert backend._client is None

    async def test_close_noop_when_not_initialized(self):
        """close() is safe to call when not initialized."""
        backend = GCPGatewayBackend(bucket_name="b")
        await backend.close()  # Should not raise


class TestPut:
    """Tests for put()."""

    async def test_put_returns_md5(self):
        backend = _make_backend()
        data = b"hello world"
        expected_md5 = hashlib.md5(data).hexdigest()

        result = await backend.put("bucket", "key", data)

        assert result == expected_md5
        backend._client.upload.assert_awaited_once_with(
            "test-bucket", "bucket/key", data
        )

    async def test_put_with_prefix(self):
        backend = _make_backend(prefix="pfx/")
        await backend.put("b", "k", b"data")
        backend._client.upload.assert_awaited_once_with(
            "test-bucket", "pfx/b/k", b"data"
        )

    async def test_put_empty_data(self):
        backend = _make_backend()
        result = await backend.put("b", "k", b"")
        assert result == hashlib.md5(b"").hexdigest()


class TestGet:
    """Tests for get()."""

    async def test_get_returns_bytes(self):
        backend = _make_backend()
        backend._client.download = AsyncMock(return_value=b"content")

        result = await backend.get("bucket", "key")
        assert result == b"content"

    async def test_get_not_found_raises_file_not_found(self):
        backend = _make_backend()
        backend._client.download = AsyncMock(side_effect=_not_found_error())

        with pytest.raises(FileNotFoundError, match="Object not found"):
            await backend.get("bucket", "key")

    async def test_get_other_error_propagates(self):
        backend = _make_backend()
        backend._client.download = AsyncMock(side_effect=_other_error())

        with pytest.raises(Exception, match="Forbidden"):
            await backend.get("bucket", "key")


class TestGetStream:
    """Tests for get_stream()."""

    async def test_get_stream_yields_chunks(self):
        backend = _make_backend()
        mock_stream = AsyncMock()
        mock_stream.read = AsyncMock(side_effect=[b"chunk1", b"chunk2", b""])
        backend._client.download_stream = AsyncMock(return_value=mock_stream)

        result = []
        async for chunk in backend.get_stream("b", "k"):
            result.append(chunk)

        assert result == [b"chunk1", b"chunk2"]

    async def test_get_stream_with_offset(self):
        backend = _make_backend()
        mock_stream = AsyncMock()
        mock_stream.read = AsyncMock(side_effect=[b"data", b""])
        backend._client.download_stream = AsyncMock(return_value=mock_stream)

        result = []
        async for chunk in backend.get_stream("b", "k", offset=100):
            result.append(chunk)

        call_kwargs = backend._client.download_stream.call_args[1]
        assert call_kwargs["headers"]["Range"] == "bytes=100-"

    async def test_get_stream_with_offset_and_length(self):
        backend = _make_backend()
        mock_stream = AsyncMock()
        mock_stream.read = AsyncMock(side_effect=[b"data", b""])
        backend._client.download_stream = AsyncMock(return_value=mock_stream)

        result = []
        async for chunk in backend.get_stream("b", "k", offset=10, length=50):
            result.append(chunk)

        call_kwargs = backend._client.download_stream.call_args[1]
        assert call_kwargs["headers"]["Range"] == "bytes=10-59"

    async def test_get_stream_not_found(self):
        backend = _make_backend()
        backend._client.download_stream = AsyncMock(
            side_effect=_not_found_error()
        )

        with pytest.raises(FileNotFoundError):
            async for _ in backend.get_stream("b", "k"):
                pass


class TestDelete:
    """Tests for delete()."""

    async def test_delete_calls_delete(self):
        backend = _make_backend()
        await backend.delete("bucket", "key")
        backend._client.delete.assert_awaited_once_with(
            "test-bucket", "bucket/key"
        )

    async def test_delete_idempotent_on_404(self):
        """delete() silently ignores 404 errors (idempotent)."""
        backend = _make_backend()
        backend._client.delete = AsyncMock(side_effect=_not_found_error())

        await backend.delete("bucket", "key")  # Should not raise

    async def test_delete_other_error_propagates(self):
        backend = _make_backend()
        backend._client.delete = AsyncMock(side_effect=_other_error())

        with pytest.raises(Exception, match="Forbidden"):
            await backend.delete("bucket", "key")


class TestExists:
    """Tests for exists()."""

    async def test_exists_true(self):
        backend = _make_backend()
        backend._client.download = AsyncMock(return_value=b"\x00")

        assert await backend.exists("b", "k") is True

    async def test_exists_false_on_404(self):
        backend = _make_backend()
        backend._client.download = AsyncMock(side_effect=_not_found_error())

        assert await backend.exists("b", "k") is False

    async def test_exists_uses_range_header(self):
        """exists() uses Range: bytes=0-0 to avoid full download."""
        backend = _make_backend()
        backend._client.download = AsyncMock(return_value=b"\x00")

        await backend.exists("b", "k")

        call_kwargs = backend._client.download.call_args[1]
        assert call_kwargs["headers"]["Range"] == "bytes=0-0"

    async def test_exists_other_error_propagates(self):
        backend = _make_backend()
        backend._client.download = AsyncMock(side_effect=_other_error())

        with pytest.raises(Exception, match="Forbidden"):
            await backend.exists("b", "k")


class TestCopyObject:
    """Tests for copy_object()."""

    async def test_copy_object_server_side(self):
        backend = _make_backend()
        backend._client.copy = AsyncMock(return_value={})
        backend._client.download = AsyncMock(return_value=b"copied-data")
        expected_md5 = hashlib.md5(b"copied-data").hexdigest()

        result = await backend.copy_object("src-b", "src-k", "dst-b", "dst-k")

        assert result == expected_md5
        backend._client.copy.assert_awaited_once_with(
            "test-bucket",
            "src-b/src-k",
            "test-bucket",
            new_name="dst-b/dst-k",
        )

    async def test_copy_object_with_prefix(self):
        backend = _make_backend(prefix="pfx/")
        backend._client.copy = AsyncMock(return_value={})
        backend._client.download = AsyncMock(return_value=b"data")

        await backend.copy_object("src-b", "src-k", "dst-b", "dst-k")

        backend._client.copy.assert_awaited_once_with(
            "test-bucket",
            "pfx/src-b/src-k",
            "test-bucket",
            new_name="pfx/dst-b/dst-k",
        )


class TestPutPart:
    """Tests for put_part()."""

    async def test_put_part_returns_md5(self):
        backend = _make_backend()
        data = b"part data"
        expected_md5 = hashlib.md5(data).hexdigest()

        result = await backend.put_part("b", "k", "uid", 1, data)

        assert result == expected_md5
        backend._client.upload.assert_awaited_once_with(
            "test-bucket",
            ".parts/uid/1",
            data,
        )


class TestAssembleParts:
    """Tests for assemble_parts()."""

    async def test_single_compose(self):
        """≤32 parts uses a single compose call."""
        backend = _make_backend()
        backend._client.compose = AsyncMock(return_value={})
        final_data = b"assembled"
        backend._client.download = AsyncMock(return_value=final_data)

        result = await backend.assemble_parts("b", "k", "uid", [1, 2, 3])

        assert result == hashlib.md5(final_data).hexdigest()
        backend._client.compose.assert_awaited_once_with(
            "test-bucket",
            "b/k",
            [".parts/uid/1", ".parts/uid/2", ".parts/uid/3"],
        )

    async def test_single_part(self):
        """Single part still uses compose."""
        backend = _make_backend()
        backend._client.compose = AsyncMock(return_value={})
        backend._client.download = AsyncMock(return_value=b"single")

        result = await backend.assemble_parts("b", "k", "uid", [1])

        assert result == hashlib.md5(b"single").hexdigest()
        backend._client.compose.assert_awaited_once()

    async def test_chain_compose_over_32_parts(self):
        """For >32 parts, chains compose calls and cleans up intermediates."""
        backend = _make_backend()
        backend._client.compose = AsyncMock(return_value={})
        backend._client.download = AsyncMock(return_value=b"big-assembled")
        backend._client.delete = AsyncMock()

        # 33 parts: should produce 2 batches (32 + 1), then a final compose
        part_numbers = list(range(1, 34))
        result = await backend.assemble_parts("b", "k", "uid", part_numbers)

        assert result == hashlib.md5(b"big-assembled").hexdigest()
        # Should have called compose multiple times:
        # - First round: 2 calls (batch of 32, batch of 1 is passthrough)
        # - Final compose: 1 call (2 sources)
        # Total: 2 compose calls (first batch of 32 → intermediate, then final with intermediate + single part)
        assert backend._client.compose.await_count >= 2
        # Should have cleaned up intermediate objects
        assert backend._client.delete.await_count >= 1

    async def test_chain_compose_64_parts(self):
        """64 parts: 2 batches of 32, then final compose."""
        backend = _make_backend()
        backend._client.compose = AsyncMock(return_value={})
        backend._client.download = AsyncMock(return_value=b"assembled-64")
        backend._client.delete = AsyncMock()

        part_numbers = list(range(1, 65))
        result = await backend.assemble_parts("b", "k", "uid", part_numbers)

        assert result == hashlib.md5(b"assembled-64").hexdigest()
        # Round 1: 2 compose calls (32 + 32)
        # Round 2: 1 final compose (2 intermediates)
        assert backend._client.compose.await_count == 3
        # 2 intermediates cleaned up
        assert backend._client.delete.await_count == 2

    async def test_assemble_with_prefix(self):
        backend = _make_backend(prefix="pfx/")
        backend._client.compose = AsyncMock(return_value={})
        backend._client.download = AsyncMock(return_value=b"data")

        await backend.assemble_parts("b", "k", "uid", [1, 2])

        backend._client.compose.assert_awaited_once_with(
            "test-bucket",
            "pfx/b/k",
            ["pfx/.parts/uid/1", "pfx/.parts/uid/2"],
        )


class TestDeleteParts:
    """Tests for delete_parts()."""

    async def test_delete_parts_deletes_all(self):
        backend = _make_backend()
        backend._client.list_objects = AsyncMock(
            return_value={
                "items": [
                    {"name": ".parts/uid/1"},
                    {"name": ".parts/uid/2"},
                ]
            }
        )

        await backend.delete_parts("b", "k", "uid")

        assert backend._client.delete.await_count == 2
        backend._client.delete.assert_any_await("test-bucket", ".parts/uid/1")
        backend._client.delete.assert_any_await("test-bucket", ".parts/uid/2")

    async def test_delete_parts_empty(self):
        """delete_parts is a no-op when no parts exist."""
        backend = _make_backend()
        backend._client.list_objects = AsyncMock(return_value={})

        await backend.delete_parts("b", "k", "uid")
        backend._client.delete.assert_not_awaited()

    async def test_delete_parts_ignores_404(self):
        """delete_parts silently ignores 404 on individual part deletes."""
        backend = _make_backend()
        backend._client.list_objects = AsyncMock(
            return_value={
                "items": [{"name": ".parts/uid/1"}]
            }
        )
        backend._client.delete = AsyncMock(side_effect=_not_found_error())

        await backend.delete_parts("b", "k", "uid")  # Should not raise


class TestIsNotFound:
    """Tests for the _is_not_found helper."""

    def test_status_404(self):
        assert _is_not_found(_not_found_error()) is True

    def test_status_403(self):
        assert _is_not_found(_other_error(403)) is False

    def test_plain_exception(self):
        assert _is_not_found(Exception("something")) is False

    def test_message_with_404(self):
        assert _is_not_found(Exception("404 Not Found")) is True


class TestServerFactory:
    """Tests for the server.py backend factory with GCP backend."""

    def test_gcp_backend_requires_bucket(self):
        """Factory raises ValueError when gcp_bucket is not set."""
        from bleepstore.config import BleepStoreConfig, StorageConfig
        from bleepstore.server import _create_storage_backend

        config = BleepStoreConfig(
            storage=StorageConfig(backend="gcp", gcp_bucket="")
        )

        with pytest.raises(ValueError, match="gcp.bucket.*required"):
            _create_storage_backend(config)

    def test_gcp_backend_creates_instance(self):
        """Factory creates GCPGatewayBackend with correct config."""
        from bleepstore.config import BleepStoreConfig, StorageConfig
        from bleepstore.server import _create_storage_backend

        config = BleepStoreConfig(
            storage=StorageConfig(
                backend="gcp",
                gcp_bucket="my-gcs-bucket",
                gcp_project="my-project",
                gcp_prefix="test/",
            )
        )

        backend = _create_storage_backend(config)
        assert isinstance(backend, GCPGatewayBackend)
        assert backend.bucket_name == "my-gcs-bucket"
        assert backend.project == "my-project"
        assert backend.prefix == "test/"
