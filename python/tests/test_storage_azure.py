"""Unit tests for the Azure Blob Storage gateway backend.

All tests use mocked azure-storage-blob — no real Azure credentials or network
access required. The mock ContainerClient is injected directly onto
backend._container_client to bypass session creation.
"""

import base64
import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError

from bleepstore.storage.azure import AzureGatewayBackend


def _make_backend(container="test-container", account_url="https://test.blob.core.windows.net", prefix=""):
    """Create an AzureGatewayBackend with a mock client (skip init)."""
    backend = AzureGatewayBackend(
        container_name=container, account_url=account_url, prefix=prefix
    )
    backend._container_client = AsyncMock()
    backend._credential = AsyncMock()
    return backend


def _mock_blob_client():
    """Create a mock BlobClient with common async methods."""
    blob = AsyncMock()
    blob.upload_blob = AsyncMock()
    blob.delete_blob = AsyncMock()
    blob.exists = AsyncMock(return_value=True)
    blob.stage_block = AsyncMock()
    blob.commit_block_list = AsyncMock()
    blob.start_copy_from_url = AsyncMock()
    return blob


def _setup_blob_client(backend):
    """Wire up get_blob_client to return a consistent mock BlobClient."""
    blob = _mock_blob_client()
    backend._container_client.get_blob_client = MagicMock(return_value=blob)
    return blob


class TestKeyMapping:
    """Tests for internal key mapping helpers."""

    def test_blob_name_no_prefix(self):
        backend = _make_backend(prefix="")
        assert backend._blob_name("mybucket", "mykey") == "mybucket/mykey"

    def test_blob_name_with_prefix(self):
        backend = _make_backend(prefix="prod/")
        assert backend._blob_name("mybucket", "mykey") == "prod/mybucket/mykey"

    def test_blob_name_nested_key(self):
        backend = _make_backend(prefix="")
        assert backend._blob_name("b", "a/b/c.txt") == "b/a/b/c.txt"


class TestBlockId:
    """Tests for block ID generation."""

    def test_block_id_format(self):
        block_id = AzureGatewayBackend._block_id("upload123", 1)
        decoded = base64.b64decode(block_id).decode()
        assert decoded == "upload123:00001"

    def test_block_id_padding(self):
        block_id = AzureGatewayBackend._block_id("uid", 42)
        decoded = base64.b64decode(block_id).decode()
        assert decoded == "uid:00042"

    def test_block_id_is_base64(self):
        block_id = AzureGatewayBackend._block_id("uid", 1)
        # Should decode without error
        base64.b64decode(block_id)

    def test_block_id_includes_upload_id(self):
        """Different upload_ids produce different block IDs for same part number."""
        id1 = AzureGatewayBackend._block_id("upload-A", 1)
        id2 = AzureGatewayBackend._block_id("upload-B", 1)
        assert id1 != id2


class TestInit:
    """Tests for init() and close()."""

    async def test_init_verifies_container_exists(self):
        """init() checks container existence via exists()."""
        with patch("bleepstore.storage.azure.DefaultAzureCredential") as mock_cred_cls, \
             patch("bleepstore.storage.azure.ContainerClient") as mock_cc_cls:
            mock_cred = AsyncMock()
            mock_cred_cls.return_value = mock_cred
            mock_cc = AsyncMock()
            mock_cc.exists = AsyncMock(return_value=True)
            mock_cc_cls.return_value = mock_cc

            backend = AzureGatewayBackend(
                container_name="my-container",
                account_url="https://acct.blob.core.windows.net",
            )
            await backend.init()

            mock_cc.exists.assert_awaited_once()
            await backend.close()

    async def test_init_raises_on_missing_container(self):
        """init() raises ValueError if the container doesn't exist."""
        with patch("bleepstore.storage.azure.DefaultAzureCredential") as mock_cred_cls, \
             patch("bleepstore.storage.azure.ContainerClient") as mock_cc_cls:
            mock_cred = AsyncMock()
            mock_cred_cls.return_value = mock_cred
            mock_cc = AsyncMock()
            mock_cc.exists = AsyncMock(return_value=False)
            mock_cc_cls.return_value = mock_cc

            backend = AzureGatewayBackend(container_name="no-such-container")
            with pytest.raises(ValueError, match="does not exist"):
                await backend.init()

            mock_cc.close.assert_awaited_once()

    async def test_init_raises_on_access_error(self):
        """init() raises ValueError if container check throws."""
        with patch("bleepstore.storage.azure.DefaultAzureCredential") as mock_cred_cls, \
             patch("bleepstore.storage.azure.ContainerClient") as mock_cc_cls:
            mock_cred = AsyncMock()
            mock_cred_cls.return_value = mock_cred
            mock_cc = AsyncMock()
            mock_cc.exists = AsyncMock(side_effect=Exception("auth failed"))
            mock_cc_cls.return_value = mock_cc

            backend = AzureGatewayBackend(container_name="bad-container")
            with pytest.raises(ValueError, match="Cannot access upstream Azure container"):
                await backend.init()

    async def test_close_closes_client(self):
        """close() closes the underlying client and credential."""
        backend = _make_backend()
        cc_ref = backend._container_client
        cred_ref = backend._credential
        await backend.close()
        cc_ref.close.assert_awaited_once()
        cred_ref.close.assert_awaited_once()
        assert backend._container_client is None
        assert backend._credential is None

    async def test_close_noop_when_not_initialized(self):
        """close() is safe to call when not initialized."""
        backend = AzureGatewayBackend(container_name="c")
        await backend.close()  # Should not raise


class TestPut:
    """Tests for put()."""

    async def test_put_returns_md5(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        data = b"hello world"
        expected_md5 = hashlib.md5(data).hexdigest()

        result = await backend.put("bucket", "key", data)

        assert result == expected_md5
        blob.upload_blob.assert_awaited_once_with(data, overwrite=True)

    async def test_put_uses_correct_blob_name(self):
        backend = _make_backend(prefix="pfx/")
        blob = _setup_blob_client(backend)

        await backend.put("b", "k", b"data")

        backend._container_client.get_blob_client.assert_called_once_with("pfx/b/k")

    async def test_put_empty_data(self):
        backend = _make_backend()
        _setup_blob_client(backend)
        result = await backend.put("b", "k", b"")
        assert result == hashlib.md5(b"").hexdigest()


class TestGet:
    """Tests for get()."""

    async def test_get_returns_bytes(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()
        mock_downloader.readall = AsyncMock(return_value=b"content")
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        result = await backend.get("bucket", "key")
        assert result == b"content"

    async def test_get_not_found_raises_file_not_found(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        blob.download_blob = AsyncMock(
            side_effect=ResourceNotFoundError("Blob not found")
        )

        with pytest.raises(FileNotFoundError, match="Object not found"):
            await backend.get("bucket", "key")

    async def test_get_other_error_propagates(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        blob.download_blob = AsyncMock(side_effect=RuntimeError("server error"))

        with pytest.raises(RuntimeError, match="server error"):
            await backend.get("bucket", "key")


class TestGetStream:
    """Tests for get_stream()."""

    async def test_get_stream_yields_chunks(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()

        async def mock_chunks():
            yield b"chunk1"
            yield b"chunk2"

        mock_downloader.chunks = mock_chunks
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        result = []
        async for chunk in backend.get_stream("b", "k"):
            result.append(chunk)

        assert result == [b"chunk1", b"chunk2"]

    async def test_get_stream_with_offset(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()

        async def mock_chunks():
            yield b"data"

        mock_downloader.chunks = mock_chunks
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        result = []
        async for chunk in backend.get_stream("b", "k", offset=100):
            result.append(chunk)

        blob.download_blob.assert_awaited_once_with(offset=100)

    async def test_get_stream_with_offset_and_length(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()

        async def mock_chunks():
            yield b"data"

        mock_downloader.chunks = mock_chunks
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        result = []
        async for chunk in backend.get_stream("b", "k", offset=10, length=50):
            result.append(chunk)

        blob.download_blob.assert_awaited_once_with(offset=10, length=50)

    async def test_get_stream_not_found(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        blob.download_blob = AsyncMock(
            side_effect=ResourceNotFoundError("Blob not found")
        )

        with pytest.raises(FileNotFoundError):
            async for _ in backend.get_stream("b", "k"):
                pass


class TestDelete:
    """Tests for delete()."""

    async def test_delete_calls_delete_blob(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)

        await backend.delete("bucket", "key")
        blob.delete_blob.assert_awaited_once()

    async def test_delete_idempotent_on_not_found(self):
        """delete() silently ignores ResourceNotFoundError (idempotent)."""
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        blob.delete_blob = AsyncMock(
            side_effect=ResourceNotFoundError("not found")
        )

        await backend.delete("bucket", "key")  # Should not raise

    async def test_delete_other_error_propagates(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        blob.delete_blob = AsyncMock(side_effect=RuntimeError("server error"))

        with pytest.raises(RuntimeError, match="server error"):
            await backend.delete("bucket", "key")


class TestExists:
    """Tests for exists()."""

    async def test_exists_true(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        blob.exists = AsyncMock(return_value=True)

        assert await backend.exists("b", "k") is True

    async def test_exists_false(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        blob.exists = AsyncMock(return_value=False)

        assert await backend.exists("b", "k") is False

    async def test_exists_uses_correct_blob_name(self):
        backend = _make_backend(prefix="pfx/")
        blob = _setup_blob_client(backend)
        blob.exists = AsyncMock(return_value=True)

        await backend.exists("b", "k")
        backend._container_client.get_blob_client.assert_called_once_with("pfx/b/k")


class TestPutPart:
    """Tests for put_part()."""

    async def test_put_part_returns_md5(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        data = b"part data"
        expected_md5 = hashlib.md5(data).hexdigest()

        result = await backend.put_part("b", "k", "uid", 1, data)

        assert result == expected_md5

    async def test_put_part_stages_block(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        data = b"part data"

        await backend.put_part("b", "k", "uid", 1, data)

        expected_block_id = AzureGatewayBackend._block_id("uid", 1)
        blob.stage_block.assert_awaited_once_with(
            expected_block_id, data, length=len(data)
        )

    async def test_put_part_uses_final_blob_name(self):
        """put_part stages blocks on the final blob, not a temp object."""
        backend = _make_backend(prefix="pfx/")
        blob = _setup_blob_client(backend)

        await backend.put_part("b", "k", "uid", 1, b"data")

        backend._container_client.get_blob_client.assert_called_once_with("pfx/b/k")


class TestAssembleParts:
    """Tests for assemble_parts()."""

    async def test_assemble_commits_block_list(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()
        mock_downloader.readall = AsyncMock(return_value=b"assembled")
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        result = await backend.assemble_parts("b", "k", "uid", [1, 2, 3])

        assert result == hashlib.md5(b"assembled").hexdigest()
        blob.commit_block_list.assert_awaited_once()

        # Verify block list contents
        call_args = blob.commit_block_list.call_args[0][0]
        assert len(call_args) == 3
        for i, block in enumerate(call_args, 1):
            expected_id = AzureGatewayBackend._block_id("uid", i)
            assert block.id == expected_id

    async def test_assemble_single_part(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()
        mock_downloader.readall = AsyncMock(return_value=b"single")
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        result = await backend.assemble_parts("b", "k", "uid", [1])

        assert result == hashlib.md5(b"single").hexdigest()
        blob.commit_block_list.assert_awaited_once()

    async def test_assemble_downloads_to_compute_md5(self):
        """assemble_parts downloads the final blob to compute MD5."""
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()
        mock_downloader.readall = AsyncMock(return_value=b"final-data")
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        result = await backend.assemble_parts("b", "k", "uid", [1])

        blob.download_blob.assert_awaited_once()
        assert result == hashlib.md5(b"final-data").hexdigest()


class TestDeleteParts:
    """Tests for delete_parts()."""

    async def test_delete_parts_is_noop(self):
        """delete_parts is a no-op for Azure (blocks auto-expire)."""
        backend = _make_backend()
        blob = _setup_blob_client(backend)

        await backend.delete_parts("b", "k", "uid")

        # No blob operations should be called
        blob.delete_blob.assert_not_awaited()


class TestCopyObject:
    """Tests for copy_object()."""

    async def test_copy_object_server_side(self):
        backend = _make_backend()
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()
        mock_downloader.readall = AsyncMock(return_value=b"copied-data")
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        result = await backend.copy_object("src-b", "src-k", "dst-b", "dst-k")

        assert result == hashlib.md5(b"copied-data").hexdigest()
        blob.start_copy_from_url.assert_awaited_once()

    async def test_copy_object_source_url(self):
        """copy_object builds correct source URL."""
        backend = _make_backend(
            account_url="https://myacct.blob.core.windows.net",
            prefix="pfx/",
        )
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()
        mock_downloader.readall = AsyncMock(return_value=b"data")
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        await backend.copy_object("src-b", "src-k", "dst-b", "dst-k")

        expected_url = "https://myacct.blob.core.windows.net/test-container/pfx/src-b/src-k"
        blob.start_copy_from_url.assert_awaited_once_with(expected_url)

    async def test_copy_object_uses_dst_blob_name(self):
        """copy_object gets BlobClient for the destination blob."""
        backend = _make_backend(prefix="")
        blob = _setup_blob_client(backend)
        mock_downloader = AsyncMock()
        mock_downloader.readall = AsyncMock(return_value=b"data")
        blob.download_blob = AsyncMock(return_value=mock_downloader)

        await backend.copy_object("src-b", "src-k", "dst-b", "dst-k")

        backend._container_client.get_blob_client.assert_called_with("dst-b/dst-k")


class TestServerFactory:
    """Tests for the server.py backend factory with Azure backend."""

    def test_azure_backend_requires_container(self):
        """Factory raises ValueError when azure_container is not set."""
        from bleepstore.config import BleepStoreConfig, StorageConfig
        from bleepstore.server import _create_storage_backend

        config = BleepStoreConfig(
            storage=StorageConfig(backend="azure", azure_container="")
        )

        with pytest.raises(ValueError, match="azure.container.*required"):
            _create_storage_backend(config)

    def test_azure_backend_creates_instance(self):
        """Factory creates AzureGatewayBackend with correct config."""
        from bleepstore.config import BleepStoreConfig, StorageConfig
        from bleepstore.server import _create_storage_backend

        config = BleepStoreConfig(
            storage=StorageConfig(
                backend="azure",
                azure_container="my-container",
                azure_account="https://myacct.blob.core.windows.net",
                azure_prefix="test/",
            )
        )

        backend = _create_storage_backend(config)
        assert isinstance(backend, AzureGatewayBackend)
        assert backend.container_name == "my-container"
        assert backend.account_url == "https://myacct.blob.core.windows.net"
        assert backend.prefix == "test/"
