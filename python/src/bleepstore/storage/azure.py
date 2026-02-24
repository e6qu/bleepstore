"""Azure Blob Storage gateway backend for BleepStore.

Proxies all data operations to an upstream Azure Blob Storage container
via azure-storage-blob (async). Metadata stays in local SQLite — this
backend handles raw bytes only.

Key mapping:
    Objects:  {prefix}{bleepstore_bucket}/{key}

Multipart strategy uses Azure Block Blob primitives:
    put_part()       → stage_block() on the final blob (no temp objects)
    assemble_parts() → commit_block_list() to finalize
    delete_parts()   → no-op (uncommitted blocks auto-expire in 7 days)

Credentials are resolved via DefaultAzureCredential (env vars, managed
identity, Azure CLI, etc.).
"""

import base64
import hashlib
import logging
from collections.abc import AsyncIterator

from azure.core.exceptions import ResourceNotFoundError
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import ContainerClient
from azure.storage.blob import BlobBlock

logger = logging.getLogger(__name__)

# Streaming chunk size: 64 KB (matches local, AWS, GCP backends)
_CHUNK_SIZE = 64 * 1024


class AzureGatewayBackend:
    """Storage backend that proxies to an Azure Blob Storage container.

    All BleepStore buckets/objects are stored under a single upstream
    Azure container with a key prefix to namespace them.

    Attributes:
        container_name: The upstream Azure Blob container name.
        account_url: The Azure Storage account URL (e.g. https://account.blob.core.windows.net).
        prefix: Key prefix for all blobs in the upstream container.
    """

    def __init__(
        self,
        container_name: str,
        account_url: str = "",
        prefix: str = "",
    ) -> None:
        self.container_name = container_name
        self.account_url = account_url
        self.prefix = prefix
        self._container_client: ContainerClient | None = None
        self._credential: DefaultAzureCredential | None = None

    def _blob_name(self, bucket: str, key: str) -> str:
        """Map a BleepStore bucket/key to an upstream Azure blob name."""
        return f"{self.prefix}{bucket}/{key}"

    @staticmethod
    def _block_id(upload_id: str, part_number: int) -> str:
        """Generate a block ID for Azure staged blocks.

        Block IDs must be base64-encoded and the same length for all
        blocks in a blob. Includes upload_id to avoid collisions between
        concurrent multipart uploads to the same key.
        """
        return base64.b64encode(f"{upload_id}:{part_number:05d}".encode()).decode()

    async def init(self) -> None:
        """Create the Azure ContainerClient and verify the container exists.

        Raises:
            ValueError: If the upstream container does not exist or is inaccessible.
        """
        self._credential = DefaultAzureCredential()
        self._container_client = ContainerClient(
            self.account_url,
            self.container_name,
            credential=self._credential,
        )

        # Verify container exists
        try:
            exists = await self._container_client.exists()
        except Exception as e:
            await self._container_client.close()
            await self._credential.close()
            self._container_client = None
            self._credential = None
            raise ValueError(
                f"Cannot access upstream Azure container '{self.container_name}': {e}"
            ) from e

        if not exists:
            await self._container_client.close()
            await self._credential.close()
            self._container_client = None
            self._credential = None
            raise ValueError(f"Upstream Azure container '{self.container_name}' does not exist")

        logger.info(
            "Azure gateway backend initialized: container=%s account=%s prefix='%s'",
            self.container_name,
            self.account_url,
            self.prefix,
        )

    async def close(self) -> None:
        """Close the Azure client session."""
        if self._container_client is not None:
            await self._container_client.close()
            self._container_client = None
        if self._credential is not None:
            await self._credential.close()
            self._credential = None

    async def put(self, bucket: str, key: str, data: bytes) -> str:
        """Upload an object to the upstream Azure container.

        Computes MD5 locally for a consistent ETag.

        Returns:
            The hex-encoded MD5 of the stored data.
        """
        blob_name = self._blob_name(bucket, key)
        md5 = hashlib.md5(data).hexdigest()

        blob_client = self._container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(data, overwrite=True)
        return md5

    async def put_stream(
        self,
        bucket: str,
        key: str,
        stream: AsyncIterator[bytes],
        content_length: int | None = None,
    ) -> tuple[str, int]:
        """Stream-write: collect stream and delegate to put()."""
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)
        data = b"".join(chunks)
        md5_hex = await self.put(bucket, key, data)
        return md5_hex, len(data)

    async def put_part_stream(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        stream: AsyncIterator[bytes],
        content_length: int | None = None,
    ) -> tuple[str, int]:
        """Stream-write part: collect stream and delegate to put_part()."""
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)
        data = b"".join(chunks)
        md5_hex = await self.put_part(bucket, key, upload_id, part_number, data)
        return md5_hex, len(data)

    async def get(self, bucket: str, key: str) -> bytes:
        """Download an object from the upstream Azure container.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        blob_name = self._blob_name(bucket, key)
        blob_client = self._container_client.get_blob_client(blob_name)

        try:
            downloader = await blob_client.download_blob()
            return await downloader.readall()
        except ResourceNotFoundError as e:
            raise FileNotFoundError(f"Object not found: {bucket}/{key}") from e

    async def get_stream(
        self, bucket: str, key: str, offset: int = 0, length: int | None = None
    ) -> AsyncIterator[bytes]:
        """Stream an object from the upstream Azure container in 64KB chunks.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        blob_name = self._blob_name(bucket, key)
        blob_client = self._container_client.get_blob_client(blob_name)

        kwargs: dict = {}
        if offset > 0:
            kwargs["offset"] = offset
        if length is not None:
            kwargs["length"] = length

        try:
            downloader = await blob_client.download_blob(**kwargs)
        except ResourceNotFoundError as e:
            raise FileNotFoundError(f"Object not found: {bucket}/{key}") from e

        async for chunk in downloader.chunks():
            yield chunk

    async def delete(self, bucket: str, key: str) -> None:
        """Delete an object from the upstream Azure container.

        Idempotent — catches ResourceNotFoundError silently.
        """
        blob_name = self._blob_name(bucket, key)
        blob_client = self._container_client.get_blob_client(blob_name)

        try:
            await blob_client.delete_blob()
        except ResourceNotFoundError:
            pass  # Idempotent: treat as success

    async def exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists in the upstream Azure container."""
        blob_name = self._blob_name(bucket, key)
        blob_client = self._container_client.get_blob_client(blob_name)
        return await blob_client.exists()

    async def put_part(
        self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        """Stage a block on the final blob (Azure Block Blob multipart).

        Unlike AWS/GCP, parts are staged directly on the final blob using
        stage_block(). No temporary objects are created. Uncommitted blocks
        auto-expire in 7 days.

        Returns:
            The hex-encoded MD5 of the part data.
        """
        blob_name = self._blob_name(bucket, key)
        blob_client = self._container_client.get_blob_client(blob_name)
        block_id = self._block_id(upload_id, part_number)
        md5 = hashlib.md5(data).hexdigest()

        await blob_client.stage_block(block_id, data, length=len(data))
        return md5

    async def assemble_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_numbers: list[int],
    ) -> str:
        """Commit staged blocks into the final blob.

        Builds a block list from the upload_id and part numbers, then
        calls commit_block_list() to finalize the blob. Downloads the
        result to compute a consistent MD5 ETag.

        Returns:
            The hex-encoded MD5 of the assembled object.
        """
        blob_name = self._blob_name(bucket, key)
        blob_client = self._container_client.get_blob_client(blob_name)

        block_list = [BlobBlock(block_id=self._block_id(upload_id, pn)) for pn in part_numbers]
        await blob_client.commit_block_list(block_list)

        # Download the committed blob to compute MD5
        downloader = await blob_client.download_blob()
        data = await downloader.readall()
        return hashlib.md5(data).hexdigest()

    async def delete_parts(self, bucket: str, key: str, upload_id: str) -> None:
        """No-op — uncommitted Azure blocks auto-expire in 7 days.

        Unlike AWS/GCP, there are no temporary part objects to clean up.
        Azure automatically garbage-collects uncommitted blocks.
        """
        pass

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> str:
        """Copy an object using Azure server-side copy.

        Builds the source URL from account URL, container, and blob name,
        then uses start_copy_from_url() for a server-side copy. Downloads
        the destination to compute a consistent MD5 ETag.

        Returns:
            The hex-encoded MD5 ETag of the copied object.
        """
        src_blob_name = self._blob_name(src_bucket, src_key)
        dst_blob_name = self._blob_name(dst_bucket, dst_key)

        # Build source URL
        source_url = f"{self.account_url}/{self.container_name}/{src_blob_name}"

        dst_blob_client = self._container_client.get_blob_client(dst_blob_name)
        await dst_blob_client.start_copy_from_url(source_url)

        # Download destination to compute MD5
        downloader = await dst_blob_client.download_blob()
        data = await downloader.readall()
        return hashlib.md5(data).hexdigest()
