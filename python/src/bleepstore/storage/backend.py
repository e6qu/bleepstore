"""Abstract storage backend protocol for BleepStore."""

from typing import AsyncIterator, Protocol


class StorageBackend(Protocol):
    """Protocol defining the object storage backend interface.

    All storage backends (local filesystem, AWS S3, GCP GCS, Azure Blob)
    must implement this interface. Methods handle raw object byte storage
    and retrieval.
    """

    async def init(self) -> None:
        """Initialize the storage backend (create directories, connect, etc.)."""
        ...

    async def close(self) -> None:
        """Release resources held by the storage backend."""
        ...

    async def put(self, bucket: str, key: str, data: bytes) -> str:
        """Store an object's bytes.

        Args:
            bucket: The bucket name.
            key: The object key.
            data: The raw bytes to store.

        Returns:
            The hex-encoded MD5 ETag of the stored data.
        """
        ...

    async def get(self, bucket: str, key: str) -> bytes:
        """Retrieve an object's bytes.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            The raw bytes of the object.
        """
        ...

    async def get_stream(
        self, bucket: str, key: str, offset: int = 0, length: int | None = None
    ) -> AsyncIterator[bytes]:
        """Retrieve an object's bytes as an async stream.

        Args:
            bucket: The bucket name.
            key: The object key.
            offset: Byte offset to start reading from.
            length: Number of bytes to read, or None for all remaining.

        Returns:
            An async iterator yielding byte chunks.
        """
        ...

    async def delete(self, bucket: str, key: str) -> None:
        """Delete an object's bytes.

        Args:
            bucket: The bucket name.
            key: The object key.
        """
        ...

    async def exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists in storage.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            True if the object exists.
        """
        ...

    async def put_part(
        self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        """Store a multipart upload part.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_number: The sequential part number.
            data: The raw bytes of this part.

        Returns:
            The hex-encoded MD5 ETag of the part.
        """
        ...

    async def assemble_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_numbers: list[int],
    ) -> str:
        """Assemble uploaded parts into a final object.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_numbers: Ordered list of part numbers to assemble.

        Returns:
            The hex-encoded ETag of the assembled object.
        """
        ...

    async def delete_parts(
        self, bucket: str, key: str, upload_id: str
    ) -> None:
        """Delete all stored parts for a multipart upload.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
        """
        ...

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> str:
        """Copy an object from one location to another.

        Args:
            src_bucket: The source bucket name.
            src_key: The source object key.
            dst_bucket: The destination bucket name.
            dst_key: The destination object key.

        Returns:
            The hex-encoded MD5 ETag of the copied object.
        """
        ...
