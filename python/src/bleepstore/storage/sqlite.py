"""SQLite BLOB storage backend for BleepStore.

Implements the StorageBackend protocol using SQLite tables to store
object data and multipart part data as BLOBs in the same database
as metadata.

Tables:
    object_data(bucket, key, data, etag) — final assembled objects
    part_data(upload_id, part_number, data, etag) — multipart parts

This backend is useful for single-node deployments where keeping
everything in one SQLite database simplifies operations and backups.
"""

import hashlib
import logging
from collections.abc import AsyncIterator

import aiosqlite

logger = logging.getLogger(__name__)

# Streaming chunk size: 64 KB
_CHUNK_SIZE = 64 * 1024

_CREATE_OBJECT_DATA = """
CREATE TABLE IF NOT EXISTS object_data (
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    data BLOB NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (bucket, key)
)
"""

_CREATE_PART_DATA = """
CREATE TABLE IF NOT EXISTS part_data (
    upload_id TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    data BLOB NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (upload_id, part_number)
)
"""


class SQLiteStorageBackend:
    """Storage backend that persists object BLOBs inside a SQLite database.

    Objects are stored in the ``object_data`` table keyed by (bucket, key).
    Multipart parts are stored in the ``part_data`` table keyed by
    (upload_id, part_number).

    Attributes:
        db_path: Path to the SQLite database file.
    """

    def __init__(self, db_path: str) -> None:
        """Initialize the SQLite storage backend.

        Args:
            db_path: Path to the SQLite database file. This should typically
                     be the same database used for metadata storage.
        """
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        """Open the SQLite connection and create tables if they do not exist.

        Configures WAL mode and a 5-second busy timeout for concurrent access.
        """
        db = await aiosqlite.connect(self.db_path)
        self._db = db
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute(_CREATE_OBJECT_DATA)
        await db.execute(_CREATE_PART_DATA)
        await db.commit()
        logger.info("SQLite storage backend initialized at %s", self.db_path)

    async def close(self) -> None:
        """Close the SQLite connection."""
        if self._db is not None:
            await self._db.close()
            self._db = None

    def _ensure_db(self) -> aiosqlite.Connection:
        """Return the active database connection or raise."""
        if self._db is None:
            raise RuntimeError("SQLiteStorageBackend not initialized — call init() first")
        return self._db

    async def put(self, bucket: str, key: str, data: bytes) -> str:
        """Store an object's bytes in the database.

        Args:
            bucket: The bucket name.
            key: The object key.
            data: The raw bytes to store.

        Returns:
            The hex-encoded MD5 ETag of the stored data.
        """
        db = self._ensure_db()
        etag = hashlib.md5(data).hexdigest()
        await db.execute(
            "INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?, ?, ?, ?)",
            (bucket, key, data, etag),
        )
        await db.commit()
        return etag

    async def put_stream(
        self,
        bucket: str,
        key: str,
        stream: AsyncIterator[bytes],
        content_length: int | None = None,
    ) -> tuple[str, int]:
        """Stream-write an object by collecting chunks, then storing.

        Args:
            bucket: The bucket name.
            key: The object key.
            stream: Async iterator of byte chunks.
            content_length: Expected total size (optional, unused).

        Returns:
            A tuple of (hex-encoded MD5, total bytes written).
        """
        chunks: list[bytes] = []
        total = 0
        async for chunk in stream:
            chunks.append(chunk)
            total += len(chunk)

        data = b"".join(chunks)
        etag = await self.put(bucket, key, data)
        return etag, total

    async def put_part_stream(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        stream: AsyncIterator[bytes],
        content_length: int | None = None,
    ) -> tuple[str, int]:
        """Stream-write a multipart part by collecting chunks, then storing.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_number: The sequential part number.
            stream: Async iterator of byte chunks.
            content_length: Expected total size (optional, unused).

        Returns:
            A tuple of (hex-encoded MD5, total bytes written).
        """
        chunks: list[bytes] = []
        total = 0
        async for chunk in stream:
            chunks.append(chunk)
            total += len(chunk)

        data = b"".join(chunks)
        etag = await self.put_part(bucket, key, upload_id, part_number, data)
        return etag, total

    async def get(self, bucket: str, key: str) -> bytes:
        """Retrieve an object's bytes from the database.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            The raw bytes of the object.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        db = self._ensure_db()
        async with db.execute(
            "SELECT data FROM object_data WHERE bucket = ? AND key = ?",
            (bucket, key),
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            raise FileNotFoundError(f"Object not found: {bucket}/{key}")

        return row[0]

    async def get_stream(
        self, bucket: str, key: str, offset: int = 0, length: int | None = None
    ) -> AsyncIterator[bytes]:
        """Retrieve an object's bytes as an async stream of 64 KB chunks.

        Args:
            bucket: The bucket name.
            key: The object key.
            offset: Byte offset to start reading from.
            length: Number of bytes to read, or None for all remaining.

        Yields:
            Chunks of bytes from the object.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        data = await self.get(bucket, key)

        # Apply offset
        if offset > 0:
            data = data[offset:]

        # Apply length limit
        if length is not None:
            data = data[:length]

        # Yield in chunks
        pos = 0
        while pos < len(data):
            end = min(pos + _CHUNK_SIZE, len(data))
            yield data[pos:end]
            pos = end

    async def delete(self, bucket: str, key: str) -> None:
        """Delete an object from the database.

        Silently succeeds if the object does not exist (idempotent).

        Args:
            bucket: The bucket name.
            key: The object key.
        """
        db = self._ensure_db()
        await db.execute(
            "DELETE FROM object_data WHERE bucket = ? AND key = ?",
            (bucket, key),
        )
        await db.commit()

    async def exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists in the database.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            True if the object exists.
        """
        db = self._ensure_db()
        async with db.execute(
            "SELECT 1 FROM object_data WHERE bucket = ? AND key = ?",
            (bucket, key),
        ) as cursor:
            row = await cursor.fetchone()

        return row is not None

    async def put_part(
        self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        """Store a multipart upload part in the database.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_number: The sequential part number.
            data: The raw bytes of this part.

        Returns:
            The hex-encoded MD5 ETag of the part.
        """
        db = self._ensure_db()
        etag = hashlib.md5(data).hexdigest()
        await db.execute(
            "INSERT OR REPLACE INTO part_data (upload_id, part_number, data, etag) VALUES (?, ?, ?, ?)",
            (upload_id, part_number, data, etag),
        )
        await db.commit()
        return etag

    async def assemble_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_numbers: list[int],
    ) -> str:
        """Assemble uploaded parts into a final object.

        Reads each part from part_data in the specified order, concatenates
        them, computes the MD5, and inserts the result into object_data.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_numbers: Ordered list of part numbers to assemble.

        Returns:
            The hex-encoded MD5 ETag of the assembled object.
        """
        db = self._ensure_db()
        assembled_chunks: list[bytes] = []

        for pn in part_numbers:
            async with db.execute(
                "SELECT data FROM part_data WHERE upload_id = ? AND part_number = ?",
                (upload_id, pn),
            ) as cursor:
                row = await cursor.fetchone()

            if row is None:
                raise FileNotFoundError(
                    f"Part not found: upload_id={upload_id}, part_number={pn}"
                )
            assembled_chunks.append(row[0])

        data = b"".join(assembled_chunks)
        etag = hashlib.md5(data).hexdigest()

        await db.execute(
            "INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?, ?, ?, ?)",
            (bucket, key, data, etag),
        )
        await db.commit()
        return etag

    async def delete_parts(self, bucket: str, key: str, upload_id: str) -> None:
        """Delete all stored parts for a multipart upload.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
        """
        db = self._ensure_db()
        await db.execute(
            "DELETE FROM part_data WHERE upload_id = ?",
            (upload_id,),
        )
        await db.commit()

    async def delete_upload_parts(self, upload_id: str) -> None:
        """Delete all stored parts for a multipart upload by upload ID only.

        Used during expired upload reaping where bucket/key are not needed
        since parts are keyed by upload_id in the database.

        Args:
            upload_id: The multipart upload identifier.
        """
        db = self._ensure_db()
        await db.execute(
            "DELETE FROM part_data WHERE upload_id = ?",
            (upload_id,),
        )
        await db.commit()

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> str:
        """Copy an object from one location to another within the database.

        Reads the source object and inserts (or replaces) at the destination.

        Args:
            src_bucket: The source bucket name.
            src_key: The source object key.
            dst_bucket: The destination bucket name.
            dst_key: The destination object key.

        Returns:
            The hex-encoded MD5 ETag of the copied object.

        Raises:
            FileNotFoundError: If the source object does not exist.
        """
        db = self._ensure_db()
        async with db.execute(
            "SELECT data FROM object_data WHERE bucket = ? AND key = ?",
            (src_bucket, src_key),
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            raise FileNotFoundError(f"Source object not found: {src_bucket}/{src_key}")

        data = row[0]
        etag = hashlib.md5(data).hexdigest()

        await db.execute(
            "INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?, ?, ?, ?)",
            (dst_bucket, dst_key, data, etag),
        )
        await db.commit()
        return etag
