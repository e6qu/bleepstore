"""In-memory storage backend for BleepStore.

Implements the StorageBackend protocol using Python dictionaries.
All objects and parts are held in memory. Optional snapshot persistence
uses an SQLite file to save/restore state across restarts.

Crash-only design:
    - persistence="none": fresh start every time (no durable state).
    - persistence="snapshot": on init(), loads from SQLite snapshot file
      if it exists. On close(), writes a final snapshot. A background task
      periodically writes snapshots at the configured interval.
    - Snapshot writes are atomic (write to temp file, then rename).
"""

import asyncio
import hashlib
import logging
import os
import tempfile
from collections.abc import AsyncIterator

import aiosqlite

logger = logging.getLogger(__name__)

# Streaming chunk size: 64 KB (matches local backend)
_CHUNK_SIZE = 64 * 1024


class MemoryStorageError(Exception):
    """Raised when the memory backend cannot fulfill a request."""


class MemoryCapacityError(MemoryStorageError):
    """Raised when a put would exceed the configured max_size_bytes."""


class MemoryStorageBackend:
    """Storage backend that holds all objects in memory.

    Objects are stored in a dictionary keyed by (bucket, key) with values
    of (data_bytes, etag_hex). Multipart parts are stored in a separate
    dictionary keyed by (upload_id, part_number).

    Attributes:
        max_size_bytes: Maximum total bytes allowed (0 = unlimited).
        persistence: "none" or "snapshot".
        snapshot_path: Filesystem path for the SQLite snapshot file.
        snapshot_interval_seconds: Seconds between periodic snapshots (0 = disabled).
    """

    def __init__(
        self,
        max_size_bytes: int = 0,
        persistence: str = "none",
        snapshot_path: str = "./data/memory.snap",
        snapshot_interval_seconds: int = 300,
    ) -> None:
        """Initialize the memory storage backend.

        Args:
            max_size_bytes: Maximum total bytes of object data to hold in memory.
                0 means unlimited.
            persistence: Either "none" (fresh start) or "snapshot" (SQLite snapshots).
            snapshot_path: Path where the SQLite snapshot file is stored.
            snapshot_interval_seconds: Interval in seconds for periodic snapshots.
                0 disables periodic snapshots.
        """
        self.max_size_bytes = max_size_bytes
        self.persistence = persistence
        self.snapshot_path = snapshot_path
        self.snapshot_interval_seconds = snapshot_interval_seconds

        # Object storage: (bucket, key) -> (data, etag)
        self._objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        # Part storage: (upload_id, part_number) -> (data, etag)
        self._parts: dict[tuple[str, int], tuple[bytes, str]] = {}
        # Track total bytes stored
        self._current_size: int = 0

        # Background snapshot task handle
        self._snapshot_task: asyncio.Task[None] | None = None

    def _check_capacity(self, additional_bytes: int) -> None:
        """Check whether storing additional_bytes would exceed max_size_bytes.

        Args:
            additional_bytes: Number of new bytes to be stored.

        Raises:
            MemoryCapacityError: If the store would exceed capacity.
        """
        if self.max_size_bytes > 0:
            if self._current_size + additional_bytes > self.max_size_bytes:
                raise MemoryCapacityError(
                    f"Cannot store {additional_bytes} bytes: would exceed "
                    f"max_size_bytes ({self._current_size} + {additional_bytes} "
                    f"> {self.max_size_bytes})"
                )

    def _add_object(self, bucket: str, key: str, data: bytes, etag: str) -> None:
        """Store an object, updating size tracking.

        If the key already exists, the old data size is subtracted first.

        Args:
            bucket: The bucket name.
            key: The object key.
            data: The raw bytes.
            etag: The MD5 hex etag.
        """
        obj_key = (bucket, key)
        # Subtract old size if overwriting
        if obj_key in self._objects:
            old_data, _ = self._objects[obj_key]
            self._current_size -= len(old_data)
        self._objects[obj_key] = (data, etag)
        self._current_size += len(data)

    def _remove_object(self, bucket: str, key: str) -> None:
        """Remove an object from storage, updating size tracking.

        Args:
            bucket: The bucket name.
            key: The object key.
        """
        obj_key = (bucket, key)
        if obj_key in self._objects:
            old_data, _ = self._objects[obj_key]
            self._current_size -= len(old_data)
            del self._objects[obj_key]

    def _add_part(self, upload_id: str, part_number: int, data: bytes, etag: str) -> None:
        """Store a part, updating size tracking.

        Args:
            upload_id: The multipart upload identifier.
            part_number: The part number.
            data: The raw bytes.
            etag: The MD5 hex etag.
        """
        part_key = (upload_id, part_number)
        # Subtract old size if overwriting
        if part_key in self._parts:
            old_data, _ = self._parts[part_key]
            self._current_size -= len(old_data)
        self._parts[part_key] = (data, etag)
        self._current_size += len(data)

    def _remove_part(self, upload_id: str, part_number: int) -> None:
        """Remove a part from storage, updating size tracking.

        Args:
            upload_id: The multipart upload identifier.
            part_number: The part number.
        """
        part_key = (upload_id, part_number)
        if part_key in self._parts:
            old_data, _ = self._parts[part_key]
            self._current_size -= len(old_data)
            del self._parts[part_key]

    async def init(self) -> None:
        """Initialize the memory storage backend.

        If persistence="snapshot" and a snapshot file exists, loads all
        objects and parts from the SQLite snapshot into memory. Then starts
        the periodic snapshot background task if configured.
        """
        if self.persistence == "snapshot":
            await self._load_snapshot()
            if self.snapshot_interval_seconds > 0:
                self._snapshot_task = asyncio.create_task(self._periodic_snapshot())

        logger.info(
            "Memory storage backend initialized (persistence=%s, max_size=%s, "
            "objects=%d, parts=%d, current_size=%d)",
            self.persistence,
            self.max_size_bytes if self.max_size_bytes > 0 else "unlimited",
            len(self._objects),
            len(self._parts),
            self._current_size,
        )

    async def close(self) -> None:
        """Shut down the memory storage backend.

        If persistence="snapshot", writes a final snapshot and cancels the
        periodic snapshot task.
        """
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
            self._snapshot_task = None

        if self.persistence == "snapshot":
            await self._write_snapshot()
            logger.info("Memory storage backend: final snapshot written")

    async def put(self, bucket: str, key: str, data: bytes) -> str:
        """Store an object's bytes in memory.

        Args:
            bucket: The bucket name.
            key: The object key.
            data: The raw bytes to store.

        Returns:
            The hex-encoded MD5 of the stored data.

        Raises:
            MemoryCapacityError: If storing this data would exceed max_size_bytes.
        """
        # Account for overwrite: net additional is new size minus old size
        obj_key = (bucket, key)
        old_size = len(self._objects[obj_key][0]) if obj_key in self._objects else 0
        net_additional = len(data) - old_size
        if net_additional > 0:
            self._check_capacity(net_additional)

        etag = hashlib.md5(data).hexdigest()
        self._add_object(bucket, key, data, etag)
        return etag

    async def put_stream(
        self,
        bucket: str,
        key: str,
        stream: AsyncIterator[bytes],
        content_length: int | None = None,
    ) -> tuple[str, int]:
        """Stream-write an object from an async iterator into memory.

        Collects all chunks, computes MD5, and stores the concatenated result.

        Args:
            bucket: The bucket name.
            key: The object key.
            stream: Async iterator of byte chunks.
            content_length: Expected total size (optional, unused).

        Returns:
            A tuple of (hex-encoded MD5, total bytes written).

        Raises:
            MemoryCapacityError: If storing this data would exceed max_size_bytes.
        """
        chunks: list[bytes] = []
        md5 = hashlib.md5()
        total = 0

        async for chunk in stream:
            chunks.append(chunk)
            md5.update(chunk)
            total += len(chunk)

        data = b"".join(chunks)
        etag = md5.hexdigest()

        # Check capacity accounting for overwrite
        obj_key = (bucket, key)
        old_size = len(self._objects[obj_key][0]) if obj_key in self._objects else 0
        net_additional = len(data) - old_size
        if net_additional > 0:
            self._check_capacity(net_additional)

        self._add_object(bucket, key, data, etag)
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
        """Stream-write a multipart part from an async iterator into memory.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_number: The sequential part number.
            stream: Async iterator of byte chunks.
            content_length: Expected total size (optional, unused).

        Returns:
            A tuple of (hex-encoded MD5, total bytes written).

        Raises:
            MemoryCapacityError: If storing this data would exceed max_size_bytes.
        """
        chunks: list[bytes] = []
        md5 = hashlib.md5()
        total = 0

        async for chunk in stream:
            chunks.append(chunk)
            md5.update(chunk)
            total += len(chunk)

        data = b"".join(chunks)
        etag = md5.hexdigest()

        # Check capacity accounting for overwrite
        part_key = (upload_id, part_number)
        old_size = len(self._parts[part_key][0]) if part_key in self._parts else 0
        net_additional = len(data) - old_size
        if net_additional > 0:
            self._check_capacity(net_additional)

        self._add_part(upload_id, part_number, data, etag)
        return etag, total

    async def get(self, bucket: str, key: str) -> bytes:
        """Retrieve an object's bytes from memory.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            The raw bytes of the object.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        obj_key = (bucket, key)
        if obj_key not in self._objects:
            raise FileNotFoundError(f"Object not found: {bucket}/{key}")
        data, _ = self._objects[obj_key]
        return data

    async def get_stream(
        self, bucket: str, key: str, offset: int = 0, length: int | None = None
    ) -> AsyncIterator[bytes]:
        """Retrieve an object as an async byte stream from memory.

        Yields 64 KB chunks from the specified offset up to ``length``
        bytes (or end of data if ``length`` is None).

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
        obj_key = (bucket, key)
        if obj_key not in self._objects:
            raise FileNotFoundError(f"Object not found: {bucket}/{key}")
        data, _ = self._objects[obj_key]

        # Slice the data according to offset and length
        if length is not None:
            end = min(offset + length, len(data))
        else:
            end = len(data)

        pos = offset
        while pos < end:
            chunk_end = min(pos + _CHUNK_SIZE, end)
            chunk = data[pos:chunk_end]
            if not chunk:
                break
            yield chunk
            pos = chunk_end

    async def delete(self, bucket: str, key: str) -> None:
        """Delete an object from memory.

        Silently ignores missing objects (idempotent).

        Args:
            bucket: The bucket name.
            key: The object key.
        """
        self._remove_object(bucket, key)

    async def exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists in memory.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            True if the object exists.
        """
        return (bucket, key) in self._objects

    async def put_part(
        self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        """Store a multipart upload part in memory.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_number: The sequential part number.
            data: The raw bytes of this part.

        Returns:
            The hex-encoded MD5 ETag of the part.

        Raises:
            MemoryCapacityError: If storing this data would exceed max_size_bytes.
        """
        part_key = (upload_id, part_number)
        old_size = len(self._parts[part_key][0]) if part_key in self._parts else 0
        net_additional = len(data) - old_size
        if net_additional > 0:
            self._check_capacity(net_additional)

        etag = hashlib.md5(data).hexdigest()
        self._add_part(upload_id, part_number, data, etag)
        return etag

    async def assemble_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_numbers: list[int],
    ) -> str:
        """Assemble uploaded parts into a final object in memory.

        Concatenates parts in order, computes the MD5 of the assembled data,
        and stores the result as a regular object. Does not delete the parts
        (caller is responsible for calling delete_parts afterwards).

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_numbers: Ordered list of part numbers to assemble.

        Returns:
            The hex-encoded MD5 ETag of the assembled object.

        Raises:
            FileNotFoundError: If any part is missing.
            MemoryCapacityError: If the assembled object would exceed max_size_bytes.
        """
        assembled_chunks: list[bytes] = []
        md5 = hashlib.md5()
        total_size = 0

        for pn in part_numbers:
            part_key = (upload_id, pn)
            if part_key not in self._parts:
                raise FileNotFoundError(
                    f"Part not found: upload_id={upload_id}, part_number={pn}"
                )
            part_data, _ = self._parts[part_key]
            assembled_chunks.append(part_data)
            md5.update(part_data)
            total_size += len(part_data)

        assembled_data = b"".join(assembled_chunks)
        etag = md5.hexdigest()

        # Check capacity: the assembled object replaces any existing object at this key,
        # and we're not removing parts here (caller does that separately).
        obj_key = (bucket, key)
        old_size = len(self._objects[obj_key][0]) if obj_key in self._objects else 0
        net_additional = total_size - old_size
        if net_additional > 0:
            self._check_capacity(net_additional)

        self._add_object(bucket, key, assembled_data, etag)
        return etag

    async def delete_parts(self, bucket: str, key: str, upload_id: str) -> None:
        """Delete all stored parts for a multipart upload.

        Removes all parts associated with the given upload_id from memory.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
        """
        keys_to_delete = [
            (uid, pn) for (uid, pn) in self._parts if uid == upload_id
        ]
        for part_key in keys_to_delete:
            self._remove_part(part_key[0], part_key[1])

    async def delete_upload_parts(self, upload_id: str) -> None:
        """Delete all stored parts for a multipart upload by upload ID only.

        Used during expired upload reaping where bucket/key are not needed
        since parts are keyed by (upload_id, part_number).

        Args:
            upload_id: The multipart upload identifier.
        """
        keys_to_delete = [
            (uid, pn) for (uid, pn) in self._parts if uid == upload_id
        ]
        for part_key in keys_to_delete:
            self._remove_part(part_key[0], part_key[1])

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> str:
        """Copy an object from one location to another in memory.

        Creates a copy of the source object's data at the destination key.
        The destination gets its own independent copy of the bytes.

        Args:
            src_bucket: The source bucket name.
            src_key: The source object key.
            dst_bucket: The destination bucket name.
            dst_key: The destination object key.

        Returns:
            The hex-encoded MD5 ETag of the copied object.

        Raises:
            FileNotFoundError: If the source object does not exist.
            MemoryCapacityError: If the copy would exceed max_size_bytes.
        """
        src_obj_key = (src_bucket, src_key)
        if src_obj_key not in self._objects:
            raise FileNotFoundError(f"Source object not found: {src_bucket}/{src_key}")

        src_data, src_etag = self._objects[src_obj_key]

        # Make a copy of the data (so mutations are independent)
        dst_data = bytes(src_data)

        # Check capacity accounting for overwrite at destination
        dst_obj_key = (dst_bucket, dst_key)
        old_size = len(self._objects[dst_obj_key][0]) if dst_obj_key in self._objects else 0
        net_additional = len(dst_data) - old_size
        if net_additional > 0:
            self._check_capacity(net_additional)

        # Recompute etag for the copy (same data, same hash)
        etag = hashlib.md5(dst_data).hexdigest()
        self._add_object(dst_bucket, dst_key, dst_data, etag)
        return etag

    # ── Snapshot Persistence ──────────────────────────────────────────

    async def _load_snapshot(self) -> None:
        """Load objects and parts from the SQLite snapshot file into memory.

        If the snapshot file does not exist, this is a no-op (fresh start).
        """
        if not os.path.exists(self.snapshot_path):
            logger.info("No snapshot file found at %s, starting fresh", self.snapshot_path)
            return

        try:
            async with aiosqlite.connect(self.snapshot_path) as db:
                # Load objects
                async with db.execute(
                    "SELECT bucket, key, data, etag FROM object_snapshots"
                ) as cursor:
                    async for row in cursor:
                        bucket, key, data, etag = row
                        # data comes back as bytes from aiosqlite BLOB columns
                        if not isinstance(data, bytes):
                            data = bytes(data)
                        self._objects[(bucket, key)] = (data, etag)
                        self._current_size += len(data)

                # Load parts
                async with db.execute(
                    "SELECT upload_id, part_number, data, etag FROM part_snapshots"
                ) as cursor:
                    async for row in cursor:
                        upload_id, part_number, data, etag = row
                        if not isinstance(data, bytes):
                            data = bytes(data)
                        self._parts[(upload_id, part_number)] = (data, etag)
                        self._current_size += len(data)

            logger.info(
                "Loaded snapshot from %s: %d objects, %d parts, %d bytes",
                self.snapshot_path,
                len(self._objects),
                len(self._parts),
                self._current_size,
            )
        except Exception:
            logger.exception("Failed to load snapshot from %s, starting fresh", self.snapshot_path)
            # Reset to clean state on load failure
            self._objects.clear()
            self._parts.clear()
            self._current_size = 0

    async def _write_snapshot(self) -> None:
        """Write all in-memory objects and parts to a SQLite snapshot file.

        Uses atomic write: writes to a temp file in the same directory,
        then renames to the final path. This ensures the snapshot file
        is never left in a partially written state.
        """
        # Ensure parent directory exists
        snapshot_dir = os.path.dirname(self.snapshot_path)
        if snapshot_dir:
            os.makedirs(snapshot_dir, exist_ok=True)

        # Write to a temp file first, then rename (atomic)
        fd, tmp_path = tempfile.mkstemp(
            suffix=".snap.tmp",
            dir=snapshot_dir if snapshot_dir else ".",
        )
        os.close(fd)

        try:
            async with aiosqlite.connect(tmp_path) as db:
                # Create tables
                await db.execute(
                    "CREATE TABLE object_snapshots ("
                    "  bucket TEXT NOT NULL,"
                    "  key TEXT NOT NULL,"
                    "  data BLOB NOT NULL,"
                    "  etag TEXT NOT NULL,"
                    "  PRIMARY KEY (bucket, key)"
                    ")"
                )
                await db.execute(
                    "CREATE TABLE part_snapshots ("
                    "  upload_id TEXT NOT NULL,"
                    "  part_number INTEGER NOT NULL,"
                    "  data BLOB NOT NULL,"
                    "  etag TEXT NOT NULL,"
                    "  PRIMARY KEY (upload_id, part_number)"
                    ")"
                )

                # Insert objects
                for (bucket, key), (data, etag) in self._objects.items():
                    await db.execute(
                        "INSERT INTO object_snapshots (bucket, key, data, etag) "
                        "VALUES (?, ?, ?, ?)",
                        (bucket, key, data, etag),
                    )

                # Insert parts
                for (upload_id, part_number), (data, etag) in self._parts.items():
                    await db.execute(
                        "INSERT INTO part_snapshots (upload_id, part_number, data, etag) "
                        "VALUES (?, ?, ?, ?)",
                        (upload_id, part_number, data, etag),
                    )

                await db.commit()

            # Atomic rename
            os.replace(tmp_path, self.snapshot_path)
            logger.debug(
                "Snapshot written to %s: %d objects, %d parts",
                self.snapshot_path,
                len(self._objects),
                len(self._parts),
            )
        except Exception:
            logger.exception("Failed to write snapshot to %s", self.snapshot_path)
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    async def _periodic_snapshot(self) -> None:
        """Background task that periodically writes snapshots.

        Runs indefinitely until cancelled. Catches all exceptions to avoid
        crashing the background task.
        """
        while True:
            try:
                await asyncio.sleep(self.snapshot_interval_seconds)
                await self._write_snapshot()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Periodic snapshot failed")
