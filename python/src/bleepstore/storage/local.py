"""Local filesystem storage backend for BleepStore.

Implements the StorageBackend protocol using the local filesystem.
Objects are stored under ``{root}/{bucket}/{key}``. Multipart parts
are stored under ``{root}/.parts/{upload_id}/{part_number}``.

Crash-only design:
    - Atomic writes via temp-fsync-rename pattern.
    - Never acknowledge before data is fsync'd to disk.
    - Startup cleans orphan temp files in ``.tmp/`` subdirectories.
"""

import hashlib
import logging
import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

logger = logging.getLogger(__name__)

# Streaming chunk size: 64 KB
_CHUNK_SIZE = 64 * 1024


class LocalStorageBackend:
    """Storage backend that persists objects on the local filesystem.

    Objects are stored under ``{root}/{bucket}/{key}``. Multipart parts
    are stored under ``{root}/.parts/{upload_id}/{part_number}``.

    Attributes:
        root: The root directory for all stored data.
    """

    def __init__(self, root: str | Path) -> None:
        """Initialize the local storage backend.

        Args:
            root: Root directory path for object storage.
        """
        self.root = Path(root)

    def _object_path(self, bucket: str, key: str) -> Path:
        """Return the filesystem path for a stored object.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            The absolute path where this object is stored.
        """
        return self.root / bucket / key

    async def init(self) -> None:
        """Create the root directory and clean up orphan temp files.

        Crash-only design: every startup is a recovery. Remove any
        leftover ``.tmp.*`` files from interrupted writes.
        """
        self.root.mkdir(parents=True, exist_ok=True)

        # Clean orphan temp files from previous crashes
        self._clean_temp_files()

        logger.info("Local storage backend initialized at %s", self.root)

    def _clean_temp_files(self) -> None:
        """Remove orphan temp files left by interrupted atomic writes."""
        count = 0
        for dirpath, _dirnames, filenames in os.walk(self.root):
            for fname in filenames:
                if ".tmp." in fname:
                    try:
                        os.unlink(os.path.join(dirpath, fname))
                        count += 1
                    except OSError:
                        pass
        if count > 0:
            logger.info("Cleaned %d orphan temp files on startup", count)

    async def close(self) -> None:
        """No-op for local filesystem backend."""
        pass

    async def put(self, bucket: str, key: str, data: bytes) -> str:
        """Store an object's bytes on the local filesystem.

        Uses the atomic temp-fsync-rename pattern for crash safety.
        Never acknowledges before data is committed to disk.

        Args:
            bucket: The bucket name.
            key: The object key.
            data: The raw bytes to store.

        Returns:
            The hex-encoded MD5 of the stored data.
        """
        path = self._object_path(bucket, key)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Compute MD5
        md5 = hashlib.md5(data).hexdigest()

        # Atomic write: temp file -> fsync -> rename
        tmp = path.with_name(f"{path.name}.tmp.{uuid.uuid4().hex[:8]}")
        try:
            fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                os.write(fd, data)
                os.fsync(fd)
            finally:
                os.close(fd)
            tmp.rename(path)
        except Exception:
            # Clean up temp file on failure
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

        return md5

    async def get(self, bucket: str, key: str) -> bytes:
        """Retrieve an object's bytes from the local filesystem.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            The raw bytes of the object.

        Raises:
            FileNotFoundError: If the object does not exist on disk.
        """
        path = self._object_path(bucket, key)
        return path.read_bytes()

    async def get_stream(
        self, bucket: str, key: str, offset: int = 0, length: int | None = None
    ) -> AsyncIterator[bytes]:
        """Retrieve an object as an async byte stream.

        Yields 64 KB chunks from the specified offset up to ``length``
        bytes (or end of file if ``length`` is None).

        Args:
            bucket: The bucket name.
            key: The object key.
            offset: Byte offset to start reading from.
            length: Number of bytes to read, or None for all remaining.

        Yields:
            Chunks of bytes from the object.
        """
        path = self._object_path(bucket, key)
        remaining = length

        with open(path, "rb") as f:
            if offset > 0:
                f.seek(offset)

            while True:
                if remaining is not None:
                    to_read = min(_CHUNK_SIZE, remaining)
                    if to_read <= 0:
                        break
                else:
                    to_read = _CHUNK_SIZE

                chunk = f.read(to_read)
                if not chunk:
                    break

                yield chunk

                if remaining is not None:
                    remaining -= len(chunk)

    async def delete(self, bucket: str, key: str) -> None:
        """Delete an object from the local filesystem.

        Silently ignores missing files (idempotent). Cleans up empty
        parent directories up to the bucket directory.

        Args:
            bucket: The bucket name.
            key: The object key.
        """
        path = self._object_path(bucket, key)

        try:
            path.unlink()
        except FileNotFoundError:
            return

        # Clean up empty parent directories (up to bucket dir)
        bucket_dir = self.root / bucket
        parent = path.parent
        while parent != bucket_dir and parent != self.root:
            try:
                parent.rmdir()  # Only removes empty dirs
            except OSError:
                break
            parent = parent.parent

    async def exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists on the local filesystem.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            True if the file exists.
        """
        return self._object_path(bucket, key).is_file()

    async def put_part(
        self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        """Store a multipart upload part on the local filesystem.

        Parts are stored under ``{root}/.parts/{upload_id}/{part_number}``.
        Uses atomic temp-fsync-rename for crash safety.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_number: The sequential part number.
            data: The raw bytes of this part.

        Returns:
            The hex-encoded MD5 ETag of the part.
        """
        part_dir = self.root / ".parts" / upload_id
        part_dir.mkdir(parents=True, exist_ok=True)
        part_path = part_dir / str(part_number)

        md5 = hashlib.md5(data).hexdigest()

        # Atomic write
        tmp = part_path.with_name(f"{part_path.name}.tmp.{uuid.uuid4().hex[:8]}")
        try:
            fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                os.write(fd, data)
                os.fsync(fd)
            finally:
                os.close(fd)
            tmp.rename(part_path)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

        return md5

    async def assemble_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_numbers: list[int],
    ) -> str:
        """Assemble uploaded parts into a final object on disk.

        Reads each part sequentially, writes concatenated to a temp file,
        then atomically renames to the final object path. Cleans up part
        files after successful assembly.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
            part_numbers: Ordered list of part numbers to assemble.

        Returns:
            The hex-encoded MD5 ETag of the assembled object.
        """
        dest = self._object_path(bucket, key)
        dest.parent.mkdir(parents=True, exist_ok=True)

        tmp = dest.with_name(f"{dest.name}.tmp.{uuid.uuid4().hex[:8]}")
        md5 = hashlib.md5()

        try:
            fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                for pn in part_numbers:
                    part_path = self.root / ".parts" / upload_id / str(pn)
                    data = part_path.read_bytes()
                    os.write(fd, data)
                    md5.update(data)
                os.fsync(fd)
            finally:
                os.close(fd)
            tmp.rename(dest)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

        return md5.hexdigest()

    async def delete_parts(self, bucket: str, key: str, upload_id: str) -> None:
        """Delete all stored parts for a multipart upload.

        Removes the upload directory and all part files within it.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The multipart upload identifier.
        """
        parts_dir = self.root / ".parts" / upload_id
        if not parts_dir.exists():
            return

        # Remove all part files
        for child in parts_dir.iterdir():
            try:
                child.unlink()
            except OSError:
                pass

        # Remove the directory
        try:
            parts_dir.rmdir()
        except OSError:
            pass

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> str:
        """Copy an object from one location to another.

        Reads source bytes and writes to destination atomically.

        Args:
            src_bucket: The source bucket name.
            src_key: The source object key.
            dst_bucket: The destination bucket name.
            dst_key: The destination object key.

        Returns:
            The hex-encoded MD5 ETag of the copied object.
        """
        data = await self.get(src_bucket, src_key)
        return await self.put(dst_bucket, dst_key, data)
