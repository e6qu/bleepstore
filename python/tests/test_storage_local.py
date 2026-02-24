"""Unit tests for the local filesystem storage backend.

Tests cover put+get round-trip, get_stream with offset/length,
delete (including idempotent re-delete), exists, atomic rename
behavior, and temp file cleanup on startup.
"""

import hashlib
import os

import pytest

from bleepstore.storage.local import LocalStorageBackend


@pytest.fixture
async def storage(tmp_path):
    """Create and initialize a local storage backend in a temp directory."""
    backend = LocalStorageBackend(str(tmp_path / "objects"))
    await backend.init()
    yield backend
    await backend.close()


class TestInit:
    """Tests for LocalStorageBackend.init()."""

    async def test_creates_root_directory(self, tmp_path):
        """init() creates the root directory if it does not exist."""
        root = tmp_path / "new-root"
        assert not root.exists()

        backend = LocalStorageBackend(str(root))
        await backend.init()

        assert root.exists()
        assert root.is_dir()

    async def test_idempotent_init(self, tmp_path):
        """init() can be called twice without error (crash-only)."""
        root = tmp_path / "idempotent"
        backend = LocalStorageBackend(str(root))
        await backend.init()
        await backend.init()
        assert root.exists()

    async def test_cleans_temp_files(self, tmp_path):
        """init() removes orphan .tmp. files from previous crashes."""
        root = tmp_path / "cleanup"
        root.mkdir(parents=True)
        bucket_dir = root / "my-bucket"
        bucket_dir.mkdir()

        # Create an orphan temp file
        orphan = bucket_dir / "file.txt.tmp.abc12345"
        orphan.write_bytes(b"leftover data")

        backend = LocalStorageBackend(str(root))
        await backend.init()

        assert not orphan.exists()


class TestPutAndGet:
    """Tests for put() and get() round-trip."""

    async def test_put_and_get_round_trip(self, storage):
        """Data written by put() can be read back by get()."""
        data = b"hello world"
        await storage.put("test-bucket", "test.txt", data)
        result = await storage.get("test-bucket", "test.txt")
        assert result == data

    async def test_put_returns_md5(self, storage):
        """put() returns the hex MD5 of the data."""
        data = b"hello world"
        md5 = await storage.put("test-bucket", "test.txt", data)
        expected = hashlib.md5(data).hexdigest()
        assert md5 == expected

    async def test_put_creates_parent_directories(self, storage):
        """put() creates parent directories for keys with path separators."""
        data = b"nested data"
        await storage.put("test-bucket", "path/to/deep/file.txt", data)
        result = await storage.get("test-bucket", "path/to/deep/file.txt")
        assert result == data

    async def test_put_overwrites_existing(self, storage):
        """put() overwrites an existing object at the same key."""
        await storage.put("test-bucket", "overwrite.txt", b"original")
        await storage.put("test-bucket", "overwrite.txt", b"updated")
        result = await storage.get("test-bucket", "overwrite.txt")
        assert result == b"updated"

    async def test_put_empty_bytes(self, storage):
        """put() handles empty data (zero-length object)."""
        md5 = await storage.put("test-bucket", "empty.txt", b"")
        result = await storage.get("test-bucket", "empty.txt")
        assert result == b""
        assert md5 == hashlib.md5(b"").hexdigest()

    async def test_put_large_data(self, storage):
        """put() handles data larger than the stream chunk size."""
        data = os.urandom(256 * 1024)  # 256 KB
        md5 = await storage.put("test-bucket", "large.bin", data)
        result = await storage.get("test-bucket", "large.bin")
        assert result == data
        assert md5 == hashlib.md5(data).hexdigest()

    async def test_get_nonexistent_raises(self, storage):
        """get() raises FileNotFoundError for a missing object."""
        with pytest.raises(FileNotFoundError):
            await storage.get("test-bucket", "nonexistent.txt")

    async def test_atomic_write_creates_final_file(self, storage):
        """After put(), only the final file exists (no temp files)."""
        await storage.put("test-bucket", "atomic.txt", b"data")
        bucket_dir = storage.root / "test-bucket"
        files = list(bucket_dir.iterdir())
        # Should be exactly one file
        assert len(files) == 1
        assert files[0].name == "atomic.txt"


class TestGetStream:
    """Tests for get_stream() async generator."""

    async def test_stream_full_file(self, storage):
        """get_stream() returns all bytes of the file."""
        data = b"stream test data"
        await storage.put("test-bucket", "stream.txt", data)

        chunks = []
        async for chunk in storage.get_stream("test-bucket", "stream.txt"):
            chunks.append(chunk)

        result = b"".join(chunks)
        assert result == data

    async def test_stream_with_offset(self, storage):
        """get_stream(offset=N) skips the first N bytes."""
        data = b"0123456789"
        await storage.put("test-bucket", "offset.txt", data)

        chunks = []
        async for chunk in storage.get_stream("test-bucket", "offset.txt", offset=5):
            chunks.append(chunk)

        result = b"".join(chunks)
        assert result == b"56789"

    async def test_stream_with_length(self, storage):
        """get_stream(length=N) returns at most N bytes."""
        data = b"0123456789"
        await storage.put("test-bucket", "length.txt", data)

        chunks = []
        async for chunk in storage.get_stream("test-bucket", "length.txt", length=5):
            chunks.append(chunk)

        result = b"".join(chunks)
        assert result == b"01234"

    async def test_stream_with_offset_and_length(self, storage):
        """get_stream(offset, length) returns a byte range."""
        data = b"0123456789"
        await storage.put("test-bucket", "range.txt", data)

        chunks = []
        async for chunk in storage.get_stream("test-bucket", "range.txt", offset=3, length=4):
            chunks.append(chunk)

        result = b"".join(chunks)
        assert result == b"3456"

    async def test_stream_large_file(self, storage):
        """get_stream() handles files larger than chunk size (multiple chunks)."""
        data = os.urandom(200 * 1024)  # 200 KB (> 64 KB chunk size)
        await storage.put("test-bucket", "big.bin", data)

        chunks = []
        async for chunk in storage.get_stream("test-bucket", "big.bin"):
            chunks.append(chunk)

        result = b"".join(chunks)
        assert result == data
        # Should have multiple chunks
        assert len(chunks) > 1


class TestDelete:
    """Tests for delete()."""

    async def test_delete_existing(self, storage):
        """delete() removes the file from disk."""
        await storage.put("test-bucket", "delete-me.txt", b"data")
        assert await storage.exists("test-bucket", "delete-me.txt")

        await storage.delete("test-bucket", "delete-me.txt")
        assert not await storage.exists("test-bucket", "delete-me.txt")

    async def test_delete_nonexistent_is_idempotent(self, storage):
        """delete() on a missing file does not raise."""
        # Should not raise
        await storage.delete("test-bucket", "does-not-exist.txt")

    async def test_delete_cleans_empty_parents(self, storage):
        """delete() removes empty parent directories."""
        await storage.put("test-bucket", "a/b/c/file.txt", b"data")
        await storage.delete("test-bucket", "a/b/c/file.txt")

        # All empty parent dirs should be cleaned up
        assert not (storage.root / "test-bucket" / "a" / "b" / "c").exists()
        assert not (storage.root / "test-bucket" / "a" / "b").exists()
        assert not (storage.root / "test-bucket" / "a").exists()

    async def test_delete_does_not_remove_nonempty_parents(self, storage):
        """delete() does not remove parent dirs that have other files."""
        await storage.put("test-bucket", "dir/file1.txt", b"one")
        await storage.put("test-bucket", "dir/file2.txt", b"two")

        await storage.delete("test-bucket", "dir/file1.txt")

        # Dir should still exist because file2.txt is there
        assert (storage.root / "test-bucket" / "dir").exists()
        assert await storage.exists("test-bucket", "dir/file2.txt")


class TestExists:
    """Tests for exists()."""

    async def test_exists_true(self, storage):
        """exists() returns True for an existing object."""
        await storage.put("test-bucket", "exists.txt", b"data")
        assert await storage.exists("test-bucket", "exists.txt") is True

    async def test_exists_false(self, storage):
        """exists() returns False for a missing object."""
        assert await storage.exists("test-bucket", "missing.txt") is False

    async def test_exists_after_delete(self, storage):
        """exists() returns False after deleting an object."""
        await storage.put("test-bucket", "temp.txt", b"data")
        await storage.delete("test-bucket", "temp.txt")
        assert await storage.exists("test-bucket", "temp.txt") is False
