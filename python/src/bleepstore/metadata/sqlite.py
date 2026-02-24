"""SQLite-backed metadata store for BleepStore.

Implements the MetadataStore protocol using aiosqlite for async access.
All tables use CREATE TABLE IF NOT EXISTS for schema idempotency.
ACL and user_metadata fields are stored as JSON text.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class SQLiteMetadataStore:
    """Metadata store backed by a local SQLite database.

    Implements the MetadataStore protocol using aiosqlite for async access.

    Attributes:
        db_path: Path to the SQLite database file.
        _db: The aiosqlite connection, set after init_db().
    """

    def __init__(self, db_path: str) -> None:
        """Initialize the SQLite metadata store.

        Args:
            db_path: Filesystem path to the SQLite database file.
                     Use ':memory:' for an in-memory database (useful in tests).
        """
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init_db(self) -> None:
        """Open the database and create tables if they do not exist.

        Sets WAL journal mode, NORMAL synchronous, enables foreign keys,
        and sets a 5-second busy timeout. Then creates all tables and indexes.
        Idempotent -- safe to call on every startup (crash-only design).
        """
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row

        # Database pragmas
        await self._db.execute("PRAGMA journal_mode = WAL")
        await self._db.execute("PRAGMA synchronous = NORMAL")
        await self._db.execute("PRAGMA foreign_keys = ON")
        await self._db.execute("PRAGMA busy_timeout = 5000")

        await self._create_tables()

    async def _create_tables(self) -> None:
        """Create all tables and indexes if they do not already exist.

        Checks sqlite_master first to skip DDL on warm starts.
        """
        assert self._db is not None

        # Fast path: if schema_version table exists, schema is already set up
        async with self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
        ) as cursor:
            if await cursor.fetchone() is not None:
                return

        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS buckets (
                name           TEXT PRIMARY KEY,
                region         TEXT NOT NULL DEFAULT 'us-east-1',
                owner_id       TEXT NOT NULL DEFAULT '',
                owner_display  TEXT NOT NULL DEFAULT '',
                acl            TEXT NOT NULL DEFAULT '{}',
                created_at     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS objects (
                bucket              TEXT NOT NULL,
                key                 TEXT NOT NULL,
                size                INTEGER NOT NULL,
                etag                TEXT NOT NULL,
                content_type        TEXT NOT NULL DEFAULT 'application/octet-stream',
                content_encoding    TEXT,
                content_language    TEXT,
                content_disposition TEXT,
                cache_control       TEXT,
                expires             TEXT,
                storage_class       TEXT NOT NULL DEFAULT 'STANDARD',
                acl                 TEXT NOT NULL DEFAULT '{}',
                user_metadata       TEXT NOT NULL DEFAULT '{}',
                last_modified       TEXT NOT NULL,
                delete_marker       INTEGER NOT NULL DEFAULT 0,

                PRIMARY KEY (bucket, key),
                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_objects_bucket
                ON objects(bucket);
            CREATE INDEX IF NOT EXISTS idx_objects_bucket_prefix
                ON objects(bucket, key);

            CREATE TABLE IF NOT EXISTS multipart_uploads (
                upload_id           TEXT PRIMARY KEY,
                bucket              TEXT NOT NULL,
                key                 TEXT NOT NULL,
                content_type        TEXT NOT NULL DEFAULT 'application/octet-stream',
                content_encoding    TEXT,
                content_language    TEXT,
                content_disposition TEXT,
                cache_control       TEXT,
                expires             TEXT,
                storage_class       TEXT NOT NULL DEFAULT 'STANDARD',
                acl                 TEXT NOT NULL DEFAULT '{}',
                user_metadata       TEXT NOT NULL DEFAULT '{}',
                owner_id            TEXT NOT NULL DEFAULT '',
                owner_display       TEXT NOT NULL DEFAULT '',
                initiated_at        TEXT NOT NULL,

                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_uploads_bucket
                ON multipart_uploads(bucket);
            CREATE INDEX IF NOT EXISTS idx_uploads_bucket_key
                ON multipart_uploads(bucket, key);

            CREATE TABLE IF NOT EXISTS multipart_parts (
                upload_id      TEXT NOT NULL,
                part_number    INTEGER NOT NULL,
                size           INTEGER NOT NULL,
                etag           TEXT NOT NULL,
                last_modified  TEXT NOT NULL,

                PRIMARY KEY (upload_id, part_number),
                FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS credentials (
                access_key_id  TEXT PRIMARY KEY,
                secret_key     TEXT NOT NULL,
                owner_id       TEXT NOT NULL DEFAULT '',
                display_name   TEXT NOT NULL DEFAULT '',
                active         INTEGER NOT NULL DEFAULT 1,
                created_at     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS schema_version (
                version    INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );
        """)

        # Seed schema version 1 if not present
        async with self._db.execute(
            "SELECT version FROM schema_version WHERE version = 1"
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                await self._db.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (1, ?)",
                    (_now_iso(),),
                )

        await self._db.commit()

    async def close(self) -> None:
        """Close the database connection."""
        if self._db is not None:
            await self._db.close()
            self._db = None

    # -- Bucket operations -----------------------------------------------------

    async def create_bucket(
        self,
        bucket: str,
        region: str = "us-east-1",
        owner_id: str = "",
        owner_display: str = "",
        acl: str = "{}",
    ) -> None:
        """Create a new bucket record.

        Args:
            bucket: The bucket name.
            region: The region for the bucket.
            owner_id: Canonical user ID of the bucket owner.
            owner_display: Display name of the owner.
            acl: JSON-serialized ACL string.
        """
        assert self._db is not None
        await self._db.execute(
            """INSERT INTO buckets (name, region, owner_id, owner_display, acl, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (bucket, region, owner_id, owner_display, acl, _now_iso()),
        )
        await self._db.commit()

    async def bucket_exists(self, bucket: str) -> bool:
        """Check whether a bucket exists.

        Args:
            bucket: The bucket name.

        Returns:
            True if the bucket exists, False otherwise.
        """
        assert self._db is not None
        async with self._db.execute("SELECT 1 FROM buckets WHERE name = ?", (bucket,)) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def delete_bucket(self, bucket: str) -> None:
        """Delete a bucket record.

        Args:
            bucket: The bucket name to delete.
        """
        assert self._db is not None
        await self._db.execute("DELETE FROM buckets WHERE name = ?", (bucket,))
        await self._db.commit()

    async def get_bucket(self, bucket: str) -> dict[str, Any] | None:
        """Retrieve metadata for a single bucket.

        Args:
            bucket: The bucket name.

        Returns:
            A dict with bucket metadata, or None if not found.
        """
        assert self._db is not None
        async with self._db.execute(
            "SELECT name, region, owner_id, owner_display, acl, created_at "
            "FROM buckets WHERE name = ?",
            (bucket,),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return dict(row)

    async def list_buckets(self, owner_id: str = "") -> list[dict[str, Any]]:
        """List all buckets, optionally filtered by owner.

        Args:
            owner_id: If non-empty, only return buckets owned by this user.

        Returns:
            A list of dicts containing bucket metadata.
        """
        assert self._db is not None
        if owner_id:
            sql = (
                "SELECT name, region, owner_id, owner_display, acl, created_at "
                "FROM buckets WHERE owner_id = ? ORDER BY name"
            )
            params: tuple = (owner_id,)
        else:
            sql = (
                "SELECT name, region, owner_id, owner_display, acl, created_at "
                "FROM buckets ORDER BY name"
            )
            params = ()
        async with self._db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def update_bucket_acl(self, bucket: str, acl: str) -> None:
        """Update the ACL on a bucket.

        Args:
            bucket: The bucket name.
            acl: New JSON-serialized ACL string.
        """
        assert self._db is not None
        await self._db.execute("UPDATE buckets SET acl = ? WHERE name = ?", (acl, bucket))
        await self._db.commit()

    # -- Object operations -----------------------------------------------------

    async def put_object(
        self,
        bucket: str,
        key: str,
        size: int,
        etag: str,
        content_type: str = "application/octet-stream",
        content_encoding: str | None = None,
        content_language: str | None = None,
        content_disposition: str | None = None,
        cache_control: str | None = None,
        expires: str | None = None,
        storage_class: str = "STANDARD",
        acl: str = "{}",
        user_metadata: str = "{}",
    ) -> None:
        """Create or update an object metadata record (upsert).

        Uses INSERT OR REPLACE to handle both create and update cases.

        Args:
            bucket: The bucket name.
            key: The object key.
            size: Size in bytes.
            etag: The object ETag (quoted MD5 hex).
            content_type: MIME content type.
            content_encoding: Content-Encoding value.
            content_language: Content-Language value.
            content_disposition: Content-Disposition value.
            cache_control: Cache-Control value.
            expires: Expires header value.
            storage_class: S3 storage class.
            acl: JSON-serialized ACL string.
            user_metadata: JSON-serialized user metadata.
        """
        assert self._db is not None
        try:
            await self._db.execute(
                """INSERT OR REPLACE INTO objects
                   (bucket, key, size, etag, content_type, content_encoding,
                    content_language, content_disposition, cache_control, expires,
                    storage_class, acl, user_metadata, last_modified)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    bucket,
                    key,
                    size,
                    etag,
                    content_type,
                    content_encoding,
                    content_language,
                    content_disposition,
                    cache_control,
                    expires,
                    storage_class,
                    acl,
                    user_metadata,
                    _now_iso(),
                ),
            )
            await self._db.commit()
        except aiosqlite.OperationalError:
            logger.exception("SQLite error in put_object %s/%s", bucket, key)
            raise

    async def object_exists(self, bucket: str, key: str) -> bool:
        """Check whether an object exists.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            True if the object exists, False otherwise.
        """
        assert self._db is not None
        async with self._db.execute(
            "SELECT 1 FROM objects WHERE bucket = ? AND key = ?", (bucket, key)
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def get_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        """Retrieve metadata for a single object.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            A dict with object metadata, or None if not found.
        """
        assert self._db is not None
        async with self._db.execute(
            """SELECT bucket, key, size, etag, content_type, content_encoding,
                      content_language, content_disposition, cache_control, expires,
                      storage_class, acl, user_metadata, last_modified, delete_marker
               FROM objects
               WHERE bucket = ? AND key = ?""",
            (bucket, key),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return dict(row)

    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object metadata record.

        Args:
            bucket: The bucket name.
            key: The object key.
        """
        assert self._db is not None
        await self._db.execute("DELETE FROM objects WHERE bucket = ? AND key = ?", (bucket, key))
        await self._db.commit()

    async def delete_objects_meta(self, bucket: str, keys: list[str]) -> list[str]:
        """Delete multiple object metadata records in a batch.

        Uses batch SQL (IN clause) to reduce 2N queries to 2 queries.

        Args:
            bucket: The bucket name.
            keys: List of object keys to delete.

        Returns:
            List of keys that were actually deleted (had existing rows).
        """
        assert self._db is not None
        if not keys:
            return []

        # Build parameterised IN clause
        placeholders = ",".join("?" for _ in keys)

        # 1. Find which keys exist (single SELECT)
        sql_select = f"SELECT key FROM objects WHERE bucket = ? AND key IN ({placeholders})"
        async with self._db.execute(sql_select, (bucket, *keys)) as cursor:
            rows = await cursor.fetchall()
        deleted = [row["key"] for row in rows]

        if deleted:
            # 2. Delete them all (single DELETE)
            del_placeholders = ",".join("?" for _ in deleted)
            sql_delete = f"DELETE FROM objects WHERE bucket = ? AND key IN ({del_placeholders})"
            await self._db.execute(sql_delete, (bucket, *deleted))
            await self._db.commit()

        return deleted

    async def update_object_acl(self, bucket: str, key: str, acl: str) -> None:
        """Update the ACL on an object.

        Args:
            bucket: The bucket name.
            key: The object key.
            acl: New JSON-serialized ACL string.
        """
        assert self._db is not None
        await self._db.execute(
            "UPDATE objects SET acl = ? WHERE bucket = ? AND key = ?",
            (acl, bucket, key),
        )
        await self._db.commit()

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        marker: str = "",
        continuation_token: str | None = None,
    ) -> dict[str, Any]:
        """List objects in a bucket with optional filtering and pagination.

        Uses application-level CommonPrefixes grouping when a delimiter is
        specified. The continuation_token (v2) and marker (v1) are both
        treated as start-after keys.

        Args:
            bucket: The bucket name.
            prefix: Key prefix filter.
            delimiter: Grouping delimiter.
            max_keys: Maximum number of keys to return.
            marker: Start listing after this key (v1).
            continuation_token: Pagination token (v2).

        Returns:
            A dict with 'contents', 'common_prefixes', 'is_truncated',
            'next_continuation_token', 'next_marker', and 'key_count'.
        """
        assert self._db is not None

        # Short-circuit for max_keys=0
        if max_keys <= 0:
            return {
                "contents": [],
                "common_prefixes": [],
                "is_truncated": False,
                "next_continuation_token": None,
                "next_marker": None,
                "key_count": 0,
            }

        # Determine the start-after key
        start_after = continuation_token or marker or ""

        # Build the query. Fetch max_keys+1 rows for the non-delimiter case
        # to detect truncation without a separate query. For the delimiter case,
        # we over-fetch because grouping can collapse multiple rows into a
        # single CommonPrefix.
        sql_parts = [
            "SELECT key, size, etag, last_modified, storage_class, user_metadata"
            " FROM objects WHERE bucket = ?"
        ]
        params: list[Any] = [bucket]

        if prefix:
            sql_parts.append("AND key LIKE ? || '%'")
            params.append(prefix)

        if start_after:
            sql_parts.append("AND key > ?")
            params.append(start_after)

        sql_parts.append("ORDER BY key")

        fetch_limit = max_keys * 3 + 100 if delimiter else max_keys + 1
        sql_parts.append(f"LIMIT {fetch_limit}")

        sql = " ".join(sql_parts)

        async with self._db.execute(sql, tuple(params)) as cursor:
            rows = await cursor.fetchall()

        # Application-level CommonPrefixes grouping
        contents: list[dict[str, Any]] = []
        common_prefixes: list[str] = []
        seen_prefixes: set[str] = set()
        rows_consumed = 0

        for row in rows:
            rows_consumed += 1
            row_key: str = row["key"]

            if delimiter:
                suffix = row_key[len(prefix) :]
                delim_pos = suffix.find(delimiter)
                if delim_pos >= 0:
                    cp = prefix + suffix[: delim_pos + len(delimiter)]
                    if cp not in seen_prefixes:
                        seen_prefixes.add(cp)
                        common_prefixes.append(cp)
                        if len(contents) + len(common_prefixes) >= max_keys:
                            break
                    continue

            contents.append(dict(row))
            if len(contents) + len(common_prefixes) >= max_keys:
                break

        total_returned = len(contents) + len(common_prefixes)

        # Detect truncation: if we hit max_keys and there are unconsumed rows,
        # results are truncated. No separate SELECT needed.
        is_truncated = total_returned >= max_keys and rows_consumed < len(rows)

        # Build pagination tokens
        next_continuation_token: str | None = None
        next_marker: str | None = None
        if is_truncated:
            if contents:
                last_key = contents[-1]["key"]
            elif common_prefixes:
                last_key = common_prefixes[-1]
            else:
                last_key = ""
            if last_key:
                next_continuation_token = last_key
                next_marker = last_key

        return {
            "contents": contents,
            "common_prefixes": sorted(common_prefixes),
            "is_truncated": is_truncated,
            "next_continuation_token": next_continuation_token,
            "next_marker": next_marker,
            "key_count": total_returned,
        }

    # -- Multipart operations --------------------------------------------------

    async def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        content_type: str = "application/octet-stream",
        content_encoding: str | None = None,
        content_language: str | None = None,
        content_disposition: str | None = None,
        cache_control: str | None = None,
        expires: str | None = None,
        storage_class: str = "STANDARD",
        acl: str = "{}",
        user_metadata: str = "{}",
        owner_id: str = "",
        owner_display: str = "",
    ) -> None:
        """Record a new multipart upload.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The generated upload identifier.
            content_type: MIME type for the final object.
            content_encoding: Content-Encoding for the final object.
            content_language: Content-Language for the final object.
            content_disposition: Content-Disposition for the final object.
            cache_control: Cache-Control for the final object.
            expires: Expires for the final object.
            storage_class: Storage class for the final object.
            acl: JSON-serialized ACL for the final object.
            user_metadata: JSON-serialized user metadata.
            owner_id: Canonical user ID of the initiator.
            owner_display: Display name of the initiator.
        """
        assert self._db is not None
        await self._db.execute(
            """INSERT INTO multipart_uploads
               (upload_id, bucket, key, content_type, content_encoding,
                content_language, content_disposition, cache_control, expires,
                storage_class, acl, user_metadata, owner_id, owner_display, initiated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                upload_id,
                bucket,
                key,
                content_type,
                content_encoding,
                content_language,
                content_disposition,
                cache_control,
                expires,
                storage_class,
                acl,
                user_metadata,
                owner_id,
                owner_display,
                _now_iso(),
            ),
        )
        await self._db.commit()

    async def get_multipart_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> dict[str, Any] | None:
        """Retrieve metadata for a multipart upload.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The upload identifier.

        Returns:
            A dict with upload metadata, or None if not found.
        """
        assert self._db is not None
        async with self._db.execute(
            """SELECT upload_id, bucket, key, content_type, content_encoding,
                      content_language, content_disposition, cache_control, expires,
                      storage_class, acl, user_metadata, owner_id, owner_display,
                      initiated_at
               FROM multipart_uploads
               WHERE upload_id = ? AND bucket = ? AND key = ?""",
            (upload_id, bucket, key),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return dict(row)

    async def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        size: int,
        etag: str,
        content_type: str = "application/octet-stream",
        content_encoding: str | None = None,
        content_language: str | None = None,
        content_disposition: str | None = None,
        cache_control: str | None = None,
        expires: str | None = None,
        storage_class: str = "STANDARD",
        acl: str = "{}",
        user_metadata: str = "{}",
    ) -> None:
        """Complete a multipart upload atomically.

        In a single transaction: inserts the final object record, deletes
        the upload parts, and deletes the upload record.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The upload identifier.
            size: Total size of the assembled object.
            etag: Composite ETag.
            content_type: MIME type.
            content_encoding: Content-Encoding.
            content_language: Content-Language.
            content_disposition: Content-Disposition.
            cache_control: Cache-Control.
            expires: Expires.
            storage_class: Storage class.
            acl: JSON-serialized ACL.
            user_metadata: JSON-serialized user metadata.
        """
        assert self._db is not None
        now = _now_iso()

        # Use explicit transaction for atomicity
        await self._db.execute("BEGIN")
        try:
            # Insert or replace the final object
            await self._db.execute(
                """INSERT OR REPLACE INTO objects
                   (bucket, key, size, etag, content_type, content_encoding,
                    content_language, content_disposition, cache_control, expires,
                    storage_class, acl, user_metadata, last_modified)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    bucket,
                    key,
                    size,
                    etag,
                    content_type,
                    content_encoding,
                    content_language,
                    content_disposition,
                    cache_control,
                    expires,
                    storage_class,
                    acl,
                    user_metadata,
                    now,
                ),
            )
            # Delete parts for this upload
            await self._db.execute("DELETE FROM multipart_parts WHERE upload_id = ?", (upload_id,))
            # Delete the upload record
            await self._db.execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?", (upload_id,)
            )
            await self._db.execute("COMMIT")
        except Exception:
            await self._db.execute("ROLLBACK")
            raise

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """Abort a multipart upload and remove its part records.

        Deletes both the upload record and all associated part records.
        The foreign key CASCADE would handle parts, but we explicitly
        delete them for clarity.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The upload identifier.
        """
        assert self._db is not None
        await self._db.execute("DELETE FROM multipart_parts WHERE upload_id = ?", (upload_id,))
        await self._db.execute("DELETE FROM multipart_uploads WHERE upload_id = ?", (upload_id,))
        await self._db.commit()

    async def put_part(
        self,
        upload_id: str,
        part_number: int,
        size: int,
        etag: str,
    ) -> None:
        """Record an uploaded part (upsert by upload_id + part_number).

        If the same part number is uploaded again, the previous record
        is replaced (INSERT OR REPLACE).

        Args:
            upload_id: The upload identifier.
            part_number: The sequential part number.
            size: Size of this part in bytes.
            etag: ETag of this part.
        """
        assert self._db is not None
        await self._db.execute(
            """INSERT OR REPLACE INTO multipart_parts
               (upload_id, part_number, size, etag, last_modified)
               VALUES (?, ?, ?, ?, ?)""",
            (upload_id, part_number, size, etag, _now_iso()),
        )
        await self._db.commit()

    async def get_parts_for_completion(self, upload_id: str) -> list[dict[str, Any]]:
        """Get all parts for a multipart upload, ordered by part number.

        Args:
            upload_id: The upload identifier.

        Returns:
            A list of part metadata dicts ordered by part_number.
        """
        assert self._db is not None
        async with self._db.execute(
            """SELECT upload_id, part_number, size, etag, last_modified
               FROM multipart_parts
               WHERE upload_id = ?
               ORDER BY part_number""",
            (upload_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def list_parts(
        self,
        upload_id: str,
        part_number_marker: int = 0,
        max_parts: int = 1000,
    ) -> dict[str, Any]:
        """List parts for a multipart upload with pagination.

        Args:
            upload_id: The upload identifier.
            part_number_marker: Start listing after this part number.
            max_parts: Maximum parts to return.

        Returns:
            A dict with 'parts', 'is_truncated', and 'next_part_number_marker'.
        """
        assert self._db is not None
        async with self._db.execute(
            """SELECT part_number, size, etag, last_modified
               FROM multipart_parts
               WHERE upload_id = ? AND part_number > ?
               ORDER BY part_number
               LIMIT ?""",
            (upload_id, part_number_marker, max_parts + 1),
        ) as cursor:
            rows = await cursor.fetchall()

        parts = [dict(r) for r in rows[:max_parts]]
        is_truncated = len(rows) > max_parts
        next_marker = parts[-1]["part_number"] if is_truncated and parts else None

        return {
            "parts": parts,
            "is_truncated": is_truncated,
            "next_part_number_marker": next_marker,
        }

    async def list_multipart_uploads(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_uploads: int = 1000,
        key_marker: str = "",
        upload_id_marker: str = "",
    ) -> dict[str, Any]:
        """List in-progress multipart uploads in a bucket.

        Args:
            bucket: The bucket name.
            prefix: Key prefix filter.
            delimiter: Grouping delimiter.
            max_uploads: Maximum uploads to return.
            key_marker: Start listing after this key.
            upload_id_marker: Start listing after this upload ID.

        Returns:
            A dict with 'uploads', 'common_prefixes', 'is_truncated',
            'next_key_marker', and 'next_upload_id_marker'.
        """
        assert self._db is not None

        sql_parts = [
            "SELECT upload_id, bucket, key, content_type, storage_class,"
            " owner_id, owner_display, initiated_at"
            " FROM multipart_uploads WHERE bucket = ?"
        ]
        params: list[Any] = [bucket]

        if prefix:
            sql_parts.append("AND key LIKE ? || '%'")
            params.append(prefix)

        if key_marker:
            if upload_id_marker:
                sql_parts.append("AND (key > ? OR (key = ? AND upload_id > ?))")
                params.extend([key_marker, key_marker, upload_id_marker])
            else:
                sql_parts.append("AND key > ?")
                params.append(key_marker)

        sql_parts.append("ORDER BY key, initiated_at")
        sql_parts.append(f"LIMIT {max_uploads + 1}")

        sql = " ".join(sql_parts)

        async with self._db.execute(sql, tuple(params)) as cursor:
            rows = await cursor.fetchall()

        # Application-level CommonPrefixes grouping for uploads
        uploads: list[dict[str, Any]] = []
        common_prefixes: list[str] = []
        seen_prefixes: set[str] = set()

        for row in rows:
            row_key: str = row["key"]

            if delimiter:
                suffix = row_key[len(prefix) :]
                delim_pos = suffix.find(delimiter)
                if delim_pos >= 0:
                    cp = prefix + suffix[: delim_pos + len(delimiter)]
                    if cp not in seen_prefixes:
                        seen_prefixes.add(cp)
                        common_prefixes.append(cp)
                        if len(uploads) + len(common_prefixes) >= max_uploads:
                            break
                    continue

            uploads.append(dict(row))
            if len(uploads) + len(common_prefixes) >= max_uploads:
                break

        total = len(uploads) + len(common_prefixes)
        is_truncated = len(rows) > total and total >= max_uploads

        next_key_marker: str | None = None
        next_upload_id_marker: str | None = None
        if is_truncated and uploads:
            last = uploads[-1]
            next_key_marker = last["key"]
            next_upload_id_marker = last["upload_id"]

        return {
            "uploads": uploads,
            "common_prefixes": sorted(common_prefixes),
            "is_truncated": is_truncated,
            "next_key_marker": next_key_marker,
            "next_upload_id_marker": next_upload_id_marker,
        }

    # -- Credential operations -------------------------------------------------

    async def get_credential(self, access_key_id: str) -> dict[str, Any] | None:
        """Retrieve a credential by access key ID.

        Only returns active credentials (active = 1).

        Args:
            access_key_id: The access key to look up.

        Returns:
            A dict with credential fields, or None if not found or inactive.
        """
        assert self._db is not None
        async with self._db.execute(
            """SELECT access_key_id, secret_key, owner_id, display_name,
                      active, created_at
               FROM credentials
               WHERE access_key_id = ? AND active = 1""",
            (access_key_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return dict(row)

    async def put_credential(
        self,
        access_key_id: str,
        secret_key: str,
        owner_id: str = "",
        display_name: str = "",
    ) -> None:
        """Create or update a credential record.

        Args:
            access_key_id: The access key identifier.
            secret_key: The secret key.
            owner_id: Canonical user ID.
            display_name: Human-readable display name.
        """
        assert self._db is not None
        await self._db.execute(
            """INSERT OR REPLACE INTO credentials
               (access_key_id, secret_key, owner_id, display_name, active, created_at)
               VALUES (?, ?, ?, ?, 1, ?)""",
            (access_key_id, secret_key, owner_id, display_name, _now_iso()),
        )
        await self._db.commit()

    async def count_objects(self, bucket: str) -> int:
        """Count the number of objects in a bucket.

        Args:
            bucket: The bucket name.

        Returns:
            The number of objects in the bucket.
        """
        assert self._db is not None
        async with self._db.execute(
            "SELECT COUNT(*) FROM objects WHERE bucket = ?", (bucket,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0
