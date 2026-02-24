"""Abstract metadata store protocol for BleepStore."""

from typing import Any, Protocol


class MetadataStore(Protocol):
    """Protocol defining the metadata store interface.

    All metadata backends (SQLite, PostgreSQL, etc.) must implement this
    interface. Methods operate on bucket and object metadata, including
    multipart upload tracking and credential management.
    """

    async def init_db(self) -> None:
        """Initialize the database schema.

        Creates tables and indices if they do not already exist.
        Sets database pragmas (WAL, synchronous, foreign keys, busy timeout).
        Must be idempotent (safe to call on every startup).
        """
        ...

    async def close(self) -> None:
        """Close the database connection and release resources."""
        ...

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
        ...

    async def bucket_exists(self, bucket: str) -> bool:
        """Check whether a bucket exists.

        Args:
            bucket: The bucket name.

        Returns:
            True if the bucket exists, False otherwise.
        """
        ...

    async def delete_bucket(self, bucket: str) -> None:
        """Delete a bucket record.

        Args:
            bucket: The bucket name to delete.
        """
        ...

    async def get_bucket(self, bucket: str) -> dict[str, Any] | None:
        """Retrieve metadata for a single bucket.

        Args:
            bucket: The bucket name.

        Returns:
            A dict with bucket metadata, or None if the bucket does not exist.
        """
        ...

    async def list_buckets(self, owner_id: str = "") -> list[dict[str, Any]]:
        """List all buckets, optionally filtered by owner.

        Args:
            owner_id: If non-empty, only return buckets owned by this user.

        Returns:
            A list of dicts containing bucket metadata.
        """
        ...

    async def update_bucket_acl(self, bucket: str, acl: str) -> None:
        """Update the ACL on a bucket.

        Args:
            bucket: The bucket name.
            acl: New JSON-serialized ACL string.
        """
        ...

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
        ...

    async def object_exists(self, bucket: str, key: str) -> bool:
        """Check whether an object exists.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            True if the object exists, False otherwise.
        """
        ...

    async def get_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        """Retrieve metadata for a single object.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            A dict with object metadata, or None if the object does not exist.
        """
        ...

    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object metadata record.

        Args:
            bucket: The bucket name.
            key: The object key.
        """
        ...

    async def delete_objects_meta(self, bucket: str, keys: list[str]) -> list[str]:
        """Delete multiple object metadata records in a batch.

        Args:
            bucket: The bucket name.
            keys: List of object keys to delete.

        Returns:
            List of keys that were successfully deleted (had rows).
        """
        ...

    async def update_object_acl(self, bucket: str, key: str, acl: str) -> None:
        """Update the ACL on an object.

        Args:
            bucket: The bucket name.
            key: The object key.
            acl: New JSON-serialized ACL string.
        """
        ...

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

        Args:
            bucket: The bucket name.
            prefix: Key prefix filter.
            delimiter: Grouping delimiter.
            max_keys: Maximum number of keys to return.
            marker: Start listing after this key (v1).
            continuation_token: Pagination token (v2, treated as start-after key).

        Returns:
            A dict with 'contents', 'common_prefixes', 'is_truncated',
            'next_continuation_token', 'next_marker', and 'key_count'.
        """
        ...

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
        ...

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
        ...

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
        """Complete a multipart upload.

        Atomically inserts the final object record and removes the upload
        and its parts in a single transaction.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The upload identifier.
            size: Total size of the assembled object.
            etag: Composite ETag of the assembled object.
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
        ...

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """Abort a multipart upload and remove its part records.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The upload identifier.
        """
        ...

    async def put_part(
        self,
        upload_id: str,
        part_number: int,
        size: int,
        etag: str,
    ) -> None:
        """Record an uploaded part (upsert by upload_id + part_number).

        Args:
            upload_id: The upload identifier.
            part_number: The sequential part number.
            size: Size of this part in bytes.
            etag: ETag of this part.
        """
        ...

    async def get_parts_for_completion(self, upload_id: str) -> list[dict[str, Any]]:
        """Get all parts for a multipart upload, ordered by part number.

        Used during CompleteMultipartUpload to validate and assemble parts.

        Args:
            upload_id: The upload identifier.

        Returns:
            A list of part metadata dicts ordered by part_number.
        """
        ...

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
        ...

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
            upload_id_marker: Start listing after this upload ID (within key_marker).

        Returns:
            A dict with 'uploads', 'common_prefixes', 'is_truncated',
            'next_key_marker', and 'next_upload_id_marker'.
        """
        ...

    # -- Credential operations -------------------------------------------------

    async def get_credential(self, access_key_id: str) -> dict[str, Any] | None:
        """Retrieve a credential by access key ID.

        Args:
            access_key_id: The access key to look up.

        Returns:
            A dict with credential fields, or None if not found or inactive.
        """
        ...

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
        ...

    async def count_objects(self, bucket: str) -> int:
        """Count the number of objects in a bucket.

        Args:
            bucket: The bucket name.

        Returns:
            The number of objects in the bucket.
        """
        ...
