"""Data model types for BleepStore metadata.

These dataclasses represent the core metadata entities stored in the
metadata store (buckets, objects, multipart uploads, parts, credentials)
and the result containers returned by list operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class BucketMeta:
    """Metadata for an S3 bucket.

    Attributes:
        name: The bucket name.
        region: The AWS region (e.g. 'us-east-1').
        owner_id: Canonical user ID of the bucket owner.
        owner_display: Display name of the bucket owner.
        acl: JSON-serialized ACL string.
        created_at: ISO 8601 creation timestamp.
    """

    name: str
    region: str = "us-east-1"
    owner_id: str = ""
    owner_display: str = ""
    acl: str = "{}"
    created_at: str = ""


@dataclass
class ObjectMeta:
    """Metadata for an S3 object.

    Attributes:
        bucket: The bucket name.
        key: The object key.
        size: Size in bytes.
        etag: Quoted MD5 hex string (e.g. '"d41d8cd98f..."').
        content_type: MIME type.
        content_encoding: Content-Encoding header value, if any.
        content_language: Content-Language header value, if any.
        content_disposition: Content-Disposition header value, if any.
        cache_control: Cache-Control header value, if any.
        expires: Expires header value (RFC 7231 date), if any.
        storage_class: S3 storage class (default STANDARD).
        acl: JSON-serialized ACL string.
        user_metadata: JSON-serialized user metadata string.
        last_modified: ISO 8601 last-modified timestamp.
        delete_marker: Whether this is a delete marker (0 or 1).
    """

    bucket: str
    key: str
    size: int = 0
    etag: str = ""
    content_type: str = "application/octet-stream"
    content_encoding: str | None = None
    content_language: str | None = None
    content_disposition: str | None = None
    cache_control: str | None = None
    expires: str | None = None
    storage_class: str = "STANDARD"
    acl: str = "{}"
    user_metadata: str = "{}"
    last_modified: str = ""
    delete_marker: int = 0


@dataclass
class UploadMeta:
    """Metadata for a multipart upload.

    Attributes:
        upload_id: The upload identifier.
        bucket: The bucket name.
        key: The object key.
        content_type: MIME type for the final object.
        content_encoding: Content-Encoding for the final object.
        content_language: Content-Language for the final object.
        content_disposition: Content-Disposition for the final object.
        cache_control: Cache-Control for the final object.
        expires: Expires for the final object.
        storage_class: Storage class for the final object.
        acl: JSON-serialized ACL for the final object.
        user_metadata: JSON-serialized user metadata for the final object.
        owner_id: Canonical user ID of the upload initiator.
        owner_display: Display name of the upload initiator.
        initiated_at: ISO 8601 initiation timestamp.
    """

    upload_id: str
    bucket: str
    key: str
    content_type: str = "application/octet-stream"
    content_encoding: str | None = None
    content_language: str | None = None
    content_disposition: str | None = None
    cache_control: str | None = None
    expires: str | None = None
    storage_class: str = "STANDARD"
    acl: str = "{}"
    user_metadata: str = "{}"
    owner_id: str = ""
    owner_display: str = ""
    initiated_at: str = ""


@dataclass
class PartMeta:
    """Metadata for a single part of a multipart upload.

    Attributes:
        upload_id: The upload identifier.
        part_number: The sequential part number.
        size: Size in bytes.
        etag: Quoted MD5 hex string.
        last_modified: ISO 8601 last-modified timestamp.
    """

    upload_id: str
    part_number: int
    size: int
    etag: str
    last_modified: str = ""


@dataclass
class Credential:
    """Authentication credential record.

    Attributes:
        access_key_id: The AWS-style access key identifier.
        secret_key: The secret key.
        owner_id: Canonical user ID derived from the access key.
        display_name: Human-readable name for the credential owner.
        active: Whether this credential is active (1) or disabled (0).
        created_at: ISO 8601 creation timestamp.
    """

    access_key_id: str
    secret_key: str
    owner_id: str = ""
    display_name: str = ""
    active: int = 1
    created_at: str = ""


@dataclass
class ListResult:
    """Result container for ListObjects (v1 and v2).

    Attributes:
        contents: List of object metadata dicts.
        common_prefixes: List of collapsed prefix strings.
        is_truncated: Whether more results are available.
        next_continuation_token: Token for the next page (v2).
        next_marker: Marker for the next page (v1).
        key_count: Number of keys returned.
    """

    contents: list[dict] = field(default_factory=list)
    common_prefixes: list[str] = field(default_factory=list)
    is_truncated: bool = False
    next_continuation_token: str | None = None
    next_marker: str | None = None
    key_count: int = 0


@dataclass
class ListUploadsResult:
    """Result container for ListMultipartUploads.

    Attributes:
        uploads: List of upload metadata dicts.
        common_prefixes: List of collapsed prefix strings.
        is_truncated: Whether more results are available.
        next_key_marker: Key marker for the next page.
        next_upload_id_marker: Upload ID marker for the next page.
    """

    uploads: list[dict] = field(default_factory=list)
    common_prefixes: list[str] = field(default_factory=list)
    is_truncated: bool = False
    next_key_marker: str | None = None
    next_upload_id_marker: str | None = None


@dataclass
class ListPartsResult:
    """Result container for ListParts.

    Attributes:
        parts: List of part metadata dicts.
        is_truncated: Whether more parts are available.
        next_part_number_marker: Part number marker for the next page.
    """

    parts: list[dict] = field(default_factory=list)
    is_truncated: bool = False
    next_part_number_marker: int | None = None
