"""Object-level S3 request handlers for BleepStore.

Implements object operations through Stage 5b:
    - PutObject (PUT /{bucket}/{key})
    - GetObject (GET /{bucket}/{key}) with range and conditional request support
    - HeadObject (HEAD /{bucket}/{key}) with conditional request support
    - DeleteObject (DELETE /{bucket}/{key})
    - CopyObject (PUT /{bucket}/{key} with x-amz-copy-source)
    - DeleteObjects (POST /{bucket}?delete)
    - ListObjectsV2 (GET /{bucket}?list-type=2)
    - ListObjectsV1 (GET /{bucket})
    - GetObjectAcl (GET /{bucket}/{key}?acl)
    - PutObjectAcl (PUT /{bucket}/{key}?acl)
"""

import email.utils
import hashlib
import json
import logging
import re
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

from bleepstore.errors import (
    InvalidRange,
    MalformedXML,
    NoSuchBucket,
    NoSuchKey,
    NotImplementedS3Error,
    PreconditionFailed,
)


def _iso_to_http_date(iso_str: str) -> str:
    """Convert an ISO 8601 timestamp to an HTTP date string (RFC 1123).

    S3 returns Last-Modified in HTTP date format:
        ``Sat, 01 Jan 2024 00:00:00 GMT``

    The metadata store stores timestamps in ISO 8601 format:
        ``2024-01-01T00:00:00.000Z``

    Args:
        iso_str: An ISO 8601 timestamp string.

    Returns:
        An RFC 1123 HTTP date string, or the original string if parsing fails.
    """
    try:
        dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        dt = dt.replace(tzinfo=timezone.utc)
        return email.utils.format_datetime(dt, usegmt=True)
    except (ValueError, TypeError):
        try:
            dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
            dt = dt.replace(tzinfo=timezone.utc)
            return email.utils.format_datetime(dt, usegmt=True)
        except (ValueError, TypeError):
            return iso_str


from bleepstore.handlers.acl import (
    acl_from_json,
    acl_to_json,
    build_default_acl,
    parse_canned_acl,
    render_acl_xml,
)
from bleepstore.validation import validate_max_keys, validate_object_key
from bleepstore.xml_utils import (
    render_copy_object_result,
    render_delete_result,
    render_list_objects_v1,
    render_list_objects_v2,
    xml_response,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Range request parsing
# ---------------------------------------------------------------------------

_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def parse_range_header(header: str, total: int) -> tuple[int, int] | None:
    """Parse an HTTP Range header into (start, end) byte offsets.

    Supports three forms:
        - bytes=start-end  (both specified)
        - bytes=start-     (suffix from start to end of file)
        - bytes=-suffix    (last N bytes)

    Args:
        header: The Range header value, e.g. "bytes=0-4".
        total: The total size of the resource in bytes.

    Returns:
        A (start, end) tuple of inclusive byte offsets, or None if the
        header cannot be parsed.

    Raises:
        InvalidRange: If the parsed range is not satisfiable.
    """
    if not header or not header.startswith("bytes="):
        return None

    range_spec = header[len("bytes="):]

    # Handle multiple ranges -- we only support a single range
    if "," in range_spec:
        return None

    m = _RANGE_RE.match(f"bytes={range_spec}")
    if not m:
        return None

    start_str, end_str = m.group(1), m.group(2)

    if not start_str and not end_str:
        # "bytes=-"  is invalid
        raise InvalidRange()

    if not start_str:
        # Suffix range: bytes=-N  -> last N bytes
        suffix_length = int(end_str)
        if suffix_length == 0:
            raise InvalidRange()
        if suffix_length > total:
            suffix_length = total
        start = total - suffix_length
        end = total - 1
    elif not end_str:
        # Open-ended: bytes=N-  -> from N to end
        start = int(start_str)
        if start >= total:
            raise InvalidRange()
        end = total - 1
    else:
        # Closed range: bytes=N-M
        start = int(start_str)
        end = int(end_str)
        if start > end or start >= total:
            raise InvalidRange()
        # Clamp end to total-1
        if end >= total:
            end = total - 1

    return (start, end)


# ---------------------------------------------------------------------------
# Conditional request evaluation
# ---------------------------------------------------------------------------

def _strip_etag_quotes(etag: str) -> str:
    """Strip surrounding double quotes and optional W/ prefix from an ETag.

    Args:
        etag: An ETag value, possibly quoted.

    Returns:
        The unquoted ETag string.
    """
    etag = etag.strip()
    if etag.startswith("W/"):
        etag = etag[2:]
    if etag.startswith('"') and etag.endswith('"'):
        etag = etag[1:-1]
    return etag


def _parse_http_date(date_str: str) -> datetime | None:
    """Parse an HTTP date string into a timezone-aware datetime.

    Args:
        date_str: An HTTP date string (RFC 1123, RFC 850, or asctime).

    Returns:
        A timezone-aware datetime in UTC, or None if parsing fails.
    """
    try:
        return email.utils.parsedate_to_datetime(date_str)
    except (ValueError, TypeError):
        return None


def _parse_last_modified(last_modified_str: str) -> datetime | None:
    """Parse a last-modified ISO 8601 timestamp into a timezone-aware datetime.

    Args:
        last_modified_str: An ISO 8601 timestamp string, e.g.
            "2024-01-01T00:00:00.000Z".

    Returns:
        A timezone-aware datetime in UTC, or None if parsing fails.
    """
    try:
        dt = datetime.strptime(last_modified_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        try:
            dt = datetime.strptime(last_modified_str, "%Y-%m-%dT%H:%M:%SZ")
            return dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return None


def evaluate_conditionals(
    request: Request,
    etag: str,
    last_modified_str: str,
    is_get_or_head: bool = True,
) -> int | None:
    """Evaluate conditional request headers against object metadata.

    Evaluation order (per HTTP/1.1 spec):
        1. If-Match -> 412 on mismatch
        2. If-Unmodified-Since -> 412 if modified after date
        3. If-None-Match -> 304 on match (GET/HEAD), 412 otherwise
        4. If-Modified-Since -> 304 if not modified (GET/HEAD only)

    Args:
        request: The incoming HTTP request.
        etag: The object's ETag (quoted).
        last_modified_str: The object's last_modified ISO 8601 timestamp.
        is_get_or_head: True for GET/HEAD requests, False for others.

    Returns:
        An HTTP status code (304 or 412) if a condition fails, or None
        if all conditions pass.
    """
    obj_etag = _strip_etag_quotes(etag)
    obj_mtime = _parse_last_modified(last_modified_str)

    # 1. If-Match: request succeeds only if ETag matches
    if_match = request.headers.get("if-match")
    if if_match is not None:
        # If-Match can be "*" (any) or a list of ETags
        if if_match.strip() != "*":
            match_tags = [
                _strip_etag_quotes(t) for t in if_match.split(",")
            ]
            if obj_etag not in match_tags:
                return 412

    # 2. If-Unmodified-Since: request succeeds only if not modified since date
    if_unmodified_since = request.headers.get("if-unmodified-since")
    if if_unmodified_since is not None and if_match is None:
        # Only evaluate if If-Match is not present
        ius_date = _parse_http_date(if_unmodified_since)
        if ius_date is not None and obj_mtime is not None:
            if obj_mtime > ius_date:
                return 412

    # 3. If-None-Match: for GET/HEAD -> 304, for others -> 412
    if_none_match = request.headers.get("if-none-match")
    if if_none_match is not None:
        if if_none_match.strip() == "*":
            return 304 if is_get_or_head else 412
        none_match_tags = [
            _strip_etag_quotes(t) for t in if_none_match.split(",")
        ]
        if obj_etag in none_match_tags:
            return 304 if is_get_or_head else 412

    # 4. If-Modified-Since: only for GET/HEAD, only if If-None-Match is absent
    if_modified_since = request.headers.get("if-modified-since")
    if (
        if_modified_since is not None
        and if_none_match is None
        and is_get_or_head
    ):
        ims_date = _parse_http_date(if_modified_since)
        if ims_date is not None and obj_mtime is not None:
            if obj_mtime <= ims_date:
                return 304

    return None


class ObjectHandler:
    """Handles S3 object operations.

    Attributes:
        app: The parent FastAPI application.
    """

    def __init__(self, app: FastAPI) -> None:
        """Initialize the object handler.

        Args:
            app: The FastAPI application instance.
        """
        self.app = app

    @property
    def metadata(self):
        """Shortcut to the metadata store on app.state."""
        return self.app.state.metadata

    @property
    def storage(self):
        """Shortcut to the storage backend on app.state."""
        return self.app.state.storage

    @property
    def config(self):
        """Shortcut to the BleepStoreConfig on app.state."""
        return self.app.state.config

    async def _ensure_bucket_exists(self, bucket: str) -> None:
        """Verify that a bucket exists, raising NoSuchBucket if not.

        Args:
            bucket: The bucket name to check.

        Raises:
            NoSuchBucket: If the bucket does not exist.
        """
        exists = await self.metadata.bucket_exists(bucket)
        if not exists:
            raise NoSuchBucket(bucket)

    def _extract_user_metadata(self, request: Request) -> dict[str, str]:
        """Extract x-amz-meta-* headers from the request.

        Strips the ``x-amz-meta-`` prefix from each header name and
        returns a dict of the remaining key-value pairs.

        Args:
            request: The incoming HTTP request.

        Returns:
            A dict mapping metadata keys to values.
        """
        meta: dict[str, str] = {}
        for name, value in request.headers.items():
            lower_name = name.lower()
            if lower_name.startswith("x-amz-meta-"):
                meta_key = lower_name[len("x-amz-meta-"):]
                meta[meta_key] = value
        return meta

    async def put_object(self, request: Request, bucket: str, key: str) -> Response:
        """Upload an object to a bucket.

        Implements: PUT /{bucket}/{key}

        Also dispatches to copy_object if x-amz-copy-source is present
        (deferred to Stage 5a).

        Crash-only design: data is written atomically via temp-fsync-rename.
        Metadata is committed to SQLite only after storage write succeeds.
        Never acknowledges before both are committed.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            200 OK with ETag header on success.
        """
        # Check bucket exists
        await self._ensure_bucket_exists(bucket)

        # Validate the object key
        validate_object_key(key)

        # Read the full body
        data = await request.body()
        size = len(data)

        # Extract content type (default to application/octet-stream)
        content_type = request.headers.get("content-type", "application/octet-stream")

        # Extract optional content headers
        content_encoding = request.headers.get("content-encoding")
        content_language = request.headers.get("content-language")
        content_disposition = request.headers.get("content-disposition")
        cache_control = request.headers.get("cache-control")
        expires = request.headers.get("expires")

        # Extract user metadata (x-amz-meta-*)
        user_metadata = self._extract_user_metadata(request)
        user_metadata_json = json.dumps(user_metadata) if user_metadata else "{}"

        # Handle x-amz-acl header for canned ACL on PutObject
        acl_json = "{}"
        canned_acl = request.headers.get("x-amz-acl")
        if canned_acl:
            access_key = self.config.auth.access_key
            owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
            owner_display = access_key
            try:
                acl = parse_canned_acl(canned_acl, owner_id, owner_display)
                acl_json = acl_to_json(acl)
            except ValueError:
                from bleepstore.errors import InvalidArgument
                raise InvalidArgument(f"Invalid canned ACL: {canned_acl}")

        # Write data to storage (atomic: temp-fsync-rename)
        md5_hex = await self.storage.put(bucket, key, data)

        # Quote the ETag: S3 ETags are always quoted
        etag = f'"{md5_hex}"'

        # Commit metadata to SQLite (only after storage write succeeded)
        await self.metadata.put_object(
            bucket=bucket,
            key=key,
            size=size,
            etag=etag,
            content_type=content_type,
            content_encoding=content_encoding,
            content_language=content_language,
            content_disposition=content_disposition,
            cache_control=cache_control,
            expires=expires,
            user_metadata=user_metadata_json,
            acl=acl_json,
        )

        return Response(
            status_code=200,
            headers={"ETag": etag},
        )

    async def get_object(self, request: Request, bucket: str, key: str) -> Response:
        """Retrieve an object from a bucket.

        Implements: GET /{bucket}/{key}

        Supports range requests (206 Partial Content) and conditional
        requests (304 Not Modified, 412 Precondition Failed).

        Uses StreamingResponse to stream the object body in 64 KB chunks
        for efficient handling of large objects.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            Response with the object body, metadata headers, and ETag.
        """
        # Check bucket exists
        await self._ensure_bucket_exists(bucket)

        # Get object metadata
        obj_meta = await self.metadata.get_object(bucket, key)
        if obj_meta is None:
            raise NoSuchKey(key)

        # Build response headers
        headers = self._build_object_headers(obj_meta)

        # Evaluate conditional request headers
        etag = obj_meta.get("etag", "")
        last_modified = obj_meta.get("last_modified", "")
        cond_status = evaluate_conditionals(request, etag, last_modified, is_get_or_head=True)
        if cond_status is not None:
            if cond_status == 304:
                # 304 Not Modified: return ETag and Last-Modified only
                not_modified_headers = {}
                if etag:
                    not_modified_headers["ETag"] = etag
                if last_modified:
                    not_modified_headers["Last-Modified"] = _iso_to_http_date(last_modified)
                return Response(status_code=304, headers=not_modified_headers)
            elif cond_status == 412:
                raise PreconditionFailed()

        # Handle range requests
        total_size = int(obj_meta.get("size", 0))
        range_header = request.headers.get("range")
        if range_header:
            parsed_range = parse_range_header(range_header, total_size)
            if parsed_range is not None:
                start, end = parsed_range
                content_length = end - start + 1
                headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
                headers["Content-Length"] = str(content_length)

                return StreamingResponse(
                    content=self.storage.get_stream(bucket, key, offset=start, length=content_length),
                    status_code=206,
                    headers=headers,
                    media_type=obj_meta.get("content_type", "application/octet-stream"),
                )

        # Stream the full object body
        return StreamingResponse(
            content=self.storage.get_stream(bucket, key),
            status_code=200,
            headers=headers,
            media_type=obj_meta.get("content_type", "application/octet-stream"),
        )

    async def head_object(self, request: Request, bucket: str, key: str) -> Response:
        """Retrieve object metadata without the body.

        Implements: HEAD /{bucket}/{key}

        Supports conditional requests (304 Not Modified, 412 Precondition
        Failed). Same as GetObject logic minus writing the body.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            200 OK with metadata headers and no body.
        """
        # Check bucket exists
        await self._ensure_bucket_exists(bucket)

        # Get object metadata
        obj_meta = await self.metadata.get_object(bucket, key)
        if obj_meta is None:
            raise NoSuchKey(key)

        # Build response headers
        headers = self._build_object_headers(obj_meta)

        # Evaluate conditional request headers
        etag = obj_meta.get("etag", "")
        last_modified = obj_meta.get("last_modified", "")
        cond_status = evaluate_conditionals(request, etag, last_modified, is_get_or_head=True)
        if cond_status is not None:
            if cond_status == 304:
                not_modified_headers = {}
                if etag:
                    not_modified_headers["ETag"] = etag
                if last_modified:
                    not_modified_headers["Last-Modified"] = _iso_to_http_date(last_modified)
                return Response(status_code=304, headers=not_modified_headers)
            elif cond_status == 412:
                raise PreconditionFailed()

        return Response(
            status_code=200,
            headers=headers,
        )

    async def delete_object(self, request: Request, bucket: str, key: str) -> Response:
        """Delete a single object from a bucket.

        Implements: DELETE /{bucket}/{key}

        Idempotent: always returns 204 even if the key does not exist.
        Deletes from both storage and metadata. Storage deletion failure
        is logged but does not prevent metadata cleanup.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            204 No Content on success.
        """
        # Check bucket exists
        await self._ensure_bucket_exists(bucket)

        # Delete from storage (silently ignores missing files)
        try:
            await self.storage.delete(bucket, key)
        except Exception:
            logger.warning(
                "Failed to delete object from storage: %s/%s", bucket, key,
                exc_info=True,
            )

        # Delete metadata
        await self.metadata.delete_object(bucket, key)

        return Response(status_code=204)

    def _build_object_headers(self, obj_meta: dict) -> dict[str, str]:
        """Build S3-compatible response headers from object metadata.

        Args:
            obj_meta: Object metadata dict from the metadata store.

        Returns:
            A dict of response headers.
        """
        headers: dict[str, str] = {}

        # ETag (always quoted)
        etag = obj_meta.get("etag", "")
        headers["ETag"] = etag

        # Last-Modified (convert ISO 8601 to HTTP date format for S3 compat)
        last_modified = obj_meta.get("last_modified", "")
        if last_modified:
            headers["Last-Modified"] = _iso_to_http_date(last_modified)

        # Content-Length
        size = obj_meta.get("size", 0)
        headers["Content-Length"] = str(size)

        # Content-Type
        content_type = obj_meta.get("content_type", "application/octet-stream")
        headers["Content-Type"] = content_type

        # Optional content headers
        if obj_meta.get("content_encoding"):
            headers["Content-Encoding"] = obj_meta["content_encoding"]
        if obj_meta.get("content_language"):
            headers["Content-Language"] = obj_meta["content_language"]
        if obj_meta.get("content_disposition"):
            headers["Content-Disposition"] = obj_meta["content_disposition"]
        if obj_meta.get("cache_control"):
            headers["Cache-Control"] = obj_meta["cache_control"]
        if obj_meta.get("expires"):
            headers["Expires"] = obj_meta["expires"]

        # Accept-Ranges (always bytes for S3 compatibility)
        headers["Accept-Ranges"] = "bytes"

        # User metadata (x-amz-meta-*)
        user_metadata_str = obj_meta.get("user_metadata", "{}")
        try:
            user_metadata = json.loads(user_metadata_str)
            for meta_key, meta_value in user_metadata.items():
                headers[f"x-amz-meta-{meta_key}"] = meta_value
        except (json.JSONDecodeError, TypeError):
            pass

        return headers

    # -- List, Copy & Batch Delete (Stage 5a) ----------------------------------

    async def copy_object(
        self, request: Request, bucket: str, key: str
    ) -> Response:
        """Copy an object within or across buckets.

        Triggered by PUT with x-amz-copy-source header. Parses the source
        bucket/key from the header, URL-decodes it, reads the source object,
        writes to the destination, and returns CopyObjectResult XML.

        Supports x-amz-metadata-directive: COPY (default) keeps source
        metadata, REPLACE uses metadata from this request.

        Args:
            request: The incoming HTTP request.
            bucket: The destination bucket name.
            key: The destination object key.

        Returns:
            XML response with CopyObjectResult.
        """
        # Parse x-amz-copy-source header
        copy_source = request.headers.get("x-amz-copy-source", "")
        copy_source = urllib.parse.unquote(copy_source)

        # Strip leading slash
        if copy_source.startswith("/"):
            copy_source = copy_source[1:]

        # Split into source bucket and key
        slash_pos = copy_source.find("/")
        if slash_pos < 0:
            raise MalformedXML("Invalid x-amz-copy-source header")

        src_bucket = copy_source[:slash_pos]
        src_key = copy_source[slash_pos + 1:]

        if not src_bucket or not src_key:
            raise MalformedXML("Invalid x-amz-copy-source header")

        # Validate source bucket and key exist
        src_bucket_exists = await self.metadata.bucket_exists(src_bucket)
        if not src_bucket_exists:
            raise NoSuchBucket(src_bucket)

        src_meta = await self.metadata.get_object(src_bucket, src_key)
        if src_meta is None:
            raise NoSuchKey(src_key)

        # Validate destination bucket exists
        await self._ensure_bucket_exists(bucket)

        # Validate destination key
        validate_object_key(key)

        # Copy the data in storage
        md5_hex = await self.storage.copy_object(
            src_bucket, src_key, bucket, key
        )

        # Quote the ETag
        etag = f'"{md5_hex}"'

        # Determine metadata directive
        directive = request.headers.get(
            "x-amz-metadata-directive", "COPY"
        ).upper()

        if directive == "REPLACE":
            # Use metadata from this request
            content_type = request.headers.get(
                "content-type", "application/octet-stream"
            )
            content_encoding = request.headers.get("content-encoding")
            content_language = request.headers.get("content-language")
            content_disposition = request.headers.get("content-disposition")
            cache_control = request.headers.get("cache-control")
            expires = request.headers.get("expires")
            user_metadata = self._extract_user_metadata(request)
            user_metadata_json = (
                json.dumps(user_metadata) if user_metadata else "{}"
            )
        else:
            # COPY: use source object metadata
            content_type = src_meta.get(
                "content_type", "application/octet-stream"
            )
            content_encoding = src_meta.get("content_encoding")
            content_language = src_meta.get("content_language")
            content_disposition = src_meta.get("content_disposition")
            cache_control = src_meta.get("cache_control")
            expires = src_meta.get("expires")
            user_metadata_json = src_meta.get("user_metadata", "{}")

        # Get the size of the copied object from the source
        size = src_meta.get("size", 0)

        # Commit metadata to SQLite
        await self.metadata.put_object(
            bucket=bucket,
            key=key,
            size=size,
            etag=etag,
            content_type=content_type,
            content_encoding=content_encoding,
            content_language=content_language,
            content_disposition=content_disposition,
            cache_control=cache_control,
            expires=expires,
            user_metadata=user_metadata_json,
        )

        # Get the last_modified from the newly created metadata
        dest_meta = await self.metadata.get_object(bucket, key)
        last_modified = dest_meta.get("last_modified", "") if dest_meta else ""

        body = render_copy_object_result(etag, last_modified)
        return xml_response(body, status=200)

    async def delete_objects(
        self, request: Request, bucket: str
    ) -> Response:
        """Delete multiple objects from a bucket (batch delete).

        Implements: POST /{bucket}?delete

        Parses an XML request body listing the objects to delete and
        returns an XML response with per-key results. Supports quiet mode.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name.

        Returns:
            XML response with deletion results.
        """
        await self._ensure_bucket_exists(bucket)

        # Read and parse XML body
        body_bytes = await request.body()
        try:
            root = ET.fromstring(body_bytes)
        except ET.ParseError:
            raise MalformedXML()

        # Handle XML namespace
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag[: root.tag.index("}") + 1]

        # Check for quiet mode
        quiet_elem = root.find(f"{ns}Quiet")
        quiet = (
            quiet_elem is not None
            and quiet_elem.text is not None
            and quiet_elem.text.lower() == "true"
        )

        # Extract object keys to delete
        keys_to_delete: list[str] = []
        for obj_elem in root.findall(f"{ns}Object"):
            key_elem = obj_elem.find(f"{ns}Key")
            if key_elem is not None and key_elem.text:
                keys_to_delete.append(key_elem.text)

        # Delete from storage and metadata
        deleted_list: list[dict[str, str]] = []
        error_list: list[dict[str, str]] = []

        for obj_key in keys_to_delete:
            try:
                # Delete from storage (silently ignores missing files)
                try:
                    await self.storage.delete(bucket, obj_key)
                except Exception:
                    logger.warning(
                        "Failed to delete object from storage: %s/%s",
                        bucket,
                        obj_key,
                        exc_info=True,
                    )

                # Delete metadata
                await self.metadata.delete_object(bucket, obj_key)

                if not quiet:
                    deleted_list.append({"key": obj_key})
            except Exception as exc:
                error_list.append(
                    {
                        "key": obj_key,
                        "code": "InternalError",
                        "message": str(exc),
                    }
                )

        result_body = render_delete_result(deleted_list, error_list)
        return xml_response(result_body, status=200)

    async def delete_multi(self, request: Request) -> Response:
        """Delete multiple objects from a bucket in a single request.

        Implements: POST /{bucket}?delete

        This is an alias kept for backward compatibility. The actual
        implementation is in delete_objects().

        Args:
            request: The incoming HTTP request.

        Returns:
            XML response with deletion results.

        Raises:
            NotImplementedS3Error: Handler is not yet implemented.
        """
        raise NotImplementedS3Error()

    async def list_objects(self, request: Request, bucket: str) -> Response:
        """List objects in a bucket, dispatching to v1 or v2.

        Implements: GET /{bucket}
        Dispatches to list_objects_v2 if list-type=2, else list_objects_v1.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            XML response with object listing.
        """
        await self._ensure_bucket_exists(bucket)

        list_type = request.query_params.get("list-type", "")
        if list_type == "2":
            return await self.list_objects_v2(request, bucket)
        else:
            return await self.list_objects_v1(request, bucket)

    async def list_objects_v2(
        self, request: Request, bucket: str
    ) -> Response:
        """List objects in a bucket using the v2 API.

        Implements: GET /{bucket}?list-type=2

        Supports prefix, delimiter, max-keys, continuation-token, and
        start-after query parameters. Returns ListBucketResult XML with
        Contents and CommonPrefixes elements.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            XML response with ListBucketResult.
        """
        prefix = request.query_params.get("prefix", "")
        delimiter = request.query_params.get("delimiter", "")
        encoding_type = request.query_params.get("encoding-type")
        continuation_token = request.query_params.get("continuation-token")
        start_after = request.query_params.get("start-after", "")

        max_keys_str = request.query_params.get("max-keys", "1000")
        max_keys = validate_max_keys(max_keys_str)

        # Query the metadata store
        result = await self.metadata.list_objects(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            continuation_token=continuation_token or start_after or None,
        )

        body = render_list_objects_v2(
            name=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            is_truncated=result["is_truncated"],
            contents=result["contents"],
            common_prefixes=result["common_prefixes"],
            continuation_token=continuation_token,
            next_continuation_token=result.get("next_continuation_token"),
            key_count=result["key_count"],
            encoding_type=encoding_type,
            start_after=start_after,
        )
        return xml_response(body, status=200)

    async def list_objects_v1(
        self, request: Request, bucket: str
    ) -> Response:
        """List objects in a bucket using the v1 API.

        Implements: GET /{bucket}

        Supports prefix, delimiter, max-keys, and marker query parameters.
        Returns ListBucketResult XML with Contents, CommonPrefixes, and
        marker-based pagination.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            XML response with ListBucketResult.
        """
        prefix = request.query_params.get("prefix", "")
        delimiter = request.query_params.get("delimiter", "")
        marker = request.query_params.get("marker", "")

        max_keys_str = request.query_params.get("max-keys", "1000")
        max_keys = validate_max_keys(max_keys_str)

        # Query the metadata store
        result = await self.metadata.list_objects(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            marker=marker,
        )

        body = render_list_objects_v1(
            name=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            is_truncated=result["is_truncated"],
            contents=result["contents"],
            common_prefixes=result["common_prefixes"],
            marker=marker,
            next_marker=result.get("next_marker"),
        )
        return xml_response(body, status=200)

    # -- Object ACLs (Stage 5b) -----------------------------------------------

    async def get_object_acl(
        self, request: Request, bucket: str, key: str
    ) -> Response:
        """Return the access control list for an object.

        Implements: GET /{bucket}/{key}?acl

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            XML response with the object ACL.
        """
        # Check bucket exists
        await self._ensure_bucket_exists(bucket)

        # Get object metadata
        obj_meta = await self.metadata.get_object(bucket, key)
        if obj_meta is None:
            raise NoSuchKey(key)

        acl_json_str = obj_meta.get("acl", "{}")
        acl = acl_from_json(acl_json_str)

        # If the ACL has no owner info, fill it in from the bucket owner
        if not acl.get("owner", {}).get("id"):
            # Derive owner from access key
            access_key = self.config.auth.access_key
            owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
            owner_display = access_key
            if not acl.get("grants"):
                acl = build_default_acl(owner_id, owner_display)
            else:
                acl["owner"] = {"id": owner_id, "display_name": owner_display}

        xml_body = render_acl_xml(acl)
        return xml_response(xml_body, status=200)

    async def put_object_acl(
        self, request: Request, bucket: str, key: str
    ) -> Response:
        """Set the access control list for an object.

        Implements: PUT /{bucket}/{key}?acl

        Supports canned ACL via x-amz-acl header or XML body with
        full AccessControlPolicy.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            200 OK on success.
        """
        # Check bucket exists
        await self._ensure_bucket_exists(bucket)

        # Check object exists
        obj_meta = await self.metadata.get_object(bucket, key)
        if obj_meta is None:
            raise NoSuchKey(key)

        # Derive owner info
        access_key = self.config.auth.access_key
        owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
        owner_display = access_key

        # Check for canned ACL header
        canned_acl = request.headers.get("x-amz-acl")
        if canned_acl:
            try:
                acl = parse_canned_acl(canned_acl, owner_id, owner_display)
            except ValueError:
                from bleepstore.errors import InvalidArgument
                raise InvalidArgument(f"Invalid canned ACL: {canned_acl}")
            acl_json = acl_to_json(acl)
            await self.metadata.update_object_acl(bucket, key, acl_json)
            return Response(status_code=200)

        # Check for XML body
        body = await request.body()
        if body:
            try:
                root = ET.fromstring(body)
                acl = _parse_acl_xml(root, owner_id, owner_display)
                acl_json = acl_to_json(acl)
                await self.metadata.update_object_acl(bucket, key, acl_json)
                return Response(status_code=200)
            except ET.ParseError:
                raise MalformedXML()

        # No ACL specified -- default to private
        acl = build_default_acl(owner_id, owner_display)
        acl_json = acl_to_json(acl)
        await self.metadata.update_object_acl(bucket, key, acl_json)
        return Response(status_code=200)


# ---------------------------------------------------------------------------
# ACL XML parsing helper (reused from bucket handler pattern)
# ---------------------------------------------------------------------------

def _find_elem(
    parent: ET.Element, ns_name: str, bare_name: str
) -> ET.Element | None:
    """Find a child element, trying namespaced name first, then bare name."""
    elem = parent.find(ns_name)
    if elem is not None:
        return elem
    return parent.find(bare_name)


def _parse_acl_xml(
    root: ET.Element,
    default_owner_id: str,
    default_owner_display: str,
) -> dict:
    """Parse an AccessControlPolicy XML element into an ACL dict."""
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

    # Parse owner
    owner_elem = _find_elem(root, f"{ns}Owner", "Owner")
    owner_id = default_owner_id
    owner_display = default_owner_display
    if owner_elem is not None:
        id_elem = _find_elem(owner_elem, f"{ns}ID", "ID")
        if id_elem is not None and id_elem.text:
            owner_id = id_elem.text
        dn_elem = _find_elem(owner_elem, f"{ns}DisplayName", "DisplayName")
        if dn_elem is not None and dn_elem.text:
            owner_display = dn_elem.text

    # Parse grants
    grants = []
    acl_elem = _find_elem(root, f"{ns}AccessControlList", "AccessControlList")
    if acl_elem is not None:
        for grant_elem in (
            acl_elem.findall(f"{ns}Grant") + acl_elem.findall("Grant")
        ):
            grantee_elem = _find_elem(
                grant_elem, f"{ns}Grantee", "Grantee"
            )
            perm_elem = _find_elem(
                grant_elem, f"{ns}Permission", "Permission"
            )
            if grantee_elem is None or perm_elem is None:
                continue

            permission = perm_elem.text or ""

            xsi_type = grantee_elem.get(
                "{http://www.w3.org/2001/XMLSchema-instance}type", ""
            )

            if xsi_type == "Group" or grantee_elem.find(f"{ns}URI") is not None:
                uri_elem = _find_elem(grantee_elem, f"{ns}URI", "URI")
                uri = uri_elem.text if uri_elem is not None else ""
                grants.append({
                    "grantee": {"type": "Group", "uri": uri},
                    "permission": permission,
                })
            else:
                g_id_elem = _find_elem(grantee_elem, f"{ns}ID", "ID")
                g_dn_elem = _find_elem(
                    grantee_elem, f"{ns}DisplayName", "DisplayName"
                )
                g_id = g_id_elem.text if g_id_elem is not None else ""
                g_dn = g_dn_elem.text if g_dn_elem is not None else ""
                grants.append({
                    "grantee": {
                        "type": "CanonicalUser",
                        "id": g_id,
                        "display_name": g_dn,
                    },
                    "permission": permission,
                })

    return {
        "owner": {"id": owner_id, "display_name": owner_display},
        "grants": grants,
    }
