"""Multipart upload S3 request handlers for BleepStore.

Implements multipart operations (Stage 7 + Stage 8):
    - CreateMultipartUpload (POST /{bucket}/{key}?uploads)
    - UploadPart (PUT /{bucket}/{key}?partNumber&uploadId)
    - CompleteMultipartUpload (POST /{bucket}/{key}?uploadId)
    - AbortMultipartUpload (DELETE /{bucket}/{key}?uploadId)
    - ListMultipartUploads (GET /{bucket}?uploads)
    - ListParts (GET /{bucket}/{key}?uploadId)

Crash-only design:
    - Part writes use atomic temp-fsync-rename pattern.
    - Metadata is committed only after storage write succeeds.
    - Upload IDs are UUIDv4 for uniqueness across crashes.
    - CompleteMultipartUpload assembles parts atomically, then commits
      metadata in a single transaction (insert object, delete upload+parts).
"""

import binascii
import hashlib
import json
import logging
import uuid
import xml.etree.ElementTree as ET

from fastapi import FastAPI, Request, Response

from bleepstore.errors import (
    EntityTooSmall,
    InvalidArgument,
    InvalidPart,
    InvalidPartOrder,
    MalformedXML,
    NoSuchBucket,
    NoSuchUpload,
)
from bleepstore.xml_utils import (
    render_complete_multipart_upload,
    render_initiate_multipart_upload,
    render_list_multipart_uploads,
    render_list_parts,
    xml_response,
)

logger = logging.getLogger(__name__)


class MultipartHandler:
    """Handles S3 multipart upload operations.

    Attributes:
        app: The parent FastAPI application.
    """

    def __init__(self, app: FastAPI) -> None:
        """Initialize the multipart handler.

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

    async def create_multipart_upload(self, request: Request, bucket: str, key: str) -> Response:
        """Initiate a new multipart upload.

        Implements: POST /{bucket}/{key}?uploads

        Generates a UUID upload_id, stores the upload metadata in the
        metadata store, and returns InitiateMultipartUploadResult XML.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            XML response with InitiateMultipartUploadResult.
        """
        # Verify bucket exists
        await self._ensure_bucket_exists(bucket)

        # Generate a new upload ID
        upload_id = str(uuid.uuid4())

        # Extract content type and other headers for the final object
        content_type = request.headers.get("content-type", "application/octet-stream")
        content_encoding = request.headers.get("content-encoding")
        content_language = request.headers.get("content-language")
        content_disposition = request.headers.get("content-disposition")
        cache_control = request.headers.get("cache-control")
        expires = request.headers.get("expires")

        # Extract user metadata (x-amz-meta-*)
        user_metadata = self._extract_user_metadata(request)
        user_metadata_json = json.dumps(user_metadata) if user_metadata else "{}"

        # Derive owner info from request state (set by auth middleware)
        owner_id = getattr(request.state, "owner_id", "")
        owner_display = getattr(request.state, "display_name", "")

        # If owner info not on request state, derive from config
        if not owner_id:
            import hashlib
            access_key = self.config.auth.access_key
            owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
            owner_display = access_key

        # Store upload metadata
        await self.metadata.create_multipart_upload(
            bucket=bucket,
            key=key,
            upload_id=upload_id,
            content_type=content_type,
            content_encoding=content_encoding,
            content_language=content_language,
            content_disposition=content_disposition,
            cache_control=cache_control,
            expires=expires,
            user_metadata=user_metadata_json,
            owner_id=owner_id,
            owner_display=owner_display,
        )

        # Render and return XML response
        body = render_initiate_multipart_upload(bucket, key, upload_id)
        return xml_response(body, status=200)

    async def upload_part(self, request: Request, bucket: str, key: str) -> Response:
        """Upload a single part of a multipart upload.

        Implements: PUT /{bucket}/{key}?partNumber={n}&uploadId={id}

        Reads the request body, writes the part to storage using atomic
        temp-fsync-rename, records part metadata, and returns the ETag.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            200 OK with ETag header for the uploaded part.
        """
        # Extract query parameters
        upload_id = request.query_params.get("uploadId", "")
        part_number_str = request.query_params.get("partNumber", "")

        if not upload_id:
            raise InvalidArgument("uploadId is required")
        if not part_number_str:
            raise InvalidArgument("partNumber is required")

        try:
            part_number = int(part_number_str)
        except ValueError:
            raise InvalidArgument("partNumber must be an integer")

        if part_number < 1 or part_number > 10000:
            raise InvalidArgument("Part number must be between 1 and 10000")

        # Verify bucket exists
        await self._ensure_bucket_exists(bucket)

        # Verify the multipart upload exists
        upload = await self.metadata.get_multipart_upload(bucket, key, upload_id)
        if upload is None:
            raise NoSuchUpload(upload_id)

        # Read the part data
        data = await request.body()
        size = len(data)

        # Write part to storage (atomic: temp-fsync-rename)
        md5_hex = await self.storage.put_part(bucket, key, upload_id, part_number, data)

        # Quote the ETag
        etag = f'"{md5_hex}"'

        # Record part metadata (upsert: replaces previous part with same number)
        await self.metadata.put_part(
            upload_id=upload_id,
            part_number=part_number,
            size=size,
            etag=etag,
        )

        return Response(
            status_code=200,
            headers={"ETag": etag},
        )

    async def upload_part_copy(self, request: Request, bucket: str, key: str) -> Response:
        """Upload a part by copying from an existing object.

        Implements: PUT /{bucket}/{key}?partNumber={n}&uploadId={id}
        with x-amz-copy-source header.

        Reads the source object (optionally a byte range), writes it as a
        part using the same atomic storage pattern as upload_part.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            XML response with CopyPartResult containing ETag and LastModified.
        """
        import re
        import urllib.parse

        from bleepstore.errors import NoSuchKey
        from bleepstore.xml_utils import _escape_xml

        # Extract query parameters
        upload_id = request.query_params.get("uploadId", "")
        part_number_str = request.query_params.get("partNumber", "")

        if not upload_id:
            raise InvalidArgument("uploadId is required")
        if not part_number_str:
            raise InvalidArgument("partNumber is required")

        try:
            part_number = int(part_number_str)
        except ValueError:
            raise InvalidArgument("partNumber must be an integer")

        if part_number < 1 or part_number > 10000:
            raise InvalidArgument("Part number must be between 1 and 10000")

        # Verify bucket exists
        await self._ensure_bucket_exists(bucket)

        # Verify the multipart upload exists
        upload = await self.metadata.get_multipart_upload(bucket, key, upload_id)
        if upload is None:
            raise NoSuchUpload(upload_id)

        # Parse x-amz-copy-source header
        copy_source = request.headers.get("x-amz-copy-source", "")
        copy_source = urllib.parse.unquote(copy_source)
        if copy_source.startswith("/"):
            copy_source = copy_source[1:]

        slash_pos = copy_source.find("/")
        if slash_pos < 0:
            raise InvalidArgument("Invalid x-amz-copy-source header")

        src_bucket = copy_source[:slash_pos]
        src_key = copy_source[slash_pos + 1:]

        if not src_bucket or not src_key:
            raise InvalidArgument("Invalid x-amz-copy-source header")

        # Validate source bucket and key exist
        src_bucket_exists = await self.metadata.bucket_exists(src_bucket)
        if not src_bucket_exists:
            raise NoSuchBucket(src_bucket)

        src_meta = await self.metadata.get_object(src_bucket, src_key)
        if src_meta is None:
            raise NoSuchKey(src_key)

        # Read source data
        data = await self.storage.get(src_bucket, src_key)

        # Handle optional x-amz-copy-source-range header
        range_header = request.headers.get("x-amz-copy-source-range", "")
        if range_header:
            range_match = re.match(r"bytes=(\d+)-(\d+)", range_header)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2))
                data = data[start:end + 1]

        # Write part to storage (atomic: temp-fsync-rename)
        md5_hex = await self.storage.put_part(bucket, key, upload_id, part_number, data)

        # Quote the ETag
        etag = f'"{md5_hex}"'

        # Record part metadata (upsert)
        await self.metadata.put_part(
            upload_id=upload_id,
            part_number=part_number,
            size=len(data),
            etag=etag,
        )

        # Build CopyPartResult XML response
        from datetime import datetime, timezone
        last_modified = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        body = "\n".join([
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            f"<ETag>{_escape_xml(etag)}</ETag>",
            f"<LastModified>{_escape_xml(last_modified)}</LastModified>",
            "</CopyPartResult>",
        ])
        from bleepstore.xml_utils import xml_response as _xml_response
        return _xml_response(body, status=200)

    @staticmethod
    def _compute_composite_etag(part_etags: list[str]) -> str:
        """Compute the S3 composite ETag from individual part ETags.

        The composite ETag is computed by concatenating the binary MD5
        digests of each part, computing the MD5 of the concatenation,
        and appending a dash followed by the number of parts.

        Args:
            part_etags: List of quoted ETag strings from each part.

        Returns:
            A quoted composite ETag string, e.g. '"abc123-3"'.
        """
        binary_md5s = b""
        for etag in part_etags:
            clean = etag.strip('"')
            binary_md5s += binascii.unhexlify(clean)
        final_md5 = hashlib.md5(binary_md5s).hexdigest()
        return f'"{final_md5}-{len(part_etags)}"'

    async def complete_multipart_upload(self, request: Request, bucket: str, key: str) -> Response:
        """Complete a multipart upload by assembling parts into the final object.

        Implements: POST /{bucket}/{key}?uploadId={id}

        Parses the XML body listing parts, validates part order and ETags,
        validates minimum part sizes (all except last must be >= 5 MiB),
        assembles parts into the final object atomically, computes the
        composite ETag, commits metadata in a single transaction, and
        cleans up part files.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            XML response with CompleteMultipartUploadResult.
        """
        upload_id = request.query_params.get("uploadId", "")
        if not upload_id:
            raise InvalidArgument("uploadId is required")

        # Verify bucket exists
        await self._ensure_bucket_exists(bucket)

        # Verify the multipart upload exists
        upload = await self.metadata.get_multipart_upload(bucket, key, upload_id)
        if upload is None:
            raise NoSuchUpload(upload_id)

        # Parse the XML body
        body = await request.body()
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            raise MalformedXML()

        # Handle XML namespace
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag[: root.tag.index("}") + 1]

        # Extract parts from XML
        requested_parts: list[dict[str, str]] = []
        for part_elem in root.findall(f"{ns}Part"):
            pn_elem = part_elem.find(f"{ns}PartNumber")
            etag_elem = part_elem.find(f"{ns}ETag")

            if pn_elem is None or pn_elem.text is None:
                raise MalformedXML("Missing PartNumber element")
            if etag_elem is None or etag_elem.text is None:
                raise MalformedXML("Missing ETag element")

            requested_parts.append({
                "part_number": pn_elem.text.strip(),
                "etag": etag_elem.text.strip(),
            })

        if not requested_parts:
            raise MalformedXML("No parts specified in request body")

        # Validate ascending part order
        prev_pn = 0
        for rp in requested_parts:
            try:
                pn = int(rp["part_number"])
            except ValueError:
                raise InvalidArgument(f"Invalid part number: {rp['part_number']}")
            if pn <= prev_pn:
                raise InvalidPartOrder()
            prev_pn = pn

        # Get all stored parts from metadata
        stored_parts = await self.metadata.get_parts_for_completion(upload_id)
        stored_by_number: dict[int, dict] = {
            p["part_number"]: p for p in stored_parts
        }

        # Validate each requested part exists and ETags match
        validated_parts: list[dict] = []
        for rp in requested_parts:
            pn = int(rp["part_number"])
            stored = stored_by_number.get(pn)
            if stored is None:
                raise InvalidPart(
                    f"One or more of the specified parts could not be found. "
                    f"The part may not have been uploaded, or the specified "
                    f"entity tag may not have matched the part's entity tag."
                )

            # Normalize ETags for comparison (strip quotes if present)
            requested_etag = rp["etag"].strip('"')
            stored_etag = stored["etag"].strip('"')
            if requested_etag != stored_etag:
                raise InvalidPart(
                    f"One or more of the specified parts could not be found. "
                    f"The part may not have been uploaded, or the specified "
                    f"entity tag may not have matched the part's entity tag."
                )

            validated_parts.append(stored)

        # Validate part sizes: all parts except last must be >= 5 MiB
        MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MiB
        for i, part in enumerate(validated_parts[:-1]):  # all except last
            if part["size"] < MIN_PART_SIZE:
                raise EntityTooSmall(
                    f"Your proposed upload is smaller than the minimum allowed size. "
                    f"Part {part['part_number']} has size {part['size']} bytes."
                )

        # Compute composite ETag from stored part ETags
        part_etags = [p["etag"] for p in validated_parts]
        composite_etag = self._compute_composite_etag(part_etags)

        # Assemble parts into final object on disk (atomic)
        part_numbers = [int(rp["part_number"]) for rp in requested_parts]
        await self.storage.assemble_parts(bucket, key, upload_id, part_numbers)

        # Compute total size from validated parts
        total_size = sum(p["size"] for p in validated_parts)

        # Commit metadata atomically (insert object, delete upload+parts)
        await self.metadata.complete_multipart_upload(
            bucket=bucket,
            key=key,
            upload_id=upload_id,
            size=total_size,
            etag=composite_etag,
            content_type=upload.get("content_type", "application/octet-stream"),
            content_encoding=upload.get("content_encoding"),
            content_language=upload.get("content_language"),
            content_disposition=upload.get("content_disposition"),
            cache_control=upload.get("cache_control"),
            expires=upload.get("expires"),
            storage_class=upload.get("storage_class", "STANDARD"),
            acl=upload.get("acl", "{}"),
            user_metadata=upload.get("user_metadata", "{}"),
        )

        # Clean up part files from storage
        try:
            await self.storage.delete_parts(bucket, key, upload_id)
        except Exception:
            logger.warning(
                "Failed to clean up part files for upload %s after completion: %s/%s",
                upload_id, bucket, key,
                exc_info=True,
            )

        # Build the location URL
        location = f"http://localhost/{bucket}/{key}"

        body_xml = render_complete_multipart_upload(
            location=location,
            bucket=bucket,
            key=key,
            etag=composite_etag,
        )
        return xml_response(body_xml, status=200)

    async def abort_multipart_upload(self, request: Request, bucket: str, key: str) -> Response:
        """Abort an in-progress multipart upload and discard parts.

        Implements: DELETE /{bucket}/{key}?uploadId={id}

        Deletes all stored part files and metadata records for the upload.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            204 No Content on success.
        """
        upload_id = request.query_params.get("uploadId", "")
        if not upload_id:
            raise InvalidArgument("uploadId is required")

        # Verify bucket exists
        await self._ensure_bucket_exists(bucket)

        # Verify the multipart upload exists
        upload = await self.metadata.get_multipart_upload(bucket, key, upload_id)
        if upload is None:
            raise NoSuchUpload(upload_id)

        # Delete part files from storage
        try:
            await self.storage.delete_parts(bucket, key, upload_id)
        except Exception:
            logger.warning(
                "Failed to delete part files for upload %s: %s/%s",
                upload_id, bucket, key,
                exc_info=True,
            )

        # Delete upload and part metadata
        await self.metadata.abort_multipart_upload(bucket, key, upload_id)

        return Response(status_code=204)

    async def list_uploads(self, request: Request, bucket: str) -> Response:
        """List all in-progress multipart uploads for a bucket.

        Implements: GET /{bucket}?uploads

        Supports prefix, delimiter, max-uploads, key-marker, and
        upload-id-marker query parameters for filtering and pagination.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            XML response with ListMultipartUploadsResult.
        """
        # Verify bucket exists
        await self._ensure_bucket_exists(bucket)

        # Extract query parameters
        prefix = request.query_params.get("prefix", "")
        delimiter = request.query_params.get("delimiter", "")
        key_marker = request.query_params.get("key-marker", "")
        upload_id_marker = request.query_params.get("upload-id-marker", "")
        max_uploads_str = request.query_params.get("max-uploads", "1000")

        try:
            max_uploads = int(max_uploads_str)
            if max_uploads < 0:
                max_uploads = 0
            if max_uploads > 1000:
                max_uploads = 1000
        except ValueError:
            max_uploads = 1000

        # Query metadata store
        result = await self.metadata.list_multipart_uploads(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_uploads=max_uploads,
            key_marker=key_marker,
            upload_id_marker=upload_id_marker,
        )

        body = render_list_multipart_uploads(
            bucket=bucket,
            uploads=result["uploads"],
            prefix=prefix,
            delimiter=delimiter,
            max_uploads=max_uploads,
            is_truncated=result["is_truncated"],
            key_marker=key_marker,
            upload_id_marker=upload_id_marker,
            next_key_marker=result.get("next_key_marker"),
            next_upload_id_marker=result.get("next_upload_id_marker"),
            common_prefixes=result.get("common_prefixes"),
        )
        return xml_response(body, status=200)

    async def list_parts(self, request: Request, bucket: str, key: str) -> Response:
        """List uploaded parts for a specific multipart upload.

        Implements: GET /{bucket}/{key}?uploadId={id}

        Supports part-number-marker and max-parts query parameters
        for pagination.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.
            key: The object key from the URL path.

        Returns:
            XML response with ListPartsResult.
        """
        upload_id = request.query_params.get("uploadId", "")
        if not upload_id:
            raise InvalidArgument("uploadId is required")

        # Verify bucket exists
        await self._ensure_bucket_exists(bucket)

        # Verify the multipart upload exists
        upload = await self.metadata.get_multipart_upload(bucket, key, upload_id)
        if upload is None:
            raise NoSuchUpload(upload_id)

        # Extract pagination parameters
        part_number_marker_str = request.query_params.get("part-number-marker", "0")
        max_parts_str = request.query_params.get("max-parts", "1000")

        try:
            part_number_marker = int(part_number_marker_str)
        except ValueError:
            part_number_marker = 0

        try:
            max_parts = int(max_parts_str)
            if max_parts < 0:
                max_parts = 0
            if max_parts > 1000:
                max_parts = 1000
        except ValueError:
            max_parts = 1000

        # Query metadata store for parts
        result = await self.metadata.list_parts(
            upload_id=upload_id,
            part_number_marker=part_number_marker,
            max_parts=max_parts,
        )

        body = render_list_parts(
            bucket=bucket,
            key=key,
            upload_id=upload_id,
            parts=result["parts"],
            is_truncated=result["is_truncated"],
            part_number_marker=part_number_marker,
            next_part_number_marker=result.get("next_part_number_marker"),
            max_parts=max_parts,
            storage_class=upload.get("storage_class", "STANDARD"),
            owner_id=upload.get("owner_id", ""),
            owner_display=upload.get("owner_display", ""),
        )
        return xml_response(body, status=200)
