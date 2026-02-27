"""S3 XML response rendering helpers for BleepStore."""

import urllib.parse
from typing import Any
from xml.sax.saxutils import escape as _sax_escape

from fastapi.responses import Response


def _escape_xml(value: str) -> str:
    """Escape special XML characters in a string value.

    Args:
        value: The raw string to escape.

    Returns:
        The XML-safe escaped string.
    """
    return _sax_escape(str(value))


def render_error(
    code: str,
    message: str,
    resource: str = "",
    request_id: str = "",
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Render an S3 XML error response body.

    The Error element has NO XML namespace (unlike success responses).

    Args:
        code: The S3 error code (e.g. "NoSuchBucket").
        message: Human-readable error message.
        resource: The resource that triggered the error.
        request_id: An opaque request identifier.
        extra_fields: Additional XML elements to include.

    Returns:
        An XML string conforming to S3 error response format.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<Error>",
        f"<Code>{_escape_xml(code)}</Code>",
        f"<Message>{_escape_xml(message)}</Message>",
    ]
    if resource:
        parts.append(f"<Resource>{_escape_xml(resource)}</Resource>")
    if request_id:
        parts.append(f"<RequestId>{_escape_xml(request_id)}</RequestId>")
    if extra_fields:
        for key, value in extra_fields.items():
            parts.append(f"<{key}>{_escape_xml(value)}</{key}>")
    parts.append("</Error>")
    return "\n".join(parts)


def xml_response(body: str, status: int = 200) -> Response:
    """Wrap an XML body string in a FastAPI Response with correct content type.

    Args:
        body: The XML body string.
        status: HTTP status code.

    Returns:
        A FastAPI Response with media_type application/xml.
    """
    return Response(
        content=body,
        status_code=status,
        media_type="application/xml",
    )


def render_list_buckets(
    owner_id: str,
    owner_display_name: str,
    buckets: list[dict[str, Any]],
) -> str:
    """Render an S3 ListAllMyBuckets XML response.

    Args:
        owner_id: The canonical user ID of the bucket owner.
        owner_display_name: Display name of the owner.
        buckets: List of dicts with 'name' and 'created_at' keys.

    Returns:
        An XML string for ListAllMyBucketsResult.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        "<Owner>",
        f"<ID>{_escape_xml(owner_id)}</ID>",
        f"<DisplayName>{_escape_xml(owner_display_name)}</DisplayName>",
        "</Owner>",
        "<Buckets>",
    ]

    for b in buckets:
        name = _escape_xml(b.get("name", ""))
        created_at = _escape_xml(b.get("created_at", ""))
        parts.append("<Bucket>")
        parts.append(f"<Name>{name}</Name>")
        parts.append(f"<CreationDate>{created_at}</CreationDate>")
        parts.append("</Bucket>")

    parts.append("</Buckets>")
    parts.append("</ListAllMyBucketsResult>")
    return "\n".join(parts)


def render_location_constraint(region: str) -> str:
    """Render an S3 GetBucketLocation XML response.

    The us-east-1 quirk: returns an empty ``<LocationConstraint/>`` element
    instead of the string ``us-east-1``.

    Args:
        region: The bucket's region string.

    Returns:
        An XML string for LocationConstraint.
    """
    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    if region == "us-east-1" or not region:
        parts.append('<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>')
    else:
        parts.append(
            f'<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"{_escape_xml(region)}</LocationConstraint>"
        )
    return "\n".join(parts)


def render_list_objects_v2(
    name: str,
    prefix: str,
    delimiter: str,
    max_keys: int,
    is_truncated: bool,
    contents: list[dict[str, Any]],
    common_prefixes: list[str],
    continuation_token: str | None = None,
    next_continuation_token: str | None = None,
    key_count: int = 0,
    encoding_type: str | None = None,
    start_after: str = "",
) -> str:
    """Render an S3 ListObjectsV2 XML response.

    Args:
        name: Bucket name.
        prefix: Key prefix filter.
        delimiter: Grouping delimiter.
        max_keys: Maximum keys to return.
        is_truncated: Whether more results are available.
        contents: List of object metadata dicts.
        common_prefixes: Collapsed prefix groups.
        continuation_token: The token used for this request.
        next_continuation_token: Token for the next page.
        key_count: Number of keys returned.
        encoding_type: Encoding type for keys, if any.
        start_after: The start-after value used for this request.

    Returns:
        An XML string for ListBucketResult.
    """

    def encode_key(key: str) -> str:
        if encoding_type == "url":
            return urllib.parse.quote(key, safe="")
        return key

    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"<Name>{_escape_xml(name)}</Name>",
        f"<Prefix>{_escape_xml(prefix)}</Prefix>",
    ]

    if delimiter:
        parts.append(f"<Delimiter>{_escape_xml(delimiter)}</Delimiter>")

    if encoding_type:
        parts.append(f"<EncodingType>{_escape_xml(encoding_type)}</EncodingType>")

    parts.append(f"<MaxKeys>{max_keys}</MaxKeys>")

    if start_after:
        parts.append(f"<StartAfter>{_escape_xml(start_after)}</StartAfter>")

    if continuation_token:
        parts.append(f"<ContinuationToken>{_escape_xml(continuation_token)}</ContinuationToken>")

    parts.append(f"<KeyCount>{key_count}</KeyCount>")
    parts.append(f"<IsTruncated>{str(is_truncated).lower()}</IsTruncated>")

    if is_truncated and next_continuation_token:
        parts.append(
            f"<NextContinuationToken>{_escape_xml(next_continuation_token)}</NextContinuationToken>"
        )

    for obj in contents:
        parts.append("<Contents>")
        parts.append(f"<Key>{_escape_xml(encode_key(obj.get('key', '')))}</Key>")
        parts.append(f"<LastModified>{_escape_xml(obj.get('last_modified', ''))}</LastModified>")
        parts.append(f"<ETag>{_escape_xml(obj.get('etag', ''))}</ETag>")
        parts.append(f"<Size>{obj.get('size', 0)}</Size>")
        parts.append(
            f"<StorageClass>{_escape_xml(obj.get('storage_class', 'STANDARD'))}</StorageClass>"
        )
        parts.append("</Contents>")

    for cp in common_prefixes:
        parts.append("<CommonPrefixes>")
        parts.append(f"<Prefix>{_escape_xml(encode_key(cp))}</Prefix>")
        parts.append("</CommonPrefixes>")

    parts.append("</ListBucketResult>")
    return "\n".join(parts)


def render_list_objects_v1(
    name: str,
    prefix: str,
    delimiter: str,
    max_keys: int,
    is_truncated: bool,
    contents: list[dict[str, Any]],
    common_prefixes: list[str],
    marker: str = "",
    next_marker: str | None = None,
    encoding_type: str | None = None,
) -> str:
    """Render an S3 ListObjects (v1) XML response.

    Args:
        name: Bucket name.
        prefix: Key prefix filter.
        delimiter: Grouping delimiter.
        max_keys: Maximum keys to return.
        is_truncated: Whether more results are available.
        contents: List of object metadata dicts.
        common_prefixes: Collapsed prefix groups.
        marker: The marker used for this request.
        next_marker: Marker for the next page.
        encoding_type: Encoding type for keys, if any.

    Returns:
        An XML string for ListBucketResult.
    """

    def encode_key(key: str) -> str:
        if encoding_type == "url":
            return urllib.parse.quote(key, safe="")
        return key

    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"<Name>{_escape_xml(name)}</Name>",
        f"<Prefix>{_escape_xml(prefix)}</Prefix>",
        f"<Marker>{_escape_xml(marker)}</Marker>",
    ]

    if delimiter:
        parts.append(f"<Delimiter>{_escape_xml(delimiter)}</Delimiter>")

    if encoding_type:
        parts.append(f"<EncodingType>{_escape_xml(encoding_type)}</EncodingType>")

    parts.append(f"<MaxKeys>{max_keys}</MaxKeys>")
    parts.append(f"<IsTruncated>{str(is_truncated).lower()}</IsTruncated>")

    if is_truncated and next_marker:
        parts.append(f"<NextMarker>{_escape_xml(encode_key(next_marker))}</NextMarker>")

    for obj in contents:
        parts.append("<Contents>")
        parts.append(f"<Key>{_escape_xml(encode_key(obj.get('key', '')))}</Key>")
        parts.append(f"<LastModified>{_escape_xml(obj.get('last_modified', ''))}</LastModified>")
        parts.append(f"<ETag>{_escape_xml(obj.get('etag', ''))}</ETag>")
        parts.append(f"<Size>{obj.get('size', 0)}</Size>")
        parts.append(
            f"<StorageClass>{_escape_xml(obj.get('storage_class', 'STANDARD'))}</StorageClass>"
        )
        parts.append("</Contents>")

    for cp in common_prefixes:
        parts.append("<CommonPrefixes>")
        parts.append(f"<Prefix>{_escape_xml(encode_key(cp))}</Prefix>")
        parts.append("</CommonPrefixes>")

    parts.append("</ListBucketResult>")
    return "\n".join(parts)


def render_copy_object_result(etag: str, last_modified: str) -> str:
    """Render an S3 CopyObject result XML response.

    Args:
        etag: The ETag of the newly copied object.
        last_modified: ISO 8601 timestamp of the copy.

    Returns:
        An XML string for CopyObjectResult.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"<ETag>{_escape_xml(etag)}</ETag>",
        f"<LastModified>{_escape_xml(last_modified)}</LastModified>",
        "</CopyObjectResult>",
    ]
    return "\n".join(parts)


def render_delete_result(
    deleted: list[dict[str, str]],
    errors: list[dict[str, str]],
) -> str:
    """Render an S3 multi-object delete result XML response.

    Args:
        deleted: List of dicts with 'key' (and optional 'version_id') for
            successfully deleted objects.
        errors: List of dicts with 'key', 'code', and 'message' for failures.

    Returns:
        An XML string for DeleteResult.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    ]

    for d in deleted:
        parts.append("<Deleted>")
        parts.append(f"<Key>{_escape_xml(d['key'])}</Key>")
        if d.get("version_id"):
            parts.append(f"<VersionId>{_escape_xml(d['version_id'])}</VersionId>")
        parts.append("</Deleted>")

    for e in errors:
        parts.append("<Error>")
        parts.append(f"<Key>{_escape_xml(e['key'])}</Key>")
        parts.append(f"<Code>{_escape_xml(e['code'])}</Code>")
        parts.append(f"<Message>{_escape_xml(e['message'])}</Message>")
        parts.append("</Error>")

    parts.append("</DeleteResult>")
    return "\n".join(parts)


def render_initiate_multipart_upload(
    bucket: str,
    key: str,
    upload_id: str,
) -> str:
    """Render an S3 InitiateMultipartUpload result.

    Args:
        bucket: Bucket name.
        key: Object key.
        upload_id: The new upload identifier.

    Returns:
        An XML string for InitiateMultipartUploadResult.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"<Bucket>{_escape_xml(bucket)}</Bucket>",
        f"<Key>{_escape_xml(key)}</Key>",
        f"<UploadId>{_escape_xml(upload_id)}</UploadId>",
        "</InitiateMultipartUploadResult>",
    ]
    return "\n".join(parts)


def render_complete_multipart_upload(
    location: str,
    bucket: str,
    key: str,
    etag: str,
) -> str:
    """Render an S3 CompleteMultipartUpload result.

    Args:
        location: Full URL of the created object.
        bucket: Bucket name.
        key: Object key.
        etag: ETag of the assembled object (composite format with dash).

    Returns:
        An XML string for CompleteMultipartUploadResult.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"<Location>{_escape_xml(location)}</Location>",
        f"<Bucket>{_escape_xml(bucket)}</Bucket>",
        f"<Key>{_escape_xml(key)}</Key>",
        f"<ETag>{_escape_xml(etag)}</ETag>",
        "</CompleteMultipartUploadResult>",
    ]
    return "\n".join(parts)


def render_list_multipart_uploads(
    bucket: str,
    uploads: list[dict[str, Any]],
    prefix: str = "",
    delimiter: str = "",
    max_uploads: int = 1000,
    is_truncated: bool = False,
    key_marker: str = "",
    upload_id_marker: str = "",
    next_key_marker: str | None = None,
    next_upload_id_marker: str | None = None,
    common_prefixes: list[str] | None = None,
) -> str:
    """Render an S3 ListMultipartUploads result.

    Args:
        bucket: Bucket name.
        uploads: List of upload metadata dicts.
        prefix: Key prefix filter.
        delimiter: Grouping delimiter.
        max_uploads: Maximum uploads to return.
        is_truncated: Whether more results are available.
        key_marker: Key marker for pagination.
        upload_id_marker: Upload ID marker for pagination.
        next_key_marker: Next key marker for pagination.
        next_upload_id_marker: Next upload ID marker.
        common_prefixes: List of common prefix strings for delimiter grouping.

    Returns:
        An XML string for ListMultipartUploadsResult.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"<Bucket>{_escape_xml(bucket)}</Bucket>",
    ]

    parts.append(f"<KeyMarker>{_escape_xml(key_marker)}</KeyMarker>")
    parts.append(f"<UploadIdMarker>{_escape_xml(upload_id_marker)}</UploadIdMarker>")

    if is_truncated and next_key_marker:
        parts.append(f"<NextKeyMarker>{_escape_xml(next_key_marker)}</NextKeyMarker>")
    if is_truncated and next_upload_id_marker:
        parts.append(
            f"<NextUploadIdMarker>{_escape_xml(next_upload_id_marker)}</NextUploadIdMarker>"
        )

    parts.append(f"<MaxUploads>{max_uploads}</MaxUploads>")
    parts.append(f"<IsTruncated>{str(is_truncated).lower()}</IsTruncated>")

    if prefix:
        parts.append(f"<Prefix>{_escape_xml(prefix)}</Prefix>")

    if delimiter:
        parts.append(f"<Delimiter>{_escape_xml(delimiter)}</Delimiter>")

    for upload in uploads:
        parts.append("<Upload>")
        parts.append(f"<Key>{_escape_xml(upload.get('key', ''))}</Key>")
        parts.append(f"<UploadId>{_escape_xml(upload.get('upload_id', ''))}</UploadId>")
        parts.append("<Initiator>")
        parts.append(f"<ID>{_escape_xml(upload.get('owner_id', ''))}</ID>")
        parts.append(f"<DisplayName>{_escape_xml(upload.get('owner_display', ''))}</DisplayName>")
        parts.append("</Initiator>")
        parts.append("<Owner>")
        parts.append(f"<ID>{_escape_xml(upload.get('owner_id', ''))}</ID>")
        parts.append(f"<DisplayName>{_escape_xml(upload.get('owner_display', ''))}</DisplayName>")
        parts.append("</Owner>")
        parts.append(
            f"<StorageClass>{_escape_xml(upload.get('storage_class', 'STANDARD'))}</StorageClass>"
        )
        parts.append(f"<Initiated>{_escape_xml(upload.get('initiated_at', ''))}</Initiated>")
        parts.append("</Upload>")

    if common_prefixes:
        for cp in common_prefixes:
            parts.append("<CommonPrefixes>")
            parts.append(f"<Prefix>{_escape_xml(cp)}</Prefix>")
            parts.append("</CommonPrefixes>")

    parts.append("</ListMultipartUploadsResult>")
    return "\n".join(parts)


def render_list_parts(
    bucket: str,
    key: str,
    upload_id: str,
    parts: list[dict[str, Any]],
    is_truncated: bool = False,
    part_number_marker: int = 0,
    next_part_number_marker: int | None = None,
    max_parts: int = 1000,
    storage_class: str = "STANDARD",
    owner_id: str = "",
    owner_display: str = "",
) -> str:
    """Render an S3 ListParts result.

    Args:
        bucket: Bucket name.
        key: Object key.
        upload_id: Multipart upload ID.
        parts: List of part metadata dicts.
        is_truncated: Whether more parts are available.
        part_number_marker: Part number marker for pagination.
        next_part_number_marker: Next marker for pagination.
        max_parts: Maximum parts to return.
        storage_class: Storage class for the upload.
        owner_id: Owner canonical user ID.
        owner_display: Owner display name.

    Returns:
        An XML string for ListPartsResult.
    """
    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"<Bucket>{_escape_xml(bucket)}</Bucket>",
        f"<Key>{_escape_xml(key)}</Key>",
        f"<UploadId>{_escape_xml(upload_id)}</UploadId>",
    ]

    xml_parts.append("<Initiator>")
    xml_parts.append(f"<ID>{_escape_xml(owner_id)}</ID>")
    xml_parts.append(f"<DisplayName>{_escape_xml(owner_display)}</DisplayName>")
    xml_parts.append("</Initiator>")

    xml_parts.append("<Owner>")
    xml_parts.append(f"<ID>{_escape_xml(owner_id)}</ID>")
    xml_parts.append(f"<DisplayName>{_escape_xml(owner_display)}</DisplayName>")
    xml_parts.append("</Owner>")

    xml_parts.append(f"<StorageClass>{_escape_xml(storage_class)}</StorageClass>")
    xml_parts.append(f"<PartNumberMarker>{part_number_marker}</PartNumberMarker>")

    if is_truncated and next_part_number_marker is not None:
        xml_parts.append(f"<NextPartNumberMarker>{next_part_number_marker}</NextPartNumberMarker>")

    xml_parts.append(f"<MaxParts>{max_parts}</MaxParts>")
    xml_parts.append(f"<IsTruncated>{str(is_truncated).lower()}</IsTruncated>")

    for part in parts:
        xml_parts.append("<Part>")
        xml_parts.append(f"<PartNumber>{part.get('part_number', 0)}</PartNumber>")
        xml_parts.append(
            f"<LastModified>{_escape_xml(part.get('last_modified', ''))}</LastModified>"
        )
        xml_parts.append(f"<ETag>{_escape_xml(part.get('etag', ''))}</ETag>")
        xml_parts.append(f"<Size>{part.get('size', 0)}</Size>")
        xml_parts.append("</Part>")

    xml_parts.append("</ListPartsResult>")
    return "\n".join(xml_parts)
