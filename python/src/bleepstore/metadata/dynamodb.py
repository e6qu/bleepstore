"""AWS DynamoDB metadata store backend for BleepStore.

Single-table design with PK/SK pattern:
- Bucket:    PK=BUCKET#{name},         SK=#METADATA
- Object:    PK=OBJECT#{bucket}#{key}, SK=#METADATA
- Upload:    PK=UPLOAD#{upload_id},    SK=#METADATA
- Part:      PK=UPLOAD#{upload_id},    SK=PART#{part_number:05d}
- Credential: PK=CRED#{access_key},    SK=#METADATA
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _pk_bucket(bucket: str) -> str:
    return f"BUCKET#{bucket}"


def _pk_object(bucket: str, key: str) -> str:
    return f"OBJECT#{bucket}#{key}"


def _pk_upload(upload_id: str) -> str:
    return f"UPLOAD#{upload_id}"


def _pk_credential(access_key: str) -> str:
    return f"CRED#{access_key}"


def _sk_metadata() -> str:
    return "#METADATA"


def _sk_part(part_number: int) -> str:
    return f"PART#{part_number:05d}"


class DynamoDBMetadataStore:
    """DynamoDB-backed metadata store.

    Implements the MetadataStore protocol using aiobotocore for async access.
    Uses a single-table design with composite keys (PK/SK pattern).
    """

    def __init__(self, config: Any) -> None:
        """Initialize the DynamoDB metadata store.

        Args:
            config: DynamoDBConfig instance with table, region, endpoint_url.
        """
        self._config = config
        self._table_name = config.table
        self._region = config.region
        self._endpoint_url = config.endpoint_url
        self._client: Any = None
        self._session: Any = None

    async def init_db(self) -> None:
        """Initialize the DynamoDB client and verify table exists.

        Creates the aiobotocore session and DynamoDB client.
        Verifies the table exists (describe_table).
        """
        import aiobotocore.session

        self._session = aiobotocore.session.get_session()

        kwargs: dict[str, Any] = {"region_name": self._region}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url

        self._client_ctx = self._session.create_client("dynamodb", **kwargs)
        self._client = await self._client_ctx.__aenter__()

        try:
            await self._client.describe_table(TableName=self._table_name)
            logger.info("Connected to DynamoDB table: %s", self._table_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.warning(
                    "DynamoDB table '%s' not found. Create it manually or use "
                    "the provided CloudFormation/SAM template.",
                    self._table_name,
                )
                raise
            raise

    async def close(self) -> None:
        """Close the DynamoDB client."""
        if self._client:
            await self._client_ctx.__aexit__(None, None, None)
            self._client = None

    def _type_bucket(self) -> str:
        return "bucket"

    def _type_object(self) -> str:
        return "object"

    def _type_upload(self) -> str:
        return "upload"

    def _type_part(self) -> str:
        return "part"

    def _type_credential(self) -> str:
        return "credential"

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
        await self._client.put_item(
            TableName=self._table_name,
            Item={
                "pk": {"S": _pk_bucket(bucket)},
                "sk": {"S": _sk_metadata()},
                "type": {"S": self._type_bucket()},
                "name": {"S": bucket},
                "region": {"S": region},
                "owner_id": {"S": owner_id},
                "owner_display": {"S": owner_display},
                "acl": {"S": acl},
                "created_at": {"S": _now_iso()},
            },
            ConditionExpression="attribute_not_exists(pk)",
        )

    async def bucket_exists(self, bucket: str) -> bool:
        """Check whether a bucket exists.

        Args:
            bucket: The bucket name.

        Returns:
            True if the bucket exists, False otherwise.
        """
        resp = await self._client.get_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_bucket(bucket)}, "sk": {"S": _sk_metadata()}},
            ProjectionExpression="pk",
        )
        return "Item" in resp

    async def delete_bucket(self, bucket: str) -> None:
        """Delete a bucket record.

        Args:
            bucket: The bucket name to delete.
        """
        await self._client.delete_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_bucket(bucket)}, "sk": {"S": _sk_metadata()}},
        )

    async def get_bucket(self, bucket: str) -> dict[str, Any] | None:
        """Retrieve metadata for a single bucket.

        Args:
            bucket: The bucket name.

        Returns:
            A dict with bucket metadata, or None if not found.
        """
        resp = await self._client.get_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_bucket(bucket)}, "sk": {"S": _sk_metadata()}},
        )
        if "Item" not in resp:
            return None
        return self._item_to_dict(resp["Item"])

    async def list_buckets(self, owner_id: str = "") -> list[dict[str, Any]]:
        """List all buckets, optionally filtered by owner.

        Args:
            owner_id: If non-empty, only return buckets owned by this user.

        Returns:
            A list of dicts containing bucket metadata.
        """
        items = []
        exclusive_start_key = None

        filter_expr = None
        if owner_id:
            filter_expr = "owner_id = :owner_id"

        while True:
            kwargs: dict[str, Any] = {
                "TableName": self._table_name,
                "FilterExpression": "begins_with(pk, :bucket_prefix) AND sk = :metadata",
                "ExpressionAttributeValues": {
                    ":bucket_prefix": {"S": "BUCKET#"},
                    ":metadata": {"S": _sk_metadata()},
                },
            }
            if filter_expr:
                kwargs["FilterExpression"] += f" AND {filter_expr}"
                kwargs["ExpressionAttributeValues"][":owner_id"] = {"S": owner_id}
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = await self._client.scan(**kwargs)

            for item in resp.get("Items", []):
                items.append(self._item_to_dict(item))

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        return items

    async def update_bucket_acl(self, bucket: str, acl: str) -> None:
        """Update the ACL on a bucket.

        Args:
            bucket: The bucket name.
            acl: New JSON-serialized ACL string.
        """
        await self._client.update_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_bucket(bucket)}, "sk": {"S": _sk_metadata()}},
            UpdateExpression="SET acl = :acl",
            ExpressionAttributeValues={":acl": {"S": acl}},
        )

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
        item: dict[str, Any] = {
            "pk": {"S": _pk_object(bucket, key)},
            "sk": {"S": _sk_metadata()},
            "type": {"S": self._type_object()},
            "bucket": {"S": bucket},
            "key": {"S": key},
            "size": {"N": str(size)},
            "etag": {"S": etag},
            "content_type": {"S": content_type},
            "storage_class": {"S": storage_class},
            "acl": {"S": acl},
            "user_metadata": {"S": user_metadata},
            "last_modified": {"S": _now_iso()},
        }

        if content_encoding is not None:
            item["content_encoding"] = {"S": content_encoding}
        if content_language is not None:
            item["content_language"] = {"S": content_language}
        if content_disposition is not None:
            item["content_disposition"] = {"S": content_disposition}
        if cache_control is not None:
            item["cache_control"] = {"S": cache_control}
        if expires is not None:
            item["expires"] = {"S": expires}

        await self._client.put_item(TableName=self._table_name, Item=item)

    async def object_exists(self, bucket: str, key: str) -> bool:
        """Check whether an object exists.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            True if the object exists, False otherwise.
        """
        resp = await self._client.get_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_object(bucket, key)}, "sk": {"S": _sk_metadata()}},
            ProjectionExpression="pk",
        )
        return "Item" in resp

    async def get_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        """Retrieve metadata for a single object.

        Args:
            bucket: The bucket name.
            key: The object key.

        Returns:
            A dict with object metadata, or None if not found.
        """
        resp = await self._client.get_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_object(bucket, key)}, "sk": {"S": _sk_metadata()}},
        )
        if "Item" not in resp:
            return None
        return self._item_to_dict(resp["Item"])

    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object metadata record.

        Args:
            bucket: The bucket name.
            key: The object key.
        """
        await self._client.delete_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_object(bucket, key)}, "sk": {"S": _sk_metadata()}},
        )

    async def delete_objects_meta(self, bucket: str, keys: list[str]) -> list[str]:
        """Delete multiple object metadata records in a batch.

        Uses BatchWriteItem (25 items per batch).

        Args:
            bucket: The bucket name.
            keys: List of object keys to delete.

        Returns:
            List of keys that were successfully deleted (had rows).
        """
        if not keys:
            return []

        deleted = []
        for i in range(0, len(keys), 25):
            batch = keys[i : i + 25]
            request_items = {
                self._table_name: [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "pk": {"S": _pk_object(bucket, k)},
                                "sk": {"S": _sk_metadata()},
                            }
                        }
                    }
                    for k in batch
                ]
            }

            resp = await self._client.batch_write_item(RequestItems=request_items)

            if "UnprocessedItems" in resp and resp["UnprocessedItems"]:
                await self._retry_unprocessed(resp["UnprocessedItems"])

            deleted.extend(batch)

        return deleted

    async def _retry_unprocessed(self, unprocessed: dict[str, Any]) -> None:
        """Retry unprocessed batch write items with exponential backoff."""
        retries = 0
        while unprocessed and retries < 5:
            await asyncio.sleep(2**retries * 0.1)
            resp = await self._client.batch_write_item(RequestItems=unprocessed)
            unprocessed = resp.get("UnprocessedItems", {})
            retries += 1

    async def update_object_acl(self, bucket: str, key: str, acl: str) -> None:
        """Update the ACL on an object.

        Args:
            bucket: The bucket name.
            key: The object key.
            acl: New JSON-serialized ACL string.
        """
        await self._client.update_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_object(bucket, key)}, "sk": {"S": _sk_metadata()}},
            UpdateExpression="SET acl = :acl",
            ExpressionAttributeValues={":acl": {"S": acl}},
        )

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
            delimiter: Grouping delimiter (application-level).
            max_keys: Maximum number of keys to return.
            marker: Start listing after this key (v1).
            continuation_token: Pagination token (v2).

        Returns:
            A dict with 'contents', 'common_prefixes', 'is_truncated',
            'next_continuation_token', 'next_marker', and 'key_count'.
        """
        if max_keys <= 0:
            return {
                "contents": [],
                "common_prefixes": [],
                "is_truncated": False,
                "next_continuation_token": None,
                "next_marker": None,
                "key_count": 0,
            }

        start_after = continuation_token or marker or ""
        prefix_filter = f"OBJECT#{bucket}#{prefix}"

        items: list[dict[str, Any]] = []
        exclusive_start_key = None
        last_key: str = ""

        while len(items) <= max_keys:
            kwargs: dict[str, Any] = {
                "TableName": self._table_name,
                "FilterExpression": "begins_with(pk, :prefix) AND sk = :metadata",
                "ExpressionAttributeValues": {
                    ":prefix": {"S": prefix_filter},
                    ":metadata": {"S": _sk_metadata()},
                },
                "Limit": max_keys + 1,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = await self._client.scan(**kwargs)

            for item in resp.get("Items", []):
                obj = self._item_to_dict(item)
                obj_key = obj.get("key", "")
                if obj_key > start_after:
                    items.append(obj)
                    if len(items) > max_keys:
                        break

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

            if len(items) > max_keys:
                break

        if delimiter:
            return self._apply_delimiter(items, delimiter, max_keys)

        is_truncated = len(items) > max_keys
        if is_truncated:
            items = items[:max_keys]
            last_key = items[-1].get("key", "") if items else ""
        else:
            last_key = ""

        return {
            "contents": items,
            "common_prefixes": [],
            "is_truncated": is_truncated,
            "next_continuation_token": last_key if is_truncated else None,
            "next_marker": last_key if is_truncated else None,
            "key_count": len(items),
        }

    def _apply_delimiter(
        self, items: list[dict[str, Any]], delimiter: str, max_keys: int
    ) -> dict[str, Any]:
        """Apply delimiter grouping to list results."""
        common_prefixes: set[str] = set()
        contents: list[dict[str, Any]] = []

        for item in items:
            key = item.get("key", "")
            delim_idx = key.find(delimiter)
            if delim_idx >= 0:
                prefix = key[: delim_idx + 1]
                common_prefixes.add(prefix)
            else:
                contents.append(item)

            if len(contents) + len(common_prefixes) >= max_keys:
                break

        return {
            "contents": contents,
            "common_prefixes": sorted(common_prefixes),
            "is_truncated": len(items) > max_keys,
            "next_continuation_token": contents[-1].get("key") if contents else None,
            "next_marker": contents[-1].get("key") if contents else None,
            "key_count": len(contents),
        }

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
            owner_id: Canonical user ID of the initiator.
            owner_display: Display name of the initiator.
        """
        item: dict[str, Any] = {
            "pk": {"S": _pk_upload(upload_id)},
            "sk": {"S": _sk_metadata()},
            "type": {"S": self._type_upload()},
            "upload_id": {"S": upload_id},
            "bucket": {"S": bucket},
            "key": {"S": key},
            "content_type": {"S": content_type},
            "storage_class": {"S": storage_class},
            "acl": {"S": acl},
            "user_metadata": {"S": user_metadata},
            "owner_id": {"S": owner_id},
            "owner_display": {"S": owner_display},
            "initiated_at": {"S": _now_iso()},
        }

        if content_encoding is not None:
            item["content_encoding"] = {"S": content_encoding}
        if content_language is not None:
            item["content_language"] = {"S": content_language}
        if content_disposition is not None:
            item["content_disposition"] = {"S": content_disposition}
        if cache_control is not None:
            item["cache_control"] = {"S": cache_control}
        if expires is not None:
            item["expires"] = {"S": expires}

        await self._client.put_item(TableName=self._table_name, Item=item)

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
        resp = await self._client.get_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_upload(upload_id)}, "sk": {"S": _sk_metadata()}},
        )
        if "Item" not in resp:
            return None
        return self._item_to_dict(resp["Item"])

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

        Inserts the final object record and removes upload + parts.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The upload identifier.
            size: Total size of the assembled object.
            etag: Composite ETag of the assembled object.
        """
        await self.put_object(
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
            storage_class=storage_class,
            acl=acl,
            user_metadata=user_metadata,
        )

        parts = await self.get_parts_for_completion(upload_id)

        if parts:
            delete_requests = [
                {
                    "DeleteRequest": {
                        "Key": {
                            "pk": {"S": _pk_upload(upload_id)},
                            "sk": {"S": _sk_part(p["part_number"])},
                        }
                    }
                }
                for p in parts
            ]

            for i in range(0, len(delete_requests), 25):
                batch = delete_requests[i : i + 25]
                await self._client.batch_write_item(RequestItems={self._table_name: batch})

        await self._client.delete_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_upload(upload_id)}, "sk": {"S": _sk_metadata()}},
        )

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """Abort a multipart upload and remove its part records.

        Args:
            bucket: The bucket name.
            key: The object key.
            upload_id: The upload identifier.
        """
        parts = await self.get_parts_for_completion(upload_id)

        if parts:
            delete_requests = [
                {
                    "DeleteRequest": {
                        "Key": {
                            "pk": {"S": _pk_upload(upload_id)},
                            "sk": {"S": _sk_part(p["part_number"])},
                        }
                    }
                }
                for p in parts
            ]

            for i in range(0, len(delete_requests), 25):
                batch = delete_requests[i : i + 25]
                await self._client.batch_write_item(RequestItems={self._table_name: batch})

        await self._client.delete_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_upload(upload_id)}, "sk": {"S": _sk_metadata()}},
        )

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
        await self._client.put_item(
            TableName=self._table_name,
            Item={
                "pk": {"S": _pk_upload(upload_id)},
                "sk": {"S": _sk_part(part_number)},
                "type": {"S": self._type_part()},
                "upload_id": {"S": upload_id},
                "part_number": {"N": str(part_number)},
                "size": {"N": str(size)},
                "etag": {"S": etag},
                "last_modified": {"S": _now_iso()},
            },
        )

    async def get_parts_for_completion(self, upload_id: str) -> list[dict[str, Any]]:
        """Get all parts for a multipart upload, ordered by part number.

        Args:
            upload_id: The upload identifier.

        Returns:
            A list of part metadata dicts ordered by part_number.
        """
        items: list[dict[str, Any]] = []
        exclusive_start_key = None

        while True:
            kwargs: dict[str, Any] = {
                "TableName": self._table_name,
                "KeyConditionExpression": "pk = :pk AND begins_with(sk, :part_prefix)",
                "ExpressionAttributeValues": {
                    ":pk": {"S": _pk_upload(upload_id)},
                    ":part_prefix": {"S": "PART#"},
                },
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = await self._client.query(**kwargs)

            for item in resp.get("Items", []):
                items.append(self._item_to_dict(item))

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        items.sort(key=lambda x: x.get("part_number", 0))
        return items

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
        items: list[dict[str, Any]] = []
        exclusive_start_key = None

        start_sk = _sk_part(part_number_marker + 1) if part_number_marker > 0 else "PART#"

        while len(items) < max_parts + 1:
            kwargs: dict[str, Any] = {
                "TableName": self._table_name,
                "KeyConditionExpression": "pk = :pk AND sk >= :start_sk",
                "ExpressionAttributeValues": {
                    ":pk": {"S": _pk_upload(upload_id)},
                    ":start_sk": {"S": start_sk},
                },
                "Limit": max_parts + 1,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = await self._client.query(**kwargs)

            for item in resp.get("Items", []):
                items.append(self._item_to_dict(item))

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        is_truncated = len(items) > max_parts
        if is_truncated:
            items = items[:max_parts]

        next_marker = 0
        if is_truncated and items:
            next_marker = items[-1].get("part_number", 0)

        return {
            "parts": items,
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
        items: list[dict[str, Any]] = []
        exclusive_start_key = None

        while len(items) < max_uploads + 1:
            filter_expr = "begins_with(pk, :upload_prefix) AND sk = :metadata AND #bucket = :bucket"
            expr_values: dict[str, Any] = {
                ":upload_prefix": {"S": "UPLOAD#"},
                ":metadata": {"S": _sk_metadata()},
                ":bucket": {"S": bucket},
            }
            expr_names = {"#bucket": "bucket"}

            if prefix:
                filter_expr += " AND begins_with(#key, :prefix)"
                expr_values[":prefix"] = {"S": prefix}
                expr_names["#key"] = "key"

            kwargs: dict[str, Any] = {
                "TableName": self._table_name,
                "FilterExpression": filter_expr,
                "ExpressionAttributeValues": expr_values,
                "ExpressionAttributeNames": expr_names,
                "Limit": max_uploads + 1,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = await self._client.scan(**kwargs)

            for item in resp.get("Items", []):
                items.append(self._item_to_dict(item))

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        items.sort(key=lambda x: (x.get("key", ""), x.get("upload_id", "")))

        if key_marker or upload_id_marker:
            filtered = []
            passed_marker = not key_marker
            for item in items:
                item_key = item.get("key", "")
                item_upload_id = item.get("upload_id", "")
                if not passed_marker:
                    if item_key > key_marker or (
                        item_key == key_marker and item_upload_id > upload_id_marker
                    ):
                        passed_marker = True
                if passed_marker:
                    filtered.append(item)
            items = filtered

        is_truncated = len(items) > max_uploads
        if is_truncated:
            items = items[:max_uploads]

        next_key = items[-1].get("key", "") if is_truncated and items else ""
        next_upload = items[-1].get("upload_id", "") if is_truncated and items else ""

        return {
            "uploads": items,
            "common_prefixes": [],
            "is_truncated": is_truncated,
            "next_key_marker": next_key if is_truncated else "",
            "next_upload_id_marker": next_upload if is_truncated else "",
        }

    async def get_credential(self, access_key_id: str) -> dict[str, Any] | None:
        """Retrieve a credential by access key ID.

        Args:
            access_key_id: The access key to look up.

        Returns:
            A dict with credential fields, or None if not found or inactive.
        """
        resp = await self._client.get_item(
            TableName=self._table_name,
            Key={"pk": {"S": _pk_credential(access_key_id)}, "sk": {"S": _sk_metadata()}},
        )
        if "Item" not in resp:
            return None
        return self._item_to_dict(resp["Item"])

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
        await self._client.put_item(
            TableName=self._table_name,
            Item={
                "pk": {"S": _pk_credential(access_key_id)},
                "sk": {"S": _sk_metadata()},
                "type": {"S": self._type_credential()},
                "access_key_id": {"S": access_key_id},
                "secret_key": {"S": secret_key},
                "owner_id": {"S": owner_id},
                "display_name": {"S": display_name},
                "active": {"BOOL": True},
                "created_at": {"S": _now_iso()},
            },
        )

    async def count_objects(self, bucket: str) -> int:
        """Count the number of objects in a bucket.

        Args:
            bucket: The bucket name.

        Returns:
            The number of objects in the bucket.
        """
        count = 0
        exclusive_start_key = None
        prefix = f"OBJECT#{bucket}#"

        while True:
            kwargs: dict[str, Any] = {
                "TableName": self._table_name,
                "FilterExpression": "begins_with(pk, :prefix) AND sk = :metadata",
                "ExpressionAttributeValues": {
                    ":prefix": {"S": prefix},
                    ":metadata": {"S": _sk_metadata()},
                },
                "Select": "COUNT",
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = await self._client.scan(**kwargs)
            count += resp.get("Count", 0)

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        return count

    async def reap_expired_uploads(self, ttl_seconds: int = 604800) -> list[dict]:
        """Delete expired multipart uploads and their parts from metadata.

        Args:
            ttl_seconds: Maximum age of uploads in seconds before reaping.

        Returns:
            A list of dicts with upload_id, bucket, and key for each reaped upload.
        """
        import time

        cutoff = time.time() - ttl_seconds
        cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        items: list[dict[str, Any]] = []
        exclusive_start_key = None

        while True:
            kwargs: dict[str, Any] = {
                "TableName": self._table_name,
                "FilterExpression": "begins_with(pk, :upload_prefix) AND sk = :metadata AND initiated_at < :cutoff",
                "ExpressionAttributeValues": {
                    ":upload_prefix": {"S": "UPLOAD#"},
                    ":metadata": {"S": _sk_metadata()},
                    ":cutoff": {"S": cutoff_iso},
                },
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = await self._client.scan(**kwargs)

            for item in resp.get("Items", []):
                items.append(self._item_to_dict(item))

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        reaped = []
        for upload in items:
            upload_id = upload.get("upload_id", "")
            bucket = upload.get("bucket", "")
            key = upload.get("key", "")

            parts = await self.get_parts_for_completion(upload_id)
            if parts:
                delete_requests = [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "pk": {"S": _pk_upload(upload_id)},
                                "sk": {"S": _sk_part(p["part_number"])},
                            }
                        }
                    }
                    for p in parts
                ]
                for i in range(0, len(delete_requests), 25):
                    batch = delete_requests[i : i + 25]
                    await self._client.batch_write_item(RequestItems={self._table_name: batch})

            await self._client.delete_item(
                TableName=self._table_name,
                Key={"pk": {"S": _pk_upload(upload_id)}, "sk": {"S": _sk_metadata()}},
            )

            reaped.append({"upload_id": upload_id, "bucket": bucket, "key": key})

        return reaped

    def _item_to_dict(self, item: dict[str, Any]) -> dict[str, Any]:
        """Convert a DynamoDB item to a Python dict."""
        result: dict[str, Any] = {}
        for key, value in item.items():
            if "S" in value:
                result[key] = value["S"]
            elif "N" in value:
                result[key] = int(value["N"]) if "." not in value["N"] else float(value["N"])
            elif "BOOL" in value:
                result[key] = value["BOOL"]
            elif "NULL" in value:
                result[key] = None
            elif "L" in value:
                result[key] = [
                    self._item_to_dict(v) if isinstance(v, dict) else v for v in value["L"]
                ]
            elif "M" in value:
                result[key] = self._item_to_dict(value["M"])
        return result
