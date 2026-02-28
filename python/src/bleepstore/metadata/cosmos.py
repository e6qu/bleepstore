"""Azure Cosmos DB metadata store backend for BleepStore.

Single-container design with partition key /type:
- Bucket:     id=bucket_{name}, type=bucket
- Object:     id=object_{bucket}_{key}, type=object
- Upload:     id=upload_{upload_id}, type=upload
- Part:       id=part_{upload_id}_{number:05d}, type=upload (same partition)
- Credential: id=cred_{access_key}, type=credential
"""

import logging
from datetime import datetime, timezone
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _doc_id_bucket(bucket: str) -> str:
    return f"bucket_{bucket}"


def _doc_id_object(bucket: str, key: str) -> str:
    return f"object_{bucket}_{key}"


def _doc_id_upload(upload_id: str) -> str:
    return f"upload_{upload_id}"


def _doc_id_part(upload_id: str, part_number: int) -> str:
    return f"part_{upload_id}_{part_number:05d}"


def _doc_id_credential(access_key: str) -> str:
    return f"cred_{access_key}"


class CosmosMetadataStore:
    """Cosmos DB-backed metadata store.

    Implements the MetadataStore protocol using azure-cosmos async client.
    Uses a single container with partition key /type.
    """

    def __init__(self, config: Any) -> None:
        self._config = config
        self._database_name = config.database
        self._container_name = config.container
        self._endpoint = config.endpoint
        self._connection_string = config.connection_string
        self._client: CosmosClient | None = None
        self._container: Any = None

    async def init_db(self) -> None:
        if self._connection_string:
            self._client = CosmosClient.from_connection_string(self._connection_string)
        elif self._endpoint:
            self._client = CosmosClient(self._endpoint, credential=self._get_credential())
        else:
            raise ValueError("Either endpoint or connection_string must be provided")

        database = self._client.get_database_client(self._database_name)
        try:
            self._container = database.get_container_client(self._container_name)
            await self._container.read()
        except ResourceNotFoundError:
            self._container = await database.create_container(
                id=self._container_name,
                partition_key=PartitionKey(path="/type"),
            )

        logger.info(
            "Connected to Cosmos DB: database=%s, container=%s",
            self._database_name,
            self._container_name,
        )

    def _get_credential(self) -> Any:
        if hasattr(self._config, "credential") and self._config.credential:
            return self._config.credential
        from azure.identity import DefaultAzureCredential

        return DefaultAzureCredential()

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
            self._container = None

    async def create_bucket(
        self,
        bucket: str,
        region: str = "us-east-1",
        owner_id: str = "",
        owner_display: str = "",
        acl: str = "{}",
    ) -> None:
        doc = {
            "id": _doc_id_bucket(bucket),
            "type": "bucket",
            "name": bucket,
            "region": region,
            "owner_id": owner_id,
            "owner_display": owner_display,
            "acl": acl,
            "created_at": _now_iso(),
        }
        await self._container.create_item(doc)

    async def bucket_exists(self, bucket: str) -> bool:
        try:
            await self._container.read_item(_doc_id_bucket(bucket), partition_key="bucket")
            return True
        except ResourceNotFoundError:
            return False

    async def delete_bucket(self, bucket: str) -> None:
        try:
            await self._container.delete_item(_doc_id_bucket(bucket), partition_key="bucket")
        except ResourceNotFoundError:
            pass

    async def get_bucket(self, bucket: str) -> dict[str, Any] | None:
        try:
            doc = await self._container.read_item(_doc_id_bucket(bucket), partition_key="bucket")
            return dict(doc)
        except ResourceNotFoundError:
            return None

    async def list_buckets(self, owner_id: str = "") -> list[dict[str, Any]]:
        query = "SELECT * FROM c WHERE c.type = 'bucket'"
        params: list[dict[str, Any]] = []
        if owner_id:
            query += " AND c.owner_id = @owner_id"
            params.append({"name": "@owner_id", "value": owner_id})

        results = []
        async for doc in self._container.query_items(query=query, parameters=params):
            results.append(dict(doc))
        return results

    async def update_bucket_acl(self, bucket: str, acl: str) -> None:
        doc = await self._container.read_item(_doc_id_bucket(bucket), partition_key="bucket")
        doc["acl"] = acl
        await self._container.replace_item(doc["id"], doc)

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
        doc: dict[str, Any] = {
            "id": _doc_id_object(bucket, key),
            "type": "object",
            "bucket": bucket,
            "key": key,
            "size": size,
            "etag": etag,
            "content_type": content_type,
            "storage_class": storage_class,
            "acl": acl,
            "user_metadata": user_metadata,
            "last_modified": _now_iso(),
        }
        if content_encoding is not None:
            doc["content_encoding"] = content_encoding
        if content_language is not None:
            doc["content_language"] = content_language
        if content_disposition is not None:
            doc["content_disposition"] = content_disposition
        if cache_control is not None:
            doc["cache_control"] = cache_control
        if expires is not None:
            doc["expires"] = expires

        await self._container.upsert_item(doc)

    async def object_exists(self, bucket: str, key: str) -> bool:
        try:
            await self._container.read_item(_doc_id_object(bucket, key), partition_key="object")
            return True
        except ResourceNotFoundError:
            return False

    async def get_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        try:
            doc = await self._container.read_item(
                _doc_id_object(bucket, key), partition_key="object"
            )
            return dict(doc)
        except ResourceNotFoundError:
            return None

    async def delete_object(self, bucket: str, key: str) -> None:
        try:
            await self._container.delete_item(_doc_id_object(bucket, key), partition_key="object")
        except ResourceNotFoundError:
            pass

    async def delete_objects_meta(self, bucket: str, keys: list[str]) -> list[str]:
        deleted = []
        for key in keys:
            try:
                await self._container.delete_item(
                    _doc_id_object(bucket, key), partition_key="object"
                )
                deleted.append(key)
            except ResourceNotFoundError:
                pass
        return deleted

    async def update_object_acl(self, bucket: str, key: str, acl: str) -> None:
        doc = await self._container.read_item(_doc_id_object(bucket, key), partition_key="object")
        doc["acl"] = acl
        await self._container.replace_item(doc["id"], doc)

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        marker: str = "",
        continuation_token: str | None = None,
    ) -> dict[str, Any]:
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
        prefix_filter = f"object_{bucket}_{prefix}"

        query = "SELECT * FROM c WHERE c.type = 'object' AND c.bucket = @bucket"
        params: list[dict[str, Any]] = [{"name": "@bucket", "value": bucket}]

        if prefix:
            query += " AND STARTSWITH(c.id, @prefix)"
            params.append({"name": "@prefix", "value": prefix_filter})
        if start_after:
            query += " AND c.id > @start_after"
            params.append({"name": "@start_after", "value": _doc_id_object(bucket, start_after)})

        query += " ORDER BY c.id"

        items: list[dict[str, Any]] = []
        async for doc in self._container.query_items(
            query=query, parameters=params, max_item_count=max_keys + 1
        ):
            items.append(dict(doc))
            if len(items) > max_keys:
                break

        if delimiter:
            return self._apply_delimiter(items, delimiter, max_keys)

        is_truncated = len(items) > max_keys
        if is_truncated:
            items = items[:max_keys]

        last_key = items[-1].get("key", "") if items else ""

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
        doc: dict[str, Any] = {
            "id": _doc_id_upload(upload_id),
            "type": "upload",
            "upload_id": upload_id,
            "bucket": bucket,
            "key": key,
            "content_type": content_type,
            "storage_class": storage_class,
            "acl": acl,
            "user_metadata": user_metadata,
            "owner_id": owner_id,
            "owner_display": owner_display,
            "initiated_at": _now_iso(),
        }
        if content_encoding is not None:
            doc["content_encoding"] = content_encoding
        if content_language is not None:
            doc["content_language"] = content_language
        if content_disposition is not None:
            doc["content_disposition"] = content_disposition
        if cache_control is not None:
            doc["cache_control"] = cache_control
        if expires is not None:
            doc["expires"] = expires

        await self._container.create_item(doc)

    async def get_multipart_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> dict[str, Any] | None:
        try:
            doc = await self._container.read_item(_doc_id_upload(upload_id), partition_key="upload")
            return dict(doc)
        except ResourceNotFoundError:
            return None

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
        for part in parts:
            try:
                await self._container.delete_item(part["id"], partition_key="upload")
            except ResourceNotFoundError:
                pass

        try:
            await self._container.delete_item(_doc_id_upload(upload_id), partition_key="upload")
        except ResourceNotFoundError:
            pass

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        parts = await self.get_parts_for_completion(upload_id)
        for part in parts:
            try:
                await self._container.delete_item(part["id"], partition_key="upload")
            except ResourceNotFoundError:
                pass

        try:
            await self._container.delete_item(_doc_id_upload(upload_id), partition_key="upload")
        except ResourceNotFoundError:
            pass

    async def put_part(
        self,
        upload_id: str,
        part_number: int,
        size: int,
        etag: str,
    ) -> None:
        doc = {
            "id": _doc_id_part(upload_id, part_number),
            "type": "upload",
            "upload_id": upload_id,
            "part_number": part_number,
            "size": size,
            "etag": etag,
            "last_modified": _now_iso(),
        }
        await self._container.upsert_item(doc)

    async def get_parts_for_completion(self, upload_id: str) -> list[dict[str, Any]]:
        query = "SELECT * FROM c WHERE c.type = 'upload' AND STARTSWITH(c.id, @prefix)"
        params = [{"name": "@prefix", "value": f"part_{upload_id}_"}]

        items: list[dict[str, Any]] = []
        async for doc in self._container.query_items(query=query, parameters=params):
            items.append(dict(doc))

        items.sort(key=lambda x: x.get("part_number", 0))
        return items

    async def list_parts(
        self,
        upload_id: str,
        part_number_marker: int = 0,
        max_parts: int = 1000,
    ) -> dict[str, Any]:
        query = "SELECT * FROM c WHERE c.type = 'upload' AND STARTSWITH(c.id, @prefix)"
        params: list[dict[str, Any]] = [{"name": "@prefix", "value": f"part_{upload_id}_"}]

        if part_number_marker > 0:
            query += " AND c.id > @start_after"
            params.append(
                {"name": "@start_after", "value": _doc_id_part(upload_id, part_number_marker)}
            )

        query += " ORDER BY c.id"

        items: list[dict[str, Any]] = []
        async for doc in self._container.query_items(
            query=query, parameters=params, max_item_count=max_parts + 1
        ):
            items.append(dict(doc))
            if len(items) > max_parts:
                break

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
        query = "SELECT * FROM c WHERE c.type = 'upload' AND c.bucket = @bucket AND c.upload_id IS NOT NULL"
        params: list[dict[str, Any]] = [{"name": "@bucket", "value": bucket}]

        if prefix:
            query += " AND STARTSWITH(c.key, @prefix)"
            params.append({"name": "@prefix", "value": prefix})

        if key_marker:
            query += " AND (c.key > @key_marker OR (c.key = @key_marker AND c.upload_id > @upload_id_marker))"
            params.append({"name": "@key_marker", "value": key_marker})
            params.append({"name": "@upload_id_marker", "value": upload_id_marker})

        query += " ORDER BY c.key, c.upload_id"

        items: list[dict[str, Any]] = []
        async for doc in self._container.query_items(
            query=query, parameters=params, max_item_count=max_uploads + 1
        ):
            items.append(dict(doc))
            if len(items) > max_uploads:
                break

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
        try:
            doc = await self._container.read_item(
                _doc_id_credential(access_key_id), partition_key="credential"
            )
            if not doc.get("active", True):
                return None
            return dict(doc)
        except ResourceNotFoundError:
            return None

    async def put_credential(
        self,
        access_key_id: str,
        secret_key: str,
        owner_id: str = "",
        display_name: str = "",
    ) -> None:
        doc = {
            "id": _doc_id_credential(access_key_id),
            "type": "credential",
            "access_key_id": access_key_id,
            "secret_key": secret_key,
            "owner_id": owner_id,
            "display_name": display_name,
            "active": True,
            "created_at": _now_iso(),
        }
        await self._container.upsert_item(doc)

    async def count_objects(self, bucket: str) -> int:
        query = "SELECT VALUE COUNT(1) FROM c WHERE c.type = 'object' AND c.bucket = @bucket"
        params = [{"name": "@bucket", "value": bucket}]

        count = 0
        async for result in self._container.query_items(query=query, parameters=params):
            count = result
            break
        return count

    async def reap_expired_uploads(self, ttl_seconds: int = 604800) -> list[dict]:
        import time

        cutoff = time.time() - ttl_seconds
        cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        query = "SELECT * FROM c WHERE c.type = 'upload' AND c.upload_id IS NOT NULL AND c.initiated_at < @cutoff"
        params = [{"name": "@cutoff", "value": cutoff_iso}]

        items: list[dict[str, Any]] = []
        async for doc in self._container.query_items(query=query, parameters=params):
            items.append(dict(doc))

        reaped = []
        for upload in items:
            upload_id = upload.get("upload_id", "")
            bucket = upload.get("bucket", "")
            key = upload.get("key", "")

            parts = await self.get_parts_for_completion(upload_id)
            for part in parts:
                try:
                    await self._container.delete_item(part["id"], partition_key="upload")
                except ResourceNotFoundError:
                    pass

            try:
                await self._container.delete_item(upload["id"], partition_key="upload")
            except ResourceNotFoundError:
                pass

            reaped.append({"upload_id": upload_id, "bucket": bucket, "key": key})

        return reaped
