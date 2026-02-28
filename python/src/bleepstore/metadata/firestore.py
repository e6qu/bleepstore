"""GCP Firestore metadata store backend for BleepStore.

Collection/document design:
- bucket_{name}              # Bucket metadata
- object_{bucket}_{key_b64}  # Object metadata (base64-encoded key)
- upload_{upload_id}         # Upload metadata
  └── parts/part_{number:05d}  # Part subcollection
- cred_{access_key}          # Credential
"""

import base64
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _encode_key(key: str) -> str:
    """URL-safe base64 encode a key for use in document IDs."""
    encoded = base64.urlsafe_b64encode(key.encode()).decode()
    return encoded.rstrip("=")


def _decode_key(encoded: str) -> str:
    """Decode a base64-encoded key."""
    padding = 4 - len(encoded) % 4
    if padding != 4:
        encoded += "=" * padding
    return base64.urlsafe_b64decode(encoded).decode()


def _doc_id_bucket(bucket: str) -> str:
    return f"bucket_{bucket}"


def _doc_id_object(bucket: str, key: str) -> str:
    return f"object_{bucket}_{_encode_key(key)}"


def _doc_id_upload(upload_id: str) -> str:
    return f"upload_{upload_id}"


def _doc_id_part(part_number: int) -> str:
    return f"part_{part_number:05d}"


def _doc_id_credential(access_key: str) -> str:
    return f"cred_{access_key}"


class FirestoreMetadataStore:
    """Firestore-backed metadata store.

    Implements the MetadataStore protocol using google-cloud-firestore AsyncClient.
    Uses a collection-based design with subcollections for multipart parts.
    """

    def __init__(self, config: Any) -> None:
        """Initialize the Firestore metadata store.

        Args:
            config: FirestoreConfig instance with collection, project, credentials_file.
        """
        self._config = config
        self._collection = config.collection
        self._project = config.project
        self._credentials_file = config.credentials_file
        self._client: Any = None

    async def init_db(self) -> None:
        """Initialize the Firestore client."""
        from google.cloud.firestore import AsyncClient

        kwargs: dict[str, Any] = {}
        if self._project:
            kwargs["project"] = self._project

        if self._credentials_file:
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_file(self._credentials_file)
            kwargs["credentials"] = creds

        self._client = AsyncClient(**kwargs)
        logger.info(
            "Connected to Firestore collection: %s, project: %s",
            self._collection,
            self._project or "default",
        )

    async def close(self) -> None:
        """Close the Firestore client."""
        if self._client:
            await self._client.close()
            self._client = None

    def _collection_ref(self):
        """Get the root collection reference."""
        return self._client.collection(self._collection)

    async def create_bucket(
        self,
        bucket: str,
        region: str = "us-east-1",
        owner_id: str = "",
        owner_display: str = "",
        acl: str = "{}",
    ) -> None:
        """Create a new bucket record."""
        doc_ref = self._collection_ref().document(_doc_id_bucket(bucket))
        await doc_ref.set(
            {
                "type": "bucket",
                "name": bucket,
                "region": region,
                "owner_id": owner_id,
                "owner_display": owner_display,
                "acl": acl,
                "created_at": _now_iso(),
            }
        )

    async def bucket_exists(self, bucket: str) -> bool:
        """Check whether a bucket exists."""
        doc_ref = self._collection_ref().document(_doc_id_bucket(bucket))
        doc = await doc_ref.get()
        return doc.exists

    async def delete_bucket(self, bucket: str) -> None:
        """Delete a bucket record."""
        doc_ref = self._collection_ref().document(_doc_id_bucket(bucket))
        await doc_ref.delete()

    async def get_bucket(self, bucket: str) -> dict[str, Any] | None:
        """Retrieve metadata for a single bucket."""
        doc_ref = self._collection_ref().document(_doc_id_bucket(bucket))
        doc = await doc_ref.get()
        if not doc.exists:
            return None
        return doc.to_dict()

    async def list_buckets(self, owner_id: str = "") -> list[dict[str, Any]]:
        """List all buckets, optionally filtered by owner."""
        query = self._collection_ref().where("type", "==", "bucket")
        if owner_id:
            query = query.where("owner_id", "==", owner_id)

        docs = query.stream()
        return [doc.to_dict() async for doc in docs]

    async def update_bucket_acl(self, bucket: str, acl: str) -> None:
        """Update the ACL on a bucket."""
        doc_ref = self._collection_ref().document(_doc_id_bucket(bucket))
        await doc_ref.update({"acl": acl})

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
        """Create or update an object metadata record (upsert)."""
        doc_ref = self._collection_ref().document(_doc_id_object(bucket, key))

        data: dict[str, Any] = {
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
            data["content_encoding"] = content_encoding
        if content_language is not None:
            data["content_language"] = content_language
        if content_disposition is not None:
            data["content_disposition"] = content_disposition
        if cache_control is not None:
            data["cache_control"] = cache_control
        if expires is not None:
            data["expires"] = expires

        await doc_ref.set(data)

    async def object_exists(self, bucket: str, key: str) -> bool:
        """Check whether an object exists."""
        doc_ref = self._collection_ref().document(_doc_id_object(bucket, key))
        doc = await doc_ref.get()
        return doc.exists

    async def get_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        """Retrieve metadata for a single object."""
        doc_ref = self._collection_ref().document(_doc_id_object(bucket, key))
        doc = await doc_ref.get()
        if not doc.exists:
            return None
        return doc.to_dict()

    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object metadata record."""
        doc_ref = self._collection_ref().document(_doc_id_object(bucket, key))
        await doc_ref.delete()

    async def delete_objects_meta(self, bucket: str, keys: list[str]) -> list[str]:
        """Delete multiple object metadata records in a batch."""
        if not keys:
            return []

        deleted = []
        batch = self._client.batch()

        for key in keys:
            doc_ref = self._collection_ref().document(_doc_id_object(bucket, key))
            batch.delete(doc_ref)
            deleted.append(key)

        await batch.commit()
        return deleted

    async def update_object_acl(self, bucket: str, key: str, acl: str) -> None:
        """Update the ACL on an object."""
        doc_ref = self._collection_ref().document(_doc_id_object(bucket, key))
        await doc_ref.update({"acl": acl})

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        marker: str = "",
        continuation_token: str | None = None,
    ) -> dict[str, Any]:
        """List objects in a bucket with optional filtering and pagination."""
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

        query = (
            self._collection_ref()
            .where("type", "==", "object")
            .where("bucket", "==", bucket)
            .order_by("key")
        )

        if start_after:
            query = query.start_after({"key": start_after})

        query = query.limit(max_keys + 1)

        docs = [doc.to_dict() async for doc in query.stream()]

        items = []
        for doc in docs:
            key = doc.get("key", "")
            if prefix and not key.startswith(prefix):
                continue
            items.append(doc)

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
        """Record a new multipart upload."""
        doc_ref = self._collection_ref().document(_doc_id_upload(upload_id))

        data: dict[str, Any] = {
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
            data["content_encoding"] = content_encoding
        if content_language is not None:
            data["content_language"] = content_language
        if content_disposition is not None:
            data["content_disposition"] = content_disposition
        if cache_control is not None:
            data["cache_control"] = cache_control
        if expires is not None:
            data["expires"] = expires

        await doc_ref.set(data)

    async def get_multipart_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> dict[str, Any] | None:
        """Retrieve metadata for a multipart upload."""
        doc_ref = self._collection_ref().document(_doc_id_upload(upload_id))
        doc = await doc_ref.get()
        if not doc.exists:
            return None
        return doc.to_dict()

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
        """Complete a multipart upload."""
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

        upload_ref = self._collection_ref().document(_doc_id_upload(upload_id))

        parts = await self.get_parts_for_completion(upload_id)

        batch = self._client.batch()
        for part in parts:
            part_ref = upload_ref.collection("parts").document(_doc_id_part(part["part_number"]))
            batch.delete(part_ref)

        batch.delete(upload_ref)
        await batch.commit()

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """Abort a multipart upload and remove its part records."""
        upload_ref = self._collection_ref().document(_doc_id_upload(upload_id))

        parts = await self.get_parts_for_completion(upload_id)

        batch = self._client.batch()
        for part in parts:
            part_ref = upload_ref.collection("parts").document(_doc_id_part(part["part_number"]))
            batch.delete(part_ref)

        batch.delete(upload_ref)
        await batch.commit()

    async def put_part(
        self,
        upload_id: str,
        part_number: int,
        size: int,
        etag: str,
    ) -> None:
        """Record an uploaded part (upsert by upload_id + part_number)."""
        upload_ref = self._collection_ref().document(_doc_id_upload(upload_id))
        part_ref = upload_ref.collection("parts").document(_doc_id_part(part_number))

        await part_ref.set(
            {
                "type": "part",
                "upload_id": upload_id,
                "part_number": part_number,
                "size": size,
                "etag": etag,
                "last_modified": _now_iso(),
            }
        )

    async def get_parts_for_completion(self, upload_id: str) -> list[dict[str, Any]]:
        """Get all parts for a multipart upload, ordered by part number."""
        upload_ref = self._collection_ref().document(_doc_id_upload(upload_id))
        parts_ref = upload_ref.collection("parts")

        query = parts_ref.order_by("part_number")
        docs = [doc.to_dict() async for doc in query.stream()]
        return docs

    async def list_parts(
        self,
        upload_id: str,
        part_number_marker: int = 0,
        max_parts: int = 1000,
    ) -> dict[str, Any]:
        """List parts for a multipart upload with pagination."""
        upload_ref = self._collection_ref().document(_doc_id_upload(upload_id))
        parts_ref = upload_ref.collection("parts")

        query = parts_ref.order_by("part_number")

        if part_number_marker > 0:
            query = query.start_after({"part_number": part_number_marker})

        query = query.limit(max_parts + 1)

        docs = [doc.to_dict() async for doc in query.stream()]

        is_truncated = len(docs) > max_parts
        if is_truncated:
            docs = docs[:max_parts]

        next_marker = 0
        if is_truncated and docs:
            next_marker = docs[-1].get("part_number", 0)

        return {
            "parts": docs,
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
        """List in-progress multipart uploads in a bucket."""
        query = self._collection_ref().where("type", "==", "upload").where("bucket", "==", bucket)

        if prefix:
            query = query.where("key", ">=", prefix).where("key", "<", prefix + "\uf8ff")

        query = query.order_by("key").order_by("upload_id")

        if key_marker or upload_id_marker:
            query = query.start_after({"key": key_marker, "upload_id": upload_id_marker or ""})

        query = query.limit(max_uploads + 1)

        docs = [doc.to_dict() async for doc in query.stream()]

        is_truncated = len(docs) > max_uploads
        if is_truncated:
            docs = docs[:max_uploads]

        next_key = docs[-1].get("key", "") if is_truncated and docs else ""
        next_upload = docs[-1].get("upload_id", "") if is_truncated and docs else ""

        return {
            "uploads": docs,
            "common_prefixes": [],
            "is_truncated": is_truncated,
            "next_key_marker": next_key,
            "next_upload_id_marker": next_upload,
        }

    async def get_credential(self, access_key_id: str) -> dict[str, Any] | None:
        """Retrieve a credential by access key ID."""
        doc_ref = self._collection_ref().document(_doc_id_credential(access_key_id))
        doc = await doc_ref.get()
        if not doc.exists:
            return None
        return doc.to_dict()

    async def put_credential(
        self,
        access_key_id: str,
        secret_key: str,
        owner_id: str = "",
        display_name: str = "",
    ) -> None:
        """Create or update a credential record."""
        doc_ref = self._collection_ref().document(_doc_id_credential(access_key_id))

        await doc_ref.set(
            {
                "type": "credential",
                "access_key_id": access_key_id,
                "secret_key": secret_key,
                "owner_id": owner_id,
                "display_name": display_name,
                "active": True,
                "created_at": _now_iso(),
            }
        )

    async def count_objects(self, bucket: str) -> int:
        """Count the number of objects in a bucket."""
        query = self._collection_ref().where("type", "==", "object").where("bucket", "==", bucket)

        count = 0
        async for _ in query.stream():
            count += 1

        return count

    async def reap_expired_uploads(self, ttl_seconds: int = 604800) -> list[dict]:
        """Delete expired multipart uploads and their parts from metadata."""
        import time

        cutoff = time.time() - ttl_seconds
        cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        query = (
            self._collection_ref()
            .where("type", "==", "upload")
            .where("initiated_at", "<", cutoff_iso)
        )

        reaped = []
        async for doc in query.stream():
            upload = doc.to_dict()
            upload_id = upload.get("upload_id", "")
            bucket = upload.get("bucket", "")
            key = upload.get("key", "")

            upload_ref = self._collection_ref().document(_doc_id_upload(upload_id))

            parts = await self.get_parts_for_completion(upload_id)

            batch = self._client.batch()
            for part in parts:
                part_ref = upload_ref.collection("parts").document(
                    _doc_id_part(part["part_number"])
                )
                batch.delete(part_ref)
            batch.delete(upload_ref)
            await batch.commit()

            reaped.append({"upload_id": upload_id, "bucket": bucket, "key": key})

        return reaped
