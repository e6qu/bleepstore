"""In-memory metadata store for BleepStore.

Useful for testing and ephemeral deployments. Data is lost on restart.
"""

from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class MemoryMetadataStore:
    """In-memory metadata store using Python dicts.

    No persistence - all data is lost on restart. Suitable for testing
    or short-lived ephemeral deployments.
    """

    def __init__(self) -> None:
        self._buckets: dict[str, dict[str, Any]] = {}
        self._objects: dict[tuple[str, str], dict[str, Any]] = {}
        self._uploads: dict[str, dict[str, Any]] = {}
        self._parts: dict[str, dict[int, dict[str, Any]]] = {}
        self._credentials: dict[str, dict[str, Any]] = {}

    async def init_db(self) -> None:
        pass

    async def close(self) -> None:
        self._buckets.clear()
        self._objects.clear()
        self._uploads.clear()
        self._parts.clear()
        self._credentials.clear()

    async def create_bucket(
        self,
        bucket: str,
        region: str = "us-east-1",
        owner_id: str = "",
        owner_display: str = "",
        acl: str = "{}",
    ) -> None:
        if bucket in self._buckets:
            raise KeyError(f"Bucket already exists: {bucket}")
        self._buckets[bucket] = {
            "name": bucket,
            "region": region,
            "owner_id": owner_id,
            "owner_display": owner_display,
            "acl": acl,
            "created_at": _now_iso(),
        }

    async def bucket_exists(self, bucket: str) -> bool:
        return bucket in self._buckets

    async def delete_bucket(self, bucket: str) -> None:
        self._buckets.pop(bucket, None)
        to_delete = [k for k in self._objects if k[0] == bucket]
        for k in to_delete:
            del self._objects[k]

    async def get_bucket(self, bucket: str) -> dict[str, Any] | None:
        return self._buckets.get(bucket)

    async def list_buckets(self, owner_id: str = "") -> list[dict[str, Any]]:
        buckets = list(self._buckets.values())
        if owner_id:
            buckets = [b for b in buckets if b["owner_id"] == owner_id]
        return sorted(buckets, key=lambda b: b["name"])

    async def update_bucket_acl(self, bucket: str, acl: str) -> None:
        if bucket in self._buckets:
            self._buckets[bucket]["acl"] = acl

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
        self._objects[(bucket, key)] = {
            "bucket": bucket,
            "key": key,
            "size": size,
            "etag": etag,
            "content_type": content_type,
            "content_encoding": content_encoding,
            "content_language": content_language,
            "content_disposition": content_disposition,
            "cache_control": cache_control,
            "expires": expires,
            "storage_class": storage_class,
            "acl": acl,
            "user_metadata": user_metadata,
            "last_modified": _now_iso(),
            "delete_marker": 0,
        }

    async def object_exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self._objects

    async def get_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        return self._objects.get((bucket, key))

    async def delete_object(self, bucket: str, key: str) -> None:
        self._objects.pop((bucket, key), None)

    async def delete_objects_meta(self, bucket: str, keys: list[str]) -> list[str]:
        deleted = []
        for key in keys:
            if (bucket, key) in self._objects:
                del self._objects[(bucket, key)]
                deleted.append(key)
        return deleted

    async def update_object_acl(self, bucket: str, key: str, acl: str) -> None:
        obj = self._objects.get((bucket, key))
        if obj:
            obj["acl"] = acl

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

        objects = [obj for (b, _), obj in self._objects.items() if b == bucket]
        objects = sorted(objects, key=lambda o: o["key"])

        if prefix:
            objects = [o for o in objects if o["key"].startswith(prefix)]

        if start_after:
            objects = [o for o in objects if o["key"] > start_after]

        contents: list[dict[str, Any]] = []
        common_prefixes: list[str] = []
        seen_prefixes: set[str] = set()

        for obj in objects:
            if len(contents) + len(common_prefixes) >= max_keys:
                break

            key: str = obj["key"]

            if delimiter:
                suffix = key[len(prefix) :]
                delim_pos = suffix.find(delimiter)
                if delim_pos >= 0:
                    cp = prefix + suffix[: delim_pos + len(delimiter)]
                    if cp not in seen_prefixes:
                        seen_prefixes.add(cp)
                        common_prefixes.append(cp)
                    continue

            contents.append(obj)

        total_returned = len(contents) + len(common_prefixes)
        is_truncated = total_returned >= max_keys and len(objects) > total_returned

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
        self._uploads[upload_id] = {
            "upload_id": upload_id,
            "bucket": bucket,
            "key": key,
            "content_type": content_type,
            "content_encoding": content_encoding,
            "content_language": content_language,
            "content_disposition": content_disposition,
            "cache_control": cache_control,
            "expires": expires,
            "storage_class": storage_class,
            "acl": acl,
            "user_metadata": user_metadata,
            "owner_id": owner_id,
            "owner_display": owner_display,
            "initiated_at": _now_iso(),
        }
        if upload_id not in self._parts:
            self._parts[upload_id] = {}

    async def get_multipart_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> dict[str, Any] | None:
        upload = self._uploads.get(upload_id)
        if upload and upload["bucket"] == bucket and upload["key"] == key:
            return upload
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
        self._parts.pop(upload_id, None)
        self._uploads.pop(upload_id, None)

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        self._parts.pop(upload_id, None)
        self._uploads.pop(upload_id, None)

    async def put_part(
        self,
        upload_id: str,
        part_number: int,
        size: int,
        etag: str,
    ) -> None:
        if upload_id not in self._parts:
            self._parts[upload_id] = {}
        self._parts[upload_id][part_number] = {
            "upload_id": upload_id,
            "part_number": part_number,
            "size": size,
            "etag": etag,
            "last_modified": _now_iso(),
        }

    async def get_parts_for_completion(self, upload_id: str) -> list[dict[str, Any]]:
        parts = self._parts.get(upload_id, {})
        return sorted(parts.values(), key=lambda p: p["part_number"])

    async def list_parts(
        self,
        upload_id: str,
        part_number_marker: int = 0,
        max_parts: int = 1000,
    ) -> dict[str, Any]:
        all_parts = self._parts.get(upload_id, {})
        parts = [p for p in all_parts.values() if p["part_number"] > part_number_marker]
        parts = sorted(parts, key=lambda p: p["part_number"])[: max_parts + 1]

        result_parts = parts[:max_parts]
        is_truncated = len(parts) > max_parts
        next_marker = result_parts[-1]["part_number"] if is_truncated and result_parts else None

        return {
            "parts": result_parts,
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
        uploads = [u for u in self._uploads.values() if u["bucket"] == bucket]

        if prefix:
            uploads = [u for u in uploads if u["key"].startswith(prefix)]

        if key_marker:
            if upload_id_marker:
                uploads = [
                    u
                    for u in uploads
                    if u["key"] > key_marker
                    or (u["key"] == key_marker and u["upload_id"] > upload_id_marker)
                ]
            else:
                uploads = [u for u in uploads if u["key"] > key_marker]

        uploads = sorted(uploads, key=lambda u: (u["key"], u["initiated_at"]))

        result_uploads: list[dict[str, Any]] = []
        common_prefixes: list[str] = []
        seen_prefixes: set[str] = set()

        for upload in uploads:
            if len(result_uploads) + len(common_prefixes) >= max_uploads:
                break

            key = upload["key"]

            if delimiter:
                suffix = key[len(prefix) :]
                delim_pos = suffix.find(delimiter)
                if delim_pos >= 0:
                    cp = prefix + suffix[: delim_pos + len(delimiter)]
                    if cp not in seen_prefixes:
                        seen_prefixes.add(cp)
                        common_prefixes.append(cp)
                    continue

            result_uploads.append(upload)

        total = len(result_uploads) + len(common_prefixes)
        is_truncated = len(uploads) > total and total >= max_uploads

        next_key_marker: str | None = None
        next_upload_id_marker: str | None = None
        if is_truncated and result_uploads:
            last = result_uploads[-1]
            next_key_marker = last["key"]
            next_upload_id_marker = last["upload_id"]

        return {
            "uploads": result_uploads,
            "common_prefixes": sorted(common_prefixes),
            "is_truncated": is_truncated,
            "next_key_marker": next_key_marker,
            "next_upload_id_marker": next_upload_id_marker,
        }

    async def get_credential(self, access_key_id: str) -> dict[str, Any] | None:
        cred = self._credentials.get(access_key_id)
        if cred and cred.get("active", 1) == 1:
            return cred
        return None

    async def put_credential(
        self,
        access_key_id: str,
        secret_key: str,
        owner_id: str = "",
        display_name: str = "",
    ) -> None:
        self._credentials[access_key_id] = {
            "access_key_id": access_key_id,
            "secret_key": secret_key,
            "owner_id": owner_id,
            "display_name": display_name,
            "active": 1,
            "created_at": _now_iso(),
        }

    async def count_objects(self, bucket: str) -> int:
        return sum(1 for (b, _) in self._objects if b == bucket)

    async def reap_expired_uploads(self, ttl_seconds: int = 604800) -> list[dict]:
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds)
        cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        reaped = []
        for upload_id, upload in list(self._uploads.items()):
            if upload["initiated_at"] < cutoff_str:
                reaped.append(
                    {
                        "upload_id": upload_id,
                        "bucket": upload["bucket"],
                        "key": upload["key"],
                    }
                )
                self._parts.pop(upload_id, None)
                del self._uploads[upload_id]

        return reaped
