"""Local JSONL-backed metadata store for BleepStore.

Stores metadata entities as JSONL (JSON Lines) files. Supports tombstone-based
deletion and compaction. Uses file locking for concurrent write safety.
"""

import fcntl
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class LocalMetadataConfig(BaseModel):
    """Configuration for local JSONL metadata backend."""

    root_dir: str = "./data/metadata"
    compact_on_startup: bool = True


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class LocalMetadataStore:
    """Metadata store backed by local JSONL files.

    Each entity type is stored in its own JSONL file:
    - buckets.jsonl
    - objects.jsonl
    - uploads.jsonl
    - parts.jsonl
    - credentials.jsonl

    Deletions use tombstones with `_deleted: true`. Compaction rewrites
    files removing tombstones.
    """

    def __init__(self, config: LocalMetadataConfig | dict[str, Any]) -> None:
        if isinstance(config, dict):
            config = LocalMetadataConfig(**config)
        self._root_dir = Path(config.root_dir)
        self._compact_on_startup = config.compact_on_startup
        self._buckets: dict[str, dict[str, Any]] = {}
        self._objects: dict[tuple[str, str], dict[str, Any]] = {}
        self._uploads: dict[str, dict[str, Any]] = {}
        self._parts: dict[str, dict[int, dict[str, Any]]] = {}
        self._credentials: dict[str, dict[str, Any]] = {}
        self._initialized = False

    async def init_db(self) -> None:
        self._root_dir.mkdir(parents=True, exist_ok=True)
        if self._compact_on_startup:
            self._compact_all()
        self._load_all()
        self._initialized = True

    async def close(self) -> None:
        self._buckets.clear()
        self._objects.clear()
        self._uploads.clear()
        self._parts.clear()
        self._credentials.clear()
        self._initialized = False

    def _file_path(self, name: str) -> Path:
        return self._root_dir / f"{name}.jsonl"

    def _read_jsonl(self, name: str) -> list[dict[str, Any]]:
        path = self._file_path(name)
        if not path.exists():
            return []
        records = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning("Invalid JSON in %s: %s", path, line[:50])
        return records

    def _write_jsonl(self, name: str, records: list[dict[str, Any]]) -> None:
        path = self._file_path(name)
        temp_path = path.with_suffix(".tmp")
        with open(temp_path, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        os.replace(temp_path, path)

    def _append_jsonl(self, name: str, record: dict[str, Any]) -> None:
        path = self._file_path(name)
        temp_path = path.with_suffix(".tmp")
        with open(path, "a") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.write(json.dumps(record) + "\n")
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def _load_all(self) -> None:
        self._load_buckets()
        self._load_objects()
        self._load_uploads()
        self._load_parts()
        self._load_credentials()

    def _load_buckets(self) -> None:
        records = self._read_jsonl("buckets")
        latest: dict[str, dict[str, Any]] = {}
        for rec in records:
            name = rec.get("name")
            if not name:
                continue
            if rec.get("_deleted"):
                latest.pop(name, None)
            else:
                latest[name] = rec
        self._buckets = latest

    def _load_objects(self) -> None:
        records = self._read_jsonl("objects")
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for rec in records:
            bucket = rec.get("bucket")
            key = rec.get("key")
            if not bucket or not key:
                continue
            pk = (bucket, key)
            if rec.get("_deleted"):
                latest.pop(pk, None)
            else:
                latest[pk] = rec
        self._objects = latest

    def _load_uploads(self) -> None:
        records = self._read_jsonl("uploads")
        latest: dict[str, dict[str, Any]] = {}
        for rec in records:
            upload_id = rec.get("upload_id")
            if not upload_id:
                continue
            if rec.get("_deleted"):
                latest.pop(upload_id, None)
            else:
                latest[upload_id] = rec
        self._uploads = latest

    def _load_parts(self) -> None:
        records = self._read_jsonl("parts")
        latest: dict[str, dict[int, dict[str, Any]]] = {}
        for rec in records:
            upload_id = rec.get("upload_id")
            part_number = rec.get("part_number")
            if not upload_id or part_number is None:
                continue
            if upload_id not in latest:
                latest[upload_id] = {}
            key = (upload_id, part_number)
            if rec.get("_deleted"):
                latest[upload_id].pop(part_number, None)
            else:
                latest[upload_id][part_number] = rec
        self._parts = latest

    def _load_credentials(self) -> None:
        records = self._read_jsonl("credentials")
        latest: dict[str, dict[str, Any]] = {}
        for rec in records:
            access_key_id = rec.get("access_key_id")
            if not access_key_id:
                continue
            if rec.get("_deleted"):
                latest.pop(access_key_id, None)
            else:
                latest[access_key_id] = rec
        self._credentials = latest

    def _compact_all(self) -> None:
        self._compact_file("buckets", "name")
        self._compact_file("objects", lambda r: (r.get("bucket"), r.get("key")))
        self._compact_file("uploads", "upload_id")
        self._compact_file("parts", lambda r: (r.get("upload_id"), r.get("part_number")))
        self._compact_file("credentials", "access_key_id")

    def _compact_file(self, name: str, key_func: Any) -> None:
        records = self._read_jsonl(name)
        if not records:
            return
        latest: dict[Any, dict[str, Any]] = {}
        for rec in records:
            try:
                key = key_func(rec) if callable(key_func) else rec.get(key_func)
            except Exception:
                continue
            if key is None:
                continue
            if rec.get("_deleted"):
                latest.pop(key, None)
            else:
                latest[key] = rec
        if latest:
            self._write_jsonl(name, list(latest.values()))
        else:
            path = self._file_path(name)
            if path.exists():
                path.unlink()

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
        rec = {
            "name": bucket,
            "region": region,
            "owner_id": owner_id,
            "owner_display": owner_display,
            "acl": acl,
            "created_at": _now_iso(),
        }
        self._buckets[bucket] = rec
        self._append_jsonl("buckets", rec)

    async def bucket_exists(self, bucket: str) -> bool:
        return bucket in self._buckets

    async def delete_bucket(self, bucket: str) -> None:
        if bucket in self._buckets:
            del self._buckets[bucket]
            self._append_jsonl("buckets", {"name": bucket, "_deleted": True})
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
            rec = self._buckets[bucket]
            rec["acl"] = acl
            self._append_jsonl("buckets", rec)

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
        rec = {
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
        self._objects[(bucket, key)] = rec
        self._append_jsonl("objects", rec)

    async def object_exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self._objects

    async def get_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        return self._objects.get((bucket, key))

    async def delete_object(self, bucket: str, key: str) -> None:
        pk = (bucket, key)
        if pk in self._objects:
            del self._objects[pk]
            self._append_jsonl("objects", {"bucket": bucket, "key": key, "_deleted": True})

    async def delete_objects_meta(self, bucket: str, keys: list[str]) -> list[str]:
        deleted = []
        for key in keys:
            pk = (bucket, key)
            if pk in self._objects:
                del self._objects[pk]
                self._append_jsonl("objects", {"bucket": bucket, "key": key, "_deleted": True})
                deleted.append(key)
        return deleted

    async def update_object_acl(self, bucket: str, key: str, acl: str) -> None:
        pk = (bucket, key)
        obj = self._objects.get(pk)
        if obj:
            obj["acl"] = acl
            self._append_jsonl("objects", obj)

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
        rec = {
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
        self._uploads[upload_id] = rec
        if upload_id not in self._parts:
            self._parts[upload_id] = {}
        self._append_jsonl("uploads", rec)

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

        parts = self._parts.pop(upload_id, {})
        for part_num in parts:
            self._append_jsonl(
                "parts",
                {
                    "upload_id": upload_id,
                    "part_number": part_num,
                    "_deleted": True,
                },
            )

        self._uploads.pop(upload_id, None)
        self._append_jsonl("uploads", {"upload_id": upload_id, "_deleted": True})

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        parts = self._parts.pop(upload_id, {})
        for part_num in parts:
            self._append_jsonl(
                "parts",
                {
                    "upload_id": upload_id,
                    "part_number": part_num,
                    "_deleted": True,
                },
            )
        self._uploads.pop(upload_id, None)
        self._append_jsonl("uploads", {"upload_id": upload_id, "_deleted": True})

    async def put_part(
        self,
        upload_id: str,
        part_number: int,
        size: int,
        etag: str,
    ) -> None:
        if upload_id not in self._parts:
            self._parts[upload_id] = {}
        rec = {
            "upload_id": upload_id,
            "part_number": part_number,
            "size": size,
            "etag": etag,
            "last_modified": _now_iso(),
        }
        self._parts[upload_id][part_number] = rec
        self._append_jsonl("parts", rec)

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
        rec = {
            "access_key_id": access_key_id,
            "secret_key": secret_key,
            "owner_id": owner_id,
            "display_name": display_name,
            "active": 1,
            "created_at": _now_iso(),
        }
        self._credentials[access_key_id] = rec
        self._append_jsonl("credentials", rec)

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
                parts = self._parts.pop(upload_id, {})
                for part_num in parts:
                    self._append_jsonl(
                        "parts",
                        {
                            "upload_id": upload_id,
                            "part_number": part_num,
                            "_deleted": True,
                        },
                    )
                del self._uploads[upload_id]
                self._append_jsonl("uploads", {"upload_id": upload_id, "_deleted": True})

        return reaped
