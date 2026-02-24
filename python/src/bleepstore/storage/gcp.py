"""Google Cloud Storage gateway backend for BleepStore.

Proxies all data operations to an upstream GCS bucket via gcloud-aio-storage.
Metadata stays in local SQLite — this backend handles raw bytes only.

Key mapping:
    Objects:  {prefix}{bleepstore_bucket}/{key}
    Parts:    {prefix}.parts/{upload_id}/{part_number}

Credentials are resolved via GCS Application Default Credentials
(GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, metadata server).
"""

import hashlib
import logging
from collections.abc import AsyncIterator

from gcloud.aio.storage import Storage

logger = logging.getLogger(__name__)

# Streaming chunk size: 64 KB (matches local and AWS backends)
_CHUNK_SIZE = 64 * 1024

# GCS compose() supports at most 32 source objects per call.
_MAX_COMPOSE_SOURCES = 32


class GCPGatewayBackend:
    """Storage backend that proxies to a Google Cloud Storage bucket.

    All BleepStore buckets/objects are stored under a single upstream
    GCS bucket with a key prefix to namespace them.

    Attributes:
        bucket_name: The upstream GCS bucket name.
        project: The GCP project ID.
        prefix: Key prefix for all objects in the upstream bucket.
    """

    def __init__(
        self,
        bucket_name: str,
        project: str = "",
        prefix: str = "",
    ) -> None:
        self.bucket_name = bucket_name
        self.project = project
        self.prefix = prefix
        self._client: Storage | None = None

    def _gcs_name(self, bucket: str, key: str) -> str:
        """Map a BleepStore bucket/key to an upstream GCS object name."""
        return f"{self.prefix}{bucket}/{key}"

    def _part_name(self, upload_id: str, part_number: int) -> str:
        """Map a multipart part to an upstream GCS object name."""
        return f"{self.prefix}.parts/{upload_id}/{part_number}"

    async def init(self) -> None:
        """Create the gcloud-aio-storage client and verify the upstream bucket exists.

        Raises:
            ValueError: If the upstream bucket does not exist or is inaccessible.
        """
        self._client = Storage()

        # Verify bucket exists by attempting to list with maxResults=1
        try:
            await self._client.list_objects(
                self.bucket_name,
                params={"maxResults": "1"},
            )
        except Exception as e:
            await self._client.close()
            self._client = None
            raise ValueError(
                f"Cannot access upstream GCS bucket '{self.bucket_name}': {e}"
            ) from e

        logger.info(
            "GCP gateway backend initialized: bucket=%s project=%s prefix='%s'",
            self.bucket_name,
            self.project,
            self.prefix,
        )

    async def close(self) -> None:
        """Close the gcloud-aio-storage client session."""
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def put(self, bucket: str, key: str, data: bytes) -> str:
        """Upload an object to the upstream GCS bucket.

        Computes MD5 locally for a consistent ETag.

        Returns:
            The hex-encoded MD5 of the stored data.
        """
        gcs_name = self._gcs_name(bucket, key)
        md5 = hashlib.md5(data).hexdigest()

        await self._client.upload(
            self.bucket_name,
            gcs_name,
            data,
        )
        return md5

    async def get(self, bucket: str, key: str) -> bytes:
        """Download an object from the upstream GCS bucket.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        gcs_name = self._gcs_name(bucket, key)
        try:
            return await self._client.download(self.bucket_name, gcs_name)
        except Exception as e:
            if _is_not_found(e):
                raise FileNotFoundError(
                    f"Object not found: {bucket}/{key}"
                ) from e
            raise

    async def get_stream(
        self, bucket: str, key: str, offset: int = 0, length: int | None = None
    ) -> AsyncIterator[bytes]:
        """Stream an object from the upstream GCS bucket in 64KB chunks.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        gcs_name = self._gcs_name(bucket, key)

        headers: dict[str, str] = {}
        if offset > 0 or length is not None:
            if length is not None:
                end = offset + length - 1
                headers["Range"] = f"bytes={offset}-{end}"
            else:
                headers["Range"] = f"bytes={offset}-"

        try:
            stream_resp = await self._client.download_stream(
                self.bucket_name,
                gcs_name,
                headers=headers if headers else None,
            )
        except Exception as e:
            if _is_not_found(e):
                raise FileNotFoundError(
                    f"Object not found: {bucket}/{key}"
                ) from e
            raise

        while True:
            chunk = await stream_resp.read(_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

    async def delete(self, bucket: str, key: str) -> None:
        """Delete an object from the upstream GCS bucket.

        Idempotent — catches 404 silently (GCS errors on delete of
        non-existent objects unlike S3).
        """
        gcs_name = self._gcs_name(bucket, key)
        try:
            await self._client.delete(self.bucket_name, gcs_name)
        except Exception as e:
            if _is_not_found(e):
                return  # Idempotent: treat as success
            raise

    async def exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists in the upstream GCS bucket.

        Uses a Range: bytes=0-0 download to avoid fetching the full object.
        """
        gcs_name = self._gcs_name(bucket, key)
        try:
            await self._client.download(
                self.bucket_name,
                gcs_name,
                headers={"Range": "bytes=0-0"},
            )
            return True
        except Exception as e:
            if _is_not_found(e):
                return False
            raise

    async def put_part(
        self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        """Store a multipart upload part as a temporary GCS object.

        Parts are stored at {prefix}.parts/{upload_id}/{part_number}.
        Computes MD5 locally for a consistent ETag.

        Returns:
            The hex-encoded MD5 of the part data.
        """
        part_name = self._part_name(upload_id, part_number)
        md5 = hashlib.md5(data).hexdigest()

        await self._client.upload(
            self.bucket_name,
            part_name,
            data,
        )
        return md5

    async def assemble_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_numbers: list[int],
    ) -> str:
        """Assemble uploaded parts into the final object using GCS compose.

        GCS compose() supports at most 32 source objects per call. For >32
        parts, chains compose in batches of 32: compose each batch into an
        intermediate object, then compose the intermediates, repeating until
        a single object remains.

        Returns:
            The hex-encoded MD5 of the assembled object (best-effort).
        """
        final_name = self._gcs_name(bucket, key)
        source_names = [self._part_name(upload_id, pn) for pn in part_numbers]

        if len(source_names) <= _MAX_COMPOSE_SOURCES:
            # Simple case: single compose call
            await self._client.compose(
                self.bucket_name, final_name, source_names
            )
        else:
            # Chain compose in batches of 32
            intermediates = await self._chain_compose(source_names, final_name)
            # Clean up intermediate composite objects
            for name in intermediates:
                try:
                    await self._client.delete(self.bucket_name, name)
                except Exception:
                    logger.warning("Failed to clean up intermediate: %s", name)

        # Compute MD5 of the final assembled object
        data = await self._client.download(self.bucket_name, final_name)
        return hashlib.md5(data).hexdigest()

    async def _chain_compose(
        self, source_names: list[str], final_name: str
    ) -> list[str]:
        """Chain GCS compose calls for >32 sources.

        Returns a list of intermediate object names that should be cleaned up.
        """
        all_intermediates: list[str] = []
        current_sources = source_names

        generation = 0
        while len(current_sources) > _MAX_COMPOSE_SOURCES:
            next_sources: list[str] = []
            for i in range(0, len(current_sources), _MAX_COMPOSE_SOURCES):
                batch = current_sources[i : i + _MAX_COMPOSE_SOURCES]
                if len(batch) == 1:
                    # Single source — no compose needed, just pass through
                    next_sources.append(batch[0])
                    continue
                intermediate_name = f"{final_name}.__compose_tmp_{generation}_{i}"
                await self._client.compose(
                    self.bucket_name, intermediate_name, batch
                )
                next_sources.append(intermediate_name)
                all_intermediates.append(intermediate_name)
            current_sources = next_sources
            generation += 1

        # Final compose
        await self._client.compose(
            self.bucket_name, final_name, current_sources
        )
        return all_intermediates

    async def delete_parts(self, bucket: str, key: str, upload_id: str) -> None:
        """Delete all temporary part objects for a multipart upload.

        Lists objects under .parts/{upload_id}/ and deletes each one.
        """
        prefix = f"{self.prefix}.parts/{upload_id}/"

        resp = await self._client.list_objects(
            self.bucket_name,
            params={"prefix": prefix},
        )

        items = resp.get("items", [])
        for item in items:
            obj_name = item["name"]
            try:
                await self._client.delete(self.bucket_name, obj_name)
            except Exception as e:
                if not _is_not_found(e):
                    raise

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> str:
        """Copy an object using GCS server-side copy.

        Returns:
            The hex-encoded MD5 ETag of the copied object.
        """
        src_gcs_name = self._gcs_name(src_bucket, src_key)
        dst_gcs_name = self._gcs_name(dst_bucket, dst_key)

        await self._client.copy(
            self.bucket_name,
            src_gcs_name,
            self.bucket_name,
            new_name=dst_gcs_name,
        )

        # Compute MD5 by downloading the result
        data = await self._client.download(self.bucket_name, dst_gcs_name)
        return hashlib.md5(data).hexdigest()


def _is_not_found(exc: Exception) -> bool:
    """Check if an exception represents a 404 Not Found from GCS."""
    # gcloud-aio-storage raises aiohttp.ClientResponseError for HTTP errors
    status = getattr(exc, "status", None)
    if status == 404:
        return True
    # Also check for wrapped exceptions
    if hasattr(exc, "args") and exc.args:
        msg = str(exc.args[0]).lower()
        if "404" in msg or "not found" in msg:
            return True
    return False
