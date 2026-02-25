"""AWS S3 gateway storage backend for BleepStore.

Proxies all data operations to an upstream AWS S3 bucket via aiobotocore.
Metadata stays in local SQLite — this backend handles raw bytes only.

Key mapping:
    Objects:  {prefix}{bleepstore_bucket}/{key}
    Parts:    {prefix}.parts/{upload_id}/{part_number}

Credentials are resolved via the standard AWS credential chain
(env vars, ~/.aws/credentials, IAM role, etc.).
"""

import hashlib
import logging
from collections.abc import AsyncIterator

from aiobotocore.session import AioSession
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Streaming chunk size: 64 KB (matches local backend)
_CHUNK_SIZE = 64 * 1024


class AWSGatewayBackend:
    """Storage backend that proxies to a real AWS S3 bucket.

    All BleepStore buckets/objects are stored under a single upstream
    S3 bucket with a key prefix to namespace them.

    Attributes:
        bucket_name: The upstream AWS S3 bucket name.
        region: The AWS region for the bucket.
        prefix: Key prefix for all objects in the upstream bucket.
    """

    def __init__(
        self,
        bucket_name: str,
        region: str = "us-east-1",
        prefix: str = "",
        endpoint_url: str = "",
        use_path_style: bool = False,
        access_key_id: str = "",
        secret_access_key: str = "",
    ) -> None:
        self.bucket_name = bucket_name
        self.region = region
        self.prefix = prefix
        self.endpoint_url = endpoint_url
        self.use_path_style = use_path_style
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self._session = AioSession()
        self._client = None
        self._client_ctx = None

    def _s3_key(self, bucket: str, key: str) -> str:
        """Map a BleepStore bucket/key to an upstream S3 key."""
        return f"{self.prefix}{bucket}/{key}"

    def _part_key(self, upload_id: str, part_number: int) -> str:
        """Map a multipart part to an upstream S3 key."""
        return f"{self.prefix}.parts/{upload_id}/{part_number}"

    async def init(self) -> None:
        """Create the aiobotocore S3 client and verify the upstream bucket exists.

        Raises:
            ValueError: If the upstream bucket does not exist or is inaccessible.
        """
        # Build client kwargs from config
        client_kwargs: dict = {"region_name": self.region}
        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url
        if self.use_path_style:
            from botocore.config import Config as BotoConfig
            client_kwargs["config"] = BotoConfig(s3={"addressing_style": "path"})

        # Use explicit credentials if provided, otherwise fall back to chain
        if self.access_key_id and self.secret_access_key:
            session = AioSession()
            session.set_credentials(self.access_key_id, self.secret_access_key)
            self._session = session
        self._client_ctx = self._session.create_client("s3", **client_kwargs)
        self._client = await self._client_ctx.__aenter__()

        # Verify bucket exists
        try:
            await self._client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            await self._client_ctx.__aexit__(None, None, None)
            self._client = None
            self._client_ctx = None
            raise ValueError(
                f"Cannot access upstream S3 bucket '{self.bucket_name}': {code}"
            ) from e

        logger.info(
            "AWS gateway backend initialized: bucket=%s region=%s prefix='%s'",
            self.bucket_name,
            self.region,
            self.prefix,
        )

    async def close(self) -> None:
        """Close the aiobotocore client session."""
        if self._client_ctx is not None:
            await self._client_ctx.__aexit__(None, None, None)
            self._client = None
            self._client_ctx = None

    async def put(self, bucket: str, key: str, data: bytes) -> str:
        """Upload an object to the upstream S3 bucket.

        Computes MD5 locally for a consistent ETag (AWS may differ with SSE).

        Returns:
            The hex-encoded MD5 of the stored data.
        """
        s3_key = self._s3_key(bucket, key)
        md5 = hashlib.md5(data).hexdigest()

        await self._client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=data,
        )
        return md5

    async def put_stream(
        self,
        bucket: str,
        key: str,
        stream: AsyncIterator[bytes],
        content_length: int | None = None,
    ) -> tuple[str, int]:
        """Stream-write: collect stream and delegate to put()."""
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)
        data = b"".join(chunks)
        md5_hex = await self.put(bucket, key, data)
        return md5_hex, len(data)

    async def put_part_stream(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        stream: AsyncIterator[bytes],
        content_length: int | None = None,
    ) -> tuple[str, int]:
        """Stream-write part: collect stream and delegate to put_part()."""
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)
        data = b"".join(chunks)
        md5_hex = await self.put_part(bucket, key, upload_id, part_number, data)
        return md5_hex, len(data)

    async def get(self, bucket: str, key: str) -> bytes:
        """Download an object from the upstream S3 bucket.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        s3_key = self._s3_key(bucket, key)
        try:
            resp = await self._client.get_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"Object not found: {bucket}/{key}") from e
            raise

        async with resp["Body"] as stream:
            return await stream.read()

    async def get_stream(
        self, bucket: str, key: str, offset: int = 0, length: int | None = None
    ) -> AsyncIterator[bytes]:
        """Stream an object from the upstream S3 bucket in 64KB chunks.

        Raises:
            FileNotFoundError: If the object does not exist.
        """
        s3_key = self._s3_key(bucket, key)

        kwargs: dict = {"Bucket": self.bucket_name, "Key": s3_key}
        if offset > 0 or length is not None:
            if length is not None:
                end = offset + length - 1
                kwargs["Range"] = f"bytes={offset}-{end}"
            else:
                kwargs["Range"] = f"bytes={offset}-"

        try:
            resp = await self._client.get_object(**kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"Object not found: {bucket}/{key}") from e
            raise

        async with resp["Body"] as stream:
            while True:
                chunk = await stream.read(_CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

    async def delete(self, bucket: str, key: str) -> None:
        """Delete an object from the upstream S3 bucket.

        Idempotent — S3 delete_object does not error on missing keys.
        """
        s3_key = self._s3_key(bucket, key)
        await self._client.delete_object(Bucket=self.bucket_name, Key=s3_key)

    async def exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists in the upstream S3 bucket."""
        s3_key = self._s3_key(bucket, key)
        try:
            await self._client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey"):
                return False
            raise

    async def put_part(
        self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        """Store a multipart upload part as a temporary S3 object.

        Parts are stored at {prefix}.parts/{upload_id}/{part_number}.
        Computes MD5 locally for a consistent ETag.

        Returns:
            The hex-encoded MD5 of the part data.
        """
        part_key = self._part_key(upload_id, part_number)
        md5 = hashlib.md5(data).hexdigest()

        await self._client.put_object(
            Bucket=self.bucket_name,
            Key=part_key,
            Body=data,
        )
        return md5

    async def assemble_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_numbers: list[int],
    ) -> str:
        """Assemble uploaded parts into the final object using server-side copy.

        For a single part, uses copy_object directly. For multiple parts,
        creates a native AWS multipart upload and uses upload_part_copy
        for server-side assembly (no data download). Falls back to
        download + re-upload if upload_part_copy fails with EntityTooSmall.

        Returns:
            The hex-encoded MD5 of the assembled object (best-effort).
        """
        final_key = self._s3_key(bucket, key)

        if len(part_numbers) == 1:
            # Single part: direct copy
            part_key = self._part_key(upload_id, part_numbers[0])
            resp = await self._client.copy_object(
                Bucket=self.bucket_name,
                Key=final_key,
                CopySource={"Bucket": self.bucket_name, "Key": part_key},
            )
            etag = resp.get("CopyObjectResult", {}).get("ETag", "")
            return etag.strip('"')

        # Multiple parts: native AWS multipart upload with server-side copy
        aws_upload = await self._client.create_multipart_upload(
            Bucket=self.bucket_name, Key=final_key
        )
        aws_upload_id = aws_upload["UploadId"]

        try:
            parts_manifest = []
            for idx, pn in enumerate(part_numbers, 1):
                part_key = self._part_key(upload_id, pn)
                try:
                    copy_resp = await self._client.upload_part_copy(
                        Bucket=self.bucket_name,
                        Key=final_key,
                        UploadId=aws_upload_id,
                        PartNumber=idx,
                        CopySource={
                            "Bucket": self.bucket_name,
                            "Key": part_key,
                        },
                    )
                    etag = copy_resp["CopyPartResult"]["ETag"]
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code == "EntityTooSmall":
                        # Fallback: download part data and re-upload
                        get_resp = await self._client.get_object(
                            Bucket=self.bucket_name, Key=part_key
                        )
                        async with get_resp["Body"] as stream:
                            part_data = await stream.read()
                        upload_resp = await self._client.upload_part(
                            Bucket=self.bucket_name,
                            Key=final_key,
                            UploadId=aws_upload_id,
                            PartNumber=idx,
                            Body=part_data,
                        )
                        etag = upload_resp["ETag"]
                    else:
                        raise

                parts_manifest.append({"ETag": etag, "PartNumber": idx})

            resp = await self._client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=final_key,
                UploadId=aws_upload_id,
                MultipartUpload={"Parts": parts_manifest},
            )
            etag = resp.get("ETag", "")
            return etag.strip('"')

        except Exception:
            # Abort on any failure
            try:
                await self._client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=final_key,
                    UploadId=aws_upload_id,
                )
            except Exception:
                logger.warning("Failed to abort AWS multipart upload %s", aws_upload_id)
            raise

    async def delete_parts(self, bucket: str, key: str, upload_id: str) -> None:
        """Delete all temporary part objects for a multipart upload.

        Lists objects under .parts/{upload_id}/ and batch-deletes them
        (up to 1000 per request, matching S3 delete_objects limit).
        """
        prefix = f"{self.prefix}.parts/{upload_id}/"
        paginator = self._client.get_paginator("list_objects_v2")

        async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue

            # Batch delete (max 1000 per call)
            objects = [{"Key": obj["Key"]} for obj in contents]
            await self._client.delete_objects(
                Bucket=self.bucket_name,
                Delete={"Objects": objects, "Quiet": True},
            )

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> str:
        """Copy an object using AWS server-side copy.

        Returns:
            The hex-encoded MD5 ETag of the copied object (quotes stripped).
        """
        src_s3_key = self._s3_key(src_bucket, src_key)
        dst_s3_key = self._s3_key(dst_bucket, dst_key)

        resp = await self._client.copy_object(
            Bucket=self.bucket_name,
            Key=dst_s3_key,
            CopySource={"Bucket": self.bucket_name, "Key": src_s3_key},
        )
        etag = resp.get("CopyObjectResult", {}).get("ETag", "")
        return etag.strip('"')
