"""boto3 high-level S3 resource client implementation."""

from __future__ import annotations

import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .base import S3ClientError

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


class Boto3ResourceClient:
    name = "boto3-resource"

    def __init__(self):
        self._resource = boto3.resource(
            "s3",
            endpoint_url=ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name=REGION,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 1, "mode": "standard"},
            ),
        )
        # Keep a low-level client for operations the resource API doesn't cover
        self._client = self._resource.meta.client

    def create_bucket(self, bucket: str) -> None:
        self._resource.create_bucket(Bucket=bucket)

    def delete_bucket(self, bucket: str) -> None:
        self._resource.Bucket(bucket).delete()

    def head_bucket(self, bucket: str) -> int:
        try:
            self._client.head_bucket(Bucket=bucket)
            return 200
        except ClientError as e:
            return int(e.response["ResponseMetadata"]["HTTPStatusCode"])

    def list_buckets(self) -> list[str]:
        return [b.name for b in self._resource.buckets.all()]

    def put_object(self, bucket: str, key: str, body: bytes) -> str:
        obj = self._resource.Bucket(bucket).put_object(Key=key, Body=body)
        return obj.e_tag or ""

    def get_object(self, bucket: str, key: str) -> bytes:
        try:
            obj = self._resource.Object(bucket, key)
            return obj.get()["Body"].read()
        except ClientError as e:
            raise S3ClientError(
                str(e), int(e.response["ResponseMetadata"]["HTTPStatusCode"])
            )

    def head_object(self, bucket: str, key: str) -> dict:
        try:
            obj = self._resource.Object(bucket, key)
            obj.load()
            return {
                "size": obj.content_length,
                "etag": obj.e_tag or "",
                "content_type": obj.content_type or "",
            }
        except ClientError as e:
            raise S3ClientError(
                str(e), int(e.response["ResponseMetadata"]["HTTPStatusCode"])
            )

    def delete_object(self, bucket: str, key: str) -> None:
        self._resource.Object(bucket, key).delete()

    def delete_objects(self, bucket: str, keys: list[str]) -> None:
        self._resource.Bucket(bucket).delete_objects(
            Delete={"Objects": [{"Key": k} for k in keys]}
        )

    def copy_object(self, bucket: str, src_key: str, dst_key: str) -> None:
        self._resource.Object(bucket, dst_key).copy_from(
            CopySource=f"{bucket}/{src_key}"
        )

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> dict:
        # Resource API doesn't expose CommonPrefixes cleanly, use low-level client
        kwargs: dict = {"Bucket": bucket, "MaxKeys": max_keys}
        if prefix:
            kwargs["Prefix"] = prefix
        if delimiter:
            kwargs["Delimiter"] = delimiter
        resp = self._client.list_objects_v2(**kwargs)
        return {
            "keys": [obj["Key"] for obj in resp.get("Contents", [])],
            "prefixes": [
                p["Prefix"] for p in resp.get("CommonPrefixes", [])
            ],
        }

    def upload_file(self, bucket: str, key: str, path: str) -> None:
        self._resource.Bucket(bucket).upload_file(path, key)

    def download_file(self, bucket: str, key: str, path: str) -> None:
        self._resource.Bucket(bucket).download_file(key, path)
