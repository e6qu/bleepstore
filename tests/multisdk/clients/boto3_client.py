"""boto3 low-level S3 client implementation."""

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


class Boto3Client:
    name = "boto3"

    def __init__(self):
        self._client = boto3.client(
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

    def create_bucket(self, bucket: str) -> None:
        self._client.create_bucket(Bucket=bucket)

    def delete_bucket(self, bucket: str) -> None:
        self._client.delete_bucket(Bucket=bucket)

    def head_bucket(self, bucket: str) -> int:
        try:
            self._client.head_bucket(Bucket=bucket)
            return 200
        except ClientError as e:
            return int(e.response["ResponseMetadata"]["HTTPStatusCode"])

    def list_buckets(self) -> list[str]:
        resp = self._client.list_buckets()
        return [b["Name"] for b in resp.get("Buckets", [])]

    def put_object(self, bucket: str, key: str, body: bytes) -> str:
        resp = self._client.put_object(Bucket=bucket, Key=key, Body=body)
        return resp.get("ETag", "")

    def get_object(self, bucket: str, key: str) -> bytes:
        try:
            resp = self._client.get_object(Bucket=bucket, Key=key)
            return resp["Body"].read()
        except ClientError as e:
            raise S3ClientError(
                str(e), int(e.response["ResponseMetadata"]["HTTPStatusCode"])
            )

    def head_object(self, bucket: str, key: str) -> dict:
        try:
            resp = self._client.head_object(Bucket=bucket, Key=key)
            return {
                "size": resp["ContentLength"],
                "etag": resp.get("ETag", ""),
                "content_type": resp.get("ContentType", ""),
            }
        except ClientError as e:
            raise S3ClientError(
                str(e), int(e.response["ResponseMetadata"]["HTTPStatusCode"])
            )

    def delete_object(self, bucket: str, key: str) -> None:
        self._client.delete_object(Bucket=bucket, Key=key)

    def delete_objects(self, bucket: str, keys: list[str]) -> None:
        self._client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in keys]},
        )

    def copy_object(self, bucket: str, src_key: str, dst_key: str) -> None:
        self._client.copy_object(
            Bucket=bucket, Key=dst_key, CopySource=f"{bucket}/{src_key}"
        )

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> dict:
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
        self._client.upload_file(path, bucket, key)

    def download_file(self, bucket: str, key: str, path: str) -> None:
        self._client.download_file(bucket, key, path)
