"""AWS CLI v2 S3 client implementation."""

from __future__ import annotations

import json
import os
import tempfile

from .base import CliS3Client, S3ClientError

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


class AwsCliClient(CliS3Client):
    name = "awscli"

    def __init__(self):
        self._base = [
            "aws",
            "--endpoint-url",
            ENDPOINT,
            "--region",
            REGION,
            "--no-cli-pager",
        ]
        self._env = {
            **os.environ,
            "AWS_ACCESS_KEY_ID": ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
            "AWS_DEFAULT_REGION": REGION,
        }

    def _run(self, *args, input_data=None, check=True):
        import subprocess

        return subprocess.run(
            args,
            input=input_data,
            capture_output=True,
            timeout=self.timeout,
            check=check,
            env=self._env,
        )

    def _api(self, *args) -> dict:
        """Run 'aws s3api' command and return parsed JSON."""
        result = self._run(*self._base, "s3api", *args, check=False)
        if result.returncode != 0:
            stderr = result.stderr.decode()
            raise S3ClientError(stderr, result.returncode)
        stdout = result.stdout.decode().strip()
        if not stdout:
            return {}
        return json.loads(stdout)

    def _s3(self, *args, check=True):
        """Run 'aws s3' command."""
        return self._run(*self._base, "s3", *args, check=check)

    def create_bucket(self, bucket: str) -> None:
        self._api("create-bucket", "--bucket", bucket)

    def delete_bucket(self, bucket: str) -> None:
        self._api("delete-bucket", "--bucket", bucket)

    def head_bucket(self, bucket: str) -> int:
        try:
            self._api("head-bucket", "--bucket", bucket)
            return 200
        except S3ClientError:
            return 404

    def list_buckets(self) -> list[str]:
        resp = self._api("list-buckets")
        return [b["Name"] for b in resp.get("Buckets", [])]

    def put_object(self, bucket: str, key: str, body: bytes) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(body)
            f.flush()
            try:
                resp = self._api(
                    "put-object", "--bucket", bucket, "--key", key, "--body", f.name
                )
                return resp.get("ETag", "")
            finally:
                os.unlink(f.name)

    def get_object(self, bucket: str, key: str) -> bytes:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            outpath = f.name
        try:
            self._api("get-object", "--bucket", bucket, "--key", key, outpath)
            with open(outpath, "rb") as f:
                return f.read()
        except S3ClientError:
            raise
        finally:
            if os.path.exists(outpath):
                os.unlink(outpath)

    def head_object(self, bucket: str, key: str) -> dict:
        try:
            resp = self._api("head-object", "--bucket", bucket, "--key", key)
            return {
                "size": resp.get("ContentLength", 0),
                "etag": resp.get("ETag", ""),
                "content_type": resp.get("ContentType", ""),
            }
        except S3ClientError:
            raise

    def delete_object(self, bucket: str, key: str) -> None:
        self._api("delete-object", "--bucket", bucket, "--key", key)

    def delete_objects(self, bucket: str, keys: list[str]) -> None:
        delete_spec = json.dumps({"Objects": [{"Key": k} for k in keys]})
        self._api("delete-objects", "--bucket", bucket, "--delete", delete_spec)

    def copy_object(self, bucket: str, src_key: str, dst_key: str) -> None:
        self._api(
            "copy-object",
            "--bucket",
            bucket,
            "--key",
            dst_key,
            "--copy-source",
            f"{bucket}/{src_key}",
        )

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> dict:
        args = ["list-objects-v2", "--bucket", bucket, "--max-items", str(max_keys)]
        if prefix:
            args += ["--prefix", prefix]
        if delimiter:
            args += ["--delimiter", delimiter]
        resp = self._api(*args)
        return {
            "keys": [obj["Key"] for obj in resp.get("Contents", [])],
            "prefixes": [p["Prefix"] for p in resp.get("CommonPrefixes", [])],
        }

    def upload_file(self, bucket: str, key: str, path: str) -> None:
        self._s3("cp", path, f"s3://{bucket}/{key}")

    def download_file(self, bucket: str, key: str, path: str) -> None:
        self._s3("cp", f"s3://{bucket}/{key}", path)
