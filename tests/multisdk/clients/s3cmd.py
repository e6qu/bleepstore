"""s3cmd S3 client implementation."""

from __future__ import annotations

import os
import re
import tempfile

from .base import CliS3Client, S3ClientError

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


def _endpoint_host_port() -> tuple[str, str]:
    """Extract host:port from endpoint URL."""
    # http://localhost:9013 -> localhost:9013
    from urllib.parse import urlparse

    parsed = urlparse(ENDPOINT)
    host = parsed.hostname or "localhost"
    port = parsed.port
    if port:
        return f"{host}:{port}", "http" if parsed.scheme == "http" else "https"
    return host, parsed.scheme or "http"


class S3CmdClient(CliS3Client):
    name = "s3cmd"

    def __init__(self):
        host, scheme = _endpoint_host_port()
        self._base = [
            "s3cmd",
            f"--access_key={ACCESS_KEY}",
            f"--secret_key={SECRET_KEY}",
            f"--host={host}",
            "--host-bucket=",  # empty = path-style
            f"--region={REGION}",
            "--signature-v4",
            "--no-ssl" if scheme == "http" else "",
        ]
        # Remove empty strings
        self._base = [a for a in self._base if a]

    def _s3cmd(self, *args, check=True) -> str:
        result = self._run(*self._base, *args, check=False)
        if result.returncode != 0 and check:
            raise S3ClientError(result.stderr.decode(), result.returncode)
        return result.stdout.decode()

    def create_bucket(self, bucket: str) -> None:
        self._s3cmd("mb", f"s3://{bucket}")

    def delete_bucket(self, bucket: str) -> None:
        self._s3cmd("rb", f"s3://{bucket}")

    def head_bucket(self, bucket: str) -> int:
        try:
            self._s3cmd("info", f"s3://{bucket}")
            return 200
        except S3ClientError:
            return 404

    def list_buckets(self) -> list[str]:
        output = self._s3cmd("ls")
        buckets = []
        for line in output.splitlines():
            # Format: "2024-01-01 00:00  s3://bucket-name"
            match = re.search(r"s3://(\S+)", line)
            if match:
                buckets.append(match.group(1))
        return buckets

    def put_object(self, bucket: str, key: str, body: bytes) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(body)
            f.flush()
            try:
                self._s3cmd("put", f.name, f"s3://{bucket}/{key}")
                # s3cmd doesn't return etag on put, get via info
                try:
                    info = self._s3cmd("info", f"s3://{bucket}/{key}")
                    match = re.search(r"ETag:\s+(\S+)", info)
                    return match.group(1) if match else ""
                except S3ClientError:
                    return ""
            finally:
                os.unlink(f.name)

    def get_object(self, bucket: str, key: str) -> bytes:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            outpath = f.name
        try:
            self._s3cmd("get", f"s3://{bucket}/{key}", outpath, "--force")
            with open(outpath, "rb") as f:
                return f.read()
        except S3ClientError:
            raise
        finally:
            if os.path.exists(outpath):
                os.unlink(outpath)

    def head_object(self, bucket: str, key: str) -> dict:
        try:
            info = self._s3cmd("info", f"s3://{bucket}/{key}")
            size = 0
            etag = ""
            content_type = ""
            for line in info.splitlines():
                line = line.strip()
                if line.startswith("File size:"):
                    match = re.search(r"(\d+)", line)
                    if match:
                        size = int(match.group(1))
                elif line.startswith("ETag:"):
                    etag = line.split(":", 1)[1].strip()
                elif line.startswith("MIME type:"):
                    content_type = line.split(":", 1)[1].strip()
            return {"size": size, "etag": etag, "content_type": content_type}
        except S3ClientError:
            raise

    def delete_object(self, bucket: str, key: str) -> None:
        self._s3cmd("del", f"s3://{bucket}/{key}")

    def delete_objects(self, bucket: str, keys: list[str]) -> None:
        for key in keys:
            self._s3cmd("del", f"s3://{bucket}/{key}", check=False)

    def copy_object(self, bucket: str, src_key: str, dst_key: str) -> None:
        self._s3cmd("cp", f"s3://{bucket}/{src_key}", f"s3://{bucket}/{dst_key}")

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> dict:
        target = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
        args = ["ls"]
        if not delimiter:
            args.append("--recursive")
        args.append(target)
        output = self._s3cmd(*args)

        keys = []
        prefixes = []
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue
            # s3cmd ls format:
            # "                       DIR  s3://bucket/prefix/"
            # "2024-01-01 00:00     1234  s3://bucket/key"
            match = re.search(r"s3://\S+/(.+)", line)
            if not match:
                continue
            name = match.group(1)
            if "DIR" in line:
                if delimiter:
                    prefixes.append(name if name.endswith("/") else name + "/")
            else:
                keys.append(name)

        return {"keys": keys[:max_keys], "prefixes": prefixes}

    def upload_file(self, bucket: str, key: str, path: str) -> None:
        self._s3cmd("put", path, f"s3://{bucket}/{key}")

    def download_file(self, bucket: str, key: str, path: str) -> None:
        self._s3cmd("get", f"s3://{bucket}/{key}", path, "--force")
