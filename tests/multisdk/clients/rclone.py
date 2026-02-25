"""rclone S3 client implementation."""

from __future__ import annotations

import json
import os
import tempfile
from urllib.parse import urlparse

from .base import CliS3Client, S3ClientError

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")
REGION = os.environ.get("BLEEPSTORE_REGION", "us-east-1")


class RcloneClient(CliS3Client):
    name = "rclone"

    def __init__(self):
        # Configure rclone via env vars (no config file mutation)
        parsed = urlparse(ENDPOINT)
        self._env = {
            **os.environ,
            "RCLONE_CONFIG_BLEEP_TYPE": "s3",
            "RCLONE_CONFIG_BLEEP_PROVIDER": "Other",
            "RCLONE_CONFIG_BLEEP_ACCESS_KEY_ID": ACCESS_KEY,
            "RCLONE_CONFIG_BLEEP_SECRET_ACCESS_KEY": SECRET_KEY,
            "RCLONE_CONFIG_BLEEP_ENDPOINT": f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname,
            "RCLONE_CONFIG_BLEEP_FORCE_PATH_STYLE": "true",
            "RCLONE_CONFIG_BLEEP_REGION": REGION,
            # Use http if endpoint is http
            **({"RCLONE_CONFIG_BLEEP_USE_SSL": "false"} if parsed.scheme == "http" else {}),
        }
        self._remote = "bleep:"

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

    def _rclone(self, *args, check=True) -> str:
        result = self._run("rclone", *args, check=False)
        if result.returncode != 0 and check:
            raise S3ClientError(result.stderr.decode(), result.returncode)
        return result.stdout.decode()

    def _rclone_json(self, *args) -> list[dict]:
        result = self._run("rclone", *args, check=False)
        stdout = result.stdout.decode().strip()
        if result.returncode != 0:
            raise S3ClientError(result.stderr.decode(), result.returncode)
        if not stdout or stdout == "null":
            return []
        return json.loads(stdout)

    def _path(self, bucket: str, key: str = "") -> str:
        if key:
            return f"{self._remote}{bucket}/{key}"
        return f"{self._remote}{bucket}"

    def create_bucket(self, bucket: str) -> None:
        self._rclone("mkdir", self._path(bucket))

    def delete_bucket(self, bucket: str) -> None:
        self._rclone("rmdir", self._path(bucket))

    def head_bucket(self, bucket: str) -> int:
        # rclone lsd lists buckets, check if our bucket is in the list
        try:
            output = self._rclone("lsd", self._remote)
            for line in output.splitlines():
                if bucket in line:
                    return 200
            return 404
        except S3ClientError:
            return 404

    def list_buckets(self) -> list[str]:
        entries = self._rclone_json("lsjson", self._remote)
        return [e["Name"] for e in entries if e.get("IsDir", False)]

    def put_object(self, bucket: str, key: str, body: bytes) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(body)
            f.flush()
            try:
                self._rclone("copyto", f.name, self._path(bucket, key))
                # Get etag via lsjson
                entries = self._rclone_json("lsjson", self._path(bucket, key))
                if entries:
                    hashes = entries[0].get("Hashes", {})
                    return hashes.get("MD5", "")
                return ""
            finally:
                os.unlink(f.name)

    def get_object(self, bucket: str, key: str) -> bytes:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            outpath = f.name
        try:
            self._rclone("copyto", self._path(bucket, key), outpath)
            with open(outpath, "rb") as f:
                return f.read()
        except S3ClientError:
            raise
        finally:
            if os.path.exists(outpath):
                os.unlink(outpath)

    def head_object(self, bucket: str, key: str) -> dict:
        entries = self._rclone_json("lsjson", self._path(bucket, key))
        if not entries:
            raise S3ClientError("not found", 404)
        entry = entries[0]
        return {
            "size": entry.get("Size", 0),
            "etag": entry.get("Hashes", {}).get("MD5", ""),
            "content_type": entry.get("MimeType", ""),
        }

    def delete_object(self, bucket: str, key: str) -> None:
        self._rclone("deletefile", self._path(bucket, key))

    def delete_objects(self, bucket: str, keys: list[str]) -> None:
        for key in keys:
            self._rclone("deletefile", self._path(bucket, key), check=False)

    def copy_object(self, bucket: str, src_key: str, dst_key: str) -> None:
        self._rclone("copyto", self._path(bucket, src_key), self._path(bucket, dst_key))

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> dict:
        target = self._path(bucket, prefix) if prefix else self._path(bucket)
        args = ["lsjson"]
        if not delimiter:
            args.append("--recursive")
        args.append(target)
        entries = self._rclone_json(*args)

        keys = []
        prefixes_list = []
        for entry in entries:
            name = entry.get("Name", "") or entry.get("Path", "")
            if entry.get("IsDir", False):
                if delimiter:
                    p = prefix + name if prefix else name
                    if not p.endswith("/"):
                        p += "/"
                    prefixes_list.append(p)
            else:
                full_key = prefix + name if prefix else name
                keys.append(full_key)

        return {"keys": keys[:max_keys], "prefixes": prefixes_list}

    def upload_file(self, bucket: str, key: str, path: str) -> None:
        self._rclone("copyto", path, self._path(bucket, key))

    def download_file(self, bucket: str, key: str, path: str) -> None:
        self._rclone("copyto", self._path(bucket, key), path)
