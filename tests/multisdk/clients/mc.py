"""MinIO mc S3 client implementation."""

from __future__ import annotations

import json
import os
import tempfile
import uuid

from .base import CliS3Client, S3ClientError

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("BLEEPSTORE_ACCESS_KEY", "bleepstore")
SECRET_KEY = os.environ.get("BLEEPSTORE_SECRET_KEY", "bleepstore-secret")


class McClient(CliS3Client):
    name = "mc"

    def __init__(self):
        self._alias = f"bleep-{uuid.uuid4().hex[:8]}"
        # Configure mc alias for this session
        self._run(
            "mc",
            "alias",
            "set",
            self._alias,
            ENDPOINT,
            ACCESS_KEY,
            SECRET_KEY,
            "--api",
            "s3v4",
        )

    def _mc(self, *args, check=True) -> str:
        result = self._run("mc", *args, check=False)
        if result.returncode != 0 and check:
            raise S3ClientError(result.stderr.decode(), result.returncode)
        return result.stdout.decode()

    def _mc_json(self, *args) -> list[dict]:
        """Run mc with --json and parse output (mc outputs one JSON object per line)."""
        result = self._run("mc", *args, "--json", check=False)
        stdout = result.stdout.decode().strip()
        if not stdout:
            return []
        lines = []
        for line in stdout.splitlines():
            line = line.strip()
            if line:
                obj = json.loads(line)
                if obj.get("status") == "error":
                    raise S3ClientError(obj.get("error", {}).get("message", "unknown error"))
                lines.append(obj)
        return lines

    def _path(self, bucket: str, key: str = "") -> str:
        if key:
            return f"{self._alias}/{bucket}/{key}"
        return f"{self._alias}/{bucket}"

    def create_bucket(self, bucket: str) -> None:
        self._mc("mb", self._path(bucket))

    def delete_bucket(self, bucket: str) -> None:
        self._mc("rb", self._path(bucket))

    def head_bucket(self, bucket: str) -> int:
        try:
            self._mc("stat", self._path(bucket))
            return 200
        except S3ClientError:
            return 404

    def list_buckets(self) -> list[str]:
        lines = self._mc_json("ls", f"{self._alias}/")
        return [
            entry["key"].rstrip("/")
            for entry in lines
            if entry.get("type") == "folder" or entry.get("key", "").endswith("/")
        ]

    def put_object(self, bucket: str, key: str, body: bytes) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(body)
            f.flush()
            try:
                self._mc("cp", f.name, self._path(bucket, key))
                # mc doesn't return etag on put, get it via stat
                lines = self._mc_json("stat", self._path(bucket, key))
                if lines:
                    return lines[0].get("etag", "")
                return ""
            finally:
                os.unlink(f.name)

    def get_object(self, bucket: str, key: str) -> bytes:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            outpath = f.name
        try:
            self._mc("cp", self._path(bucket, key), outpath)
            with open(outpath, "rb") as f:
                return f.read()
        finally:
            if os.path.exists(outpath):
                os.unlink(outpath)

    def head_object(self, bucket: str, key: str) -> dict:
        lines = self._mc_json("stat", self._path(bucket, key))
        if not lines:
            raise S3ClientError("not found", 404)
        info = lines[0]
        return {
            "size": info.get("size", 0),
            "etag": info.get("etag", ""),
            "content_type": info.get("metadata", {}).get("Content-Type", ""),
        }

    def delete_object(self, bucket: str, key: str) -> None:
        self._mc("rm", self._path(bucket, key))

    def delete_objects(self, bucket: str, keys: list[str]) -> None:
        for key in keys:
            self._mc("rm", self._path(bucket, key), check=False)

    def copy_object(self, bucket: str, src_key: str, dst_key: str) -> None:
        self._mc("cp", self._path(bucket, src_key), self._path(bucket, dst_key))

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> dict:
        target = self._path(bucket, prefix) if prefix else self._path(bucket) + "/"
        # mc ls doesn't support delimiter well — use non-recursive for delimiter="/"
        args = ["ls"]
        if not delimiter:
            args.append("--recursive")
        args.append(target)
        lines = self._mc_json(*args)

        keys = []
        prefixes = []
        for entry in lines:
            name = entry.get("key", "")
            if entry.get("type") == "folder" or name.endswith("/"):
                if delimiter:
                    prefixes.append(prefix + name if prefix and not name.startswith(prefix) else name)
            else:
                full_key = prefix + name if prefix and not name.startswith(prefix) else name
                keys.append(full_key)

        return {"keys": keys[:max_keys], "prefixes": prefixes}

    def upload_file(self, bucket: str, key: str, path: str) -> None:
        self._mc("cp", path, self._path(bucket, key))

    def download_file(self, bucket: str, key: str, path: str) -> None:
        self._mc("cp", self._path(bucket, key), path)
