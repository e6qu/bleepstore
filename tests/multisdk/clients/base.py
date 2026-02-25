"""S3Client protocol and CLI base class for multi-SDK testing."""

from __future__ import annotations

import json
import subprocess
from typing import Protocol, runtime_checkable


@runtime_checkable
class S3Client(Protocol):
    """Abstract S3 client — one implementation per SDK."""

    name: str

    def create_bucket(self, bucket: str) -> None: ...
    def delete_bucket(self, bucket: str) -> None: ...
    def head_bucket(self, bucket: str) -> int: ...
    def list_buckets(self) -> list[str]: ...

    def put_object(self, bucket: str, key: str, body: bytes) -> str: ...
    def get_object(self, bucket: str, key: str) -> bytes: ...
    def head_object(self, bucket: str, key: str) -> dict: ...
    def delete_object(self, bucket: str, key: str) -> None: ...
    def delete_objects(self, bucket: str, keys: list[str]) -> None: ...
    def copy_object(self, bucket: str, src_key: str, dst_key: str) -> None: ...

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> dict: ...

    def upload_file(self, bucket: str, key: str, path: str) -> None: ...
    def download_file(self, bucket: str, key: str, path: str) -> None: ...


class CliS3Client:
    """Base for CLI-based S3 clients. Provides subprocess execution and error handling."""

    timeout: int = 30

    def _run(
        self, *args: str, input_data: bytes | None = None, check: bool = True
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            args,
            input=input_data,
            capture_output=True,
            timeout=self.timeout,
            check=check,
        )

    def _run_json(self, *args: str) -> dict:
        result = self._run(*args)
        return json.loads(result.stdout)


class S3ClientError(Exception):
    """Raised when an S3 operation fails."""

    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code
