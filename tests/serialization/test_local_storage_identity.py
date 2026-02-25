"""Cross-language local storage file identity test.

Verifies that all 4 implementations (Python, Go, Rust, Zig) produce
identical file trees on the local filesystem when given the same
sequence of S3 operations via boto3.

Compares:
  - File tree structure (paths and names)
  - File content (MD5 checksums)

Does NOT compare:
  - Metadata SQLite databases (timestamps differ)
  - Temp/log files

Usage:
    pytest tests/serialization/test_local_storage_identity.py -v

Prerequisites:
    Each server must be built and ready to run on its assigned port.
"""

import hashlib
import os
import shutil
import signal
import subprocess
import time

import boto3
import pytest
from botocore.config import Config

# Language → (port, data_dir, start_cmd)
SERVERS = {
    "python": {
        "port": 9010,
        "data_dir": "python/data/objects",
        "start_cmd": [
            "bash", "-c",
            "cd python && source .venv/bin/activate && "
            "bleepstore --config ../bleepstore.example.yaml --port 9010",
        ],
    },
    "go": {
        "port": 9011,
        "data_dir": "golang/data/objects",
        "start_cmd": [
            "bash", "-c",
            "cd golang && go run ./cmd/bleepstore "
            "--config ../bleepstore.example.yaml --port 9011",
        ],
    },
    "rust": {
        "port": 9012,
        "data_dir": "rust/data/objects",
        "start_cmd": [
            "bash", "-c",
            "cd rust && cargo run -- "
            "--config ../bleepstore.example.yaml --bind 0.0.0.0:9012",
        ],
    },
    "zig": {
        "port": 9013,
        "data_dir": "zig/data/objects",
        "start_cmd": [
            "bash", "-c",
            "cd zig && ./zig-out/bin/bleepstore "
            "--config ../bleepstore.example.yaml --port 9013",
        ],
    },
}

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Fixed bucket/key names for determinism.
BUCKET = "identity-test-bucket"
OBJECTS = {
    "hello.txt": b"Hello, World!",
    "empty.txt": b"",
    "binary.bin": bytes(range(256)) * 4,
    "nested/path/deep.txt": b"deeply nested object",
}

MULTIPART_KEY = "multipart-file.bin"
MULTIPART_PARTS = [b"part-one-data-" * 100, b"part-two-data-" * 100, b"part-three-" * 100]


def make_client(port: int):
    """Create a boto3 S3 client pointing at a local server."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{port}",
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def wait_for_server(port: int, timeout: float = 30.0) -> bool:
    """Wait for a server to accept connections."""
    import socket

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sock = socket.create_connection(("localhost", port), timeout=1.0)
            sock.close()
            return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False


def run_operations(client):
    """Run a deterministic sequence of S3 operations."""
    # Create bucket
    client.create_bucket(Bucket=BUCKET)

    # Put objects
    for key, data in sorted(OBJECTS.items()):
        client.put_object(Bucket=BUCKET, Key=key, Body=data)

    # Multipart upload
    mpu = client.create_multipart_upload(Bucket=BUCKET, Key=MULTIPART_KEY)
    upload_id = mpu["UploadId"]

    parts = []
    for i, part_data in enumerate(MULTIPART_PARTS, start=1):
        resp = client.upload_part(
            Bucket=BUCKET,
            Key=MULTIPART_KEY,
            UploadId=upload_id,
            PartNumber=i,
            Body=part_data,
        )
        parts.append({"PartNumber": i, "ETag": resp["ETag"]})

    client.complete_multipart_upload(
        Bucket=BUCKET,
        Key=MULTIPART_KEY,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )


def collect_file_tree(data_dir: str) -> dict[str, str]:
    """Walk a directory and return {relative_path: md5hex} for all files.

    Skips hidden files/dirs and metadata databases.
    """
    tree = {}
    for dirpath, dirnames, filenames in os.walk(data_dir):
        # Skip hidden dirs (but include .multipart)
        dirnames[:] = [
            d for d in dirnames
            if not d.startswith(".") or d == ".multipart"
        ]
        for fname in filenames:
            if fname.endswith((".db", ".db-wal", ".db-shm", ".db-journal")):
                continue
            if fname.startswith("."):
                continue
            full = os.path.join(dirpath, fname)
            rel = os.path.relpath(full, data_dir)
            md5 = hashlib.md5(open(full, "rb").read()).hexdigest()
            tree[rel] = md5
    return tree


def clean_data_dir(data_dir: str):
    """Remove the data directory for a clean test."""
    full_path = os.path.join(ROOT_DIR, data_dir)
    if os.path.exists(full_path):
        shutil.rmtree(full_path)


@pytest.fixture(scope="module")
def file_trees():
    """Start each server, run operations, collect file trees, stop servers."""
    trees = {}
    procs = {}

    for lang, cfg in SERVERS.items():
        # Clean data directory
        clean_data_dir(cfg["data_dir"])

        # Kill anything on the port
        subprocess.run(
            f"lsof -ti:{cfg['port']} 2>/dev/null | xargs kill -9 2>/dev/null || true",
            shell=True,
        )
        time.sleep(0.3)

    for lang, cfg in SERVERS.items():
        # Start server
        proc = subprocess.Popen(
            cfg["start_cmd"],
            cwd=ROOT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid,
        )
        procs[lang] = proc

    try:
        # Wait for all servers
        for lang, cfg in SERVERS.items():
            if not wait_for_server(cfg["port"]):
                pytest.skip(f"{lang} server did not start on port {cfg['port']}")

        # Run operations on each server
        for lang, cfg in SERVERS.items():
            client = make_client(cfg["port"])
            run_operations(client)

        # Brief pause for any async writes to flush
        time.sleep(1.0)

        # Collect file trees
        for lang, cfg in SERVERS.items():
            data_dir = os.path.join(ROOT_DIR, cfg["data_dir"])
            if os.path.exists(data_dir):
                trees[lang] = collect_file_tree(data_dir)
            else:
                trees[lang] = {}
    finally:
        # Stop all servers
        for lang, proc in procs.items():
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
            proc.wait()

    return trees


def test_all_servers_have_files(file_trees):
    """All servers should produce a non-empty file tree."""
    for lang, tree in file_trees.items():
        assert len(tree) > 0, f"{lang} produced no files"


def test_file_lists_identical(file_trees):
    """All servers should produce the same set of file paths."""
    langs = sorted(file_trees.keys())
    ref_lang = langs[0]
    ref_files = sorted(file_trees[ref_lang].keys())

    for lang in langs[1:]:
        lang_files = sorted(file_trees[lang].keys())
        assert ref_files == lang_files, (
            f"File list mismatch between {ref_lang} and {lang}.\n"
            f"  Only in {ref_lang}: {set(ref_files) - set(lang_files)}\n"
            f"  Only in {lang}: {set(lang_files) - set(ref_files)}"
        )


def test_file_contents_identical(file_trees):
    """All servers should produce files with identical content (by MD5)."""
    langs = sorted(file_trees.keys())
    ref_lang = langs[0]

    for lang in langs[1:]:
        for path, ref_md5 in file_trees[ref_lang].items():
            assert path in file_trees[lang], f"{lang} missing file: {path}"
            assert ref_md5 == file_trees[lang][path], (
                f"Content mismatch for {path}: "
                f"{ref_lang}={ref_md5}, {lang}={file_trees[lang][path]}"
            )
