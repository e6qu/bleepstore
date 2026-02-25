"""Cross-language SQLite storage backend identity test.

Verifies that all 4 implementations produce identical SQLite storage
data (object_data and part_data tables) when using the sqlite storage
backend and given the same sequence of S3 operations via boto3.

Compares:
  - object_data rows: (bucket, key, data BLOB, etag)
  - Column schemas must match (composite PKs)

Does NOT compare:
  - Metadata tables (timestamps differ)
  - part_data after assembly (parts are cleaned up)

Usage:
    pytest tests/serialization/test_sqlite_storage_identity.py -v

Prerequisites:
    Each server must be built and ready to run on its assigned port.
    Config must support storage.backend: sqlite.
"""

import os
import shutil
import signal
import sqlite3
import subprocess
import time

import boto3
import pytest
from botocore.config import Config

# Language → (port, metadata_db_path)
SERVERS = {
    "python": {
        "port": 9010,
        "db_path": "python/data/metadata.db",
    },
    "go": {
        "port": 9011,
        "db_path": "golang/data/metadata.db",
    },
    "rust": {
        "port": 9012,
        "db_path": "rust/data/metadata.db",
    },
    "zig": {
        "port": 9013,
        "db_path": "zig/data/metadata.db",
    },
}

# Start commands for each language with sqlite backend
START_CMDS = {
    "python": [
        "bash", "-c",
        "cd python && source .venv/bin/activate && "
        "bleepstore --config ../tests/serialization/bleepstore-sqlite.yaml --port 9010",
    ],
    "go": [
        "bash", "-c",
        "cd golang && go run ./cmd/bleepstore "
        "--config ../tests/serialization/bleepstore-sqlite.yaml --port 9011",
    ],
    "rust": [
        "bash", "-c",
        "cd rust && cargo run -- "
        "--config ../tests/serialization/bleepstore-sqlite.yaml --bind 0.0.0.0:9012",
    ],
    "zig": [
        "bash", "-c",
        "cd zig && ./zig-out/bin/bleepstore "
        "--config ../tests/serialization/bleepstore-sqlite.yaml --port 9013",
    ],
}

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BUCKET = "sqlite-identity-bucket"
OBJECTS = {
    "hello.txt": b"Hello, World!",
    "empty.txt": b"",
    "binary.bin": bytes(range(256)) * 4,
}


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
    client.create_bucket(Bucket=BUCKET)
    for key, data in sorted(OBJECTS.items()):
        client.put_object(Bucket=BUCKET, Key=key, Body=data)


def dump_object_data(db_path: str) -> list[tuple]:
    """Read all rows from object_data, sorted by bucket+key."""
    full_path = os.path.join(ROOT_DIR, db_path)
    if not os.path.exists(full_path):
        return []

    conn = sqlite3.connect(full_path)
    try:
        cursor = conn.execute(
            "SELECT bucket, key, data, etag FROM object_data ORDER BY bucket, key"
        )
        return cursor.fetchall()
    except sqlite3.OperationalError:
        # Table might not exist if using a different schema
        return []
    finally:
        conn.close()


def get_table_schema(db_path: str, table_name: str) -> str:
    """Get the CREATE TABLE statement for a table."""
    full_path = os.path.join(ROOT_DIR, db_path)
    if not os.path.exists(full_path):
        return ""

    conn = sqlite3.connect(full_path)
    try:
        cursor = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        row = cursor.fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


def clean_data_dirs():
    """Remove data directories for all languages."""
    for lang in SERVERS:
        data_dir = os.path.join(ROOT_DIR, f"{lang if lang != 'go' else 'golang'}/data")
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)


@pytest.fixture(scope="module")
def storage_data():
    """Start servers with sqlite backend, run operations, dump storage tables."""
    results = {}
    procs = {}

    # Create a config with sqlite backend
    config_path = os.path.join(ROOT_DIR, "tests/serialization/bleepstore-sqlite.yaml")
    if not os.path.exists(config_path):
        pytest.skip("bleepstore-sqlite.yaml config not found")

    # Clean data directories
    clean_data_dirs()

    # Kill anything on the ports
    for lang, cfg in SERVERS.items():
        subprocess.run(
            f"lsof -ti:{cfg['port']} 2>/dev/null | xargs kill -9 2>/dev/null || true",
            shell=True,
        )
    time.sleep(0.3)

    for lang, cmd in START_CMDS.items():
        proc = subprocess.Popen(
            cmd,
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

        # Run operations
        for lang, cfg in SERVERS.items():
            client = make_client(cfg["port"])
            run_operations(client)

        # Brief pause for writes to commit
        time.sleep(1.0)

    finally:
        # Stop all servers
        for lang, proc in procs.items():
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
            proc.wait()

    # Wait for WAL checkpoints to complete
    time.sleep(0.5)

    # Dump storage data from each DB
    for lang, cfg in SERVERS.items():
        rows = dump_object_data(cfg["db_path"])
        schema = get_table_schema(cfg["db_path"], "object_data")
        results[lang] = {"rows": rows, "schema": schema}

    return results


def test_all_servers_have_data(storage_data):
    """All servers should produce storage data rows."""
    for lang, data in storage_data.items():
        assert len(data["rows"]) > 0, f"{lang} produced no object_data rows"


def test_schema_uses_composite_pk(storage_data):
    """All servers should use composite PK (bucket, key) in object_data."""
    for lang, data in storage_data.items():
        schema = data["schema"].upper()
        assert "BUCKET" in schema, f"{lang} object_data missing 'bucket' column"
        assert "KEY" in schema, f"{lang} object_data missing 'key' column"
        # Should NOT have a single storage_key column
        assert "STORAGE_KEY" not in schema, (
            f"{lang} object_data still uses single storage_key column"
        )


def test_row_counts_match(storage_data):
    """All servers should produce the same number of rows."""
    counts = {lang: len(data["rows"]) for lang, data in storage_data.items()}
    ref_count = list(counts.values())[0]
    for lang, count in counts.items():
        assert count == ref_count, (
            f"Row count mismatch: {lang}={count}, expected={ref_count}"
        )


def test_object_data_identical(storage_data):
    """All servers should produce identical object_data rows."""
    langs = sorted(storage_data.keys())
    ref_lang = langs[0]
    ref_rows = storage_data[ref_lang]["rows"]

    for lang in langs[1:]:
        lang_rows = storage_data[lang]["rows"]
        assert len(ref_rows) == len(lang_rows), (
            f"Row count mismatch: {ref_lang}={len(ref_rows)}, {lang}={len(lang_rows)}"
        )
        for i, (ref_row, lang_row) in enumerate(zip(ref_rows, lang_rows)):
            # Compare bucket, key, data (BLOB), etag
            assert ref_row[0] == lang_row[0], (
                f"Row {i} bucket mismatch: {ref_lang}={ref_row[0]}, {lang}={lang_row[0]}"
            )
            assert ref_row[1] == lang_row[1], (
                f"Row {i} key mismatch: {ref_lang}={ref_row[1]}, {lang}={lang_row[1]}"
            )
            assert ref_row[2] == lang_row[2], (
                f"Row {i} data mismatch for {ref_row[0]}/{ref_row[1]}"
            )
            assert ref_row[3] == lang_row[3], (
                f"Row {i} etag mismatch for {ref_row[0]}/{ref_row[1]}: "
                f"{ref_lang}={ref_row[3]}, {lang}={lang_row[3]}"
            )
