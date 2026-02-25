#!/usr/bin/env bash
# Cross-language metadata serialization round-trip validation.
#
# Tests that all 4 implementations (Python, Go, Rust, Zig) produce identical
# JSON output when importing from the reference fixture and re-exporting.
#
# Usage: ./test_cross_language.sh
#
# Prerequisites:
#   - Python: cd python && uv pip install -e ".[dev]"
#   - Go:     cd golang && go build ./cmd/bleepstore-meta
#   - Rust:   cd rust   && cargo build --bin bleepstore-meta
#   - Zig:    cd zig    && zig build

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
FIXTURE="$ROOT_DIR/tests/fixtures/metadata-export-reference.json"
TMPDIR="$(mktemp -d)"
trap "rm -rf $TMPDIR" EXIT

PASS=0
FAIL=0

check() {
    local lang=$1
    local export_file="$TMPDIR/${lang}_export.json"

    echo "--- $lang ---"

    # Create fresh DB with schema.
    local db="$TMPDIR/${lang}.db"
    python3 -c "
import sqlite3, sys
conn = sqlite3.connect('$db')
conn.executescript('''
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (1, '2026-01-01T00:00:00.000Z');
CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, region TEXT NOT NULL DEFAULT 'us-east-1', owner_id TEXT NOT NULL, owner_display TEXT NOT NULL DEFAULT '', acl TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS objects (bucket TEXT NOT NULL, key TEXT NOT NULL, size INTEGER NOT NULL, etag TEXT NOT NULL, content_type TEXT NOT NULL DEFAULT 'application/octet-stream', content_encoding TEXT, content_language TEXT, content_disposition TEXT, cache_control TEXT, expires TEXT, storage_class TEXT NOT NULL DEFAULT 'STANDARD', acl TEXT NOT NULL DEFAULT '{}', user_metadata TEXT NOT NULL DEFAULT '{}', last_modified TEXT NOT NULL, delete_marker INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (bucket, key), FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS multipart_uploads (upload_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, key TEXT NOT NULL, content_type TEXT NOT NULL DEFAULT 'application/octet-stream', content_encoding TEXT, content_language TEXT, content_disposition TEXT, cache_control TEXT, expires TEXT, storage_class TEXT NOT NULL DEFAULT 'STANDARD', acl TEXT NOT NULL DEFAULT '{}', user_metadata TEXT NOT NULL DEFAULT '{}', owner_id TEXT NOT NULL, owner_display TEXT NOT NULL DEFAULT '', initiated_at TEXT NOT NULL, FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS multipart_parts (upload_id TEXT NOT NULL, part_number INTEGER NOT NULL, size INTEGER NOT NULL, etag TEXT NOT NULL, last_modified TEXT NOT NULL, PRIMARY KEY (upload_id, part_number), FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS credentials (access_key_id TEXT PRIMARY KEY, secret_key TEXT NOT NULL, owner_id TEXT NOT NULL, display_name TEXT NOT NULL DEFAULT '', active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL);
''')
conn.commit()
conn.close()
"

    # Import and re-export using language-specific tool.
    case "$lang" in
        python)
            (cd "$ROOT_DIR/python" && source .venv/bin/activate && \
                bleepstore-meta import --db "$db" --input "$FIXTURE" 2>/dev/null && \
                bleepstore-meta export --db "$db" --include-credentials --output "$export_file" 2>/dev/null)
            ;;
        go)
            "$ROOT_DIR/golang/bleepstore-meta" import -db "$db" -input "$FIXTURE" 2>/dev/null
            "$ROOT_DIR/golang/bleepstore-meta" export -db "$db" -include-credentials -output "$export_file" 2>/dev/null
            ;;
        rust)
            "$ROOT_DIR/rust/target/debug/bleepstore-meta" import --db "$db" --input "$FIXTURE" 2>/dev/null
            "$ROOT_DIR/rust/target/debug/bleepstore-meta" export --db "$db" --include-credentials --output "$export_file" 2>/dev/null
            ;;
        zig)
            "$ROOT_DIR/zig/zig-out/bin/bleepstore-meta" import --db "$db" --input "$FIXTURE" 2>/dev/null
            "$ROOT_DIR/zig/zig-out/bin/bleepstore-meta" export --db "$db" --include-credentials --output "$export_file" 2>/dev/null
            ;;
    esac

    # Compare data sections (skip envelope since timestamps differ).
    python3 -c "
import json, sys
ref = json.load(open('$FIXTURE'))
lang = json.load(open('$export_file'))
tables = ['buckets', 'objects', 'multipart_uploads', 'multipart_parts', 'credentials']
ok = True
for t in tables:
    if ref.get(t) != lang.get(t):
        print(f'  FAIL: {t} mismatch')
        ok = False
    else:
        print(f'  OK: {t}')
sys.exit(0 if ok else 1)
"
    if [ $? -eq 0 ]; then
        echo "  PASS: $lang"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $lang"
        FAIL=$((FAIL + 1))
    fi
    echo
}

echo "=== Cross-Language Metadata Serialization Test ==="
echo "Reference fixture: $FIXTURE"
echo

# Build Go binary if needed.
if [ ! -f "$ROOT_DIR/golang/bleepstore-meta" ]; then
    echo "Building Go bleepstore-meta..."
    (cd "$ROOT_DIR/golang" && go build -o bleepstore-meta ./cmd/bleepstore-meta)
fi

check python
check go
check rust
check zig

echo "=== Results ==="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"

if [ $FAIL -gt 0 ]; then
    echo "FAILED"
    exit 1
else
    echo "ALL PASSED"
    exit 0
fi
