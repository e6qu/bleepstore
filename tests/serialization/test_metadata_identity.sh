#!/usr/bin/env bash
# Cross-language metadata SQLite identity test.
#
# Verifies that all 4 implementations produce identical SQLite metadata
# databases when importing the same reference fixture.
#
# This goes beyond JSON round-trip (test_cross_language.sh) by comparing
# the actual SQLite row data after import.
#
# Usage: ./test_metadata_identity.sh
#
# Prerequisites:
#   - Python: cd python && uv pip install -e ".[dev]"
#   - Go:     cd golang && go build -o bleepstore-meta ./cmd/bleepstore-meta
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

TABLES="buckets objects multipart_uploads multipart_parts credentials"

# Create a fresh SQLite DB with the expected schema and import using the
# given language's bleepstore-meta tool.
import_with() {
    local lang=$1
    local db="$TMPDIR/${lang}.db"

    # Create schema (identical across all languages).
    python3 -c "
import sqlite3
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

    # Import using the language-specific tool.
    case "$lang" in
        python)
            (cd "$ROOT_DIR/python" && source .venv/bin/activate && \
                bleepstore-meta import --db "$db" --input "$FIXTURE" 2>/dev/null)
            ;;
        go)
            "$ROOT_DIR/golang/bleepstore-meta" import -db "$db" -input "$FIXTURE" 2>/dev/null
            ;;
        rust)
            "$ROOT_DIR/rust/target/debug/bleepstore-meta" import --db "$db" --input "$FIXTURE" 2>/dev/null
            ;;
        zig)
            "$ROOT_DIR/zig/zig-out/bin/bleepstore-meta" import --db "$db" --input "$FIXTURE" 2>/dev/null
            ;;
    esac
}

# Dump a table's rows as sorted, deterministic text.
# Normalizes JSON whitespace (compact format) for consistent comparison.
dump_table() {
    local db=$1
    local table=$2
    sqlite3 "$db" "SELECT * FROM $table ORDER BY 1, 2;" 2>/dev/null | \
        python3 -c "
import sys, json
for line in sys.stdin:
    fields = line.rstrip('\n').split('|')
    normalized = []
    for f in fields:
        try:
            obj = json.loads(f)
            normalized.append(json.dumps(obj, separators=(',', ':'), sort_keys=True))
        except (json.JSONDecodeError, ValueError):
            normalized.append(f)
    print('|'.join(normalized))
" || true
}

echo "=== Cross-Language Metadata SQLite Identity Test ==="
echo "Reference fixture: $FIXTURE"
echo

# Build Go binary if needed.
if [ ! -f "$ROOT_DIR/golang/bleepstore-meta" ]; then
    echo "Building Go bleepstore-meta..."
    (cd "$ROOT_DIR/golang" && go build -o bleepstore-meta ./cmd/bleepstore-meta)
fi

# Import with all 4 languages.
LANGS="python go rust zig"
for lang in $LANGS; do
    echo "Importing with $lang..."
    import_with "$lang"
done
echo

# Use Python as the reference, compare others against it.
REF_LANG="python"
COMPARE_LANGS="go rust zig"

for table in $TABLES; do
    dump_table "$TMPDIR/${REF_LANG}.db" "$table" > "$TMPDIR/ref_${table}.txt"
done

ALL_OK=true
for lang in $COMPARE_LANGS; do
    echo "--- Comparing $lang vs $REF_LANG ---"
    LANG_OK=true
    for table in $TABLES; do
        dump_table "$TMPDIR/${lang}.db" "$table" > "$TMPDIR/${lang}_${table}.txt"
        if diff -q "$TMPDIR/ref_${table}.txt" "$TMPDIR/${lang}_${table}.txt" >/dev/null 2>&1; then
            echo "  OK: $table"
        else
            echo "  FAIL: $table"
            diff "$TMPDIR/ref_${table}.txt" "$TMPDIR/${lang}_${table}.txt" | head -10
            LANG_OK=false
            ALL_OK=false
        fi
    done
    if $LANG_OK; then
        echo "  PASS: $lang"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $lang"
        FAIL=$((FAIL + 1))
    fi
    echo
done

NUM_LANGS=$(echo $COMPARE_LANGS | wc -w | tr -d ' ')
echo "=== Results ==="
echo "  Passed: $PASS / $NUM_LANGS"
echo "  Failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
    echo "FAILED"
    exit 1
else
    echo "ALL PASSED"
    exit 0
fi
