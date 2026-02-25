#!/usr/bin/env bash
#
# BleepStore — Ceph s3-tests Conformance Runner
#
# Clones ceph/s3-tests, generates config from env vars, and runs the test suite
# with filters to skip unsupported features (versioning, encryption, etc.).
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9013 ./run_s3tests.sh [options] [pytest-args...]
#
# Options:
#   --setup-only    Clone and install without running tests
#   --list          List available tests without running
#   [pytest-args]   Passed through (e.g., -k test_bucket, -x, --tb=long)
#
# Environment:
#   BLEEPSTORE_ENDPOINT    Server URL (default: http://localhost:9000)
#   BLEEPSTORE_ACCESS_KEY  Access key (default: bleepstore)
#   BLEEPSTORE_SECRET_KEY  Secret key (default: bleepstore-secret)
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

ENDPOINT="${BLEEPSTORE_ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${BLEEPSTORE_ACCESS_KEY:-bleepstore}"
SECRET_KEY="${BLEEPSTORE_SECRET_KEY:-bleepstore-secret}"

# Parse host and port from endpoint
PROTO="${ENDPOINT%%://*}"
HOSTPORT="${ENDPOINT#*://}"
HOST="${HOSTPORT%%:*}"
PORT="${HOSTPORT##*:}"
PORT="${PORT%%/*}"

# Default port if none specified
if [ "$HOST" = "$PORT" ]; then
    if [ "$PROTO" = "https" ]; then
        PORT=443
    else
        PORT=80
    fi
fi

S3TESTS_DIR="$SCRIPT_DIR/s3-tests"
CONF_FILE="$SCRIPT_DIR/s3tests.conf"

# --- Clone / update s3-tests ---
setup_s3tests() {
    if [ -d "$S3TESTS_DIR" ]; then
        echo "s3-tests already cloned at $S3TESTS_DIR"
    else
        echo "Cloning ceph/s3-tests..."
        git clone --depth 1 https://github.com/ceph/s3-tests.git "$S3TESTS_DIR"
    fi

    # Install dependencies via tox (creates .tox virtualenv)
    cd "$S3TESTS_DIR"
    if ! command -v tox &>/dev/null; then
        echo "Error: tox is required. Install with: pip install tox"
        exit 1
    fi
    echo "Setting up tox environment (first run may take a minute)..."
    tox -e py -- --co -q 2>/dev/null || true
    cd "$SCRIPT_DIR"
}

# --- Generate s3tests.conf ---
generate_config() {
    echo "Generating s3tests.conf..."
    sed \
        -e "s|{host}|$HOST|g" \
        -e "s|{port}|$PORT|g" \
        -e "s|{access_key}|$ACCESS_KEY|g" \
        -e "s|{secret_key}|$SECRET_KEY|g" \
        "$SCRIPT_DIR/s3tests.conf.template" > "$CONF_FILE"
    echo "  Config: $CONF_FILE"
    echo "  Host:   $HOST:$PORT"
}

# --- Feature exclusion filters ---
# Skip tests for features BleepStore doesn't support yet.
# These are pytest marker/keyword filters.
EXCLUDE_MARKERS=(
    "versioning"
    "lifecycle"
    "encryption"
    "replication"
    "website"
    "test_of_sts"
    "webidentity_test"
    "bucket_logging"
    "tagging"
    "object_lock"
    "sse"
    "cors"
    "policy"
    "notification"
    "inventory"
    "analytics"
    "select"
    "torrent"
    "payment"
)

build_exclude_expr() {
    local expr=""
    for marker in "${EXCLUDE_MARKERS[@]}"; do
        if [ -z "$expr" ]; then
            expr="$marker"
        else
            expr="$expr or $marker"
        fi
    done
    echo "$expr"
}

# --- Main ---
echo "BleepStore — Ceph s3-tests Conformance"
echo "  Endpoint: $ENDPOINT"
echo "=============================================="

# Handle --setup-only
if [ "${1:-}" = "--setup-only" ]; then
    setup_s3tests
    generate_config
    echo ""
    echo "Setup complete. Run without --setup-only to execute tests."
    exit 0
fi

# Handle --list
if [ "${1:-}" = "--list" ]; then
    setup_s3tests
    generate_config
    cd "$S3TESTS_DIR"
    S3TEST_CONF="$CONF_FILE" tox -e py -- \
        s3tests_boto3/functional/test_s3.py \
        --co -q
    exit 0
fi

# Normal run
setup_s3tests
generate_config

echo ""
echo "Running s3-tests (excluding unsupported features)..."
echo ""

EXCLUDE_EXPR="$(build_exclude_expr)"

cd "$S3TESTS_DIR"
S3TEST_CONF="$CONF_FILE" tox -e py -- \
    s3tests_boto3/functional/test_s3.py \
    -k "not ($EXCLUDE_EXPR)" \
    --tb=short -v \
    "$@"

EXIT_CODE=$?

echo ""
echo "=============================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "PASS: All filtered s3-tests passed"
else
    echo "DONE: Some tests failed (exit code $EXIT_CODE)"
    echo ""
    echo "Expected failures include:"
    echo "  - Tests requiring distinct users (BleepStore is single-user)"
    echo "  - Tests for edge cases not yet implemented"
    echo ""
    echo "Re-run with -x to stop on first failure, or -k PATTERN to filter."
fi

exit $EXIT_CODE
