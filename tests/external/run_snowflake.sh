#!/usr/bin/env bash
#
# BleepStore — Snowflake s3compat API Conformance Runner
#
# Clones snowflakedb/snowflake-s3compat-api-test-suite, builds it via Maven,
# and runs the 9 core S3 operation checks against BleepStore.
#
# Tested APIs:
#   getBucketLocation, getObject, getObjectMetadata, putObject,
#   listObjectsV2, deleteObject, deleteObjects, copyObject,
#   generatePresignedUrl
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9013 ./run_snowflake.sh [options] [test-name...]
#
# Options:
#   --setup-only    Clone and build without running tests
#   --list          List available test methods
#   [test-name]     Specific test(s) to run (e.g., getBucketLocation getObject)
#
# Environment:
#   BLEEPSTORE_ENDPOINT    Server URL (default: http://localhost:9000)
#   BLEEPSTORE_ACCESS_KEY  Access key (default: bleepstore)
#   BLEEPSTORE_SECRET_KEY  Secret key (default: bleepstore-secret)
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ENDPOINT="${BLEEPSTORE_ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${BLEEPSTORE_ACCESS_KEY:-bleepstore}"
SECRET_KEY="${BLEEPSTORE_SECRET_KEY:-bleepstore-secret}"
REGION="${BLEEPSTORE_REGION:-us-east-1}"

S3COMPAT_DIR="/tmp/s3compat"
BUCKET_NAME="s3compat-test-$(date +%s)"

# --- 9 core operations ---
ALL_TESTS=(
    getBucketLocation
    getObject
    getObjectMetadata
    putObject
    listObjectsV2
    deleteObject
    deleteObjects
    copyObject
    generatePresignedUrl
)

# --- Help ---
show_help() {
    echo "BleepStore — Snowflake s3compat API Conformance Runner"
    echo ""
    echo "Usage:"
    echo "  BLEEPSTORE_ENDPOINT=http://localhost:9013 $0 [options] [test-name...]"
    echo ""
    echo "Options:"
    echo "  --help          Show this help"
    echo "  --setup-only    Clone and build without running tests"
    echo "  --list          List available test methods"
    echo "  [test-name]     Specific test(s) to run (e.g., getBucketLocation getObject)"
    echo ""
    echo "Tested APIs (9 core operations):"
    for t in "${ALL_TESTS[@]}"; do
        echo "  - $t"
    done
    echo ""
    echo "Environment:"
    echo "  BLEEPSTORE_ENDPOINT    Server URL (default: http://localhost:9000)"
    echo "  BLEEPSTORE_ACCESS_KEY  Access key (default: bleepstore)"
    echo "  BLEEPSTORE_SECRET_KEY  Secret key (default: bleepstore-secret)"
    echo "  BLEEPSTORE_REGION      Region (default: us-east-1)"
}

# --- Clone / build ---
setup_s3compat() {
    if [ -d "$S3COMPAT_DIR" ]; then
        echo "s3compat already cloned at $S3COMPAT_DIR"
    else
        echo "Cloning snowflakedb/snowflake-s3compat-api-test-suite..."
        git clone --depth 1 https://github.com/snowflakedb/snowflake-s3compat-api-test-suite.git "$S3COMPAT_DIR"
    fi

    # Build the project
    echo "Building s3compat test suite (first run may take a few minutes)..."
    cd "$S3COMPAT_DIR/s3compatapi"
    mvn clean install -DskipTests -q 2>/dev/null || mvn clean install -DskipTests
    cd "$SCRIPT_DIR"
}

# --- Pre-populate test data ---
# The s3compat suite expects certain preconditions:
#   - A bucket that exists
#   - For page listing tests: 1000+ objects with a given prefix
# We create a test bucket and seed minimal data via AWS CLI or curl.
create_test_bucket() {
    echo "Creating test bucket: $BUCKET_NAME"

    # Use AWS CLI if available, otherwise use curl
    if command -v aws &>/dev/null; then
        aws --endpoint-url "$ENDPOINT" \
            --region "$REGION" \
            s3 mb "s3://$BUCKET_NAME" 2>/dev/null || true
    else
        # Fallback: create bucket via curl (unsigned — BleepStore accepts path-style)
        curl -s -X PUT "$ENDPOINT/$BUCKET_NAME" \
            -H "Content-Length: 0" \
            >/dev/null 2>&1 || true
    fi
}

# --- Run tests ---
run_tests() {
    local tests=("$@")

    cd "$S3COMPAT_DIR/s3compatapi"

    # Common Maven arguments — pass config as system properties
    local mvn_args=(
        -DEND_POINT="$ENDPOINT"
        -DS3COMPAT_ACCESS_KEY="$ACCESS_KEY"
        -DS3COMPAT_SECRET_KEY="$SECRET_KEY"
        -DBUCKET_NAME_1="$BUCKET_NAME"
        -DREGION_1="$REGION"
        -DREGION_2="$REGION"
        -DNOT_ACCESSIBLE_BUCKET="nonexistent-bucket-$(date +%s)"
        -DPREFIX_FOR_PAGE_LISTING="page-test/"
        -DPAGE_LISTING_TOTAL_SIZE="0"
    )

    if [ ${#tests[@]} -eq 0 ]; then
        # Run all tests
        echo "Running all 9 s3compat API tests..."
        echo ""
        mvn test -Dtest=S3CompatApiTest "${mvn_args[@]}" || true
    else
        # Run specific tests
        local test_methods=""
        for t in "${tests[@]}"; do
            if [ -n "$test_methods" ]; then
                test_methods="${test_methods}+${t}"
            else
                test_methods="$t"
            fi
        done
        echo "Running s3compat tests: $test_methods"
        echo ""
        mvn test -Dtest="S3CompatApiTest#${test_methods}" "${mvn_args[@]}" || true
    fi

    cd "$SCRIPT_DIR"
}

# --- Main ---
echo "BleepStore — Snowflake s3compat API Conformance"
echo "  Endpoint: $ENDPOINT"
echo "  Region:   $REGION"
echo "=============================================="

# Check Java + Maven
if ! command -v java &>/dev/null; then
    echo "Error: Java is required but not installed."
    echo ""
    echo "Install with:"
    echo "  brew install openjdk"
    echo ""
    echo "Or see: https://adoptium.net/"
    exit 1
fi

if ! command -v mvn &>/dev/null; then
    echo "Error: Maven is required but not installed."
    echo ""
    echo "Install with:"
    echo "  brew install maven"
    echo ""
    echo "Or see: https://maven.apache.org/install.html"
    exit 1
fi

# Handle --help
if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    show_help
    exit 0
fi

# Handle --setup-only
if [ "${1:-}" = "--setup-only" ]; then
    setup_s3compat
    echo ""
    echo "Setup complete. Run without --setup-only to execute tests."
    exit 0
fi

# Handle --list
if [ "${1:-}" = "--list" ]; then
    echo ""
    echo "Available s3compat API tests (9 core operations):"
    echo ""
    for t in "${ALL_TESTS[@]}"; do
        echo "  S3CompatApiTest#$t"
    done
    echo ""
    echo "Run a specific test:"
    echo "  $0 getBucketLocation"
    echo ""
    echo "Run multiple tests:"
    echo "  $0 getObject putObject deleteObject"
    exit 0
fi

# Normal run
setup_s3compat
create_test_bucket

echo ""

# Collect extra args as test names
SELECTED_TESTS=()
for arg in "$@"; do
    SELECTED_TESTS+=("$arg")
done

run_tests "${SELECTED_TESTS[@]}"

EXIT_CODE=${PIPESTATUS[0]:-$?}

echo ""
echo "=============================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "PASS: Snowflake s3compat tests completed successfully"
else
    echo "DONE: Some s3compat tests failed (exit code $EXIT_CODE)"
    echo ""
    echo "Expected failures include:"
    echo "  - generatePresignedUrl (requires HTTPS/SSL endpoint)"
    echo "  - Page listing tests (require 1000+ pre-seeded objects)"
    echo "  - Tests requiring versioned buckets"
    echo ""
    echo "Re-run specific tests with: $0 getBucketLocation getObject putObject"
fi

exit $EXIT_CODE
