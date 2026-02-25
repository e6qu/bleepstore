#!/usr/bin/env bash
#
# BleepStore — Unified External Test Runner
#
# Single entry point for all external/third-party test suites.
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9013 ./run_external.sh [suite] [args...]
#
# Suites:
#   s3tests   Ceph s3-tests conformance (default)
#   mint      MinIO Mint multi-SDK (requires Docker)
#   warp      MinIO Warp performance
#   snowflake Snowflake s3compat API tests (requires Java + Maven)
#   all       Run all available suites
#   --list    Show available suites and their status
#
# Environment:
#   BLEEPSTORE_ENDPOINT    Server URL (default: http://localhost:9000)
#   BLEEPSTORE_ACCESS_KEY  Access key (default: bleepstore)
#   BLEEPSTORE_SECRET_KEY  Secret key (default: bleepstore-secret)
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTERNAL_DIR="$SCRIPT_DIR/external"

ENDPOINT="${BLEEPSTORE_ENDPOINT:-http://localhost:9000}"
SUITE="${1:-s3tests}"
shift 2>/dev/null || true

# --- Availability checks ---
check_s3tests() {
    command -v tox &>/dev/null && command -v git &>/dev/null
}

check_mint() {
    command -v docker &>/dev/null && docker info &>/dev/null 2>&1
}

check_warp() {
    command -v warp &>/dev/null
}

check_snowflake() {
    command -v java &>/dev/null && command -v mvn &>/dev/null
}

# --- List available suites ---
list_suites() {
    echo "BleepStore — External Test Suites"
    echo "  Endpoint: $ENDPOINT"
    echo "=============================================="
    echo ""

    printf "  %-12s %-40s %s\n" "Suite" "Description" "Status"
    printf "  %-12s %-40s %s\n" "-----" "-----------" "------"

    if check_s3tests; then
        printf "  %-12s %-40s %s\n" "s3tests" "Ceph s3-tests (~400 conformance tests)" "READY"
    else
        printf "  %-12s %-40s %s\n" "s3tests" "Ceph s3-tests (~400 conformance tests)" "MISSING (need: git, tox)"
    fi

    if check_mint; then
        printf "  %-12s %-40s %s\n" "mint" "MinIO Mint multi-SDK tests" "READY"
    else
        printf "  %-12s %-40s %s\n" "mint" "MinIO Mint multi-SDK tests" "MISSING (need: Docker)"
    fi

    if check_warp; then
        printf "  %-12s %-40s %s\n" "warp" "MinIO Warp S3 load testing" "READY"
    else
        printf "  %-12s %-40s %s\n" "warp" "MinIO Warp S3 load testing" "MISSING (brew install minio/stable/warp)"
    fi

    if check_snowflake; then
        printf "  %-12s %-40s %s\n" "snowflake" "Snowflake s3compat (9 core ops)" "READY"
    else
        printf "  %-12s %-40s %s\n" "snowflake" "Snowflake s3compat (9 core ops)" "MISSING (need: java, mvn)"
    fi

    echo ""
    echo "Usage: BLEEPSTORE_ENDPOINT=http://localhost:9013 $0 {s3tests|mint|warp|snowflake|all}"
}

# --- Run suites ---
run_s3tests() {
    echo ""
    echo ">>> Running Ceph s3-tests..."
    echo ""
    "$EXTERNAL_DIR/run_s3tests.sh" "$@"
}

run_mint() {
    echo ""
    echo ">>> Running MinIO Mint..."
    echo ""
    "$EXTERNAL_DIR/run_mint.sh" "$@"
}

run_warp() {
    echo ""
    echo ">>> Running MinIO Warp..."
    echo ""
    "$EXTERNAL_DIR/run_warp.sh" "$@"
}

run_snowflake() {
    echo ""
    echo ">>> Running Snowflake s3compat..."
    echo ""
    "$EXTERNAL_DIR/run_snowflake.sh" "$@"
}

# --- Main ---
case "$SUITE" in
    --list|-l|list)
        list_suites
        ;;
    s3tests)
        run_s3tests "$@"
        ;;
    mint)
        run_mint "$@"
        ;;
    warp)
        run_warp "$@"
        ;;
    snowflake)
        run_snowflake "$@"
        ;;
    all)
        FAILED=0

        if check_s3tests; then
            run_s3tests "$@" || FAILED=$((FAILED + 1))
        else
            echo ""
            echo ">>> Skipping s3tests (tox/git not available)"
        fi

        if check_mint; then
            run_mint "$@" || FAILED=$((FAILED + 1))
        else
            echo ""
            echo ">>> Skipping mint (Docker not available)"
        fi

        if check_warp; then
            run_warp "$@" || FAILED=$((FAILED + 1))
        else
            echo ""
            echo ">>> Skipping warp (not installed)"
        fi

        if check_snowflake; then
            run_snowflake "$@" || FAILED=$((FAILED + 1))
        else
            echo ""
            echo ">>> Skipping snowflake (java/mvn not available)"
        fi

        echo ""
        echo "=============================================="
        if [ $FAILED -eq 0 ]; then
            echo "All available external suites completed."
        else
            echo "$FAILED suite(s) had failures."
            exit 1
        fi
        ;;
    *)
        echo "Unknown suite: $SUITE"
        echo ""
        echo "Available suites: s3tests, mint, warp, snowflake, all"
        echo "Use --list to see availability."
        exit 1
        ;;
esac
