#!/usr/bin/env bash
#
# BleepStore E2E Test Runner
#
# This uses its OWN virtualenv (tests/.venv), completely independent from
# any language implementation virtualenv (e.g., python/.venv).
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9000 ./run_tests.sh [pytest-args...]
#
# Examples:
#   ./run_tests.sh                          # Run all E2E tests
#   ./run_tests.sh -m bucket_ops            # Run only bucket tests
#   ./run_tests.sh -m object_ops            # Run only object tests
#   ./run_tests.sh -m multipart_ops         # Run only multipart tests
#   ./run_tests.sh -m presigned             # Run only presigned URL tests
#   ./run_tests.sh -k test_put              # Run tests matching "test_put"
#   ./run_tests.sh --co                     # List tests without running
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Require uv
if ! command -v uv &>/dev/null; then
    echo "Error: uv is required. Install from https://docs.astral.sh/uv/"
    exit 1
fi

# Create tests-specific virtualenv (independent from python/.venv)
VENV_DIR="$SCRIPT_DIR/.venv"
uv venv "$VENV_DIR" --quiet 2>/dev/null || true
source "$VENV_DIR/bin/activate"
uv pip install -r e2e/requirements.txt --quiet

# Run E2E tests
ENDPOINT="${BLEEPSTORE_ENDPOINT:-http://localhost:9000}"
echo "BleepStore E2E Tests"
echo "  Endpoint:   $ENDPOINT"
echo "  Virtualenv: $VENV_DIR"
echo "=============================================="
python -m pytest e2e/ -v --tb=short "$@"
