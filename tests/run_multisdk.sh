#!/usr/bin/env bash
#
# BleepStore Multi-SDK Test Runner
#
# Runs S3 operations through every available S3 client (boto3, AWS CLI, mc, s3cmd, rclone).
# Uses the same virtualenv as run_tests.sh (tests/.venv).
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9013 ./run_multisdk.sh [pytest-args...]
#
# Examples:
#   ./run_multisdk.sh                         # All available clients
#   ./run_multisdk.sh -k boto3                # Only boto3 clients
#   ./run_multisdk.sh -k awscli               # Only AWS CLI
#   ./run_multisdk.sh -m bucket_ops           # Only bucket tests
#   ./run_multisdk.sh --co                    # List tests without running
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Require uv
if ! command -v uv &>/dev/null; then
    echo "Error: uv is required. Install from https://docs.astral.sh/uv/"
    exit 1
fi

# Create/reuse tests-specific virtualenv
VENV_DIR="$SCRIPT_DIR/.venv"
uv venv "$VENV_DIR" --quiet 2>/dev/null || true
source "$VENV_DIR/bin/activate"
uv pip install -r multisdk/requirements.txt --quiet

# Report tool availability
ENDPOINT="${BLEEPSTORE_ENDPOINT:-http://localhost:9000}"
echo "BleepStore Multi-SDK Tests"
echo "  Endpoint:   $ENDPOINT"
echo "  Virtualenv: $VENV_DIR"
echo ""
echo "  Tool availability:"
echo "    boto3      ✓ (always available)"
echo "    boto3-res  ✓ (always available)"

for tool in aws mc s3cmd rclone; do
    if command -v "$tool" &>/dev/null; then
        version=$("$tool" --version 2>&1 | head -1 || echo "unknown")
        printf "    %-10s ✓ (%s)\n" "$tool" "$version"
    else
        printf "    %-10s ✗ (not installed, skipping)\n" "$tool"
    fi
done

echo ""
echo "=============================================="

# Run multi-SDK tests
python -m pytest multisdk/ -v --tb=short "$@"
