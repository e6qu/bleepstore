#!/usr/bin/env bash
#
# BleepStore — MinIO Mint Multi-SDK Conformance Runner
#
# Runs the MinIO Mint test suite via Docker. Mint tests BleepStore against
# Go, Java, .NET, PHP, Ruby, and Python SDKs.
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9013 ./run_mint.sh
#
# Requires: Docker Desktop running
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

# Parse host and port from endpoint
HOSTPORT="${ENDPOINT#*://}"
HOST="${HOSTPORT%%:*}"
PORT="${HOSTPORT##*:}"
PORT="${PORT%%/*}"

echo "BleepStore — MinIO Mint Multi-SDK Tests"
echo "  Endpoint: $ENDPOINT"
echo "=============================================="

# Check Docker
if ! command -v docker &>/dev/null; then
    echo "Error: Docker is required but not installed."
    echo "Install Docker Desktop from https://www.docker.com/products/docker-desktop/"
    exit 1
fi

if ! docker info &>/dev/null 2>&1; then
    echo "Error: Docker daemon is not running."
    echo "Start Docker Desktop and try again."
    exit 1
fi

# For macOS Docker, use host.docker.internal to reach the host
# For Linux, use --network=host
DOCKER_HOST_ADDR="host.docker.internal"
if [ "$(uname)" = "Linux" ]; then
    DOCKER_HOST_ADDR="$HOST"
fi

SERVER="${DOCKER_HOST_ADDR}:${PORT}"

echo ""
echo "Pulling minio/mint image (if needed)..."
docker pull minio/mint --quiet 2>/dev/null || docker pull minio/mint

echo ""
echo "Running Mint tests against $SERVER..."
echo ""

CONTAINER_NAME="bleepstore-mint-$(date +%s)"

# Run Mint container
# ENABLE_VIRTUAL_STYLE=0 forces path-style addressing
docker run --rm \
    --name "$CONTAINER_NAME" \
    -e "SERVER_ENDPOINT=$SERVER" \
    -e "ACCESS_KEY=$ACCESS_KEY" \
    -e "SECRET_KEY=$SECRET_KEY" \
    -e "ENABLE_VIRTUAL_STYLE=0" \
    -e "SERVER_REGION=us-east-1" \
    minio/mint

EXIT_CODE=$?

echo ""
echo "=============================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "PASS: Mint tests completed successfully"
else
    echo "DONE: Some Mint tests failed (exit code $EXIT_CODE)"
    echo ""
    echo "Expected failures include:"
    echo "  - Tests for features BleepStore doesn't support (versioning, etc.)"
    echo "  - Tests requiring virtual-hosted-style bucket addressing"
fi

exit $EXIT_CODE
