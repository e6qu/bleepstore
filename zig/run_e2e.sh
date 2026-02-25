#!/usr/bin/env bash
#
# BleepStore Zig — E2E Test Runner
#
# Builds the project, starts the server, runs E2E tests, stops the server.
# Logs are written to zig/logs/ (gitignored).
#
# Usage:
#   ./run_e2e.sh [pytest-args...]
#   ./run_e2e.sh --backend memory
#   ./run_e2e.sh --backend sqlite -m bucket_ops
#   ./run_e2e.sh -k test_put
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PORT=9013
LOG_DIR="$SCRIPT_DIR/logs"
SERVER_LOG="$LOG_DIR/server.log"
E2E_LOG="$LOG_DIR/e2e.log"

# Parse --backend flag (defaults to "local")
BACKEND="local"
REMAINING_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --backend)
            BACKEND="$2"
            shift 2
            ;;
        *)
            REMAINING_ARGS+=("$1")
            shift
            ;;
    esac
done
set -- "${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}"

mkdir -p "$LOG_DIR"

# Build (skip if binary already exists, e.g. from `make build`)
cd "$SCRIPT_DIR"
if [ ! -f zig-out/bin/bleepstore ]; then
    echo "Building BleepStore Zig..."
    zig build 2>&1
else
    echo "Using existing BleepStore Zig binary."
fi

# Kill any existing server on our port
lsof -ti:$PORT 2>/dev/null | xargs kill -9 2>/dev/null || true
sleep 0.5

# Ensure data directories exist (config uses ./data/metadata.db and ./data/objects)
mkdir -p "$SCRIPT_DIR/data/objects"

# Generate config with selected backend
CONFIG_FILE="$PROJECT_ROOT/bleepstore.example.yaml"
if [ "$BACKEND" != "local" ]; then
    CONFIG_FILE="$LOG_DIR/bleepstore-${BACKEND}.yaml"
    sed "s/backend: \"local\"/backend: \"${BACKEND}\"/" \
        "$PROJECT_ROOT/bleepstore.example.yaml" > "$CONFIG_FILE"
    echo "Using storage backend: $BACKEND"
fi

# Start the server in background
echo "Starting BleepStore Zig on port $PORT..."
./zig-out/bin/bleepstore --config "$CONFIG_FILE" --port $PORT \
    > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# Cleanup on exit
cleanup() {
    echo "Stopping server (PID $SERVER_PID)..."
    kill -9 $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to be ready
echo "Waiting for server..."
SERVER_READY=false
for i in $(seq 1 30); do
    if curl -s "http://localhost:$PORT/" >/dev/null 2>&1; then
        echo "Server ready."
        SERVER_READY=true
        break
    fi
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "Server failed to start. Log output:"
        cat "$SERVER_LOG" 2>/dev/null || true
        exit 1
    fi
    sleep 0.5
done
if [ "$SERVER_READY" = false ]; then
    echo "Server did not become ready within 15s. Log output:"
    cat "$SERVER_LOG" 2>/dev/null || true
    exit 1
fi

# Run E2E tests
echo ""
echo "Running E2E tests against http://localhost:$PORT"
echo "=============================================="
cd "$PROJECT_ROOT"
BLEEPSTORE_ENDPOINT="http://localhost:$PORT" \
    tests/run_tests.sh "$@" 2>&1 | tee "$E2E_LOG"
