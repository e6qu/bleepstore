#!/usr/bin/env bash
#
# BleepStore Rust — E2E Test Runner
#
# Builds the project, starts the server, runs E2E tests, stops the server.
# Logs are written to rust/logs/ (gitignored).
#
# Usage:
#   ./run_e2e.sh [pytest-args...]
#   ./run_e2e.sh -m bucket_ops
#   ./run_e2e.sh -k test_put
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PORT=9012
LOG_DIR="$SCRIPT_DIR/logs"
SERVER_LOG="$LOG_DIR/server.log"
E2E_LOG="$LOG_DIR/e2e.log"

mkdir -p "$LOG_DIR"

# Build
echo "Building BleepStore Rust..."
cd "$SCRIPT_DIR"
cargo build --release 2>&1

# Kill any existing server on our port
lsof -ti:$PORT 2>/dev/null | xargs kill -9 2>/dev/null || true
sleep 0.5

# Start the server in background
echo "Starting BleepStore Rust on port $PORT..."
./target/release/bleepstore --config "$PROJECT_ROOT/bleepstore.example.yaml" --bind "0.0.0.0:$PORT" \
    > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# Cleanup on exit
cleanup() {
    echo "Stopping server (PID $SERVER_PID)..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to be ready
echo "Waiting for server..."
for i in $(seq 1 30); do
    if curl -s "http://localhost:$PORT/" >/dev/null 2>&1; then
        echo "Server ready."
        break
    fi
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "Server failed to start. Check $SERVER_LOG"
        exit 1
    fi
    sleep 0.5
done

# Run E2E tests
echo ""
echo "Running E2E tests against http://localhost:$PORT"
echo "=============================================="
cd "$PROJECT_ROOT"
BLEEPSTORE_ENDPOINT="http://localhost:$PORT" \
    tests/run_tests.sh "$@" 2>&1 | tee "$E2E_LOG"
