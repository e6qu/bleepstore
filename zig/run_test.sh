#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
rm -rf data
mkdir -p logs

# Start server
./zig-out/bin/bleepstore --port 9013 > logs/server.log 2>&1 &
SERVER_PID=$!
trap "kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null" EXIT

# Wait for server
for i in $(seq 1 30); do
    if curl -s http://localhost:9013/health >/dev/null 2>&1; then break; fi
    sleep 0.5
done

# Run tests
cd ..
BLEEPSTORE_ENDPOINT=http://localhost:9013 tests/.venv/bin/python -m pytest tests/e2e/ -v --tb=short 2>&1
