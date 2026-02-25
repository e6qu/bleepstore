#!/usr/bin/env bash
#
# BleepStore Performance Test Runner
#
# Uses its OWN virtualenv (tests/performance/.venv), completely independent
# from the E2E test virtualenv (tests/.venv) and any language implementation
# virtualenv (e.g., python/.venv).
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9000 ./run_benchmarks.sh [tool] [args...]
#
# Tools:
#   ./run_benchmarks.sh boto          # Run boto3 benchmarks (latency + throughput + multipart)
#   ./run_benchmarks.sh locust        # Run Locust load test (headless, 30s, 20 users)
#   ./run_benchmarks.sh k6            # Run k6 load test (requires k6 installed separately)
#   ./run_benchmarks.sh stress        # Run stress test scenarios (connection storms, large objects, etc.)
#   ./run_benchmarks.sh scaling       # Run concurrency scaling benchmarks (throughput curve)
#   ./run_benchmarks.sh all           # Run boto3 + locust
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

ENDPOINT="${BLEEPSTORE_ENDPOINT:-http://localhost:9000}"
TOOL="${1:-boto}"
shift 2>/dev/null || true

# Require uv
if ! command -v uv &>/dev/null; then
    echo "Error: uv is required. Install from https://docs.astral.sh/uv/"
    exit 1
fi

# Create performance-specific virtualenv
VENV_DIR="$SCRIPT_DIR/.venv"
uv venv "$VENV_DIR" --quiet 2>/dev/null || true
source "$VENV_DIR/bin/activate"
uv pip install -r requirements.txt --quiet

echo "BleepStore Performance Tests"
echo "  Endpoint:   $ENDPOINT"
echo "  Tool:       $TOOL"
echo "  Virtualenv: $VENV_DIR"
echo "=============================================="

case "$TOOL" in
    boto)
        echo ""
        echo "--- Latency Benchmarks ---"
        python bench_latency.py --endpoint "$ENDPOINT" "$@"
        echo ""
        echo "--- Throughput Benchmarks ---"
        python bench_throughput.py --endpoint "$ENDPOINT" "$@"
        echo ""
        echo "--- Multipart Benchmarks ---"
        python bench_multipart.py --endpoint "$ENDPOINT" "$@"
        ;;
    locust)
        echo ""
        echo "--- Locust Load Test (headless) ---"
        locust -f locustfile.py \
            --headless \
            -u "${LOCUST_USERS:-20}" \
            -r "${LOCUST_SPAWN_RATE:-5}" \
            --run-time "${LOCUST_DURATION:-30s}" \
            "$@"
        ;;
    k6)
        if ! command -v k6 &>/dev/null; then
            echo "Error: k6 is required. Install from https://grafana.com/docs/k6/latest/set-up/install-k6/"
            exit 1
        fi
        echo ""
        echo "--- k6 Load Test ---"
        k6 run \
            --env "ENDPOINT=$ENDPOINT" \
            --env "ACCESS_KEY=${BLEEPSTORE_ACCESS_KEY:-bleepstore}" \
            --env "SECRET_KEY=${BLEEPSTORE_SECRET_KEY:-bleepstore-secret}" \
            k6-s3.js "$@"
        ;;
    stress)
        echo ""
        echo "--- Stress Test Scenarios ---"
        python bench_stress.py --endpoint "$ENDPOINT" "$@"
        ;;
    scaling)
        echo ""
        echo "--- Concurrency Scaling Benchmarks ---"
        python bench_scaling.py --endpoint "$ENDPOINT" "$@"
        ;;
    all)
        "$0" boto "$@"
        echo ""
        "$0" locust "$@"
        ;;
    *)
        echo "Unknown tool: $TOOL"
        echo "Usage: $0 {boto|locust|k6|stress|scaling|all} [args...]"
        exit 1
        ;;
esac
