#!/usr/bin/env bash
#
# BleepStore — MinIO Warp Load Testing Runner
#
# Runs structured S3 performance benchmarks using MinIO Warp with
# statistical output (throughput, p50/p90/p99 latency, ops/sec).
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9013 ./run_warp.sh [profile]
#
# Profiles:
#   quick       30s mixed workload, 4 concurrent, 100 objects (default)
#   stress      5m mixed workload, 32 concurrent, 1000 objects
#   put         PUT-only benchmark
#   get         GET-only benchmark
#   mixed       Mixed operations
#   multipart   Multipart upload benchmark
#
# Environment:
#   BLEEPSTORE_ENDPOINT    Server URL (default: http://localhost:9000)
#   BLEEPSTORE_ACCESS_KEY  Access key (default: bleepstore)
#   BLEEPSTORE_SECRET_KEY  Secret key (default: bleepstore-secret)
#
set -euo pipefail

ENDPOINT="${BLEEPSTORE_ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${BLEEPSTORE_ACCESS_KEY:-bleepstore}"
SECRET_KEY="${BLEEPSTORE_SECRET_KEY:-bleepstore-secret}"
PROFILE="${1:-quick}"

# Parse host:port for warp --host flag
HOSTPORT="${ENDPOINT#*://}"

echo "BleepStore — MinIO Warp Load Test"
echo "  Endpoint: $ENDPOINT"
echo "  Profile:  $PROFILE"
echo "=============================================="

# Check warp is installed
if ! command -v warp &>/dev/null; then
    echo "Error: warp is required but not installed."
    echo ""
    echo "Install with:"
    echo "  brew install minio/stable/warp"
    echo ""
    echo "Or see: https://github.com/minio/warp"
    exit 1
fi

# Common warp flags
WARP_COMMON=(
    --host "$HOSTPORT"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --tls=false
    --autoterm
)

echo ""

case "$PROFILE" in
    quick)
        echo "Running: Quick mixed workload (30s, 4 concurrent, 100 objects)"
        echo ""
        warp mixed "${WARP_COMMON[@]}" \
            --concurrent 4 \
            --objects 100 \
            --obj.size 1KiB \
            --duration 30s \
            --benchdata /tmp/bleepstore-warp-quick
        ;;
    stress)
        echo "Running: Stress mixed workload (5m, 32 concurrent, 1000 objects)"
        echo ""
        warp mixed "${WARP_COMMON[@]}" \
            --concurrent 32 \
            --objects 1000 \
            --obj.size 10KiB \
            --duration 5m \
            --benchdata /tmp/bleepstore-warp-stress
        ;;
    put)
        echo "Running: PUT-only benchmark (30s, 8 concurrent)"
        echo ""
        warp put "${WARP_COMMON[@]}" \
            --concurrent 8 \
            --obj.size 1KiB \
            --duration 30s \
            --autoterm \
            --benchdata /tmp/bleepstore-warp-put
        ;;
    get)
        echo "Running: GET-only benchmark (30s, 8 concurrent, 100 objects)"
        echo ""
        warp get "${WARP_COMMON[@]}" \
            --concurrent 8 \
            --objects 100 \
            --obj.size 1KiB \
            --duration 30s \
            --benchdata /tmp/bleepstore-warp-get
        ;;
    mixed)
        echo "Running: Mixed benchmark (60s, 16 concurrent, 500 objects)"
        echo ""
        warp mixed "${WARP_COMMON[@]}" \
            --concurrent 16 \
            --objects 500 \
            --obj.size 10KiB \
            --duration 60s \
            --benchdata /tmp/bleepstore-warp-mixed
        ;;
    multipart)
        echo "Running: Multipart upload benchmark (60s, 4 concurrent)"
        echo ""
        warp multipart "${WARP_COMMON[@]}" \
            --concurrent 4 \
            --parts 10 \
            --part.size 5MiB \
            --duration 60s \
            --benchdata /tmp/bleepstore-warp-multipart
        ;;
    *)
        echo "Unknown profile: $PROFILE"
        echo ""
        echo "Available profiles:"
        echo "  quick       30s mixed workload, 4 concurrent, 100 objects (default)"
        echo "  stress      5m mixed workload, 32 concurrent, 1000 objects"
        echo "  put         PUT-only benchmark"
        echo "  get         GET-only benchmark"
        echo "  mixed       Mixed operations (60s, 16 concurrent)"
        echo "  multipart   Multipart upload benchmark"
        exit 1
        ;;
esac

echo ""
echo "=============================================="
echo "Warp benchmark complete."
