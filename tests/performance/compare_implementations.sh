#!/usr/bin/env bash
#
# BleepStore Cross-Implementation Benchmark Comparison
#
# Builds, starts, benchmarks, and compares all 4 BleepStore implementations.
# Generates a markdown report with side-by-side performance tables.
#
# Uses its OWN virtualenv (tests/performance/.venv), completely independent
# from the E2E test virtualenv (tests/.venv) and any language implementation
# virtualenv (e.g., python/.venv).
#
# Usage:
#   ./compare_implementations.sh [options]
#
# Options:
#   --implementations python,go,rust,zig   Comma-separated list (default: all)
#   --benchmarks latency,throughput,scaling Comma-separated list (default: all)
#   --output-dir ./results                 Output directory (default: ./results)
#   --help                                 Show this help message
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Defaults
IMPLEMENTATIONS="python,go,rust,zig"
BENCHMARKS="latency,throughput,scaling"
OUTPUT_DIR="$SCRIPT_DIR/results"

# Port map
declare -A PORTS
PORTS[python]=9010
PORTS[go]=9011
PORTS[rust]=9012
PORTS[zig]=9013

# PID tracking for cleanup
declare -A SERVER_PIDS

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --implementations)
            IMPLEMENTATIONS="$2"
            shift 2
            ;;
        --benchmarks)
            BENCHMARKS="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help)
            echo "BleepStore Cross-Implementation Benchmark Comparison"
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --implementations LIST  Comma-separated implementations (default: python,go,rust,zig)"
            echo "  --benchmarks LIST       Comma-separated benchmarks (default: latency,throughput,scaling)"
            echo "  --output-dir PATH       Output directory for JSON results (default: ./results)"
            echo "  --help                  Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run $0 --help for usage."
            exit 1
            ;;
    esac
done

# Convert comma-separated to arrays
IFS=',' read -ra IMPL_ARRAY <<< "$IMPLEMENTATIONS"
IFS=',' read -ra BENCH_ARRAY <<< "$BENCHMARKS"

# Cleanup function — kills all started servers
cleanup() {
    echo ""
    echo "Cleaning up servers..."
    for impl in "${!SERVER_PIDS[@]}"; do
        pid="${SERVER_PIDS[$impl]}"
        if kill -0 "$pid" 2>/dev/null; then
            echo "  Stopping $impl (PID $pid)..."
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    echo "All servers stopped."
}
trap cleanup EXIT

# Require uv
if ! command -v uv &>/dev/null; then
    echo "Error: uv is required. Install from https://docs.astral.sh/uv/"
    exit 1
fi

# Setup performance virtualenv
VENV_DIR="$SCRIPT_DIR/.venv"
uv venv "$VENV_DIR" --quiet 2>/dev/null || true
source "$VENV_DIR/bin/activate"
uv pip install -r "$SCRIPT_DIR/requirements.txt" --quiet

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "BleepStore Cross-Implementation Benchmark"
echo "  Implementations: ${IMPL_ARRAY[*]}"
echo "  Benchmarks:      ${BENCH_ARRAY[*]}"
echo "  Output dir:      $OUTPUT_DIR"
echo "=============================================="

# ---- Build Phase ----
echo ""
echo "=== Build Phase ==="

for impl in "${IMPL_ARRAY[@]}"; do
    case "$impl" in
        python)
            echo "  Python: no build needed (interpreted)"
            # Ensure Python venv exists and package is installed
            if [ ! -d "$PROJECT_ROOT/python/.venv" ]; then
                echo "  Python: creating virtualenv..."
                cd "$PROJECT_ROOT/python"
                uv venv .venv --quiet
                source .venv/bin/activate
                uv pip install -e ".[dev]" --quiet
                deactivate
                # Re-activate perf venv
                source "$VENV_DIR/bin/activate"
            fi
            ;;
        go)
            echo "  Go: building..."
            cd "$PROJECT_ROOT/golang"
            go build -o ./bleepstore ./cmd/bleepstore
            echo "  Go: build complete."
            ;;
        rust)
            echo "  Rust: building (release)..."
            cd "$PROJECT_ROOT/rust"
            cargo build --release 2>&1 | tail -1
            echo "  Rust: build complete."
            ;;
        zig)
            echo "  Zig: building (ReleaseFast)..."
            cd "$PROJECT_ROOT/zig"
            zig build -Doptimize=ReleaseFast 2>&1
            echo "  Zig: build complete."
            ;;
        *)
            echo "  Warning: unknown implementation '$impl', skipping."
            ;;
    esac
done

# ---- Start Phase ----
echo ""
echo "=== Start Phase ==="

for impl in "${IMPL_ARRAY[@]}"; do
    port="${PORTS[$impl]}"

    # Kill any existing process on this port
    lsof -ti:"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
    sleep 0.3

    # Ensure data directories exist
    mkdir -p "$PROJECT_ROOT/$([[ "$impl" == "go" ]] && echo "golang" || echo "$impl")/data/objects"

    case "$impl" in
        python)
            echo "  Starting Python on port $port..."
            cd "$PROJECT_ROOT/python"
            source .venv/bin/activate
            bleepstore --config "$PROJECT_ROOT/bleepstore.example.yaml" --port "$port" \
                > "$PROJECT_ROOT/python/logs/benchmark.log" 2>&1 &
            SERVER_PIDS[python]=$!
            # Re-activate perf venv
            source "$VENV_DIR/bin/activate"
            ;;
        go)
            echo "  Starting Go on port $port..."
            cd "$PROJECT_ROOT/golang"
            ./bleepstore --config "$PROJECT_ROOT/bleepstore.example.yaml" --port "$port" \
                > "$PROJECT_ROOT/golang/logs/benchmark.log" 2>&1 &
            SERVER_PIDS[go]=$!
            ;;
        rust)
            echo "  Starting Rust on port $port..."
            cd "$PROJECT_ROOT/rust"
            ./target/release/bleepstore --config "$PROJECT_ROOT/bleepstore.example.yaml" --bind "0.0.0.0:$port" \
                > "$PROJECT_ROOT/rust/logs/benchmark.log" 2>&1 &
            SERVER_PIDS[rust]=$!
            ;;
        zig)
            echo "  Starting Zig on port $port..."
            cd "$PROJECT_ROOT/zig"
            ./zig-out/bin/bleepstore --config "$PROJECT_ROOT/bleepstore.example.yaml" --port "$port" \
                > "$PROJECT_ROOT/zig/logs/benchmark.log" 2>&1 &
            SERVER_PIDS[zig]=$!
            ;;
    esac
done

# ---- Health Check Phase ----
echo ""
echo "=== Health Check Phase ==="

READY_IMPLS=()
for impl in "${IMPL_ARRAY[@]}"; do
    port="${PORTS[$impl]}"
    pid="${SERVER_PIDS[$impl]:-}"

    if [ -z "$pid" ]; then
        echo "  $impl: no PID (not started), skipping."
        continue
    fi

    ready=false
    for i in $(seq 1 30); do
        if curl -sf "http://localhost:$port/health" >/dev/null 2>&1; then
            echo "  $impl: ready on port $port (PID $pid)"
            ready=true
            break
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "  $impl: server process died. Check logs."
            break
        fi
        sleep 0.5
    done

    if [ "$ready" = true ]; then
        READY_IMPLS+=("$impl")
    else
        echo "  $impl: FAILED to start within 15s, skipping benchmarks."
    fi
done

if [ ${#READY_IMPLS[@]} -eq 0 ]; then
    echo ""
    echo "ERROR: No implementations started successfully. Aborting."
    exit 1
fi

echo ""
echo "Ready implementations: ${READY_IMPLS[*]}"

# ---- Benchmark Phase ----
echo ""
echo "=== Benchmark Phase ==="

cd "$SCRIPT_DIR"

for impl in "${READY_IMPLS[@]}"; do
    port="${PORTS[$impl]}"
    endpoint="http://localhost:$port"

    echo ""
    echo "--- Benchmarking $impl ($endpoint) ---"

    for bench in "${BENCH_ARRAY[@]}"; do
        json_file="$OUTPUT_DIR/${impl}_${bench}.json"
        echo "  Running $bench benchmark..."

        case "$bench" in
            latency)
                python bench_latency.py \
                    --endpoint "$endpoint" \
                    --implementation "$impl" \
                    --json-file "$json_file" \
                    2>&1 | tail -5
                ;;
            throughput)
                python bench_throughput.py \
                    --endpoint "$endpoint" \
                    --implementation "$impl" \
                    --json-file "$json_file" \
                    2>&1 | tail -5
                ;;
            scaling)
                python bench_scaling.py \
                    --endpoint "$endpoint" \
                    --implementation "$impl" \
                    --json-file "$json_file" \
                    2>&1 | tail -5
                ;;
            *)
                echo "  Warning: unknown benchmark '$bench', skipping."
                ;;
        esac

        if [ -f "$json_file" ]; then
            echo "  -> Saved $json_file"
        else
            echo "  -> WARNING: $json_file was not created"
        fi
    done
done

# ---- Report Phase ----
echo ""
echo "=== Report Phase ==="

REPORT_FILE="$OUTPUT_DIR/BENCHMARK_RESULTS.md"
python "$SCRIPT_DIR/compare_report.py" \
    --results-dir "$OUTPUT_DIR" \
    --output "$REPORT_FILE"

echo "Report written to: $REPORT_FILE"
echo ""
echo "=== Done ==="
echo "Results directory: $OUTPUT_DIR"
ls -la "$OUTPUT_DIR"
