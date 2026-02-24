#!/usr/bin/env bash
#
# BleepStore Benchmark Runner
#
# Builds & starts a BleepStore implementation (and optionally MinIO as baseline),
# runs the benchmark suite, then tears everything down.
#
# Usage:
#   ./run.sh python                  # Benchmark Python impl only
#   ./run.sh python --vs-minio       # Benchmark Python impl vs MinIO
#   ./run.sh go --vs-minio           # Benchmark Go impl vs MinIO
#   ./run.sh rust --vs-minio         # Benchmark Rust impl vs MinIO
#   ./run.sh zig --vs-minio          # Benchmark Zig impl vs MinIO
#   ./run.sh all --vs-minio          # Benchmark all impls vs MinIO
#   ./run.sh python -n 200           # 200 iterations per latency test
#   ./run.sh python --json           # Output raw JSON
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="$PROJECT_ROOT/bleepstore.example.yaml"

MINIO_PORT=9099
MINIO_CONSOLE_PORT=9098
MINIO_DATA="/tmp/bleepstore-bench-minio-$$"

# Implementation → port mapping (function for bash 3.x compat)
get_port() {
    case "$1" in
        python) echo 9010 ;;
        go)     echo 9011 ;;
        rust)   echo 9012 ;;
        zig)    echo 9013 ;;
        *)      echo "" ;;
    esac
}

# Track PIDs for cleanup
IMPL_PID=""
MINIO_PID=""

cleanup() {
    if [[ -n "$IMPL_PID" ]]; then
        kill "$IMPL_PID" 2>/dev/null || true
        wait "$IMPL_PID" 2>/dev/null || true
    fi
    if [[ -n "$MINIO_PID" ]]; then
        kill "$MINIO_PID" 2>/dev/null || true
        wait "$MINIO_PID" 2>/dev/null || true
    fi
    rm -rf "$MINIO_DATA" 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

wait_for_health() {
    local url="$1" name="$2" timeout="${3:-30}"
    local elapsed=0
    while ! curl -sf "$url" >/dev/null 2>&1; do
        sleep 0.2
        elapsed=$(echo "$elapsed + 0.2" | bc)
        if (( $(echo "$elapsed >= $timeout" | bc -l) )); then
            echo "ERROR: $name did not become healthy at $url within ${timeout}s" >&2
            exit 1
        fi
    done
}

kill_port() {
    local port="$1"
    lsof -ti:"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
    sleep 0.3
}

# ---------------------------------------------------------------------------
# Server start functions
# ---------------------------------------------------------------------------

start_python() {
    local port="$(get_port python)"
    local dir="$PROJECT_ROOT/python"
    kill_port "$port"

    # Ensure venv + deps
    (cd "$dir" && uv venv .venv --quiet 2>/dev/null || true)
    (cd "$dir" && source .venv/bin/activate && uv pip install -e ".[dev]" --quiet 2>/dev/null)

    mkdir -p "$dir/data/objects"
    (cd "$dir" && source .venv/bin/activate && \
        bleepstore --config "$CONFIG" --port "$port" >/dev/null 2>&1) &
    IMPL_PID=$!

    wait_for_health "http://localhost:$port/health" "Python BleepStore"
    echo "  Python BleepStore running on :$port (PID $IMPL_PID)"
}

start_go() {
    local port="$(get_port go)"
    local dir="$PROJECT_ROOT/golang"
    kill_port "$port"

    # Build if needed
    if [[ ! -f "$dir/bleepstore" ]]; then
        echo "  Building Go..."
        (cd "$dir" && go build -o bleepstore ./cmd/bleepstore)
    fi

    mkdir -p "$dir/data/objects"
    (cd "$dir" && ./bleepstore --config "$CONFIG" --port "$port" >/dev/null 2>&1) &
    IMPL_PID=$!

    wait_for_health "http://localhost:$port/health" "Go BleepStore"
    echo "  Go BleepStore running on :$port (PID $IMPL_PID)"
}

start_rust() {
    local port="$(get_port rust)"
    local dir="$PROJECT_ROOT/rust"
    kill_port "$port"

    # Find or build binary
    local binary=""
    if [[ -f "$dir/target/release/bleepstore" ]]; then
        binary="$dir/target/release/bleepstore"
    elif [[ -f "$dir/target/debug/bleepstore" ]]; then
        binary="$dir/target/debug/bleepstore"
    else
        echo "  Building Rust (release)..."
        (cd "$dir" && cargo build --release)
        binary="$dir/target/release/bleepstore"
    fi

    mkdir -p "$dir/data/objects"
    (cd "$dir" && "$binary" --config "$CONFIG" --bind "0.0.0.0:$port" >/dev/null 2>&1) &
    IMPL_PID=$!

    wait_for_health "http://localhost:$port/health" "Rust BleepStore"
    echo "  Rust BleepStore running on :$port (PID $IMPL_PID)"
}

start_zig() {
    local port="$(get_port zig)"
    local dir="$PROJECT_ROOT/zig"
    kill_port "$port"

    # Build if needed
    if [[ ! -f "$dir/zig-out/bin/bleepstore" ]]; then
        echo "  Building Zig..."
        (cd "$dir" && zig build)
    fi

    mkdir -p "$dir/data/objects"
    (cd "$dir" && ./zig-out/bin/bleepstore --config "$CONFIG" --port "$port" >/dev/null 2>&1) &
    IMPL_PID=$!

    wait_for_health "http://localhost:$port/health" "Zig BleepStore"
    echo "  Zig BleepStore running on :$port (PID $IMPL_PID)"
}

start_minio() {
    kill_port "$MINIO_PORT"
    mkdir -p "$MINIO_DATA"

    if ! command -v minio &>/dev/null; then
        echo "ERROR: minio not found. Install with: brew install minio/stable/minio" >&2
        exit 1
    fi

    MINIO_ROOT_USER=bleepstore MINIO_ROOT_PASSWORD=bleepstore-secret \
        minio server "$MINIO_DATA" \
        --address ":$MINIO_PORT" \
        --console-address ":$MINIO_CONSOLE_PORT" >/dev/null 2>&1 &
    MINIO_PID=$!

    wait_for_health "http://localhost:$MINIO_PORT/minio/health/live" "MinIO"
    echo "  MinIO running on :$MINIO_PORT (PID $MINIO_PID)"
}

stop_impl() {
    if [[ -n "$IMPL_PID" ]]; then
        kill "$IMPL_PID" 2>/dev/null || true
        wait "$IMPL_PID" 2>/dev/null || true
        IMPL_PID=""
    fi
}

# ---------------------------------------------------------------------------
# Benchmark virtualenv
# ---------------------------------------------------------------------------

setup_bench_venv() {
    local venv="$SCRIPT_DIR/.venv"
    uv venv "$venv" --quiet 2>/dev/null || true
    source "$venv/bin/activate"
    uv pip install -r "$SCRIPT_DIR/requirements.txt" --quiet 2>/dev/null
}

# ---------------------------------------------------------------------------
# Run benchmark for one implementation
# ---------------------------------------------------------------------------

run_one() {
    local impl="$1"
    shift
    local vs_minio=false
    local extra_args=()

    # Parse remaining args
    for arg in "$@"; do
        case "$arg" in
            --vs-minio) vs_minio=true ;;
            *) extra_args+=("$arg") ;;
        esac
    done

    local port="$(get_port "$impl")"
    local label
    case "$impl" in
        python) label="Python" ;;
        go)     label="Go" ;;
        rust)   label="Rust" ;;
        zig)    label="Zig" ;;
    esac

    echo ""
    echo "=========================================="
    echo "  Benchmarking: $label"
    echo "=========================================="
    echo ""

    # Start the implementation server
    echo "Starting servers..."
    "start_$impl"

    local bench_args=(
        --endpoint "http://localhost:$port"
        --label "$label"
        --pid "$IMPL_PID"
    )

    if $vs_minio; then
        if [[ -z "$MINIO_PID" ]]; then
            start_minio
        fi
        bench_args+=(
            --baseline "http://localhost:$MINIO_PORT"
            --baseline-label "MinIO"
            --baseline-pid "$MINIO_PID"
        )
    fi

    if [[ ${#extra_args[@]} -gt 0 ]]; then
        bench_args+=("${extra_args[@]}")
    fi

    echo ""
    python "$SCRIPT_DIR/bench.py" "${bench_args[@]}"

    # Stop the implementation (but leave MinIO for next impl if running "all")
    stop_impl
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <python|go|rust|zig|all> [--vs-minio] [-n N] [--json]"
    exit 1
fi

IMPL="$1"
shift

# Require uv
if ! command -v uv &>/dev/null; then
    echo "Error: uv is required. Install from https://docs.astral.sh/uv/" >&2
    exit 1
fi

setup_bench_venv

if [[ "$IMPL" == "all" ]]; then
    # Check if --vs-minio is in the args
    for arg in "$@"; do
        if [[ "$arg" == "--vs-minio" ]]; then
            start_minio
            break
        fi
    done

    for lang in python go rust zig; do
        run_one "$lang" "$@"
    done
else
    if [[ -z "$(get_port "$IMPL")" ]]; then
        echo "Unknown implementation: $IMPL"
        echo "Usage: $0 <python|go|rust|zig|all> [--vs-minio] [-n N] [--json]"
        exit 1
    fi
    run_one "$IMPL" "$@"
fi

echo ""
echo "Done."
