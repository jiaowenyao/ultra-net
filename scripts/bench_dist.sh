#!/bin/bash
# Distributed benchmark orchestrator — proper multi-process TCP testing.
#
# Usage:
#   bash scripts/bench_dist.sh local  <workers> <params> <steps>
#   bash scripts/bench_dist.sh tcp    <workers> <params> <steps>
#   bash scripts/bench_dist.sh scale  <params> <steps>

set -e

MODE="${1:-local}"
WORKERS="${2:-4}"
PARAMS="${3:-50000}"
STEPS="${4:-100}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="$SCRIPT_DIR/../build/bin/dist-bench"
PORT=18001
RESULT_DIR="/tmp/bench_$$"

mkdir -p "$RESULT_DIR"

wait_for_port() {
    local port=$1
    local timeout=${2:-30}
    for i in $(seq 1 $timeout); do
        if ss -tlnp 2>/dev/null | grep -q ":$port "; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

run_tcp_test() {
    echo "=== TCP Multi-Process Benchmark ==="
    echo "  workers: $WORKERS  params: $PARAMS  steps: $STEPS"
    echo ""

    # Kill any stale processes on the port.
    fuser -k ${PORT}/tcp 2>/dev/null || true
    sleep 1

    # Start PS process.
    echo "[orch] starting PS on :$PORT ..."
    $BINARY ps $PORT $WORKERS $PARAMS $STEPS > "$RESULT_DIR/ps.log" 2>&1 &
    PS_PID=$!

    # Wait for PS to be ready.
    echo "[orch] waiting for PS ..."
    if ! wait_for_port $PORT 30; then
        echo "[orch] ERROR: PS failed to start"
        kill $PS_PID 2>/dev/null
        return 1
    fi
    echo "[orch] PS ready (pid=$PS_PID)"

    # Start worker processes.
    WORKER_PIDS=()
    for i in $(seq 0 $((WORKERS - 1))); do
        $BINARY worker 127.0.0.1:$PORT $i $STEPS 1 $PARAMS \
            > "$RESULT_DIR/w${i}.log" 2>&1 &
        WORKER_PIDS+=($!)
    done
    echo "[orch] started $WORKERS workers (pids: ${WORKER_PIDS[*]})"

    # Wait for all workers to finish.
    echo "[orch] waiting for workers ..."
    for pid in "${WORKER_PIDS[@]}"; do
        wait $pid 2>/dev/null || true
    done

    # Wait for PS to finish.
    wait $PS_PID 2>/dev/null || true

    echo ""
    echo "=== PS Output ==="
    cat "$RESULT_DIR/ps.log"

    echo ""
    echo "=== Worker Summary ==="
    local total_ok=0 total_err=0
    for i in $(seq 0 $((WORKERS - 1))); do
        local log="$RESULT_DIR/w${i}.log"
        if [ -f "$log" ]; then
            local errs=$(grep -c 'lost\|failed\|error' "$log" 2>/dev/null || echo 0)
            if [ "$errs" -eq 0 ]; then
                echo "  W$i: OK ($(wc -l < "$log") lines)"
                ((total_ok++))
            else
                echo "  W$i: $errs errors"
                ((total_err++))
            fi
        else
            echo "  W$i: NO LOG"
            ((total_err++))
        fi
    done
    echo "  Total: $total_ok OK, $total_err errors"

    # Cleanup.
    fuser -k ${PORT}/tcp 2>/dev/null || true
    echo ""
    echo "Results saved to: $RESULT_DIR"
}

run_local_test() {
    echo "=== Local (Shared Memory) Benchmark ==="
    $BINARY local $WORKERS $PARAMS $STEPS
}

run_scale_test() {
    # Scale test: 1, 2, 4, 8 workers with fixed params and steps.
    echo "=== Scaling Benchmark ==="
    echo "  params: $PARAMS  steps: $STEPS"
    echo ""
    printf "  %-10s %-10s %-12s %-12s\n" "Workers" "Data(MB)" "Time(ms)" "MB/s"

    for w in 1 2 4 8; do
        fuser -k ${PORT}/tcp 2>/dev/null || true
        sleep 1

        $BINARY ps $PORT $w $PARAMS $((STEPS / w)) > "$RESULT_DIR/ps_${w}.log" 2>&1 &
        PS_PID=$!

        wait_for_port $PORT 20 || { kill $PS_PID 2>/dev/null; continue; }

        for i in $(seq 0 $((w - 1))); do
            $BINARY worker 127.0.0.1:$PORT $i $((STEPS / w)) 1 $PARAMS \
                > "$RESULT_DIR/w_${w}_${i}.log" 2>&1 &
        done

        wait

        local data_mb=$(grep "data:" "$RESULT_DIR/ps_${w}.log" 2>/dev/null | \
            awk '{print $NF}')
        local time_ms=$(grep "wall time\|time:" "$RESULT_DIR/ps_${w}.log" 2>/dev/null | \
            grep -oP '[0-9]+\s*ms' | grep -oP '[0-9]+')
        if [ -n "$data_mb" ] && [ -n "$time_ms" ] && [ "$time_ms" -gt 0 ]; then
            local mbps=$(echo "scale=1; $data_mb / ($time_ms / 1000.0)" | bc 2>/dev/null || echo "?")
            printf "  %-10s %-10s %-12s %-12s\n" "$w" "$data_mb" "$time_ms" "$mbps"
        fi

        kill $PS_PID 2>/dev/null || true
    done

    fuser -k ${PORT}/tcp 2>/dev/null || true
}

# ── Main ───────────────────────────────────────────────────────────────

case "$MODE" in
    local)  run_local_test ;;
    tcp)    run_tcp_test ;;
    scale)  run_scale_test ;;
    *)
        echo "Usage: $0 {local|tcp|scale} [workers] [params] [steps]"
        exit 1
        ;;
esac
