#!/usr/bin/env bash
#
# mydis benchmark harness.
#
# Captures the protocol worked out during Phase 7/8:
#   * release build, both processes pinned to fixed cores
#   * a fresh server and a fresh AOF for every configuration
#   * one discarded warmup run, then $RUNS measured runs
#   * --csv output (rps, p50, p99 per test) stamped with the git SHA
#   * a median/spread summary printed at the end
#
# Usage:
#   ./scripts/bench.sh                 # 7 runs
#   RUNS=3 ./scripts/bench.sh          # fewer runs
#   PORT=7000 ./scripts/bench.sh       # different port
#
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# --- pinning ---------------------------------------------------------------
# CPUs 1 and 2 are distinct physical cores that share an L3 and are NOT SMT
# siblings. Siblings on this box are N and N+8 -- verify with `lscpu -e` before
# changing these. CPU 0 is avoided because interrupts tend to land there.
#
# The server gets ONE core, which pins the event loop and the AOF worker thread
# together. That understates absolute throughput but it is what the 70dfead
# baseline used, so do not change it without re-baselining everything.
SERVER_CPU=${SERVER_CPU:-1}
BENCH_CPU=${BENCH_CPU:-2}

# --- knobs -----------------------------------------------------------------
PORT=${PORT:-6399}
RUNS=${RUNS:-7}
DISK_AOF=${DISK_AOF:-/tmp/mydis-bench.aof}
TMPFS_AOF=${TMPFS_AOF:-/dev/shm/mydis-bench.aof}
OUT=${OUT:-docs/benchmarks}
BIN=target/release/server

SHA=$(git rev-parse --short HEAD)
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -f "$DISK_AOF" "$TMPFS_AOF"
}
trap cleanup EXIT INT TERM

# --- preflight -------------------------------------------------------------
command -v redis-benchmark >/dev/null || { echo "redis-benchmark not found" >&2; exit 1; }
command -v taskset         >/dev/null || { echo "taskset not found" >&2; exit 1; }

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "WARNING: working tree is dirty -- $SHA does not describe the binary being measured" >&2
fi

governor=$(cat "/sys/devices/system/cpu/cpu${SERVER_CPU}/cpufreq/scaling_governor" 2>/dev/null || echo unknown)
echo "sha:       $SHA"
echo "runs:      $RUNS"
echo "cores:     server=$SERVER_CPU bench=$BENCH_CPU"
echo "governor:  $governor"
[[ "$governor" != "performance" ]] && echo "           (not 'performance' -- the clock will move during runs)"
echo

cargo build --release

# --- helpers ---------------------------------------------------------------

start_server() {
    local aof_path=$1
    rm -f "$aof_path"

    MYDIS_PORT="$PORT" MYDIS_AOF_PATH="$aof_path" \
        taskset -c "$SERVER_CPU" "$BIN" >/dev/null 2>&1 &
    SERVER_PID=$!

    local waited=0
    until (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; do
        sleep 0.05
        waited=$((waited + 1))
        if (( waited > 200 )); then
            echo "server never listened on port $PORT" >&2
            exit 1
        fi
    done
}

stop_server() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
}

# Pull "SET=<rps> GET=<rps>" out of a --csv result file, for live progress.
rps_line() {
    awk -F'","' 'NR > 1 { gsub(/"/, "", $1); printf "%s=%.0f ", $1, $2 }' "$1"
}

# bench_config <label> <aof_path> <category> <suffix> <redis-benchmark args...>
bench_config() {
    local label=$1 aof_path=$2 category=$3 suffix=$4
    shift 4

    local dir="$OUT/$category/$SHA"
    mkdir -p "$dir"
    local base="$dir/bench_${label}${suffix}_${SHA}"

    echo "== ${label}${suffix} ($category): $*"
    start_server "$aof_path"

    taskset -c "$BENCH_CPU" redis-benchmark -p "$PORT" -t set,get "$@" --csv \
        > "${base}_warmup.txt"

    for i in $(seq 1 "$RUNS"); do
        taskset -c "$BENCH_CPU" redis-benchmark -p "$PORT" -t set,get "$@" --csv \
            > "${base}_${i}.txt"
        printf '   run %d: %s\n' "$i" "$(rps_line "${base}_${i}.txt")"
    done

    stop_server
    rm -f "$aof_path"
    echo
}

# summarize <category> <label> <suffix>
summarize() {
    local category=$1 label=$2 suffix=$3
    local dir="$OUT/$category/$SHA"
    local base="bench_${label}${suffix}_${SHA}"

    local test vals
    for test in SET GET; do
        vals=$(grep -h "^\"${test}\"" "${dir}/${base}"_[0-9]*.txt 2>/dev/null \
               | awk -F'","' '{ gsub(/"/, "", $2); print $2 }') || true
        [[ -z "$vals" ]] && continue
        echo "$vals" | sort -n | awk -v t="$test" -v c="${label}${suffix}" '
            { a[NR] = $1 }
            END {
                med = a[int((NR + 1) / 2)]; min = a[1]; max = a[NR]
                printf "  %-14s %-3s  median=%12.0f  min=%12.0f  max=%12.0f  spread=%5.1f%%\n",
                       c, t, med, min, max, (max - min) / med * 100
            }'
    done
}

# --- runs ------------------------------------------------------------------
#
# AOF on tmpfs. fsync is a memory-speed no-op, so the server's own cost is
# visible. This is the configuration that can detect a runtime regression.
# (The "no_aof" label is a legacy misnomer kept for continuity with earlier
# results -- the AOF is fully enabled, it just isn't on a disk.)
bench_config no_aof "$TMPFS_AOF" latency    ""     -n 100000 -c 1
bench_config no_aof "$TMPFS_AOF" throughput "_p16" -n 200000 -c 50 -P 16
bench_config no_aof "$TMPFS_AOF" throughput ""     -n 100000 -c 50

# AOF on disk. SET is fsync-bound at roughly 1k/sec unpipelined, so `-n` is cut
# to keep runs near 20s instead of 100s. GET is under-measured here as a result
# (it finishes in milliseconds) -- the tmpfs runs above are the real GET numbers.
# What matters here is SET: this is the true durable-write throughput, and it is
# the tightest instrument in the set.
bench_config aof "$DISK_AOF" latency    ""     -n 20000  -c 1
bench_config aof "$DISK_AOF" throughput "_p16" -n 200000 -c 50 -P 16
bench_config aof "$DISK_AOF" throughput ""     -n 20000  -c 50

# --- summary ---------------------------------------------------------------
mkdir -p "$OUT"
SUMMARY="$OUT/summary_${SHA}.txt"

{
    echo "======================================================================"
    echo "medians over $RUNS runs @ $SHA"
    echo "governor: $governor   cores: server=$SERVER_CPU bench=$BENCH_CPU"
    echo "======================================================================"
    summarize latency    no_aof ""
    summarize throughput no_aof "_p16"
    summarize throughput no_aof ""
    echo
    summarize latency    aof ""
    summarize throughput aof "_p16"
    summarize throughput aof ""
    echo
    echo "raw csv under $OUT/{latency,throughput}/$SHA/"
} | tee "$SUMMARY"

echo
echo "summary saved to $SUMMARY"
