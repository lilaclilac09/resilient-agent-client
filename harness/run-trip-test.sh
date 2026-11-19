#!/usr/bin/env bash
# End-to-end circuit-breaker trip test (SPEC §3).
#
# Runs proxy-flapper (fault injection + breaker-gated client) while
# fd-monitor.sh samples the flapper's FD count OUT OF PROCESS, then
# asserts: the breaker tripped, work was rejected with zero socket cost
# while Open, the breaker closed again after recovery, and the flapper's
# FD count stayed bounded and returned to baseline.
#
# Neither this script nor fd-monitor.sh opens any network socket, so the
# test runner itself cannot leak FDs into the measurement.
#
# Usage: ./harness/run-trip-test.sh [duration_secs]
# Outputs: harness/trip-test-output/flapper.log, fd.csv
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/harness/trip-test-output"
mkdir -p "$OUT"

PORT="${PORT:-18099}"
DURATION="${1:-60}"

export PATH="$HOME/.cargo/bin:$PATH"
cd "$ROOT"
cargo build --quiet

FLAPPER_LOG="$OUT/flapper.log"
FD_CSV="$OUT/fd.csv"

"$ROOT/target/debug/proxy-flapper" --port "$PORT" --duration-secs "$DURATION" >"$FLAPPER_LOG" 2>&1 &
FLAPPER_PID=$!

./harness/fd-monitor/fd-monitor.sh "$FLAPPER_PID" 1 $((DURATION + 30)) "$FD_CSV" &
MONITOR_PID=$!

FLAPPER_RC=0
wait "$FLAPPER_PID" || FLAPPER_RC=$?
wait "$MONITOR_PID" || true

# --- assertions on the external FD measurement ---
# CSV columns: timestamp,fd_count,tcp_total,tcp_established,tcp_time_wait,tcp_close_wait,tcp_syn_sent
MAX_FD="$(awk -F, 'NR>1 && $2 ~ /^[0-9]+$/ { if ($2 > m) m=$2 } END { print m+0 }' "$FD_CSV")"
BASE_FD="$(awk -F, 'NR>1 && $2 ~ /^[0-9]+$/ { print $2; exit }' "$FD_CSV")"
LAST_FD="$(awk -F, 'NR>1 && $2 ~ /^[0-9]+$/ { last=$2 } END { print last+0 }' "$FD_CSV")"
BASE_FD="${BASE_FD:-0}"
MAX_FD="${MAX_FD:-0}"
LAST_FD="${LAST_FD:-0}"

PASS=1
[ "$FLAPPER_RC" -eq 0 ] || PASS=0
grep -q "result: PASS" "$FLAPPER_LOG" || PASS=0
# The client holds at most one socket at a time; a leak would grow the
# count by hundreds over the run. Allow a generous +32 over baseline.
[ "$MAX_FD" -le $((BASE_FD + 32)) ] || PASS=0
[ "$LAST_FD" -le $((BASE_FD + 4)) ] || PASS=0

echo "=== TRIP TEST VERDICT ==="
echo "flapper_exit: $FLAPPER_RC"
echo "baseline_fd: $BASE_FD"
echo "max_fd: $MAX_FD"
echo "final_fd: $LAST_FD"
grep -E "^(attempts|successes|network_failures|other_errors|rejected_while_open|trips|final_state|max_self_fd):" "$FLAPPER_LOG" || true
if [ "$PASS" -eq 1 ]; then
    echo "result: PASS"
else
    echo "result: FAIL"
    echo "flapper log: $FLAPPER_LOG"
    echo "fd csv: $FD_CSV"
    exit 1
fi
