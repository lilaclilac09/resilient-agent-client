#!/usr/bin/env bash
# fd-monitor: sample open FD count and TCP socket states for a PID.
# Emits CSV rows: timestamp_utc,fd_count,tcp_total,tcp_established,tcp_time_wait,tcp_close_wait,tcp_syn_sent
# Usage: ./fd-monitor.sh <pid> [interval_seconds] [samples] [output.csv]
set -euo pipefail

pid="${1:?usage: fd-monitor.sh <pid> [interval_seconds] [samples] [output.csv]}"
interval="${2:-1}"
samples="${3:-10}"
outfile="${4:-}"

header="timestamp_utc,fd_count,tcp_total,tcp_established,tcp_time_wait,tcp_close_wait,tcp_syn_sent"

emit() {
    if [ -n "$outfile" ]; then
        printf '%s\n' "$1" >> "$outfile"
    else
        printf '%s\n' "$1"
    fi
}

if [ -n "$outfile" ]; then
    printf '%s\n' "$header" > "$outfile"
else
    printf '%s\n' "$header"
fi

for _ in $(seq 1 "$samples"); do
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [ ! -d "/proc/$pid" ]; then
        emit "$ts,PID_GONE,NA,NA,NA,NA,NA"
        exit 1
    fi
    if command -v lsof >/dev/null 2>&1; then
        fd_count="$(lsof -p "$pid" 2>/dev/null | wc -l)"
    else
        fd_count="$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)"
    fi
    fd_count="${fd_count//[[:space:]]/}"

    tcp_total="NA"; tcp_est="NA"; tcp_tw="NA"; tcp_cw="NA"; tcp_syn="NA"
    if command -v ss >/dev/null 2>&1; then
        stats="$(ss -tanp 2>/dev/null | awk -v p="pid=$pid," '
            $0 ~ p {
                total++
                if ($1 == "ESTAB") est++
                else if ($1 == "TIME-WAIT") tw++
                else if ($1 == "CLOSE-WAIT") cw++
                else if ($1 == "SYN-SENT") syn++
            }
            END { printf "%d,%d,%d,%d,%d", total+0, est+0, tw+0, cw+0, syn+0 }')"
        IFS=, read -r tcp_total tcp_est tcp_tw tcp_cw tcp_syn <<< "$stats"
    fi

    emit "$ts,$fd_count,$tcp_total,$tcp_est,$tcp_tw,$tcp_cw,$tcp_syn"
    sleep "$interval"
done
