# fd-monitor

Status: LOCAL ONLY — local diagnostic script, not a deployed monitoring service; next human action: choose a local test process and inspect its CSV output during a controlled harness run.

Monitors a local process's open file descriptors and TCP socket states
while a fault-injection run is in progress, and emits CSV rows for
charting (see `docs/SPEC.md`, section 3).

Usage:

```sh
./fd-monitor.sh <pid> [interval_seconds] [samples] [output.csv]
```

Each row records a UTC timestamp, the FD count for `<pid>`
(`lsof -p <pid> | wc -l`, falling back to `/proc/<pid>/fd` when `lsof`
is unavailable), and the count of TCP sockets by state
(`ESTABLISHED`, `TIME-WAIT`, `CLOSE-WAIT`, `SYN-SENT`, plus total).
Plot the CSV to verify that FD usage stays bounded under flapping.

If `<pid>` exits mid-run, the script emits a `PID_GONE` row and exits
with status 1.
