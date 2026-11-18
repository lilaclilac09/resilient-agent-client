# resilient-agent-client
Resilient network client for coding agents under high concurrency: bounded connection pools, decorrelated-jitter retry budgets, per-origin circuit breakers, and telemetry isolation. Remediation for socket/FD avalanche (EMFILE) under proxy flapping. Motivating case: Cursor (api2/api3/cursorvm).
