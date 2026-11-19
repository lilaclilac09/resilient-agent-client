# RSI and reinforcement: what applies here

"RSI" (recursive self-improvement) and reinforcement learning are used
here only in a bounded, defensive sense: a measure → change → re-measure
loop run by people or a reviewing agent against local fixtures. Nothing in
this repository modifies its own code, learns online in production, or
generates traffic against real services.

## The loop that exists today

1. **Signal.** `multi-agent-proxy` and `run-trip-test.sh` emit numbers
   that map to SPEC §5: peak sockets per origin, retries vs ceiling,
   breaker trips, local rejections, FD start/end, recovery time.
2. **Reward.** A run passes only if every bound holds; the exit code is
   the reward signal. Regressions fail the run.
3. **Change.** A person (or an agent opening a PR) adjusts a constant or
   policy.
4. **Re-measure.** The same bounded run is repeated and both numbers go
   in the PR.

## Safe next steps

| Idea | Value | Guard rails |
|---|---|---|
| Offline parameter sweep of `max_per_origin`, breaker threshold, backoff base/cap against the loopback harness | Picks constants from data instead of guesses | Loopback only, bounded duration, results reviewed in a PR |
| Adaptive concurrency (AIMD on pool cap from observed latency/errors) | Fewer queue waits when the proxy is healthy | Hard ceiling stays at SPEC §4.1 caps; adaptation can only lower them |
| Bandit selection between preset retry profiles per origin | Different origins tolerate different pressure | Profiles all inside the retry budget; never more retries than the budget |
| Keep a findings log of what was tried and the measured effect | Stops repeating failed changes | Human-readable, versioned in git |

## Not done, on purpose

- No self-modifying code or automatic merges.
- No learning from live third-party traffic, no stress tests against real
  hosts, no proxy bypass.
- Any learned value is clamped to the SPEC limits, so the safety bounds
  can't be tuned away.
