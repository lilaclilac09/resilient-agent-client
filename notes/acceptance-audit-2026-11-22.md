# Acceptance audit — 2026-11-22

Acceptance criteria come only from this repository's [ACCEPTANCE.md](../ACCEPTANCE.md). This is a final audit of the documented baseline and the current branch, not production acceptance.

## Findings and disposition

| Item | Source rule | Baseline evidence | This PR / remaining limitation |
|---|---|---|---|
| Missing evidence closers on PRs #2–#8 | §8 | Current checker rejects the historical bodies; #9 passes | Not fixable after merge by this branch; preserve the historical gap and provide a complete closer on the new PR. |
| Dashboard PR #7 lacks delivered 390×844 stills and interaction proof | §§2,4 | Its body describes 375/320px checks but supplies neither required artifact | Fixed prospectively: Chromium screenshots of current dashboard, steady/flap views and offline snapshot, plus animated WebP of scenario navigation and unchanged refresh. Original merge-time evidence remains incomplete. |
| No explicit product status / next action | §5 | README had loopback results and a Status heading, not the required status line | Fixed: root and harness READMEs state LOCAL ONLY and concrete next human actions; no operator/production validation claimed. |

## Existing work and scope

Baseline `6f5bc489783eb3210a64f4b93e6392eecf3e52f6`; all returned PRs #2–#9 were merged and their merge SHAs passed ancestry checks against origin/main. Work includes Rust resilience modules, MCP tool, proxy diagnostics, framing support, CA-fixture isolation and local benchmark dashboard. Docs reviewed: README, SPEC, PROVENANCE, MIHOMO, RSI-REINFORCEMENT, CURSOR and BAOLIANDENG. PR #1 was not returned; no assumption about it.

Dashboard results explicitly state no demonstrated speedup and local loopback, not production. GitHub deployment listing was empty and Pages returned HTTP 404; this does not exclude external hosting. No actual BaoLianDeng, signed-in Cursor or native-macOS verification was performed.

## Current verification

- `cargo test --locked --workspace`: 80 passed, 0 failed across workspace test/doc-test result lines; log retained outside the repository at `/home/ubuntu/audits/resilient-cargo-test.log`.
- `cargo fmt --check`: exit 0. `cargo clippy --locked --workspace --all-targets -- -D warnings`: exit 0, `Finished dev profile` (full output in `/home/ubuntu/audits/resilient-clippy.log`).
- `python3 -B -m unittest discover -s dashboard -v`: `Ran 12 tests in 4.546s` / `OK`; includes pending/missing/invalid-data rendering cases. Those states were not separately browser-captured.
- Playwright Chromium at exactly 390×844: current dashboard → steady → flap → refresh; asserted `No change since last refresh`. Offline page captured separately. Stills and `interaction.webp`: `/home/ubuntu/audits/shots/resilient-agent-client/`, embedded in the PR body.
- Final PR body checked with `python scripts/check_acceptance.py <body>`; output recorded in the PR body. No new library/UI feature or dependency added.

## Coverage of all acceptance sections

- §1: this branch inspected existing work before scoped documentation edits and fresh verification; historical approval/inspection chronology is not assessable from PR bodies alone.
- §2: executed checks and artifacts above; original benchmark measurements are not independently reproduced. No source-inspection-only correctness claim.
- §3: no new bug fix here; historical #5 framing and #8 CA collision describe repro/root cause, but full chronology is not assessable.
- §4: current stills and interaction artifact delivered prospectively; historical absence remains.
- §5: LOCAL ONLY; no deployment, production fix or completed merge claimed. Merge ancestry was checked for returned historical PRs; owner approval history is not established.
- §6: repository acceptance pointers and README commands reviewed. Historical dated test/benchmark numbers are not automatically new verification.
- §7: operator validation remains outside scope, not silently bypassed. No new stuck execution is represented as successful.
- §8: historical failures retained; new PR supplies the required eight-field closer.

Not run: release-profile tests, standalone framing/trip/Mihomo harnesses, real proxy/operator integration, production deployment or native-macOS UI. These documentation changes do not establish those flows as accepted.
