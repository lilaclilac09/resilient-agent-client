# proxy-flapper

Status: LOCAL ONLY — local fault-injection harness, not a production proxy; next human action: run the controlled loopback trip test before using its results in an operator integration assessment.

`src/main.rs` kills and restarts a local fault server while a breaker-gated client connects. From the repository root, run `bash harness/run-trip-test.sh`; this checks trip/reject/recovery behavior and samples file descriptors. See `docs/SPEC.md` §3 for the fault scenario. This is not a BaoLianDeng or production incident reproduction.
