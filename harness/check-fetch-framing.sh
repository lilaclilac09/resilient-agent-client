#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
failed=0
for case in chunked_with_extensions_trailers_and_reuse close_delimited content_length chunked_over_proxy body_limits_apply_to_every_framing empty_bodies malformed_or_truncated_bodies_fail no_body_responses_leave_connection_in_sync; do
    if cargo test -p agent-client --test body_framing "$case" -- --exact; then
        echo "PASS: $case"
    else
        echo "FAIL: $case"
        failed=1
    fi
done
exit "$failed"
