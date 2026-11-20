#!/usr/bin/env bash
# Bounded loopback test against a real mihomo binary (never installed system-wide,
# never set as the system proxy). Usage: MIHOMO_BIN=/path/to/mihomo harness/mihomo/run-mihomo-e2e.sh [agents] [steady_secs] [restart]
set -euo pipefail
cd "$(dirname "$0")/../.."
BIN="${MIHOMO_BIN:-$(command -v mihomo || true)}"
[ -x "$BIN" ] || { echo "set MIHOMO_BIN to a mihomo binary (e.g. from github.com/MetaCubeX/mihomo/releases)"; exit 2; }
cargo build -q --release -p mihomo-e2e
cargo build -q --release -p agent-client --example fetch
export FETCH_EXAMPLE="$PWD/target/release/examples/fetch"
cargo build -q --release -p agent-fetch-mcp
export MCP_SERVER="$PWD/target/release/agent-fetch-mcp"
exec timeout 180 ./target/release/mihomo-e2e "$BIN" "${1:-8}" "${2:-15}" "${3:-1}"
