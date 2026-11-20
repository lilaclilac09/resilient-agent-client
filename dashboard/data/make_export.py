import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


DESCRIPTIONS = {
    "steady": "24 agents x 10 s, Client::send GET through a loopback HTTP CONNECT proxy fixture to a loopback keep-alive upstream; 5 interleaved reps per build; latency = per successful request wall time; values are the median across reps",
    "flap": "Same workload, proxy killed for 1 s every 3 s after a 2 s warmup; latency only over successful requests; errors include local breaker fail-fast rejections",
}


def build_exports(records, baseline, candidate, measured_at):
    for scenario, description in DESCRIPTIONS.items():
        export = {
            "scenario": description,
            "source": {
                "environment": "Linux VM, loopback fixture proxy (harness/loopback-fixtures ConnectProxy), not mihomo/BaoLianDeng, no external network",
                "measured_at": measured_at,
                "command": f"bench-<tree> {scenario} 24 10 (standalone crate path-depending on agent-client + loopback-fixtures; source in bench-main.rs)",
            },
        }
        for side, tree, label in (
            ("before", "basemain", f"main {baseline}"),
            ("after", "cand", f"candidate #4+#5+#6 {candidate}"),
        ):
            trials = [r["result"] for r in records if r["tree"] == tree and r["result"]["scenario"] == scenario]
            successes = sum(r["ok"] for r in trials)
            export[side] = {
                "label": label,
                "samples": successes,
                **{
                    f"p{p}_ms": round(median(r[f"p{p}_us"] for r in trials) / 1000, 3)
                    for p in (50, 95, 99)
                },
                "success": successes,
                "errors": sum(r["errors"] for r in trials),
                "reps": len(trials),
                "max_recovery_ms": max(r["recovery_ms_max"] for r in trials),
            }
        yield scenario, export


def main():
    parser = argparse.ArgumentParser(description="Aggregate recorded loopback benchmark trial summaries.")
    parser.add_argument("samples", type=Path)
    parser.add_argument("output_directory", type=Path)
    parser.add_argument("baseline")
    parser.add_argument("candidate")
    parser.add_argument("--measured-at", default=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    args = parser.parse_args()
    records = [json.loads(line) for line in args.samples.read_text(encoding="utf-8").splitlines()]
    for scenario, export in build_exports(records, args.baseline, args.candidate, args.measured_at):
        (args.output_directory / f"proxy-latency-{scenario}.json").write_text(
            json.dumps(export, indent=2), encoding="utf-8"
        )


if __name__ == "__main__":
    main()
