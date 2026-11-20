#!/usr/bin/env python3
import argparse
import json
import math
import os
from functools import partial
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from string import Template
from urllib.parse import urlsplit


DIRECTORY = Path(__file__).resolve().parent
TEMPLATE = Template((DIRECTORY / "index.html").read_text(encoding="utf-8"))
DEFAULT_RESULTS = DIRECTORY.parent / "target" / "proxy-latency.json"
PERCENTILES = (
    ("Median (p50)", "p50_ms"),
    ("95th percentile (p95)", "p95_ms"),
    ("99th percentile (p99)", "p99_ms"),
)
COUNTS = (("Successful requests", "samples"), ("Failed requests", "failures"))


def object_field(parent, name):
    value = parent.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object.")
    return value


def text_field(parent, name):
    value = parent.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty text.")
    return value


def read_results(path):
    with path.open(encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError("Results must be a JSON object.")
    text_field(data, "scenario")
    source = object_field(data, "source")
    for field in ("environment", "measured_at", "command"):
        text_field(source, field)
    for name in ("before", "after"):
        series = object_field(data, name)
        text_field(series, "label")
        samples = series.get("samples")
        failures = series.get("failures")
        for field, count in (("samples", samples), ("failures", failures)):
            if type(count) is not int or count < 0:
                raise ValueError(f"{name}.{field} must be a non-negative integer.")
        if samples + failures == 0:
            raise ValueError(f"{name} must include at least one request outcome.")
        latencies = []
        for _, field in PERCENTILES:
            value = series.get(field)
            if not samples:
                if field not in series or value is not None:
                    raise ValueError(f"{name}.{field} must be null when there are no successes.")
            elif type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{name}.{field} must be a finite, non-negative number.")
            latencies.append(value)
        if samples and latencies != sorted(latencies):
            raise ValueError(f"{name} must have p50_ms ≤ p95_ms ≤ p99_ms.")
        if "errors" in series:
            errors = object_field(series, "errors")
            if any(
                not category.strip() or type(count) is not int or count <= 0
                for category, count in errors.items()
            ) or sum(errors.values()) != failures:
                raise ValueError(f"{name}.errors must map categories to positive counts adding up to failures.")
    return data


def metric_row(title, before, after, change="—"):
    return f"<tr><th scope='row'>{escape(title)}</th><td>{before}</td><td>{after}</td><td>{change}</td></tr>"


def comparison_row(title, before, after, value_format, delta_format):
    delta = after - before if before is not None and after is not None else None
    return metric_row(
        title,
        value_format.format(before) if before is not None else "—",
        value_format.format(after) if after is not None else "—",
        delta_format.format(delta) if delta is not None else "—",
    )


def measured_rows(before, after):
    rows = [
        comparison_row(title, before[field], after[field], "{:.3f} ms", "{:+.3f} ms")
        for title, field in PERCENTILES
    ]
    for title, field in COUNTS:
        rows.append(comparison_row(title, before[field], after[field], "{:,}", "{:+,}"))
    rates = [100 * (s["failures"] / (s["samples"] + s["failures"])) for s in (before, after)]
    rows.append(comparison_row("Failure rate", *rates, "{:.2f}%", "{:+.2f} pp"))
    errors = [s.get("errors", {} if not s["failures"] else None) for s in (before, after)]
    for category in sorted(set(errors[0] or {}) | set(errors[1] or {})):
        counts = [e.get(category, 0) if e is not None else None for e in errors]
        rows.append(comparison_row(f"Failed requests: {category}", *counts, "{:,}", "{:+,}"))
    return rows


def render(path):
    rows = [metric_row(title, "—", "—") for title, _ in PERCENTILES + COUNTS]
    rows.append(metric_row("Failure rate", "—", "—"))
    page = {
        "state": "Pending measurement",
        "message": "No measured comparison is available yet. Both columns will populate when the benchmark results are added.",
        "before_label": "Baseline",
        "after_label": "Candidate",
        "source": "",
    }
    try:
        data = read_results(path)
    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, UnicodeError):
        page.update(state="Results unavailable", message="The results file is not valid UTF-8 JSON.")
    except (OSError, OverflowError):
        page.update(state="Results unavailable", message="The results file could not be read or contains an out-of-range number.")
    except ValueError as error:
        page.update(state="Results unavailable", message=escape(str(error)))
    else:
        before, after = data["before"], data["after"]
        rows = measured_rows(before, after)
        source = {name: escape(data["source"][name]) for name in ("environment", "measured_at", "command")}
        page.update(
            state="Measured comparison",
            message=escape(data["scenario"]),
            before_label=escape(before["label"]),
            after_label=escape(after["label"]),
            source=(
                "<section class='source' aria-labelledby='source-title'>"
                "<h2 id='source-title'>Measurement source</h2><dl>"
                f"<dt>Environment</dt><dd>{source['environment']}</dd>"
                f"<dt>Measured at</dt><dd>{source['measured_at']}</dd>"
                f"<dt>Command</dt><dd><code>{source['command']}</code></dd>"
                "</dl></section>"
            ),
        )
    return TEMPLATE.substitute(page, rows="".join(rows)).encode("utf-8")


class DashboardHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, results_path, **kwargs):
        self.results_path = results_path
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if urlsplit(self.path).path != "/":
            self.send_error(404)
            return
        body = render(self.results_path)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def make_server(results_path, port, host="127.0.0.1"):
    return ThreadingHTTPServer(
        (host, port), partial(DashboardHandler, results_path=results_path)
    )


def main():
    parser = argparse.ArgumentParser(description="Serve the local proxy latency dashboard.")
    parser.add_argument("--results", type=Path, default=os.environ.get("RESULTS_PATH", DEFAULT_RESULTS))
    parser.add_argument("--port", type=int, default=os.environ.get("PORT", 8765))
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    args = parser.parse_args()

    with make_server(args.results, args.port, args.host) as server:
        print(f"Listening on http://{args.host}:{server.server_port}/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
