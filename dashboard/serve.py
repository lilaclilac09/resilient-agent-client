#!/usr/bin/env python3
import argparse
import errno
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
DEFAULT_INPUT_LABEL = "target/proxy-latency.json"
CONFIGURED_INPUT_LABEL = "Configured export (RESULTS_PATH or --results)"
DEFAULT_RESULTS = DIRECTORY.parent / DEFAULT_INPUT_LABEL
FORMAT_HELP = "Follow the Before / after proxy latency dashboard format in README.md, then refresh."
PERCENTILES = (
    ("Median (p50)", "p50_ms"),
    ("95th percentile (p95)", "p95_ms"),
    ("99th percentile (p99)", "p99_ms"),
)
COUNTS = (("Successful requests", "samples"), ("Failed requests", "failures"))


class ResultsValidationError(ValueError):
    pass


def object_field(parent, name, prefix=""):
    value = parent.get(name)
    if not isinstance(value, dict):
        raise ResultsValidationError(f"Set {prefix}{name} to a JSON object.")
    return value


def text_field(parent, name, prefix=""):
    value = parent.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ResultsValidationError(f"Set {prefix}{name} to a non-empty string.")
    return value


def read_results(path):
    with path.open(encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ResultsValidationError("Wrap the results in a top-level JSON object.")
    text_field(data, "scenario")
    source = object_field(data, "source")
    for field in ("environment", "measured_at", "command"):
        text_field(source, field, "source.")
    for name in ("before", "after"):
        series = object_field(data, name)
        text_field(series, "label", f"{name}.")
        samples = series.get("samples")
        failures = series.get("failures")
        for field, count in (("samples", samples), ("failures", failures)):
            if type(count) is not int or count < 0:
                raise ResultsValidationError(f"Set {name}.{field} to a non-negative integer.")
        if samples + failures == 0:
            raise ResultsValidationError(f"Include at least one request outcome in {name}.samples or {name}.failures.")
        latencies = []
        for _, field in PERCENTILES:
            value = series.get(field)
            if not samples:
                if field not in series or value is not None:
                    raise ResultsValidationError(f"Set {name}.{field} to null when {name}.samples is zero.")
            else:
                try:
                    valid = type(value) in (int, float) and math.isfinite(value) and value >= 0
                except OverflowError:
                    raise ResultsValidationError(f"Re-export {name}.{field} as a supported finite number in milliseconds.") from None
                if not valid:
                    raise ResultsValidationError(f"Set {name}.{field} to a finite, non-negative number in milliseconds.")
            latencies.append(value)
        if samples and latencies != sorted(latencies):
            raise ResultsValidationError(f"Put the percentiles in order: {name}.p50_ms ≤ {name}.p95_ms ≤ {name}.p99_ms.")
        if "errors" in series:
            errors = object_field(series, "errors", f"{name}.")
            if any(
                not category.strip() or type(count) is not int or count <= 0
                for category, count in errors.items()
            ) or sum(errors.values()) != failures:
                raise ResultsValidationError(f"Set {name}.errors to category counts adding up to {name}.failures, or omit it.")
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


def read_error_message(error):
    if error.errno == errno.EISDIR:
        return "The expected input is a directory. Select a JSON results file instead."
    if error.errno in (errno.EACCES, errno.EPERM):
        return "Permission denied while reading the expected input. Grant the dashboard process read access to the export and access to its parent directories."
    reason = os.strerror(error.errno) if error.errno is not None else "Unknown OS error"
    return f"Could not read the expected input ({reason}). Check that the export is a readable JSON file."


def render(path, configured=False):
    rows = [metric_row(title, "—", "—") for title, _ in PERCENTILES + COUNTS]
    rows.append(metric_row("Failure rate", "—", "—"))
    page = {
        "state": "Pending measurement",
        "message": "No measured comparison is available yet. Both columns will populate when the benchmark results are added.",
        "before_label": "Baseline",
        "after_label": "Candidate",
        "source": "",
        "input_label": CONFIGURED_INPUT_LABEL if configured else DEFAULT_INPUT_LABEL,
        "help": "",
    }
    try:
        data = read_results(path)
    except FileNotFoundError:
        if configured:
            page.update(
                state="Results file missing",
                message="No results file at the configured input. Add the export or restart with the correct RESULTS_PATH or --results.",
                help=FORMAT_HELP,
            )
    except json.JSONDecodeError as error:
        page.update(
            state="Results unavailable",
            message=f"Fix the JSON syntax at line {error.lineno}, column {error.colno} of the expected input.",
            help=FORMAT_HELP,
        )
    except UnicodeError:
        page.update(state="Results unavailable", message="Save the expected input as UTF-8 JSON.", help=FORMAT_HELP)
    except OSError as error:
        page.update(state="Results unavailable", message=escape(read_error_message(error)), help=FORMAT_HELP)
    except ResultsValidationError as error:
        page.update(state="Results unavailable", message=escape(str(error)), help=FORMAT_HELP)
    except (ValueError, OverflowError):
        page.update(state="Results unavailable", message="Re-export the expected input with supported JSON values.", help=FORMAT_HELP)
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
    def __init__(self, *args, results_path, configured, **kwargs):
        self.results_path = results_path
        self.configured = configured
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if urlsplit(self.path).path != "/":
            self.send_error(404)
            return
        body = render(self.results_path, self.configured)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def make_server(results_path, port, host="127.0.0.1", *, configured=False):
    return ThreadingHTTPServer(
        (host, port), partial(DashboardHandler, results_path=results_path, configured=configured)
    )


def main():
    parser = argparse.ArgumentParser(description="Serve the local proxy latency dashboard.")
    parser.add_argument("--results", type=Path, default=os.environ.get("RESULTS_PATH"))
    parser.add_argument("--port", type=int, default=os.environ.get("PORT", 8765))
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    args = parser.parse_args()

    path = args.results if args.results is not None else DEFAULT_RESULTS
    with make_server(path, args.port, args.host, configured=args.results is not None) as server:
        print(f"Listening on http://{args.host}:{server.server_port}/", flush=True)
        print(f"Reading {path.absolute()}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
