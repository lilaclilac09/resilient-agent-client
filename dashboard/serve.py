#!/usr/bin/env python3
import argparse
import json
import math
from functools import partial
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from string import Template


DIRECTORY = Path(__file__).resolve().parent
TEMPLATE = Template((DIRECTORY / "index.html").read_text(encoding="utf-8"))
DEFAULT_RESULTS = DIRECTORY.parent / "target" / "proxy-latency.json"


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
        if type(samples) is not int or samples <= 0:
            raise ValueError(f"{name}.samples must be a positive integer.")
        for field in ("p50_ms", "p95_ms"):
            value = series.get(field)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{name}.{field} must be a finite, non-negative number.")
        if series["p50_ms"] > series["p95_ms"]:
            raise ValueError(f"{name}.p50_ms must not exceed {name}.p95_ms.")
    return data


def metric_row(title, before, after, change="—"):
    return f"<tr><th scope='row'>{title}</th><td>{before}</td><td>{after}</td><td>{change}</td></tr>"


def render(path):
    rows = [
        metric_row("Median (p50)", "—", "—"),
        metric_row("95th percentile (p95)", "—", "—"),
        metric_row("Measured requests", "—", "—"),
    ]
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
        rows = []
        for title, field in (("Median (p50)", "p50_ms"), ("95th percentile (p95)", "p95_ms")):
            a, b = before[field], after[field]
            delta = b - a
            change = f"{delta:+.3f} ms" if delta else "0.000 ms"
            rows.append(metric_row(title, f"{a:.3f} ms", f"{b:.3f} ms", change))
        rows.append(metric_row("Measured requests", f"{before['samples']:,}", f"{after['samples']:,}"))
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
        if self.path != "/":
            self.send_error(404)
            return
        body = render(self.results_path)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def make_server(results_path, port):
    return ThreadingHTTPServer(
        ("127.0.0.1", port), partial(DashboardHandler, results_path=results_path)
    )


def main():
    parser = argparse.ArgumentParser(description="Serve the local proxy latency dashboard.")
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    with make_server(args.results, args.port) as server:
        print(f"Open http://127.0.0.1:{server.server_port}/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
