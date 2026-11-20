import errno
import io
import json
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

from data.make_export import build_exports
from serve import DIRECTORY, bundled_paths, main, make_server, read_results, render


class DashboardSmokeTest(unittest.TestCase):
    def check_dashboard(self, results=None, *, configured=False, input_directory=False, raw=None, repeat=False):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private-export.json"
            if input_directory:
                path.mkdir()
            elif raw is not None:
                path.write_bytes(raw)
            elif results is not None:
                path.write_text(json.dumps(results), encoding="utf-8")
            with make_server(path, 0, configured=configured) as server:
                worker = threading.Thread(target=server.serve_forever)
                worker.start()
                try:
                    with urlopen(f"http://127.0.0.1:{server.server_port}/", timeout=5) as response:
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.headers.get_content_type(), "text/html")
                        self.assertEqual(response.headers["Cache-Control"], "no-store")
                        body = response.read().decode("utf-8")
                    if repeat:
                        with urlopen(f"http://127.0.0.1:{server.server_port}/", timeout=5) as response:
                            self.assertEqual(response.read().decode("utf-8"), body)
                finally:
                    server.shutdown()
                    worker.join()
            self.assertNotIn(str(path), body)
            self.assertNotIn(path.name, body)
        self.assertIn(">Before <small>", body)
        self.assertIn(">After <small>", body)
        return body

    def test_pending_without_results(self):
        body = self.check_dashboard()
        self.assertIn("Pending measurement", body)
        self.assertIn("<td role='cell'>—</td><td role='cell'>—</td><td role='cell'>—</td>", body)
        self.assertIn("Expected input: <code>target/proxy-latency.json</code>", body)
        for title in ("99th percentile (p99)", "Failed requests", "Failure rate"):
            self.assert_metric(body, title, "—", "—", "—")
        self.assertNotIn("Measurement source", body)
        self.assertIn('data-revision=""', body)

    def test_measured_export_displays_both_series_and_provenance(self):
        body = self.check_dashboard({
            "scenario": "Smoke test fixture",
            "source": {
                "environment": "local <fixture>",
                "measured_at": "2026-11-20T00:00:00Z",
                "command": "test export",
            },
            "before": {
                "label": "baseline", "samples": 24, "p50_ms": 4.5, "p95_ms": 7,
                "p99_ms": 10, "failures": 6, "errors": {"timeout <proxy>": 6},
            },
            "after": {
                "label": "candidate", "samples": 27, "p50_ms": 3, "p95_ms": 8,
                "p99_ms": 9, "failures": 3, "errors": {"refused": 1, "timeout <proxy>": 2},
            },
        }, repeat=True)
        self.assertIn("Measured comparison", body)
        self.assert_metric(body, "Median (p50)", "4.500 ms", "3.000 ms", "Observed: faster (-1.500 ms)")
        self.assert_metric(body, "95th percentile (p95)", "7.000 ms", "8.000 ms", "Observed: slower (+1.000 ms)")
        self.assert_metric(body, "99th percentile (p99)", "10.000 ms", "9.000 ms", "Observed: faster (-1.000 ms)")
        self.assert_metric(body, "Successful requests", "24", "27", "Observed: more successes (+3)")
        self.assert_metric(body, "Failed requests", "6", "3", "Observed: fewer failures (-3)")
        self.assert_metric(body, "Failure rate", "20.00%", "10.00%", "Observed: lower failure rate (-10.00 pp)")
        self.assert_metric(body, "Failed requests: timeout &lt;proxy&gt;", "6", "2", "Observed: fewer failures (-4)")
        self.assert_metric(body, "Failed requests: refused", "0", "1", "Observed: more failures (+1)")
        self.assertIn("local &lt;fixture&gt;", body)
        self.assertIn("test export", body)
        self.assertIn("<dt>Applies to</dt><dd>Both runs (Before and After)</dd>", body)
        self.assertRegex(body, r'data-revision="[a-f0-9]{64}"')
        self.assertIn("BaoLianDeng setup guide for macOS", body)

    def test_all_failed_config_has_rate_but_no_success_latency(self):
        body = self.check_dashboard({
            "scenario": "Smoke test fixture",
            "source": {
                "environment": "local fixture",
                "measured_at": "2026-11-20T00:00:00Z",
                "command": "test export",
                "scope": "after",
            },
            "before": {
                "label": "baseline", "samples": 0, "failures": 2,
                "p50_ms": None, "p95_ms": None, "p99_ms": None,
            },
            "after": {
                "label": "candidate", "samples": 1, "failures": 1,
                "p50_ms": 3, "p95_ms": 3, "p99_ms": 3, "errors": {"timeout": 1},
            },
        })
        self.assert_metric(body, "99th percentile (p99)", "—", "3.000 ms", "Not comparable")
        self.assert_metric(body, "Failure rate", "100.00%", "50.00%", "Observed: lower failure rate (-50.00 pp)")
        self.assert_metric(body, "Failed requests: timeout", "—", "1", "Not comparable")
        self.assertIn("<dt>Applies to</dt><dd>After run only</dd>", body)

    def test_invalid_export_does_not_display_measurements(self):
        body = self.check_dashboard({"scenario": "Incomplete export", "source": {"environment": ""}})
        self.assertIn("Results unavailable", body)
        self.assertIn("<td role='cell'>—</td><td role='cell'>—</td><td role='cell'>—</td>", body)
        self.assertNotIn("Measurement source", body)
        self.assertIn("Set source.environment to a non-empty string.", body)
        self.assertIn("format in README.md", body)

    def test_missing_configured_file_is_not_pending(self):
        body = self.check_dashboard(configured=True)
        self.assertIn("Results file missing", body)
        self.assertNotIn("Pending measurement", body)
        self.assertIn("No results file at the configured input.", body)
        self.assertIn("Expected input: <code>Configured export (RESULTS_PATH or --results)</code>", body)
        self.assertIn("restart with the correct RESULTS_PATH or --results", body)

    def test_directory_input_has_a_specific_read_error(self):
        body = self.check_dashboard(configured=True, input_directory=True)
        self.assertIn("The expected input is a directory. Select a JSON results file instead.", body)
        self.assertIn("format in README.md", body)

    def test_permission_failure_does_not_print_os_paths(self):
        error = PermissionError(errno.EACCES, "Permission denied", "/secret/server/input.json")
        with patch.object(Path, "open", side_effect=error):
            body = self.check_dashboard(configured=True)
        self.assertIn("Permission denied while reading the expected input.", body)
        self.assertIn("Grant the dashboard process read access", body)
        self.assertNotIn("/secret/server/input.json", body)

    def test_json_syntax_error_points_to_line_and_column(self):
        body = self.check_dashboard(configured=True, raw=b"{\n?")
        self.assertIn("Fix the JSON syntax at line 2, column 1", body)
        self.assertIn("format in README.md", body)

    def test_out_of_range_latency_identifies_the_field(self):
        body = self.check_dashboard({
            "scenario": "Smoke test fixture",
            "source": {"environment": "fixture", "measured_at": "test time", "command": "test export"},
            "before": {
                "label": "baseline", "samples": 1, "failures": 0,
                "p50_ms": 1, "p95_ms": 1, "p99_ms": 10 ** 400,
            },
        }, configured=True)
        self.assertIn("Re-export before.p99_ms as a supported finite number in milliseconds.", body)
        self.assertNotIn("Could not read", body)

    def test_startup_logs_the_actual_input_path_and_uses_explicit_config(self):
        for env, argv in (
            ({"RESULTS_PATH": "/private/results.json", "HOST": "0.0.0.0", "PORT": "8123"}, ["serve.py"]),
            (
                {"RESULTS_PATH": "/unused/results.json", "HOST": "127.0.0.1", "PORT": "8011"},
                ["serve.py", "--results", "/private/results.json", "--host", "0.0.0.0", "--port", "8123"],
            ),
        ):
            with self.subTest(argv=argv), patch.dict("os.environ", env, clear=True), patch("sys.argv", argv):
                with patch("serve.make_server") as make_server_mock, redirect_stdout(io.StringIO()) as output:
                    make_server_mock.return_value.__enter__.return_value.server_port = 8123
                    main()
                make_server_mock.assert_called_once_with(Path("/private/results.json"), 8123, "0.0.0.0", configured=True)
                self.assertIn("Listening on http://0.0.0.0:8123/", output.getvalue())
                self.assertIn("Reading /private/results.json", output.getvalue())

    def assert_metric(self, body, title, before, after, change):
        cells = "".join(f"<td role='cell'>{value}</td>" for value in (before, after, change))
        self.assertIn(f">{title}</th>{cells}</tr>", body)

    def test_bundled_exports_match_recorded_trials_and_render(self):
        records = [
            json.loads(line)
            for line in (DIRECTORY / "data" / "raw-samples.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(len(records), 20)
        for path, (_, expected) in zip(
            bundled_paths(), build_exports(records, "b966dbb", "86d382a", "2026-11-20T07:09:32+00:00"), strict=True,
        ):
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), expected)
            loaded = read_results(path)
            for side in ("before", "after"):
                self.assertEqual(loaded[side]["failures"], expected[side]["errors"])
                self.assertNotIn("errors", loaded[side])
        body = render().decode("utf-8")
        self.assertIn('<h2 id="steady-title">Steady proxy</h2>', body)
        self.assertIn('<h2 id="flap-title">Flapping proxy</h2>', body)
        self.assertIn("Local loopback benchmark — not production", body)
        self.assertIn("No demonstrated speedup.", body)
        self.assertIn("Compare request latency for the baseline and candidate client revisions.", body)
        self.assert_metric(body, "95th percentile (p95)", "282.078 ms", "280.909 ms", "Observed: faster (-1.169 ms)")
        self.assert_metric(body, "95th percentile (p95)", "239.992 ms", "241.767 ms", "Observed: slower (+1.775 ms)")
        self.assert_metric(body, "Failed requests", "0", "0", "Observed: no change")
        self.assert_metric(body, "Failed requests", "14,542", "13,132", "Observed: fewer failures (-1,410)")
        self.assert_metric(body, "Failure rate", "81.51%", "77.14%", "Observed: lower failure rate (-4.38 pp)")
        self.assert_metric(body, "Fail-fast rejections", "Not recorded", "Not recorded", "Not comparable")
        self.assert_metric(body, "Max observed recovery", "1,813 ms", "1,874 ms", "Observed: slower (+61 ms)")

    def test_offline_snapshot_matches_bundled_dashboard_without_refresh_link(self):
        snapshot = (DIRECTORY / "proxy-latency-offline.html").read_bytes()
        self.assertEqual(snapshot, render(offline=True))
        self.assertNotIn(b'id="refresh-results"', snapshot)
        self.assertIn(b"Offline snapshot", snapshot)
