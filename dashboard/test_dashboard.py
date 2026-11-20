import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import urlopen

from serve import make_server


class DashboardSmokeTest(unittest.TestCase):
    def check_dashboard(self, results=None):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.json"
            if results is not None:
                path.write_text(json.dumps(results), encoding="utf-8")
            with make_server(path, 0) as server:
                worker = threading.Thread(target=server.serve_forever)
                worker.start()
                try:
                    with urlopen(f"http://127.0.0.1:{server.server_port}/", timeout=5) as response:
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.headers.get_content_type(), "text/html")
                        self.assertEqual(response.headers["Cache-Control"], "no-store")
                        body = response.read().decode("utf-8")
                finally:
                    server.shutdown()
                    worker.join()
        self.assertIn(">Before<", body)
        self.assertIn(">After<", body)
        return body

    def test_pending_without_results(self):
        body = self.check_dashboard()
        self.assertIn("Pending measurement", body)
        self.assertIn("<td>—</td><td>—</td><td>—</td>", body)
        for title in ("99th percentile (p99)", "Failed requests", "Failure rate"):
            self.assertIn(f">{title}</th><td>—</td><td>—</td><td>—</td>", body)
        self.assertNotIn("Measurement source", body)

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
        })
        self.assertIn("Measured comparison", body)
        self.assertIn("<td>4.500 ms</td><td>3.000 ms</td><td>-1.500 ms</td>", body)
        self.assertIn("<td>7.000 ms</td><td>8.000 ms</td><td>+1.000 ms</td>", body)
        self.assertIn(">99th percentile (p99)</th><td>10.000 ms</td><td>9.000 ms</td><td>-1.000 ms</td>", body)
        self.assertIn(">Failed requests</th><td>6</td><td>3</td><td>-3</td>", body)
        self.assertIn(">Failure rate</th><td>20.00%</td><td>10.00%</td><td>-10.00 pp</td>", body)
        self.assertIn(">Failed requests: timeout &lt;proxy&gt;</th><td>6</td><td>2</td><td>-4</td>", body)
        self.assertIn(">Failed requests: refused</th><td>0</td><td>1</td><td>+1</td>", body)
        self.assertIn("local &lt;fixture&gt;", body)
        self.assertIn("test export", body)

    def test_all_failed_config_has_rate_but_no_success_latency(self):
        body = self.check_dashboard({
            "scenario": "Smoke test fixture",
            "source": {
                "environment": "local fixture",
                "measured_at": "2026-11-20T00:00:00Z",
                "command": "test export",
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
        self.assertIn(">99th percentile (p99)</th><td>—</td><td>3.000 ms</td><td>—</td>", body)
        self.assertIn(">Failure rate</th><td>100.00%</td><td>50.00%</td><td>-50.00 pp</td>", body)
        self.assertIn(">Failed requests: timeout</th><td>—</td><td>1</td><td>—</td>", body)

    def test_invalid_export_does_not_display_measurements(self):
        body = self.check_dashboard({"scenario": "Incomplete export"})
        self.assertIn("Results unavailable", body)
        self.assertIn("<td>—</td><td>—</td><td>—</td>", body)
        self.assertNotIn("Measurement source", body)
