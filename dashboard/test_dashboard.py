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
        self.assertNotIn("Measurement source", body)

    def test_measured_export_displays_both_series_and_provenance(self):
        body = self.check_dashboard({
            "scenario": "Smoke test fixture",
            "source": {
                "environment": "local <fixture>",
                "measured_at": "2026-11-20T00:00:00Z",
                "command": "test export",
            },
            "before": {"label": "baseline", "samples": 25, "p50_ms": 4.5, "p95_ms": 7},
            "after": {"label": "candidate", "samples": 25, "p50_ms": 3, "p95_ms": 8},
        })
        self.assertIn("Measured comparison", body)
        self.assertIn("<td>4.500 ms</td><td>3.000 ms</td><td>-1.500 ms</td>", body)
        self.assertIn("<td>7.000 ms</td><td>8.000 ms</td><td>+1.000 ms</td>", body)
        self.assertIn("local &lt;fixture&gt;", body)
        self.assertIn("test export", body)

    def test_invalid_export_does_not_display_measurements(self):
        body = self.check_dashboard({"scenario": "Incomplete export"})
        self.assertIn("Results unavailable", body)
        self.assertIn("<td>—</td><td>—</td><td>—</td>", body)
        self.assertNotIn("Measurement source", body)
