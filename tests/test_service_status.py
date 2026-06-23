import unittest

from research_api.service_status import ApiMetrics


class ApiMetricsTest(unittest.TestCase):
    def test_snapshot_includes_idle_and_observed_routes(self) -> None:
        metrics = ApiMetrics()
        metrics.record("POST", "/market/quotes", 200, 12.25)

        rows = metrics.snapshot(
            [("GET", "/health"), ("POST", "/market/quotes")]
        )

        self.assertEqual(
            rows[0], {"method": "GET", "path": "/health", "state": "idle"}
        )
        self.assertEqual(rows[1]["state"], "healthy")
        self.assertEqual(rows[1]["last_status"], 200)
        self.assertEqual(rows[1]["request_count"], 1)
        self.assertEqual(rows[1]["last_duration_ms"], 12.2)

    def test_latest_error_marks_route_as_error(self) -> None:
        metrics = ApiMetrics()
        metrics.record("GET", "/research/jobs/{job_id}", 200, 5)
        metrics.record("GET", "/research/jobs/{job_id}", 404, 2)

        row = metrics.snapshot([("GET", "/research/jobs/{job_id}")])[0]

        self.assertEqual(row["state"], "error")
        self.assertEqual(row["request_count"], 2)
        self.assertEqual(row["error_count"], 1)


if __name__ == "__main__":
    unittest.main()
