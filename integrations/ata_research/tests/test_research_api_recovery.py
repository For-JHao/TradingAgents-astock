from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from research_api.storage.job_store import ResearchJobStore


class ResearchApiRecoveryTest(unittest.TestCase):
    def test_interrupted_job_is_restored_as_failed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "jobs.db"
            store = ResearchJobStore(database_path)
            store.save(
                {
                    "job_id": "job-interrupted",
                    "status": "running",
                    "ticker": "600519",
                    "requested_ticker": "600519",
                    "trade_date": "2026-06-21",
                    "created_at": 1000.0,
                    "updated_at": 1001.0,
                    "error": None,
                    "signal": None,
                    "result": None,
                    "request": {"ticker": "600519"},
                    "config": {},
                    "selected_analysts": ["market"],
                }
            )
            store.close()

            environment = os.environ.copy()
            environment["ASTOCK_RESEARCH_DB_PATH"] = str(database_path)
            command = (
                "import json; "
                "from research_api.app import get_research_job; "
                "print('RECOVERY=' + json.dumps(get_research_job('job-interrupted'), ensure_ascii=False))"
            )
            completed = subprocess.run(
                [sys.executable, "-c", command],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            line = next(item for item in completed.stdout.splitlines() if item.startswith("RECOVERY="))
            recovered = json.loads(line.removeprefix("RECOVERY="))
            self.assertEqual(recovered["job_id"], "job-interrupted")
            self.assertEqual(recovered["status"], "failed")
            self.assertIn("重启", recovered["error"])

    def test_client_request_id_lookup_survives_service_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "jobs.db"
            environment = os.environ.copy()
            environment["ASTOCK_RESEARCH_DB_PATH"] = str(database_path)
            create_command = """
from unittest.mock import patch
from research_api.app import ResearchJobRequest, create_research_job
with patch("research_api.app.threading.Thread.start", lambda self: None):
    created = create_research_job(ResearchJobRequest(
        ticker="600519",
        trade_date="2026-07-18",
        client_request_id="ASTOCKJOB-restart-reference",
    ))
print("CREATED=" + created["job_id"])
"""
            subprocess.run(
                [sys.executable, "-c", create_command],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            query_command = """
import json
from research_api.app import get_research_job_by_client_request_id
job = get_research_job_by_client_request_id("ASTOCKJOB-restart-reference")
print("RECOVERED=" + json.dumps(job))
"""
            completed = subprocess.run(
                [sys.executable, "-c", query_command],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            line = next(
                item for item in completed.stdout.splitlines() if item.startswith("RECOVERED=")
            )
            recovered = json.loads(line.removeprefix("RECOVERED="))
            self.assertEqual(recovered["client_request_id"], "ASTOCKJOB-restart-reference")
            self.assertEqual(recovered["status"], "failed")
            self.assertIn("重启", recovered["error"])


if __name__ == "__main__":
    unittest.main()
