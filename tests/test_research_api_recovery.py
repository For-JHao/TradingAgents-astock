from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from research_api.job_store import ResearchJobStore


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


if __name__ == "__main__":
    unittest.main()
