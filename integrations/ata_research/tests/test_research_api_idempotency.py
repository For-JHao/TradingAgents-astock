from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class ResearchApiIdempotencyTest(unittest.TestCase):
    def test_client_request_id_returns_same_job_and_supports_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "jobs.db"
            environment = os.environ.copy()
            environment["ASTOCK_RESEARCH_DB_PATH"] = str(database_path)
            script = """
import json
from unittest.mock import patch
from fastapi import HTTPException
from research_api.app import (
    ResearchJobRequest,
    create_research_job,
    get_research_job_by_client_request_id,
)

request = ResearchJobRequest(
    ticker="600519",
    trade_date="2026-07-18",
    client_request_id="ASTOCKJOB-stable-reference",
)
with patch("research_api.app.threading.Thread.start", lambda self: None):
    first = create_research_job(request)
    second = create_research_job(request)
lookup = get_research_job_by_client_request_id("ASTOCKJOB-stable-reference")
try:
    create_research_job(request.model_copy(update={"ticker": "000001"}))
except HTTPException as error:
    conflict_status = error.status_code
else:
    conflict_status = None
print("RESULT=" + json.dumps({
    "first": first,
    "second": second,
    "lookup": lookup,
    "conflict_status": conflict_status,
}))
"""
            completed = subprocess.run(
                [sys.executable, "-c", script],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            line = next(
                item for item in completed.stdout.splitlines() if item.startswith("RESULT=")
            )
            result = json.loads(line.removeprefix("RESULT="))
            self.assertEqual(result["first"]["job_id"], result["second"]["job_id"])
            self.assertEqual(result["lookup"]["job_id"], result["first"]["job_id"])
            self.assertEqual(
                result["lookup"]["client_request_id"],
                "ASTOCKJOB-stable-reference",
            )
            self.assertEqual(result["conflict_status"], 409)


if __name__ == "__main__":
    unittest.main()
