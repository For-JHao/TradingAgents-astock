from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from research_api.job_store import ResearchJobStore, restore_job_database


class ResearchJobStoreTest(unittest.TestCase):
    def test_legacy_schema_is_rejected_without_changing_jobs_or_reports(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "jobs.db"
            legacy = {
                "job_id": "legacy-job",
                "status": "succeeded",
                "updated_at": 1000.0,
            }
            connection = sqlite3.connect(database_path)
            connection.execute(
                """
                CREATE TABLE research_jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT INTO research_jobs(job_id, status, updated_at, payload_json) "
                "VALUES (?, ?, ?, ?)",
                (
                    legacy["job_id"],
                    legacy["status"],
                    legacy["updated_at"],
                    json.dumps(legacy),
                ),
            )
            connection.commit()
            connection.close()
            before = database_path.read_bytes()
            reports = Path(directory) / "reports"
            reports.mkdir()
            report = reports / "full_states_log_2026-08-31.json"
            report_bytes = b'{"final_trade_decision":"synthetic report"}'
            report.write_bytes(report_bytes)

            with self.assertRaisesRegex(ValueError, "unsupported research job database schema"):
                ResearchJobStore(database_path)

            self.assertEqual(database_path.read_bytes(), before)
            with sqlite3.connect(database_path) as unchanged:
                columns = [row[1] for row in unchanged.execute("PRAGMA table_info(research_jobs)")]
                self.assertNotIn("client_request_id", columns)
                rows = unchanged.execute("SELECT payload_json FROM research_jobs").fetchall()
                self.assertEqual([json.loads(row[0]) for row in rows], [legacy])
            self.assertEqual(report.read_bytes(), report_bytes)
            current_path = Path(directory) / "current.db"
            ResearchJobStore(current_path).close()
            current_before = current_path.read_bytes()
            with self.assertRaisesRegex(ValueError, "unsupported research job database schema"):
                restore_job_database(database_path, current_path)
            self.assertEqual(current_path.read_bytes(), current_before)
            self.assertFalse(current_path.with_suffix(".db.before-restore").exists())
            self.assertEqual(report.read_bytes(), report_bytes)

    def test_job_survives_reopen_and_update(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "jobs.db"
            initial = {
                "job_id": "job-1",
                "status": "running",
                "updated_at": 1000.0,
                "request": {"ticker": "600519"},
                "result": None,
            }
            first = ResearchJobStore(database_path)
            first.save(initial)
            first.close()

            reopened = ResearchJobStore(database_path)
            self.assertEqual(reopened.load_all(), [initial])
            completed = {**initial, "status": "succeeded", "updated_at": 2000.0}
            reopened.save(completed)
            self.assertEqual(reopened.load_all(), [completed])
            self.assertEqual(
                reopened.health(),
                {"ready": True, "jobs": 1, "journal_mode": "wal"},
            )
            backup_path = Path(directory) / "backup.db"
            reopened.backup_to(backup_path)
            reopened.close()

            restore_target = Path(directory) / "restored" / "jobs.db"
            stale = ResearchJobStore(restore_target)
            stale.save({**initial, "job_id": "stale-job"})
            stale.close()
            previous = restore_job_database(backup_path, restore_target)
            self.assertIsNotNone(previous)
            restored = ResearchJobStore(restore_target)
            self.assertEqual(restored.load_all(), [completed])
            restored.close()

    def test_client_request_id_is_unique_and_queryable_after_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "jobs.db"
            initial = {
                "job_id": "job-1",
                "client_request_id": "ASTOCKJOB-stable-reference",
                "status": "queued",
                "updated_at": 1000.0,
            }
            first = ResearchJobStore(database_path)
            first.save(initial)
            self.assertEqual(
                first.load_by_client_request_id("ASTOCKJOB-stable-reference"),
                initial,
            )
            first.close()

            reopened = ResearchJobStore(database_path)
            self.assertEqual(
                reopened.load_by_client_request_id("ASTOCKJOB-stable-reference"),
                initial,
            )
            with self.assertRaises(ValueError):
                reopened.save(
                    {
                        **initial,
                        "job_id": "job-2",
                        "updated_at": 1001.0,
                    }
                )
            reopened.close()


if __name__ == "__main__":
    unittest.main()
