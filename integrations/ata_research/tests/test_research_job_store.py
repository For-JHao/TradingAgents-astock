from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from research_api.storage.job_store import ResearchJobStore, restore_job_database


class ResearchJobStoreTest(unittest.TestCase):
    def test_cli_backup_uses_repository_root_env_in_nested_layout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "research"
            src = root / "integrations" / "ata_research" / "src"
            package = Path(__file__).resolve().parents[1] / "src" / "research_api"
            shutil.copytree(package, src / "research_api", ignore=shutil.ignore_patterns("__pycache__"))
            database = root / "configured.db"
            store = ResearchJobStore(database)
            store.save({"job_id": "root-env-job", "status": "succeeded", "updated_at": 1.0})
            store.close()
            (root / ".env").write_text(f"ASTOCK_RESEARCH_DB_PATH={database}\n")
            (root / ".env.enterprise").write_text(f"ASTOCK_RESEARCH_DB_PATH={root / 'wrong-enterprise.db'}\n")
            environment = os.environ.copy()
            environment.pop("ASTOCK_RESEARCH_DB_PATH", None)
            environment["PYTHONPATH"] = str(src)
            (src / ".env").write_text(f"ASTOCK_RESEARCH_DB_PATH={root / 'wrong-src.db'}\n")
            backup = root / "backup.db"
            subprocess.run([sys.executable, "-B", "-m", "research_api.job_store_cli", "backup", str(backup)],
                           cwd=src, env=environment, check=True, capture_output=True, text=True)
            copied = ResearchJobStore(backup)
            self.assertEqual([row["job_id"] for row in copied.load_all()], ["root-env-job"])
            copied.close()
            self.assertFalse((root / "wrong-enterprise.db").exists())
            self.assertFalse((root / "wrong-src.db").exists())

    def test_restore_preserves_crashed_wal_and_refuses_busy_checkpoint(self) -> None:
        for busy in (False, True):
            with self.subTest(busy=busy), tempfile.TemporaryDirectory() as directory:
                source = Path(directory) / "source.db"
                destination = Path(directory) / "current.db"
                ResearchJobStore(source).close()
                subprocess.run([
                    sys.executable, "-c",
                    "import os, sqlite3, sys; "
                    "db = sqlite3.connect(sys.argv[1]); "
                    "db.executescript('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; "
                    "CREATE TABLE restore_test (id INTEGER); INSERT INTO restore_test VALUES (1); "
                    "PRAGMA wal_checkpoint(TRUNCATE); INSERT INTO restore_test VALUES (2);'); "
                    "os._exit(0)", str(destination),
                ], check=True)
                self.assertTrue(Path(f"{destination}-wal").exists())
                if busy:
                    reader = sqlite3.connect(destination)
                    try:
                        reader.execute("BEGIN")
                        reader.execute("SELECT * FROM restore_test").fetchall()
                        with self.assertRaisesRegex(ValueError, "checkpoint incomplete"):
                            restore_job_database(source, destination)
                        self.assertFalse(destination.with_suffix(".db.before-restore").exists())
                        self.assertFalse(destination.with_suffix(".db.restore.tmp").exists())
                        self.assertEqual(reader.execute("SELECT COUNT(*) FROM restore_test").fetchone()[0], 2)
                    finally:
                        reader.close()
                else:
                    previous = restore_job_database(source, destination)
                    with sqlite3.connect(previous) as retained:
                        self.assertEqual(retained.execute("SELECT COUNT(*) FROM restore_test").fetchone()[0], 2)
                    restored = ResearchJobStore(destination)
                    self.assertEqual(restored.load_all(), [])
                    restored.close()

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

    def test_backup_rejects_an_invalid_snapshot_before_reporting_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "jobs.db"
            store = ResearchJobStore(database_path)
            try:
                with sqlite3.connect(database_path) as source:
                    source.execute("DROP TABLE research_jobs")
                with self.assertRaisesRegex(ValueError, "not a research job database"):
                    store.backup_to(Path(directory) / "invalid-backup.db")
            finally:
                store.close()

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
