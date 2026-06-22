from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_api.job_store import ResearchJobStore, restore_job_database


class ResearchJobStoreTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
