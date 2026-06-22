from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
from pathlib import Path
from typing import Any


def default_job_database_path() -> Path:
    configured = os.getenv("ASTOCK_RESEARCH_DB_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return Path.home() / ".tradingagents" / "research-api" / "jobs.db"


class ResearchJobStore:
    def __init__(self, database_path: str | Path | None = None) -> None:
        self.database_path = Path(database_path or default_job_database_path())
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            timeout=5,
        )
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._connection.execute("PRAGMA busy_timeout = 5000")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS research_jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS research_jobs_updated_idx "
                "ON research_jobs(updated_at DESC)"
            )
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def backup_to(self, destination_path: str | Path) -> Path:
        destination = Path(destination_path).expanduser().resolve()
        if destination == self.database_path.resolve():
            raise ValueError("backup path cannot be the active research job database")
        destination.parent.mkdir(parents=True, exist_ok=True)
        target = sqlite3.connect(str(destination))
        try:
            with self._lock:
                self._connection.backup(target)
        finally:
            target.close()
        return destination

    def save(self, record: dict[str, Any]) -> None:
        job_id = str(record["job_id"])
        status = str(record["status"])
        updated_at = float(record["updated_at"])
        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO research_jobs(job_id, status, updated_at, payload_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    payload_json = excluded.payload_json
                """,
                (job_id, status, updated_at, payload),
            )
            self._connection.commit()

    def load_all(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload_json FROM research_jobs ORDER BY updated_at DESC"
            ).fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
            try:
                value = json.loads(str(row["payload_json"]))
            except (TypeError, ValueError):
                continue
            if isinstance(value, dict):
                records.append(value)
        return records

    def health(self) -> dict[str, Any]:
        with self._lock:
            result = self._connection.execute("PRAGMA quick_check").fetchone()
            count = self._connection.execute("SELECT COUNT(*) FROM research_jobs").fetchone()
        return {
            "ready": bool(result and result[0] == "ok"),
            "jobs": int(count[0]) if count else 0,
            "journal_mode": "wal",
        }


def validate_job_database(database_path: str | Path) -> None:
    source = sqlite3.connect(f"file:{Path(database_path).resolve()}?mode=ro", uri=True)
    try:
        result = source.execute("PRAGMA quick_check").fetchone()
        if not result or result[0] != "ok":
            raise ValueError("research job database quick_check failed")
        table = source.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='research_jobs'"
        ).fetchone()
        if not table:
            raise ValueError("not a research job database")
    finally:
        source.close()


def restore_job_database(source_path: str | Path, destination_path: str | Path) -> Path | None:
    source = Path(source_path).expanduser().resolve()
    destination = Path(destination_path).expanduser().resolve()
    if source == destination:
        raise ValueError("restore source cannot be the active research job database")
    validate_job_database(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".restore.tmp")
    previous = destination.with_suffix(destination.suffix + ".before-restore")
    temporary.unlink(missing_ok=True)
    shutil.copy2(source, temporary)
    if destination.exists():
        previous.unlink(missing_ok=True)
        destination.replace(previous)
    try:
        Path(f"{destination}-wal").unlink(missing_ok=True)
        Path(f"{destination}-shm").unlink(missing_ok=True)
        temporary.replace(destination)
    except Exception:
        if previous.exists() and not destination.exists():
            previous.replace(destination)
        raise
    return previous if previous.exists() else None
