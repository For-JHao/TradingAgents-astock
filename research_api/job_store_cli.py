from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from research_api.job_store import (
    ResearchJobStore,
    default_job_database_path,
    restore_job_database,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")


def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace(":", "-").replace(".", "-")


def _default_backup_path() -> Path:
    root = Path(
        os.getenv("ASTOCK_RESEARCH_BACKUP_DIR", "").strip()
        or Path.home() / ".tradingagents" / "backups"
    )
    return root.expanduser().resolve() / f"research-jobs-{_stamp()}.db"


def _prune(directory: Path) -> None:
    try:
        retention = max(1, min(int(os.getenv("ASTOCK_RESEARCH_BACKUP_RETENTION", "30")), 365))
    except ValueError:
        retention = 30
    backups = sorted(directory.glob("research-jobs-*.db"), key=lambda item: item.stat().st_mtime, reverse=True)
    for expired in backups[retention:]:
        expired.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Research API SQLite backup and restore")
    subcommands = parser.add_subparsers(dest="command", required=True)
    backup_parser = subcommands.add_parser("backup")
    backup_parser.add_argument("destination", nargs="?")
    restore_parser = subcommands.add_parser("restore")
    restore_parser.add_argument("source")
    restore_parser.add_argument("--confirm", action="store_true", required=True)
    args = parser.parse_args()

    if args.command == "backup":
        destination = Path(args.destination).resolve() if args.destination else _default_backup_path()
        store = ResearchJobStore()
        try:
            store.backup_to(destination)
        finally:
            store.close()
        if not args.destination:
            _prune(destination.parent)
        print(destination)
        return

    previous = restore_job_database(args.source, default_job_database_path())
    print({"restored": str(default_job_database_path()), "previous": str(previous) if previous else None})


if __name__ == "__main__":
    main()
