"""Manage analysis history by scanning existing log files."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_STATES_LOG_RE = re.compile(
    r"^full_states_log_(?P<date>\d{4}-\d{2}-\d{2})(?:_(?P<time>\d{6}))?\.json$"
)


def _results_dir() -> Path:
    return Path.home() / ".tradingagents" / "logs"


def _parse_log_filename(name: str) -> tuple[str, str] | None:
    match = _STATES_LOG_RE.match(name)
    if not match:
        return None
    date = match.group("date")
    raw_time = match.group("time")
    if raw_time:
        time_label = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}"
    else:
        time_label = ""
    return date, time_label


def get_history() -> list[dict[str, str]]:
    """Scan saved analysis logs and return a sorted list (newest first).

    Each entry: {"ticker": "300750", "date": "2026-05-12", "time": "14:30:52", "path": "..."}
    """
    root = _results_dir()
    if not root.exists():
        return []

    entries: list[dict[str, str]] = []
    for log_file in root.rglob("full_states_log_*.json"):
        parsed = _parse_log_filename(log_file.name)
        if not parsed:
            continue
        date, time_label = parsed
        ticker = log_file.parent.parent.name
        entries.append(
            {
                "ticker": ticker,
                "date": date,
                "time": time_label,
                "path": str(log_file),
            }
        )

    entries.sort(key=lambda e: Path(e["path"]).stat().st_mtime, reverse=True)
    return entries


def load_analysis(path: str) -> dict[str, Any]:
    """Load a saved analysis JSON file."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def extract_signal(state: dict[str, Any]) -> str:
    """Extract the short signal (Buy/Sell/Hold) from a final state dict."""
    import re

    for field in (
        "investment_plan",
        "trader_investment_decision",
        "final_trade_decision",
    ):
        text = state.get(field, "")
        if not text:
            continue
        cleaned = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
        for keyword in ("BUY", "SELL", "HOLD"):
            if keyword in cleaned.upper():
                return keyword.capitalize()
    return "N/A"
