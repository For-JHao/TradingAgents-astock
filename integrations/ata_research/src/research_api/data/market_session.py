"""Bounded, offline exchange-session proof from the installed AkShare calendar."""
from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import date, datetime, time
from functools import lru_cache
import hashlib
import importlib.util
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_BYTES = 1_048_576

@dataclass(frozen=True)
class TradingCalendar:
    dates: tuple[date, ...]
    checksum: str


def parse_calendar(raw: bytes) -> TradingCalendar:
    if not raw or len(raw) > _MAX_BYTES:
        raise ValueError("calendar_size")
    values = json.loads(raw)
    if not isinstance(values, list) or not 2 <= len(values) <= 30_000:
        raise ValueError("calendar_shape")
    dates = []
    for value in values:
        if not isinstance(value, str) or not re.fullmatch(r"\d{8}", value):
            raise ValueError("calendar_date")
        dates.append(datetime.strptime(value, "%Y%m%d").date())
    if any(left >= right for left, right in zip(dates, dates[1:])):
        raise ValueError("calendar_order")
    return TradingCalendar(tuple(dates), hashlib.sha256(raw).hexdigest())


@lru_cache(maxsize=1)
def installed_calendar() -> TradingCalendar:
    # Locate without importing AkShare (its import has unrelated provider side effects).
    spec = importlib.util.find_spec("akshare")
    if spec is None or not spec.submodule_search_locations:
        raise ValueError("calendar_missing")
    path = Path(next(iter(spec.submodule_search_locations))) / "file_fold" / "calendar.json"
    with path.open("rb") as stream:
        return parse_calendar(stream.read(_MAX_BYTES + 1))


def market_session(now: datetime, calendar: TradingCalendar | None = None) -> dict:
    """No weekday guesses: absent/out-of-coverage calendar leaves the session unknown."""
    unavailable = {"session_status": None, "latest_session_date": None,
                   "session_closed_at": None, "calendar_proof": None}
    try:
        if now.tzinfo is None:
            return unavailable
        calendar = calendar or installed_calendar()
        local = now.astimezone(_SHANGHAI)
        day = local.date()
        if not calendar.dates[0] <= day <= calendar.dates[-1]:
            return unavailable
        index = bisect_left(calendar.dates, day)
        today_trades = index < len(calendar.dates) and calendar.dates[index] == day
        clock = local.time().replace(tzinfo=None)
        trading = today_trades and (time(9, 30) <= clock < time(11, 30) or time(13) <= clock < time(15))
        if trading:
            latest, closed_at = day, None
        elif today_trades and time(11, 30) <= clock < time(13):
            latest, closed_at = day, datetime.combine(day, time(11, 30), _SHANGHAI)
        elif today_trades and clock >= time(15):
            latest, closed_at = day, datetime.combine(day, time(15), _SHANGHAI)
        else:
            if index == 0:
                return unavailable
            latest = calendar.dates[index - 1]
            closed_at = datetime.combine(latest, time(15), _SHANGHAI)
        return {"session_status": "trading" if trading else "closed",
                "latest_session_date": latest.isoformat(),
                "session_closed_at": closed_at.isoformat() if closed_at else None,
                "calendar_proof": {"source": "akshare-bundled-calendar", "sha256": calendar.checksum,
                                   "coverage_start": calendar.dates[0].isoformat(),
                                   "coverage_end": calendar.dates[-1].isoformat()}}
    except (OSError, ValueError, TypeError, ImportError):
        return unavailable
