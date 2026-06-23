from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Iterable


@dataclass
class _EndpointMetric:
    request_count: int = 0
    error_count: int = 0
    total_duration_ms: float = 0
    last_duration_ms: float | None = None
    last_status: int | None = None
    last_called_at: float | None = None


class ApiMetrics:
    def __init__(self) -> None:
        self._metrics: dict[tuple[str, str], _EndpointMetric] = {}
        self._lock = threading.Lock()

    def record(self, method: str, path: str, status: int, duration_ms: float) -> None:
        key = (method.upper(), path)
        with self._lock:
            metric = self._metrics.setdefault(key, _EndpointMetric())
            metric.request_count += 1
            metric.error_count += int(status >= 400)
            metric.total_duration_ms += duration_ms
            metric.last_duration_ms = duration_ms
            metric.last_status = status
            metric.last_called_at = time.time()

    def snapshot(self, routes: Iterable[tuple[str, str]]) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        with self._lock:
            for method, path in sorted(set(routes)):
                metric = self._metrics.get((method, path))
                if metric is None:
                    rows.append({"method": method, "path": path, "state": "idle"})
                    continue
                average = metric.total_duration_ms / metric.request_count
                rows.append(
                    {
                        "method": method,
                        "path": path,
                        "state": "healthy" if (metric.last_status or 500) < 400 else "error",
                        "request_count": metric.request_count,
                        "error_count": metric.error_count,
                        "last_status": metric.last_status,
                        "last_duration_ms": round(metric.last_duration_ms or 0, 1),
                        "average_duration_ms": round(average, 1),
                        "last_called_at": metric.last_called_at,
                    }
                )
        return rows
