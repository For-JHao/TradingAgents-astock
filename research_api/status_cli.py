from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import requests


def _get_json(url: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        response = requests.get(url, timeout=3)
        response.raise_for_status()
        return response.json(), None
    except Exception as exc:
        return None, str(exc)


def _format_time(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return datetime.fromtimestamp(value).strftime("%H:%M:%S")


def main() -> None:
    research_url = os.getenv("ASTOCK_RESEARCH_API_URL", "http://127.0.0.1:8008").rstrip("/")
    platform_url = os.getenv("AI_ASSISTANT_API_URL", "http://127.0.0.1:3001").rstrip("/")

    status, error = _get_json(f"{research_url}/status")
    print("Trading platform service status")
    print(f"  research-api  {'OFFLINE' if error else 'OK':<7} {research_url}")
    if error:
        print(f"    {error}")
    else:
        endpoints = status.get("endpoints", []) if status else []
        print("\n  METHOD  STATE    STATUS  CALLS  LAST      LATENCY  PATH")
        for item in endpoints:
            last_status = item.get("last_status", "-")
            calls = item.get("request_count", 0)
            latency = item.get("last_duration_ms")
            latency_text = f"{latency}ms" if latency is not None else "-"
            print(
                f"  {item['method']:<7} {item['state']:<8} {str(last_status):<7} "
                f"{str(calls):<6} {_format_time(item.get('last_called_at')):<9} "
                f"{latency_text:<8} {item['path']}"
            )

    platform, platform_error = _get_json(f"{platform_url}/health")
    platform_state = "OFFLINE" if platform_error else str((platform or {}).get("status", "OK")).upper()
    print(f"\n  assistant-api {platform_state:<7} {platform_url}")
    if platform_error:
        print(f"    {platform_error}")


if __name__ == "__main__":
    main()
