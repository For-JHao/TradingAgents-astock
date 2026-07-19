from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Barrier, Lock

from tradingagents.agents.utils.memory import TradingMemoryLog
from tradingagents.dataflows import a_stock
from tradingagents.dataflows.config import get_config, set_config
from tradingagents.graph.trading_graph import states_log_filename


def test_runtime_config_is_isolated_between_parallel_jobs() -> None:
    barrier = Barrier(2)

    def read_language(language: str) -> str:
        set_config({"output_language": language})
        barrier.wait(timeout=2)
        return str(get_config()["output_language"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(read_language, ["Chinese", "English"]))

    assert results == ["Chinese", "English"]


def test_parallel_memory_updates_do_not_lose_entries(tmp_path: Path) -> None:
    log_path = tmp_path / "memory.md"
    logs = [TradingMemoryLog({"memory_log_path": str(log_path)}) for _ in range(8)]
    for index, log in enumerate(logs):
        log.store_decision(f"6000{index:02d}", "2026-07-19", "Rating: Hold")

    def resolve(index: int) -> None:
        logs[index].update_with_outcome(
            f"6000{index:02d}",
            "2026-07-19",
            0.01,
            0.005,
            5,
            f"reflection-{index}",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(resolve, range(8)))

    entries = logs[0].load_entries()
    assert len(entries) == 8
    assert all(not entry["pending"] for entry in entries)
    assert {entry["reflection"] for entry in entries} == {
        f"reflection-{index}" for index in range(8)
    }


def test_shared_market_clients_are_serialized(monkeypatch) -> None:
    counter_lock = Lock()
    active = 0
    peak = 0

    def record_call(*args, **kwargs):
        nonlocal active, peak
        with counter_lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.02)
        with counter_lock:
            active -= 1
        return object()

    class FakeMootdxClient:
        bars = staticmethod(record_call)

    class FakeEastmoneySession:
        get = staticmethod(record_call)

    monkeypatch.setattr(a_stock, "_mootdx_client", FakeMootdxClient())
    monkeypatch.setattr(a_stock, "_EM_SESSION", FakeEastmoneySession())
    monkeypatch.setattr(a_stock, "_EM_MIN_INTERVAL", 0.0)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _: a_stock._mootdx_call("bars", symbol="600001"), range(4)))
        list(executor.map(lambda _: a_stock._em_get("https://example.test"), range(4)))

    assert peak == 1


def test_result_log_names_do_not_collide_within_the_same_second() -> None:
    timestamp = datetime(2026, 7, 19, 12, 0, 0)
    assert states_log_filename("2026-07-19", timestamp) != states_log_filename(
        "2026-07-19", timestamp
    )


def test_research_api_runs_two_jobs_in_parallel_and_retries_transient_failures() -> None:
    with tempfile.TemporaryDirectory() as directory:
        environment = os.environ.copy()
        environment["ASTOCK_RESEARCH_DB_PATH"] = str(Path(directory) / "jobs.db")
        environment["ASTOCK_MAX_CONCURRENT_JOBS"] = "2"
        environment["ASTOCK_TRANSIENT_JOB_RETRIES"] = "1"
        environment["ASTOCK_TRANSIENT_RETRY_DELAY_SECONDS"] = "0"
        script = r'''
import json
import threading
import time

import research_api.app as api


def final_state(ticker):
    return {
        "final_trade_decision": "Rating: Hold",
        "market_report": ticker,
    }


class FakeGraph:
    condition = threading.Condition()
    active = 0
    peak = 0
    attempts = {}

    def __init__(self, **kwargs):
        self.last_log_path = None

    def propagate(self, ticker, trade_date):
        cls = type(self)
        with cls.condition:
            cls.attempts[ticker] = cls.attempts.get(ticker, 0) + 1
            attempt = cls.attempts[ticker]
            cls.active += 1
            cls.peak = max(cls.peak, cls.active)
            cls.condition.notify_all()
            if ticker in {"600001", "600002"}:
                cls.condition.wait_for(lambda: cls.active >= 2, timeout=2)
        try:
            if ticker == "600003" and attempt == 1:
                raise ValueError("not enough values to unpack (expected 2, got 0)")
            if ticker == "600004":
                raise ValueError("not enough values to unpack (expected 2, got 0)")
            time.sleep(0.05)
            return final_state(ticker), "Hold"
        finally:
            with cls.condition:
                cls.active -= 1
                cls.condition.notify_all()


api.TradingAgentsGraph = FakeGraph


def start(ticker):
    return api.create_research_job(api.ResearchJobRequest(ticker=ticker, trade_date="2026-07-19"))


def wait(job_id):
    deadline = time.time() + 5
    while time.time() < deadline:
        status = api.get_research_job(job_id)
        if status["status"] in {"succeeded", "failed"}:
            return status
        time.sleep(0.01)
    raise TimeoutError(job_id)


parallel_jobs = [start("600001"), start("600002")]
parallel_results = [wait(job["job_id"]) for job in parallel_jobs]
retry_job = start("600003")
retry_result = wait(retry_job["job_id"])
failed_job = start("600004")
failed_result = wait(failed_job["job_id"])
failed_record = api._job_store.load_all()[0]

print("RESULT=" + json.dumps({
    "parallel_statuses": [item["status"] for item in parallel_results],
    "peak": FakeGraph.peak,
    "retry_status": retry_result["status"],
    "retry_attempt": retry_result["attempt"],
    "failed_status": failed_result["status"],
    "failed_attempt": failed_result["attempt"],
    "error_stage": failed_record.get("error_stage"),
    "error_type": failed_record.get("error_type"),
    "has_traceback": "ValueError" in str(failed_record.get("error_traceback")),
}))
'''
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

    assert result["parallel_statuses"] == ["succeeded", "succeeded"]
    assert result["peak"] == 2
    assert result["retry_status"] == "succeeded"
    assert result["retry_attempt"] == 2
    assert result["failed_status"] == "failed"
    assert result["failed_attempt"] == 2
    assert result["error_stage"] == "graph_execution"
    assert result["error_type"] == "ValueError"
    assert result["has_traceback"] is True
