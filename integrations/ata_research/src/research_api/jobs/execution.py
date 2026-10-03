"""Bounded single-process worker lifecycle; durable storage owns completion."""
from pathlib import Path
from types import SimpleNamespace
import os, time
import httpx, requests
from research_api.http.errors import safe_error
from research_api.storage.reports import publish_report

REPORT_FIELDS = {"market": "market_report", "sentiment": "sentiment_report", "news": "news_report", "fundamentals": "fundamentals_report", "policy": "policy_report", "hot_money": "hot_money_report", "lockup": "lockup_report", "investment_plan": "investment_plan", "trader_investment_plan": "trader_investment_plan"}

def summarize_state(state, *, ticker, trade_date, signal, report_path=None):
    return {"ticker": ticker, "trade_date": trade_date, "signal": signal, "final_trade_decision": str(state.get("final_trade_decision", "")), "reports": {key: state.get(field, "") for key, field in REPORT_FIELDS.items()}, "report_path": report_path or "", "missing_data_tasks": state.get("missing_data_tasks", []), "missing_data_complete": state.get("missing_data_complete", False)}

def is_transient_error(exc):
    if isinstance(exc, (TimeoutError, httpx.ConnectError, httpx.ReadTimeout, requests.Timeout, requests.ConnectionError)):
        return True
    from openai import APIConnectionError, RateLimitError, InternalServerError
    return isinstance(exc, (APIConnectionError, RateLimitError, InternalServerError))

def run_job(job, *, lock, store, semaphore, retries, retry_delay, adapter=None):
    from research_api.engine.adapter import EngineAdapter, ENGINE_VERSION, ENGINE_SHA, INTEGRATION_VERSION
    from tradingagents.default_config import DEFAULT_CONFIG
    adapter = adapter or EngineAdapter()
    root = Path(os.getenv("TRADINGAGENTS_RESULTS_DIR", str(DEFAULT_CONFIG["results_dir"])))
    directory = Path(os.getenv("ASTOCK_RUNTIME_DIR", str(store.database_path.parent / "runs")))
    def progress(stage):
        with lock:
            job.stage = stage
            job.updated_at = time.time()
            store.save(job.persistence_record())
    with semaphore:
        for attempt in range(1, retries + 2):
            job.status = "running"
            job.attempt = attempt
            try:
                progress("graph_initialization")
                result = adapter.analyze(job, SimpleNamespace(directory=directory, progress=progress))
                progress("report_publication")
                report_path = publish_report(result.state, root, job.ticker, job.trade_date, job.job_id,
                    {"engineVersion": ENGINE_VERSION, "engineSha": ENGINE_SHA, "integrationVersion": INTEGRATION_VERSION})
                summary = summarize_state(result.state, ticker=job.ticker, trade_date=job.trade_date, signal=result.signal, report_path=report_path)
                # Completion is not visible in memory until the exact succeeded record is durable.
                progress("job_commit")
                with lock:
                    record = {**job.persistence_record(), "status": "succeeded", "stage": "completed", "error": None, "error_type": None, "error_stage": None, "error_traceback": None, "signal": result.signal, "result": summary, "updated_at": time.time()}
                    store.save(record)
                    job.status = "succeeded"
                    job.stage = "completed"
                    job.error = job.error_type = job.error_stage = job.error_traceback = None
                    job.result = summary
                    job.signal = result.signal
                    job.updated_at = record["updated_at"]
                return
            except Exception as exc:
                stage = job.stage or "unknown"
                retry = stage == "graph_execution" and attempt <= retries and is_transient_error(exc)
                with lock:
                    job.status = "running" if retry else "failed"
                    job.error = safe_error(exc, stage)
                    job.error_type = "missing_data_tracking_failed" if str(exc) == "missing_data_tracking_failed" else "research_operation_failed"
                    job.error_stage = stage
                    job.error_traceback = None
                    job.stage = "retry_wait" if retry else stage
                    job.updated_at = time.time()
                    try:
                        store.save(job.persistence_record())
                    except Exception:
                        # Keep the failed process-local state and the original durable record/outputs.
                        pass
                if retry:
                    if retry_delay:
                        time.sleep(retry_delay)
                    continue
                return
