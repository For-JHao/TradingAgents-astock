from __future__ import annotations

import copy
import json
import math
import os
import re
import threading
import time
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

import httpx
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from research_api.http.errors import safe_error
from pydantic import BaseModel, Field

from research_api.data.provider import (
    _akshare_etf_kline,
    _eastmoney_kline_fallback,
    _eastmoney_security_snapshot,
    _is_etf_like_code,
    _tencent_quote,
)
from research_api.storage.job_store import ResearchJobStore
from research_api.service_status import ApiMetrics


from research_api.config import ROOT_DIR
load_dotenv(ROOT_DIR / ".env")
load_dotenv(ROOT_DIR / ".env.enterprise", override=False)



from research_api.http.dto import *

from research_api.jobs.model import _Job


def service_identity():
    return {"engineVersion": "v0.5.20", "integrationVersion": "1.0.0", "contractVersion": 1, "profile": "shared"}

# Disable third-party wire payload logging in ordinary service logs.
import logging
for _logger_name in ("httpx", "httpcore", "openai", "anthropic", "urllib3", "langchain", "langgraph"):
    logging.getLogger(_logger_name).setLevel(logging.CRITICAL)

app = FastAPI(title="TradingAgents-Astock Research API", version="1.0.0")
_api_metrics = ApiMetrics()
_jobs: dict[str, _Job] = {}
_lock = threading.Lock()


def _bounded_env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


_MAX_CONCURRENT_JOBS = _bounded_env_int(
    "ASTOCK_MAX_CONCURRENT_JOBS", 2, minimum=1, maximum=8
)
_TRANSIENT_JOB_RETRIES = _bounded_env_int(
    "ASTOCK_TRANSIENT_JOB_RETRIES", 1, minimum=0, maximum=3
)
try:
    _TRANSIENT_RETRY_DELAY_SECONDS = max(
        0.0, float(os.getenv("ASTOCK_TRANSIENT_RETRY_DELAY_SECONDS", "2"))
    )
except (TypeError, ValueError):
    _TRANSIENT_RETRY_DELAY_SECONDS = 2.0
_job_run_semaphore = threading.BoundedSemaphore(_MAX_CONCURRENT_JOBS)
_job_store = ResearchJobStore()
_HAS_CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")
_TICKER_RE = re.compile(r"^(?:SH|SZ|BJ)?(\d{6})(?:\.(?:SH|SZ|BJ))?$", re.IGNORECASE)


@app.middleware("http")
async def observe_api_requests(request: Request, call_next):
    started_at = time.perf_counter()
    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as exc:
        status_code = 500
        return JSONResponse(status_code=500, content={"detail": safe_error(exc, "http")})
    finally:
        duration_ms = (time.perf_counter() - started_at) * 1000
        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        _api_metrics.record(request.method, route_path, status_code, duration_ms)
        if status_code >= 400:
            print(
                f"[research-api] {request.method} {route_path} "
                f"{status_code} {duration_ms:.1f}ms"
            )
    return response


from research_api.config import ROOT_DIR, _choose_provider, _model_defaults, _build_config, _select_analysts

def _akshare_qfq_kline(code: str, start_date: str, end_date: str):
    if _is_etf_like_code(code):
        return _akshare_etf_kline(code, start_date, end_date)
    try:
        import akshare as ak
    except Exception as exc:
        raise RuntimeError("AKShare is not installed") from exc
    frame = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_date.replace("-", ""),
        end_date=end_date.replace("-", ""),
        adjust="qfq",
    )
    if frame is None or frame.empty:
        return frame
    frame = frame.rename(columns={"日期": "Date", "收盘": "Close"})
    return frame


def _daily_bars_for_code(
    code: str,
    from_date: date,
    to_date: date,
    *,
    include_prior_session: bool = False,
) -> list[dict[str, Any]]:
    buffered_from = (from_date - timedelta(days=14)).isoformat()

    def load_frame(start_date: str):
        selected_source = "eastmoney_push2his"
        try:
            selected_frame = _eastmoney_kline_fallback(
                code, start_date, to_date.isoformat()
            )
        except Exception:
            selected_frame = None
        if selected_frame is None or selected_frame.empty:
            selected_source = "akshare_qfq"
            selected_frame = _akshare_qfq_kline(
                code, start_date, to_date.isoformat()
            )
        return selected_frame, selected_source

    frame, source = load_frame(buffered_from)
    if include_prior_session:
        has_prior = False
        if frame is not None and not frame.empty and "Date" in frame:
            has_prior = any(
                date.fromisoformat(str(value)[:10]) < from_date
                for value in frame["Date"]
            )
        if not has_prior:
            frame, source = load_frame("1990-01-01")
    if frame is None or frame.empty or "Date" not in frame or "Close" not in frame:
        raise RuntimeError("前复权日线数据源未返回有效数据")

    rows: list[dict[str, Any]] = []
    prior_row: dict[str, Any] | None = None
    previous_close: float | None = None
    for _, row in frame.sort_values("Date").iterrows():
        raw_trade_day = row["Date"]
        trade_day = (
            raw_trade_day.date()
            if hasattr(raw_trade_day, "date")
            else date.fromisoformat(str(raw_trade_day)[:10])
        )
        close = float(row["Close"])
        if not math.isfinite(close) or close <= 0:
            continue
        item = {
            "code": code,
            "trade_date": trade_day.isoformat(),
            "close": close,
            "previous_close": previous_close,
            "change_pct": (
                ((close / previous_close) - 1) * 100
                if previous_close and previous_close > 0
                else None
            ),
            "source": source,
            "adjustment_mode": "qfq",
        }
        if trade_day < from_date:
            prior_row = item
        elif trade_day <= to_date:
            rows.append(item)
        previous_close = close
    if include_prior_session and prior_row:
        rows.insert(0, prior_row)
    if not rows:
        raise RuntimeError("请求区间内没有已完成的交易日")
    return rows


def _search_tickers_via_eastmoney(user_input: str) -> list[StockCandidate]:
    from research_api.data.provider import search_tickers
    return [StockCandidate.model_validate(item) for item in search_tickers(user_input)]


def _resolve_ticker_via_eastmoney(user_input: str) -> str | None:
    candidates = _search_tickers_via_eastmoney(user_input)
    if not candidates:
        return None

    clean = user_input.replace(" ", "").replace("　", "")
    exact = [item for item in candidates if item.name.replace(" ", "").replace("　", "") == clean]
    return (exact[0] if exact else candidates[0]).code


def _resolve_request_ticker(user_input: str) -> str:
    raw = user_input.strip()
    match = _TICKER_RE.fullmatch(raw)
    if match:
        return match.group(1)

    candidates = _search_tickers_via_eastmoney(raw)
    exact = [item for item in candidates if item.name.replace(" ", "") == raw.replace(" ", "")]
    if len(exact) == 1:
        return exact[0].code
    if len(candidates) == 1:
        return candidates[0].code
    raise HTTPException(status_code=400, detail="标的无法唯一核验，请使用精确代码")


def _normalize_market_code(raw: str) -> str:
    code = raw.strip().upper()
    match = _TICKER_RE.fullmatch(code)
    if match:
        return match.group(1)
    if _HAS_CHINESE_RE.search(code):
        return _resolve_request_ticker(code)
    if re.fullmatch(r"\d{6}", code):
        return code
    raise HTTPException(status_code=400, detail=f"无效股票代码: {raw}")


from research_api.jobs.execution import summarize_state as _summarize_state


def _run_job(job_id: str) -> None:
    from research_api.jobs.execution import run_job
    run_job(_jobs[job_id], lock=_lock, store=_job_store, semaphore=_job_run_semaphore,
            retries=_TRANSIENT_JOB_RETRIES, retry_delay=_TRANSIENT_RETRY_DELAY_SECONDS)


from research_api.jobs.execution import is_transient_error as _is_transient_job_error


def _restore_jobs() -> None:
    for record in _job_store.load_all():
        try:
            job = _Job.from_record(record)
        except Exception as exc:
            raise RuntimeError("persisted_job_contract_invalid") from None
            continue
        if job.status in {"queued", "running"}:
            interrupted_stage = job.stage or job.status
            job.status = "failed"
            job.stage = interrupted_stage
            job.error = "投研服务曾重启，原任务执行状态无法安全续跑，请重新提交"
            job.error_type = "ServiceRestarted"
            job.error_stage = interrupted_stage
            job.updated_at = time.time()
            _job_store.save(job.persistence_record())
        _jobs[job.job_id] = job


_restore_jobs()


@app.get("/health")
def health() -> dict[str, Any]:
    with _lock:
        job_counts = {
            status: sum(1 for job in _jobs.values() if job.status == status)
            for status in ("queued", "running", "succeeded", "failed")
        }
    return {
        **service_identity(),
        "status": "ok",
        "service": "tradingagents-astock-research-api",
        "jobs": len(_jobs),
        "execution": {
            "max_concurrent_jobs": _MAX_CONCURRENT_JOBS,
            "transient_job_retries": _TRANSIENT_JOB_RETRIES,
            "job_counts": job_counts,
        },
        "database": _job_store.health(),
    }


@app.get("/status")
def service_status() -> dict[str, Any]:
    routes: list[tuple[str, str]] = []
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if not path or not methods or not getattr(route, "include_in_schema", False):
            continue
        routes.extend((method, path) for method in methods if method not in {"HEAD", "OPTIONS"})
    return {**service_identity(), "health": health(), "endpoints": _api_metrics.snapshot(routes)}


@app.get("/research/search", response_model=StockSearchResponse)
def search_stocks(keyword: str) -> dict[str, Any]:
    raw = keyword.strip()
    if not raw:
        raise HTTPException(status_code=400, detail="keyword 不能为空")


    candidates = _search_tickers_via_eastmoney(raw)
    return {
        "keyword": keyword,
        "candidates": [item.model_dump() for item in candidates[:10]],
    }


@app.post("/market/quotes", response_model=MarketQuotesResponse)
def get_market_quotes(request: MarketQuotesRequest) -> dict[str, Any]:
    if not request.codes:
        raise HTTPException(status_code=400, detail="codes 不能为空")

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in request.codes:
        code = _normalize_market_code(raw)
        if code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    if not normalized:
        raise HTTPException(status_code=400, detail="codes 无有效项")

    try:
        quote_map = _tencent_quote(normalized)
    except Exception as exc:
        raise HTTPException(status_code=502, detail="行情源请求失败") from exc

    from research_api.data.provider import finite_number
    now_ts = time.time()
    from datetime import datetime, timezone
    from research_api.data.market_session import market_session
    session = market_session(datetime.fromtimestamp(now_ts, timezone.utc))
    quotes: list[dict[str, Any]] = []
    for code in normalized:
        item = quote_map.get(code)
        if not item:
            try:
                snapshot = _eastmoney_security_snapshot(code)
                if snapshot:
                    item = {
                        "name": snapshot.get("f58"),
                        "source": "eastmoney",
                        "quote_at": None,
                        "price": finite_number(snapshot.get("f43")),
                        "last_close": finite_number(snapshot.get("f60")),
                        "open": finite_number(snapshot.get("f46")),
                        "change_pct": finite_number(snapshot.get("f170")),
                        "high": finite_number(snapshot.get("f44")),
                        "low": finite_number(snapshot.get("f45")),
                        "turnover_pct": finite_number(snapshot.get("f168")),
                    }
            except Exception:
                item = None
        if not item or finite_number(item.get("price"), positive=True) is None:
            continue
        quotes.append(
            {
                "code": code,
                "name": item.get("name"),
                "price": finite_number(item.get("price"), positive=True),
                "last_close": finite_number(item.get("last_close"), positive=True),
                "open": finite_number(item.get("open"), positive=True),
                "change_pct": finite_number(item.get("change_pct"), positive=False),
                "high": finite_number(item.get("high"), positive=True),
                "low": finite_number(item.get("low"), positive=True),
                "volume": None,
                "turnover_pct": finite_number(item.get("turnover_pct"), positive=False),
                "ts": now_ts,
                "source": item.get("source"),
                "quote_at": item.get("quote_at"),
                **session,
            }
        )

    return {"quotes": quotes}


@app.post("/research/jobs", response_model=ResearchJobCreated)
def create_research_job(request: ResearchJobRequest) -> dict[str, Any]:
    if request.client_request_id:
        with _lock:
            existing = _find_job_by_client_request_id(request.client_request_id)
            if existing:
                return _idempotent_job_snapshot(existing, request)

    ticker = _resolve_request_ticker(request.ticker)
    trade_date = request.trade_date or date.today().isoformat()
    try:
        if date.fromisoformat(trade_date).isoformat() != trade_date:
            raise ValueError("noncanonical_date")
    except ValueError:
        raise HTTPException(status_code=400, detail="trade_date 必须为 YYYY-MM-DD") from None
    selected_analysts = _select_analysts(request, ticker)
    config = _build_config(request)
    job_id = uuid.uuid4().hex
    job = _Job(
        job_id=job_id,
        request=request,
        ticker=ticker,
        trade_date=trade_date,
        config=config,
        selected_analysts=selected_analysts,
    )

    with _lock:
        if request.client_request_id:
            existing = _find_job_by_client_request_id(request.client_request_id)
            if existing:
                return _idempotent_job_snapshot(existing, request)
        if not request.client_request_id:
            for active_job in _jobs.values():
                if (
                    active_job.ticker == ticker
                    and active_job.trade_date == trade_date
                    and active_job.status in {"queued", "running"}
                ):
                    return active_job.snapshot()
        _job_store.save(job.persistence_record())
        _jobs[job_id] = job

    thread = threading.Thread(target=_run_job, args=(job_id,), daemon=True)
    thread.start()

    return job.snapshot()


@app.post("/market/daily-bars", response_model=DailyMarketBarsResponse)
def get_daily_market_bars(request: DailyMarketBarsRequest) -> dict[str, Any]:
    try:
        from_date = date.fromisoformat(request.from_date)
        to_date = date.fromisoformat(request.to_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="from_date/to_date 必须为 YYYY-MM-DD") from exc
    if from_date > to_date:
        raise HTTPException(status_code=400, detail="from_date 不能晚于 to_date")

    normalized: list[str] = []
    for value in request.codes:
        ticker = _resolve_request_ticker(value)
        if ticker not in normalized:
            normalized.append(ticker)
    bars: list[dict[str, Any]] = []
    unavailable: list[dict[str, str]] = []
    for ticker in normalized:
        try:
            bars.extend(
                _daily_bars_for_code(
                    ticker,
                    from_date,
                    to_date,
                    include_prior_session=request.include_prior_session,
                )
            )
        except Exception as exc:
            unavailable.append({"code": ticker, "reason": "public_data_unavailable"})
    return {"bars": bars, "unavailable": unavailable}


def _find_job_by_client_request_id(client_request_id: str) -> _Job | None:
    return next(
        (
            job
            for job in _jobs.values()
            if job.request.client_request_id == client_request_id
        ),
        None,
    )


def _idempotent_job_snapshot(job: _Job, request: ResearchJobRequest) -> dict[str, Any]:
    if job.request != request:
        raise HTTPException(
            status_code=409,
            detail="client_request_id 已绑定不同的投研请求",
        )
    return job.snapshot()


@app.get(
    "/research/jobs/by-client-request-id/{client_request_id}",
    response_model=ResearchJobStatus,
)
def get_research_job_by_client_request_id(client_request_id: str) -> dict[str, Any]:
    with _lock:
        job = _find_job_by_client_request_id(client_request_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research job not found")
        return job.snapshot()


@app.get("/research/jobs/{job_id}", response_model=ResearchJobStatus)
def get_research_job(job_id: str) -> dict[str, Any]:
    with _lock:
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research job not found")
        return job.snapshot()


@app.get("/research/jobs/{job_id}/result", response_model=ResearchJobResult)
def get_research_job_result(job_id: str) -> dict[str, Any]:
    with _lock:
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research job not found")
        if job.status != "succeeded":
            raise HTTPException(
                status_code=409,
                detail=f"Research job is {job.status}, result is not ready",
            )
        return job.snapshot(include_result=True)


def main() -> None:
    import uvicorn

    host = os.getenv("ASTOCK_RESEARCH_API_HOST", "127.0.0.1")
    port = int(os.getenv("ASTOCK_RESEARCH_API_PORT", "8008"))
    access_log = os.getenv("ASTOCK_HTTP_ACCESS_LOG", "0").lower() in {"1", "true", "yes"}
    uvicorn.run(
        "research_api.app:app",
        host=host,
        port=port,
        reload=False,
        access_log=access_log,
    )


if __name__ == "__main__":
    main()
