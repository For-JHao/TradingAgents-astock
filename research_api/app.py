from __future__ import annotations

import copy
import json
import os
import re
import threading
import time
import uuid
from datetime import date
from pathlib import Path
from typing import Any, Literal

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from tradingagents.dataflows.a_stock import resolve_ticker
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.graph.trading_graph import TradingAgentsGraph


ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")
load_dotenv(ROOT_DIR / ".env.enterprise", override=False)

ALLOWED_ANALYSTS = {
    "market",
    "social",
    "news",
    "fundamentals",
    "policy",
    "hot_money",
    "lockup",
}

DEFAULT_MODELS = {
    "minimax": ("MiniMax-M2.7-highspeed", "MiniMax-M2.7"),
    "deepseek": ("deepseek-chat", "deepseek-chat"),
    "qwen": ("qwen-plus", "qwen3.6-plus"),
    "glm": ("glm-5", "glm-5.1"),
    "openai": ("gpt-5.4-mini", "gpt-5.4"),
    "anthropic": ("claude-sonnet-4-6", "claude-sonnet-4-6"),
    "google": ("gemini-2.5-flash", "gemini-2.5-pro"),
    "xai": ("grok-4-1-fast-non-reasoning", "grok-4-0709"),
    "ollama": ("qwen3:latest", "qwen3:latest"),
}


class ResearchJobRequest(BaseModel):
    ticker: str = Field(description="6-digit A-share code or Chinese stock name")
    trade_date: str | None = Field(
        default=None,
        description="Analysis date in YYYY-MM-DD format. Defaults to today.",
    )
    llm_provider: str | None = None
    quick_think_llm: str | None = None
    deep_think_llm: str | None = None
    backend_url: str | None = None
    research_depth: int = Field(default=1, ge=1, le=5)
    output_language: str = "Chinese"
    selected_analysts: list[str] | None = None
    checkpoint_enabled: bool = False


class ResearchJobCreated(BaseModel):
    job_id: str
    status: str
    ticker: str
    requested_ticker: str
    trade_date: str


class ResearchJobStatus(BaseModel):
    job_id: str
    status: Literal["queued", "running", "succeeded", "failed"]
    ticker: str
    requested_ticker: str
    trade_date: str
    created_at: float
    updated_at: float
    error: str | None = None
    signal: str | None = None


class ResearchJobResult(ResearchJobStatus):
    result: dict[str, Any] | None = None


class StockCandidate(BaseModel):
    code: str
    name: str
    market: str | None = None
    quote_id: str | None = None


class StockSearchResponse(BaseModel):
    keyword: str
    candidates: list[StockCandidate]


class _Job:
    def __init__(
        self,
        *,
        job_id: str,
        request: ResearchJobRequest,
        ticker: str,
        trade_date: str,
        config: dict[str, Any],
        selected_analysts: list[str],
    ) -> None:
        now = time.time()
        self.job_id = job_id
        self.request = request
        self.ticker = ticker
        self.trade_date = trade_date
        self.config = config
        self.selected_analysts = selected_analysts
        self.status: Literal["queued", "running", "succeeded", "failed"] = "queued"
        self.created_at = now
        self.updated_at = now
        self.error: str | None = None
        self.signal: str | None = None
        self.result: dict[str, Any] | None = None

    def snapshot(self, include_result: bool = False) -> dict[str, Any]:
        data: dict[str, Any] = {
            "job_id": self.job_id,
            "status": self.status,
            "ticker": self.ticker,
            "requested_ticker": self.request.ticker,
            "trade_date": self.trade_date,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "error": self.error,
            "signal": self.signal,
        }
        if include_result:
            data["result"] = self.result
        return data


app = FastAPI(title="TradingAgents-Astock Research API", version="0.1.0")
_jobs: dict[str, _Job] = {}
_lock = threading.Lock()
_HAS_CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")
_TICKER_RE = re.compile(r"^(?:SH|SZ|BJ)?(\d{6})(?:\.(?:SH|SZ|BJ))?$", re.IGNORECASE)


def _choose_provider(request_provider: str | None) -> str:
    provider = (
        request_provider
        or os.getenv("ASTOCK_LLM_PROVIDER")
        or os.getenv("TRADINGAGENTS_LLM_PROVIDER")
    )
    if provider:
        return provider.lower()
    if os.getenv("MINIMAX_API_KEY"):
        return "minimax"
    if os.getenv("DEEPSEEK_API_KEY"):
        return "deepseek"
    return str(DEFAULT_CONFIG["llm_provider"]).lower()


def _model_defaults(provider: str) -> tuple[str, str]:
    return DEFAULT_MODELS.get(
        provider.lower(),
        (str(DEFAULT_CONFIG["quick_think_llm"]), str(DEFAULT_CONFIG["deep_think_llm"])),
    )


def _build_config(request: ResearchJobRequest) -> dict[str, Any]:
    config = copy.deepcopy(DEFAULT_CONFIG)
    provider = _choose_provider(request.llm_provider)
    default_quick, default_deep = _model_defaults(provider)

    config["llm_provider"] = provider
    config["quick_think_llm"] = (
        request.quick_think_llm
        or os.getenv("ASTOCK_QUICK_THINK_LLM")
        or os.getenv("TRADINGAGENTS_QUICK_THINK_LLM")
        or default_quick
    )
    config["deep_think_llm"] = (
        request.deep_think_llm
        or os.getenv("ASTOCK_DEEP_THINK_LLM")
        or os.getenv("TRADINGAGENTS_DEEP_THINK_LLM")
        or default_deep
    )
    backend_url = (
        request.backend_url
        or os.getenv("ASTOCK_BACKEND_URL")
        or os.getenv("BACKEND_URL")
        or ""
    ).strip()
    config["backend_url"] = backend_url or None
    config["max_debate_rounds"] = request.research_depth
    config["max_risk_discuss_rounds"] = request.research_depth
    config["output_language"] = request.output_language
    config["checkpoint_enabled"] = request.checkpoint_enabled
    return config


def _select_analysts(request: ResearchJobRequest) -> list[str]:
    analysts = request.selected_analysts or [
        "market",
        "social",
        "news",
        "fundamentals",
        "policy",
        "hot_money",
        "lockup",
    ]
    invalid = sorted(set(analysts) - ALLOWED_ANALYSTS)
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unsupported analysts: {invalid}")
    return analysts


def _search_tickers_via_eastmoney(user_input: str) -> list[StockCandidate]:
    """Search Chinese stock names through Eastmoney suggest API.

    This is a lightweight fallback for environments where mootdx cannot build
    the full name-code map during HTTP job creation.
    """
    try:
        response = requests.get(
            "https://searchapi.eastmoney.com/api/suggest/get",
            params={"input": user_input, "type": "14"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        response.raise_for_status()
        data = json.loads(response.content.decode("utf-8", errors="replace"))
    except Exception:
        return []

    table = data.get("QuotationCodeTable")
    if not isinstance(table, dict):
        return []

    rows = table.get("Data") or []
    if not isinstance(rows, list):
        return []

    candidates: list[StockCandidate] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("Code") or row.get("UnifiedCode") or "").strip()
        name = str(row.get("Name") or "").strip()
        classify = str(row.get("Classify") or "")
        security_type = str(row.get("SecurityType") or "")
        if re.fullmatch(r"\d{6}", code) and (
            not classify
            or classify == "AStock"
            or security_type in {"2", "25"}
        ):
            if code in seen:
                continue
            seen.add(code)
            candidates.append(
                StockCandidate(
                    code=code,
                    name=name or code,
                    market=str(row.get("SecurityTypeName") or row.get("JYS") or "") or None,
                    quote_id=str(row.get("QuoteID") or "") or None,
                )
            )
    return candidates


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

    try:
        return resolve_ticker(raw)
    except Exception as exc:
        if _HAS_CHINESE_RE.search(raw):
            try:
                fallback = _resolve_ticker_via_eastmoney(raw)
            except Exception:
                fallback = None
            if fallback:
                return fallback
        raise HTTPException(
            status_code=400,
            detail=(
                f"无法解析股票 '{user_input}'。请改用 6 位 A 股代码，"
                f"或检查名称是否正确。原始错误: {exc}"
            ),
        ) from exc


def _summarize_state(
    final_state: dict[str, Any],
    *,
    ticker: str,
    trade_date: str,
    signal: str,
    report_path: str | None = None,
) -> dict[str, Any]:
    final_decision = str(final_state.get("final_trade_decision", ""))
    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "signal": signal,
        "final_trade_decision": final_decision,
        "reports": {
            "market": final_state.get("market_report", ""),
            "sentiment": final_state.get("sentiment_report", ""),
            "news": final_state.get("news_report", ""),
            "fundamentals": final_state.get("fundamentals_report", ""),
            "policy": final_state.get("policy_report", ""),
            "hot_money": final_state.get("hot_money_report", ""),
            "lockup": final_state.get("lockup_report", ""),
            "investment_plan": final_state.get("investment_plan", ""),
            "trader_investment_plan": final_state.get("trader_investment_plan", ""),
        },
        "report_path": report_path or "",
    }


def _run_job(job_id: str) -> None:
    with _lock:
        job = _jobs[job_id]
        job.status = "running"
        job.updated_at = time.time()

    try:
        graph = TradingAgentsGraph(
            selected_analysts=job.selected_analysts,
            config=job.config,
            debug=False,
        )
        final_state, signal = graph.propagate(job.ticker, job.trade_date)
        result = _summarize_state(
            final_state,
            ticker=job.ticker,
            trade_date=job.trade_date,
            signal=signal,
            report_path=str(graph.last_log_path) if graph.last_log_path else None,
        )
        with _lock:
            job.status = "succeeded"
            job.signal = signal
            job.result = result
            job.updated_at = time.time()
    except Exception as exc:
        with _lock:
            job.status = "failed"
            job.error = str(exc)
            job.updated_at = time.time()


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "tradingagents-astock-research-api",
        "jobs": len(_jobs),
    }


@app.get("/research/search", response_model=StockSearchResponse)
def search_stocks(keyword: str) -> dict[str, Any]:
    raw = keyword.strip()
    if not raw:
        raise HTTPException(status_code=400, detail="keyword 不能为空")

    match = _TICKER_RE.fullmatch(raw)
    if match:
        code = match.group(1)
        return {
            "keyword": keyword,
            "candidates": [StockCandidate(code=code, name=code).model_dump()],
        }

    candidates = _search_tickers_via_eastmoney(raw)
    return {
        "keyword": keyword,
        "candidates": [item.model_dump() for item in candidates[:10]],
    }


@app.post("/research/jobs", response_model=ResearchJobCreated)
def create_research_job(request: ResearchJobRequest) -> dict[str, Any]:
    ticker = _resolve_request_ticker(request.ticker)
    trade_date = request.trade_date or date.today().isoformat()
    selected_analysts = _select_analysts(request)
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
        _jobs[job_id] = job

    thread = threading.Thread(target=_run_job, args=(job_id,), daemon=True)
    thread.start()

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
    uvicorn.run("research_api.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
