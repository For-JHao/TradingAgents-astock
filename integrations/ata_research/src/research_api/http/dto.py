from typing import Any, Literal
from pydantic import BaseModel, Field

class ResearchJobRequest(BaseModel):
    model_config = {"extra": "forbid"}
    ticker: str = Field(description="6-digit A-share code or Chinese stock name")
    client_request_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
        description="Stable caller-generated idempotency reference.",
    )
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
    client_request_id: str | None = None
    status: str
    ticker: str
    requested_ticker: str
    trade_date: str


class ResearchJobStatus(BaseModel):
    job_id: str
    client_request_id: str | None = None
    status: Literal["queued", "running", "succeeded", "failed"]
    ticker: str
    requested_ticker: str
    trade_date: str
    created_at: float
    updated_at: float
    attempt: int = 0
    stage: str | None = None
    error: str | None = None
    error_type: str | None = None
    error_stage: str | None = None
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


class MarketQuotesRequest(BaseModel):
    codes: list[str] = Field(default_factory=list, description="A-share 6-digit code list")


class MarketCalendarProof(BaseModel):
    source: Literal["akshare-bundled-calendar"]
    sha256: str
    coverage_start: str
    coverage_end: str


class MarketQuoteItem(BaseModel):
    code: str
    name: str | None = None
    price: float
    last_close: float | None = None
    open: float | None = None
    change_pct: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None
    turnover_pct: float | None = None
    ts: float
    source: str | None = None
    quote_at: str | None = None
    session_status: Literal["trading", "closed"] | None = None
    latest_session_date: str | None = None
    session_closed_at: str | None = None
    calendar_proof: MarketCalendarProof | None = None


class MarketQuotesResponse(BaseModel):
    quotes: list[MarketQuoteItem]


class DailyMarketBarsRequest(BaseModel):
    codes: list[str] = Field(min_length=1, max_length=5)
    from_date: str
    to_date: str
    include_prior_session: bool = False


class DailyMarketBarItem(BaseModel):
    code: str
    trade_date: str
    close: float
    previous_close: float | None = None
    change_pct: float | None = None
    source: str
    adjustment_mode: Literal["qfq"] = "qfq"


class DailyMarketBarsResponse(BaseModel):
    bars: list[DailyMarketBarItem]
    unavailable: list[dict[str, str]]
