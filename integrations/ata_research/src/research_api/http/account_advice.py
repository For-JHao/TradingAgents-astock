"""Private, bounded account advice DTOs. Validation errors are never reflected."""
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, JsonValue

class StrictDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

class Identity(StrictDTO):
    bindingId: str = Field(min_length=1, max_length=128)
    sourceRequestId: str = Field(min_length=1, max_length=256)
    jobId: str = Field(min_length=1, max_length=128)
    reportId: str = Field(min_length=1, max_length=8192)
    ticker: str = Field(pattern=r"^[0-9]{6}$")
    tradeDate: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    metadata: dict[str, JsonValue]
    sourceChecksum: str = Field(pattern=r"^[a-f0-9]{64}$")

class AnalystReport(StrictDTO):
    role: Literal["market", "social", "news", "fundamentals", "policy", "hot_money", "lockup"]
    status: Literal["selected", "not_selected"]
    content: str | None

class DataQuality(StrictDTO):
    summary: str
    missingTasks: list[JsonValue]
    complete: bool

class FinalDecision(StrictDTO):
    content: str = Field(min_length=1)
    rating: str | None = None

class ResearchContext(StrictDTO):
    schemaVersion: Literal[1]
    identity: Identity
    analystReports: list[AnalystReport] = Field(min_length=1, max_length=7)
    dataQuality: DataQuality
    investmentDebate: dict[str, JsonValue]
    investmentPlan: str = Field(min_length=1)
    traderPlan: str = Field(min_length=1)
    riskDebate: dict[str, JsonValue]
    finalDecision: FinalDecision
    priorContext: str | None

class Position(StrictDTO):
    ticker: str = Field(pattern=r"^[0-9]{6}$")
    market: Literal["SSE", "SZSE"]
    companyName: str = Field(min_length=1, max_length=200)
    averageCost: str = Field(pattern=r"^(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,6})?$")
    quantity: str = Field(pattern=r"^[1-9][0-9]{0,11}$")

class Profile(StrictDTO):
    schemaVersion: Literal[1]
    revision: int = Field(ge=1)
    currency: Literal["CNY"]
    cashAvailable: str | None = Field(pattern=r"^(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,2})?$")
    positions: list[Position] = Field(min_length=1, max_length=100)
    updatedAt: str | None = Field(max_length=40)

class Account(StrictDTO):
    profile: Profile
    capturedAt: str = Field(min_length=1, max_length=40)

class CalendarProof(StrictDTO):
    source: str = Field(max_length=100)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    coverageStart: str = Field(max_length=10)
    coverageEnd: str = Field(max_length=10)

class ValuationRow(StrictDTO):
    ticker: str = Field(pattern=r"^[0-9]{6}$")
    market: Literal["SSE", "SZSE"]
    price: str | None
    source: str | None
    quoteAt: str | None
    receivedAt: str
    latestSessionDate: str | None = None
    sessionClosedAt: str | None = None
    calendarProof: CalendarProof | None = None
    sessionStatus: Literal["trading", "closed", "unknown"]
    freshnessStatus: Literal["fresh", "last_session", "stale", "unavailable"]
    marketValue: str | None
    profitLoss: str | None
    profitLossPercent: str | None

class Valuation(StrictDTO):
    valuationAt: str = Field(min_length=1, max_length=40)
    rows: list[ValuationRow] = Field(max_length=100)
    marketValue: str | None
    totalAssets: str | None
    cashRatio: str | None
    complete: bool

class Payload(StrictDTO):
    researchContext: ResearchContext
    account: Account
    valuation: Valuation

class Invocation(StrictDTO):
    schemaVersion: Literal[1]
    reportId: str = Field(min_length=1, max_length=8192)
    ticker: str = Field(pattern=r"^[0-9]{6}$")
    tradeDate: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    modelIdentity: str = Field(pattern=r"^[a-f0-9]{64}$")
    timeoutMs: int = Field(ge=1, le=900000)
    payload: Payload

class ModelLimit(StrictDTO):
    provider: str = Field(min_length=1, max_length=40)
    model: str = Field(min_length=1, max_length=128)
    backend_url: str = Field(min_length=1, max_length=2048)
    context_window: int = Field(ge=1, le=2000000)
    output_reserve: int = Field(ge=1, le=32768)
