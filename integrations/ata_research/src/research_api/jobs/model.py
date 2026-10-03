import time
from typing import Any, Literal
from research_api.http.dto import ResearchJobRequest

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
        self.attempt = 0
        self.stage: str | None = "queued"
        self.error: str | None = None
        self.error_type: str | None = None
        self.error_stage: str | None = None
        self.error_traceback: str | None = None
        self.signal: str | None = None
        self.result: dict[str, Any] | None = None

    def snapshot(self, include_result: bool = False) -> dict[str, Any]:
        data: dict[str, Any] = {
            "job_id": self.job_id,
            "client_request_id": self.request.client_request_id,
            "status": self.status,
            "ticker": self.ticker,
            "requested_ticker": self.request.ticker,
            "trade_date": self.trade_date,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "attempt": self.attempt,
            "stage": self.stage,
            "error": self.error,
            "error_type": self.error_type,
            "error_stage": self.error_stage,
            "signal": self.signal,
        }
        if include_result:
            data["result"] = self.result
        return data

    def persistence_record(self) -> dict[str, Any]:
        return {
            **self.snapshot(include_result=True),
            "request": self.request.model_dump(mode="json"),
            "config": self.config,
            "selected_analysts": self.selected_analysts,
            "error_traceback": self.error_traceback,
        }

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "_Job":
        request = ResearchJobRequest.model_validate(record["request"])
        job = cls(
            job_id=str(record["job_id"]),
            request=request,
            ticker=str(record["ticker"]),
            trade_date=str(record["trade_date"]),
            config=dict(record["config"]),
            selected_analysts=list(record["selected_analysts"]),
        )
        job.status = str(record["status"])
        job.created_at = float(record["created_at"])
        job.updated_at = float(record["updated_at"])
        job.attempt = int(record.get("attempt") or 0)
        job.stage = record.get("stage")
        job.error = record.get("error")
        job.error_type = record.get("error_type")
        job.error_stage = record.get("error_stage")
        job.error_traceback = record.get("error_traceback")
        job.signal = record.get("signal")
        job.result = record.get("result")
        return job
