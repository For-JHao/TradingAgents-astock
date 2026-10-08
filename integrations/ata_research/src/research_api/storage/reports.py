"""Publish complete report bytes without overwriting any existing report."""
import hashlib, json, os, tempfile
from pathlib import Path

def publish_report(state, root, ticker, trade_date, job_id, metadata):
    if state.get("company_of_interest") != ticker or str(state.get("trade_date")) != trade_date or not str(state.get("final_trade_decision", "")).strip():
        raise ValueError("engine_result_contract_invalid")
    document = {**state, "ata_research": {**metadata, "job_id": job_id}}
    # LangChain messages are private runtime state, not part of the old report contract.
    document.pop("messages", None)
    payload = json.dumps(document, ensure_ascii=False, indent=4, default=str).encode("utf-8")
    target = Path(root) / ticker / "TradingAgentsStrategy_logs" / f"full_states_log_{trade_date}_{job_id}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".ata-report-", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            # New report bytes are readable by the inherited ata-reports group.
            os.fchmod(output.fileno(), 0o640)
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.is_symlink() or target.read_bytes() != payload:
                raise ValueError("report_publish_conflict") from None
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return str(target.resolve())
    finally:
        Path(temporary).unlink(missing_ok=True)

def report_context_metadata(state, selected_analysts, memory_enabled=None):
    """Publish effective engine configuration and actual context, without altering it."""
    context = state.get("past_context")
    valid = isinstance(context, str) and isinstance(memory_enabled, bool)
    # Disabled memory with nonempty injected text is a contract mismatch.
    memory_used = memory_enabled if valid and (memory_enabled or not context) else None
    return {"contextVersion": 1, "selectedAnalysts": list(selected_analysts), "memoryUsed": memory_used}
