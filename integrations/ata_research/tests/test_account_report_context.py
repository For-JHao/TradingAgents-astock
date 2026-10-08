import json
from pathlib import Path
from research_api.storage.reports import publish_report, report_context_metadata
from research_api.data.provider import _tencent_quote_at


def test_context_metadata_publishes_actual_roles_and_memory_without_rewriting_body(tmp_path):
    full = "原始全部论证" * 12000
    state = {"company_of_interest": "600519", "trade_date": "2026-10-08", "final_trade_decision": full,
             "market_report": full, "past_context": "真实历史经验", "messages": ["private"]}
    metadata = report_context_metadata(state, ["market", "social"], True)
    assert metadata == {"contextVersion": 1, "selectedAnalysts": ["market", "social"], "memoryUsed": True}
    report = Path(publish_report(state, tmp_path, "600519", "2026-10-08", "job1", metadata))
    result = json.loads(report.read_text())
    assert result["final_trade_decision"] == full and result["market_report"] == full
    assert result["past_context"] == state["past_context"] and "messages" not in result
    assert result["ata_research"]["job_id"] == "job1"
    assert state["messages"] == ["private"]
    assert report_context_metadata({}, ["market"])["memoryUsed"] is None
    assert report_context_metadata({"past_context": ""}, ["market"], True)["memoryUsed"] is True
    assert report_context_metadata({"past_context": ""}, ["market"], False)["memoryUsed"] is False
    assert report_context_metadata(state, ["market"], False)["memoryUsed"] is None


def test_source_quote_time_does_not_fabricate_receipt_timestamp():
    assert _tencent_quote_at("20261008145930") == "2026-10-08T14:59:30+08:00"
    assert _tencent_quote_at(1791440000) is None
    assert _tencent_quote_at("20260230120000") is None
    assert _tencent_quote_at("") is None


def test_offline_calendar_session_proof_has_bounded_schema_and_real_coverage():
    from datetime import datetime
    from research_api.data.market_session import parse_calendar, market_session
    import pytest
    calendar = parse_calendar(b'["20260930","20261008","20261009","20261231"]')
    def session(value):
        return market_session(datetime.fromisoformat(value + "+08:00"), calendar)
    assert session("2026-10-08T10:00:00")["session_status"] == "trading"
    assert session("2026-10-08T14:00:00")["session_status"] == "trading"
    lunch = session("2026-10-08T12:00:00")
    assert lunch["session_closed_at"] == "2026-10-08T11:30:00+08:00"
    assert lunch["latest_session_date"] == "2026-10-08"
    assert session("2026-10-08T16:00:00")["session_closed_at"] == "2026-10-08T15:00:00+08:00"
    assert session("2026-10-07T16:00:00")["latest_session_date"] == "2026-09-30"
    assert session("2026-10-08T09:00:00")["latest_session_date"] == "2026-09-30"
    assert session("2027-01-01T09:00:00")["session_status"] is None
    assert lunch["calendar_proof"]["sha256"] == calendar.checksum
    for raw in (b'[]', b'["20260230","20261231"]', b'["20261008","20261008"]', b'["20261231","20261008"]', b' ' * 1_048_577):
        with pytest.raises(ValueError):
            parse_calendar(raw)
