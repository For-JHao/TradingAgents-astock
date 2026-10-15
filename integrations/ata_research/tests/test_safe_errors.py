"""Finite diagnostics preserve privacy and the existing failed-data marker."""
import json
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
import requests
from langchain_core.messages import ToolMessage
from research_api.http.errors import safe_error as http_safe_error
from tradingagents.safe_errors import safe_error

SENTINEL = "secret-account /private/credential https://user:password@invalid/api Authorization: bearer-secret body-secret"


@pytest.mark.parametrize("error,category", [
    (TimeoutError(SENTINEL), "timeout"),
    (requests.exceptions.Timeout(SENTINEL), "timeout"),
    (requests.exceptions.ConnectTimeout(SENTINEL), "timeout"),
    (httpx.ReadTimeout(SENTINEL), "timeout"),
    (ConnectionError(SENTINEL), "connection_error"),
    (requests.exceptions.ConnectionError(SENTINEL), "connection_error"),
    (httpx.ConnectError(SENTINEL), "connection_error"),
    (httpx.ReadError(SENTINEL), "connection_error"),
    (requests.exceptions.HTTPError(SENTINEL), "http_error"),
    (httpx.HTTPStatusError(SENTINEL, request=None, response=None), "http_error"),
    (json.JSONDecodeError(SENTINEL, SENTINEL, 0), "json_decode_error"),
    (requests.exceptions.JSONDecodeError(SENTINEL, SENTINEL, 0), "json_decode_error"),
    (TypeError(SENTINEL), "type_error"),
    (AttributeError(SENTINEL), "attribute_error"),
    (KeyError(SENTINEL), "key_error"),
])
def test_fixed_real_types_share_one_projection(error, category):
    assert http_safe_error is safe_error
    text = safe_error(error)
    assert text == f"Error: external_operation_failed (data) [category={category}]"
    assert SENTINEL not in text


@pytest.mark.parametrize("stage", ["data", "http", "tool", "missing_data", "graph_initialization",
                                    "graph_execution", "engine_finalization", "report_publication", "job_commit", "unknown"])
def test_unknown_exception_retains_original_bytes_for_known_stages(stage):
    assert safe_error(RuntimeError(SENTINEL), stage) == f"Error: external_operation_failed ({stage})"


def test_does_not_read_arbitrary_exception_attributes_or_spoofed_type_names():
    class Hostile(RuntimeError):
        def __str__(self):
            raise AssertionError("str must not run")
        def __repr__(self):
            raise AssertionError("repr must not run")
        def __getattribute__(self, name):
            if name in {"cause", "__cause__", "message", "response", "request", "status_code", "headers", "body", "url", "path", "args"}:
                raise AssertionError("exception attribute must not be read")
            return super().__getattribute__(name)
    assert safe_error(Hostile(SENTINEL)) == "Error: external_operation_failed (data)"
    for name in ("TimeoutError", "ConnectionError", "HTTPError", "JSONDecodeError"):
        spoof = type(name, (RuntimeError,), {"status_code": 401})(SENTINEL)
        assert safe_error(spoof) == "Error: external_operation_failed (data)"
    class HostileTimeout(TimeoutError, Hostile):
        pass
    assert safe_error(HostileTimeout(SENTINEL)) == "Error: external_operation_failed (data) [category=timeout]"
    assert safe_error({"message": SENTINEL, "status_code": 429}) == "Error: external_operation_failed (data)"


def test_invalid_stage_cannot_inject_payload_or_run_string_methods():
    class HostileStage(str):
        def __hash__(self):
            raise AssertionError("hash must not run")
        def __str__(self):
            raise AssertionError("str must not run")
    for stage in (SENTINEL, None, {}, [], 1, HostileStage("http")):
        assert safe_error(RuntimeError(SENTINEL), stage) == "Error: external_operation_failed (data)"


def test_existing_stock_catches_and_missing_recorder_keep_failures_and_category(tmp_path, caplog):
    from tradingagents.dataflows import a_stock, missing_data
    from tradingagents.dataflows.config import runtime_context, assert_tracking_healthy
    with runtime_context({"missing_data_dir": str(tmp_path / "missing")}):
        with patch.object(a_stock, "_em_get", side_effect=requests.exceptions.Timeout(SENTINEL)):
            content = a_stock.get_fund_flow("601689", "2026-10-15")
        assert "[category=timeout]" in content
        # A non-string F10 payload simulates the unchanged upstream .strip() risk; no TCP request.
        with patch.object(a_stock, "_mootdx_call", return_value={"unexpected": "public fixture"}):
            f10_content = a_stock.get_insider_transactions("601689")
        assert "[category=attribute_error]" in f10_content
        for name, text in (("get_fund_flow", content), ("get_insider_transactions", f10_content)):
            request = SimpleNamespace(state={"company_of_interest": "601689", "trade_date": "2026-10-15"},
                                      tool_call={"name": name, "args": {"ticker": "601689"}, "id": name})
            output = ToolMessage(content=text, name=name, tool_call_id=name, status="success")
            recorded = missing_data.make_tool_call_recorder("hot_money")(request, lambda _: output)
            assert recorded is output
        tasks = missing_data.get_missing_tasks("601689", "2026-10-15", active_only=True)
        assert len(tasks) == 2
        assert all(task["severity"] == "failed" and task["status"] == "active" for task in tasks)
        assert {task["tool_name"] for task in tasks} == {"get_fund_flow", "get_insider_transactions"}
        assert any("[category=timeout]" in task["message"] for task in tasks)
        assert any("[category=attribute_error]" in task["message"] for task in tasks)
        assert_tracking_healthy()
        serialized = json.dumps(tasks) + (tmp_path / "missing" / "index.json").read_text() + caplog.text
        assert SENTINEL not in serialized
