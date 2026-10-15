"""Native source contracts; no network, model, user cache or service writes."""
from datetime import date, datetime

import pytest
import requests

from tradingagents.dataflows import a_stock, missing_data
from tradingagents.dataflows.config import runtime_context

SENTINEL = "secret-source-SENTINEL /private/key https://user:password@invalid/"


class Response:
    def __init__(self, payload=None, status=200, invalid_json=False):
        self.payload = payload
        self.status = status
        self.invalid_json = invalid_json

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(SENTINEL)

    def json(self):
        if self.invalid_json:
            raise requests.exceptions.JSONDecodeError(SENTINEL, "", 0)
        return self.payload


def fail(*args, **kwargs):
    raise requests.ConnectionError(SENTINEL)


@pytest.fixture(autouse=True)
def isolated_sources(monkeypatch, tmp_path):
    def unexpected(*args, **kwargs):
        raise AssertionError("unexpected external call")
    monkeypatch.setattr(a_stock, "_em_get", unexpected)
    monkeypatch.setattr(a_stock._requests, "get", unexpected)
    monkeypatch.setattr(a_stock, "_mootdx_call", unexpected)
    monkeypatch.setattr(a_stock, "_market_today", lambda: date(2026, 10, 15))
    with runtime_context({"missing_data_dir": str(tmp_path / "missing")}):
        yield


def page():
    return {
        "sjkzr": [{"HOLDER_NAME": "Example controller"}],
        "gdrs": [{"END_DATE": "2026-06-30 00:00:00", "HOLDER_TOTAL_NUM": 1000,
                  "TOTAL_NUM_RATIO": -2, "AVG_FREE_SHARES": 12000}],
        "sdgd": [{"END_DATE": "2026-06-30", "HOLDER_NAME": "Example holder",
                  "HOLD_NUM": 1000000, "HOLD_NUM_RATIO": 12, "HOLD_NUM_CHANGE": -10000}],
        "sdltgd": [],
    }


def executive():
    return {"SECURITY_CODE": "601689", "EXECUTIVE_NAME": "Example executive",
            "CHANGE_DATE": "2026-08-10", "CHANGE_NUM": -10000,
            "CHANGE_RATIO": -1, "AVERAGE_PRICE": 15}


def holder():
    return {"SECURITY_CODE": "601689", "HOLDER_NAME": "Example changing holder",
            "NOTICE_DATE": "2026-08-15", "END_DATE": "2026-08-10",
            "CHANGE_NUM": 3, "DIRECTION": "减持", "CHANGE_RATE": 999,
            "AFTER_HOLDER_NUM": 200, "HOLD_RATIO": 2}


def install_shareholder(monkeypatch, *, broken=None, page_payload=None,
                        executive_rows=None, holder_rows=None, custom=None):
    calls = []
    def get(url, *, params, headers=None, timeout):
        label = "page" if "PageAjax" in url else params["reportName"]
        calls.append((label, params, headers, timeout))
        if label == broken:
            return fail()
        if custom and label in custom:
            return custom[label]
        if label == "page":
            return Response(page() if page_payload is None else page_payload)
        rows = (executive_rows if executive_rows is not None else [executive()]) if label == "RPT_EXECUTIVE_HOLD_CHANGE" else (holder_rows if holder_rows is not None else [holder()])
        return Response({"success": True, "result": {"data": rows}})
    monkeypatch.setattr(a_stock, "_em_get", get)
    return calls


def record(output, tool, args):
    entry = missing_data.record_tool_result(ticker="601689", trade_date="2026-10-15",
        stage="fundamentals", tool_name=tool, args=args, content=output)
    state = {}
    missing_data.attach_missing_data_snapshot(state, "601689", "2026-10-15")
    return entry, state


def test_native_f10_text_has_priority_and_no_http(monkeypatch):
    calls = []
    def native(method, **kwargs):
        calls.append((method, kwargs))
        return "Original F10 text"
    monkeypatch.setattr(a_stock, "_mootdx_call", native)
    output = a_stock.get_insider_transactions("601689")
    assert output.endswith("Original F10 text")
    assert "# Data source: mootdx F10" in output
    assert calls == [("F10", {"symbol": "601689", "name": "股东研究"})]


@pytest.mark.parametrize("native", [None, "", "  ", {}, {"body": "not F10 text"}, 1])
def test_empty_or_nontext_native_uses_one_http_fallback(monkeypatch, native):
    monkeypatch.setattr(a_stock, "_mootdx_call", lambda *a, **k: native)
    calls = install_shareholder(monkeypatch)
    output = a_stock.get_insider_transactions("601689")
    assert "Example controller" in output and "Example executive" in output
    assert len(calls) == 3
    assert calls[0][1] == {"code": "SH601689"}
    assert all(x[3] == 15 for x in calls)
    assert calls[1][1]["pageSize"] == "10"


def test_fallback_units_direction_dates_and_limits(monkeypatch):
    monkeypatch.setattr(a_stock, "_mootdx_call", fail)
    install_shareholder(monkeypatch, executive_rows=[executive()] * 12, holder_rows=[holder()] * 12)
    output = a_stock.get_insider_transactions("601689")
    assert "减持 1.00万股" in output  # Executive CHANGE_NUM is shares.
    assert "减持 3.00万股" in output  # Holder CHANGE_NUM is ten-thousand shares.
    assert "200.00万股" in output and "100.00万股" in output
    assert "999" not in output  # Holder CHANGE_RATE does not supply direction.
    assert output.count("Example executive") == output.count("Example changing holder") == 10
    assert "2026-08-15 | 2026-08-10" in output and "2026-06-30" in output
    assert "不代表历史分析日已知事实" in output


@pytest.mark.parametrize("broken", ["page", "RPT_EXECUTIVE_HOLD_CHANGE", "RPT_SHARE_HOLDER_INCREASE"])
def test_partial_fallback_preserves_other_sections_and_records_missing(monkeypatch, broken):
    monkeypatch.setattr(a_stock, "_mootdx_call", fail)
    calls = install_shareholder(monkeypatch, broken=broken)
    output = a_stock.get_insider_transactions("601689")
    assert len(calls) == 3 and "[category=connection_error]" in output
    if broken != "page":
        assert "Example controller" in output
    if broken != "RPT_EXECUTIVE_HOLD_CHANGE":
        assert "Example executive" in output
    if broken != "RPT_SHARE_HOLDER_INCREASE":
        assert "Example changing holder" in output
    entry, state = record(output, "get_insider_transactions", {"ticker": "601689"})
    assert entry["severity"] == "partial" and entry["status"] == "active"
    assert state["missing_data_complete"] is False


def test_all_http_failures_not_no_records_or_secret_leak(monkeypatch, caplog):
    monkeypatch.setattr(a_stock, "_mootdx_call", fail)
    calls = []
    def all_fail(*a, **k):
        calls.append(1)
        return fail()
    monkeypatch.setattr(a_stock, "_em_get", all_fail)
    output = a_stock.get_insider_transactions("601689")
    assert len(calls) == 3 and output.startswith("Error retrieving insider/shareholder")
    assert "查询无记录" not in output
    assert SENTINEL not in output + caplog.text
    entry, state = record(output, "get_insider_transactions", {"ticker": "601689"})
    assert entry["severity"] == "failed" and state["missing_data_complete"] is False


def test_healthy_empty_shareholder_sources_remain_healthy(monkeypatch):
    monkeypatch.setattr(a_stock, "_mootdx_call", lambda *a, **k: None)
    install_shareholder(monkeypatch, page_payload={key: [] for key in page()}, executive_rows=[], holder_rows=[])
    output = a_stock.get_insider_transactions("601689")
    assert output.count("本次查询无记录") == 6
    assert "[数据缺失" not in output and "Error" not in output
    entry, state = record(output, "get_insider_transactions", {"ticker": "601689"})
    assert entry is None and state["missing_data_complete"] is True


def test_observed_datacenter_no_records_tuple_is_healthy(monkeypatch):
    empty = Response({"success": False, "code": 9201, "result": None})
    install_shareholder(monkeypatch, page_payload={key: [] for key in page()},
        custom={"RPT_EXECUTIVE_HOLD_CHANGE": empty, "RPT_SHARE_HOLDER_INCREASE": empty})
    output = a_stock._em_shareholder_research("601689")
    assert output.count("本次查询无记录") == 6 and "[数据缺失" not in output


@pytest.mark.parametrize("bad", [Response({}, status=404, invalid_json=True),
    Response(invalid_json=True), Response([]), Response({"success": False, "result": {"data": []}}),
    Response({"result": {"data": ["not a row"]}}),
    Response({"result": {"data": [{"SECURITY_CODE": "000001"}]}})])
def test_bad_shareholder_response_keeps_good_sections(monkeypatch, bad):
    install_shareholder(monkeypatch, custom={"RPT_EXECUTIVE_HOLD_CHANGE": bad})
    output = a_stock._em_shareholder_research("601689")
    assert "Example controller" in output and "Example changing holder" in output
    assert "[数据缺失: 股东HTTP高管增减持]" in output
    assert SENTINEL not in output


def test_all_malformed_payloads_are_failure_not_success(monkeypatch):
    monkeypatch.setattr(a_stock, "_em_get", lambda *a, **k: Response({}))
    output = a_stock._em_shareholder_research("601689")
    assert output.startswith("Error retrieving insider/shareholder")


def test_bad_subsection_does_not_discard_other_page_sections(monkeypatch):
    data = page(); data["sdgd"] = {"wrong": "shape"}
    install_shareholder(monkeypatch, page_payload=data)
    output = a_stock._em_shareholder_research("601689")
    assert "Example controller" in output and "Example executive" in output
    assert "[数据缺失: 股东HTTP十大股东" in output


def test_missing_fields_or_invalid_values_cannot_look_complete(monkeypatch):
    data = page(); data["sdgd"] = [{}]
    bad = holder(); bad["NOTICE_DATE"] = "unknown"; bad["CHANGE_NUM"] = "nan"
    bad["DIRECTION"] = ""; bad["AFTER_HOLDER_NUM"] = float("inf")
    install_shareholder(monkeypatch, page_payload=data, holder_rows=[bad])
    output = a_stock._em_shareholder_research("601689")
    assert "[数据缺失: 股东HTTP十大股东" in output
    assert "[数据缺失: 股东HTTP股东增减持关键日期或数值]" in output
    assert "N/A" in output and "nan" not in output and "inf" not in output
    entry, state = record(output, "get_insider_transactions", {"ticker": "601689"})
    assert entry["severity"] == "partial" and state["missing_data_complete"] is False


@pytest.mark.parametrize("value", [None, "", "bad", "nan", float("inf"), True])
def test_invalid_numeric_values_are_not_fabricated(value):
    assert a_stock._shareholder_quantity(value) == "N/A"
    assert a_stock._shareholder_pct(value) == "N/A"
    assert a_stock._shareholder_change(value) == "N/A"


def test_unknown_date_and_holder_direction_not_guessed():
    assert a_stock._shareholder_day("not a date") == "N/A"
    assert a_stock._shareholder_day(None) == "N/A"
    assert a_stock._shareholder_change(10, unit="万股", direction="") == "方向未知 10.00万股"


@pytest.mark.parametrize("direction", [None, "", "invalid", 1])
def test_holder_invalid_direction_stays_unknown_in_rendered_source(monkeypatch, direction):
    bad = holder(); bad["DIRECTION"] = direction; bad["CHANGE_NUM"] = 3
    install_shareholder(monkeypatch, holder_rows=[bad])
    output = a_stock._em_shareholder_research("601689")
    assert "方向未知 3.00万股" in output
    assert "增持 3.00万股" not in output and "减持 3.00万股" not in output
    assert "[数据缺失: 股东HTTP股东增减持关键日期或数值]" in output
    assert a_stock._shareholder_change(-3) == "减持 3股"


def test_realtime_success_history_failure_keeps_result_and_missing(monkeypatch):
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        if "daykline" in url:
            return fail()
        return Response({"data": {"klines": ["14:50,10000,20000,30000,40000,50000"]}})
    monkeypatch.setattr(a_stock, "_em_get", get)
    output = a_stock.get_fund_flow("601689", "2026-10-15")
    assert len(calls) == 2 and "Close: 主力净流入=1万元" in output
    assert "[数据缺失: 历史资金流查询失败]" in output
    entry, state = record(output, "get_fund_flow", {"ticker": "601689", "curr_date": "2026-10-15"})
    assert entry["severity"] == "partial" and state["missing_data_complete"] is False
    assert SENTINEL not in output


def test_realtime_failure_does_not_add_history_request(monkeypatch):
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return fail()
    monkeypatch.setattr(a_stock, "_em_get", get)
    output = a_stock.get_fund_flow("601689", "2026-10-15")
    assert len(calls) == 1
    assert output == "Error fetching fund flow for 601689: Error: external_operation_failed (data) [category=connection_error]"


def test_history_only_failure_preserves_original_failed_result(monkeypatch):
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return fail()
    monkeypatch.setattr(a_stock, "_em_get", get)
    output = a_stock.get_fund_flow("601689", "2026-10-01")
    assert len(calls) == 1 and "daykline" in calls[0]
    assert output.startswith("Error fetching fund flow for 601689:")
    entry, state = record(output, "get_fund_flow", {"ticker": "601689", "curr_date": "2026-10-01"})
    assert entry["severity"] == "failed" and state["missing_data_complete"] is False


def test_normal_fund_flow_format_and_request_contract(monkeypatch):
    class FrozenTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 15, 13, 0, 0, tzinfo=tz)
    monkeypatch.setattr(a_stock, "datetime", FrozenTime)
    calls = []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response({"data": {"klines": ["2026-10-14,10000,20000,30000,40000,50000"]}})
    monkeypatch.setattr(a_stock, "_em_get", get)
    output = a_stock.get_fund_flow("601689", "2026-10-15")
    assert [c[1]["timeout"] for c in calls] == [10, 10]
    assert calls[0][1]["params"]["klt"] == 1 and calls[1][1]["params"]["lmt"] == 20
    assert output == (
        "# Fund Flow for 601689 (A-stock)\n# Source: 东财 push2 (Eastmoney)\n"
        "# Retrieved: 2026-10-15 13:00:00\n\n"
        "## Realtime Minute Flow (主力/小单/中单/大单/超大单 净流入, 元)\n"
        "  2026-10-14: 主力=1万 大单=4万 超大单=5万\n\nClose: 主力净流入=1万元\n"
        "Signal: Net main force INFLOW (bullish)\n\n"
        "## Historical Daily Fund Flow (last 1 trading days)\n"
        "Date | 主力净流入(万) | 大单(万) | 中单(万) | 小单(万) | 超大单(万)\n"
        "  2026-10-14 | main=1 | large=4 | mid=3 | small=2 | super=5")


def install_news(monkeypatch, cls, em):
    def result(value):
        if isinstance(value, Exception):
            raise value
        return value
    monkeypatch.setattr(a_stock._requests, "get", lambda *a, **k: result(cls))
    monkeypatch.setattr(a_stock, "_em_get", lambda *a, **k: result(em))


def test_cls_404_html_does_not_parse_html_or_discard_other_source(monkeypatch, caplog):
    install_news(monkeypatch, Response(status=404, invalid_json=True),
                 Response({"data": {"fastNewsList": [{"title": "Example news", "summary": "Useful text"}]}}))
    output = a_stock.get_global_news("2026-10-15")
    assert "Example news" in output and "Useful text" in output
    assert "[category=http_error]" in output and "json_decode_error" not in output
    assert SENTINEL not in output + caplog.text
    entry, state = record(output, "get_global_news", {"curr_date": "2026-10-15"})
    assert entry["severity"] == "partial" and state["missing_data_complete"] is False


def test_all_news_sources_failed_is_not_healthy_empty(monkeypatch):
    install_news(monkeypatch, requests.ConnectionError(SENTINEL), Response(status=500))
    output = a_stock.get_global_news("2026-10-15")
    assert output.startswith("Error fetching global news:") and "No global news found" not in output
    assert SENTINEL not in output
    entry, state = record(output, "get_global_news", {"curr_date": "2026-10-15"})
    assert entry["severity"] == "failed" and state["missing_data_complete"] is False


def test_healthy_empty_news_output_unchanged(monkeypatch):
    install_news(monkeypatch, Response({"data": {"roll_data": []}}), Response({"data": {"fastNewsList": []}}))
    assert a_stock.get_global_news("2026-10-15") == "No global news found for 2026-10-15"


@pytest.mark.parametrize("source,field,value", [
    ("cls", "title", []), ("cls", "title", {}), ("cls", "content", 12),
    ("cls", "brief", None), ("em", "title", []), ("em", "title", {}),
    ("em", "summary", 12), ("em", "summary", None),
])
def test_bad_news_text_cannot_discard_healthy_alternate(monkeypatch, source, field, value):
    cls_rows = [{"title": "Healthy CLS", "content": "CLS text"}]
    em_rows = [{"title": "Healthy EM", "summary": "EM text"}]
    bad_rows = cls_rows if source == "cls" else em_rows
    bad_rows.append({"title": "Bad row", field: value})
    install_news(monkeypatch, Response({"data": {"roll_data": cls_rows}}),
                 Response({"data": {"fastNewsList": em_rows}}))
    output = a_stock.get_global_news("2026-10-15")
    assert ("Healthy EM" if source == "cls" else "Healthy CLS") in output
    assert ("Healthy CLS" if source == "cls" else "Healthy EM") not in output
    assert "Bad row" not in output and "[category=type_error]" in output
    entry, state = record(output, "get_global_news", {"curr_date": "2026-10-15"})
    assert entry["severity"] == "partial" and state["missing_data_complete"] is False


@pytest.mark.parametrize("bad", [Response([]), Response({}), Response({"data": None}),
    Response({"data": {"roll_data": None}}), Response({"data": {"roll_data": [1]}}), Response(invalid_json=True)])
def test_malformed_news_source_stays_missing_even_if_other_empty(monkeypatch, bad):
    install_news(monkeypatch, bad, Response({"data": {"fastNewsList": []}}))
    output = a_stock.get_global_news("2026-10-15")
    assert "[数据缺失: 财联社宏观新闻来源失败]" in output


def test_normal_news_output_and_source_order_unchanged(monkeypatch):
    install_news(monkeypatch, Response({"data": {"roll_data": [{"title": "CLS title", "content": "CLS content"}]}}),
                 Response({"data": {"fastNewsList": [{"title": "EM title", "summary": "EM content"}]}}))
    assert a_stock.get_global_news("2026-10-15", look_back_days=7) == (
        "## China & Global Market News, from 2026-10-08 to 2026-10-15:\n\n"
        "### CLS title (source: CLS Wire)\nCLS content\n\n"
        "### EM title (source: Eastmoney Global)\nEM content\n\n")
