from datetime import date
from unittest.mock import patch

import pandas as pd
from fastapi import HTTPException

from research_api.app import DailyMarketBarsRequest, get_daily_market_bars


def test_daily_bars_are_qfq_and_include_prior_close() -> None:
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-07-01", "2026-07-02", "2026-07-03"]),
            "Close": [10.0, 10.5, 10.29],
        }
    )
    with (
        patch("research_api.app._resolve_request_ticker", return_value="600519"),
        patch("research_api.app._eastmoney_kline_fallback", return_value=frame),
    ):
        result = get_daily_market_bars(
            DailyMarketBarsRequest(
                codes=["600519"], from_date="2026-07-02", to_date="2026-07-03"
            )
        )

    assert result["unavailable"] == []
    assert result["bars"][0] == {
        "code": "600519",
        "trade_date": "2026-07-02",
        "close": 10.5,
        "previous_close": 10.0,
        "change_pct": 5.000000000000004,
        "source": "eastmoney_push2his",
        "adjustment_mode": "qfq",
    }


def test_daily_bars_validate_date_range() -> None:
    try:
        get_daily_market_bars(
            DailyMarketBarsRequest(
                codes=["600519"], from_date="2026-07-03", to_date="2026-07-02"
            )
        )
    except HTTPException as error:
        assert error.status_code == 400
    else:
        raise AssertionError("expected invalid range to fail")


def test_daily_bars_accept_string_dates_from_akshare_fallback() -> None:
    frame = pd.DataFrame({"Date": ["2026-07-01", "2026-07-02"], "Close": [10.0, 10.2]})
    with (
        patch("research_api.app._resolve_request_ticker", return_value="000001"),
        patch("research_api.app._eastmoney_kline_fallback", side_effect=RuntimeError("down")),
        patch("research_api.app._akshare_qfq_kline", return_value=frame),
    ):
        result = get_daily_market_bars(
            DailyMarketBarsRequest(
                codes=["000001"], from_date="2026-07-02", to_date="2026-07-02"
            )
        )

    assert result["unavailable"] == []
    assert result["bars"][0]["trade_date"] == "2026-07-02"
    assert result["bars"][0]["source"] == "akshare_qfq"


def test_daily_bars_can_return_the_latest_session_before_a_long_suspension() -> None:
    recent_frame = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-07-20"]),
            "Close": [12.0],
        }
    )
    full_frame = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2025-01-02", "2026-07-20"]),
            "Close": [10.0, 12.0],
        }
    )
    with (
        patch("research_api.app._resolve_request_ticker", return_value="600519"),
        patch(
            "research_api.app._eastmoney_kline_fallback",
            side_effect=[recent_frame, full_frame],
        ) as fetch,
    ):
        result = get_daily_market_bars(
            DailyMarketBarsRequest(
                codes=["600519"],
                from_date="2026-07-01",
                to_date="2026-07-23",
                include_prior_session=True,
            )
        )

    assert fetch.call_count == 2
    assert fetch.call_args.args[1] == "1990-01-01"
    assert [item["trade_date"] for item in result["bars"]] == [
        "2025-01-02",
        "2026-07-20",
    ]
