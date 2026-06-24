import pytest

from tradingagents.dataflows.a_stock import (
    _eastmoney_market_id,
    _get_prefix,
    _is_etf_like_code,
)


@pytest.mark.unit
def test_shanghai_etf_codes_use_shanghai_market_prefixes():
    assert _get_prefix("562060") == "sh"
    assert _eastmoney_market_id("562060") == 1


@pytest.mark.unit
def test_listed_fund_codes_are_detected():
    assert _is_etf_like_code("562060") is True
    assert _is_etf_like_code("159915") is True
    assert _is_etf_like_code("600519") is False
