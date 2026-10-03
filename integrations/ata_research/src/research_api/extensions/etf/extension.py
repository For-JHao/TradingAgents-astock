"""Fund data and applicability, composed into the native research graph."""
from datetime import date, timedelta
from langchain_core.tools import tool
from tradingagents.graph.extensions import AnalystExtension, GraphExtensions
from tradingagents.agents.utils.agent_utils import get_stock_data as native_stock_data, get_indicators as native_indicators, get_news, get_global_news
from research_api.data.provider import verify_etf_identity, get_etf_profile as profile_data, _eastmoney_kline_fallback, _akshare_etf_kline
from research_api.storage.memory import SynchronizedMemoryLog

@tool
def get_etf_profile(ticker: str, curr_date: str) -> str:
    """Get verified listed-fund identity, NAV, holdings, scale and liquidity."""
    return profile_data(ticker, curr_date)

@tool
def get_stock_data(symbol: str, start_date: str, end_date: str) -> str:
    """Get front-adjusted fund OHLCV between start_date and end_date."""
    if not verify_etf_identity(symbol):
        return native_stock_data.invoke(dict(symbol=symbol, start_date=start_date, end_date=end_date))
    try:
        frame = _eastmoney_kline_fallback(symbol, start_date, end_date)
        if frame is None or frame.empty:
            frame = _akshare_etf_kline(symbol, start_date, end_date)
        if frame is None or frame.empty:
            return "[数据缺失: ETF前复权行情]"
        return "Source: Eastmoney/AKShare; adjustment=qfq; available at retrieval; historical prices only\n" + frame.to_csv(index=False)
    except Exception:
        return "[数据缺失: ETF前复权行情来源不可用]"

@tool
def get_indicators(symbol: str, indicator: str, curr_date: str, look_back_days: int = 30) -> str:
    """Get a fund technical indicator from the same qfq provider as fund prices."""
    if not verify_etf_identity(symbol):
        return native_indicators.invoke(dict(symbol=symbol, indicator=indicator, curr_date=curr_date, look_back_days=look_back_days))
    try:
        from stockstats import StockDataFrame
        end = date.fromisoformat(curr_date)
        frame = _eastmoney_kline_fallback(symbol, (end - timedelta(days=max(365, look_back_days * 3))).isoformat(), curr_date)
        if frame is None or frame.empty:
            return "[数据缺失: ETF技术指标行情]"
        frame = frame.rename(columns={key: key.lower() for key in frame.columns}).set_index("date")
        stock = StockDataFrame.retype(frame)
        return f"Source: Eastmoney qfq; indicator={indicator}\n" + stock[indicator].tail(look_back_days).to_string()
    except Exception:
        return "[数据缺失: ETF技术指标]"

def etf_extensions(ticker, trade_date):
    identity = verify_etf_identity(ticker)
    if not identity:
        # Fund candidates must not silently run company fundamentals on unknown identity.
        if ticker.startswith(("1", "5")):
            raise ValueError("instrument_identity_unverified")
        return None
    instruction = "这是经来源核验的场内基金。用 get_etf_profile 分析NAV、持仓、规模、流动性；公司三张表、个股解禁/龙虎榜不适用，不可标成已获取。缺失数据必须保留缺失说明。"
    def context(code):
        return f"Verified fund: {identity['name']} ({code}); source={identity['source']}; observed_at={identity['observed_at']}; analysis_date={trade_date}. 当前画像不能作为历史已知事实。"
    def precheck(state):
        return {"data_quality_summary": "ETF适用性：公司三张表、限售解禁和个股龙虎榜不适用；基金画像/NAV/持仓/规模/流动性缺失以工具及报告标记为准，不推定完整。"}
    return GraphExtensions(
        analysts={
            "market": AnalystExtension((get_stock_data, get_indicators, get_etf_profile), instruction),
            "social": AnalystExtension((get_news, get_stock_data, get_etf_profile), instruction),
            "news": AnalystExtension((get_news, get_global_news, get_etf_profile), instruction),
            "policy": AnalystExtension((get_news, get_global_news, get_etf_profile), instruction),
            "fundamentals": AnalystExtension((get_etf_profile,), instruction),
            "hot_money": AnalystExtension((get_stock_data, get_news, get_etf_profile), instruction),
            "lockup": AnalystExtension((get_etf_profile,), instruction),
        }, instrument_context=context, quality_precheck=precheck, memory_factory=SynchronizedMemoryLog,
    )
