"""Public-data provider for the platform and ETF extension only.

Company research and all other stock tools remain upstream-owned.
"""
from __future__ import annotations

from datetime import datetime, date
import json as _json
import os, logging, math, random, re as _re, threading, time, urllib.request, contextlib, io
import pandas as pd
import requests as _requests
from research_api.http.errors import safe_error
from tradingagents.dataflows.resources import eastmoney_budget
logger = logging.getLogger(__name__)
_ETF_CACHE_LOCK = threading.RLock()
_UA = "Mozilla/5.0"
_EM_SESSION = _requests.Session()
_EM_SESSION.headers.update({"User-Agent": _UA})
_EM_LOCK = threading.RLock()
_EM_MIN_INTERVAL = float(os.environ.get("EM_MIN_INTERVAL", "1.0"))
_em_last_call = [0.0]
_AK_ETF_SPOT_DF_CACHE = {"ts": 0.0, "df": None}
_AK_ETF_KLINE_CACHE = {}
_AK_ETF_SPOT_TTL_SECONDS = 3600
_AK_ETF_SPOT_BACKOFF_SECONDS = 1800
_AK_ETF_SPOT_BACKOFF_UNTIL = [0.0]

def safe_ticker_component(value):
    if not _re.fullmatch(r"\d{6}", value):
        raise ValueError("invalid_ticker")
    return value

def _get_prefix(code: str) -> str:
    """6-digit A-stock code -> market prefix for Tencent API."""
    if code.startswith(("4", "8", "92")):
        return "bj"
    if code.startswith(("5", "6", "9")):
        return "sh"
    elif code.startswith("8"):
        return "bj"
    return "sz"


def _eastmoney_market_id(code: str) -> int:
    """6-digit security code -> Eastmoney secid market id."""
    return 1 if code.startswith(("5", "6", "9")) and not code.startswith("92") else 0


def _is_etf_like_code(code: str) -> bool:
    """Best-effort A-share listed fund/ETF code detection."""
    return code.startswith(("1", "5"))


def _normalize_ticker(symbol: str) -> str:
    """Strip exchange prefix/suffix, return pure 6-digit code.

    Handles: '688017', 'SH688017', '688017.SH', 'sh688017'
    """
    s = symbol.strip().upper()
    # Remove .SH / .SZ / .BJ suffix
    for suffix in (".SH", ".SZ", ".BJ"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    # Remove SH / SZ / BJ prefix
    for prefix in ("SH", "SZ", "BJ"):
        if s.startswith(prefix):
            s = s[len(prefix) :]
            break
    return safe_ticker_component(s)


def _tencent_quote_at(value):
    """Tencent field 30 is exchange-local quote time, not HTTP receipt time."""
    from zoneinfo import ZoneInfo
    try:
        if not isinstance(value, str) or not _re.fullmatch(r"\d{14}", value):
            return None
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=ZoneInfo("Asia/Shanghai")).isoformat()
    except ValueError:
        return None

def _tencent_quote(codes: list[str]) -> dict[str, dict]:
    """Batch real-time quotes from Tencent Finance (qt.gtimg.cn).

    Returns dict[code] -> {name, price, pe_ttm, pb, mcap_yi, ...}
    """
    prefixed = [f"{_get_prefix(c)}{c}" for c in codes]
    url = "https://qt.gtimg.cn/q=" + ",".join(prefixed)
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0")
    resp = urllib.request.urlopen(req, timeout=10)
    raw = resp.read().decode("gbk")

    result = {}
    for line in raw.strip().split(";"):
        if not line.strip() or "=" not in line or '"' not in line:
            continue
        key = line.split("=")[0].split("_")[-1]
        vals = line.split('"')[1].split("~")
        if len(vals) < 53:
            continue
        code = key[2:]  # strip sh/sz/bj prefix
        candidate = {
            "name": vals[1],
            "source": "tencent",
            "quote_at": _tencent_quote_at(vals[30]),
            "price": finite_number(vals[3]),
            "last_close": finite_number(vals[4]),
            "open": finite_number(vals[5]),
            "change_pct": finite_number(vals[32]),
            "high": finite_number(vals[33]),
            "low": finite_number(vals[34]),
            "turnover_pct": finite_number(vals[38]),
            "pe_ttm": finite_number(vals[39]),
            "mcap_yi": finite_number(vals[44]),
            "float_mcap_yi": finite_number(vals[45]),
            "pb": finite_number(vals[46]),
            "limit_up": finite_number(vals[47]),
            "limit_down": finite_number(vals[48]),
            "pe_static": finite_number(vals[52]),
        }
        if finite_number(candidate["price"], positive=True) is not None:
            result[code] = candidate
    return result


def _is_rate_limited_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in ("429", "too many requests", "rate limit", "rate-limited", "rate limited")
    )


def _em_get(url, params=None, headers=None, timeout=15, **kwargs):
    """东财统一请求入口：自动节流 + 复用 session + 默认 UA。

    所有 eastmoney.com 接口都应通过它请求，避免多 Agent 高频拉数据被封 IP。
    串行限流：与上次东财请求间隔 < EM_MIN_INTERVAL 时 sleep 补足 + 0.1~0.5s 随机抖动。
    传入的 headers 会覆盖 session 默认 UA（用于保留各端点自己的 Referer/Origin）。
    """
    with eastmoney_budget(_EM_MIN_INTERVAL):
        return _EM_SESSION.get(url, params=params, headers=headers, timeout=timeout, **kwargs)


def _eastmoney_kline_fallback(
    code: str, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """Fetch daily K-line from Eastmoney push2his.

    This endpoint covers both A-shares and listed funds/ETFs, so it is a better
    fallback for 5xxxxx/1xxxxx ETF codes than mootdx or Sina.
    """
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"{_eastmoney_market_id(code)}.{code}",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": (start_date or "19900101").replace("-", ""),
        "end": (end_date or "20500101").replace("-", ""),
    }
    r = _em_get(url, params=params, timeout=15)
    r.raise_for_status()
    klines = (r.json().get("data") or {}).get("klines") or []
    if not klines:
        return pd.DataFrame()

    rows = []
    for line in klines:
        parts = str(line).split(",")
        if len(parts) < 6:
            continue
        rows.append(
            {
                "Date": parts[0],
                "Open": float(parts[1]),
                "Close": float(parts[2]),
                "High": float(parts[3]),
                "Low": float(parts[4]),
                "Volume": int(float(parts[5])),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["Date"] = pd.to_datetime(df["Date"])
    return df


def _eastmoney_security_snapshot(code: str) -> dict:
    """Fetch a broad Eastmoney quote snapshot for stocks and listed funds."""
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "fltt": "2",
        "invt": "2",
        "fields": ",".join(
            [
                "f43",  # latest price
                "f44",  # high
                "f45",  # low
                "f46",  # open
                "f47",  # volume
                "f48",  # amount
                "f57",  # code
                "f58",  # name
                "f60",  # previous close
                "f116",  # total market cap / fund market value where available
                "f117",  # float market cap
                "f127",  # industry/category
                "f168",  # turnover rate
                "f169",  # change
                "f170",  # change pct
            ]
        ),
        "secid": f"{_eastmoney_market_id(code)}.{code}",
    }
    r = _em_get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("data") or {}


def _extract_js_var(text: str, name: str) -> str | None:
    match = _re.search(rf"var\s+{_re.escape(name)}\s*=\s*(.*?);", text, _re.S)
    if not match:
        return None
    return match.group(1).strip()


def _parse_js_json_var(text: str, name: str, default=None):
    raw = _extract_js_var(text, name)
    if raw is None:
        return default
    try:
        return _json.loads(raw)
    except Exception:
        return default


def _parse_js_string_var(text: str, name: str) -> str:
    raw = _extract_js_var(text, name)
    if raw is None:
        return ""
    try:
        value = _json.loads(raw)
        return "" if value is None else str(value)
    except Exception:
        return raw.strip().strip('"').strip("'")


def _eastmoney_fund_script(code: str) -> str:
    """Fetch Eastmoney fund profile script for ETF/open fund metadata."""
    url = f"https://fund.eastmoney.com/pingzhongdata/{code}.js"
    headers = {
        "User-Agent": _UA,
        "Referer": "https://fund.eastmoney.com/",
    }
    r = _em_get(url, headers=headers, timeout=15)
    r.raise_for_status()
    return r.text


def _akshare_etf_spot(code: str) -> dict:
    """Fetch ETF spot row from AKShare when the optional dependency is available."""
    now = time.time()
    if now < _AK_ETF_SPOT_BACKOFF_UNTIL[0]:
        wait_left = int(_AK_ETF_SPOT_BACKOFF_UNTIL[0] - now)
        raise RuntimeError(f"AKShare ETF spot is cooling down after rate limit ({wait_left}s left)")

    try:
        import akshare as ak
    except Exception as exc:
        raise RuntimeError("AKShare is not installed") from exc

    cached_df = _AK_ETF_SPOT_DF_CACHE.get("df")
    cached_ts = float(_AK_ETF_SPOT_DF_CACHE.get("ts") or 0.0)
    if isinstance(cached_df, pd.DataFrame) and now - cached_ts < _AK_ETF_SPOT_TTL_SECONDS:
        df = cached_df
    else:
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                df = ak.fund_etf_spot_em()
        except Exception as exc:
            if _is_rate_limited_error(exc):
                _AK_ETF_SPOT_BACKOFF_UNTIL[0] = time.time() + _AK_ETF_SPOT_BACKOFF_SECONDS
            raise
        _AK_ETF_SPOT_DF_CACHE["df"] = df
        _AK_ETF_SPOT_DF_CACHE["ts"] = time.time()

    if df is None or df.empty:
        return {}
    code_col = "代码" if "代码" in df.columns else None
    if not code_col:
        return {}
    matched = df[df[code_col].astype(str).str.zfill(6) == code]
    if matched.empty:
        return {}
    row = matched.iloc[0].to_dict()
    return {str(k): v for k, v in row.items()}


def _akshare_etf_kline(
    code: str, start_date: str = None, end_date: str = None
) -> pd.DataFrame:
    """Fetch ETF daily K-line through AKShare fund_etf_hist_em."""
    cache_key = (
        code,
        start_date or "1990-01-01",
        end_date or "2050-01-01",
    )
    cached = _AK_ETF_KLINE_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()

    try:
        import akshare as ak
    except Exception as exc:
        raise RuntimeError("AKShare is not installed") from exc

    try:
        df = ak.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=(start_date or "1990-01-01").replace("-", ""),
            end_date=(end_date or "2050-01-01").replace("-", ""),
            adjust="qfq",
        )
    except Exception as exc:
        if _is_rate_limited_error(exc):
            _AK_ETF_SPOT_BACKOFF_UNTIL[0] = time.time() + _AK_ETF_SPOT_BACKOFF_SECONDS
        raise

    if df is None or df.empty:
        return pd.DataFrame()

    rename_candidates = {
        "日期": "Date",
        "开盘": "Open",
        "收盘": "Close",
        "最高": "High",
        "最低": "Low",
        "成交量": "Volume",
        "成交额": "Amount",
    }
    df = df.rename(columns={k: v for k, v in rename_candidates.items() if k in df.columns})
    required = ["Date", "Open", "High", "Low", "Close", "Volume"]
    if not all(col in df.columns for col in required):
        return pd.DataFrame()
    df = df[required]
    df["Date"] = pd.to_datetime(df["Date"])
    for col in ["Open", "High", "Low", "Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype(int)
    result = df.dropna(subset=["Date", "Open", "High", "Low", "Close"])
    _AK_ETF_KLINE_CACHE[cache_key] = result.copy()
    return result


def _format_latest_net_worth(items: list[dict]) -> list[str]:
    if not items:
        return []
    latest = items[-1]
    lines = ["--- Net Value / Price Proxy Trend (Eastmoney fund page) ---"]
    timestamp = latest.get("x")
    if timestamp:
        try:
            date_text = datetime.fromtimestamp(int(timestamp) / 1000).strftime("%Y-%m-%d")
        except Exception:
            date_text = str(timestamp)
        lines.append(f"Latest NAV Date: {date_text}")
    for key, label in {
        "y": "Latest Unit NAV",
        "equityReturn": "Daily NAV Return (%)",
        "unitMoney": "Distribution per Unit",
    }.items():
        value = latest.get(key)
        if value not in (None, "", "-"):
            lines.append(f"{label}: {value}")
    if len(items) >= 20:
        try:
            first = float(items[-20].get("y"))
            last = float(items[-1].get("y"))
            if first:
                lines.append(f"20-point NAV Return: {(last / first - 1) * 100:.2f}%")
        except Exception:
            pass
    return lines


def _format_asset_allocation(items) -> list[str]:
    if not items:
        return []
    lines = ["--- Asset Allocation ---"]
    if isinstance(items, dict) and isinstance(items.get("series"), list):
        categories = items.get("categories") or []
        if categories:
            lines.append(f"Allocation Date: {categories[-1]}")
        for series in items["series"]:
            if not isinstance(series, dict):
                continue
            name = series.get("name")
            data = series.get("data") or []
            if name and data:
                lines.append(f"{name}: {data[-1]}")
        return lines
    latest = items[-1] if isinstance(items, list) else items
    if isinstance(latest, dict):
        date_text = latest.get("date") or latest.get("x") or latest.get("FSRQ")
        if date_text:
            lines.append(f"Allocation Date: {date_text}")
        for key, label in {
            "gp": "Stock Position (%)",
            "zq": "Bond Position (%)",
            "xj": "Cash Position (%)",
            "jz": "Net Asset Value",
        }.items():
            value = latest.get(key)
            if value not in (None, "", "-"):
                lines.append(f"{label}: {value}")
    return lines


def _format_top_holdings(script_text: str) -> list[str]:
    holdings = _parse_js_json_var(script_text, "stockCodesNew", []) or []
    if not holdings:
        holdings = _parse_js_json_var(script_text, "stockCodes", []) or []
    if not holdings:
        return []
    codes = []
    for item in holdings[:10]:
        raw = str(item)
        code = raw.split(".")[-1][-6:]
        if _re.fullmatch(r"\d{6}", code):
            codes.append(code)
    if not codes:
        return []

    names = {}
    try:
        quotes = _tencent_quote(codes)
        names = {code: data.get("name") for code, data in quotes.items()}
    except Exception as e:
        logger.warning("ETF holding names failed: %s", safe_error(e))

    lines = ["--- Top Holding Codes (latest public fund page) ---"]
    for idx, code in enumerate(codes, start=1):
        name = names.get(code)
        lines.append(f"{idx}. {code}" + (f" {name}" if name else ""))
    return lines



_native_etf_spot = _akshare_etf_spot
_native_etf_kline = _akshare_etf_kline

def _akshare_etf_spot(code):
    with _ETF_CACHE_LOCK:
        return _native_etf_spot(code)

def _akshare_etf_kline(code, start_date=None, end_date=None):
    with _ETF_CACHE_LOCK:
        return _native_etf_kline(code, start_date, end_date)

def finite_number(value, positive=False):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and (not positive or number > 0) else None

def verify_etf_identity(code):
    # Prefix only selects candidates; identity comes from public fund evidence.
    if not _is_etf_like_code(code):
        return None
    try:
        row = _akshare_etf_spot(code)
        if str(row.get("代码", "")).zfill(6) == code and str(row.get("名称", "")).strip():
            return {"code": code, "name": str(row["名称"]), "source": "akshare_fund_etf_spot_em", "observed_at": datetime.now().isoformat()}
    except Exception:
        pass
    try:
        script = _eastmoney_fund_script(code)
        name = _parse_js_string_var(script, "fS_name")
        fund_code = _parse_js_string_var(script, "fS_code")
        if name and fund_code == code:
            return {"code": code, "name": name, "source": "eastmoney_fund_page", "observed_at": datetime.now().isoformat()}
    except Exception:
        pass
    return None

def get_etf_profile(ticker, curr_date=None):
    code = _normalize_ticker(ticker)
    identity = verify_etf_identity(code)
    if not identity:
        return "[数据缺失: 基金身份未获权威资料核验]"
    analysis_date = curr_date or date.today().isoformat()
    header = f"# ETF Analysis Data for {code} {identity['name']}\nSource: {identity['source']}\nObserved/available at: {identity['observed_at']}\nAnalysis date: {analysis_date}\n"
    if analysis_date < date.today().isoformat():
        return header + "[数据缺失: 当前基金画像不能视为历史时点已知数据；NAV、持仓、规模、实时流动性未获历史可得证据]"
    parts = [header, "公司三张表、限售解禁与个股龙虎榜：不适用（不表示已获取）。"]
    try:
        script = _eastmoney_fund_script(code)
        sections = [
            ("净值", _format_latest_net_worth(_parse_js_json_var(script, "Data_netWorthTrend", []))),
            ("资产配置", _format_asset_allocation(_parse_js_json_var(script, "Data_assetAllocation", []))),
            ("持仓", _format_top_holdings(script)),
            ("规模", _parse_js_json_var(script, "Data_fluctuationScale", None)),
        ]
        for label, data in sections:
            parts.append(f"{label}: " + (_json.dumps(data, ensure_ascii=False, default=str) if data else f"[数据缺失: {label}]"))
    except Exception:
        parts.append("[数据缺失: 基金净值、持仓、规模来源不可用]")
    try:
        row = _akshare_etf_spot(code)
        liquid = {key: finite_number(row.get(key)) for key in ("成交量", "成交额", "最新价")}
        parts.append("流动性（当前观测）: " + _json.dumps(liquid, ensure_ascii=False))
        if any(value is None for value in liquid.values()):
            parts.append("[数据缺失: 流动性部分字段]")
    except Exception:
        parts.append("[数据缺失: 流动性]")
    return "\n".join(parts)

def search_tickers(user_input: str) -> list[dict]:
    """Search Chinese stock names through Eastmoney suggest API.

    This is a lightweight fallback for environments where mootdx cannot build
    the full name-code map during HTTP job creation.
    """
    try:
        response = _em_get(
            "https://searchapi.eastmoney.com/api/suggest/get",
            params={"input": user_input, "type": "14"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        response.raise_for_status()
        data = response.json()
    except Exception:
        return []

    table = data.get("QuotationCodeTable")
    if not isinstance(table, dict):
        return []

    rows = table.get("Data") or []
    if not isinstance(rows, list):
        return []

    candidates: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("Code") or row.get("UnifiedCode") or "").strip()
        name = str(row.get("Name") or "").strip()
        classify = str(row.get("Classify") or "")
        security_type = str(row.get("SecurityType") or "")
        is_supported_security = (
            not classify
            or classify in {"AStock", "Fund", "ETF"}
            or security_type in {"2", "22", "24", "25"}

        )
        if _re.fullmatch(r"\d{6}", code) and is_supported_security:
            if code in seen:
                continue
            seen.add(code)
            candidates.append(
                dict(code=code, name=name or code, market=str(row.get("SecurityTypeName") or row.get("JYS") or "") or None, quote_id=str(row.get("QuoteID") or "") or None)
            )
    return candidates
