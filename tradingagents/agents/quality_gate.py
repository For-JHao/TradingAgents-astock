from typing import Annotated
import re

from tradingagents.dataflows.interface import route_to_vendor

REPORT_FIELDS = {
    "market": "market_report",
    "social": "sentiment_report",
    "news": "news_report",
    "fundamentals": "fundamentals_report",
    "policy": "policy_report",
    "hot_money": "hot_money_report",
    "lockup": "lockup_report",
}

ANALYST_NAMES = {
    "market": "技术分析师",
    "social": "情绪分析师",
    "news": "新闻分析师",
    "fundamentals": "基本面分析师",
    "policy": "政策分析师",
    "hot_money": "游资追踪师",
    "lockup": "解禁监控师",
}

MIN_REPORT_LENGTH = 200

FAILURE_MARKERS = [
    "无法获取",
    "I cannot retrieve",
    "I don't have access",
    "unable to fetch",
    "工具调用失败",
]


def _is_listed_fund_ticker(ticker: str) -> bool:
    normalized = ticker.strip().upper()
    for suffix in (".SH", ".SZ", ".BJ"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
    for prefix in ("SH", "SZ", "BJ"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
    return bool(re.fullmatch(r"[15]\d{5}", normalized))


def _report_is_data_poor(report: str) -> bool:
    if not report or len(report.strip()) < MIN_REPORT_LENGTH:
        return True
    return (
        report.count("[数据缺失") >= 3
        or "数据源不可用" in report
        or "无法计算任何指标" in report
        or "无法确定" in report
    )


def _extract_profile_value(profile: str, label: str) -> str:
    pattern = re.compile(rf"^{re.escape(label)}:\s*(.+)$", re.MULTILINE)
    match = pattern.search(profile)
    return match.group(1).strip() if match else "N/A"


def _build_etf_guard_report(kind: str, ticker: str, trade_date: str, profile: str) -> str:
    name = _extract_profile_value(profile, "Name")
    nav = _extract_profile_value(profile, "Latest Unit NAV")
    nav_date = _extract_profile_value(profile, "Latest NAV Date")
    one_month = _extract_profile_value(profile, "1-Month Return (%)")
    three_month = _extract_profile_value(profile, "3-Month Return (%)")
    six_month = _extract_profile_value(profile, "6-Month Return (%)")
    one_year = _extract_profile_value(profile, "1-Year Return (%)")
    stock_position = _extract_profile_value(profile, "股票占净比")
    net_assets = _extract_profile_value(profile, "净资产")
    latest_close = _extract_profile_value(profile, "Latest Close")
    price_return_20 = _extract_profile_value(profile, "20-bar Price Return")
    avg_volume_20 = _extract_profile_value(profile, "20-bar Average Volume")

    if kind == "market":
        title = "ETF 技术面与流动性分析"
        focus = (
            "本节由 ETF Data Guard 基于东财 ETF 数据源确定性生成，用于替代原个股技术分析中误报的"
            "[数据缺失]。重点观察场内价格、净值走势、成交活跃度和短中期收益表现。"
        )
        table = [
            "| 必采项目 | 状态 | 数据 |",
            "|---|---|---|",
            f"| 最新净值/日期 | 已获取 | {nav_date}，单位净值 {nav} |",
            f"| 近 1月/3月/6月/1年收益 | 已获取 | {one_month}% / {three_month}% / {six_month}% / {one_year}% |",
            f"| 场内最新收盘 | 已获取 | {latest_close} |",
            f"| 20 日价格表现 | 已获取 | {price_return_20}% |",
            f"| 20 日平均成交量 | 已获取 | {avg_volume_20} |",
        ]
    elif kind == "hot_money":
        title = "ETF 资金面与持仓暴露分析"
        focus = (
            "ETF 不适用个股龙虎榜、内部人交易和大股东减持逻辑。本节改用场内成交、资产配置、"
            "规模变化和前十大持仓暴露来评估资金面。"
        )
        table = [
            "| 必采项目 | 状态 | 数据 |",
            "|---|---|---|",
            f"| 股票仓位 | 已获取 | {stock_position}% |",
            f"| 净资产/规模线索 | 已获取 | {net_assets} |",
            f"| 20 日平均成交量 | 已获取 | {avg_volume_20} |",
            f"| 20 日价格表现 | 已获取 | {price_return_20}% |",
            "| ETF 持仓暴露 | 已获取 | 见下方东财 ETF 原始数据中的 Top Holding Codes |",
        ]
    else:
        title = "ETF 基金画像与基本面替代分析"
        focus = (
            "ETF 不应套用上市公司 PE/PB、营收、净利润、ROE、三张表或 EPS。"
            "本节以净值表现、资产配置、规模、基金经理和持仓暴露作为基本面替代框架。"
        )
        table = [
            "| 必采项目 | 状态 | 数据 |",
            "|---|---|---|",
            f"| 官方名称 | 已获取 | {name}（{ticker}） |",
            f"| 净值日期/单位净值 | 已获取 | {nav_date} / {nav} |",
            f"| 近 1月/3月/6月/1年收益 | 已获取 | {one_month}% / {three_month}% / {six_month}% / {one_year}% |",
            f"| 股票仓位 | 已获取 | {stock_position}% |",
            f"| 净资产/规模线索 | 已获取 | {net_assets} |",
        ]

    return (
        f"## {title}\n\n"
        f"**标的**：{name}（{ticker}）  \n"
        f"**分析日期**：{trade_date}  \n"
        f"**数据源**：东方财富基金页 + push2his\n\n"
        f"{focus}\n\n"
        + "\n".join(table)
        + "\n\n### 东财 ETF 原始数据摘录\n\n"
        + profile
    )


def create_etf_data_guard():
    """Patch ETF reports with deterministic ETF data before debate/quality gate."""

    def etf_data_guard_node(state) -> dict:
        ticker = state["company_of_interest"]
        trade_date = state["trade_date"]
        if not _is_listed_fund_ticker(ticker):
            return {}

        try:
            profile = route_to_vendor("get_etf_profile", ticker, trade_date)
        except Exception as exc:
            return {
                "data_quality_summary": (
                    f"ETF Data Guard 未能获取 {ticker} 的东财 ETF 数据: {type(exc).__name__}: {exc}"
                )
            }

        updates = {}
        for field, kind in (
            ("market_report", "market"),
            ("fundamentals_report", "fundamentals"),
            ("hot_money_report", "hot_money"),
        ):
            current = state.get(field, "")
            guard_report = _build_etf_guard_report(kind, ticker, trade_date, profile)
            if _report_is_data_poor(current):
                updates[field] = guard_report
            elif "东财 ETF 原始数据摘录" not in current:
                updates[field] = current + "\n\n---\n\n" + guard_report
        return updates

    return etf_data_guard_node


def _hard_check_report(analyst_type: str, report: str) -> tuple:
    """Run hard checks on a single report. Returns (grade, detail)."""
    if not report or not report.strip():
        return ("F", "报告为空")

    length = len(report.strip())
    if length < MIN_REPORT_LENGTH:
        return ("D", f"报告过短 ({length} chars < {MIN_REPORT_LENGTH})")

    failure_count = sum(1 for m in FAILURE_MARKERS if m in report)
    stripped = report
    for m in FAILURE_MARKERS:
        stripped = stripped.replace(m, "")
    if failure_count > 0 and len(stripped.strip()) < MIN_REPORT_LENGTH:
        return ("D", f"报告主要由失败信息构成 ({failure_count} 处)")

    has_table = "|" in report and "---" in report
    missing_count = report.count("[数据缺失")

    issues = []
    if not has_table:
        issues.append("缺少汇总表格")
    if missing_count > 0:
        issues.append(f"{missing_count} 处数据缺失")

    if missing_count >= 3:
        return ("C", "；".join(issues))
    if not has_table or missing_count > 0:
        return ("B", "；".join(issues) if issues else "基本合格")

    return ("A", f"完整 ({length} chars)")


def _build_review_prompt(
    reports: dict, trade_date: str, ticker: str
) -> str:
    """Build the LLM review prompt."""
    report_sections = []
    for analyst_type, field in REPORT_FIELDS.items():
        name = ANALYST_NAMES[analyst_type]
        content = reports.get(field, "（未运行）")
        if not content:
            content = "（报告为空）"
        if len(content) > 3000:
            content = content[:3000] + "\n... (truncated for review)"
        report_sections.append(f"### {name} ({analyst_type})\n{content}")

    all_reports = "\n\n".join(report_sections)

    return f"""你是数据质量审核员。以下是 7 位分析师对 {ticker} 在 {trade_date} 的研究报告。请逐一审核。

{all_reports}

---

请按以下格式输出审核结果（不要输出其他内容）：

## 数据质量审核报告

**标的**: {ticker} | **日期**: {trade_date}

| 分析师 | 评级 | 数据时效 | 缺失项 | 备注 |
|--------|------|----------|--------|------|
| 技术分析师 | A/B/C/D/F | 是否匹配交易日 | 列出缺失的必采项 | 简要说明 |
| 情绪分析师 | ... | ... | ... | ... |
| 新闻分析师 | ... | ... | ... | ... |
| 基本面分析师 | ... | ... | ... | ... |
| 政策分析师 | ... | ... | ... | ... |
| 游资追踪师 | ... | ... | ... | ... |
| 解禁监控师 | ... | ... | ... | ... |

**整体评级**: A/B/C/D/F
**数据可信度**: 高/中/低
**建议**: （如有数据缺失，提醒辩论阶段谨慎使用该报告）

评级标准：
- A: 必采清单全部覆盖，数据时效匹配，有汇总表格
- B: 缺少 1-2 项非关键数据，整体可用
- C: 缺少 3+ 项或有数据时效问题，需谨慎使用
- D: 大量缺失或主要为失败信息，可信度低
- F: 报告为空或完全无效
"""


def create_quality_gate(llm):
    """Factory for the data quality gate node.

    Sits between the last analyst Msg Clear and Bull Researcher.
    Layer 1: hard checks (code). Layer 2: LLM review (one call).
    Writes data_quality_summary to state for downstream consumers.
    """

    def quality_gate_node(state) -> dict:
        trade_date = state["trade_date"]
        ticker = state["company_of_interest"]

        reports = {}
        for analyst_type, field in REPORT_FIELDS.items():
            reports[field] = state.get(field, "")

        hard_results = {}
        for analyst_type, field in REPORT_FIELDS.items():
            grade, detail = _hard_check_report(analyst_type, reports[field])
            hard_results[analyst_type] = (grade, detail)

        hard_summary_lines = []
        for analyst_type, (grade, detail) in hard_results.items():
            name = ANALYST_NAMES[analyst_type]
            hard_summary_lines.append(f"- {name}: [{grade}] {detail}")
        hard_summary = "\n".join(hard_summary_lines)

        fail_count = sum(
            1 for _, (g, _) in hard_results.items() if g in ("F", "D")
        )

        llm_review = ""
        if fail_count < 4:
            try:
                review_prompt = _build_review_prompt(reports, trade_date, ticker)
                response = llm.invoke(review_prompt)
                llm_review = response.content
            except Exception as e:
                llm_review = f"（LLM 复审失败: {type(e).__name__}: {e}）"

        summary = (
            f"## 数据质量门控结果\n\n"
            f"**标的**: {ticker} | **交易日**: {trade_date}\n\n"
            f"### 硬检查结果\n{hard_summary}\n\n"
            f"### LLM 复审\n"
            f"{llm_review if llm_review else '（跳过 — 多数报告未通过硬检查）'}\n"
        )

        return {"data_quality_summary": summary}

    return quality_gate_node
