import concurrent.futures, json, threading, time, os, stat, socket, subprocess, sys, urllib.request, urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import pytest
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool
from langgraph.prebuilt import ToolNode
from tradingagents.dataflows.config import runtime_context, get_config, mark_tracking_failed, assert_tracking_healthy
from tradingagents.dataflows import missing_data
from tradingagents.graph.extensions import GraphExtensions, AnalystExtension
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.agents.quality_gate import create_quality_gate
from research_api.storage.reports import publish_report
from research_api.storage.memory import SynchronizedMemoryLog
from research_api.jobs.execution import run_job
from research_api.jobs.model import _Job
from research_api.http.dto import ResearchJobRequest
from research_api.storage.job_store import ResearchJobStore
from research_api.data import provider

def execute_toolnode(node, state):
    from langgraph.graph import StateGraph, START, END
    from tradingagents.agents.utils.agent_states import AgentState
    graph = StateGraph(AgentState)
    graph.add_node("tools", node)
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    return graph.compile().invoke(state)

SENTINEL = "secret-SENTINEL /private/credential https://user:password@invalid/api"

@tool
def scoped_tool(value: str) -> str:
    """Read the runtime configuration in an actual ToolNode worker."""
    if value == "fail-tracking":
        mark_tracking_failed()
    if value == "raise":
        raise RuntimeError(SENTINEL)
    return get_config()["output_language"] + ":" + get_config()["missing_data_dir"]

def test_real_toolnode_threads_context_and_failure_flag(tmp_path):
    barrier = threading.Barrier(2)
    def run(language):
        config = {"output_language": language, "missing_data_dir": str(tmp_path / language)}
        with runtime_context(config):
            barrier.wait()
            node = ToolNode([scoped_tool], wrap_tool_call=missing_data.make_tool_call_recorder("market"))
            result = execute_toolnode(node, {"messages": [AIMessage(content="", tool_calls=[{"name": "scoped_tool", "args": {"value": "fail-tracking"}, "id": "one", "type": "tool_call"}])], "company_of_interest": "600519", "trade_date": "2026-10-03"})
            assert result["messages"][-1].content == language + ":" + str(tmp_path / language)
            with pytest.raises(RuntimeError, match="missing_data_tracking_failed"):
                assert_tracking_healthy()
            return result
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        list(pool.map(run, ["Chinese", "English"]))

@pytest.mark.parametrize("fault", ["bad-json", "bad-schema", "read", "stat", "write"])
def test_tracking_failure_never_becomes_complete(tmp_path, fault):
    root = tmp_path / fault
    root.mkdir()
    index = root / "index.json"
    original = b"[invalid" if fault == "bad-json" else b'[{"ticker":"600519"}]' if fault == "bad-schema" else b"[]"
    index.write_bytes(original)
    with runtime_context({"missing_data_dir": str(root)}):
        if fault == "write":
            with patch.object(Path, "replace", side_effect=OSError(SENTINEL)):
                node = ToolNode([scoped_tool], wrap_tool_call=missing_data.make_tool_call_recorder("market"), handle_tool_errors=lambda error: "Error: safe")
                execute_toolnode(node, {"messages": [AIMessage(content="", tool_calls=[{"name": "scoped_tool", "args": {"value": "raise"}, "id": "one", "type": "tool_call"}])], "company_of_interest": "600519", "trade_date": "2026-10-03"})
            with pytest.raises(RuntimeError, match="missing_data_tracking_failed"):
                missing_data.attach_missing_data_snapshot({}, "600519", "2026-10-03")
        else:
            target = Path.stat if fault == "stat" else Path.read_text
            def fail(path, *args, **kwargs):
                if path == index:
                    raise OSError(SENTINEL)
                return target(path, *args, **kwargs)
            if fault in ("stat", "read"):
                with patch.object(Path, "stat" if fault == "stat" else "read_text", fail):
                    with pytest.raises(RuntimeError, match="missing_data_tracking_failed"):
                        missing_data.attach_missing_data_snapshot({}, "600519", "2026-10-03")
            else:
                with pytest.raises(RuntimeError, match="missing_data_tracking_failed"):
                    missing_data.attach_missing_data_snapshot({}, "600519", "2026-10-03")
            with pytest.raises(RuntimeError, match="missing_data_tracking_failed"):
                assert_tracking_healthy()
    assert index.read_bytes() == original

class StubLLM(RunnableLambda):
    def __init__(self):
        self.bound = []
        self.prompts = []
        super().__init__(self.respond)
    def respond(self, value):
        self.prompts.append(str(value))
        return AIMessage(content="A sufficiently detailed report " * 50)
    def bind_tools(self, tools, **kwargs):
        self.bound.append([item.name for item in tools])
        return self

def test_same_tools_bind_model_and_execution_and_native_default(tmp_path):
    from tradingagents.agents.analysts.market_analyst import create_market_analyst
    llm = StubLLM()
    extensions = GraphExtensions(analysts={"market": AnalystExtension((scoped_tool,), "fund applicability")})
    config = {"data_cache_dir": str(tmp_path / "cache"), "results_dir": str(tmp_path / "results"), "memory_log_path": None}
    with runtime_context(config), patch("tradingagents.graph.trading_graph.create_llm_client", return_value=SimpleNamespace(get_llm=lambda: llm)):
        graph = TradingAgentsGraph(selected_analysts=["market"], config=config, extensions=extensions)
        graph.graph_setup.setup_graph(["market"])
        create_market_analyst(llm, extensions.analysts["market"] )({"company_of_interest": "600519", "trade_date": "2026-10-03", "messages": []})
        assert llm.bound[-1] == list(graph.tool_nodes["market"].tools_by_name) == ["scoped_tool"]
        assert "fund applicability" in llm.prompts[-1]
        native = TradingAgentsGraph(selected_analysts=["market"], config=config)
        create_market_analyst(llm)({"company_of_interest": "600519", "trade_date": "2026-10-03", "messages": []})
        assert llm.bound[-1] == list(native.tool_nodes["market"].tools_by_name) == ["get_stock_data", "get_indicators"]
        assert set(native.workflow.nodes) == set(graph.workflow.nodes)

def test_memory_same_day_dedup_and_parallel_different_days(tmp_path):
    memory = SynchronizedMemoryLog({"memory_log_path": str(tmp_path / "memory.md")})
    def write(index):
        memory.store_decision("600519", f"2026-10-{1 + index % 5:02}", "Rating: Buy")
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        list(pool.map(write, range(40)))
    assert len(memory.load_entries()) == 5

@pytest.mark.parametrize("stage", ["publication", "commit"])
def test_job_failure_after_engine_does_not_retry_or_forge_success(tmp_path, monkeypatch, stage):
    monkeypatch.setenv("TRADINGAGENTS_RESULTS_DIR", str(tmp_path / "reports"))
    store = ResearchJobStore(tmp_path / "jobs.db")
    job = _Job(job_id="a" * 32, request=ResearchJobRequest(ticker="600519", client_request_id="one"), ticker="600519", trade_date="2026-10-03", config={}, selected_analysts=["market"])
    store.save(job.persistence_record())
    state = {"company_of_interest": "600519", "trade_date": "2026-10-03", "final_trade_decision": "Rating: Buy", "missing_data_tasks": [], "missing_data_complete": True}
    calls = []
    class Adapter:
        def analyze(self, spec, runtime):
            calls.append(spec.attempt)
            runtime.progress("engine_finalization")
            return SimpleNamespace(state=state, signal="Buy")
    native = store.save
    def save(record):
        if record["status"] == "succeeded":
            raise OSError(SENTINEL)
        return native(record)
    with patch.object(store, "save", save) if stage == "commit" else patch("research_api.jobs.execution.publish_report", side_effect=OSError(SENTINEL)):
        run_job(job, lock=threading.Lock(), store=store, semaphore=threading.Semaphore(2), retries=2, retry_delay=0, adapter=Adapter())
    assert calls == [1] and job.status == "failed" and job.result is None
    assert SENTINEL not in json.dumps(job.persistence_record())
    assert store.load_all()[0]["status"] == "failed"
    reports = list((tmp_path / "reports").rglob("*.json"))
    assert len(reports) == (1 if stage == "commit" else 0)
    store.close()

def test_reports_preserve_old_bytes_mtime_and_refuse_overwrite(tmp_path):
    old = tmp_path / "600519/TradingAgentsStrategy_logs/full_states_log_2025-01-01.json"
    old.parent.mkdir(parents=True); old.write_bytes(b"historical original")
    before = old.stat().st_mtime_ns
    state = {"company_of_interest": "600519", "trade_date": "2026-10-03", "final_trade_decision": "Rating: Buy"}
    old.parent.chmod(0o2750)
    result = publish_report(state, tmp_path, "600519", "2026-10-03", "a" * 32, {})
    assert stat.S_IMODE(Path(result).stat().st_mode) == 0o640
    assert Path(result).stat().st_gid == old.parent.stat().st_gid
    assert publish_report(state, tmp_path, "600519", "2026-10-03", "a" * 32, {}) == result
    with pytest.raises(ValueError, match="report_publish_conflict"):
        publish_report({**state, "final_trade_decision": "Sell"}, tmp_path, "600519", "2026-10-03", "a" * 32, {})
    assert old.read_bytes() == b"historical original" and old.stat().st_mtime_ns == before

def test_safe_errors_at_quality_and_stock_tool_first_boundary(caplog):
    from tradingagents.dataflows import a_stock
    broken = SimpleNamespace(invoke=lambda *a, **k: (_ for _ in ()).throw(RuntimeError(SENTINEL)))
    state = {"trade_date": "2026-10-03", "company_of_interest": "600519", **{field: "A report " * 80 for field in ("market_report", "sentiment_report", "news_report", "fundamentals_report", "policy_report", "hot_money_report", "lockup_report")}}
    result = create_quality_gate(broken)(state)
    with patch.object(a_stock, "_load_ohlcv_astock", side_effect=RuntimeError(SENTINEL)):
        result["tool"] = a_stock.get_indicators("600519", "rsi", "2026-10-03", 30)
    assert SENTINEL not in json.dumps(result) + caplog.text

def test_etf_evidence_and_historical_availability(monkeypatch):
    monkeypatch.setattr(provider, "_akshare_etf_spot", lambda code: {})
    monkeypatch.setattr(provider, "_eastmoney_fund_script", lambda code: 'var fS_name="some stock";var fS_code="600519";')
    assert provider.verify_etf_identity("562060") is None
    monkeypatch.setattr(provider, "_akshare_etf_spot", lambda code: {"代码": code, "名称": "基金来源名称"})
    text = provider.get_etf_profile("562060", "2025-01-01")
    assert "历史时点" in text and "数据缺失" in text and "基金来源名称" in text
    assert provider._get_prefix("920002") == "bj"
    assert provider.finite_number("nan", True) is None

@pytest.mark.parametrize("tracking_failure", [False, True])
def test_adapter_lifecycle_isolated_same_ticker_attempts_and_tracking_not_retried(tmp_path, monkeypatch, tracking_failure):
    from research_api.engine.adapter import EngineAdapter
    monkeypatch.setenv("TRADINGAGENTS_RESULTS_DIR", str(tmp_path / "published"))
    barrier = threading.Barrier(2)
    calls = []
    class NativeGraph:
        def __init__(self, **kwargs):
            self.config = kwargs["config"]
            self.graph = SimpleNamespace(invoke=self.invoke)
        def prepare_graph_run(self, ticker, trade_date):
            self.ticker, self.trade_date = ticker, trade_date
            return {}, {}, None
        def invoke(self, state, **args):
            calls.append(self.config["results_dir"])
            barrier.wait()
            current = get_config()
            assert current["output_language"] == self.config["output_language"]
            assert current["checkpoint_dir"] != current["missing_data_dir"]
            if tracking_failure:
                mark_tracking_failed()
            return {"company_of_interest": self.ticker, "trade_date": self.trade_date, "final_trade_decision": "Rating: Buy", "market_report": current["output_language"]}
        def finalize_graph_run(self, ticker, trade_date, state):
            missing_data.attach_missing_data_snapshot(state, ticker, trade_date)
            output = Path(self.config["results_dir"]) / ticker / "TradingAgentsStrategy_logs" / f"full_states_log_{trade_date}.json"
            output.parent.mkdir(parents=True)
            output.write_text(json.dumps(state))
            return "Buy"
        def close_graph_run(self):
            pass
    stores = [ResearchJobStore(tmp_path / name / "jobs.db") for name in ("a", "b")]
    jobs = [_Job(job_id=name * 32, request=ResearchJobRequest(ticker="600519", client_request_id=name), ticker="600519", trade_date="2026-10-03", config={"output_language": language}, selected_analysts=["market"]) for name, language in (("a", "Chinese"), ("b", "English"))]
    for job, store in zip(jobs, stores):
        store.save(job.persistence_record())
    with patch("research_api.engine.adapter.TradingAgentsGraph", NativeGraph):
        def run(index):
            run_job(jobs[index], lock=threading.Lock(), store=stores[index], semaphore=threading.Semaphore(2), retries=2, retry_delay=0, adapter=EngineAdapter())
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            list(pool.map(run, (0, 1)))
    assert len(calls) == 2 and len(set(calls)) == 2
    if tracking_failure:
        assert all(job.status == "failed" and job.error_type == "missing_data_tracking_failed" for job in jobs)
        assert not list((tmp_path / "published").rglob("*.json"))
    else:
        assert all(job.status == "succeeded" for job in jobs)
        assert jobs[0].result["reports"]["market"] == "Chinese"
        assert jobs[1].result["reports"]["market"] == "English"
        assert len(list((tmp_path / "published").rglob("*.json"))) == 2
    for store in stores:
        store.close()

def test_shared_public_cache_competing_updates_are_preserved(tmp_path):
    from tradingagents.dataflows.a_stock import _save_northbound_snapshot, _load_northbound_history
    def write(index):
        with runtime_context({"data_cache_dir": str(tmp_path)}):
            _save_northbound_snapshot(f"2026-10-{index + 1:02}", index, index + 1)
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        list(pool.map(write, range(20)))
    with runtime_context({"data_cache_dir": str(tmp_path)}):
        assert len(_load_northbound_history(30)) == 20
    assert not list(tmp_path.glob(".data-cache-*"))

def test_missing_index_disappearance_after_read_is_corruption(tmp_path):
    index = tmp_path / "index.json"
    index.write_text("[]")
    with runtime_context({"missing_data_dir": str(tmp_path)}):
        assert missing_data.get_missing_tasks("600519", "2026-10-03") == []
        index.unlink()
        with pytest.raises(RuntimeError, match="missing_data_tracking_failed"):
            missing_data.attach_missing_data_snapshot({}, "600519", "2026-10-03")


def test_unexpected_http_failure_projects_before_uvicorn_logs(tmp_path):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    source = Path(__file__).resolve().parents[3]
    src = source / "integrations/ata_research/src"
    environment = os.environ.copy()
    environment.update(PYTHONPATH=str(src) + os.pathsep + str(source),
        ASTOCK_RESEARCH_DB_PATH=str(tmp_path / "jobs.db"),
        TRADINGAGENTS_RESULTS_DIR=str(tmp_path / "reports"))
    code = """import research_api.app as module, uvicorn, sys
sentinel = sys.argv[2]
def fail_save(*args, **kwargs):
    raise OSError(sentinel)
module._job_store.save = fail_save
module._run_job = lambda *args: (_ for _ in ()).throw(AssertionError("unexpected_model_run"))
uvicorn.run(module.app, host="127.0.0.1", port=int(sys.argv[1]), log_level="info", access_log=False)
"""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    base = f"http://127.0.0.1:{port}"
    log_path = tmp_path / "uvicorn.log"
    with log_path.open("w") as log:
        process = subprocess.Popen([sys.executable, "-B", "-c", code, str(port), SENTINEL], cwd=src,
            env=environment, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 20
            while True:
                assert process.poll() is None, log_path.read_text()
                try:
                    with opener.open(base + "/health", timeout=.5) as response:
                        assert json.load(response)["jobs"] == 0
                    break
                except urllib.error.URLError:
                    assert time.monotonic() < deadline, log_path.read_text()
                    time.sleep(.05)
            for _ in range(2):
                request = urllib.request.Request(base + "/research/jobs",
                    data=json.dumps({"ticker": "600519", "trade_date": "2026-10-03", "client_request_id": "http-failure"}).encode(),
                    headers={"Content-Type": "application/json"})
                with pytest.raises(urllib.error.HTTPError) as caught:
                    opener.open(request, timeout=3)
                assert caught.value.code == 500
                body = json.loads(caught.value.read())
                assert body == {"detail": "Error: external_operation_failed (http)"}
                assert SENTINEL not in json.dumps(body)
            with opener.open(base + "/health", timeout=3) as response:
                assert json.load(response)["jobs"] == 0
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=5)
    ordinary_logs = log_path.read_text()
    assert SENTINEL not in ordinary_logs and "Traceback" not in ordinary_logs
    assert "unexpected_model_run" not in ordinary_logs
