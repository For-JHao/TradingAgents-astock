"""The only integration boundary that knows the engine lifecycle and types."""
from dataclasses import dataclass
from pathlib import Path
import copy
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.graph.extensions import GraphExtensions
from tradingagents.dataflows.config import runtime_context, assert_tracking_healthy
from research_api.extensions.etf.extension import etf_extensions
from research_api.storage.memory import SynchronizedMemoryLog

ENGINE_VERSION = "v0.5.20"
ENGINE_SHA = "d393981690826e46d4fb3d52e8fe7765fdc8b9cb"
INTEGRATION_VERSION = "1.0.0"
CONTRACT_VERSION = 1

@dataclass(frozen=True)
class EngineResult:
    state: dict
    signal: str
    private_output: Path
    memory_enabled: bool | None = None

class EngineAdapter:
    def analyze(self, spec, runtime):
        config = copy.deepcopy(DEFAULT_CONFIG)
        config.update(spec.config)
        attempt_root = Path(runtime.directory) / spec.job_id / str(spec.attempt)
        config.update(results_dir=str(attempt_root / "results"), checkpoint_dir=str(attempt_root / "checkpoint"), missing_data_dir=str(attempt_root / "missing"))
        runtime.progress("graph_initialization")
        extensions = etf_extensions(spec.ticker, spec.trade_date) or GraphExtensions(memory_factory=SynchronizedMemoryLog)
        with runtime_context(config):
            graph = TradingAgentsGraph(selected_analysts=spec.selected_analysts, config=config, extensions=extensions, debug=False)
            try:
                # Stage transition prevents retries once lifecycle persistence begins.
                runtime.progress("graph_execution")
                initial, args, _ = graph.prepare_graph_run(spec.ticker, spec.trade_date)
                state = graph.graph.invoke(initial, **args)
                runtime.progress("engine_finalization")
                signal = graph.finalize_graph_run(spec.ticker, spec.trade_date, state)
                assert_tracking_healthy()
                private = attempt_root / "results" / spec.ticker / "TradingAgentsStrategy_logs" / f"full_states_log_{spec.trade_date}.json"
                if not private.is_file():
                    raise ValueError("private_engine_output_missing")
                return EngineResult(state, signal, private, bool(config.get("memory_log_path")))
            finally:
                graph.close_graph_run()
