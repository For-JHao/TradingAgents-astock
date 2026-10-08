# ATA Research integration

This service preserves the existing internal HTTP contract and SQLite job schema.
The engine baseline is TradingAgents-astock v0.5.20 at
`d393981690826e46d4fb3d52e8fe7765fdc8b9cb`. The integration has its own manifest;
install third-party dependencies from both manifests together, without installing
project wheels. The native dependency combination uses HTTPX <0.26 with mootdx;
Google and Agent SDK extras are not installed by this integration. Selecting an
unavailable native provider fails explicitly, without substituting a provider.

Run `python -m research_api.app` from `integrations/ata_research/src`, with the
trusted repository root as `PYTHONPATH`. The root `.env` and `.env.enterprise`
locations are explicit. `research_api.job_store_cli` and `research_api.status_cli`
retain their module names. Run one service process (default two bounded execution
threads), not multiple HTTP workers sharing process-local job state.

`http/` holds fixed DTOs/errors, `jobs/` durable job state and worker lifecycle,
`engine/adapter.py` owns the engine lifecycle, `data/provider.py` only the market
and fund data functions required by the platform, `extensions/etf/` fund tools and
applicability, and `storage/` the existing database, synchronized native Markdown
memory and report publication. Ordinary company research remains native.

## Finite upstream seams

- `graph/extensions.py`, analyst factory optional arguments and GraphSetup support
  the exact same effective tools for bind_tools and ToolNode, an instrument-context
  supplement, a pre-quality check and a native-memory wrapper factory. There is no
  dynamic plugin loader, arbitrary dispatch or alternative debate graph.
- `dataflows/config.py` provides a scoped configuration plus a shared Event for
  missing-data failures, propagated into actual LangGraph ToolNode worker threads.
- `checkpoint_dir` and `missing_data_dir` isolate attempts; native defaults stay
  available for independent upstream CLI/Web usage. Scoped missing-index failures
  are explicit and cannot produce a complete report.
- `dataflows/resources.py` supplies fixed shared client/cache guards, atomic CSV
  writes and one Eastmoney request budget, used by the public provider too.
- `safe_errors.py` projects exceptions before tool/node/log text. Third-party wire
  debug logs are disabled at service startup. Exceptions are not persisted as
  traceback or arbitrary response text.

New reports are privately generated and then hard-link published after fsync to
`<ticker>/TradingAgentsStrategy_logs/full_states_log_<date>_<job_id>.json`.
Publication refuses overwrite; the successful record is durable before it becomes
visible in memory. Publication or job commit failure never restarts the model.
Restart keeps the existing interrupted-job failure semantics. Old report files,
paths, bytes and mtime are never rewritten by this upgrade; reports remain
independent of the job database. No schema conversion, extra report ledger,
automatic resume service or new production cleanup is introduced.

Deterministic tests use a temporary database/report/memory/cache root and
`PYTHONPATH=<root>/integrations/ata_research/src:<root>`; run
`python -m pytest integrations/ata_research/tests` and selected native regressions.
Tests use fixtures/stubs, without paid model calls or production data. Linux
systemd identity/permissions and real-model shared prod/pre acceptance remain
`external_validation_pending` until the approved exact production release.

## Account-context publication and quote provenance

New report metadata adds `contextVersion: 1`, the job's effective
`selectedAnalysts`, and `memoryUsed` from the adapter's effective
`memory_log_path` configuration plus the actual `past_context` state. Missing or
inconsistent evidence remains null. The original full report state, decisions,
debate histories and injected context are preserved; no graph or memory behavior
changes. Historical files are never backfilled or rewritten.

`/market/quotes` retains legacy receipt-time `ts`. Optional `source` and
`quote_at` describe Tencent field 30 in Asia/Shanghai; unavailable source time
stays null, including the existing Eastmoney fallback. `session_status`,
`latest_session_date`, `session_closed_at` and `calendar_proof` come from a
bounded read of the installed AkShare `file_fold/calendar.json`: strict date
schema/order, actual coverage bounds and SHA256. Fixed A-share morning/afternoon
hours are combined with calendar membership; no weekday holiday guesses,
online refresh, new dependency or background timer is introduced. Outside the
bundled coverage, or on invalid calendar data, the session proof is unavailable.
The platform validates quote observation age and the last completed session;
post-close source observations may be later than 15:00.
