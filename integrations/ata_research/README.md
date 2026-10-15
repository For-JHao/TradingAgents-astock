# ATA Research integration

This service preserves the existing internal HTTP contract and SQLite job schema.
The engine baseline is TradingAgents-astock v0.5.21 at
`e3981af1e4c94656cdc663f1466d54fd49253dd5`. The integration has its own manifest;
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
- `tradingagents/safe_errors.py` is an integration-owned finite shim (added locally,
  not an upstream business module); `research_api.http.errors` re-exports the same
  function. It preserves the generic `Error: external_operation_failed (stage)`
  prefix and appends only fixed, type-based categories for known timeout,
  connection, HTTP, JSON-decode, type, attribute and key errors. Unknown exceptions
  retain the generic output. Stages are fixed and unsupported values become `data`.
  Exception text, dynamic class names and request/response attributes are never
  inspected. Third-party wire debug logs are disabled at service startup.
  Exceptions are not persisted as traceback or arbitrary response text. Historical
  generic failures cannot be retrospectively classified.

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

## Financial statement baseline

Company financial statements use v0.5.21's Sina `report_list` parser and its
announcement/ingestion-date filter. The three statement tools require a nonempty
analysis date. Historical native missing-task retries supply that task's analysis
date only for invocation and retain the original task arguments and identity.
The existing scoped missing-data failure checks and safe error projection apply.

This filter retains upstream's legal-deadline inference when original dates are
lost, the specific 2020-03-13 batch-ingestion handling, and historical restatement
limits. It does not establish strict historical point-in-time financial values.
No historical report, memory entry or missing-task record is migrated or rewritten
by installing this baseline.

## Native source reliability

The stock data functions retain their original tool names and schemas. Valid
mootdx F10 text remains preferred; an unavailable, empty or non-text result uses
three bounded Eastmoney HTTP reads for shareholder research, executive changes
and shareholder changes. Each section retains usable data when another source
fails. Share counts, ten-thousand-share counts, direction and source dates are
handled separately; these are limited current snapshots, not a historical
point-in-time or complete insider transaction database. Structured empty lists
and the observed datacenter `success=false/code=9201/result=null` no-record tuple
remain healthy empty results. Other source/shape failures remain missing or
failed through the existing recorder and safe error projection.

Fund flow keeps the existing realtime-to-history request order and limits. A
later historical failure retains available realtime output with an explicit gap;
without realtime data it retains the native failed-result path. Macro news checks
HTTP status before JSON, preserves the existing alternate source and distinguishes
partial source failure, total failure and healthy empty responses.

These data corrections do not change analyst prompts, tool schemas, graph nodes
or edges, model routing, retries, memory, checkpoint or report publication. No
source replacement is introduced for the unverified CLS endpoint; its failures
are reported safely while the existing macro source remains available.

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
