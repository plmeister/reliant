# Dashboard Plan (pending signoff)

## Goal

Web dashboard that renders candlestick price charts for any instrument in
`candles.db`, overlays long/short trades (entry, stop, target, exit), and
drills into running portfolios (open + closed trades, jump-to-chart). Timeframe
selectable; chart supports zoom and horizontal scroll.

## Prerequisite gap: trades are not persisted

`TradeResult` carries only `pnl_r` + `reason`. `DummyBroker` holds open-trade
state in its in-memory `Context` and publishes a `ContextState` snapshot, but
no lifecycle record (entry bar, entry price, exit bar, exit price, direction,
R) is written anywhere. The dashboard cannot show closed trades without this.

## Phase 1 — Trade persistence (backend prerequisite)

Design (per user suggestion): a **TradeLogger node** that subscribes to the same
`trade` topic as `DummyBroker`, receives `TradeSignal` (open) and `TradeResult`
(close) events, and appends a complete lifecycle record to a file in the
portfolio directory. This keeps persistence decoupled from the broker — the
broker stays the single source of truth for trade state, the logger is a pure
sink.

1. Enrich broker output: `DummyBroker` already holds entry/stop/target/direction/
   entry_ts at resolution time. Change `TradeResult` to carry the full lifecycle
   (`symbol`, `timeframe`, `direction`, `entry`, `stop`, `target`, `entry_ts`,
   `exit_ts`, `exit_price`, `reason`, `pnl_r`, `size`) — additive fields with
   `#[serde(default)]` for backward compat. Broker records `entry_ts` from the
   current candle timestamp when it opens.
   Alternative: emit a separate `Event::TradeClosed` to avoid touching
   `TradeResult` consumers. Recommend enriching `TradeResult` (simpler, one
   event type).
2. TradeLogger node: subscribes to `trade` topic. On `TradeSignal` → record
   pending open (symbol, timeframe, direction, entry, stop, target, entry_ts,
   size). On `TradeResult` → correlate with the most recent pending open for
   that symbol/timeframe, write a complete JSONL record to
   `/portfolios/{id}/state/trades.jsonl` (append, fsync). Open trades are
   inferred from broker `Context` snapshots (state-broker.sqlite) — no separate
   open-trade file needed.
   **Write-behind:** TradeLogger buffers records in memory and flushes every N
   trades or every T seconds (or on shutdown). No synchronous file I/O in
   `on_event` — consistent with the "no IO in the event loop" rule. On
   SIGTERM, flush remaining buffer before exit.
3. In daemon mode, TradeLogger runs as part of the trade component (or as a
   separate node in the trade graph). In backtest, it can be enabled with a
   `--trade-log <path>` flag for analysis.
4. Update `audit` tool and tests that pattern-match `TradeResult { pnl_r, reason }`
   (additive fields, backward compatible).
5. Concurrency: TradeLogger writes are buffered (write-behind, flush every N
   trades or T seconds). On shutdown, flush remaining buffer. No SQLite
   contention — trades are a separate append-only JSONL file. State-writer
   is not involved — keeps single-writer discipline for context snapshots.

Open-trade visibility: broker `Context` snapshot already has `open_trade`,
`trade_entry`, `trade_stop`, `trade_target`, `trade_direction`,
`trade_max_hold`, `trade_bars`, `latest`. Dashboard reads these from
`state-broker.sqlite` for open trades. Closed trades come from `trades.jsonl`.

## Phase 2 — Backend API (extend audit-server → dashboard-server)

New endpoints (read-only, same axum server):

- `GET /api/instruments` — distinct symbols in candles.db.
- `GET /api/candles?symbol=&tf=&from=&to=&limit=` — OHLCV array.
- `GET /api/portfolios` — list portfolios from `--portfolios-root`.
- `GET /api/portfolios/{id}/state` — broker open-trade state per instrument/tf.
- `GET /api/portfolios/{id}/trades?status=open|closed&symbol=&tf=` — trade list.
- `GET /api/portfolios/{id}/trades/{idx}` — single trade detail.
- Reuse `SqliteCandleStore::open_read_only` and `SqliteStateStore`.

Server args: `--audit-dir`, `--candle-db`, `--portfolios-root`, `--port`.
Embed frontend as before (no build step).

## Phase 3 — Frontend chart

- Vendor `lightweight-charts` UMD bundle into `frontend/lib/` (single file,
  ~40KB gzipped, works offline via `<script src>`). Download once, commit.
  Fallback: if network blocked, use a minimal canvas candlestick renderer.
- Chart view: candlestick series + volume histogram. Timeframe selector
  (1m/5m/1h/1d/1w). Zoom/scroll via library defaults (mouse wheel, drag).
- Trade overlay: for each trade on the visible symbol/tf, draw:
  - entry price line (green long / red short) spanning entry→exit bars
  - stop + target horizontal lines (dashed)
  - entry marker (arrow) and exit marker (square) at bar timestamps
  - tooltip on hover: entry/exit ts, prices, reason, R
- Portfolio drill-down panel: list open trades (from broker state) and closed
  trades (from trades store). Click → set chart symbol/tf and scroll to
  entry bar. Highlight the selected trade on the chart.
- Instrument picker (searchable) + timeframe picker + portfolio picker.

## Phase 4 — Polish

- Persist chart preferences (symbol, tf, range) in localStorage.
- Dark theme (already have audit viewer palette).
- Auto-refresh open trades every N seconds when a portfolio is selected.
- Deep link: `?symbol=AUDCAD&tf=1d&trade=42`.

## Phases & effort

| Phase | Work | Estimate |
|---|---|---|
| 1 | Trade persistence (event + broker + state-writer + tests) | 1-2 days |
| 2 | Backend API (5 endpoints + wiring) | 1 day |
| 3 | Frontend chart + overlays + drill-down | 2-3 days |
| 4 | Polish + tests | 1 day |
| 5 | Control plane (portfolio registration, backfill, start/stop) | 2-3 days (Option A) |

## Open questions for signoff

1. **Chart library**: lightweight-charts (vendored UMD) vs. pure canvas?
   Recommend lightweight-charts — handles zoom/scroll/pan/crosshair for free.
2. **Trade storage**: TradeLogger appends to `trades.jsonl` (JSONL, one line
   per closed trade). Open trades read from broker `Context` snapshots in
   `state-broker.sqlite`. No SQLite contention for trade writes.
3. **Real-time updates**: poll broker state every 5s, or add a NATS/SSE
   stream? Recommend polling for v1 (simpler, matches existing daemon model).
4. **Scope of open-trade detail**: broker state snapshot only has entry/stop/
   target/direction/bars. Is max_hold (bar count) enough, or do we need the
   original signal timestamp? Recommend storing entry_ts in broker context
   (phase 1).
5. **Historical trades**: only available after phase 1 ships. Pre-existing runs
   have no trade history. Acceptable? Recommend yes — backfill not worth it.

Sign off on phases 1-5, or adjust scope.
## Phase 5 — Control plane (daemon orchestration from dashboard)

The dashboard must bring up a new portfolio at runtime: register the folder,
backfill historical candles, compile the DSL, and start processing — without
restarting the whole stack.

### 5.1 Control messages (NATS subject: `control`)

New `Event` variants (additive, backward compatible):

- `PortfolioRegister { id, path }` — orchestrator loads + validates the folder,
  compiles the DSL, builds runtimes for each role, adds to the worker pool.
- `PortfolioUnregister { id }` — drain in-flight work, remove runtimes.
- `PortfolioReload { id }` — recompile DSL + rebuild runtimes (restart-only in
  v1; full hot-reload is future work).
- `Backfill { symbols, timeframes, from, to }` — trigger fetcher for historical
  data; publishes `FetchCompleted` when done.
- `PortfolioStatus { id }` — query; response carries state (running/stopped/
  error, last bar, trade count).

All messages are JSON-serialized on the `control` subject. Components ignore
unknown variants (serde default).

### 5.2 Orchestrator

Option A (recommended): **supervisor watches `--portfolios-root`** with `notify`
crate (fs events). New folder → validate → register → backfill → start. No
message protocol needed for registration; dashboard just creates the folder and
writes `portfolio.json` + `strategy.dsl`. Supervisor picks it up within N seconds.

Option B: **message-based control plane**. Dashboard publishes `PortfolioRegister`
to `control`. Supervisor subscribes and handles. More explicit, supports
remote portfolios and audit trail. Requires control subscription in supervisor.

Recommendation: Option A for v1 (simpler, matches "folder = portfolio" model),
Option B for v2 (remote management, multi-host).

### 5.3 Backfill flow

1. Dashboard creates portfolio folder + files.
2. Supervisor detects new folder (Option A) or receives `PortfolioRegister`
   (Option B).
3. Supervisor triggers `fetcher` (or fetcher daemon picks up via
   `FetchRequested` on `fetch` subject) for the portfolio's instruments/timeframes.
4. Fetcher backfills candles.db, publishes `FetchCompleted`.
5. Supervisor's enricher runtime processes the backfilled range (cursor starts
   at 0 → full replay).
6. Broker opens trades on historical signals (paper-only; DummyBroker).

### 5.4 Dashboard controls

- **New portfolio wizard**: name, instruments (multi-select), timeframes,
  strategy (template picker), account (balance, risk%). Writes folder.
- **Backfill button**: triggers `Backfill` for selected symbols/timeframes/date range.
- **Start/Stop/Reload** buttons per portfolio.
- **Status indicator**: running (green), stopped (grey), error (red) with
  last-error tooltip.

### 5.5 Scope

Phase 5 is independent of Phases 1-4. Can ship after Phase 4 or in parallel
with Phase 1. Effort: 2-3 days (Option A) or 4-5 days (Option B).

## Open questions (updated)

6. **Control plane**: Option A (fs watch) or Option B (NATS messages)?
   Recommend Option A for v1.
