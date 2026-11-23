# Multi-portfolio design (v1 spec)

Status: **implemented** (registry, subjects, ingress fan-out, supervisor pool,
per-portfolio state, DSL wiring, warm-up). Decision log at the bottom; the
implementation sequence in §11 records what each step landed.

What is deliberately not in v1: registry hot-reload (restart-only), DSL param
slots (boxes declare literals; portfolios bind nothing yet), cross-portfolio risk.

## 1. Model

A portfolio is a directory. It owns identity, strategy params, capital, state.
Market data is shared infrastructure outside it.

```
/data/                          shared, portfolio-agnostic
├── candles.db                  all instruments/timeframes the union of portfolios needs
└── ingress.sqlite              global provider watermark: {inst}:{tf}:last_ts

/portfolios/
├── audcad_mr/
│   ├── portfolio.json          v1 schema
│   ├── strategy.dsl            box graph + wiring (absent → built-in graph, params from json)
│   └── state/
│       ├── state-strategy.sqlite
│       ├── state-enricher.sqlite
│       └── state-broker.sqlite
└── eurusd_swing/
    ├── portfolio.json
    └── state/
        ├── state-strategy.sqlite
        ├── state-enricher.sqlite
        └── state-broker.sqlite
```

Directory name = portfolio id = NATS subject token = state-key prefix. Validated
`^[a-z0-9_-]+$`.

`portfolio.json` v1:

```json
{
  "version": 1,
  "id": "audcad_mr",
  "instruments": ["AUDCAD"],
  "timeframes": ["1d"],
  "tradeTimeframe": "1d",
  "contextTimeframes": [],
  "strategy": { "file": "strategy.dsl", "params": { "k": 3.5 } },
  "account": { "startingBalance": 10000, "riskPct": 0.01 }
}
```

Keys are camelCase (`tradeTimeframe`, `startingBalance`, `riskPct`); `strategy`
and `account` are optional, `timeframes` must contain `tradeTimeframe`, and
`id` must equal the directory name.

- `strategy.params` is strategy-only, bind-only; the DSL (future) publishes
  parameter slots, portfolios reference + configure. Never move `account` into
  strategy binding. Separation stays stable when DSL lands.
- `version` field from day one (future: `groups: [{instruments, strategy}]`).

## 2. Subjects

```
ingress      → pf.{id}.feed          (CandleClosed, filtered to this portfolio)
enricher    → pf.{id}.enrichment    (IndicatorUpdated, SwingPivotEvent)
strategy    → pf.{id}.exec           (republished CandleClosed + SetupSignal)
broker      → pf.{id}.exec           (subscribed here, one sub per portfolio)
state-writer→ state                   (global; keys include portfolio)
```

- `{id}.exec` is the only execution stream; the rename from the old `.ingress`
  name is the "resolve candle N then apply setup N" guarantee — broker keeps one
  subscription, candle-before-setups is per-subject order.
- `pf.{id}.warmup` exists as a subject but is consumed by the enricher only, in the
  bootstrap phase. The strategy and broker never subscribe to it.

## 3. Watermarks / cursors

Two distinct cursors, never conflated:

| cursor | key | owner | meaning |
|---|---|---|---|
| provider watermark | `{inst}:{tf}:last_ts` | fetcher→ingress store | fetch idempotency: already pulled+persisted |
| pipeline cursor | `{pf}:{inst}:{tf}:cursor` | **enricher** | last bar this portfolio's pipeline consumed |

Enricher owns the pipeline cursor (persisted in its context snapshot, so
`StateCheckpoint` already carries it). Ingress reads the per-portfolio cursor and
publishes bars strictly after it. Enricher advances it as it consumes; bars at or
before it are dropped (dedupe contract, §5).

## 4. Ordering

Per portfolio: `feed(N) → enrichment(N) → exec(N)` per-subject FIFO from NATS.
Strategy republishes the closed candle as the first event of the bar on `exec`,
then the setups derived from it, on the same subject. Broker resolves candle N
before applying setup N. No pacing, no cross-subject races.

Dedupe contract: every stage drops events whose bar timestamp ≤ its context's last
applied bar (`ctx.latest`, which is keyed per `(symbol, timeframe)` so cross-
timeframe collisions don't interfere). `SetupSignal` derives only from candles the
strategy already accepted, so candle-level dedupe ⇒ setup-level non-duplication.
Follow-up: `SetupSignal.decided_at` (backlog) to make setup-level idempotence
independent of that reasoning.

## 5. Timeframes & max hold

- `portfolio.json` has explicit roles:
  - `trade_timeframe` — the timeframe trades live in (`exec` broker-side filter)
  - `context_timeframes` — analysis timeframes for the portfolio (default: none
    beyond the trade timeframe)
- Broker subscribes to `pf.{id}.exec` and **filters to its trade timeframe** before
  applying anything. Context timeframes never reach the broker.
- `max_hold` is a **timespan** (seconds), not a candle count. Strategy computes the
  bar count from the trade timeframe: `max_hold_bars = max(1, ceil(max_hold_secs / bar_secs))`.
  Broker counts bars against that. Candle-count semantics were the ambiguity;
  timespan removes it.

## 6. Sizing

- `GeometryNode` sends the desired risk **% of current balance**. It does not know
  the hard balance; the broker module does the % → position maths (shares/lots/
  units).
- Dummy broker: keeps a tracked balance (updated when trades open/close) and
  supplies it as the equity source for sizing. Real broker: replaces the source
  with reported external value.

## 7. State

- One file per portfolio per role: `/portfolios/{id}/state/state-{strategy,enricher,broker}.sqlite`.
- Keys: `{pf}:{instrument}:{tf}:{role}:state` (strategy/broker/enricher snapshots).
- Pipeline cursor key: `{pf}:{instrument}:{tf}:cursor`.
- Write-behind unchanged: components publish `Event::ContextState` on `state`;
  `state-writer` flushes per file in one transaction; `--role-db` superseded by
  per-portfolio routing (`--portfolios-root`).

## 8. Process topology (T2)

One process per role, bounded thread pool, one worker per portfolio.

```
pf.{id}.feed       → [dispatcher: tagged subscription] → per-portfolio queue → pool worker
pf.{id}.enrichment → same shape
pf.{id}.exec       → same shape (+ pf.*.warmup → enricher only)
```

Pool rules:
- concurrency across portfolios allowed; **serialisation within a portfolio** enforced by a per-portfolio `busy` flag + CAS (worker takes portfolio, drains up to K items, releases). No per-portfolio mutex across the full processing span, no head-of-line stalls. `try_lock` on the runtime is the CAS; a busy portfolio is skipped this pass, never waited on.
- runtime objects (`Graph`, state handle, checkpoint) live in a map keyed by portfolio id; map lock held only for lookup.
- pool size: enricher/strategy CPU-bound → `num_cpus`; broker → 1–2. All overridable with `--workers`.
- **one bus owner per process**: workers never touch NATS. They hand `(subject, event)` to the process's single publisher thread, which preserves per-portfolio order because publishing happens under the runtime lock.
- subject-tagged subscriptions (`MessageBus::subscribe_tagged`) keep the delivered subject, so `pf.*.feed` routes by the portfolio named in the subject. The event payload carries no portfolio id.
- dispatch is per stage, not per portfolio: enricher consumes `feed`, strategy `enrichment`, broker `exec` (plus the broker's trade-timeframe filter).

## 9. Lifecycle / failure modes

- Invalid portfolio (parse/validation/DSL compile) → skip that portfolio, loud log, others run.
- Worker panic → the portfolio's runtime lock is poisoned, which would look identical to "busy"; the supervisor detects the poison, quarantines that portfolio loudly and keeps the others running. A restart recovers it.
- State row unreadable → loud log, that context starts fresh; the cursor/dedupe contract keeps it correct (at-least-once).
- Writer can't open a portfolio file → its snapshots are retained and retried, logged, never misrouted (destination comes from the key prefix).
- Registry reload: restart-only (v1).
- Down/up without data loss: SIGINT **and** SIGTERM both trap the shutdown flag; the dispatcher stops enqueueing, workers drain the queues (bounded 30 s), then final snapshots are published and flushed. Verified round trip: second boot replays nothing and opens no trades.
- Readiness: a daemon writes `RELIANT_READY_FILE` only after its NATS SUB is flushed, so a later-started publisher cannot lose its first burst. Compose healthchecks key on it.
- Runtime UID: non-root (`USER 10001:10001` in runtime stage, one-shot `perm-fix` service chowns `/data` and `/portfolios`).

## 10. Decisions log

| # | question | decision |
|---|---|---|
| 1 | topology | T2: supervisor per role, thread pool per portfolio |
| 2 | state file | split per portfolio **and** role |
| 3 | shared facts | per-portfolio in v1 (no dedupe work yet) |
| 4 | params | strategy-only binding from `portfolio.json`; DSL param slots later |
| 5 | warmup | yes; bootstrap warm-up via `pf.{id}.warmup`, enricher-only |
| 6 | cross-portfolio risk | out of scope |
| 7 | registry reload | restart-only |

Session follow-ups resolved:

| # | question | decision |
|---|---|---|
| Q1 | JetStream? | **No.** Core NATS + at-least-once + dedupe + cursor suffices; all state is local files; restart = replay from cursor. JetStream = durable storage + replay + exactly-once — none currently required. |
| Q3 | max_hold as timespan? | **Yes.** Timespan (seconds) → bar count at strategy. Broker counts bars against its trade timeframe. |
| Q4 | geometry % vs hard balance? | **% of current balance**; broker does %→position maths. Dummy broker = tracked balance. |
| C1 | no-IO-in-event-loop? | **Keep.** |
| C2 | ingress reads portfolio cursors? | **Yes.** |
| C3 | broker foreign-instrument reject + logging? | **Yes.** Log infra not yet built — will add component-scoped exception logs. |
| C4 | dedupe key? | **(timeframe, timestamp) within per-(sym,tf) context** — sound. `SetupSignal.decided_at` still flagged for setup-level idempotence. |
| C5 | graph build timing? | **Build + validate all portfolios at startup, then push into pool.** |

## 11. Implementation sequence

1. ✅ `reliant-portfolio` registry crate: `Portfolio`, `Registry::load(root)`, validation, id rules, universe helpers.
2. ✅ Subject rename `{ctx}` → `pf.{id}` with **single-portfolio parity**: still 9 trades / 6 wins / +9.000R through the full stack.
3. ✅ Replay utility (`enricher --replay --replay-to feed|warmup`) + bootstrap warm-up (`pf.{id}.warmup`, enricher-only, derived facts not published).
4. ✅ Ingress fan-out: global candle store, per-portfolio `(instrument, timeframe)` fan-out, reads `{pf}:{inst}:{tf}:cursor` read-only and never advances it.
5. ✅ Supervisor loop per role (`crates/cli/src/supervisor.rs`): wildcard tagged subscription → per-portfolio queue → bounded pool, `try_lock` = busy flag, single publisher thread.
6. ✅ Split state files per portfolio/role + `state-writer --portfolios-root` (destination derived from the key prefix) + cursor row written with the enricher snapshot.
7. ✅ Broker `exec` subject + trade-timeframe filter + `max_hold_secs` → bar count at the strategy.
8. ✅ Two-portfolio test on one instrument (`audcad_mr` 9 trades / +9.000R, `audcad_fast` 448 trades / +131.285R), independent state files and cursors; SIGTERM trapped; runtime UID 10001.

## 12. Strategy DSL (v1)

A portfolio's graph is `strategy.dsl` in its folder. Syntax:
`name := kind { field: value }`, `//` comments, ints/floats/strings/bools/null,
lists, trailing commas, refs (`ema: ema`) and shorthand (`ema`, `swing`).

```dsl
ema   := ema   { period: 20 }
atr   := atr   { period: 14 }
swing := swing { k: 3.5, lookback: 5 }

strategy := strategy { k: 3.5, ema: ema, atr: atr, swing: swing }
sizing   := sizing   { setups: strategy }
broker   := broker   { trades: sizing }
```

Box catalogue and role ownership:

| box | role | consumed from | produces |
|---|---|---|---|
| `ema`, `atr`, `swing` | Enricher | `feed` | `IndicatorUpdated` / `SwingPivotEvent` |
| `strategy` | Strategy | `enrichment` (candles, indicators, swings) | `SetupSignal` |
| `sizing` | Broker | setups | `TradeSignal` |
| `broker` | Broker | candles + trade signals | `TradeResult` |

- Box name = processor/fact key. `strategy` resolves its `ema`/`atr`/`swing`
  slots from its input edges, so renaming a box renames the fact.
- Producer types are checked at compile time (`Indicator`, `Swing`, `Setup`,
  `Trade`), as are cycles, dangling refs and duplicate names.
- Cross-role edges map to the role channels: Enricher→Strategy = `enrichment`,
  Strategy→Broker = `exec`. Same-role edges stay in-process (`channel: None`).
- Node multiplicity: `ema|atr|swing|broker` instantiate per (instrument ×
  timeframe); `strategy|sizing` instantiate once per portfolio (a second
  instance would process every event twice).
- v1 deliberately omits choices/groups/templates/provider resolution/timeframes
  and DSL param slots; a box takes literals from its own file.
- Compiled once per portfolio at startup; a compile error skips that portfolio
  loudly and the others still run.

