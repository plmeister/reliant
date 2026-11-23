# Component integration stack

Brings up NATS + all components (ingress → enricher → strategy → trade, plus the
fetcher and the state writer) in daemon mode, sharing one candle store, so the
message-bus wiring can be exercised outside the in-process backtest.

Every component serves **all portfolios** under `deploy/portfolios/` from one
process, with a bounded worker pool. See `PORTFOLIOS.md` for the model and
`portfolios/*/strategy.dsl` for the graph each portfolio runs.

## What runs

| service   | binary              | role                                                        |
|-----------|---------------------|-------------------------------------------------------------|
| `nats`    | nats:2.10-alpine    | message bus                                                 |
| `seed`    | `fetcher --import-parquet` | one-shot: import `SYMBOL.TF.parquet` into `candles.db` |
| `fetcher` | `fetcher --daemon`  | incremental candle updates, publishes `FetchCompleted`      |
| `state-writer` | `state-writer --daemon` | write-behind: the only writer of the per-portfolio state DBs |
| `ingress` | `ingress --daemon --portfolios-root` | publishes `CandleClosed` onto `pf.{id}.feed`, per portfolio, after that portfolio's cursor |
| `enricher`| `enricher --daemon --portfolios-root` | DSL boxes for the Enricher role → `pf.{id}.enrichment` |
| `strategy`| `strategy --daemon --portfolios-root` | DSL boxes for the Strategy role → republishes the closed candle + its setups on `pf.{id}.exec` |
| `trade`   | `trade --daemon --portfolios-root` | DSL boxes for the Broker role; `pf.{id}.exec` only → trade results |
| `tester`  | ingress batch + `backtest` | component-wired backtest, fails if it does not match the known-good 9 trades / 6 wins / +9.0R |

Each daemon also accepts `--context <id> --instrument <sym> --timeframe <tf>
--dsl <file>` to run a single portfolio in the foreground, which is what the
test harnesses use.

## Usage

```bash
cd deploy

# build everything (includes the tester profile)
docker compose --profile test -f docker-compose.test.yml build

# start the daemons
docker compose -f docker-compose.test.yml up -d

# follow one component
docker compose -f docker-compose.test.yml logs -f enricher

# authoritative backtest through the component wiring
docker compose -f docker-compose.test.yml run --rm tester

# tear down (and drop the seeded candle store)
docker compose -f docker-compose.test.yml down -v

# first run on a host with root-owned volumes:
#   chown -R 10001:10001 deploy/data deploy/portfolios
```

The candle store is seeded from local parquet files instead of the Dukascopy
datafeed. Override the source directory with `PARQUET_DIR`:

```bash
PARQUET_DIR=/path/to/parquet docker compose -f docker-compose.test.yml up -d
```

Docker Compose is the supported deployment path. `deploy/systemd/` predates the
multi-portfolio supervisor (old binary names, no state-writer / trade / strategy
units) and should be rewritten, not copied, if a non-Docker deployment is needed.

## Portfolios

```
deploy/portfolios/
├── audcad_mr/          k=3.5, ema20/atr14/swing k=3.5 (mean reversion)
│   ├── portfolio.json
│   ├── strategy.dsl
│   └── state/          created at runtime, gitignored
└── audcad_fast/        k=0.5, ema10/atr7/swing k=0.5
    ├── portfolio.json
    └── strategy.dsl
```

- `portfolio.json` = identity, universe, strategy binding, capital.
- `strategy.dsl` = the boxes and wiring for this portfolio
  (`name := kind { field: value }`, refs wire producers to consumers).
- The DSL role plan decides the subjects; nothing in a component hard-codes
  them. Absent DSL → built-in atr/ema/swing graph from `strategy.params`.
- A portfolio that fails to load or compile is skipped loudly; the others run.
- Registry reload is restart-only in v1.

## Notes / caveats

- **Start order is a readiness chain, not a sleep.** Consumers write
  `RELIANT_READY_FILE` only after their NATS subscription exists *and*
  `flush` has been acknowledged by the server (`subscribe_tagged` blocks on
  flush), and the compose healthcheck keys on that file, so
  `state-writer → trade → strategy → enricher → ingress` is enforced by
  `depends_on: service_healthy`. A publisher therefore cannot lose its first
  burst into a subscription that does not exist yet. Core NATS does not replay,
  so this is the only correct ordering.
- **Shutdown drains, then snapshots.** SIGINT/SIGTERM set a shutdown flag; the
  dispatcher stops enqueueing and every worker drains what is already queued
  (bounded by a 30 s deadline) before the final per-portfolio snapshot is
  published and the writer flushes. Queued events are never discarded, and a
  restart with all cursors at the last applied bar replays nothing (verified:
  0 new trades, 0 candle republishes).
- **Candle ordering is structural, not paced.** `ingress` publishes as fast as it
  can. Each stage drops bars whose timestamp is ≤ its context's `latest`, so
  at-least-once redelivery is harmless. The enricher republishes each closed
  candle on `pf.{id}.enrichment` before its derived facts, and the strategy
  republishes that candle on `pf.{id}.exec` as the first event of the bar,
  followed by that bar's setups on the *same* subject. The trade engine has a
  single subscription and never sees raw ingress, so
  `candle(N) → setup(N) → candle(N+1)` is guaranteed by NATS per-subject
  ordering. No sleeps, no replay rate, no cross-subject races.
- **One bus owner per process.** Workers never touch NATS; they hand
  `(subject, event)` to a single publisher thread. A worker only takes a
  portfolio whose runtime lock is free (`try_lock`, never `lock`), so a busy
  portfolio is skipped rather than blocking the pool.
- **Warm-up** bars go to `pf.{id}.warmup`, which only the enricher subscribes to.
  They advance its indicators but are never published downstream, so warm-up
  can never open a historical trade.
- The fetcher throttles (`--request-delay-ms`, default 150 ms) and retries
  with backoff. The Dukascopy datafeed returns 429/503 when hammered and drops
  requests that do not look like a browser, so the client sends a browser UA and
  referer.

## State

Each component keeps its context **in memory** and is authoritative for it. State
files are only touched at startup (one read of the component's own key) and by
`state-writer` (write-behind); nothing in an event loop ever waits on SQLite.

| store | owner | contents |
|-------|-------|----------|
| `data/candles.db` | ingress, enricher | candles (shared, portfolio-agnostic) |
| `data/state-ingress.sqlite` | ingress | provider watermark `{inst}:{tf}:last_ts` |
| `portfolios/{id}/state/state-enricher.sqlite` | enricher | indicators/swings **and** the pipeline cursor `{pf}:{inst}:{tf}:cursor` |
| `portfolios/{id}/state/state-strategy.sqlite` | strategy | strategy state and decisions |
| `portfolios/{id}/state/state-broker.sqlite` | trade (broker) | open position, bars held, entry/stop/target |

- **Write-behind.** After processing a bar a component publishes
  `Event::ContextState { role, context, instrument, timeframe, version, state }`
  on the global `state` subject. `state-writer` is its only subscriber: it
  coalesces the latest snapshot per key and flushes every `--flush-ms` (250 ms),
  one transaction per flush, and again on shutdown.
- **Routing.** `--portfolios-root ROOT` derives the destination from the key
  prefix (`{pf}:…` → `ROOT/{pf}/state/state-{role}.sqlite`), so a snapshot can
  never be written into another portfolio's file. The file is opened lazily and
  the parent directory created on demand.
- **Cursor.** For every `*:enricher:state` row the writer also writes
  `{pf}:{inst}:{tf}:cursor = version` in the same transaction. Ingress reads it
  (read-only, cached), publishes bars strictly after it, and never advances it —
  the enricher owns it, so a stalled ingress cannot skip work.
- **Key layout** is `{context}:{instrument}:{timeframe}:{role}:state`; keys are
  never positional, so a portfolio can host several instruments/timeframes in
  one file.
- **Throttle.** Snapshots are published at most every 2 s (`StateCheckpoint`),
  plus one on shutdown.
- **Crash window.** A hard kill loses at most `--flush-ms` (250 ms) of buffered
  snapshots, and at most the events still in the queue. On a clean stop both
  SIGINT and SIGTERM set the shutdown flag, workers drain the queue (30 s
  bound), a final snapshot per portfolio is published and the writer flushes
  before exit.
- **Restart semantics.** The pipeline cursor is the resume point: ingress
  republishes from `cursor + 1`, every stage dedupes, so down/up continues
  without data loss or duplicate bars. Deleting a state directory is the reset
  switch.
