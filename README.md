# Reliant — component-wired trading backtest & audit toolkit

Reliant is a Rust workspace that replays candle data through role-separated
graphs (enricher → strategy → broker) and reports the resulting trades. It can
run as a one-shot backtest over audit files, or as long-running NATS-backed
daemons for live/paper trading.

## Repository layout

```
crates/                 Rust workspace members
  cli/                  binaries + supervisor helpers
  engine/               graph, context, nodes (indicators, strategy, broker)
  dsl/                  portfolio strategy DSL compiler
  datamodel/            Event, Symbol, Timeframe, Candle
  messaging/            in-memory + NATS bus, audit read/write
  store/                candle store, parquet/dukascopy providers
  strategy/ portfolio/  portfolio + strategy helpers
  indicators/
deploy/                 Docker Compose stack, runbook, portfolios, Dockerfile
tools/audit-viewer/     web UI + backend for inspecting audit files and trades
notes.md                data model notes
```

## Build

```sh
cd /home/scott/src/reliant
cargo build --release
```

Binaries land in `target/release/`: `backtest`, `ingress`, `audit`,
`fetcher`, `trade`, `strategy`, `enricher`, `portfolio`, `state-writer`,
`reliant`.

## Where settings, data and portfolios live

### XDG (user-level, used by the daemons and CLI defaults)

| Purpose | Path |
|---|---|
| Config / portfolios | `~/.config/reliant/` (or `$XDG_CONFIG_HOME/reliant`) |
| Portfolios | `~/.config/reliant/portfolios/<id>/` |
| Data (candles.db) | `~/.local/share/reliant/` (or `$XDG_DATA_HOME/reliant`) |
| State / audit files | `~/.local/state/reliant/` (or `$XDG_STATE_HOME/reliant`) |
| Audit viewer default audit dir | `~/.local/state/reliant/audit` |

### Repo / deploy (used by Docker and the documented runbook)

| Purpose | Path |
|---|---|
| Stack config | `deploy/docker-compose.yml`, `deploy/docker-compose.test.yml` |
| Portfolios (deploy) | `deploy/portfolios/<id>/` |
| Candle store (deploy) | `deploy/data/candles.db` |
| Audit files (deploy) | `deploy/data/audit/` |
| Audit viewer frontend | `tools/audit-viewer/frontend/` |

A portfolio directory contains at least:

```
<portfolio>/
├── portfolio.json      identity, instruments, timeframes, strategy binding, capital
└── strategy.dsl        boxes and wiring for this portfolio
```

`portfolio.json` keys: `instruments`, `timeframes`, `tradeTimeframe`,
`contextTimeframes`, `strategy.file`, `strategy.params`, `account.startingBalance`,
`account.riskPct`. See `deploy/PORTFOLIOS.md` for the DSL model.

## Running backtests

### One-shot backtest over audit files

1. Generate an ingress audit file (single instrument):

```sh
./target/release/ingress \
  --instrument AUDCAD --timeframe 1d \
  --candle-db deploy/data/candles.db \
  --output /tmp/oc/AUDCAD_all.dat
```

For a whole portfolio's universe, loop over its instruments or use
`--portfolio <id> --portfolios-root <root>`.

2. Run the component-wired backtest:

```sh
./target/release/backtest \
  --portfolio deploy/portfolios/audcad_mr \
  --ingress '/tmp/oc/AUDCAD_all.dat'
```

Or with an explicit symbol list and DSL:

```sh
./target/release/backtest \
  --symbols AUDCAD \
  --dsl deploy/portfolios/audcad_mr/strategy.dsl \
  --timeframe 1d --balance 10000 --risk 0.01 \
  --ingress '/tmp/oc/AUDCAD_all.dat'
```

Useful flags:

- `--ingress` accepts a single file, comma list, or glob (`*.dat`).
- `--trades-out <path>` writes closed trades as JSONL.
- `--enrichment-out <path>` writes enrichment events as an audit `.dat`.
- `--signals-out <path>` writes setup signals as an audit `.dat`.

### Output

The backtest prints per-symbol results, then a monthly breakdown, then overall
totals:

```
backtest: files=1 symbols=1
AUDCAD   trades=4 wins=3 total_r=5.000
MONTHLY:
2024-... trades=... wins=... win_rate=...% total_r=...
AGGREGATE: files=1 trades=4 wins=3 win_rate=75.0% total_r=5.000
```

Monthly rows are sorted by `YYYY-MM` and grouped by trade exit time (falling
back to entry time when no exit time is present). Each month reports number of
trades, wins, win rate and total R; the aggregate row reports overall totals.

### Docker test gate

```sh
cd deploy
docker compose --profile test -f docker-compose.test.yml run --rm tester
```

This builds an ingress audit file and runs the backtest, failing unless the result
matches the known-good baseline.

## Audit dashboard (audit viewer)

The audit viewer is a web UI over `.dat` / `.dat.zstd` audit files plus the
portfolio trade log.

### Build and run the backend

```sh
cd /home/scott/src/reliant
cargo build --release --manifest-path tools/audit-viewer/backend/Cargo.toml
./tools/audit-viewer/backend/target/release/audit-server \
  --audit-dir deploy/data/audit \
  --candle-db deploy/data/candles.db \
  --portfolios-root deploy/portfolios \
  --port 8787
```

Defaults: audit dir `~/.local/state/reliant/audit`, candle db
`~/.local/share/reliant/candles.db`, portfolios root
`~/.config/reliant/portfolios`, port `8787`.

Open `http://127.0.0.1:8787`. The frontend is served by the same binary (plain
HTML/JS/CSS, no build step).

### Features

- Browse audit files (size, count, time range).
- Paginated message table with topic/type filters and free-text search.
- Inspect full JSON for any message.
- Candle charts and timelines (indicators, swings, setups) from audit files.
- Portfolio trade list from `state/trades.jsonl` plus open positions from broker
  state snapshots.

### API

- `GET /api/files`
- `GET /api/files/{name}/meta`
- `GET /api/files/{name}/messages?offset=&limit=&topic=&q=`
- `GET /api/files/{name}/messages/{idx}`
- `GET /api/instruments`, `GET /api/candles`
- `GET /api/portfolios`, `GET /api/portfolios/{id}/trades`

## Single-trade audit (`audit` binary)

Replays a portfolio's DSL and re-prices recorded trades under attainable-fill
assumptions:

```sh
./target/release/audit \
  --portfolio deploy/portfolios/audcad_mr \
  --instrument AUDCAD --timeframe 1d \
  --candle-db deploy/data/candles.db \
  --jsonl /tmp/oc/audit.jsonl
```

## Long-running daemons

See `deploy/OPERATIONS.md` for the full runbook. Quick start:

```sh
cd deploy
docker compose -f docker-compose.yml up -d
```

Components: `nats`, `seed`, `fetcher`, `state-writer`, `ingress`, `enricher`,
`strategy`, `trade`. They share `deploy/data/candles.db` and per-portfolio
state DBs under `deploy/portfolios/<id>/state/`.

## Notes

- Timestamps in audit files fall back to the payload's candle timestamp when
  the audit header carried 0.
- Deleting a portfolio's `state/` directory resets its pipeline cursor.
- `deploy/systemd/` predates the multi-portfolio supervisor and should not be
  used as-is.
