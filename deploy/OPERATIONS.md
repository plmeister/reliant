# Operations Runbook

## 1. Backtest from Ingress Audit (step-by-step)

```sh
# 1. build
cargo build --release

# 2. ingress -> audit file
./target/release/ingress \
  --instrument AUDCAD --timeframe 1d \
  --candle-db deploy/data/candles.db \
  --output deploy/data/audit/AUDCAD_all.dat \
  --output-dir deploy/data/audit

# 3. component-wired backtest (same graphs the daemons use)
./target/release/backtest \
  --instruments AUDCAD --timeframe 1d --k 3.5 \
  --ingress 'deploy/data/audit/*.dat' \
  --balance 10000 --risk 0.01

# expected: files=1 trades=9 wins=6 win_rate=66.7% total_r=9.000
```

## 2. Ingress for Different Timespans

```sh
./target/release/ingress --instrument AUDCAD --timeframe 1h \
  --candle-db deploy/data/candles.db --output deploy/data/audit/AUDCAD_1h_all.dat

./target/release/ingress --instrument AUDCAD --timeframe 1w ...

# trim ingestion window (ISO dates)
./target/release/ingress --instrument AUDCAD --timeframe 1d \
  --from 2025-01-01 --to 2025-06-30
```

- `--timeframe`: `1m|5m|1h|1d|1w`
- fetcher accepts `--timeframes` comma list (default `1d`).
- strategy `max_hold` (secs) -> bars via `ceil(max_hold / bar_seconds())`.

## 3. Paper-Run Mode

Trading uses `DummyBroker` -> paper fills only, no real order path.

### 3.1 Single tick-style run
```sh
./target/release/ingress --instrument AUDCAD --timeframe 1d \
  --candle-db deploy/data/candles.db --state-db /tmp/state-ingress.sqlite

./target/release/trade --instrument AUDCAD --timeframe 1d --balance 10000 \
  --risk 0.01 --state-db /tmp/state-trade.sqlite \
  --dsl deploy/portfolios/audcad_mr/strategy.dsl \
  --portfolios-root deploy/portfolios --workers 1

./target/release/strategy --instrument AUDCAD --timeframe 1d --state-db /tmp/state-strategy.sqlite \
  --dsl deploy/portfolios/audcad_mr/strategy.dsl \
  --portfolios-root deploy/portfolios --workers 1
```

### 3.2 Daemon-style always-on
```sh
docker compose -f deploy/docker-compose.yml up -d --build
```
Readiness chain: `nats` -> `seed` -> `state-writer` -> `trade` -> `strategy` -> `enricher` -> `ingress`.

### 3.3 Scheduled candle downloads
```sh
./target/release/fetcher --instrument AUDCAD --timeframes 1d,1h \
  --candle-db deploy/data/candles.db --daemon \
  --interval 3600 --max-stale-days 1 --request-delay-ms 150
```

### 3.4 Monitoring signals + results
- Trade log line: `portfolio {id} trades=N wins=W win_rate=X% total_r=Y`.
- Broker state (drain signal, completion): `state-broker.sqlite` `latest` kv per `{pf}:{inst}:{tf}:broker:state`.
- Per-portfolio state files: `deploy/portfolios/{id}/state/state-{enricher,strategy,broker}.sqlite`.
- `state-writer` coalesces context snapshots (write-behind), no component writes DB directly.

## 4. Test Gate

```sh
docker compose -f deploy/docker-compose.test.yml up -d --build
docker compose -f deploy/docker-compose.test.yml run --rm tester
# expect: trades=9 wins=6 win_rate=66.7% total_r=9.000
```
