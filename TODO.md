# Recovery & architectural QA follow-up backlog

## Critical: end-to-end recovery (point A)

- [x] Persist a cursor for every role, not only enricher.
  - [x] State writer writes `{base}:cursor` for all role snapshots.
  - [x] Ingress reads all role cursors and uses the minimum as its resume point.
  - [ ] (Follow-up) cursors alone do not replay downstream events; durable hop still required.
- [x] Make stages idempotent but still republish on replay.
  - [x] Enricher caches last derived enrichment events per bar.
  - [x] On a replayed (already-processed) candle, republish cached enrichment without re-running indicators.
  - [x] Strategy/broker replay publish exec candle and setups; broker dedupes setups to avoid duplicate trades.
  - [x] Broker dedupe persisted in context state (survives restart).
  - [x] Enricher output persisted in context state for durable replay across restarts.
- [ ] Give each hop a durable input. (Deferred: JetStream/outbox not yet implemented.)
  - [ ] Option 1: JetStream streams + durable consumers per subject.
  - [ ] Option 2: SQLite outbox per stage with per-consumer offsets.
  - [ ] Choose one and document the trade-offs.
  - [x] Interim: per-role cursors + cached republish + broker dedupe provide in-process replay.
- [ ] Make checkpoint advancement transactional with downstream publish.
  - [ ] A role advances its cursor only after processing the bar and publishing output.
  - [ ] State snapshot + cursor write happen in the same flush.
- [ ] Add independent-restart tests.
  - [ ] Kill enricher mid-bar, restart, compare final state/trades to uninterrupted run.
  - [ ] Kill strategy mid-bar, restart, compare.
  - [ ] Kill broker mid-bar, restart, compare.
  - [ ] Kill state-writer mid-flush, restart, compare.

## High priority (remaining QA items)

- [ ] Make readiness truly reflect subscription success.
  - [ ] Replace panic-on-subscribe-failure with explicit Result propagation where practical.
  - [ ] Ensure ready file is never written if any required subscription fails.
- [ ] Eliminate remaining ignored `Result`s from `MessageBus::publish`.
  - [ ] Handle or log publish errors at call sites (ingress, enricher, strategy, trade, fetcher).
- [ ] Improve shutdown drain.
  - [ ] Drain dispatcher inbound receiver (done).
  - [ ] Drain warm-up receiver (done).
  - [ ] Coordinate final state-writer flush after all workers finish.
- [ ] Increase transport test fidelity.
  - [ ] Add integration tests against a real NATS instance.
  - [ ] Cover wildcard subjects and queue groups in tests.

## Medium priority

- [ ] Decide on JetStream usage.
  - [ ] Either remove `-js` permanently (done) or adopt JetStream for recovery.
- [ ] Add end-to-end trace documentation.
  - [ ] Trace one candle ingress -> enricher -> strategy -> broker -> state-writer.
  - [ ] Include failure/restart paths.

## Completed from QA

- [x] Fix swing signal timestamp semantics (use confirmation timestamp).
- [x] Make `MessageBus::publish` return `Result<(), BusError>`.
- [x] Make subscription failures explicit (panic instead of silent disconnected channel).
- [x] Remove `-js` from compose files.
- [x] Improve in-memory bus: wildcard matching and queue groups.
- [x] Drain inbound receivers on shutdown.
- [x] Document recovery caveat in README.
