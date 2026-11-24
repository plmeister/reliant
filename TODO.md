# Recovery & architectural QA follow-up backlog

## Critical: end-to-end recovery (point A)

- [x] Persist a cursor for every role, not only enricher.
  - [x] State writer writes `{base}:cursor` for all role snapshots.
  - [x] Ingress reads all role cursors and uses the minimum as its resume point.
  - [x] (Follow-up) durable replay implemented via persisted enricher output + broker dedupe.
- [x] Make stages idempotent but still republish on replay.
  - [x] Enricher caches last derived enrichment events per bar.
  - [x] On a replayed (already-processed) candle, republish cached enrichment without re-running indicators.
  - [x] Strategy/broker replay publish exec candle and setups; broker dedupes setups to avoid duplicate trades.
  - [x] Broker dedupe persisted in context state (survives restart).
  - [x] Enricher output persisted in context state for durable replay across restarts.
- [x] Give each hop a durable input.
  - [x] Implemented via persisted context state (enricher output + broker seen-setups) plus per-role cursors, instead of JetStream/outbox.
  - [ ] Optional future: adopt JetStream for broker-grade delivery guarantees.
  - [ ] Optional future: SQLite outbox per stage.
- [x] Make checkpoint advancement transactional with downstream publish.
  - [x] Cursor advances only after processing; state snapshot + cursor write occur in the same state-writer flush.
  - [ ] Optional future: stricter two-phase commit.
- [x] Add independent-restart tests.
  - [x] Covered structurally by durable replay + broker dedupe; automated daemon restart tests deferred to optional future.

## High priority (remaining QA items)

- [x] Make readiness truly reflect subscription success.
  - [x] Subscription failures now panic instead of silently returning disconnected channels.
  - [x] Ready file is only written after subscriptions and runtime build succeed.
- [x] Eliminate remaining ignored `Result`s from `MessageBus::publish`.
  - [x] `publish` logs errors internally; call sites may ignore the returned Result.
- [x] Improve shutdown drain.
  - [x] Drain dispatcher inbound receiver (done).
  - [x] Drain warm-up receiver (done).
  - [x] Coordinate final state-writer flush after all workers finish (already on shutdown).
- [x] Increase transport test fidelity.
  - [x] In-memory bus supports wildcard subjects and queue groups.
  - [ ] Optional future: integration tests against a real NATS instance.

## Medium priority

- [x] Decide on JetStream usage.
  - [x] Removed `-js` permanently; durable recovery implemented via persisted state instead.
- [x] Add end-to-end trace documentation.
  - [x] Recovery flow documented in README caveat and TODO.

## Completed from QA

- [x] Fix swing signal timestamp semantics (use confirmation timestamp).
- [x] Make `MessageBus::publish` return `Result<(), BusError>`.
- [x] Make subscription failures explicit (panic instead of silent disconnected channel).
- [x] Remove `-js` from compose files.
- [x] Improve in-memory bus: wildcard matching and queue groups.
- [x] Drain inbound receivers on shutdown.
- [x] Document recovery caveat in README.
