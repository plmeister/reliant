# Audit Viewer

Web-based inspector for reliant audit files (`.dat` / `.dat.zstd`).

## Run

```sh
cd /home/scott/src/reliant
cargo build --release --manifest-path tools/audit-viewer/backend/Cargo.toml
./tools/audit-viewer/backend/target/release/audit-server \
  --audit-dir deploy/data/audit --port 8787
# open http://127.0.0.1:8787
```

## Features

- Browse audit files in a directory (size, count, time range).
- Paginated message table: index, timestamp, topic, event type, summary.
- Filter by topic, event type, free-text search over payload + header.
- Click a row to inspect the full JSON (header + payload) in a side panel.
- Timestamps fall back to the payload's candle timestamp when the audit
  header carried 0 (ingress-generated audits).

## Backend API

- `GET /api/files` — list files
- `GET /api/files/{name}/meta` — count, topics, type counts, time range
- `GET /api/files/{name}/messages?offset=&limit=&topic=&q=` — paginated
- `GET /api/files/{name}/messages/{idx}` — single message

## Notes

Frontend is plain HTML/JS/CSS (no build step, no npm) so it works fully
offline. The Svelte/TS variant was not feasible without network access to the
npm registry; the vanilla implementation exposes the same data and filters.