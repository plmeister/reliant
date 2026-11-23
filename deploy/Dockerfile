FROM rust:1.88-slim AS builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev libsqlite3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release --workspace

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libsqlite3-0 libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/reliant /usr/local/bin/
COPY --from=builder /app/target/release/ingress /usr/local/bin/
COPY --from=builder /app/target/release/enricher /usr/local/bin/
COPY --from=builder /app/target/release/strategy /usr/local/bin/
COPY --from=builder /app/target/release/trade /usr/local/bin/
COPY --from=builder /app/target/release/state-writer /usr/local/bin/
COPY --from=builder /app/target/release/backtest /usr/local/bin/
COPY --from=builder /app/target/release/fetcher /usr/local/bin/
COPY --from=builder /app/target/release/portfolio /usr/local/bin/
RUN groupadd -g 10001 reliant && useradd -u 10001 -g 10001 -m reliant
USER 10001:10001
