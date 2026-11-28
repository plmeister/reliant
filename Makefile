# Convenience targets for the strategy development loop.
# Usage:
#   make dev P=audcad_mr CDB=/path/to/candles.db
#   make reset P=audcad_mr
#   make audit I=AUDCAD T=1d CDB=/path/to/candles.db
#   make test

P ?= audcad_mr
I ?= AUDCAD
T ?= 1d
CDB ?= deploy/data/candles.db
ROOT ?= $(HOME)/.config/reliant/portfolios
BIN ?= ./target/debug

.PHONY: dev reset audit test build

build:
	cargo build -p reliant-cli

test:
	cargo test --workspace

audit:
	@mkdir -p $(HOME)/.local/state/reliant/audit
	$(BIN)/ingress --instrument $(I) --timeframe $(T) --candle-db $(CDB) \
	  --output $(HOME)/.local/state/reliant/audit/$(I)_$(T).dat

reset:
	$(BIN)/reliant reset --portfolio $(ROOT)/$(P)

dev:
	$(BIN)/reliant dev --portfolio $(ROOT)/$(P) --candles-db $(CDB) --instrument $(I)
