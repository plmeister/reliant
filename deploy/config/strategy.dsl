// AUDCAD mean-reversion: take pullbacks off a confirmed swing, with EMA/ATR
// context. Keep this in step with `portfolio.json` (same id, same timeframe).

ema   := ema   { period: 20 }
atr   := atr   { period: 14 }
swing := swing { k: 3.5, lookback: 5 }

strategy := strategy {
  k: 3.5,
  ema: ema,
  atr: atr,
  swing: swing,
}

sizing := sizing { setups: strategy }
broker := broker { trades: sizing }
