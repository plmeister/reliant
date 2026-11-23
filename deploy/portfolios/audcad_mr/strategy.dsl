// AUDCAD mean-reversion: take pullbacks off a confirmed swing, with EMA/ATR
// context. Keep this in step with `portfolio.json` (same id, same timeframe).

ema   := ema   { period: 20 }
atr   := atr   { period: 14 }
swing := swing { k: 4.0, lookback: 5 }

strategy := strategy {
  k: 4.0,
  stop_atr_mult: 2.0,
  target_atr_mult: 2.0,
  ema: ema,
  atr: atr,
  swing: swing,
}

sizing := sizing { setups: strategy }
broker := broker { trades: sizing }
