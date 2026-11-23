// AUDCAD variant: faster EMA/ATR and a much tighter swing threshold. Its only
// purpose is to prove portfolios are wired independently — different boxes,
// different trade counts, no shared state.

ema   := ema   { period: 10 }
atr   := atr   { period: 7 }
swing := swing { k: 0.5, lookback: 3 }

strategy := strategy {
  k: 0.5,
  ema: ema,
  atr: atr,
  swing: swing,
}

sizing := sizing { setups: strategy }
broker := broker { trades: sizing }
