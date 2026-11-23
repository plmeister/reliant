// raw candle data
candle (instrument, timestamp, timeframe, o,h,l,c,v)

we get a stream of candleclosed events into the system, which are then processed by the indicators and processors defined below. The candleclosed event is generated when a candle is closed, and contains the open, high, low, close and volume of the candle.

each candle gets picked up by whatever indicator is defined / live, and the correct context state is mutated based on the candle data. The context state is keyed by the name of the indicator / processor, and is serialised to JSON and stored in a bag owned by the context.

this is data collection / enrichment phase

when all candles for a particular timestamp have been sent, then we get a tick event which is the trigger for the next stage to occur which is the strategy processing phase - it takes the state from each context and examines it to see if it matches its configured setup trigger - then emits a setup signal

we first scan the system to see what instruments and timeframes are in scope, and then create a context for each <instrument, timeframe> pair. Each context has a bag of serialised state, which is keyed by the name of the indicator / processor. The state is serialised to JSON and stored in the bag, and can be retrieved by the indicator / processor when it is called again for the same <instrument, timeframe>.

ema "ema20" (n_candles: 20)

atr "atr14" (n_candles: 14)

swingdetector "swing" (n_candles: window)

interface serialisedstate<name>
// any indicator / processor which needs to store state should implement this interface, and the state will be stored in a bag owned by a context, keyed by the name given to the indicator

// multiple contexts exist - one for each <instrument, timeframe> in scope. Each context has a bag of serialised state, which is keyed by the name of the indicator / processor. The state is serialised to JSON and stored in the bag, and can be retrieved by the indicator / processor when it is called again for the same <instrument, timeframe>.
context<instrument, timeframe> (
state {
serialisedstate<"latest"> - latest timstamp that has been processed for this context so far
serialisedstate<"ema20">
serialisedstate<"atr14">
serialisedstate<"swing">
}
)
