use reliant_datamodel::{Candle, Event, Symbol, Timeframe};
use reliant_engine::{Context, MarketContext, StateMachine};
use chrono::DateTime;

#[test]
fn candle_closed_updates_context_state() {
    let symbol = Symbol("EURUSD".into());
    let mut ctx = MarketContext::new();
    ctx.register(symbol.clone(), Timeframe::D1);
    let c = Context::new(symbol.clone(), Timeframe::D1);
    let _ = c;
    let candle = Candle {
        timestamp: DateTime::from_timestamp(0, 0).unwrap(),
        open: 1.0,
        high: 2.0,
        low: 0.5,
        close: 1.5,
        volume: 10.0,
    };
    let ev = Event::CandleClosed {
        symbol: symbol.clone(),
        timeframe: Timeframe::D1,
        candle,
    };
    ctx.handle_event(&ev);
    let c = ctx.get(&symbol, &Timeframe::D1).unwrap();
    assert!(c.latest.is_some());
}
