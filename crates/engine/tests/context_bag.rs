use reliant_datamodel::{Candle, Symbol, Timeframe};
use reliant_engine::Context;
use reliant_indicators::{Atr, Ema};
use chrono::DateTime;

#[test]
fn context_bag_holds_named_processor_state() {
    let mut ctx = Context::new(Symbol("EURUSD".into()), Timeframe::D1);
    ctx.register(Box::new(Ema::new("ema20", 3)));
    ctx.register(Box::new(Atr::new("atr14", 2)));

    let candle = Candle {
        timestamp: DateTime::from_timestamp(0, 0).unwrap(),
        open: 1.0,
        high: 2.0,
        low: 0.5,
        close: 1.5,
        volume: 10.0,
    };
    ctx.apply(&candle);

    let json = ctx.state_json();
    assert!(json.contains("ema20"));
    assert!(json.contains("atr14"));

    // roundtrip: load into a fresh context with same processors
    let mut ctx2 = Context::new(Symbol("EURUSD".into()), Timeframe::D1);
    ctx2.register(Box::new(Ema::new("ema20", 3)));
    ctx2.register(Box::new(Atr::new("atr14", 2)));
    ctx2.load_json(&json);
    assert_eq!(ctx2.state_json(), json);
}

#[test]
fn context_state_persists_via_sqlite() {
    use reliant_store::{SqliteStateStore, StateStore};
    let mut ctx = Context::new(Symbol("EURUSD".into()), Timeframe::D1);
    ctx.register(Box::new(Ema::new("ema20", 3)));
    ctx.register(Box::new(Atr::new("atr14", 2)));
    let candle = Candle {
        timestamp: DateTime::from_timestamp(0, 0).unwrap(),
        open: 1.0,
        high: 2.0,
        low: 0.5,
        close: 1.5,
        volume: 10.0,
    };
    ctx.apply(&candle);
    let json = ctx.state_json();

    let mut store = SqliteStateStore::new(":memory:").unwrap();
    store.save("EURUSD:D1", &json).unwrap();
    let loaded = store.load("EURUSD:D1").unwrap().unwrap();

    let mut ctx2 = Context::new(Symbol("EURUSD".into()), Timeframe::D1);
    ctx2.register(Box::new(Ema::new("ema20", 3)));
    ctx2.register(Box::new(Atr::new("atr14", 2)));
    ctx2.load_json(&loaded);
    assert_eq!(ctx2.state_json(), json);
}
