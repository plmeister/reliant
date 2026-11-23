use reliant_datamodel::{Candle, Symbol, Timeframe};
use chrono::{TimeZone, Utc};

#[test]
fn candle_round_trips_through_serde() {
    let c = Candle {
        timestamp: Utc.timestamp_opt(0, 0).unwrap(),
        open: 1.0,
        high: 2.0,
        low: 0.5,
        close: 1.5,
        volume: 10.0,
    };
    let json = serde_json::to_string(&c).unwrap();
    let back: Candle = serde_json::from_str(&json).unwrap();
    assert_eq!(back.close, 1.5);
    assert_eq!(Timeframe::D1, Timeframe::D1);
    assert_eq!(Symbol("EURUSD".into()), Symbol("EURUSD".into()));
}
