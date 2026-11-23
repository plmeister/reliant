use reliant_datamodel::{Candle, Symbol, Timeframe};
use reliant_store::{
    candles_to_events, read_parquet, InMemorySource, InMemoryStateStore,
    MarketDataSource, ParquetDirProvider, StateStore, SqliteStateStore,
};
use chrono::DateTime;

fn candle(ts: i64, close: f64) -> Candle {
    Candle {
        timestamp: DateTime::from_timestamp(ts, 0).unwrap(),
        open: close - 0.5,
        high: close + 1.0,
        low: close - 1.0,
        close,
        volume: 100.0,
    }
}

#[test]
fn in_memory_source_roundtrip() {
    let mut src = InMemorySource::new();
    let sym = Symbol("EURUSD".into());
    src.insert(&sym, &Timeframe::D1, vec![candle(0, 1.1)]);
    assert_eq!(src.candles(&sym, &Timeframe::D1).len(), 1);
    assert!(src.candles(&Symbol("GBPUSD".into()), &Timeframe::D1).is_empty());
}

#[test]
fn in_memory_state_store_roundtrip() {
    let mut s = InMemoryStateStore::new();
    assert!(s.load("k").unwrap().is_none());
    s.save("k", "v").unwrap();
    assert_eq!(s.load("k").unwrap().as_deref(), Some("v"));
}

#[test]
fn sqlite_state_store_roundtrip() {
    let mut s = SqliteStateStore::new(":memory:").unwrap();
    s.save("k", "v").unwrap();
    assert_eq!(s.load("k").unwrap().as_deref(), Some("v"));
}

#[test]
fn parquet_reader_reads_real_file() {
    let path = "/home/scott/src/marketatlas/data/study/AUDCAD.1d.parquet";
    let candles = read_parquet(path).expect("read parquet");
    assert!(!candles.is_empty());
    assert!(candles[0].close > 0.0);
}

#[test]
fn parquet_provider_and_events() {
    use reliant_store::CandleProvider;
    let provider = ParquetDirProvider {
        root: "/home/scott/src/marketatlas/data/study".into(),
    };
    let candles = provider
        .candles(&Symbol("AUDCAD".into()), &Timeframe::D1)
        .expect("provider");
    assert!(!candles.is_empty());
    let n = candles.len();
    let events = candles_to_events(&Symbol("AUDCAD".into()), &Timeframe::D1, candles);
    assert_eq!(events.len(), n);
    let ticks = events
        .iter()
        .filter(|e| matches!(e, reliant_datamodel::Event::Tick { .. }))
        .count();
    assert_eq!(ticks, 0);
}

#[test]
fn interleaved_events_groups_by_timestamp() {
    use reliant_store::interleaved_events;
    let c = |ts: i64| candle(ts, 1.0 + ts as f64);
    let srcs = vec![
        (Symbol("A".into()), Timeframe::D1, vec![c(1), c(2)]),
        (Symbol("B".into()), Timeframe::D1, vec![c(1), c(3)]),
    ];
    let evs = interleaved_events(srcs);
    // 4 candleclosed + 0 ticks
    let cc = evs.iter().filter(|e| matches!(e, reliant_datamodel::Event::CandleClosed { .. })).count();
    let tk = evs.iter().filter(|e| matches!(e, reliant_datamodel::Event::Tick { .. })).count();
    assert_eq!(cc, 4);
    assert_eq!(tk, 0);
}

#[test]
fn interleaved_events_from_parquet() {
    use reliant_store::{interleaved_events, ParquetDirProvider, CandleProvider};
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let a = provider.candles(&Symbol("AUDCAD".into()), &Timeframe::D1).unwrap();
    let b = provider.candles(&Symbol("AUDNZD".into()), &Timeframe::D1).unwrap();
    let n = a.len() + b.len();
    let evs = interleaved_events(vec![
        (Symbol("AUDCAD".into()), Timeframe::D1, a),
        (Symbol("AUDNZD".into()), Timeframe::D1, b),
    ]);
    let cc = evs.iter().filter(|e| matches!(e, reliant_datamodel::Event::CandleClosed { .. })).count();
    let tk = evs.iter().filter(|e| matches!(e, reliant_datamodel::Event::Tick { .. })).count();
    assert_eq!(cc, n);
    assert_eq!(tk, 0);
}

#[test]
fn dukascopy_canonical_mapping() {
    use reliant_store::canonical_to_dukascopy;
    assert_eq!(canonical_to_dukascopy("eurusd"), "EURUSD");
    assert_eq!(canonical_to_dukascopy("EUR/USD"), "EURUSD");
    assert_eq!(canonical_to_dukascopy("xau-usd"), "XAUUSD");
}

#[test]
fn dukascopy_url_format() {
    use chrono::NaiveDate;
    use reliant_store::min1_url;
    let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    assert_eq!(
        min1_url("EURUSD", d),
        "https://datafeed.dukascopy.com/datafeed/EURUSD/2024/00/15/BID_candles_min_1.bi5"
    );
}

#[test]
fn dukascopy_timeframe_minutes() {
    use reliant_store::timeframe_minutes;
    use reliant_datamodel::Timeframe;
    assert_eq!(timeframe_minutes(&Timeframe::M1), 1);
    assert_eq!(timeframe_minutes(&Timeframe::D1), 1440);
}

#[test]
fn dukascopy_aggregate_empty() {
    use reliant_store::aggregate;
    assert!(aggregate(vec![], 5).is_empty());
}

#[test]
#[ignore]
fn dukascopy_fetch_one_day() {
    use chrono::NaiveDate;
    use reliant_store::DukascopyProvider;
    use reliant_datamodel::{Symbol, Timeframe};
    let p = DukascopyProvider::new(100000.0);
    let from = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let to = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
    let candles = p.fetch(&Symbol("EURUSD".into()), &Timeframe::M1, from, to).unwrap();
    assert!(!candles.is_empty(), "expected minute candles");
    // aggregate to daily
    use reliant_store::aggregate;
    let daily = aggregate(candles, 1440);
    assert_eq!(daily.len(), 1);
}
