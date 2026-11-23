use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{AtrNode, CandleSource, DummyBroker, EmaNode, GeometryNode, Graph, Node, StrategyNode, SwingNode};
use reliant_messaging::{write_audit, event_to_audit, read_audit};
use reliant_store::{CandleProvider, ParquetDirProvider, interleaved_events};

#[test]
fn backtest_five_years_audcad() {
    let symbol = Symbol("AUDCAD".into());
    let timeframe = Timeframe::D1;
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let mut candles = provider.candles(&symbol, &timeframe).unwrap();
    // last ~5 years of daily bars
    let five_years = 252 * 5;
    if candles.len() > five_years {
        candles = candles[candles.len() - five_years..].to_vec();
    }
    let n_bars = candles.len();

    // record ingress audit
    let ingress_path = "/tmp/oc/bt_ingress.dat";
    let _ = std::fs::remove_file(ingress_path);
    let ingress_events = interleaved_events(vec![(symbol.clone(), timeframe.clone(), candles.clone())]);
    let msgs: Vec<_> = ingress_events.iter().map(|e| event_to_audit(e, "ingress", 0)).collect();
    write_audit(ingress_path, &msgs).unwrap();

    // build graph
    let nodes: Vec<Box<dyn Node>> = vec![
        Box::new(CandleSource::new("cs", symbol.clone(), timeframe.clone(), candles)),
        Box::new(AtrNode::new("atr", symbol.clone(), timeframe.clone(), 14)),
        Box::new(EmaNode::new("ema", symbol.clone(), timeframe.clone(), 20)),
        Box::new(SwingNode::new("swing", symbol.clone(), timeframe.clone(), 3.5, 5)),
        Box::new(StrategyNode::new("strat", 3.5)),
        Box::new(GeometryNode::new("geom", 10000.0, 0.01)),
        Box::new(DummyBroker::with_params("broker", symbol.clone(), timeframe.clone(), "immediate".to_string(), 3)),
    ];
    let mut graph = Graph::new(nodes);

    // replay ingress
    let ingress = read_audit(ingress_path).unwrap();
    let events: Vec<Event> = ingress.into_iter().map(|m| serde_json::from_value(m.payload).unwrap()).collect();
    let tagged: Vec<(String, Event)> = events.into_iter().map(|e| ("ingress".to_string(), e)).collect();
    let emitted = graph.run(tagged);

    // collect trade results
    let results: Vec<(f64, String)> = emitted
        .iter()
        .filter_map(|e| if let Event::TradeResult { pnl_r, reason, .. } = e { Some((*pnl_r, reason.clone())) } else { None })
        .collect();

    let n_trades = results.len();
    let wins = results.iter().filter(|(r, _)| *r > 0.0).count();
    let total_r: f64 = results.iter().map(|(r, _)| r).sum();
    let avg_r = if n_trades > 0 { total_r / n_trades as f64 } else { 0.0 };
    let win_rate = if n_trades > 0 { wins as f64 / n_trades as f64 * 100.0 } else { 0.0 };

    println!("BACKTEST 5y AUDCAD: bars={} trades={} wins={} win_rate={:.1}% total_r={:.3} avg_r={:.4}",
             n_bars, n_trades, wins, win_rate, total_r, avg_r);

    // record trade audit
    let trade_path = "/tmp/oc/bt_trades.dat";
    let _ = std::fs::remove_file(trade_path);
    let trade_msgs: Vec<_> = emitted.iter()
        .filter(|e| matches!(e, Event::TradeResult { .. }))
        .map(|e| event_to_audit(e, "trade", 0))
        .collect();
    write_audit(trade_path, &trade_msgs).unwrap();

    println!("DEBUG: bars={} swing_pivots={} setup={} trade_signals={} trade_results={}",
             n_bars,
             emitted.iter().filter(|e| matches!(e, Event::SwingPivotEvent { .. })).count(),
             emitted.iter().filter(|e| matches!(e, Event::SetupSignal { .. })).count(),
             emitted.iter().filter(|e| matches!(e, Event::TradeSignal { .. })).count(),
             n_trades);
    assert!(n_trades > 0, "expected at least one trade");
}
