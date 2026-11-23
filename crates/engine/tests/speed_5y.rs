use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{AtrNode, CandleSource, DummyBroker, EmaNode, GeometryNode, Graph, Node, StrategyNode, SwingNode};
use reliant_store::{CandleProvider, ParquetDirProvider, interleaved_events};
use std::time::Instant;

#[test]
#[ignore]
fn speed_five_years_all_instruments() {
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let symbols: Vec<String> = "AUDCAD AUDJPY AUDNZD AUDUSD CADJPY EURAUD EURCAD EURCHF EURGBP EURJPY EURNZD EURUSD GBPAUD GBPJPY GBPNZD GBPUSD NZDCAD NZDJPY NZDUSD USDCAD USDCHF XAGUSD XAUUSD"
        .split_whitespace().map(String::from).collect();

    let five_years = 252 * 5;
    let mut sources = Vec::new();
    let mut nodes: Vec<Box<dyn Node>> = Vec::new();
    for sym in &symbols {
        let symbol = Symbol(sym.clone());
        let timeframe = Timeframe::D1;
        let mut candles = provider.candles(&symbol, &timeframe).unwrap();
        if candles.len() > five_years {
            candles = candles[candles.len() - five_years..].to_vec();
        }
        sources.push((symbol.clone(), timeframe.clone(), candles.clone()));
        nodes.push(Box::new(CandleSource::new(&format!("cs_{}", sym), symbol.clone(), timeframe.clone(), candles)));
        nodes.push(Box::new(AtrNode::new(&format!("atr_{}", sym), symbol.clone(), timeframe.clone(), 14)));
        nodes.push(Box::new(EmaNode::new(&format!("ema_{}", sym), symbol.clone(), timeframe.clone(), 20)));
        nodes.push(Box::new(SwingNode::new(&format!("swing_{}", sym), symbol.clone(), timeframe.clone(), 3.5, 5)));
        nodes.push(Box::new(DummyBroker::new(&format!("broker_{}", sym), symbol.clone(), timeframe.clone())));
    }
    nodes.push(Box::new(StrategyNode::new("strat", 3.5)));
    nodes.push(Box::new(GeometryNode::new("geom", 10000.0, 0.01)));

    let events = interleaved_events(sources);

    let t0 = Instant::now();
    let mut graph = Graph::new(nodes);
    let tagged: Vec<(String, Event)> = events.into_iter().map(|e| ("ingress".to_string(), e)).collect();
    let emitted = graph.run(tagged);
    let elapsed = t0.elapsed();

    let n_trades = emitted.iter().filter(|e| matches!(e, Event::TradeResult { .. })).count();
    let total_r: f64 = emitted.iter().filter_map(|e| if let Event::TradeResult { pnl_r, .. } = e { Some(*pnl_r) } else { None }).sum();
    println!("SPEED: instruments={} bars_per_inst={} total_events={} trades={} total_r={:.3} elapsed={:?} events_per_sec={:.0}",
             symbols.len(), five_years, emitted.len(), n_trades, total_r, elapsed,
             emitted.len() as f64 / elapsed.as_secs_f64());
}
