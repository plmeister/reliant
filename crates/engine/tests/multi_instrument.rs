use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{AtrNode, CandleSource, EmaNode, Graph, Node, StrategyNode};
use reliant_store::{CandleProvider, ParquetDirProvider, interleaved_events};

#[test]
fn multi_instrument_interleaved_events_drive_graph() {
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let a = provider.candles(&Symbol("AUDCAD".into()), &Timeframe::D1).unwrap();
    let b = provider.candles(&Symbol("AUDNZD".into()), &Timeframe::D1).unwrap();
    let a = a.into_iter().take(25).collect();
    let b = b.into_iter().take(25).collect();

    let mut nodes: Vec<Box<dyn Node>> = Vec::new();
    for (sym, candles) in [("AUDCAD", a), ("AUDNZD", b)] {
        let sym = Symbol(sym.into());
        nodes.push(Box::new(CandleSource::new(&format!("cs_{}", sym.0), sym.clone(), Timeframe::D1.clone(), candles)));
        nodes.push(Box::new(AtrNode::new(&format!("atr_{}", sym.0), sym.clone(), Timeframe::D1.clone(), 14)));
        nodes.push(Box::new(EmaNode::new(&format!("ema_{}", sym.0), sym.clone(), Timeframe::D1.clone(), 20)));
    }
    nodes.push(Box::new(StrategyNode::new("strat", 3.5)));

    let events = interleaved_events(vec![
        (Symbol("AUDCAD".into()), Timeframe::D1, provider.candles(&Symbol("AUDCAD".into()), &Timeframe::D1).unwrap().into_iter().take(25).collect()),
        (Symbol("AUDNZD".into()), Timeframe::D1, provider.candles(&Symbol("AUDNZD".into()), &Timeframe::D1).unwrap().into_iter().take(25).collect()),
    ]);

    let mut graph = Graph::new(nodes);
    let events: Vec<(String, Event)> = events.into_iter().map(|e| ("ingress".to_string(), e)).collect();
    let emitted = graph.run(events);

    // strategy may or may not fire (needs 20 bars ema) — just ensure no panic
    let _ = emitted;
}
