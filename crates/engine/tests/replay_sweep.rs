use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{AtrNode, DummyBroker, EmaNode, GeometryNode, Graph, Node, StrategyNode, SwingNode};
use reliant_messaging::{write_audit, read_audit};
use reliant_store::{CandleProvider, ParquetDirProvider, interleaved_events};

#[test]
fn replay_ingress_audit_for_parameter_sweep() {
    let symbol = Symbol("AUDCAD".into());
    let timeframe = Timeframe::D1;
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let a = provider.candles(&symbol, &timeframe).unwrap();
    let a = a.into_iter().take(80).collect();
    let events = interleaved_events(vec![(symbol.clone(), timeframe.clone(), a)]);

    // 1. record ingress audit once
    let path = "/tmp/oc/sweep_ingress.dat";
    let _ = std::fs::remove_file(path);
    let msgs: Vec<_> = events.iter().map(|e| reliant_messaging::event_to_audit(e, "ingress", 0)).collect();
    write_audit(path, &msgs).unwrap();

    // 2. replay through two configurations
    let ingress = read_audit(path).unwrap();
    let ingress_events: Vec<Event> = ingress.into_iter().map(|m| serde_json::from_value(m.payload).unwrap()).collect();

    for k in [3.5_f64, 5.0] {
        let nodes: Vec<Box<dyn Node>> = vec![
            Box::new(AtrNode::new("atr", symbol.clone(), timeframe.clone(), 14)),
            Box::new(EmaNode::new("ema", symbol.clone(), timeframe.clone(), 20)),
            Box::new(SwingNode::new("swing", symbol.clone(), timeframe.clone(), k, 5)),
            Box::new(StrategyNode::new("strat", k)),
            Box::new(GeometryNode::new("geom", 10000.0, 0.01)),
            Box::new(DummyBroker::new("broker", symbol.clone(), timeframe.clone())),
        ];
        // CandleSource not needed: replaying CandleClosed directly
        let mut graph = Graph::new(nodes);
        let ingress_tagged: Vec<(String, Event)> = ingress_events.clone().into_iter().map(|e| ("ingress".to_string(), e)).collect();
        let out = graph.run(ingress_tagged);
        let trades = out.iter().filter(|e| matches!(e, Event::TradeResult { .. })).count();
        let _ = trades;
    }
}
