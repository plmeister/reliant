use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{AtrNode, CandleSource, DummyBroker, EmaNode, GeometryNode, Graph, Node, StrategyNode, SwingNode};
use reliant_store::{CandleProvider, ParquetDirProvider};

#[test]
fn full_pipeline_runs_without_panic() {
    let symbol = Symbol("AUDCAD".into());
    let timeframe = Timeframe::D1;
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let mut candles = provider.candles(&symbol, &timeframe).unwrap();
    candles.truncate(60);

    let nodes: Vec<Box<dyn Node>> = vec![
        Box::new(CandleSource::new("cs", symbol.clone(), timeframe.clone(), candles.clone())),
        Box::new(AtrNode::new("atr", symbol.clone(), timeframe.clone(), 14)),
        Box::new(EmaNode::new("ema", symbol.clone(), timeframe.clone(), 20)),
        Box::new(SwingNode::new("swing", symbol.clone(), timeframe.clone(), 3.5, 5)),
        Box::new(StrategyNode::new("strat", 3.5)),
        Box::new(GeometryNode::new("geom", 10000.0, 0.01)),
        Box::new(DummyBroker::new("broker", symbol.clone(), timeframe.clone())),
    ];
    let mut graph = Graph::new(nodes);
    let emitted = graph.run(vec![("control".into(), Event::Start)]);
    let _ = graph.run(vec![("ingress".into(), Event::Tick { timestamp: 0 })]);

    // should have produced indicator updates and (possibly) trade results
    let _ = emitted;
}
