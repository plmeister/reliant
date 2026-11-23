use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{AtrNode, CandleSource, DummyBroker, EmaNode, GeometryNode, Graph, Node, StrategyNode, SwingNode};
use reliant_engine::{InMemoryMessageBus, MessageBus};
use reliant_store::{CandleProvider, ParquetDirProvider};

#[test]
fn dummybroker_publishes_traderesult_on_trade_topic() {
    let symbol = Symbol("AUDCAD".into());
    let timeframe = Timeframe::D1;
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let mut candles = provider.candles(&symbol, &timeframe).unwrap();
    candles.truncate(80);

    let mut bus = InMemoryMessageBus::new();
    let rx_trade = bus.subscribe("trade");

    let nodes: Vec<Box<dyn Node>> = vec![
        Box::new(CandleSource::new("cs", symbol.clone(), timeframe.clone(), candles)),
        Box::new(AtrNode::new("atr", symbol.clone(), timeframe.clone(), 14)),
        Box::new(EmaNode::new("ema", symbol.clone(), timeframe.clone(), 20)),
        Box::new(SwingNode::new("swing", symbol.clone(), timeframe.clone(), 3.5, 5)),
        Box::new(StrategyNode::new("strat", 3.5)),
        Box::new(GeometryNode::new("geom", 10000.0, 0.01)),
        Box::new(DummyBroker::new("broker", symbol.clone(), timeframe.clone())),
    ];
    let mut graph = Graph::new(nodes);
    let emitted = graph.run_with_bus(vec![("control".into(), Event::Start)], &mut bus);
    let _ = graph.run_with_bus(vec![("ingress".into(), Event::Tick { timestamp: 0 })], &mut bus);

    let trade_results = emitted.iter().filter(|e| matches!(e, Event::TradeResult { .. })).count();
    // drain trade topic receiver
    let mut rx_count = 0;
    while let Ok(_) = rx_trade.try_recv() { rx_count += 1; }
    // every TradeResult must have been published on the trade topic
    assert!(rx_count >= trade_results, "trade-topic messages {} < TradeResult {}", rx_count, trade_results);
}
