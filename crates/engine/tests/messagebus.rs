use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{InMemoryMessageBus, MessageBus, Node, Graph, CandleSource, AtrNode, EmaNode};
use reliant_store::{CandleProvider, ParquetDirProvider};

#[test]
fn bus_receives_ingress_events() {
    let symbol = Symbol("AUDCAD".into());
    let timeframe = Timeframe::D1;
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let mut candles = provider.candles(&symbol, &timeframe).unwrap();
    candles.truncate(5);

    let mut bus = InMemoryMessageBus::new();
    let rx_ingress = bus.subscribe("ingress");

    let nodes: Vec<Box<dyn Node>> = vec![
        Box::new(CandleSource::new("cs", symbol.clone(), timeframe.clone(), candles)),
        Box::new(AtrNode::new("atr", symbol.clone(), timeframe.clone(), 14)),
        Box::new(EmaNode::new("ema", symbol.clone(), timeframe.clone(), 20)),
    ];
    let mut graph = Graph::new(nodes);
    graph.run_with_bus(vec![("control".into(), Event::Start)], &mut bus);

    // ingress receiver should have received CandleClosed events
    let mut count = 0;
    while let Ok(_) = rx_ingress.try_recv() { count += 1; }
    assert!(count >= 5, "expected ingress events, got {}", count);
}
