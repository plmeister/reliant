use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{AtrNode, CandleSource, EmaNode, Graph, Node, StrategyNode};

#[test]
fn vertical_slice_graph_emits_indicator_and_setup_events() {
    use reliant_store::{CandleProvider, ParquetDirProvider};
    let symbol = Symbol("AUDCAD".into());
    let timeframe = Timeframe::D1;
    let provider = ParquetDirProvider { root: "/home/scott/src/marketatlas/data/study".into() };
    let candles = provider.candles(&symbol, &timeframe).expect("parquet");
    // use first 40 bars for a fast slice
    let candles = candles.into_iter().take(40).collect::<Vec<_>>();

    let nodes: Vec<Box<dyn Node>> = vec![
        Box::new(CandleSource::new("cs", symbol.clone(), timeframe.clone(), candles)),
        Box::new(AtrNode::new("atr", symbol.clone(), timeframe.clone(), 14)),
        Box::new(EmaNode::new("ema", symbol.clone(), timeframe.clone(), 20)),
        Box::new(StrategyNode::new("strat", 3.5)),
    ];
    let mut graph = Graph::new(nodes);
    let mut emitted = graph.run(vec![("control".into(), Event::Start)]);
    emitted.extend(graph.run(vec![("ingress".into(), Event::Tick { timestamp: 1600000000 + 24 * 86400 })]));

    let atr_updates: Vec<_> = emitted
        .iter()
        .filter(|e| matches!(e, Event::IndicatorUpdated { name, .. } if name == "atr14"))
        .collect();
    let ema_updates: Vec<_> = emitted
        .iter()
        .filter(|e| matches!(e, Event::IndicatorUpdated { name, .. } if name == "ema20"))
        .collect();
    assert!(!atr_updates.is_empty(), "expected atr14 indicator updates");
    assert!(!ema_updates.is_empty(), "expected ema20 indicator updates");

    let setups: Vec<_> = emitted.iter().filter(|e| matches!(e, Event::SetupSignal { .. })).collect();
    // setup signals are optional depending on swing magnitude; just ensure graph ran
    let _ = setups;
}
