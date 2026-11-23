use chrono::{DateTime, Duration, Utc};
use reliant_datamodel::{Candle, Event, Symbol, Timeframe};
use reliant_dsl::{build_all_nodes, build_role_nodes, compile, BuildOptions, Channel, Role};
use reliant_engine::{Graph, Node};

fn source() -> &'static str {
    r#"
        // enrichment boxes
        ema   := ema   { period: 20 }
        atr   := atr   { period: 14 }
        swing := swing { k: 3.5, lookback: 5 }

        strategy := strategy {
            k: 3.5,
            ema: ema,
            atr: atr,
            swing: swing,
        }
        sizing := sizing { setups: strategy }
        broker := broker { trades: sizing }
    "#
}

#[test]
fn parses_comments_forward_refs_shorthand_and_trailing_commas() {
    let program = reliant_dsl::parse(
        r#"
        // a comment
        strategy := strategy { ema: fast, atr, swing }
        fast := ema { period: 50, }
    "#,
    )
    .unwrap();
    assert_eq!(program.definitions.len(), 2);
    let strategy = &program.definitions[0];
    assert_eq!(strategy.name, "strategy");
    assert_eq!(strategy.kind, "strategy");
    // `atr` and `swing` are shorthand refs, `ema` is an explicit ref.
    assert_eq!(strategy.fields.len(), 3);
}

#[test]
fn compiles_to_nodes_edges_and_role_topics() {
    let compiled = compile(source(), "mr").unwrap();

    let names: Vec<&str> = compiled.nodes.iter().map(|n| n.name.as_str()).collect();
    let position = |name: &str| names.iter().position(|n| *n == name).unwrap();
    assert!(position("strategy") > position("ema"));
    assert!(position("strategy") > position("atr"));
    assert!(position("strategy") > position("swing"));
    assert!(position("sizing") > position("strategy"));
    assert!(position("broker") > position("sizing"));

    // The strategy's declared slots resolve to the named producers.
    let strategy = compiled.node("strategy").unwrap();
    assert_eq!(strategy.inputs["ema"], "ema");
    assert_eq!(strategy.inputs["atr"], "atr");
    assert_eq!(strategy.inputs["swing"], "swing");

    // Enricher -> strategy crosses on enrichment; strategy -> broker on exec.
    assert!(compiled.edges.iter().any(|e| e.from == "ema"
        && e.to == "strategy"
        && e.channel == Some(Channel::Enrichment)));
    assert!(compiled
        .edges
        .iter()
        .any(|e| e.from == "strategy" && e.to == "sizing" && e.channel == Some(Channel::Exec)));
    // sizing -> broker is same-role: no bus subject.
    assert!(compiled
        .edges
        .iter()
        .any(|e| e.from == "sizing" && e.to == "broker" && e.channel.is_none()));

    assert_eq!(compiled.subscribe_topics(Role::Enricher), vec!["pf.mr.feed"]);
    assert_eq!(compiled.publish_topics(Role::Enricher), vec!["pf.mr.enrichment"]);
    assert_eq!(compiled.subscribe_topics(Role::Strategy), vec!["pf.mr.enrichment"]);
    assert_eq!(compiled.publish_topics(Role::Strategy), vec!["pf.mr.exec"]);
    assert_eq!(compiled.subscribe_topics(Role::Broker), vec!["pf.mr.exec"]);
    assert!(compiled.publish_topics(Role::Broker).is_empty());
}

#[test]
fn unknown_kind_is_positioned() {
    let err = compile("nope := wat {}", "mr").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown box kind"), "{msg}");
    assert!(msg.contains("known:"), "{msg}");
}

#[test]
fn unknown_field_lists_valid_names() {
    let err = compile("ema := ema { priod: 20 }", "mr").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown field \"priod\""), "{msg}");
    assert!(msg.contains("period"), "{msg}");
}

#[test]
fn missing_input_is_reported() {
    let err = compile("strategy := strategy { k: 1.0 }", "mr").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("missing input"), "{msg}");
}

#[test]
fn producer_type_is_checked() {
    // A swing producer cannot fill the strategy's EMA slot.
    let err = compile(
        "swing := swing {}\nstrategy := strategy { ema: swing, atr: swing, swing: swing }",
        "mr",
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("expects producer type indicator"), "{msg}");
}

#[test]
fn literal_on_input_is_rejected() {
    let err = compile("strategy := strategy { ema: 20, atr: 14, swing: 1 }", "mr").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("expects a box reference"), "{msg}");
}

#[test]
fn rename_wiring_flows_into_the_runtime() {
    // Producers are renamed; the strategy must read those exact keys.
    let compiled = compile(
        r#"
        fast_ema := ema { period: 5 }
        vol      := atr { period: 5 }
        pivots   := swing { k: 0.0, lookback: 2 }
        strategy := strategy { k: 0.0, ema: fast_ema, atr: vol, swing: pivots }
        sizing   := sizing { setups: strategy }
        broker   := broker { trades: sizing }
    "#,
        "rename",
    )
    .unwrap();

    let symbol = Symbol("TEST".into());
    let timeframe = Timeframe::D1;
    let nodes = build_all_nodes(&compiled, &symbol, &timeframe, &BuildOptions::default()).unwrap();
    assert_eq!(nodes.len(), 6);
    assert_eq!(nodes.iter().filter(|n| n.id() == "strategy").count(), 1);

    let mut graph = Graph::with_context("rename", nodes);
    let mut emitted: Vec<Event> = Vec::new();
    for candle in synthetic_candles(500) {
        let event = Event::CandleClosed {
            symbol: symbol.clone(),
            timeframe: timeframe.clone(),
            candle,
        };
        emitted.extend(graph.run(vec![("ingress".into(), event)]));
    }

    let ema_updates = emitted
        .iter()
        .filter(|e| matches!(e, Event::IndicatorUpdated { name, .. } if name == "fast_ema"))
        .count();
    let atr_updates = emitted
        .iter()
        .filter(|e| matches!(e, Event::IndicatorUpdated { name, .. } if name == "vol"))
        .count();
    let pivots = emitted
        .iter()
        .filter(|e| matches!(e, Event::SwingPivotEvent { name, .. } if name == "pivots"))
        .count();
    let setups = emitted.iter().filter(|e| matches!(e, Event::SetupSignal { .. })).count();
    let results = emitted.iter().filter(|e| matches!(e, Event::TradeResult { .. })).count();

    assert!(ema_updates > 0, "expected fast_ema updates");
    assert!(atr_updates > 0, "expected vol updates");
    assert!(pivots > 0, "expected named swing pivots");
    assert!(setups > 0, "expected setups through renamed wiring");
    let _ = results;
}

#[test]
fn role_build_only_returns_that_components_boxes() {
    let compiled = compile(source(), "mr").unwrap();
    let symbol = Symbol("TEST".into());
    let timeframe = Timeframe::D1;
    let strategy = build_role_nodes(
        &compiled,
        Role::Strategy,
        &symbol,
        &timeframe,
        &BuildOptions::default(),
    )
    .unwrap();
    assert_eq!(strategy.len(), 1);
    assert_eq!(strategy[0].id(), "strategy");

    let broker = build_role_nodes(
        &compiled,
        Role::Broker,
        &symbol,
        &timeframe,
        &BuildOptions::default(),
    )
    .unwrap();
    let ids: Vec<&str> = broker.iter().map(|n| n.id()).collect();
    assert_eq!(ids, vec!["sizing", "broker"]);
}

fn synthetic_candles(n: usize) -> Vec<Candle> {
    let start = DateTime::<Utc>::from_timestamp(1_600_000_000, 0).unwrap();
    let mut out = Vec::with_capacity(n);
    let mut previous = 1.0_f64;
    for i in 0..n {
        let close = 1.0 + (i as f64 / 8.0).sin() * 0.02 + i as f64 * 0.0005;
        let open = previous;
        let high = open.max(close) + 0.002;
        let low = open.min(close) - 0.002;
        out.push(Candle {
            timestamp: start + Duration::days(i as i64),
            open,
            high,
            low,
            close,
            volume: 100.0,
        });
        previous = close;
    }
    out
}
