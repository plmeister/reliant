use clap::Parser;
use reliant_cli::{parse_timeframe, collect_trade_results};
use reliant_datamodel::{Event, Symbol};
use reliant_dsl::Role;
use reliant_engine::{AtrNode, EmaNode, Graph, Node, StrategyNode, SwingNode};
use reliant_messaging::{InMemoryMessageBus, MessageBus, NatsMessageBus, subject, write_audit, event_to_audit};
use reliant_store::SqliteStateStore;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "reliant-strategy", about = "Run strategy processor over candles")]
struct Cli {
    #[arg(long, default_value = "local")]
    context: String,
    #[arg(long, default_value = "AUDCAD")]
    instrument: String,
    #[arg(long, default_value = "1d")]
    timeframe: String,
    #[arg(long, default_value = "3.5")]
    k: f64,
    #[arg(long)]
    state_db: Option<String>,
    #[arg(long, alias = "audit-in", alias = "ingress-file")]
    input: Option<String>,
    #[arg(long, alias = "audit-out")]
    output: Option<String>,
    #[arg(long)]
    audit_file: Option<String>,
    #[arg(long)]
    daemon: bool,
    #[arg(long)]
    bus: Option<String>,
    #[arg(long)]
    queue_group: Option<String>,
    #[arg(long)]
    run_dir: Option<String>,
    /// Strategy DSL file. When set, the boxes this component owns are built
    /// from the DSL and the subjects come from its role plan.
    #[arg(long)]
    dsl: Option<String>,
    /// Serve every portfolio under this directory with one bounded worker pool
    /// instead of a single context.
    #[arg(long)]
    portfolios_root: Option<String>,
    /// Pool size for --portfolios-root (0 = auto by role).
    #[arg(long, default_value = "0")]
    workers: usize,
}

fn main() {
    let mut cli = Cli::parse();
    if let Some(rd) = &cli.run_dir {
        let _ = std::fs::create_dir_all(rd);
        cli.state_db = Some(format!("{}/state.sqlite", rd));
    }
    let state_db = cli.state_db.clone().unwrap_or_else(|| reliant_cli::state_db().to_string_lossy().to_string());
    let portfolios_root = if cli.input.is_some() { String::new() } else { cli.portfolios_root.clone().unwrap_or_else(|| reliant_cli::portfolios_root().to_string_lossy().to_string()) };
    if !portfolios_root.is_empty() {
        let shutdown = reliant_cli::shutdown_flag();
        let bus_url = cli
            .bus
            .clone()
            .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
        match reliant_cli::supervisor::run(
            Role::Strategy,
            &portfolios_root,
            cli.workers,
            &bus_url,
            shutdown,
        ) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("strategy: {}", e);
                std::process::exit(1);
            }
        }
        return;
    }
    let tf = parse_timeframe(&cli.timeframe);
    let sym = Symbol(cli.instrument.clone());

    let compiled = match &cli.dsl {
        Some(path) => match reliant_cli::load_strategy(path, &cli.context) {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("strategy: {}", e);
                std::process::exit(1);
            }
        },
        None => None,
    };

    let nodes: Vec<Box<dyn Node>> = match &compiled {
        Some(c) => reliant_cli::build_component_nodes(
            c,
            Role::Strategy,
            &cli.instrument,
            &tf,
            10_000.0,
            0.01,
        )
        .unwrap_or_else(|e| {
            eprintln!("strategy: {}", e);
            std::process::exit(1);
        }),
        None => vec![
            Box::new(AtrNode::new(&format!("atr_{}", cli.instrument), sym.clone(), tf.clone(), 14)),
            Box::new(EmaNode::new(&format!("ema_{}", cli.instrument), sym.clone(), tf.clone(), 20)),
            Box::new(SwingNode::new(&format!("swing_{}", cli.instrument), sym.clone(), tf.clone(), cli.k, 5)),
            Box::new(StrategyNode::new("strat", cli.k)),
        ],
    };

    if cli.daemon {
        let mut audit_buf: Vec<Event> = Vec::new();
        let shutdown = reliant_cli::shutdown_flag();
        let mut bus: Box<dyn MessageBus> = match &cli.bus {
            Some(url) => Box::new(NatsMessageBus::new(url)),
            None => Box::new(InMemoryMessageBus::new()),
        };
        let mut graph = Graph::with_context(&cli.context, nodes);
        let mut store = SqliteStateStore::new(&state_db).unwrap();
        let mut checkpoint = reliant_cli::StateCheckpoint::new("strategy", &cli.context, &sym, &tf);
        graph.context().get_or_create(sym.clone(), tf.clone());
        reliant_cli::restore_context_state(&mut store, checkpoint.key(), &mut graph, &sym, &tf);
        let topic = match &compiled {
            Some(c) => c
                .subscribe_topics(Role::Strategy)
                .into_iter()
                .next()
                .unwrap_or_else(|| subject(&cli.context, "enrichment")),
            None => subject(&cli.context, "enrichment"),
        };
        // The strategy is the last analysis stage for a bar, so it republishes
        // the closed candle onto the context stream before the setups for that
        // bar. Which subject that is comes from the DSL role plan when a
        // strategy is bound (`pf.{id}.exec`), otherwise the legacy ingress.
        let ctx_stream = match &compiled {
            Some(c) => c
                .publish_topics(Role::Strategy)
                .into_iter()
                .next()
                .unwrap_or_else(|| subject(&cli.context, "ingress")),
            None => subject(&cli.context, "ingress"),
        };
        if cli.context.is_empty() {
            eprintln!("strategy: --context must be non-empty in daemon mode (the context stream is where the broker subscribes)");
            return;
        }
        let rx = match &cli.queue_group {
            Some(g) => bus.queue_subscribe(&topic, g),
            None => bus.subscribe(&topic),
        };
        println!(
            "strategy daemon: listening on {} (publishing bars on {})",
            topic, ctx_stream
        );
        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(ev) => {
                    // `{ctx}.enrichment` carries every instrument in the portfolio.
                    // This daemon owns one instrument: drop the rest so its context
                    // and its context stream stay single-instrument (the trade
                    // engine subscribes to `{ctx}.ingress`).
                    let ev_symbol = match &ev {
                        Event::CandleClosed { symbol, .. }
                        | Event::IndicatorUpdated { symbol, .. }
                        | Event::SwingPivotEvent { symbol, .. } => Some(symbol),
                        _ => None,
                    };
                    if let Some(s) = ev_symbol {
                        if s != &sym {
                            continue;
                        }
                    }
                    // The strategy is the last analysis stage for a bar, so it republishes
                    // the closed candle as the first event of the bar on the context
                    // stream. Its setups follow on that same subject, which is what makes
                    // "resolve candle N, then apply bar N's setups" an ordering guarantee
                    // of the transport rather than an assumption about arrival timing.
                    if let Event::CandleClosed { .. } = &ev {
                        bus.publish(&ctx_stream, ev.clone());
                    }
                    let derived = graph.compute("enrichment", &ev);
                    checkpoint.maybe_publish(&mut *bus, &mut graph);
                    let mut setups = 0usize;
                    for (stage, out) in derived {
                        if let Event::SetupSignal { .. } = out {
                            setups += 1;
                            audit_buf.push(out.clone());
                        }
                        let target = if matches!(out, Event::SetupSignal { .. }) {
                            ctx_stream.clone()
                        } else {
                            subject(&cli.context, &stage)
                        };
                        bus.publish(&target, out);
                    }
                    if let Some(path) = &cli.audit_file {
                        if audit_buf.len() % 10 == 0 && !audit_buf.is_empty() {
                            let msgs: Vec<_> = audit_buf.iter().map(|e| event_to_audit(e, "signal", 0)).collect();
                            let _ = write_audit(path, &msgs);
                        }
                    }
                    if setups > 0 { println!("strategy: {} setups", setups); }
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        checkpoint.publish(&mut *bus, &mut graph);
        if let Some(path) = &cli.audit_file {
            if !audit_buf.is_empty() {
                let msgs: Vec<_> = audit_buf.iter().map(|e| event_to_audit(e, "signal", 0)).collect();
                let _ = write_audit(path, &msgs);
                println!("strategy: wrote {} signal events -> {}", msgs.len(), path);
            }
        }
        println!("strategy: shutting down");
        return;
    }

    // batch mode — supports comma-separated --instrument (multi-symbol)
    let syms: Vec<&str> = cli.instrument.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    let syms = if syms.is_empty() { vec![cli.instrument.as_str()] } else { syms };

    let mut graphs: std::collections::HashMap<String, Graph> = std::collections::HashMap::new();
    for s in &syms {
        let symbol = Symbol(s.to_string());
        let n: Vec<Box<dyn Node>> = if let Some(c) = &compiled {
            reliant_cli::build_component_nodes(c, Role::Strategy, s, &tf, 10_000.0, 0.01)
                .unwrap_or_default()
        } else {
            vec![
                Box::new(AtrNode::new(&format!("atr_{}", s), symbol.clone(), tf.clone(), 14)),
                Box::new(EmaNode::new(&format!("ema_{}", s), symbol.clone(), tf.clone(), 20)),
                Box::new(SwingNode::new(&format!("swing_{}", s), symbol.clone(), tf.clone(), cli.k, 5)),
                Box::new(StrategyNode::new("strat", cli.k)),
            ]
        };
        if !n.is_empty() {
            graphs.insert(s.to_string(), Graph::with_context(&cli.context, n));
        }
    }

    let mut bus = InMemoryMessageBus::new();
    let mut outputs: Vec<Event> = Vec::new();
    let inp = match &cli.input {
        Some(p) => p.clone(),
        None => { eprintln!("strategy: --input <audit file> required in batch mode (or use --daemon)"); return; }
    };
    match reliant_messaging::read_audit(&inp) {
        Ok(msgs) => {
            // read audit ok
            for m in msgs {
            if let Ok(ev) = serde_json::from_value::<Event>(m.payload) {
                let stage = match &ev {
                    Event::CandleClosed { .. } => "ingress",
                    Event::SwingPivotEvent { .. } | Event::IndicatorUpdated { .. } => "enrichment",
                    _ => continue,
                };
                let sym_name = match &ev {
                    Event::CandleClosed { symbol, .. } => symbol.0.clone(),
                    Event::SwingPivotEvent { symbol, .. } => symbol.0.clone(),
                    Event::IndicatorUpdated { symbol, .. } => symbol.0.clone(),
                    _ => continue,
                };
                if let Some(g) = graphs.get_mut(&sym_name) {
                    let derived = g.process(stage, &ev, &mut bus);
                    if stage == "ingress" && !derived.is_empty() {
                    }
                    for (_, e) in derived { outputs.push(e); }
                } else {
                }
            }
            }
        }
        Err(e) => eprintln!("strategy: read_audit failed: {}", e),
    }

    let setups: Vec<Event> = outputs.iter().filter(|e| matches!(e, Event::SetupSignal { .. })).cloned().collect();
    if let Some(out) = cli.output {
        let msgs: Vec<_> = setups.iter().map(|e| event_to_audit(e, "signal", 0)).collect();
        let _ = write_audit(&out, &msgs);
        println!("strategy: wrote {} setups -> {}", msgs.len(), out);
    } else {
        let results = collect_trade_results(&outputs);
        println!("strategy: processed {} outputs, setups={}, trade_results={}", outputs.len(), setups.len(), results.len());
    }
}
