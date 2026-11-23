use clap::Parser;
use reliant_cli::{parse_timeframe, price_scale};
use reliant_datamodel::{Event, Symbol};
use reliant_dsl::Role;
use reliant_engine::{AtrNode, EmaNode, Graph, Node, SwingNode};
use reliant_messaging::{InMemoryMessageBus, MessageBus, NatsMessageBus, subject, write_audit, event_to_audit};
use reliant_store::SqliteStateStore;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "reliant-enricher", about = "Enrich candles into indicator/swing events")]
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
    #[arg(long)]
    candle_db: Option<String>,
    #[arg(long, alias = "audit-in", alias = "ingress-file")]
    input: Option<String>,
    #[arg(long, alias = "audit-out")]
    output: Option<String>,
    /// Daemon-mode audit file: enrichment events are recorded here for offline
    /// inspection by the audit viewer (overwritten/flushed periodically).
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
    #[arg(long)]
    replay: bool,
    #[arg(long, default_value = "feed")]
    replay_to: String,
    #[arg(long, default_value = "0")]
    replay_from: i64,
    /// Strategy DSL file. When set, the boxes this component owns are built
    /// from the DSL and the subjects come from its role plan, instead of the
    /// built-in atr/ema/swing trio.
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
        cli.candle_db = Some(format!("{}/candles.db", rd));
    }
    let state_db = cli.state_db.clone().unwrap_or_else(|| reliant_cli::state_db().to_string_lossy().to_string());
    let candle_db = cli.candle_db.clone().unwrap_or_else(|| reliant_cli::candles_db().to_string_lossy().to_string());
    let portfolios_root = if cli.input.is_some() { String::new() } else { cli.portfolios_root.clone().unwrap_or_else(|| reliant_cli::portfolios_root().to_string_lossy().to_string()) };

    if !portfolios_root.is_empty() {
        if !cli.replay {
            let shutdown = reliant_cli::shutdown_flag();
            let bus_url = cli
                .bus
                .clone()
                .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
            match reliant_cli::supervisor::run(
                Role::Enricher,
                &portfolios_root,
                cli.workers,
                &bus_url,
                shutdown,
            ) {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("enricher: {}", e);
                    std::process::exit(1);
                }
            }
            return;
        }
    }

    let tf = parse_timeframe(&cli.timeframe);
    let tf_str = cli.timeframe.clone();
    let sym = Symbol(cli.instrument.clone());

    // Replay mode: publisher-only. Publish stored candles to a target subject;
    // the owning daemons consume and advance state. Never processes locally, so
    // it cannot double-apply bars against a running daemon.
    if cli.replay {
        let bus_url = cli.bus.clone().unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
        let mut bus: Box<dyn MessageBus> = Box::new(NatsMessageBus::new(&bus_url));
        let candle_store = match reliant_store::SqliteCandleStore::new(&candle_db) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("enricher: candle store {}: {}", candle_db, e);
                std::process::exit(1);
            }
        };
        let candles = reliant_store::CandleStore::range(&candle_store, &sym, &tf, None, None)
            .unwrap_or_default();
        let target = subject(&cli.context, &cli.replay_to);
        let mut n = 0usize;
        for c in candles {
            if c.timestamp.timestamp() <= cli.replay_from {
                continue;
            }
            bus.publish(
                &target,
                Event::CandleClosed {
                    symbol: sym.clone(),
                    timeframe: tf.clone(),
                    candle: c.clone(),
                },
            );
            n += 1;
        }
        println!("enricher: replayed {} bars to {}", n, cli.replay_to);
        return;
    }

    let compiled = match &cli.dsl {
        Some(path) => {
            if cli.context.is_empty() || cli.context == "local" {
                eprintln!("enricher: --dsl requires --context <portfolio id>");
                std::process::exit(1);
            }
            match reliant_cli::load_strategy(path, &cli.context) {
                Ok(c) => Some(c),
                Err(e) => {
                    eprintln!("enricher: {}", e);
                    std::process::exit(1);
                }
            }
        }
        None => None,
    };

    let nodes: Vec<Box<dyn Node>> = match &compiled {
        Some(c) => reliant_cli::build_component_nodes(
            c,
            Role::Enricher,
            &cli.instrument,
            &tf,
            10_000.0,
            0.01,
        )
        .unwrap_or_else(|e| {
            eprintln!("enricher: {}", e);
            std::process::exit(1);
        }),
        None => vec![
            Box::new(AtrNode::new(&format!("atr_{}", cli.instrument), sym.clone(), tf.clone(), 14)),
            Box::new(EmaNode::new(&format!("ema_{}", cli.instrument), sym.clone(), tf.clone(), 20)),
            Box::new(SwingNode::new(&format!("swing_{}", cli.instrument), sym.clone(), tf.clone(), cli.k, 5)),
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
        let mut checkpoint = reliant_cli::StateCheckpoint::new("enricher", &cli.context, &sym, &tf);
        reliant_cli::restore_context_state(&mut store, checkpoint.key(), &mut graph, &sym, &tf);
        let topic = match &compiled {
            Some(c) => c
                .subscribe_topics(Role::Enricher)
                .into_iter()
                .next()
                .unwrap_or_else(|| subject(&cli.context, "feed")),
            None if cli.context.is_empty() || cli.context == "local" => "ingress".to_string(),
            None => subject(&cli.context, "feed"),
        };
        let enrich_topic = match &compiled {
            Some(c) => c
                .publish_topics(Role::Enricher)
                .into_iter()
                .next()
                .unwrap_or_else(|| subject(&cli.context, "enrichment")),
            None => subject(&cli.context, "enrichment"),
        };
        let warmup_topic = subject(&cli.context, "warmup");
        let rx = match &cli.queue_group {
            Some(g) => bus.queue_subscribe(&topic, g),
            None => bus.subscribe(&topic),
        };
        let rx_warmup = match &cli.queue_group {
            Some(g) => bus.queue_subscribe(&warmup_topic, g),
            None => bus.subscribe(&warmup_topic),
        };
        println!("enricher daemon: listening on {} (+ warmup {})", topic, warmup_topic);
        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
            let mut did = false;
            // Warm-up bars: advance context, never publish to strategy.
            while let Ok(ev) = rx_warmup.try_recv() {
                let stage = match &ev {
                    Event::CandleClosed { .. } => "ingress",
                    _ => continue,
                };
                if let Event::CandleClosed { candle, .. } = &ev {
                    let ts = candle.timestamp.timestamp();
                    let skip = graph
                        .context()
                        .contexts
                        .get(&(sym.clone(), tf.clone()))
                        .and_then(|c| c.latest)
                        .map_or(false, |l| ts <= l);
                    if skip {
                        continue;
                    }
                }
                let _ = graph.process(stage, &ev, &mut *bus);
                checkpoint.maybe_publish(&mut *bus, &mut graph);
                did = true;
            }
            // Live bars: candle + facts to the strategy.
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(ev) => {
                    let stage = match &ev {
                        Event::CandleClosed { .. } => "ingress",
                        _ => continue,
                    };
                    if let Event::CandleClosed { candle, .. } = &ev {
                        let ts = candle.timestamp.timestamp();
                        let skip = graph
                            .context()
                            .contexts
                            .get(&(sym.clone(), tf.clone()))
                            .and_then(|c| c.latest)
                            .map_or(false, |l| ts <= l);
                        if skip {
                            continue;
                        }
                    }
                    bus.publish(&enrich_topic, ev.clone());
                    let derived = graph.process(stage, &ev, &mut *bus);
                    checkpoint.maybe_publish(&mut *bus, &mut graph);
                    for (_, e) in &derived { audit_buf.push(e.clone()); }
                    if let Some(path) = &cli.audit_file {
                        if audit_buf.len() % 50 == 0 && !audit_buf.is_empty() {
                            let msgs: Vec<_> = audit_buf.iter().map(|e| event_to_audit(e, "enrichment", 0)).collect();
                            let _ = write_audit(path, &msgs);
                        }
                    }
                    println!("enricher: processed event, derived {}", derived.len());
                    did = true;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            if !did {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        checkpoint.publish(&mut *bus, &mut graph);
        if let Some(path) = &cli.audit_file {
            if !audit_buf.is_empty() {
                let msgs: Vec<_> = audit_buf.iter().map(|e| event_to_audit(e, "enrichment", 0)).collect();
                let _ = write_audit(path, &msgs);
                println!("enricher: wrote {} enrichment events -> {}", msgs.len(), path);
            }
        }
        println!("enricher: shutting down");
        return;
    }

    // batch mode — supports comma-separated --instrument (multi-symbol)
    let syms: Vec<&str> = cli.instrument.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    let syms = if syms.is_empty() { vec![cli.instrument.as_str()] } else { syms };

    let mut graphs: std::collections::HashMap<String, Graph> = std::collections::HashMap::new();
    for s in &syms {
        let symbol = Symbol(s.to_string());
        let n: Vec<Box<dyn Node>> = if let Some(c) = &compiled {
            reliant_cli::build_component_nodes(c, Role::Enricher, s, &tf, 10_000.0, 0.01)
                .unwrap_or_default()
        } else {
            vec![
                Box::new(AtrNode::new(&format!("atr_{}", s), symbol.clone(), tf.clone(), 14)),
                Box::new(EmaNode::new(&format!("ema_{}", s), symbol.clone(), tf.clone(), 20)),
                Box::new(SwingNode::new(&format!("swing_{}", s), symbol.clone(), tf.clone(), cli.k, 5)),
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
        None => { eprintln!("enricher: --input <audit file> required in batch mode (or use --daemon)"); return; }
    };
    if let Ok(msgs) = reliant_messaging::read_audit(&inp) {
        for m in msgs {
            if let Ok(ev) = serde_json::from_value::<Event>(m.payload) {
                let stage = match &ev {
                    Event::CandleClosed { .. } => "ingress",
                    _ => continue,
                };
                let sym_name = match &ev {
                    Event::CandleClosed { symbol, .. } => symbol.0.clone(),
                    _ => continue,
                };
                if let Some(g) = graphs.get_mut(&sym_name) {
                    // Also carry the original candle so downstream (strategy in
                    // bar-trigger mode, viewer) has per-bar context.
                    outputs.push(ev.clone());
                    let derived = g.process(stage, &ev, &mut bus);
                    for (_, e) in derived { outputs.push(e); }
                }
            }
        }
    }

    if let Some(out) = cli.output {
        let msgs: Vec<_> = outputs.iter().map(|e| event_to_audit(e, "enrichment", 0)).collect();
        let _ = write_audit(&out, &msgs);
        println!("enricher: wrote {} enrichment events -> {}", msgs.len(), out);
    } else {
        println!("enricher: processed {} outputs", outputs.len());
    }
    let _ = price_scale(&cli.instrument);
}
