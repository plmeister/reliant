use clap::Parser;
use reliant_cli::{parse_timeframe, collect_trade_results};
use reliant_datamodel::{Event, Symbol};
use reliant_dsl::Role;
use reliant_engine::{DummyBroker, GeometryNode, Graph, Node};
use reliant_messaging::{InMemoryMessageBus, MessageBus, NatsMessageBus, subject, read_audit, write_audit, event_to_audit};
use reliant_store::SqliteStateStore;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "reliant-trade", about = "Trade engine: size setups and resolve trades")]
struct Cli {
    #[arg(long, default_value = "local")]
    context: String,
    #[arg(long, default_value = "AUDCAD")]
    instrument: String,
    #[arg(long, default_value = "1d")]
    timeframe: String,
    #[arg(long, default_value = "10000")]
    balance: f64,
    #[arg(long, default_value = "0.01")]
    risk: f64,
    #[arg(long)]
    state_db: Option<String>,
    #[arg(long, alias = "input", alias = "setups-file")]
    setups_file: Option<String>,
    #[arg(long, alias = "candles-file")]
    ingress_file: Option<String>,
    #[arg(long, alias = "stream")]
    combined: Option<String>,
    #[arg(long, alias = "audit-out")]
    output: Option<String>,
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
    let portfolios_root = if cli.ingress_file.is_some() || cli.setups_file.is_some() || cli.combined.is_some() { String::new() } else { cli.portfolios_root.clone().unwrap_or_else(|| reliant_cli::portfolios_root().to_string_lossy().to_string()) };
    if !portfolios_root.is_empty() {
        let shutdown = reliant_cli::shutdown_flag();
        let bus_url = cli
            .bus
            .clone()
            .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
        match reliant_cli::supervisor::run(Role::Broker, &portfolios_root, cli.workers, &bus_url, shutdown)
        {
            Ok(()) => {}
            Err(e) => {
                eprintln!("trade: {}", e);
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
                eprintln!("trade: {}", e);
                std::process::exit(1);
            }
        },
        None => None,
    };

    let nodes: Vec<Box<dyn Node>> = match &compiled {
        Some(c) => reliant_cli::build_component_nodes(
            c,
            Role::Broker,
            &cli.instrument,
            &tf,
            cli.balance,
            cli.risk,
        )
        .unwrap_or_else(|e| {
            eprintln!("trade: {}", e);
            std::process::exit(1);
        }),
        None => vec![
            Box::new(GeometryNode::new("geom", cli.balance, cli.risk)),
            Box::new(DummyBroker::new(&format!("broker_{}", cli.instrument), sym.clone(), tf.clone())),
        ],
    };

    if cli.daemon {
        let shutdown = reliant_cli::shutdown_flag();
        let mut bus: Box<dyn MessageBus> = match &cli.bus {
            Some(url) => Box::new(NatsMessageBus::new(url)),
            None => Box::new(InMemoryMessageBus::new()),
        };
        let mut graph = Graph::with_context(&cli.context, nodes);
        let mut store = SqliteStateStore::new(&state_db).unwrap();
        let mut checkpoint = reliant_cli::StateCheckpoint::new("broker", &cli.context, &sym, &tf);
        graph.context().get_or_create(sym.clone(), tf.clone());
        reliant_cli::restore_context_state(&mut store, checkpoint.key(), &mut graph, &sym, &tf);
        // One subscription only. The strategy publishes each closed candle onto the
        // context stream first, then the setups derived from that bar, so the trade
        // engine resolves bar N before applying bar N's setups by construction.
        let topic = match &compiled {
            Some(c) => c
                .subscribe_topics(Role::Broker)
                .into_iter()
                .next()
                .unwrap_or_else(|| subject(&cli.context, "ingress")),
            None => subject(&cli.context, "ingress"),
        };
        if cli.context.is_empty() {
            eprintln!("trade: --context must be non-empty in daemon mode (raw ingress is never consumed directly)");
            return;
        }
        let rx = match &cli.queue_group {
            Some(g) => bus.queue_subscribe(&topic, g),
            None => bus.subscribe(&topic),
        };
        let mut results_emitted: Vec<Event> = Vec::new();
        let mut last_candle_ts: i64 = 0;
        println!("trade daemon: listening on {}", topic);
        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
            let mut did = false;
            while let Ok(ev) = rx.try_recv() {
                let stage = match &ev {
                    Event::CandleClosed { .. } => "ingress",
                    Event::SetupSignal { .. } => "signal",
                    _ => continue,
                };
                let derived = graph.process(stage, &ev, &mut *bus);
                checkpoint.maybe_publish(&mut *bus, &mut graph);
                if let Event::CandleClosed { candle, .. } = &ev {
                    last_candle_ts = candle.timestamp.timestamp();
                }
                for (_, e) in derived {
                    if let Event::TradeResult { pnl_r, reason, .. } = &e {
                        println!(
                            "trade: result r={:.2} {} at bar {}",
                            pnl_r, reason, last_candle_ts
                        );
                        results_emitted.push(e);
                    }
                }
                did = true;
            }
            if !did { std::thread::sleep(Duration::from_millis(50)); }
        }
        let results = collect_trade_results(&results_emitted);
        checkpoint.publish(&mut *bus, &mut graph);
        let wins = results.iter().filter(|(r, _)| *r > 0.0).count();
        let total_r: f64 = results.iter().map(|(r, _)| r).sum();
        println!("trade: shutting down: trades={} wins={} total_r={:.3}", results.len(), wins, total_r);
        if let Some(out) = &cli.output {
            let msgs: Vec<_> = results_emitted.iter()
                .map(|e| event_to_audit(e, "trade", 0))
                .collect();
            let _ = write_audit(out, &msgs);
            println!("trade: wrote {} trade results -> {}", msgs.len(), out);
        }
        return;
    }

    // batch mode (unchanged)
    let mut graph = Graph::with_context(&cli.context, nodes);
    let mut bus = InMemoryMessageBus::new();

    let mut inputs: Vec<(String, Event)> = Vec::new();
    if let Some(stream) = &cli.combined {
        if let Ok(msgs) = read_audit(stream) {
            for m in msgs {
                if let Ok(ev) = serde_json::from_value::<Event>(m.payload) {
                    let stage = match &ev {
                        Event::CandleClosed { .. } => "ingress",
                        Event::SetupSignal { .. } => "signal",
                        _ => continue,
                    };
                    inputs.push((stage.to_string(), ev));
                }
            }
        }
    } else {
        if let Some(setups) = &cli.setups_file {
            if let Ok(msgs) = read_audit(setups) {
                for m in msgs {
                    if let Ok(ev) = serde_json::from_value::<Event>(m.payload) {
                        if let Event::SetupSignal { .. } = ev { inputs.push(("signal".to_string(), ev)); }
                    }
                }
            }
        }
        if let Some(ingress) = &cli.ingress_file {
            if let Ok(msgs) = read_audit(ingress) {
                for m in msgs {
                    if let Ok(ev) = serde_json::from_value::<Event>(m.payload) {
                        if let Event::CandleClosed { .. } = ev { inputs.push(("ingress".to_string(), ev)); }
                    }
                }
            }
        } else {
            eprintln!("trade: --ingress-file <candle audit> required for candle resolution in batch mode (or use --daemon)");
        }
    }

    let mut outputs: Vec<Event> = Vec::new();
    for (stage, ev) in inputs {
        let derived = graph.process(&stage, &ev, &mut bus);
        for (_, e) in derived { outputs.push(e); }
    }
    let results = collect_trade_results(&outputs);
    let n = results.len();
    let wins = results.iter().filter(|(r, _)| *r > 0.0).count();
    let total_r: f64 = results.iter().map(|(r, _)| r).sum();
    println!("trade: trades={} wins={} total_r={:.3}", n, wins, total_r);

    if let Some(out) = cli.output {
        let msgs: Vec<_> = outputs.iter().filter(|e| matches!(e, Event::TradeResult { .. })).map(|e| event_to_audit(e, "trade", 0)).collect();
        let _ = write_audit(&out, &msgs);
        println!("trade: wrote {} trade results -> {}", msgs.len(), out);
    }
}
