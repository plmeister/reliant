use clap::Parser;
use reliant_cli::parse_timeframe;
use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_dsl::{build_role_nodes, BuildOptions, Compiled, Role};
use reliant_engine::Graph;
use reliant_messaging::{InMemoryMessageBus, MessageBus, read_audit};
use reliant_portfolio::Portfolio;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::mpsc;
use std::thread;

#[derive(Parser)]
#[command(name = "reliant-backtest", about = "Run component-wired backtests over ingress audit files")]
struct Cli {
    /// Portfolio directory containing portfolio.json and the strategy file.
    #[arg(long)]
    portfolio: Option<String>,
    /// Comma-separated symbol list (alternative to --portfolio).
    #[arg(long)]
    symbols: Option<String>,
    /// Strategy DSL file (required with --symbols).
    #[arg(long)]
    dsl: Option<String>,
    #[arg(long, default_value = "1d")]
    timeframe: String,
    #[arg(long, default_value = "10000")]
    balance: f64,
    #[arg(long, default_value = "0.01")]
    risk: f64,
    /// One or more ingress audit files (comma-separated or glob).
    #[arg(long)]
    ingress: String,
    #[arg(long, default_value = "local")]
    context: String,
    /// Write closed-trade records as JSONL to this path (for the audit viewer).
    #[arg(long)]
    trades_out: Option<String>,
    /// Write enrichment events (CandleClosed + indicators/swings) as an audit .dat file.
    #[arg(long)]
    enrichment_out: Option<String>,
    /// Write signal events (SetupSignal) as an audit .dat file.
    #[arg(long)]
    signals_out: Option<String>,
}

fn expand_paths(pattern: &str) -> Vec<String> {
    if pattern.contains('*') {
        let dir = std::path::Path::new(pattern).parent().unwrap_or(std::path::Path::new("."));
        let fname = std::path::Path::new(pattern).file_name().unwrap().to_string_lossy();
        let prefix = fname.split('*').next().unwrap_or("").to_string();
        let mut out = Vec::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let n = e.file_name().to_string_lossy().to_string();
                if n.ends_with(".dat") || n.ends_with(".dat.zstd") {
                    if prefix.is_empty() || n.starts_with(&prefix) { out.push(e.path().to_string_lossy().to_string()); }
                }
            }
        }
        out.sort();
        out
    } else {
        pattern.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
    }
}

fn load_portfolio(dir: &str) -> Result<(Vec<String>, Timeframe, Vec<String>, String, f64, f64), String> {
    let dir_name = Path::new(dir).file_name().and_then(|s| s.to_str()).unwrap_or("portfolio");
    let p = Portfolio::from_file(Path::new(&format!("{}/portfolio.json", dir)), dir_name)?;
    Ok((p.instruments, p.trade_timeframe, p.context_timeframes.iter().map(|t| t.as_str().to_string()).collect(), p.strategy_file, p.starting_balance, p.risk_pct))
}

/// Per-symbol state: graphs + bus + subscriptions.
struct SymbolState {
    enricher_graph: Graph,
    strategy_graph: Graph,
    trade_graph: Graph,
    bus: InMemoryMessageBus,
    enricher_ingress: mpsc::Receiver<Event>,
    strategy_enrichment: mpsc::Receiver<Event>,
    trade_ingress: mpsc::Receiver<Event>,
    trade_signal: mpsc::Receiver<Event>,
}

fn run_one(
    paths: &[String],
    _ctx: &str,
    compiled: Option<&Compiled>,
    symbols: &[String],
    trade_tf: &Timeframe,
    context_tfs: &[String],
    balance: f64,
    risk: f64,
    trades_out: Option<String>,
    enrichment_out: Option<String>,
    signals_out: Option<String>,
) -> Result<(HashMap<String, (usize, usize, f64)>, BTreeMap<String, (usize, usize, f64)>), String> {
    // Read events from all input files and merge by timestamp.
    let mut all_events: Vec<Event> = Vec::new();
    for p in paths {
        let msgs = read_audit(p).map_err(|e| format!("read {}: {}", p, e))?;
        for m in msgs {
            if let Ok(ev) = serde_json::from_value::<Event>(m.payload) {
                all_events.push(ev);
            }
        }
    }
    // Sort by timestamp for deterministic interleaving.
    all_events.sort_by_key(|e| match e {
        Event::CandleClosed { candle, .. } => candle.timestamp.timestamp(),
        _ => 0,
    });

    // Build per-symbol state. For MTF, build an enricher graph per timeframe.
    struct SymState {
        trade_tf: Timeframe,
        context_tfs: Vec<Timeframe>,
        enricher_graphs: HashMap<Timeframe, Graph>,
        strategy_graph: Graph,
        trade_graph: Graph,
        bus: InMemoryMessageBus,
        enricher_ingress: HashMap<Timeframe, mpsc::Receiver<Event>>,
        strategy_enrichment: mpsc::Receiver<Event>,
        trade_ingress: mpsc::Receiver<Event>,
        trade_signal: mpsc::Receiver<Event>,
    }

    let mut states: HashMap<String, SymState> = HashMap::new();
    let mut results: HashMap<String, (usize, usize, f64)> = HashMap::new();
    let mut trade_results: Vec<Event> = Vec::new();
    let mut enrichment_events: Vec<Event> = Vec::new();
    let mut signal_events: Vec<Event> = Vec::new();
    let mut monthly: BTreeMap<String, (usize, usize, f64)> = BTreeMap::new();

    let mut all_tfs: Vec<Timeframe> = vec![trade_tf.clone()];
    for c in context_tfs {
        if let Some(t) = Timeframe::parse(c) {
            if !all_tfs.contains(&t) { all_tfs.push(t); }
        }
    }

    for sym in symbols {
        let symbol = Symbol(sym.clone());
        let options = BuildOptions { balance, risk_pct: risk };
        let compiled = compiled.ok_or("backtest: no compiled DSL")?;

        // Build enricher graph per timeframe.
        let mut enricher_graphs: HashMap<Timeframe, Graph> = HashMap::new();
        let mut enricher_ingress: HashMap<Timeframe, mpsc::Receiver<Event>> = HashMap::new();
        for tf in &all_tfs {
            let nodes = build_role_nodes(compiled, Role::Enricher, &symbol, tf, &options)
                .map_err(|e| format!("build enricher {}: {}", sym, e))?;
            // Each timeframe gets its own bus subscription? No — shared bus.
            // We'll create subscriptions after bus creation. For now, store nodes.
            enricher_graphs.insert(tf.clone(), Graph::with_context("", nodes));
        }

        let strategy_nodes = build_role_nodes(compiled, Role::Strategy, &symbol, trade_tf, &options)
            .map_err(|e| format!("build strategy {}: {}", sym, e))?;
        let trade_nodes = build_role_nodes(compiled, Role::Broker, &symbol, trade_tf, &options)
            .map_err(|e| format!("build broker {}: {}", sym, e))?;

        let mut bus = InMemoryMessageBus::new();
        let strategy_enrichment = bus.subscribe("enrichment");
        let trade_ingress = bus.subscribe("ingress");
        let trade_signal = bus.subscribe("signal");

        // Subscribe enricher graphs to "ingress" on the shared bus.
        let mut ei_map = HashMap::new();
        for tf in &all_tfs {
            ei_map.insert(tf.clone(), bus.subscribe("ingress"));
        }

        states.insert(sym.clone(), SymState {
            trade_tf: trade_tf.clone(),
            context_tfs: all_tfs.clone(),
            enricher_graphs,
            strategy_graph: Graph::with_context("", strategy_nodes),
            trade_graph: Graph::with_context("", trade_nodes),
            bus,
            enricher_ingress: ei_map,
            strategy_enrichment,
            trade_ingress,
            trade_signal,
        });
        results.insert(sym.clone(), (0, 0, 0.0));
    }

    for ev in &all_events {
        let sym = match ev {
            Event::CandleClosed { symbol, .. } => symbol.0.clone(),
            _ => continue,
        };
        let ev_tf = match ev {
            Event::CandleClosed { timeframe, .. } => timeframe.clone(),
            _ => continue,
        };
        let st = match states.get_mut(&sym) {
            Some(s) => s,
            None => continue,
        };

        // Publish the candle to this symbol's bus (like ingress in daemon mode).
        st.bus.publish("ingress", ev.clone());

        // 0a. Enricher first: update indicators (support/resistance/ema/atr) for
        //     this bar so bar-triggered strategy nodes read fresh values.
        if let Some(g) = st.enricher_graphs.get_mut(&ev_tf) {
            enrichment_events.push(ev.clone());
            let derived = g.process("ingress", &ev, &mut st.bus);
            for (_, e) in &derived {
                enrichment_events.push(e.clone());
            }
        }
        // Drain the enricher's bus subscription (already processed above).
        if let Some(rx) = st.enricher_ingress.get_mut(&ev_tf) {
            while rx.try_recv().is_ok() {}
        }
        // 0b. Strategy consumes the freshly-published enrichment (indicators).
        if ev_tf == st.trade_tf {
            let mut n = 0;
            while let Ok(e) = st.strategy_enrichment.try_recv() {
                n += 1;
                let derived = st.strategy_graph.process("enrichment", &e, &mut st.bus);
                for (_, ev2) in &derived {
                    if let Event::SetupSignal { .. } = ev2 {
                        signal_events.push(ev2.clone());
                    }
                }
            }
        }
        // 0c. Strategy: bar-triggered nodes (subscribe to ingress) run once per bar.
        if ev_tf == st.trade_tf {
            let derived = st.strategy_graph.process("ingress", &ev, &mut st.bus);
            for (_, e) in &derived {
                if let Event::SetupSignal { .. } = e {
                    signal_events.push(e.clone());
                }
            }
        }

        // Dispatch loop.
        loop {
            let mut progress = false;

            // 1. Trade: resolve (trade timeframe only).
            if ev_tf == st.trade_tf {
                while let Ok(e) = st.trade_ingress.try_recv() {
                    let out = st.trade_graph.process("ingress", &e, &mut st.bus);
                    for (_, e) in &out {
                        if let Event::TradeResult { pnl_r, entry_ts, exit_ts, .. } = e {
                            let entry = results.entry(sym.clone()).or_insert((0, 0, 0.0));
                            entry.0 += 1;
                            if *pnl_r > 0.0 { entry.1 += 1; }
                            entry.2 += pnl_r;
                            trade_results.push(e.clone());
                            let ts = if *exit_ts != 0 { *exit_ts } else { *entry_ts };
                            let key = month_key(ts);
                            let m = monthly.entry(key).or_insert((0, 0, 0.0));
                            m.0 += 1;
                            if *pnl_r > 0.0 { m.1 += 1; }
                            m.2 += pnl_r;
                        }
                    }
                    progress = true;
                }
            }

            // 2. Enricher: process on the matching timeframe graph.
            let ei = st.enricher_ingress.get_mut(&ev_tf);
            if let Some(rx) = ei {
                while let Ok(e) = rx.try_recv() {
                    if let Some(g) = st.enricher_graphs.get_mut(&ev_tf) {
                        g.process("ingress", &e, &mut st.bus);
                    }
                    progress = true;
                }
            }

            // 2b done before loop; placeholder
            // 3. Strategy: consume enrichment (trade timeframe only).
            if ev_tf == st.trade_tf {
                while let Ok(e) = st.strategy_enrichment.try_recv() {
                    let derived = st.strategy_graph.process("enrichment", &e, &mut st.bus);
                    for (_, ev2) in &derived {
                        if let Event::SetupSignal { .. } = ev2 {
                            signal_events.push(ev2.clone());
                        }
                    }
                    progress = true;
                }
            }

            // 4. Trade: sizing + broker (trade timeframe only).
            if ev_tf == st.trade_tf {
                while let Ok(e) = st.trade_signal.try_recv() {
                    let out = st.trade_graph.process("signal", &e, &mut st.bus);
                    for (_, e) in &out {
                        if let Event::TradeResult { pnl_r, entry_ts, exit_ts, .. } = e {
                            let entry = results.entry(sym.clone()).or_insert((0, 0, 0.0));
                            entry.0 += 1;
                            if *pnl_r > 0.0 { entry.1 += 1; }
                            entry.2 += pnl_r;
                            trade_results.push(e.clone());
                            let ts = if *exit_ts != 0 { *exit_ts } else { *entry_ts };
                            let key = month_key(ts);
                            let m = monthly.entry(key).or_insert((0, 0, 0.0));
                            m.0 += 1;
                            if *pnl_r > 0.0 { m.1 += 1; }
                            m.2 += pnl_r;
                        }
                    }
                    progress = true;
                }
            }

            if !progress {
                break;
            }
        }
    }

    // Write enrichment audit file.
    if let Some(path) = enrichment_out {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        use reliant_messaging::event_to_audit;
        let msgs: Vec<_> = enrichment_events.iter().map(|e| event_to_audit(e, "enrichment", 0)).collect();
        let _ = reliant_messaging::write_audit(&path, &msgs);
        println!("backtest: wrote {} enrichment events -> {}", msgs.len(), path);
    }
    // Write signal audit file.
    if let Some(path) = signals_out {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        use reliant_messaging::event_to_audit;
        let msgs: Vec<_> = signal_events.iter().map(|e| event_to_audit(e, "signal", 0)).collect();
        let _ = reliant_messaging::write_audit(&path, &msgs);
        println!("backtest: wrote {} signal events -> {}", msgs.len(), path);
    }

    if let Some(path) = trades_out {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(&path) {
            for e in &trade_results {
                if let Event::TradeResult { pnl_r, reason, symbol, timeframe, direction, entry, stop, target, entry_ts, exit_ts, exit_price, size, source } = e {
                    let mut obj = serde_json::Map::new();
                    obj.insert("symbol".to_string(), serde_json::Value::String(symbol.clone().unwrap_or_default()));
                    obj.insert("timeframe".to_string(), serde_json::Value::String(timeframe.clone().unwrap_or_default()));
                    obj.insert("direction".to_string(), serde_json::json!(direction));
                    obj.insert("entry".to_string(), serde_json::json!(entry));
                    obj.insert("stop".to_string(), serde_json::json!(stop));
                    obj.insert("target".to_string(), serde_json::json!(target));
                    obj.insert("entry_ts".to_string(), serde_json::json!(entry_ts));
                    obj.insert("exit_ts".to_string(), serde_json::json!(exit_ts));
                    obj.insert("exit_price".to_string(), serde_json::json!(exit_price));
                    obj.insert("reason".to_string(), serde_json::Value::String(reason.clone()));
                    obj.insert("pnl_r".to_string(), serde_json::json!(pnl_r));
                    obj.insert("size".to_string(), serde_json::json!(size));
                    obj.insert("source".to_string(), serde_json::Value::String(source.clone()));
                    if let Ok(line) = serde_json::to_string(&serde_json::Value::Object(obj)) {
                        let _ = writeln!(f, "{}", line);
                    }
                }
            }
            let _ = f.flush();
            println!("backtest: wrote {} trade records -> {}", trade_results.len(), path);
        }
    }

    Ok((results, monthly))
}


fn month_key(ts: i64) -> String {
    if ts <= 0 {
        return "unknown".to_string();
    }
    chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m").to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn main() {
    let cli = Cli::parse();

    let (symbols, tf, context_tfs, compiled, balance, risk) = if let Some(pf) = &cli.portfolio {
        let (syms, tf, ctx_tfs, strategy_file, bal, rsk) = match load_portfolio(pf) {
            Ok(v) => v,
            Err(e) => { eprintln!("backtest: {}", e); std::process::exit(2); }
        };
        let dsl_path = format!("{}/{}", pf, strategy_file);
        let src = std::fs::read_to_string(&dsl_path).unwrap_or_else(|e| { eprintln!("backtest: read {}: {}", dsl_path, e); std::process::exit(2); });
        let compiled = reliant_dsl::compile(&src, pf).unwrap_or_else(|e| { eprintln!("backtest: compile: {}", e); std::process::exit(2); });
        (syms, tf, ctx_tfs, Some(compiled), bal, rsk)
    } else if let Some(syms_str) = &cli.symbols {
        let syms: Vec<String> = syms_str.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        if syms.is_empty() {
            eprintln!("backtest: --symbols requires at least one symbol");
            std::process::exit(2);
        }
        let tf = parse_timeframe(&cli.timeframe);
        let dsl_path = match &cli.dsl {
            Some(p) => p.clone(),
            None => {
                eprintln!("backtest: --symbols requires --dsl <strategy file>");
                std::process::exit(2);
            }
        };
        let src = std::fs::read_to_string(&dsl_path).unwrap_or_else(|e| { eprintln!("backtest: read dsl: {}", e); std::process::exit(2); });
        let compiled = reliant_dsl::compile(&src, "backtest").unwrap_or_else(|e| { eprintln!("backtest: compile dsl: {}", e); std::process::exit(2); });
        (syms, tf, Vec::new(), Some(compiled), cli.balance, cli.risk)
    } else {
        eprintln!("backtest: requires --portfolio <dir> or --symbols <list> --dsl <file>");
        std::process::exit(2);
    };

    let tf_str = cli.timeframe.clone();
    let paths = expand_paths(&cli.ingress);
    if paths.is_empty() {
        eprintln!("backtest: no input files matched");
        return;
    }

    let res = run_one(&paths, "local", compiled.as_ref(), &symbols, &tf, &context_tfs, balance, risk, cli.trades_out.clone(), cli.enrichment_out.clone(), cli.signals_out.clone());
    match res {
        Ok((results, monthly)) => {
            println!("backtest: files={} symbols={}", paths.len(), symbols.len());
            let mut total_trades = 0usize;
            let mut total_wins = 0usize;
            let mut total_r = 0.0f64;
            for sym in &symbols {
                if let Some((n, w, r)) = results.get(sym) {
                    println!("{:<8} trades={} wins={} total_r={:.3}", sym, n, w, r);
                    total_trades += n;
                    total_wins += w;
                    total_r += r;
                } else {
                    println!("{:<8} trades=0 wins=0 total_r=0.000", sym);
                }
            }
            println!("MONTHLY:");
            for (month, (mn, mw, mr)) in &monthly {
                println!("{:<8} trades={} wins={} win_rate={:.1}% total_r={:.3}",
                    month, mn, mw,
                    if *mn > 0 { *mw as f64 / *mn as f64 * 100.0 } else { 0.0 },
                    mr);
            }
            println!("AGGREGATE: files={} trades={} wins={} win_rate={:.1}% total_r={:.3}",
                paths.len(), total_trades, total_wins,
                if total_trades > 0 { total_wins as f64 / total_trades as f64 * 100.0 } else { 0.0 },
                total_r);
        }
        Err(e) => { eprintln!("backtest: {}", e); std::process::exit(2); }
    }
    let _ = tf_str;
}