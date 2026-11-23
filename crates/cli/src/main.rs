use clap::{Parser, Subcommand};
use reliant_datamodel::{Symbol, Timeframe};
use reliant_engine::{AtrNode, CandleSource, DummyBroker, EmaNode, GeometryNode, Graph, StrategyNode, SwingNode};
use reliant_messaging::{write_audit, read_audit, event_to_audit, compress_audit};
use reliant_store::{interleaved_events, read_parquet, write_parquet, DukascopyProvider};
use std::path::Path;

#[derive(Parser)]
#[command(name = "reliant", version, about = "Event-driven market analysis engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Fetch {
        #[arg(long, default_value = "AUDCAD")]
        instruments: String,
        #[arg(long, default_value = "1d")]
        timeframes: String,
        #[arg(long, default_value = "data/study")]
        storage: String,
        #[arg(long, default_value = "dukascopy")]
        provider: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        portfolio: Option<String>,
    },
    Capture {
        #[arg(long, default_value = "AUDCAD")]
        instruments: String,
        #[arg(long, default_value = "1d")]
        timeframe: String,
        #[arg(long, default_value = "data/study")]
        storage: String,
        #[arg(long, default_value = "audits")]
        output: String,
        #[arg(long, default_value = "2020-01-01:2021-01-01,2021-01-01:2022-01-01")]
        windows: String,
    },
    Backtest {
        #[arg(long, default_value = "AUDCAD")]
        instruments: String,
        #[arg(long, default_value = "1d")]
        timeframe: String,
        #[arg(long, default_value = "audits/*.dat")]
        ingress: String,
        #[arg(long, default_value = "10000")]
        balance: f64,
        #[arg(long, default_value = "0.01")]
        risk: f64,
    },
    Run {
        #[arg(long, default_value = "AUDCAD")]
        instruments: String,
        #[arg(long, default_value = "1d")]
        timeframes: String,
        #[arg(long, default_value = "data/study")]
        data_dir: String,
        #[arg(long, default_value = "state.sqlite")]
        state_db: String,
        #[arg(long)]
        portfolio: Option<String>,
    },
    Compress { src: String, dst: String },
    ListInstruments {
        #[arg(long, default_value = "data/study")]
        storage: String,
    },
}

fn parse_tf(s: &str) -> Timeframe {
    match s {
        "1m" | "m1" => Timeframe::M1,
        "5m" | "m5" => Timeframe::M5,
        "1h" | "h1" => Timeframe::H1,
        "1d" | "d1" => Timeframe::D1,
        "1w" | "w1" => Timeframe::W1,
        _ => Timeframe::D1,
    }
}

fn price_scale(sym: &str) -> f64 {
    if sym.ends_with("JPY") { 1000.0 } else { 100000.0 }
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::ListInstruments { storage } => {
            for e in std::fs::read_dir(&storage).unwrap().flatten() {
                let n = e.file_name().to_string_lossy().to_string();
                if n.ends_with(".parquet") { println!("{}", n); }
            }
        }
        Commands::Compress { src, dst } => {
            compress_audit(&src, &dst).unwrap();
            println!("compressed {} -> {}", src, dst);
        }
        Commands::Fetch { instruments, timeframes, storage, provider, from, to, portfolio } => {
            let mut instruments = instruments;
            if let Some(p) = portfolio {
                let loaded = reliant_cli::load_portfolio(&p);
                if !loaded.is_empty() { instruments = loaded.join(","); }
            }
            let syms: Vec<&str> = instruments.split(',').collect();
            let tfs: Vec<&str> = timeframes.split(',').collect();
            let to_date = to.map(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").unwrap())
                .unwrap_or_else(|| chrono::Utc::now().date_naive());
            for sym in syms {
                for tf_str in &tfs {
                    let tf = parse_tf(tf_str);
                    let path = format!("{}/{}.{}.parquet", storage, sym, tf_str);
                    let existing = read_parquet(&path).unwrap_or_default();
                    let from_date = from.as_ref()
                        .map(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap())
                        .or_else(|| existing.last().map(|c| c.timestamp.date_naive() + chrono::Duration::days(1)))
                        .unwrap_or_else(|| to_date - chrono::Duration::days(365 * 5));
                    if from_date >= to_date {
                        println!("{} {} up to date", sym, tf_str);
                        continue;
                    }
                    if provider == "dukascopy" {
                        let prov = DukascopyProvider::new(price_scale(sym));
                        let new = prov.fetch(&Symbol(sym.to_string()), &tf, from_date, to_date)
                            .unwrap_or_else(|e| { eprintln!("fetch error: {}", e); std::process::exit(1); });
                        let mut all = existing.clone();
                        for c in new {
                            if !all.iter().any(|x| x.timestamp == c.timestamp) { all.push(c); }
                        }
                        all.sort_by_key(|c| c.timestamp);
                        if let Some(parent) = Path::new(&path).parent() { let _ = std::fs::create_dir_all(parent); }
                        write_parquet(&path, &all).unwrap();
                        println!("{} {}: {} candles (fetched {} new)", sym, tf_str, all.len(), all.len().saturating_sub(existing.len()));
                    } else {
                        eprintln!("unknown provider: {}", provider);
                    }
                }
            }
        }
        Commands::Capture { instruments, timeframe, storage, output, windows } => {
            std::fs::create_dir_all(&output).unwrap();
            let tf = parse_tf(&timeframe);
            for sym in instruments.split(',') {
                let path = format!("{}/{}.{}.parquet", storage, sym, timeframe);
                let candles = read_parquet(&path).unwrap_or_default();
                for (i, w) in windows.split(',').enumerate() {
                    let parts: Vec<&str> = w.split(':').collect();
                    let from = chrono::NaiveDate::parse_from_str(parts[0], "%Y-%m-%d").unwrap();
                    let to = chrono::NaiveDate::parse_from_str(parts[1], "%Y-%m-%d").unwrap();
                    let slice: Vec<_> = candles.iter().filter(|c| {
                        let d = c.timestamp.date_naive();
                        d >= from && d < to
                    }).cloned().collect();
                    let events = interleaved_events(vec![(Symbol(sym.to_string()), tf.clone(), slice)]);
                    let out_path = format!("{}/{}_{}_{}.dat", output, sym, timeframe, i);
                    let msgs: Vec<_> = events.iter().map(|e| event_to_audit(e, "ingress", 0)).collect();
                    write_audit(&out_path, &msgs).unwrap();
                    println!("{}: {} events -> {}", sym, msgs.len(), out_path);
                }
            }
        }
        Commands::Backtest { instruments, timeframe, ingress, balance, risk } => {
            let tf = parse_tf(&timeframe);
            let syms: Vec<&str> = instruments.split(',').collect();
            // expand ingress glob (simple: if contains '*', list dir)
            let mut paths: Vec<String> = Vec::new();
            if ingress.contains('*') {
                let dir = std::path::Path::new(&ingress).parent().unwrap_or(std::path::Path::new("."));
                let fname = std::path::Path::new(&ingress).file_name().unwrap().to_string_lossy();
                let prefix = fname.split('*').next().unwrap_or("").to_string();
                for e in std::fs::read_dir(dir).unwrap().flatten() {
                    let n = e.file_name().to_string_lossy().to_string();
                    if n.ends_with(".dat") || n.ends_with(".dat.zstd") {
                        if prefix.is_empty() || n.starts_with(&prefix) { paths.push(e.path().to_string_lossy().to_string()); }
                    }
                }
            } else {
                paths.push(ingress.clone());
            }
            paths.sort();
            for path in paths {
                // build graph for all instruments
                let mut nodes: Vec<Box<dyn reliant_engine::Node>> = Vec::new();
                for sym in &syms {
                    let symbol = Symbol(sym.to_string());
                    nodes.push(Box::new(CandleSource::new(&format!("cs_{}", sym), symbol.clone(), tf.clone(), vec![])));
                    nodes.push(Box::new(AtrNode::new(&format!("atr_{}", sym), symbol.clone(), tf.clone(), 14)));
                    nodes.push(Box::new(EmaNode::new(&format!("ema_{}", sym), symbol.clone(), tf.clone(), 20)));
                    nodes.push(Box::new(SwingNode::new(&format!("swing_{}", sym), symbol.clone(), tf.clone(), 3.5, 5)));
                    nodes.push(Box::new(DummyBroker::new(&format!("broker_{}", sym), symbol.clone(), tf.clone())));
                }
                nodes.push(Box::new(StrategyNode::new("strat", 3.5)));
                nodes.push(Box::new(GeometryNode::new("geom", balance, risk)));
                let mut graph = Graph::new(nodes);
                let msgs = read_audit(&path).unwrap();
                let events: Vec<_> = msgs.into_iter().map(|m| serde_json::from_value(m.payload).unwrap()).collect();
                let tagged: Vec<_> = events.into_iter().map(|e| ("ingress".to_string(), e)).collect();
                let emitted = graph.run(tagged);
                let results: Vec<(f64, String)> = emitted.iter().filter_map(|e| if let reliant_datamodel::Event::TradeResult { pnl_r, reason, .. } = e { Some((*pnl_r, reason.clone())) } else { None }).collect();
                let n = results.len();
                let wins = results.iter().filter(|(r, _)| *r > 0.0).count();
                let total_r: f64 = results.iter().map(|(r, _)| r).sum();
                let wr = if n > 0 { wins as f64 / n as f64 * 100.0 } else { 0.0 };
                println!("{}: trades={} wins={} win_rate={:.1}% total_r={:.3}", path, n, wins, wr, total_r);
            }
        }
        Commands::Run { instruments, timeframes, data_dir, state_db, portfolio } => {
            let mut instruments = instruments;
            if let Some(p) = portfolio {
                let loaded = reliant_cli::load_portfolio(&p);
                if !loaded.is_empty() { instruments = loaded.join(","); }
            }
            use reliant_store::SqliteStateStore;
            use reliant_store::StateStore;
            let syms: Vec<&str> = instruments.split(',').collect();
            let tfs: Vec<&str> = timeframes.split(',').collect();
            // fetch latest data
            for sym in &syms {
                for tf_str in &tfs {
                    let tf = parse_tf(tf_str);
                    let path = format!("{}/{}.{}.parquet", data_dir, sym, tf_str);
                    let existing = read_parquet(&path).unwrap_or_default();
                    let from = existing.last().map(|c| c.timestamp.date_naive() + chrono::Duration::days(1))
                        .unwrap_or_else(|| chrono::Utc::now().date_naive() - chrono::Duration::days(365));
                    let to = chrono::Utc::now().date_naive() + chrono::Duration::days(1);
                    if from < to {
                        let prov = DukascopyProvider::new(price_scale(sym));
                        if let Ok(new) = prov.fetch(&Symbol(sym.to_string()), &tf, from, to) {
                            let mut all = existing;
                            for c in new { if !all.iter().any(|x| x.timestamp == c.timestamp) { all.push(c); } }
                            all.sort_by_key(|c| c.timestamp);
                            let _ = std::fs::create_dir_all(&data_dir);
                            let _ = write_parquet(&path, &all);
                            println!("fetched {} {}: {} candles", sym, tf_str, all.len());
                        }
                    }
                }
            }
            // load state
            let mut store = SqliteStateStore::new(&state_db).unwrap();
            for sym in &syms {
                for tf_str in &tfs {
                    let key = format!("{}:{}:last_ts", sym, tf_str);
                    let last_ts = store.load(&key).unwrap().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                    let path = format!("{}/{}.{}.parquet", data_dir, sym, tf_str);
                    let candles = read_parquet(&path).unwrap_or_default();
                    let new_candles: Vec<_> = candles.into_iter().filter(|c| c.timestamp.timestamp() > last_ts).collect();
                    if new_candles.is_empty() { continue; }
                    let tf = parse_tf(tf_str);
                    let symbol = Symbol(sym.to_string());
                    // load context state
                    let state_key = format!("{}:{}:state", sym, tf_str);
                    let ctx_json = store.load(&state_key).unwrap();
                    // build graph
                    let nodes: Vec<Box<dyn reliant_engine::Node>> = vec![
                        Box::new(AtrNode::new(&format!("atr_{}", sym), symbol.clone(), tf.clone(), 14)),
                        Box::new(EmaNode::new(&format!("ema_{}", sym), symbol.clone(), tf.clone(), 20)),
                        Box::new(SwingNode::new(&format!("swing_{}", sym), symbol.clone(), tf.clone(), 3.5, 5)),
                        Box::new(DummyBroker::new(&format!("broker_{}", sym), symbol.clone(), tf.clone())),
                        Box::new(StrategyNode::new("strat", 3.5)),
                        Box::new(GeometryNode::new("geom", 10000.0, 0.01)),
                    ];
                    let mut graph = Graph::new(nodes);
                    // restore context state if present
                    if let Some(json) = ctx_json {
                        // apply to each context in graph
                        for ctx in graph.context().contexts.values_mut() {
                            ctx.load_json(&json);
                        }
                    }
                    // process new candles
                    let events: Vec<_> = new_candles.iter().map(|c| ("ingress".to_string(), reliant_datamodel::Event::CandleClosed {
                        symbol: symbol.clone(),
                        timeframe: tf.clone(),
                        candle: c.clone(),
                    })).collect();
                    let emitted = graph.run(events);
                    let results: Vec<(f64, String)> = emitted.iter().filter_map(|e| if let reliant_datamodel::Event::TradeResult { pnl_r, reason, .. } = e { Some((*pnl_r, reason.clone())) } else { None }).collect();
                    for (r, reason) in &results {
                        println!("{} {}: trade result {:.4}R ({})", sym, tf_str, r, reason);
                    }
                    // persist state
                    let last = new_candles.last().unwrap().timestamp.timestamp();
                    store.save(&key, &last.to_string()).unwrap();
                    for ctx in graph.context().contexts.values() {
                        store.save(&state_key, &ctx.state_json()).unwrap();
                    }
                    println!("{} {}: processed {} new candles", sym, tf_str, new_candles.len());
                }
            }
        }
    }
}
