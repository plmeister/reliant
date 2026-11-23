use clap::Parser;
use reliant_cli::{load_portfolio, parse_timeframe, price_scale};
use reliant_datamodel::{Candle, Event, Symbol};
use reliant_messaging::{InMemoryMessageBus, MessageBus, NatsMessageBus, subject};
use reliant_store::{CandleStore, SqliteCandleStore, SqliteStateStore, StateStore, interleaved_events};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "reliant-ingress", about = "Publish candle events onto the message bus")]
struct Cli {
    #[arg(long, default_value = "AUDCAD")]
    instrument: String,
    #[arg(long)]
    portfolio: Option<String>,
    #[arg(long)]
    portfolios_root: Option<String>,
    #[arg(long, default_value = "1d")]
    timeframe: String,
    #[arg(long)]
    candle_db: Option<String>,
    #[arg(long, default_value = "data/study")]
    data_dir: String,
    #[arg(long)]
    state_db: Option<String>,
    #[arg(long)]
    from: Option<String>,
    #[arg(long)]
    to: Option<String>,
    #[arg(long)]
    daemon: bool,
    #[arg(long)]
    bus: Option<String>,
    #[arg(long, default_value = "3600")]
    interval: u64,
    #[arg(long, alias = "audit-out")]
    output: Option<String>,
    #[arg(long)]
    output_dir: Option<String>,
    #[arg(long)]
    windows: Option<String>,
    #[arg(long)]
    random: Option<usize>,
    #[arg(long, default_value = "90")]
    span_days_min: i64,
    #[arg(long, default_value = "365")]
    span_days_max: i64,
    #[arg(long, default_value = "42")]
    seed: u64,
    #[arg(long)]
    run_dir: Option<String>,
    #[arg(long)]
    ensure_data: bool,
    #[arg(long, default_value = "1")]
    max_stale_days: i64,
}

fn main() {
    let mut cli = Cli::parse();
    if let Some(rd) = &cli.run_dir {
        let _ = std::fs::create_dir_all(rd);
        cli.state_db = Some(format!("{}/state.sqlite", rd));
        cli.candle_db = Some(format!("{}/candles.db", rd));
    }
    let candle_db = cli.candle_db.clone().unwrap_or_else(|| reliant_cli::candles_db().to_string_lossy().to_string());
    let state_db = cli.state_db.clone().unwrap_or_else(|| reliant_cli::state_db().to_string_lossy().to_string());
    let tf = parse_timeframe(&cli.timeframe);
    let tf_str = cli.timeframe.clone();

    let instruments: Vec<String> = if let Some(p) = &cli.portfolio {
        let loaded = load_portfolio(p);
        if loaded.is_empty() { vec![cli.instrument.clone()] } else { loaded }
    } else {
        vec![cli.instrument.clone()]
    };

    // ensure data if requested
    if cli.ensure_data {
        for instrument in &instruments {
            let store = SqliteCandleStore::new(&candle_db).unwrap();
            let sym = Symbol(instrument.clone());
            let last = store.last_ts(&sym, &tf).unwrap_or(None);
            let stale = match last {
                Some(ts) => {
                    let last_date = chrono::DateTime::from_timestamp(ts, 0).unwrap().date_naive();
                    (chrono::Utc::now().date_naive() - last_date).num_days() > cli.max_stale_days
                }
                None => true,
            };
            if stale {
                let to_date = chrono::Utc::now().date_naive();
                let from_date = last.map(|ts| chrono::DateTime::from_timestamp(ts, 0).unwrap().date_naive() + chrono::Duration::days(1))
                    .unwrap_or_else(|| to_date - chrono::Duration::days(365 * 20));
                if from_date < to_date {
                    let prov = reliant_store::DukascopyProvider::new(if instrument.ends_with("JPY") { 1000.0 } else { 100000.0 });
                    if let Ok(new) = prov.fetch(&sym, &tf, from_date, to_date) {
                        let mut st = SqliteCandleStore::new(&candle_db).unwrap();
                        let _ = st.insert(&sym, &tf, &new);
                        println!("ingress: fetched {} for {} ({} new)", instrument, tf_str, new.len());
                    }
                }
            }
        }
    }

    // daemon mode
    if cli.daemon {
        let shutdown = reliant_cli::shutdown_flag();
        let mut bus: Box<dyn MessageBus> = match &cli.bus {
            Some(url) => Box::new(NatsMessageBus::new(url)),
            None => Box::new(InMemoryMessageBus::new()),
        };
        let mut store_state = SqliteStateStore::new(&state_db).unwrap();
        let mut cursors: HashMap<String, Option<SqliteStateStore>> = HashMap::new();
        let topic = subject("", "ingress");
        let fetch_topic = subject("", "fetch");
        let fetch_rx = bus.subscribe(&fetch_topic);
        println!("ingress daemon: publishing new candles to {} every {}s", topic, cli.interval);
        loop {
            // trigger immediate publish on FetchCompleted
            while let Ok(ev) = fetch_rx.try_recv() {
                if let reliant_datamodel::Event::FetchCompleted { .. } = ev {
                    // fall through to publish below
                }
            }
            // Portfolio fan-out (optional): when --portfolios-root is given, every
            // candle is published once per interested portfolio on pf.{id}.feed.
            // Otherwise legacy single-stream publish to `ingress`.
            let registry = cli.portfolios_root.as_ref().and_then(|r| {
                match reliant_portfolio::Registry::load(std::path::Path::new(r)) {
                    Ok(reg) => Some(reg),
                    Err(e) => {
                        eprintln!("ingress: portfolio registry: {:?}", e.failures);
                        None
                    }
                }
            });
            match &registry {
                Some(reg) => {
                    // Portfolio fan-out: publish exactly the (instrument, timeframe)
                    // pairs each portfolio asks for, strictly after that portfolio's
                    // own pipeline cursor. Ingress reads the cursor and never
                    // advances it — the enricher is the only writer.
                    let root = cli.portfolios_root.clone().unwrap_or_default();
                    for portfolio in reg.all() {
                        for tf in portfolio.all_timeframes() {
                            let tf_str = tf.as_str().to_string();
                            let mut cstore =
                                SqliteCandleStore::new(&candle_db).unwrap();
                            for instrument in &portfolio.instruments {
                                let sym = Symbol(instrument.clone());
                                // Per-stage recovery: publish only bars newer than the
                                // slowest role's cursor. A role that has not yet consumed
                                // a bar (missing file/cursor) contributes 0, forcing a
                                // replay of everything for that role.
                                let mut cursor = i64::MAX;
                                for role in ["enricher", "strategy", "broker"] {
                                    let cursor_key = format!(
                                        "{}:{}:{}:{}:cursor",
                                        portfolio.id, instrument, tf_str, role
                                    );
                                    let c = pipeline_cursor(
                                        &mut cursors, &root, &portfolio.id, role, &cursor_key,
                                    );
                                    cursor = cursor.min(c);
                                }
                                if cursor == i64::MAX {
                                    cursor = 0;
                                }
                                let candles = cstore
                                    .range(&sym, &tf, Some(cursor + 1), None)
                                    .unwrap_or_default();
                                if candles.is_empty() {
                                    continue;
                                }
                                let n = candles.len();
                                for c in &candles {
                                    bus.publish(
                                        &subject(&portfolio.id, "feed"),
                                        Event::CandleClosed {
                                            symbol: sym.clone(),
                                            timeframe: tf.clone(),
                                            candle: c.clone(),
                                        },
                                    );
                                }
                                println!(
                                    "ingress: published {} candles for {} {} -> {} (from {})",
                                    n, instrument, tf_str, portfolio.id, cursor
                                );
                            }
                        }
                    }
                }
                None => {
                    for instrument in &instruments {
                        let key = format!("{}:{}:last_ts", instrument, tf_str);
                        let mut cstore = SqliteCandleStore::new(&candle_db).unwrap();
                        let sym = Symbol(instrument.clone());
                        let last_published: i64 = store_state
                            .load(&key)
                            .unwrap_or(None)
                            .and_then(|s| s.parse::<i64>().ok())
                            .unwrap_or(0);
                        let candles = cstore
                            .range(&sym, &tf, Some(last_published + 1), None)
                            .unwrap_or_default();
                        if candles.is_empty() {
                            continue;
                        }
                        let n = candles.len();
                        for c in &candles {
                            bus.publish(
                                &topic,
                                Event::CandleClosed {
                                    symbol: sym.clone(),
                                    timeframe: tf.clone(),
                                    candle: c.clone(),
                                },
                            );
                        }
                        let last = candles.last().unwrap().timestamp.timestamp();
                        if let Err(e) = store_state.save(&key, &last.to_string()) {
                            eprintln!("ingress: watermark save failed for {}: {}", key, e);
                        }
                        println!("ingress: published {} candles for {}", n, instrument);
                    }
                }
            }
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
            std::thread::sleep(Duration::from_secs(1));
        }
        println!("ingress: shutting down");
    }

    // batch mode
    let out_dir = cli.output_dir.clone().unwrap_or_else(|| ".".to_string());
    let _ = std::fs::create_dir_all(&out_dir);

    let mut candles_by_instr: Vec<(String, Vec<Candle>)> = Vec::new();
    for instrument in &instruments {
        let store = SqliteCandleStore::new(&candle_db).unwrap();
        let sym = Symbol(instrument.clone());
        let mut candles = store.range(&sym, &tf, None, None).unwrap_or_default();
        if let Some(s) = &cli.from {
            if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                candles.retain(|c| c.timestamp.date_naive() >= d);
            }
        }
        if let Some(s) = &cli.to {
            if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                candles.retain(|c| c.timestamp.date_naive() < d);
            }
        }
        if !candles.is_empty() {
            candles_by_instr.push((instrument.clone(), candles));
        }
    }
    if candles_by_instr.is_empty() {
        println!("ingress: no candles found in {}", candle_db);
        return;
    }

    struct Span { label: String, data: Vec<(Symbol, Vec<Candle>)> }
    let mut spans: Vec<Span> = Vec::new();

    let ref_candles = &candles_by_instr[0].1;

    if let Some(w) = &cli.windows {
        for (i, part) in w.split(',').enumerate() {
            let segs: Vec<&str> = part.trim().split(':').collect();
            if segs.len() == 2 {
                if let (Ok(from), Ok(to)) = (
                    chrono::NaiveDate::parse_from_str(segs[0], "%Y-%m-%d"),
                    chrono::NaiveDate::parse_from_str(segs[1], "%Y-%m-%d"),
                ) {
                    let mut data = Vec::new();
                    for (instr, candles) in &candles_by_instr {
                        let slice: Vec<_> = candles.iter().filter(|c| {
                            let d = c.timestamp.date_naive();
                            d >= from && d < to
                        }).cloned().collect();
                        if !slice.is_empty() {
                            data.push((Symbol(instr.clone()), slice));
                        }
                    }
                    if !data.is_empty() { spans.push(Span { label: format!("w{}", i), data }); }
                }
            }
        }
    } else if let Some(n) = cli.random {
        let mut state = cli.seed;
        let mut next = || { state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); state };
        let min_ts = ref_candles.first().unwrap().timestamp.timestamp();
        let max_ts = ref_candles.last().unwrap().timestamp.timestamp();
        let lo = cli.span_days_min.min(cli.span_days_max);
        let hi = cli.span_days_min.max(cli.span_days_max);
        for i in 0..n {
            let span_days = lo + (next() % ((hi - lo + 1) as u64)) as i64;
            let span_secs = span_days * 86400;
            let range = (max_ts - min_ts - span_secs).max(1);
            let start = min_ts + (next() % (range as u64)) as i64;
            let end = start + span_secs;
            let mut data = Vec::new();
            for (instr, candles) in &candles_by_instr {
                let slice: Vec<_> = candles.iter().filter(|c| {
                    let t = c.timestamp.timestamp();
                    t >= start && t < end
                }).cloned().collect();
                if !slice.is_empty() {
                    data.push((Symbol(instr.clone()), slice));
                }
            }
            if !data.is_empty() { spans.push(Span { label: format!("r{}", i), data }); }
        }
    } else {
        let mut data = Vec::new();
        for (instr, candles) in &candles_by_instr {
            let mut c = candles.clone();
            if let Some(s) = &cli.from {
                if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    c.retain(|x| x.timestamp.date_naive() >= d);
                }
            }
            if let Some(s) = &cli.to {
                if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    c.retain(|x| x.timestamp.date_naive() < d);
                }
            }
            if !c.is_empty() { data.push((Symbol(instr.clone()), c)); }
        }
        if !data.is_empty() { spans.push(Span { label: "all".to_string(), data }); }
    }

    if spans.is_empty() {
        println!("ingress: no data spans produced");
        return;
    }

    let out_dir = cli.output_dir.clone().unwrap_or_else(|| ".".to_string());
    let _ = std::fs::create_dir_all(&out_dir);

    let single = spans.len() == 1;
    for span in spans {
        let sources: Vec<(Symbol, reliant_datamodel::Timeframe, Vec<Candle>)> =
            span.data.into_iter().map(|(sym, c)| (sym, tf.clone(), c)).collect();
        let events = interleaved_events(sources);
        let msgs: Vec<_> = events.iter().map(|e| reliant_messaging::event_to_audit(e, "ingress", 0)).collect();
        let label = &span.label;
        let names: Vec<&str> = instruments.iter().map(|s| s.as_str()).collect();
        let out_path = if let Some(o) = &cli.output {
            if single { o.clone() } else { format!("{}/{}.dat.zstd", out_dir, label) }
        } else {
            format!("{}/{}_{}.dat.zstd", out_dir, names.join("_"), label)
        };
        let _ = reliant_messaging::write_audit(&out_path, &msgs);
        let n = events.iter().filter(|e| matches!(e, Event::CandleClosed { .. })).count();
        println!("ingress: wrote {} candles -> {}", n, out_path);
    }
    if let Some(i) = instruments.first() { let _ = price_scale(i); }
}

/// Read one portfolio's pipeline cursor from its enricher state file. The
/// handle is cached (read-only) because a cursor is read once per
/// instrument/timeframe per tick. A missing file means "nothing consumed yet".
fn pipeline_cursor(
    cache: &mut HashMap<String, Option<SqliteStateStore>>,
    root: &str,
    portfolio: &str,
    role: &str,
    key: &str,
) -> i64 {
    let path = format!(
        "{}/{}/state/state-{}.sqlite",
        root.trim_end_matches('/'),
        portfolio,
        role
    );
    if matches!(cache.get(&path), None | Some(None)) {
        cache.insert(path.clone(), SqliteStateStore::open_readonly(&path).ok());
    }
    match cache.get_mut(&path) {
        Some(Some(store)) => store
            .load(key)
            .ok()
            .flatten()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0),
        _ => 0,
    }
}
