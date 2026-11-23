use clap::Parser;
use reliant_cli::load_portfolio;
use reliant_datamodel::Timeframe;
use reliant_store::{CandleStore, DukascopyProvider, SqliteCandleStore};
use chrono::{Duration, NaiveDate, Utc};
use std::time::Duration as StdDuration;

#[derive(Parser)]
#[command(name = "reliant-fetcher", about = "Keep the shared candle store up to date")]
struct Cli {
    #[arg(long, default_value = "AUDCAD")]
    instrument: String,
    #[arg(long)]
    portfolio: Option<String>,
    #[arg(long, default_value = "1d")]
    timeframes: String,
    #[arg(long)]
    candle_db: Option<String>,
    #[arg(long, default_value = "dukascopy")]
    provider: String,
    #[arg(long)]
    from: Option<String>,
    #[arg(long)]
    to: Option<String>,
    #[arg(long)]
    daemon: bool,
    #[arg(long, default_value = "3600")]
    interval: u64,
    #[arg(long, default_value = "1")]
    max_stale_days: i64,
    /// Delay between daily datafeed requests, in milliseconds (rate limiting).
    #[arg(long, default_value = "150")]
    request_delay_ms: u64,
    #[arg(long)]
    bus: Option<String>,
    #[arg(long)]
    watermarks: bool,
    #[arg(long)]
    import_parquet: Option<String>,
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

fn fetch_one(store: &mut SqliteCandleStore, sym: &str, tf_str: &str, from: Option<NaiveDate>, to: NaiveDate, provider: &str, request_delay_ms: u64) {
    let tf = parse_tf(tf_str);
    let last = store.last_ts(&reliant_datamodel::Symbol(sym.to_string()), &tf).unwrap_or(None);
    let after_last = last.map(|ts| chrono::DateTime::from_timestamp(ts, 0).unwrap().date_naive() + Duration::days(1));
    // an explicit --from must not force a re-download of already stored candles
    let from_date = match (from, after_last) {
        (Some(f), Some(l)) => f.max(l),
        (Some(f), None) => f,
        (None, Some(l)) => l,
        (None, None) => to - Duration::days(365 * 20),
    };
    if from_date >= to {
        println!("fetcher: {} {} up to date", sym, tf_str);
        return;
    }
    if provider == "dukascopy" {
        let prov = DukascopyProvider::with_delay(price_scale(sym), StdDuration::from_millis(request_delay_ms));
        match prov.fetch(&reliant_datamodel::Symbol(sym.to_string()), &tf, from_date, to) {
            Ok(new) => {
                let n = new.len();
                if let Err(e) = store.insert(&reliant_datamodel::Symbol(sym.to_string()), &tf, &new) {
                    eprintln!("fetcher: insert error: {}", e);
                } else {
                    println!("fetcher: {} {} inserted {} new candles", sym, tf_str, n);
                }
            }
            Err(e) => eprintln!("fetcher: {} {} fetch error: {}", sym, tf_str, e),
        }
    }
}

fn is_stale(store: &SqliteCandleStore, sym: &str, tf_str: &str, max_stale_days: i64) -> bool {
    let tf = parse_tf(tf_str);
    match store.last_ts(&reliant_datamodel::Symbol(sym.to_string()), &tf) {
        Ok(Some(ts)) => {
            let last = chrono::DateTime::from_timestamp(ts, 0).unwrap().date_naive();
            (Utc::now().date_naive() - last).num_days() > max_stale_days
        }
        _ => true,
    }
}

fn main() {
    let cli = Cli::parse();
    let candle_db = cli.candle_db.clone().unwrap_or_else(|| reliant_cli::candles_db().to_string_lossy().to_string());
    let instruments: Vec<String> = if let Some(p) = &cli.portfolio {
        let loaded = load_portfolio(p);
        if loaded.is_empty() { vec![cli.instrument.clone()] } else { loaded }
    } else {
        vec![cli.instrument.clone()]
    };
    let tfs: Vec<&str> = cli.timeframes.split(',').collect();
    let to_date = cli.to.as_ref()
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap())
        .unwrap_or_else(|| Utc::now().date_naive());

    let mut store = SqliteCandleStore::new(&candle_db).unwrap();

    if let Some(dir) = &cli.import_parquet {
        // import all parquet files in dir
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                // expect SYMBOL.TF.parquet
                if !name.ends_with(".parquet") { continue; }
                let base = name.trim_end_matches(".parquet");
                let parts: Vec<&str> = base.split('.').collect();
                if parts.len() != 2 { continue; }
                let sym = parts[0];
                let tf_str = parts[1];
                let tf = parse_tf(tf_str);
                let path = e.path().to_string_lossy().to_string();
                let candles = reliant_store::read_parquet(&path).unwrap_or_default();
                if candles.is_empty() { continue; }
                if let Err(err) = store.insert(&reliant_datamodel::Symbol(sym.to_string()), &tf, &candles) {
                    eprintln!("import error {}: {}", name, err);
                } else {
                    println!("imported {} ({} candles)", name, candles.len());
                }
            }
        }
        return;
    }

    if cli.watermarks {
        println!("{:<10} {:<6} {:<20} {:<20}", "symbol", "tf", "earliest", "latest");
        for (sym, tf, e, l) in store.watermarks().unwrap_or_default() {
            println!("{:<10} {:<6} {:<20} {:<20}", sym, tf, e, l);
        }
        return;
    }

    if cli.daemon {
        use reliant_messaging::{InMemoryMessageBus, MessageBus, NatsMessageBus, subject};
        use reliant_datamodel::Event;
        use std::collections::BTreeSet;
        let shutdown = reliant_cli::shutdown_flag();
        let mut bus: Box<dyn MessageBus> = match &cli.bus {
            Some(url) => Box::new(NatsMessageBus::new(url)),
            None => Box::new(InMemoryMessageBus::new()),
        };
        // subscribe to fetch requests
        let fetch_topic = subject("", "fetch");
        let rx = bus.subscribe(&fetch_topic);
        println!("fetcher daemon: listening on {} + periodic check every {}s", fetch_topic, cli.interval);
        let mut pending_syms: BTreeSet<String> = instruments.iter().cloned().collect();
        let mut pending_tfs: BTreeSet<String> = tfs.iter().map(|s| s.to_string()).collect();
        let mut last_check = std::time::Instant::now();
        loop {
            // collect fetch requests (non-blocking)
            while let Ok(ev) = rx.try_recv() {
                if let Event::FetchRequested { symbols, timeframes } = ev {
                    for s in symbols { pending_syms.insert(s); }
                    for t in timeframes { pending_tfs.insert(t); }
                }
            }
            // periodic staleness check
            if last_check.elapsed() >= StdDuration::from_secs(cli.interval) {
                for sym in &instruments {
                    for tf in &tfs {
                        if is_stale(&store, sym, tf, cli.max_stale_days) {
                            pending_syms.insert(sym.clone());
                            pending_tfs.insert(tf.to_string());
                        }
                    }
                }
                last_check = std::time::Instant::now();
            }
            // process pending
            if !pending_syms.is_empty() {
                let syms: Vec<String> = std::mem::take(&mut pending_syms).into_iter().collect();
                let tfs_p: Vec<String> = std::mem::take(&mut pending_tfs).into_iter().collect();
                let tfs_to_use: Vec<&str> = if tfs_p.is_empty() { tfs.clone() } else { tfs_p.iter().map(|s| s.as_str()).collect() };
                for sym in &syms {
                    for tf in &tfs_to_use {
                        let from = cli.from.as_ref().map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap());
                        fetch_one(&mut store, sym, tf, from, to_date, &cli.provider, cli.request_delay_ms);
                    }
                }
                // publish fetch complete
                let complete = Event::FetchCompleted {
                    symbols: syms,
                    timeframes: tfs_to_use.iter().map(|s| s.to_string()).collect(),
                };
                bus.publish(&fetch_topic, complete);
                println!("fetcher: published FetchCompleted");
            }
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
            std::thread::sleep(StdDuration::from_millis(200));
        }
        println!("fetcher: shutting down");
    } else {
        for sym in &instruments {
            for tf in &tfs {
                let from = cli.from.as_ref().map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap());
                fetch_one(&mut store, sym, tf, from, to_date, &cli.provider, cli.request_delay_ms);
            }
        }
    }
}
