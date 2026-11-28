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
    Reset {
        /// Portfolio directory (or id under --portfolios-root).
        #[arg(long)]
        portfolio: String,
        /// Portfolios root used to resolve an id.
        #[arg(long)]
        portfolios_root: Option<String>,
        /// Also remove trades.jsonl.
        #[arg(long, default_value_t = true)]
        trades: bool,
    },
    Dev {
        /// Portfolio directory (or id under --portfolios-root).
        #[arg(long)]
        portfolio: String,
        /// Portfolios root used to resolve an id.
        #[arg(long)]
        portfolios_root: Option<String>,
        /// Instrument to audit (default: first portfolio instrument).
        #[arg(long)]
        instrument: Option<String>,
        /// Timeframe (default: portfolio trade timeframe).
        #[arg(long)]
        timeframe: Option<String>,
        /// Candles db for auto-generating an ingress audit.
        #[arg(long)]
        candles_db: Option<String>,
        /// Ingress audit file (skip auto-generation).
        #[arg(long)]
        ingress: Option<String>,
        /// Baseline file to compare against.
        #[arg(long)]
        compare: Option<String>,
        /// Write a baseline JSON for this run.
        #[arg(long)]
        baseline_out: Option<String>,
        /// Do not reset state first.
        #[arg(long)]
        no_reset: bool,
    },
    ListInstruments {
        #[arg(long, default_value = "data/study")]
        storage: String,
    },
    Evolve {
        /// Portfolio directory (or id under --portfolios-root). Optional if --dsl given.
        #[arg(long)]
        portfolio: Option<String>,
        /// Strategy DSL file to evolve directly (alternative to --portfolio).
        #[arg(long)]
        dsl: Option<String>,
        /// Comma-separated symbols (required with --dsl).
        #[arg(long)]
        symbols: Option<String>,
        /// Portfolios root used to resolve an id.
        #[arg(long)]
        portfolios_root: Option<String>,
        /// Mode: "params" (evolve numeric params) or "combo" (enumerate strategy subsets).
        #[arg(long, default_value = "params")]
        mode: String,
        /// Param ranges for params mode, e.g. "k:2.0-5.0,stop_atr_mult:1.0-3.0".
        #[arg(long, default_value = "k:2.0-5.0,stop_atr_mult:1.0-3.0,target_atr_mult:1.0-4.0")]
        params: String,
        /// Training ingress audit file (comma-separated for multiple).
        #[arg(long)]
        train: Option<String>,
        /// Candles db: auto-generate train audits for all portfolio instruments.
        #[arg(long)]
        candles_db: Option<String>,
        /// Date range for auto-generated audits (from:to, e.g. 2015-01-01:2020-01-01).
        #[arg(long)]
        train_range: Option<String>,
        /// Validation ingress audit file (optional; for OOS check).
        #[arg(long)]
        val: Option<String>,
        /// Date range for auto-generated val audits.
        #[arg(long)]
        val_range: Option<String>,
        /// Number of generations.
        #[arg(long, default_value_t = 20)]
        generations: usize,
        /// Population size.
        #[arg(long, default_value_t = 20)]
        pop: usize,
        /// RNG seed.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Minimum trades for a valid candidate.
        #[arg(long, default_value_t = 10)]
        min_trades: usize,
        /// Transaction cost per trade in R (subtracted from fitness).
        #[arg(long, default_value_t = 0.0)]
        cost: f64,
        /// Fitness metric: "train" (default), "val" (validation R), "gap" (train - |val-train| penalty).
        #[arg(long, default_value = "train")]
        fitness: String,
        /// Write best params + candidate DSL here.
        #[arg(long)]
        out: Option<String>,
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


fn resolve_portfolio_dir(portfolio: &str, root: &str) -> std::path::PathBuf {
    let p = std::path::Path::new(portfolio);
    if p.exists() { return p.to_path_buf(); }
    let cand = std::path::Path::new(root).join(portfolio);
    if cand.exists() { return cand; }
    // fallback: treat as path
    p.to_path_buf()
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
        Commands::Reset { portfolio, portfolios_root, trades } => {
            let root = portfolios_root.clone().unwrap_or_else(|| reliant_cli::portfolios_root().to_string_lossy().to_string());
            let dir = resolve_portfolio_dir(&portfolio, &root);
            let state_dir = dir.join("state");
            let mut removed = 0usize;
            for name in ["state-strategy.sqlite", "state-enricher.sqlite", "state-broker.sqlite"] {
                let p = state_dir.join(name);
                if p.exists() {
                    if std::fs::remove_file(&p).is_ok() { println!("removed {}", p.display()); removed += 1; }
                    else { eprintln!("failed to remove {}", p.display()); }
                }
            }
            if trades {
                let t = state_dir.join("trades.jsonl");
                if t.exists() {
                    if std::fs::remove_file(&t).is_ok() { println!("removed {}", t.display()); removed += 1; }
                }
            }
            println!("reset: {} files removed for {}", removed, dir.display());
        }
        Commands::Dev { portfolio, portfolios_root, instrument, timeframe, candles_db, ingress, compare, baseline_out, no_reset } => {
            let root = portfolios_root.clone().unwrap_or_else(|| reliant_cli::portfolios_root().to_string_lossy().to_string());
            let dir = resolve_portfolio_dir(&portfolio, &root);
            let exe_dir = std::env::current_exe().ok().and_then(|p| p.parent().map(|p| p.to_path_buf())).unwrap_or_default();

            if !no_reset {
                // reset state
                let state_dir = dir.join("state");
                for name in ["state-strategy.sqlite", "state-enricher.sqlite", "state-broker.sqlite", "trades.jsonl"] {
                    let p = state_dir.join(name);
                    if p.exists() { let _ = std::fs::remove_file(&p); }
                }
                println!("dev: reset state for {}", dir.display());
            }

            // Determine ingress audit path
            let mut ingress_path = ingress.clone();
            if ingress_path.is_none() {
                if let Some(db) = candles_db {
                    // resolve instrument
                    let inst = instrument.clone().unwrap_or_else(|| {
                        // read portfolio.json
                        let pj = dir.join("portfolio.json");
                        if let Ok(raw) = std::fs::read_to_string(&pj) {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                                if let Some(arr) = v.get("instruments").and_then(|x| x.as_array()) {
                                    if let Some(s) = arr.first().and_then(|x| x.as_str()) { return s.to_string(); }
                                }
                            }
                        }
                        "AUDCAD".to_string()
                    });
                    let tf = timeframe.clone().unwrap_or_else(|| "1d".to_string());
                    let audit_dir = reliant_cli::audit_dir();
                    let _ = std::fs::create_dir_all(&audit_dir);
                    let out = audit_dir.join(format!("{}_{}.dat", inst, tf));
                    let out_str = out.to_string_lossy().to_string();
                    // run ingress binary
                    let ingress_bin = exe_dir.join("ingress");
                    let status = std::process::Command::new(&ingress_bin)
                        .arg("--instrument").arg(&inst)
                        .arg("--timeframe").arg(&tf)
                        .arg("--candle-db").arg(db.clone())
                        .arg("--output").arg(&out_str)
                        .status();
                    match status {
                        Ok(s) if s.success() => { println!("dev: generated audit {}", out_str); ingress_path = Some(out_str); }
                        _ => { eprintln!("dev: ingress failed; is {} built?", ingress_bin.display()); std::process::exit(1); }
                    }
                } else {
                    eprintln!("dev: provide --ingress or --candles-db");
                    std::process::exit(2);
                }
            }
            let ingress_path = ingress_path.unwrap();

            // run backtest binary
            let backtest_bin = exe_dir.join("backtest");
            let mut cmd = std::process::Command::new(&backtest_bin);
            cmd.arg("--portfolio").arg(dir.to_string_lossy().to_string())
               .arg("--ingress").arg(&ingress_path);
            if let Some(b) = baseline_out { cmd.arg("--baseline-out").arg(b); }
            if let Some(c) = compare { cmd.arg("--compare").arg(c); }
            let status = cmd.status();
            match status {
                Ok(s) if s.success() => {}
                _ => { eprintln!("dev: backtest failed"); std::process::exit(1); }
            }
        }
        Commands::Evolve { portfolio, dsl, symbols, portfolios_root, mode, params, train, candles_db, train_range, val, val_range, generations, pop, seed, min_trades, cost, fitness, out } => {
            // simple xorshift64 rng
            struct Rng { state: u64 }
            impl Rng {
                fn new(seed: u64) -> Self { Rng { state: if seed == 0 { 1 } else { seed } } }
                fn next(&mut self) -> u64 {
                    let mut x = self.state;
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    self.state = x;
                    x
                }
                fn f64(&mut self) -> f64 { (self.next() as f64) / (u64::MAX as f64) }
                fn range(&mut self, lo: f64, hi: f64) -> f64 { lo + (hi - lo) * self.f64() }
                fn gen_bool(&mut self, p: f64) -> bool { self.f64() < p }
                fn gen_range_usize(&mut self, n: usize) -> usize { (self.next() as usize) % n.max(1) }
            }
            let exe_dir = std::env::current_exe().ok().and_then(|p| p.parent().map(|p| p.to_path_buf())).unwrap_or_default();
            let backtest_bin = exe_dir.join("backtest");
            let tmp_root = std::env::temp_dir().join("reliant_evolve");
            let _ = std::fs::create_dir_all(&tmp_root);

            // Pre-parse param ranges (used in params mode; harmless otherwise)
            let mut param_names: Vec<String> = Vec::new();
            let mut param_lo: Vec<f64> = Vec::new();
            let mut param_hi: Vec<f64> = Vec::new();
            for spec in params.split(',') {
                let parts: Vec<&str> = spec.split(':').collect();
                if parts.len() != 2 { continue; }
                let name = parts[0].trim().to_string();
                let range: Vec<&str> = parts[1].split('-').collect();
                if range.len() != 2 { continue; }
                let lo: f64 = range[0].parse().unwrap_or(0.0);
                let hi: f64 = range[1].parse().unwrap_or(1.0);
                param_names.push(name);
                param_lo.push(lo);
                param_hi.push(hi);
            }

            // Resolve DSL path and symbols
            let (dsl_path, symbols, portfolio_dir) = if let Some(d) = dsl {
                let syms = symbols.clone().unwrap_or_else(|| "AUDCAD".to_string());
                (std::path::PathBuf::from(d), syms, None)
            } else if let Some(p) = portfolio {
                let root = portfolios_root.clone().unwrap_or_else(|| reliant_cli::portfolios_root().to_string_lossy().to_string());
                let dir = resolve_portfolio_dir(&p, &root);
                let syms = {
                    let pj = dir.join("portfolio.json");
                    if let Ok(raw) = std::fs::read_to_string(&pj) {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                            if let Some(arr) = v.get("instruments").and_then(|x| x.as_array()) {
                                arr.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join(",")
                            } else { "AUDCAD".to_string() }
                        } else { "AUDCAD".to_string() }
                    } else { "AUDCAD".to_string() }
                };
                (dir.join("strategy.dsl"), syms, Some(dir))
            } else {
                eprintln!("evolve: provide --portfolio or --dsl");
                std::process::exit(2);
            };
            let original_dsl = std::fs::read_to_string(&dsl_path).unwrap_or_default();
            let portfolio_dir_ref = portfolio_dir;

            // Resolve train ingress: auto-generate from candles_db if needed
            let train = if let Some(t) = train {
                t
            } else if let Some(db) = candles_db.clone() {
                // determine instruments
                let insts: Vec<String> = if let Some(pd) = &portfolio_dir_ref {
                    let pj = pd.join("portfolio.json");
                    if let Ok(raw) = std::fs::read_to_string(&pj) {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                            if let Some(arr) = v.get("instruments").and_then(|x| x.as_array()) {
                                arr.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect()
                            } else { vec!["AUDCAD".to_string()] }
                        } else { vec!["AUDCAD".to_string()] }
                    } else { vec!["AUDCAD".to_string()] }
                } else {
                    symbols.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
                };
                let (from, to) = if let Some(r) = train_range {
                    let parts: Vec<String> = r.split(':').map(|s| s.to_string()).collect();
                    (parts.first().cloned().unwrap_or_else(|| "2010-01-01".to_string()),
                     parts.get(1).cloned().unwrap_or_else(|| "2020-01-01".to_string()))
                } else {
                    ("2010-01-01".to_string(), "2020-01-01".to_string())
                };
                let mut paths = Vec::new();
                let audit_dir = reliant_cli::audit_dir();
                let _ = std::fs::create_dir_all(&audit_dir);
                for inst in &insts {
                    let out = audit_dir.join(format!("{}_{}_train.dat", inst, "1d"));
                    let out_str = out.to_string_lossy().to_string();
                    let status = std::process::Command::new(exe_dir.join("ingress"))
                        .arg("--instrument").arg(inst)
                        .arg("--timeframe").arg("1d")
                        .arg("--candle-db").arg(db.clone())
                        .arg("--from").arg(from.clone())
                        .arg("--to").arg(to.clone())
                        .arg("--output").arg(&out_str)
                        .status();
                    if status.map(|s| s.success()).unwrap_or(false) {
                        paths.push(out_str);
                    } else {
                        eprintln!("evolve: failed to generate audit for {}", inst);
                    }
                }
                paths.join(",")
            } else {
                eprintln!("evolve: provide --train or --candles-db");
                std::process::exit(2);
            };

            // Resolve val ingress: auto-generate from candles_db if needed
            let val = if val.is_some() {
                val
            } else if candles_db.is_some() && fitness == "val" {
                // auto-generate val audits
                let db = candles_db.as_ref().unwrap();
                let insts: Vec<String> = if let Some(pd) = &portfolio_dir_ref {
                    let pj = pd.join("portfolio.json");
                    if let Ok(raw) = std::fs::read_to_string(&pj) {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                            if let Some(arr) = v.get("instruments").and_then(|x| x.as_array()) {
                                arr.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect()
                            } else { vec!["AUDCAD".to_string()] }
                        } else { vec!["AUDCAD".to_string()] }
                    } else { vec!["AUDCAD".to_string()] }
                } else {
                    symbols.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
                };
                let (from, to) = if let Some(r) = val_range {
                    let parts: Vec<String> = r.split(':').map(|s| s.to_string()).collect();
                    (parts.first().cloned().unwrap_or_else(|| "2018-01-01".to_string()),
                     parts.get(1).cloned().unwrap_or_else(|| "2026-01-01".to_string()))
                } else {
                    ("2018-01-01".to_string(), "2026-01-01".to_string())
                };
                let mut paths = Vec::new();
                let audit_dir = reliant_cli::audit_dir();
                let _ = std::fs::create_dir_all(&audit_dir);
                for inst in &insts {
                    let out = audit_dir.join(format!("{}_{}_val.dat", inst, "1d"));
                    let out_str = out.to_string_lossy().to_string();
                    let status = std::process::Command::new(exe_dir.join("ingress"))
                        .arg("--instrument").arg(inst)
                        .arg("--timeframe").arg("1d")
                        .arg("--candle-db").arg(db)
                        .arg("--from").arg(&from)
                        .arg("--to").arg(&to)
                        .arg("--output").arg(&out_str)
                        .status();
                    if status.map(|s| s.success()).unwrap_or(false) {
                        paths.push(out_str);
                    }
                }
                if paths.is_empty() { None } else { Some(paths.join(",")) }
            } else {
                val
            };

            // Helper: run backtest on a candidate DSL, return (trades, total_r)
            let run_candidate = |dsl_text: &str, params: Option<&[f64]>, ingress: &str| -> (usize, f64) {
                let output = if let Some(pd) = &portfolio_dir_ref {
                    let pid = pd.file_name().and_then(|s| s.to_str()).unwrap_or("portfolio").to_string();
                    let cand_dir = tmp_root.join(&pid);
                    let _ = std::fs::remove_dir_all(&cand_dir);
                    let _ = std::fs::create_dir_all(&cand_dir);
                    let _ = std::fs::write(cand_dir.join("strategy.dsl"), dsl_text);
                    let _ = std::fs::copy(pd.join("portfolio.json"), cand_dir.join("portfolio.json"));
                    // copy lib dir to temp and evolve params in all lib files
                    let cand_lib = tmp_root.join(format!("lib_{}", pid));
                    let _ = std::fs::create_dir_all(&cand_lib);
                    let orig_lib = std::path::PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".config/reliant/lib");
                    if orig_lib.exists() {
                        if let Ok(rd) = std::fs::read_dir(&orig_lib) {
                            for e in rd.flatten() {
                                let src = e.path();
                                if src.is_file() {
                                    let dst = cand_lib.join(e.file_name());
                                    let _ = std::fs::copy(&src, &dst);
                                }
                            }
                        }
                    }
                    // apply param replacements to all lib files in cand_lib
                    if let Some(pv) = params {
                        if let Ok(rd) = std::fs::read_dir(&cand_lib) {
                            for e in rd.flatten() {
                                let fp = e.path();
                                if !fp.is_file() { continue; }
                                if let Ok(text) = std::fs::read_to_string(&fp) {
                                    let mut t = text;
                                    for (i, name) in param_names.iter().enumerate() {
                                        let val = pv[i];
                                        let mut result = String::new();
                                        let mut search_from = 0;
                                        let needle = format!("{}:", name);
                                        let mut pos = 0;
                                        let bytes = t.as_bytes();
                                        while pos < t.len() {
                                            // word boundary: needle must not be preceded by an ident char
                                            let prev_ok = pos == 0 || {
                                                let b = t.as_bytes()[pos-1];
                                                !(b.is_ascii_alphanumeric() || b == b'_')
                                            };
                                            if prev_ok && t[pos..].starts_with(&needle) {
                                                let mut j = pos + needle.len();
                                                while j < t.len() && (bytes[j] == b' ' || bytes[j] == b'\t') { j += 1; }
                                                let start = j;
                                                while j < t.len() && (bytes[j].is_ascii_digit() || bytes[j] == b'.' || bytes[j] == b'-') { j += 1; }
                                                if j > start {
                                                    result.push_str(&t[search_from..start]);
                                                    result.push_str(&format!("{}", val));
                                                    search_from = j;
                                                    pos = j;
                                                    continue;
                                                }
                                            }
                                            pos += 1;
                                        }
                                        result.push_str(&t[search_from..]);
                                        t = result;
                                    }
                                    let _ = std::fs::write(&fp, t);
                                }
                            }
                        }
                    }
                    let mut cmd = std::process::Command::new(&backtest_bin);
                    cmd.arg("--portfolio").arg(cand_dir.to_string_lossy().to_string())
                       .arg("--ingress").arg(ingress);
                    cmd.env("RELIANT_LIB", cand_lib.to_string_lossy().to_string());
                    cmd.output()
                } else {
                    // write candidate dsl to temp file
                    let cand_dsl = tmp_root.join(format!("cand_{}.dsl", std::process::id()));
                    let _ = std::fs::write(&cand_dsl, dsl_text);
                    std::process::Command::new(&backtest_bin)
                        .arg("--symbols").arg(&symbols)
                        .arg("--dsl").arg(cand_dsl.to_string_lossy().to_string())
                        .arg("--ingress").arg(ingress)
                        .output()
                };
                match output {
                    Ok(o) => {
                        let stdout = String::from_utf8_lossy(&o.stdout);
                        let mut trades = 0usize;
                        let mut total_r = 0.0f64;
                        for line in stdout.lines() {
                            if line.starts_with("AGGREGATE:") {
                                for part in line.split_whitespace() {
                                    if part.starts_with("trades=") {
                                        trades = part.trim_start_matches("trades=").parse().unwrap_or(0);
                                    }
                                    if part.starts_with("total_r=") {
                                        total_r = part.trim_start_matches("total_r=").parse().unwrap_or(0.0);
                                    }
                                }
                            }
                        }
                        (trades, total_r)
                    }
                    Err(_) => (0, 0.0),
                }
            };

            let fitness = |dsl_text: &str, params: Option<&[f64]>, ingress: &str, min_trades: usize| -> f64 {
                let (t_train, r_train) = run_candidate(dsl_text, params, ingress);
                if t_train < min_trades { return -1000.0; }
                let cost_adj = cost * t_train as f64;
                if fitness == "val" {
                    if let Some(v) = &val {
                        let (t_v, r_v) = run_candidate(dsl_text, params, v);
                        if t_v < min_trades / 2 { return -1000.0; }
                        return r_v - cost * t_v as f64;
                    }
                }
                if fitness == "gap" {
                    if let Some(v) = &val {
                        let (t_v, r_v) = run_candidate(dsl_text, params, v);
                        // penalize train-val gap
                        let gap = (r_train - r_v).abs();
                        return r_train - cost_adj - gap * 0.5;
                    }
                }
                r_train - cost_adj
            };

            if mode == "combo" {
                // Enumerate subsets of strategies found in the original DSL imports
                // Simple: try removing each import line and evaluate
                println!("evolve(combo): enumerating strategy subsets...");
                let imports: Vec<&str> = original_dsl.lines().filter(|l| l.trim().starts_with("import ")).collect();
                let n = imports.len();
                let mut best_r = f64::NEG_INFINITY;
                let mut best_dsl = original_dsl.clone();
                let mut best_desc = "all".to_string();
                // Evaluate full set
                let r = fitness(&original_dsl, None, &train, min_trades);
                println!("  all strategies: train_r={:.3}", r);
                if r > best_r { best_r = r; best_dsl = original_dsl.clone(); best_desc = "all".to_string(); }
                // Try removing each single import
                for i in 0..n {
                    let mut lines: Vec<&str> = original_dsl.lines().collect();
                    // remove import line i
                    let mut idx = 0;
                    let mut removed = 0;
                    let mut new_lines = Vec::new();
                    for line in &lines {
                        if line.trim().starts_with("import ") && removed == 0 && idx == i {
                            removed = 1;
                        } else {
                            new_lines.push(*line);
                        }
                        if line.trim().starts_with("import ") { idx += 1; }
                    }
                    let cand = new_lines.join("\n");
                    let r = fitness(&cand, None, &train, min_trades);
                    let desc = format!("without import {}", i);
                    println!("  {}: train_r={:.3}", desc, r);
                    if r > best_r { best_r = r; best_dsl = cand; best_desc = desc; }
                }
                println!("best combo: {} (train_r={:.3})", best_desc, best_r);
                if let Some(v) = &val {
                    let (t, r) = run_candidate(&best_dsl, None, v);
                    println!("  val: trades={} total_r={:.3}", t, r);
                }
                if let Some(out_path) = out {
                    let _ = std::fs::write(&out_path, &best_dsl);
                    println!("wrote best DSL -> {}", out_path);
                }
            } else {
                // params mode: GA
                let nparams = param_names.len();
                if nparams == 0 {
                    eprintln!("evolve: no valid params parsed from --params");
                    std::process::exit(2);
                }
                println!("evolve(params): {} params, {} gens, pop {}", nparams, generations, pop);

                let mut rng = Rng::new(seed);

                // Generate a candidate DSL from a param vector
                // Initialize population
                let mut population: Vec<Vec<f64>> = Vec::with_capacity(pop);
                for _ in 0..pop {
                    let mut v = Vec::with_capacity(nparams);
                    for i in 0..nparams {
                        let x = rng.range(param_lo[i], param_hi[i]);
                        v.push(x);
                    }
                    population.push(v);
                }

                let mut best_vec: Vec<f64> = population[0].clone();
                let mut best_fit = f64::NEG_INFINITY;

                for g in 0..generations {
                    // Evaluate
                    let mut fits: Vec<f64> = Vec::with_capacity(population.len());
                    for cand in &population {
                        let f = fitness(&original_dsl, Some(cand), &train, min_trades);
                        fits.push(f);
                        if f > best_fit {
                            best_fit = f;
                            best_vec = cand.clone();
                        }
                    }
                    let gen_best = fits.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                    let gen_avg = fits.iter().sum::<f64>() / fits.len() as f64;
                    println!("gen {:3}: best={:.3} avg={:.3} (overall best={:.3})", g, gen_best, gen_avg, best_fit);

                    // Selection + crossover + mutation
                    let mut new_pop = Vec::with_capacity(pop);
                    // Elitism: keep best
                    new_pop.push(best_vec.clone());
                    while new_pop.len() < pop {
                        // Tournament selection
                        let mut pick = |rng: &mut Rng, pop: &Vec<Vec<f64>>, fits: &Vec<f64>| -> usize {
                            let a = rng.gen_range_usize(pop.len());
                            let b = rng.gen_range_usize(pop.len());
                            if fits[a] > fits[b] { a } else { b }
                        };
                        let i1 = pick(&mut rng, &population, &fits);
                        let i2 = pick(&mut rng, &population, &fits);
                        // Crossover
                        let mut child = Vec::with_capacity(nparams);
                        for k in 0..nparams {
                            if rng.gen_bool(0.5) { child.push(population[i1][k]); }
                            else { child.push(population[i2][k]); }
                        }
                        // Mutation
                        for k in 0..nparams {
                            if rng.gen_bool(0.3) {
                                let delta = (param_hi[k] - param_lo[k]) * 0.1 * (rng.f64() * 2.0 - 1.0);
                                child[k] = (child[k] + delta).clamp(param_lo[k], param_hi[k]);
                            }
                        }
                        new_pop.push(child);
                    }
                    population = new_pop;
                }

                println!("\nBest params:");
                for (i, name) in param_names.iter().enumerate() {
                    println!("  {} = {:.4}", name, best_vec[i]);
                }
                println!("Best train fitness: {:.3}", best_fit);
                // Build best DSL: apply params to original (if inline) else to lib
                let mut best_dsl = original_dsl.clone();
                // Try applying params to original_dsl
                let mut applied = false;
                for (i, name) in param_names.iter().enumerate() {
                    let val = best_vec[i];
                    let mut result = String::new();
                    let mut search_from = 0;
                    let needle = format!("{}:", name);
                    let mut pos = 0;
                    let bytes = best_dsl.as_bytes();
                    while pos < best_dsl.len() {
                        let prev_ok = pos == 0 || {
                            let b = best_dsl.as_bytes()[pos-1];
                            !(b.is_ascii_alphanumeric() || b == b'_')
                        };
                        if prev_ok && best_dsl[pos..].starts_with(&needle) {
                            let mut j = pos + needle.len();
                            while j < best_dsl.len() && (bytes[j] == b' ' || bytes[j] == b'\t') { j += 1; }
                            let start = j;
                            while j < best_dsl.len() && (bytes[j].is_ascii_digit() || bytes[j] == b'.' || bytes[j] == b'-') { j += 1; }
                            if j > start {
                                result.push_str(&best_dsl[search_from..start]);
                                result.push_str(&format!("{}", val));
                                search_from = j;
                                pos = j;
                                applied = true;
                                continue;
                            }
                        }
                        pos += 1;
                    }
                    result.push_str(&best_dsl[search_from..]);
                    best_dsl = result;
                }
                if let Some(v) = &val {
                    let (t, r) = run_candidate(&best_dsl, Some(&best_vec), v);
                    println!("Validation: trades={} total_r={:.3}", t, r);
                }
                if let Some(out_path) = out {
                    let _ = std::fs::write(&out_path, &best_dsl);
                    println!("wrote best DSL -> {}", out_path);
                    // Also write params as json
                    let params_json = format!("{{ {} }}",
                        param_names.iter().enumerate().map(|(i, n)| format!("\"{}\": {}", n, best_vec[i])).collect::<Vec<_>>().join(", "));
                    let params_path = format!("{}.params.json", out_path);
                    let _ = std::fs::write(&params_path, params_json);
                    println!("wrote params -> {}", params_path);
                    if !applied {
                        println!("note: params live in an imported lib; apply them to the lib file manually or via RELIANT_LIB.");
                    }
                }
            }
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
