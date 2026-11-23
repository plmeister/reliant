pub mod supervisor;

use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_engine::{Graph, Node};
use reliant_messaging::MessageBus;
use reliant_store::StateStore;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use std::path::PathBuf;

/// XDG config directory for reliant: ~/.config/reliant (or $XDG_CONFIG_HOME/reliant).
pub fn config_dir() -> PathBuf {
    if let Ok(x) = std::env::var("XDG_CONFIG_HOME") {
        if !x.is_empty() { return PathBuf::from(x).join("reliant"); }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".config").join("reliant")
}

/// XDG data directory: ~/.local/share/reliant (or $XDG_DATA_HOME/reliant).
pub fn data_dir() -> PathBuf {
    if let Ok(x) = std::env::var("XDG_DATA_HOME") {
        if !x.is_empty() { return PathBuf::from(x).join("reliant"); }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".local").join("share").join("reliant")
}

/// XDG state directory: ~/.local/state/reliant (or $XDG_STATE_HOME/reliant).
pub fn state_dir() -> PathBuf {
    if let Ok(x) = std::env::var("XDG_STATE_HOME") {
        if !x.is_empty() { return PathBuf::from(x).join("reliant"); }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".local").join("state").join("reliant")
}

pub fn portfolios_root() -> PathBuf { config_dir().join("portfolios") }
pub fn candles_db() -> PathBuf { data_dir().join("candles.db") }
pub fn state_db() -> PathBuf { state_dir().join("state.sqlite") }
pub fn audit_dir() -> PathBuf { state_dir().join("audit") }

pub fn parse_timeframe(s: &str) -> Timeframe {
    match s {
        "1m" | "m1" => Timeframe::M1,
        "5m" | "m5" => Timeframe::M5,
        "1h" | "h1" => Timeframe::H1,
        "1d" | "d1" => Timeframe::D1,
        "1w" | "w1" => Timeframe::W1,
        _ => Timeframe::D1,
    }
}

pub fn price_scale(sym: &str) -> f64 {
    if sym.ends_with("JPY") { 1000.0 } else { 100000.0 }
}

pub fn context_subject(context: &str, stage: &str) -> String {
    reliant_messaging::subject(context, stage)
}

pub fn collect_trade_results(emitted: &[Event]) -> Vec<(f64, String)> {
    emitted.iter().filter_map(|e| if let Event::TradeResult { pnl_r, reason, .. } = e {
        Some((*pnl_r, reason.clone()))
    } else { None }).collect()
}

pub fn load_portfolio(path: &str) -> Vec<String> {
    if let Ok(raw) = std::fs::read_to_string(path) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
            if let Some(arr) = v.get("instruments").and_then(|x| x.as_array()) {
                return arr.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect();
            }
        }
    }
    Vec::new()
}

/// Load and compile a portfolio's strategy DSL.
pub fn load_strategy(
    path: &str,
    portfolio: &str,
) -> Result<reliant_dsl::Compiled, String> {
    reliant_dsl::compile_file(path, portfolio)
        .map_err(|e| format!("strategy {}: {}", path, e))
}

/// Build the boxes owned by one component from a compiled strategy.
pub fn build_component_nodes(
    compiled: &reliant_dsl::Compiled,
    role: reliant_dsl::Role,
    instrument: &str,
    tf: &Timeframe,
    balance: f64,
    risk_pct: f64,
) -> Result<Vec<Box<dyn Node>>, String> {
    reliant_dsl::build_role_nodes(
        compiled,
        role,
        &Symbol(instrument.to_string()),
        tf,
        &reliant_dsl::BuildOptions { balance, risk_pct },
    )
}

pub fn run_over_candles(
    context: &str,
    symbols: &[String],
    tf: &Timeframe,
    tf_str: &str,
    candles_by_sym: &mut std::collections::HashMap<String, Vec<reliant_datamodel::Candle>>,
    graph: &mut Graph,
    bus: &mut dyn MessageBus,
) -> Vec<Event> {
    let _ = (context, symbols, tf, tf_str, candles_by_sym);
    let _ = graph;
    let _ = bus;
    Vec::new()
}

/// Restore a component's own context snapshot. The key scopes the row to
/// `{context}:{instrument}:{timeframe}:{role}:state`, so several instruments and
/// several components can share one state file without overwriting each other.
pub fn restore_context_state(
    store: &mut reliant_store::SqliteStateStore,
    key: &str,
    graph: &mut Graph,
    instrument: &Symbol,
    timeframe: &Timeframe,
) {
    match store.load(key) {
        Ok(Some(json)) => {
            if let Some(ctx) = graph
                .context()
                .contexts
                .get_mut(&(instrument.clone(), timeframe.clone()))
            {
                ctx.load_json(&json);
            }
        }
        Ok(None) => {}
        Err(e) => eprintln!("state: failed to load {}: {}", key, e),
    }
}

/// Publishes throttled context snapshots onto the global `state` subject.
///
/// Components do not write the state database: a single `state-writer` does,
/// so no process ever blocks on a SQLite write lock in its event loop. The
/// snapshot carries a version (last applied bar) so a stale publish arriving
/// late cannot roll the stored state backwards.
pub struct StateCheckpoint {
    role: String,
    context: String,
    instrument: Symbol,
    timeframe: Timeframe,
    key: String,
    interval: std::time::Duration,
    last: Option<std::time::Instant>,
}

impl StateCheckpoint {
    pub fn new(role: &str, context: &str, instrument: &Symbol, timeframe: &Timeframe) -> Self {
        Self {
            role: role.to_string(),
            context: context.to_string(),
            instrument: instrument.clone(),
            timeframe: timeframe.clone(),
            key: format!(
                "{}:{}:{}:{}:state",
                context,
                instrument.0,
                timeframe.as_str(),
                role
            ),
            interval: std::time::Duration::from_secs(2),
            last: None,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn maybe_publish(&mut self, bus: &mut dyn MessageBus, graph: &mut Graph) {
        let due = self.last.map_or(true, |t| t.elapsed() >= self.interval);
        if due {
            self.publish(bus, graph);
        }
    }

    pub fn publish(&mut self, bus: &mut dyn MessageBus, graph: &mut Graph) {
        if let Some(ev) = self.snapshot_event(graph) {
            bus.publish(reliant_messaging::STATE_SUBJECT, ev);
        }
    }

    /// Build the snapshot event without publishing it. Multi-portfolio
    /// components hand it to their single publisher thread instead of touching
    /// the bus from a worker.
    pub fn snapshot_event(&mut self, graph: &mut Graph) -> Option<Event> {
        let ctx = graph
            .context()
            .contexts
            .get(&(self.instrument.clone(), self.timeframe.clone()))?;
        let ev = Event::ContextState {
            role: self.role.clone(),
            context: self.context.clone(),
            instrument: self.instrument.0.clone(),
            timeframe: self.timeframe.as_str().to_string(),
            version: ctx.latest.unwrap_or(0),
            state: ctx.state_json(),
        };
        self.last = Some(std::time::Instant::now());
        Some(ev)
    }
}

pub fn build_backtest_nodes(syms: &[&str], tf: Timeframe, k: f64, balance: f64, risk: f64) -> Vec<Box<dyn Node>> {
    let mut nodes: Vec<Box<dyn Node>> = Vec::new();
    for sym in syms {
        let symbol = Symbol(sym.to_string());
        nodes.push(Box::new(reliant_engine::CandleSource::new(&format!("cs_{}", sym), symbol.clone(), tf.clone(), vec![])));
        nodes.push(Box::new(reliant_engine::AtrNode::new(&format!("atr_{}", sym), symbol.clone(), tf.clone(), 14)));
        nodes.push(Box::new(reliant_engine::EmaNode::new(&format!("ema_{}", sym), symbol.clone(), tf.clone(), 20)));
        nodes.push(Box::new(reliant_engine::SwingNode::new(&format!("swing_{}", sym), symbol.clone(), tf.clone(), k, 5)));
        nodes.push(Box::new(reliant_engine::DummyBroker::new(&format!("broker_{}", sym), symbol.clone(), tf.clone())));
    }
    nodes.push(Box::new(reliant_engine::StrategyNode::new("strat", k)));
    nodes.push(Box::new(reliant_engine::GeometryNode::new("geom", balance, risk)));
    nodes
}

pub fn stage_for_event(e: &Event) -> &'static str {
    reliant_messaging::stage_for_event(e)
}

static mut SHUTDOWN_PTR: *const AtomicBool = std::ptr::null();

extern "C" fn term_handler(_sig: libc::c_int) {
    unsafe {
        if !SHUTDOWN_PTR.is_null() {
            (*SHUTDOWN_PTR).store(true, Ordering::SeqCst);
        }
    }
}

pub fn shutdown_flag() -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    unsafe { SHUTDOWN_PTR = Arc::as_ptr(&flag); }
    let f = flag.clone();
    let _ = ctrlc::set_handler(move || {
        f.store(true, Ordering::SeqCst);
    });
    unsafe {
        libc::signal(libc::SIGTERM, term_handler as libc::sighandler_t);
    }
    flag
}
