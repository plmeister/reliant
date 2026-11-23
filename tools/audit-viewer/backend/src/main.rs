use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use clap::Parser;
use reliant_messaging::{read_audit, MessageBus};
use reliant_store::{CandleStore, SqliteCandleStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

#[derive(Parser)]
#[command(name = "dashboard-server", about = "Dashboard + audit viewer backend")]
struct Cli {
    #[arg(long, default_value_t = default_audit_dir())]
    audit_dir: String,
    #[arg(long, default_value_t = default_candles_db())]
    candle_db: String,
    #[arg(long, default_value_t = default_portfolios_root())]
    portfolios_root: String,
    #[arg(long, default_value = "8787")]
    port: u16,
}

#[derive(Clone)]
struct AppState {
    audit_dir: PathBuf,
    candle_db: String,
    portfolios_root: PathBuf,
}

// --- Audit file types (same as before) ---

#[derive(Serialize)]
struct FileInfo { name: String, size: u64, modified: i64 }

#[derive(Serialize)]
struct FileMeta {
    name: String, count: usize, topics: Vec<String>,
    types: HashMap<String, usize>, first_ts: Option<i64>, last_ts: Option<i64>,
}

#[derive(Serialize)]
struct MsgHeader { topic: String, timestamp: i64 }

#[derive(Serialize)]
struct Message { index: usize, header: MsgHeader, payload: Value }

#[derive(Serialize)]
struct MessagePage { total: usize, offset: usize, limit: usize, messages: Vec<Message> }

#[derive(Deserialize)]
struct MsgQuery { offset: Option<usize>, limit: Option<usize>, topic: Option<String>, q: Option<String> }

// --- Candle types ---

#[derive(Serialize)]
struct CandleOut {
    ts: i64, open: f64, high: f64, low: f64, close: f64, volume: f64,
}

// --- Portfolio types ---

#[derive(Serialize)]
struct PortfolioInfo { id: String, instruments: Vec<String>, trade_tf: String }

#[derive(Serialize)]
struct TradeOut {
    symbol: String, timeframe: String, direction: i8,
    entry: f64, stop: f64, target: f64,
    entry_ts: i64, exit_ts: i64, exit_price: f64,
    reason: String, pnl_r: f64, size: f64, status: String,
}

// --- Helpers ---

fn list_audit_files(dir: &FsPath) -> Vec<FileInfo> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if !(name.ends_with(".dat") || name.ends_with(".dat.zstd")) { continue; }
            if let Ok(md) = e.metadata() {
                if md.is_file() {
                    let modified = md.modified().ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64).unwrap_or(0);
                    out.push(FileInfo { name, size: md.len(), modified });
                }
            }
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn event_type(payload: &Value) -> String {
    if let Some(t) = payload.get("type").and_then(|v| v.as_str()) { return t.to_string(); }
    if let Some(obj) = payload.as_object() {
        if obj.len() == 1 { return obj.keys().next().cloned().unwrap_or_default(); }
    }
    "Unknown".to_string()
}

// --- Audit handlers (same as before) ---

async fn api_files(State(st): State<Arc<AppState>>) -> impl IntoResponse {
    Json(list_audit_files(&st.audit_dir))
}

async fn api_file_meta(State(st): State<Arc<AppState>>, Path(name): Path<String>) -> Result<Json<FileMeta>, (StatusCode, String)> {
    let path = st.audit_dir.join(&name);
    if !path.is_file() { return Err((StatusCode::NOT_FOUND, "file not found".into())); }
    let msgs = read_audit(path.to_str().unwrap()).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let mut topics: Vec<String> = msgs.iter().map(|m| m.header.get("topic").cloned().unwrap_or_default()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    topics.sort();
    let mut types: HashMap<String, usize> = HashMap::new();
    for m in &msgs { *types.entry(event_type(&m.payload)).or_insert(0) += 1; }
    let first_ts = msgs.first().and_then(|m| m.header.get("timestamp").and_then(|s| s.parse().ok()));
    let last_ts = msgs.last().and_then(|m| m.header.get("timestamp").and_then(|s| s.parse().ok()));
    Ok(Json(FileMeta { name, count: msgs.len(), topics, types, first_ts, last_ts }))
}

async fn api_messages(State(st): State<Arc<AppState>>, Path(name): Path<String>, Query(q): Query<MsgQuery>) -> Result<Json<MessagePage>, (StatusCode, String)> {
    let path = st.audit_dir.join(&name);
    if !path.is_file() { return Err((StatusCode::NOT_FOUND, "file not found".into())); }
    let msgs = read_audit(path.to_str().unwrap()).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let offset = q.offset.unwrap_or(0);
    let limit = q.limit.unwrap_or(200).min(1000);
    let topic_filter = q.topic.clone().unwrap_or_default();
    let search = q.q.clone().unwrap_or_default().to_lowercase();
    let filtered: Vec<(usize, &reliant_messaging::AuditMessage)> = msgs.iter().enumerate().filter(|(_, m)| {
        if !topic_filter.is_empty() {
            let t = m.header.get("topic").cloned().unwrap_or_default();
            if t != topic_filter { return false; }
        }
        if !search.is_empty() {
            let hay = format!("{} {}", serde_json::to_string(&m.payload).unwrap_or_default(), m.header.values().cloned().collect::<Vec<_>>().join(" ")).to_lowercase();
            if !hay.contains(&search) { return false; }
        }
        true
    }).collect();
    let total = filtered.len();
    let page: Vec<Message> = filtered.into_iter().skip(offset).take(limit).map(|(i, m)| {
        let topic = m.header.get("topic").cloned().unwrap_or_default();
        let ts = m.header.get("timestamp").and_then(|s| s.parse().ok()).unwrap_or(0);
        Message { index: i, header: MsgHeader { topic, timestamp: ts }, payload: m.payload.clone() }
    }).collect();
    Ok(Json(MessagePage { total, offset, limit, messages: page }))
}

async fn api_message_one(State(st): State<Arc<AppState>>, Path((name, idx)): Path<(String, usize)>) -> Result<Json<Message>, (StatusCode, String)> {
    let path = st.audit_dir.join(&name);
    if !path.is_file() { return Err((StatusCode::NOT_FOUND, "file not found".into())); }
    let msgs = read_audit(path.to_str().unwrap()).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if idx >= msgs.len() { return Err((StatusCode::NOT_FOUND, "index out of range".into())); }
    let m = &msgs[idx];
    let topic = m.header.get("topic").cloned().unwrap_or_default();
    let ts = m.header.get("timestamp").and_then(|s| s.parse().ok()).unwrap_or(0);
    Ok(Json(Message { index: idx, header: MsgHeader { topic, timestamp: ts }, payload: m.payload.clone() }))
}

// --- Candle endpoints ---

#[derive(Deserialize)]
struct CandleQuery { symbol: Option<String>, tf: Option<String>, from: Option<i64>, to: Option<i64>, limit: Option<usize> }

async fn api_instruments(State(st): State<Arc<AppState>>) -> impl IntoResponse {
    let store = match SqliteCandleStore::open_read_only(&st.candle_db) { Ok(s) => s, Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "db open failed").into_response() };
    let syms = store.symbols().unwrap_or_default();
    Json(syms).into_response()
}

async fn api_candles(State(st): State<Arc<AppState>>, Query(q): Query<CandleQuery>) -> impl IntoResponse {
    let symbol = q.symbol.clone().unwrap_or_default();
    let tf_str = q.tf.clone().unwrap_or_else(|| "1d".to_string());
    let tf = match reliant_datamodel::Timeframe::parse(&tf_str) {
        Some(t) => t,
        None => return (StatusCode::BAD_REQUEST, "bad timeframe").into_response(),
    };
    let store = match SqliteCandleStore::open_read_only(&st.candle_db) { Ok(s) => s, Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "db open failed").into_response() };
    let sym = reliant_datamodel::Symbol(symbol);
    let bars = store.range(&sym, &tf, q.from, q.to).unwrap_or_default();
    let limit = q.limit.unwrap_or(5000).min(50000);
    let out: Vec<CandleOut> = bars.iter().rev().take(limit).rev().map(|c| CandleOut {
        ts: c.timestamp.timestamp(), open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume,
    }).collect();
    Json(out).into_response()
}

// --- Portfolio endpoints ---

async fn api_portfolios(State(st): State<Arc<AppState>>) -> impl IntoResponse {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&st.portfolios_root) {
        for e in rd.flatten() {
            if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) { continue; }
            let id = e.file_name().to_string_lossy().to_string();
            if id.starts_with('.') { continue; }
            let pf_path = e.path().join("portfolio.json");
            if !pf_path.is_file() { continue; }
            if let Ok(raw) = std::fs::read_to_string(&pf_path) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                    let instruments = v.get("instruments").and_then(|x| x.as_array())
                        .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                        .unwrap_or_default();
                    let trade_tf = v.get("tradeTimeframe").and_then(|x| x.as_str()).unwrap_or("1d").to_string();
                    out.push(PortfolioInfo { id, instruments, trade_tf });
                }
            }
        }
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Json(out).into_response()
}

async fn api_portfolio_trades(State(st): State<Arc<AppState>>, Path(id): Path<String>) -> impl IntoResponse {
    // Read closed trades from trades.jsonl
    let mut trades = Vec::new();
    let trades_path = st.portfolios_root.join(&id).join("state").join("trades.jsonl");
    if let Ok(content) = std::fs::read_to_string(&trades_path) {
        for line in content.lines() {
            if line.trim().is_empty() { continue; }
            if let Ok(v) = serde_json::from_str::<Value>(line) {
                let status = "closed".to_string();
                trades.push(TradeOut {
                    symbol: v.get("symbol").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                    timeframe: v.get("timeframe").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                    direction: v.get("direction").and_then(|x| x.as_i64()).unwrap_or(0) as i8,
                    entry: v.get("entry").and_then(|x| x.as_f64()).unwrap_or(0.0),
                    stop: v.get("stop").and_then(|x| x.as_f64()).unwrap_or(0.0),
                    target: v.get("target").and_then(|x| x.as_f64()).unwrap_or(0.0),
                    entry_ts: v.get("entry_ts").and_then(|x| x.as_i64()).unwrap_or(0),
                    exit_ts: v.get("exit_ts").and_then(|x| x.as_i64()).unwrap_or(0),
                    exit_price: v.get("exit_price").and_then(|x| x.as_f64()).unwrap_or(0.0),
                    reason: v.get("reason").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                    pnl_r: v.get("pnl_r").and_then(|x| x.as_f64()).unwrap_or(0.0),
                    size: v.get("size").and_then(|x| x.as_f64()).unwrap_or(0.0),
                    status,
                });
            }
        }
    }
    // Read open trades from broker state snapshots
    let broker_db = st.portfolios_root.join(&id).join("state").join("state-broker.sqlite");
    if broker_db.is_file() {
        // Read all kv rows, parse broker state JSON
        // Use rusqlite via the store crate (already a dep)
        if let Ok(conn) = rusqlite::Connection::open(&broker_db) {
            let mut stmt = conn.prepare("SELECT key, value FROM kv").ok();
            if let Some(ref mut stmt) = stmt {
                if let Ok(rows) = stmt.query_map([], |row| {
                    let key: String = row.get(0)?;
                    let val: String = row.get(1)?;
                    Ok((key, val))
                }) {
                    for row in rows.flatten() {
                        let (key, val) = row;
                        if !key.ends_with(":broker:state") { continue; }
                        if let Ok(json) = serde_json::from_str::<Value>(&val) {
                            // json might be a string containing JSON
                            let obj = if let Some(s) = json.as_str() {
                                serde_json::from_str::<Value>(s).ok()
                            } else {
                                Some(json)
                            };
                            if let Some(o) = obj {
                                if o.get("open_trade").and_then(|x| x.as_bool()).unwrap_or(false) {
                                    let entry = o.get("trade_entry").and_then(|x| x.as_f64()).unwrap_or(0.0);
                                    let stop = o.get("trade_stop").and_then(|x| x.as_f64()).unwrap_or(0.0);
                                    let target = o.get("trade_target").and_then(|x| x.as_f64()).unwrap_or(0.0);
                                    let dir = o.get("trade_direction").and_then(|x| x.as_i64()).unwrap_or(0) as i8;
                                    let entry_ts = o.get("trade_entry_ts").and_then(|x| x.as_i64()).unwrap_or(0);
                                    // Extract symbol/tf from key: {id}:{symbol}:{tf}:broker:state
                                    let parts: Vec<&str> = key.split(':').collect();
                                    let symbol = if parts.len() >= 3 { parts[1].to_string() } else { "".to_string() };
                                    let timeframe = if parts.len() >= 3 { parts[2].to_string() } else { "".to_string() };
                                    trades.push(TradeOut {
                                        symbol, timeframe, direction: dir, entry, stop, target,
                                        entry_ts, exit_ts: 0, exit_price: 0.0,
                                        reason: "open".to_string(), pnl_r: 0.0, size: 0.0,
                                        status: "open".to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Json(trades).into_response()
}

// --- Timeline endpoint (indicators / swings / setups from an audit file) ---

#[derive(Deserialize)]
struct TimelineQuery { file: Option<String>, symbol: Option<String>, tf: Option<String> }

#[derive(Serialize)]
struct IndicatorPoint { ts: i64, value: f64 }
#[derive(Serialize)]
struct SwingPoint { ts: i64, confirm_ts: i64, kind: i8, price: f64, name: String, atr: f64 }
#[derive(Serialize)]
struct SetupPoint { ts: i64, direction: i8, entry: f64, stop: f64, target: f64, source: String }
#[derive(Serialize)]
struct TimelineOut {
    symbol: String, timeframe: String,
    indicators: HashMap<String, Vec<IndicatorPoint>>,
    swings: Vec<SwingPoint>,
    setups: Vec<SetupPoint>,
}

fn event_inner(payload: &Value) -> Value {
    if let Some(obj) = payload.as_object() {
        if obj.len() == 1 {
            if let Some(v) = obj.values().next() { return v.clone(); }
        }
    }
    payload.clone()
}

fn msg_ts(m: &reliant_messaging::AuditMessage) -> i64 {
    let hdr = m.header.get("timestamp").and_then(|s| s.parse().ok()).unwrap_or(0i64);
    if hdr != 0 { return hdr; }
    let p = &m.payload;
    if let Some(c) = p.get("candle") { if let Some(ts) = c.get("timestamp").and_then(|v| v.as_i64()) { return ts; } }
    if let Some(ts) = p.get("timestamp").and_then(|v| v.as_i64()) { return ts; }
    // IndicatorUpdated/SwingPivot carry no ts; fall back 0
    0
}

async fn api_timeline(State(st): State<Arc<AppState>>, Query(q): Query<TimelineQuery>) -> impl IntoResponse {
    let file = q.file.clone().unwrap_or_default();
    let symbol = q.symbol.clone().unwrap_or_default();
    let tf_str = q.tf.clone().unwrap_or_else(|| "1d".to_string());
    let path = st.audit_dir.join(&file);
    if !path.is_file() { return (StatusCode::NOT_FOUND, "file not found").into_response(); }
    let msgs = match read_audit(path.to_str().unwrap()) { Ok(m) => m, Err(e) => return (StatusCode::BAD_REQUEST, e).into_response() };

    let mut indicators: HashMap<String, Vec<IndicatorPoint>> = HashMap::new();
    let mut swings: Vec<SwingPoint> = Vec::new();
    let mut setups: Vec<SetupPoint> = Vec::new();

    for m in &msgs {
        let payload = &m.payload;
        let inner = event_inner(payload);
        let et = event_type(payload);
        let p_sym = inner.get("symbol").and_then(|v| v.as_str()).or_else(|| payload.get("symbol").and_then(|v| v.as_str())).unwrap_or("");
        let p_tf = inner.get("timeframe").and_then(|v| v.as_str()).or_else(|| payload.get("timeframe").and_then(|v| v.as_str())).unwrap_or("");
        if !symbol.is_empty() && !p_sym.is_empty() && p_sym != symbol { continue; }
        if !tf_str.is_empty() && !p_tf.is_empty() {
            let a = reliant_datamodel::Timeframe::parse(&tf_str);
            let b = reliant_datamodel::Timeframe::parse(p_tf);
            if a.is_some() && b.is_some() && a != b { continue; }
            if a.is_none() || b.is_none() { if p_tf != tf_str { continue; } }
        }

        let ts = msg_ts(m);
        match et.as_str() {
            "IndicatorUpdated" => {
                let name = inner.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let value = inner.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
                if !name.is_empty() {
                    indicators.entry(name).or_default().push(IndicatorPoint { ts, value });
                }
            }
            "SwingPivotEvent" => {
                let name = inner.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let kind = inner.get("kind").and_then(|v| v.as_i64()).unwrap_or(0) as i8;
                let price = inner.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let atr = inner.get("atr").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let pivot_ts = inner.get("index").and_then(|v| v.as_i64()).unwrap_or(ts);
                let confirm = inner.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(ts);
                let confirm = if confirm == 0 { ts } else { confirm };
                swings.push(SwingPoint { ts: pivot_ts, confirm_ts: confirm, kind, price, name, atr });
            }
            "SetupSignal" => {
                let direction = inner.get("direction").and_then(|v| v.as_i64()).unwrap_or(0) as i8;
                let entry = inner.get("entry").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let stop = inner.get("stop").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let target = inner.get("target").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let source = inner.get("source").and_then(|v| v.as_str()).unwrap_or("").to_string();
                setups.push(SetupPoint { ts, direction, entry, stop, target, source });
            }
            _ => {}
        }
    }
    Json(TimelineOut { symbol, timeframe: tf_str, indicators, swings, setups }).into_response()
}

// --- Indicator replay endpoint: re-runs the enricher graph over candles.db ---

#[derive(Deserialize)]
struct IndicatorsQuery { portfolio: Option<String>, symbol: Option<String>, tf: Option<String> }

#[derive(Serialize)]
struct IndPoint { ts: i64, value: f64 }
#[derive(Serialize)]
struct SwingPt { ts: i64, confirm_ts: i64, kind: i8, price: f64, name: String }
#[derive(Serialize)]
struct IndicatorsOut {
    symbol: String, timeframe: String,
    indicators: HashMap<String, Vec<IndPoint>>,
    swings: Vec<SwingPt>,
    setups: Vec<SetupPoint>,
}

async fn api_indicators(State(st): State<Arc<AppState>>, Query(q): Query<IndicatorsQuery>) -> impl IntoResponse {
    let portfolio_id = q.portfolio.clone().unwrap_or_default();
    let symbol = q.symbol.clone().unwrap_or_default();
    let tf_str = q.tf.clone().unwrap_or_else(|| "1d".to_string());
    if portfolio_id.is_empty() || symbol.is_empty() {
        return (StatusCode::BAD_REQUEST, "portfolio and symbol required").into_response();
    }
    let tf = match reliant_datamodel::Timeframe::parse(&tf_str) { Some(t) => t, None => return (StatusCode::BAD_REQUEST, "bad tf").into_response() };
    let pf_dir = st.portfolios_root.join(&portfolio_id);
    let pf_path = pf_dir.join("portfolio.json");
    let portfolio = match reliant_portfolio::Portfolio::from_file(&pf_path, &portfolio_id) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let dsl_path = pf_dir.join(&portfolio.strategy_file);
    let compiled = match reliant_dsl::compile_file(dsl_path.to_str().unwrap_or(""), &portfolio_id) { Ok(c) => c, Err(e) => return (StatusCode::BAD_REQUEST, format!("compile: {}", e)).into_response() };

    let store = match SqliteCandleStore::open_read_only(&st.candle_db) { Ok(s) => s, Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("db: {}", e)).into_response() };
    let sym = reliant_datamodel::Symbol(symbol.clone());
    let bars = store.range(&sym, &tf, None, None).unwrap_or_default();
    if bars.is_empty() { return (StatusCode::NOT_FOUND, "no candles").into_response(); }

    let options = reliant_dsl::BuildOptions { balance: portfolio.starting_balance, risk_pct: portfolio.risk_pct };
    let enricher_nodes = match reliant_dsl::build_role_nodes(&compiled, reliant_dsl::Role::Enricher, &sym, &tf, &options) {
        Ok(n) => n, Err(e) => return (StatusCode::BAD_REQUEST, format!("build enricher: {}", e)).into_response(),
    };
    let strategy_nodes = reliant_dsl::build_role_nodes(&compiled, reliant_dsl::Role::Strategy, &sym, &tf, &options).unwrap_or_default();

    let mut enricher_graph = reliant_engine::Graph::with_context("", enricher_nodes);
    let mut strategy_graph = reliant_engine::Graph::with_context("", strategy_nodes);
    let mut bus = reliant_messaging::InMemoryMessageBus::new();
    let strategy_rx = bus.subscribe("enrichment");

    let mut indicators: HashMap<String, Vec<IndPoint>> = HashMap::new();
    let mut swings: Vec<SwingPt> = Vec::new();
    let mut setups: Vec<SetupPoint> = Vec::new();

    for c in &bars {
        let ev = reliant_datamodel::Event::CandleClosed { symbol: sym.clone(), timeframe: tf.clone(), candle: c.clone() };
        let out = enricher_graph.process("ingress", &ev, &mut bus);
        for (_t, e) in &out {
            match e {
                reliant_datamodel::Event::IndicatorUpdated { name, value, .. } => {
                    indicators.entry(name.clone()).or_default().push(IndPoint { ts: c.timestamp.timestamp(), value: *value });
                }
                reliant_datamodel::Event::SwingPivotEvent { name, kind, price, index, timestamp, .. } => {
                    let confirm = if *timestamp == 0 { c.timestamp.timestamp() } else { *timestamp };
                    swings.push(SwingPt { ts: *index, confirm_ts: confirm, kind: *kind, price: *price, name: name.clone() });
                }
                _ => {}
            }
        }
        // forward enrichment messages to strategy
        while let Ok(e) = strategy_rx.try_recv() {
            let out = strategy_graph.process("enrichment", &e, &mut bus);
            for (_t, ev2) in &out {
                if let reliant_datamodel::Event::SetupSignal { direction, entry, stop, target, source, .. } = ev2 {
                    setups.push(SetupPoint { ts: c.timestamp.timestamp(), direction: *direction, entry: *entry, stop: *stop, target: *target, source: source.clone() });
                }
            }
        }
    }

    Json(IndicatorsOut { symbol, timeframe: tf_str, indicators, swings, setups }).into_response()
}

async fn app_js() -> impl IntoResponse {
    ([("content-type", "application/javascript"), ("cache-control", "no-store")], include_str!("../../frontend/app.js"))
}
async fn style_css() -> impl IntoResponse {
    ([("content-type", "text/css"), ("cache-control", "no-store")], include_str!("../../frontend/style.css"))
}
async fn lc_js() -> impl IntoResponse {
    ([("content-type", "application/javascript"), ("cache-control", "no-store")], include_str!("../../frontend/lib/lightweight-charts.js"))
}
async fn index_html() -> impl IntoResponse {
    ([("content-type", "text/html"), ("cache-control", "no-store")], include_str!("../../frontend/index.html"))
}

fn default_data_dir() -> String {
    if let Ok(x) = std::env::var("XDG_DATA_HOME") {
        if !x.is_empty() { return format!("{}/reliant", x); }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/.local/share/reliant", home)
}
fn default_config_dir() -> String {
    if let Ok(x) = std::env::var("XDG_CONFIG_HOME") {
        if !x.is_empty() { return format!("{}/reliant", x); }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/.config/reliant", home)
}
fn default_candles_db() -> String { format!("{}/candles.db", default_data_dir()) }
fn default_portfolios_root() -> String { format!("{}/portfolios", default_config_dir()) }

fn default_audit_dir() -> String {
    if let Ok(x) = std::env::var("XDG_STATE_HOME") {
        if !x.is_empty() { return format!("{}/reliant/audit", x); }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/.local/state/reliant/audit", home)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let state = Arc::new(AppState {
        audit_dir: PathBuf::from(&cli.audit_dir),
        candle_db: cli.candle_db.clone(),
        portfolios_root: PathBuf::from(&cli.portfolios_root),
    });

    let app = Router::new()
        .route("/api/files", get(api_files))
        .route("/api/files/{name}/meta", get(api_file_meta))
        .route("/api/files/{name}/messages", get(api_messages))
        .route("/api/files/{name}/messages/{idx}", get(api_message_one))
        .route("/api/instruments", get(api_instruments))
        .route("/api/candles", get(api_candles))
        .route("/api/timeline", get(api_timeline))
        .route("/api/indicators", get(api_indicators))
        .route("/api/portfolios", get(api_portfolios))
        .route("/api/portfolios/{id}/trades", get(api_portfolio_trades))
        .route("/", get(index_html))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/lib/lightweight-charts.js", get(lc_js))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cli.port);
    println!("dashboard-server listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
