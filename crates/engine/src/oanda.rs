// ---- OandaBroker: live broker via OANDA v20 REST (practice or live) ----
//
// Credentials come from the environment:
//   OANDA_ACCOUNT_ID    - account id (e.g. 101-001-12345678-001)
//   OANDA_ACCESS_TOKEN  - bearer token
//   OANDA_BASE_URL      - optional, defaults to practice endpoint
//   OANDA_DRY_RUN       - if "1", log orders instead of placing them
//
// The node mirrors DummyBroker's state machine but delegates fills/exits to
// OANDA. State is held in-process (v1: not persisted across restarts).

use crate::context::MarketContext;
use crate::graph::Node;
use reliant_datamodel::{Event, Symbol, Timeframe};
use serde_json::Value;
use std::env;

pub struct OandaBroker {
    pub id: String,
    pub symbol: Symbol,
    pub timeframe: Timeframe,
    pub account_id: String,
    pub access_token: String,
    pub base_url: String,
    pub dry_run: bool,
    pub client: reqwest::blocking::Client,
    pub entry_mode: String,

    // tracked position state
    pub open_trade_id: Option<String>,
    pub pending_order_id: Option<String>,
    pub entry: f64,
    pub stop: f64,
    pub target: f64,
    pub direction: i8,
    pub max_hold_bars: usize,
    pub bars_held: usize,
    pub entry_ts: i64,
    pub size: f64,
    pub source: String,
    pub pending_bars: usize,
}

impl OandaBroker {
    pub fn new(id: &str, symbol: Symbol, timeframe: Timeframe, entry_mode: String) -> Self {
        let account_id = env::var("OANDA_ACCOUNT_ID").unwrap_or_default();
        let access_token = env::var("OANDA_ACCESS_TOKEN").unwrap_or_default();
        let base_url = env::var("OANDA_BASE_URL")
            .unwrap_or_else(|_| "https://api-fxpractice.oanda.com".to_string());
        let dry_run = env::var("OANDA_DRY_RUN").map(|v| v == "1").unwrap_or(false);
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::blocking::Client::new());
        Self {
            id: id.to_string(),
            symbol,
            timeframe,
            account_id,
            access_token,
            base_url,
            dry_run,
            client,
            entry_mode,
            open_trade_id: None,
            pending_order_id: None,
            entry: 0.0,
            stop: 0.0,
            target: 0.0,
            direction: 0,
            max_hold_bars: 0,
            bars_held: 0,
            entry_ts: 0,
            size: 0.0,
            source: String::new(),
            pending_bars: 0,
        }
    }

    fn instrument(&self) -> String {
        let s = self.symbol.0.as_str();
        if s.len() == 6 && s.chars().all(|c| c.is_ascii_uppercase()) {
            format!("{}_{}", &s[..3], &s[3..])
        } else {
            s.to_string()
        }
    }

    fn creds_ok(&self) -> bool {
        !self.account_id.is_empty() && !self.access_token.is_empty()
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.access_token)
    }

    fn place_market_order(&mut self) -> Option<String> {
        if !self.creds_ok() {
            eprintln!("oanda: missing credentials (OANDA_ACCOUNT_ID / OANDA_ACCESS_TOKEN)");
            return None;
        }
        let units = if self.direction > 0 { self.size.round() as i64 } else { -(self.size.round() as i64) };
        let units = if units == 0 { if self.direction > 0 { 1 } else { -1 } } else { units };
        let body = serde_json::json!({
            "order": {
                "type": "MARKET",
                "instrument": self.instrument(),
                "units": units.to_string(),
                "timeInForce": "FOK",
                "stopLossOnFill": { "price": format!("{:.5}", self.stop), "timeInForce": "GTC" },
                "takeProfitOnFill": { "price": format!("{:.5}", self.target), "timeInForce": "GTC" },
            }
        });
        if self.dry_run {
            eprintln!("oanda DRY RUN market order: {}", body);
            // simulate a trade id
            let id = format!("dry-{}", chrono::Utc::now().timestamp_millis());
            return Some(id);
        }
        let url = format!("{}/v3/accounts/{}/orders", self.base_url, self.account_id);
        let resp = self.client.post(&url)
            .header("Authorization", self.auth_header())
            .json(&body)
            .send();
        match resp {
            Ok(r) => {
                let status = r.status();
                let text = r.text().unwrap_or_default();
                if !status.is_success() {
                    eprintln!("oanda: market order failed {}: {}", status, text);
                    return None;
                }
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                // orderFillTransaction.tradeOpened.tradeID
                let tid = v.get("orderFillTransaction")
                    .and_then(|f| f.get("tradeOpened"))
                    .and_then(|t| t.get("tradeID"))
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string());
                if tid.is_none() {
                    eprintln!("oanda: no tradeID in response: {}", text);
                }
                tid
            }
            Err(e) => {
                eprintln!("oanda: request error: {}", e);
                None
            }
        }
    }

    fn place_limit_order(&mut self) -> Option<String> {
        if !self.creds_ok() {
            eprintln!("oanda: missing credentials");
            return None;
        }
        let units = if self.direction > 0 { self.size.round() as i64 } else { -(self.size.round() as i64) };
        let units = if units == 0 { if self.direction > 0 { 1 } else { -1 } } else { units };
        let expiry = chrono::Utc::now() + chrono::Duration::seconds(self.max_hold_bars as i64 * self.timeframe.bar_seconds() as i64);
        let gtd = expiry.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let body = serde_json::json!({
            "order": {
                "type": "LIMIT",
                "instrument": self.instrument(),
                "units": units.to_string(),
                "price": format!("{:.5}", self.entry),
                "timeInForce": "GTD",
                "gtdTime": gtd,
                "stopLossOnFill": { "price": format!("{:.5}", self.stop), "timeInForce": "GTC" },
                "takeProfitOnFill": { "price": format!("{:.5}", self.target), "timeInForce": "GTC" },
            }
        });
        if self.dry_run {
            eprintln!("oanda DRY RUN limit order: {}", body);
            let id = format!("dry-{}", chrono::Utc::now().timestamp_millis());
            return Some(id);
        }
        let url = format!("{}/v3/accounts/{}/orders", self.base_url, self.account_id);
        let resp = self.client.post(&url)
            .header("Authorization", self.auth_header())
            .json(&body)
            .send();
        match resp {
            Ok(r) => {
                let status = r.status();
                let text = r.text().unwrap_or_default();
                if !status.is_success() {
                    eprintln!("oanda: limit order failed {}: {}", status, text);
                    return None;
                }
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                // orderCreated.orderID (limit order resting) OR orderFillTransaction.tradeOpened.tradeID (immediate fill)
                let oid = v.get("orderCreateTransaction").and_then(|t| t.get("orderID")).and_then(|x| x.as_str())
                    .or_else(|| v.get("orderFillTransaction").and_then(|f| f.get("orderID")).and_then(|x| x.as_str()))
                    .map(|s| s.to_string());
                // if immediately filled, capture trade id too
                if let Some(tid) = v.get("orderFillTransaction").and_then(|f| f.get("tradeOpened")).and_then(|t| t.get("tradeID")).and_then(|x| x.as_str()) {
                    self.open_trade_id = Some(tid.to_string());
                }
                oid
            }
            Err(e) => {
                eprintln!("oanda: request error: {}", e);
                None
            }
        }
    }

    fn get_trade(&self, trade_id: &str) -> Option<Value> {
        if !self.creds_ok() || self.dry_run { return None; }
        let url = format!("{}/v3/accounts/{}/trades/{}", self.base_url, self.account_id, trade_id);
        let resp = self.client.get(&url).header("Authorization", self.auth_header()).send();
        match resp {
            Ok(r) => {
                if r.status() == reqwest::StatusCode::NOT_FOUND { return None; }
                let text = r.text().unwrap_or_default();
                serde_json::from_str(&text).ok()
            }
            Err(_) => None,
        }
    }

    fn close_trade(&mut self, trade_id: &str) {
        if !self.creds_ok() || self.dry_run { return; }
        let url = format!("{}/v3/accounts/{}/trades/{}/close", self.base_url, self.account_id, trade_id);
        let body = serde_json::json!({ "units": "ALL" });
        let _ = self.client.put(&url).header("Authorization", self.auth_header()).json(&body).send();
    }

    fn cancel_order(&mut self, order_id: &str) {
        if !self.creds_ok() || self.dry_run { return; }
        let url = format!("{}/v3/accounts/{}/orders/{}", self.base_url, self.account_id, order_id);
        let _ = self.client.delete(&url).header("Authorization", self.auth_header()).send();
    }

    fn emit_result(&self, exit_price: f64, reason: &str, exit_ts: i64) -> Event {
        let r_dist = (self.entry - self.stop).abs();
        let pnl_r = if r_dist > 0.0 { (self.direction as f64) * (exit_price - self.entry) / r_dist } else { 0.0 };
        Event::TradeResult {
            symbol: Some(self.symbol.0.clone()),
            timeframe: Some(self.timeframe.as_str().to_string()),
            direction: self.direction,
            entry: self.entry,
            stop: self.stop,
            target: self.target,
            entry_ts: self.entry_ts,
            exit_ts,
            exit_price,
            reason: reason.to_string(),
            pnl_r,
            size: self.size,
            source: self.source.clone(),
        }
    }
}

impl Node for OandaBroker {
    fn id(&self) -> &str { &self.id }
    fn subscriptions(&self) -> &[&'static str] { &["ingress", "trade"] }

    fn on_event(&mut self, topic: &str, event: &Event, _ctx: &mut MarketContext) -> Vec<(String, Event)> {
        let mut out: Vec<(String, Event)> = Vec::new();
        match event {
            Event::TradeSignal { symbol, timeframe, direction, entry, stop, target, max_hold_secs, size, timestamp, source, .. } => {
                if symbol != &self.symbol || timeframe != &self.timeframe || topic != "trade" {
                    return out;
                }
                if self.open_trade_id.is_some() || self.pending_order_id.is_some() {
                    return out; // busy
                }
                self.direction = *direction;
                self.entry = *entry;
                self.stop = *stop;
                self.target = *target;
                self.size = *size;
                self.source = source.clone();
                self.max_hold_bars = (*max_hold_secs / timeframe.bar_seconds() as f64).ceil().max(1.0) as usize;
                self.entry_ts = if *timestamp != 0 { *timestamp } else { 0 };
                self.bars_held = 0;
                self.pending_bars = 0;

                if self.entry_mode == "immediate" || self.entry_mode == "market" {
                    if let Some(tid) = self.place_market_order() {
                        self.open_trade_id = Some(tid);
                    }
                } else {
                    if let Some(oid) = self.place_limit_order() {
                        // if immediately filled, open_trade_id already set
                        if self.open_trade_id.is_none() {
                            self.pending_order_id = Some(oid);
                        }
                    }
                }
                out
            }
            Event::CandleClosed { symbol, timeframe, candle } => {
                if symbol != &self.symbol || timeframe != &self.timeframe || topic != "ingress" {
                    return out;
                }
                // Open position management.
                if let Some(tid) = self.open_trade_id.clone() {
                    self.bars_held += 1;
                    // timeout?
                    if self.bars_held >= self.max_hold_bars {
                        self.close_trade(&tid);
                        let exit_price = candle.close;
                        let ev = self.emit_result(exit_price, "timeout", candle.timestamp.timestamp());
                        out.push(("trade".to_string(), ev));
                        self.open_trade_id = None;
                        return out;
                    }
                    // check if closed by SL/TP
                    if !self.dry_run {
                        if let Some(v) = self.get_trade(&tid) {
                            let state = v.get("trade").and_then(|t| t.get("state")).and_then(|s| s.as_str()).unwrap_or("OPEN");
                            if state != "OPEN" {
                                // closed
                                let exit_price = v.get("trade").and_then(|t| t.get("price")).and_then(|x| x.as_f64())
                                    .or_else(|| v.get("trade").and_then(|t| t.get("averageClosePrice")).and_then(|x| x.as_f64()))
                                    .unwrap_or(candle.close);
                                // reason unknown; default target/stop detection
                                let reason = if (exit_price - self.target).abs() < (exit_price - self.stop).abs() { "target" } else { "stop" };
                                let ev = self.emit_result(exit_price, reason, candle.timestamp.timestamp());
                                out.push(("trade".to_string(), ev));
                                self.open_trade_id = None;
                            }
                        } else {
                            // trade not found -> closed
                            let exit_price = candle.close;
                            let ev = self.emit_result(exit_price, "stop", candle.timestamp.timestamp());
                            out.push(("trade".to_string(), ev));
                            self.open_trade_id = None;
                        }
                    }
                    return out;
                }
                // Pending limit order management.
                if let Some(oid) = self.pending_order_id.clone() {
                    self.pending_bars += 1;
                    if self.pending_bars >= self.max_hold_bars {
                        self.cancel_order(&oid);
                        self.pending_order_id = None;
                        return out;
                    }
                    if !self.dry_run {
                        // check if filled: try get order; if 404, maybe filled
                        let url = format!("{}/v3/accounts/{}/orders/{}", self.base_url, self.account_id, oid);
                        let resp = self.client.get(&url).header("Authorization", self.auth_header()).send();
                        let filled = match resp {
                            Ok(r) => {
                                if r.status() == reqwest::StatusCode::NOT_FOUND {
                                    // order gone; check if a trade opened
                                    true
                                } else {
                                    false
                                }
                            }
                            Err(_) => false,
                        };
                        if filled {
                            // find open trade for this instrument
                            // best-effort: list open trades
                            let url = format!("{}/v3/accounts/{}/openTrades", self.base_url, self.account_id);
                            if let Ok(r) = self.client.get(&url).header("Authorization", self.auth_header()).send() {
                                let text = r.text().unwrap_or_default();
                                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                    if let Some(trades) = v.get("trades").and_then(|t| t.as_array()) {
                                        for t in trades {
                                            if t.get("instrument").and_then(|x| x.as_str()) == Some(self.instrument().as_str()) {
                                                if let Some(tid) = t.get("id").and_then(|x| x.as_str()) {
                                                    self.open_trade_id = Some(tid.to_string());
                                                    self.pending_order_id = None;
                                                    self.bars_held = 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // dry run: simulate fill on touch
                        let touched = if self.direction > 0 { candle.low <= self.entry } else { candle.high >= self.entry };
                        if touched {
                            let fill = self.entry;
                            self.entry = fill;
                            let id = format!("dry-{}", chrono::Utc::now().timestamp_millis());
                            self.open_trade_id = Some(id);
                            self.pending_order_id = None;
                            self.bars_held = 1;
                        } else {
                            let _ = candle;
                        }
                    }
                }
                out
            }
            _ => out,
        }
    }
}
