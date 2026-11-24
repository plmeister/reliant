use reliant_datamodel::{Candle, Event, Processor, Symbol, Timeframe};
use std::collections::{HashMap, HashSet};

pub struct Context {
    pub instrument: Symbol,
    pub timeframe: Timeframe,
    pub latest: Option<i64>,
    pub processors: HashMap<String, Box<dyn Processor>>,
    /// Latest value of every indicator fact this context has seen, keyed by the
    /// producer's DSL name. Strategy boxes read their wired slots from here.
    pub indicators: HashMap<String, f64>,
    pub open_trade: bool,
    pub trade_entry: Option<f64>,
    pub trade_stop: Option<f64>,
    pub trade_target: Option<f64>,
    pub trade_direction: Option<i8>,
    pub trade_max_hold: Option<usize>,
    pub trade_bars: usize,
    pub trade_entry_ts: Option<i64>,
    pub trade_size: Option<f64>,
    pub trade_source: Option<String>,
    /// Resting limit order (pivot retest) awaiting a fill.
    pub trade_pending: bool,
    pub trade_pending_market: bool,
    pub trade_pending_entry: Option<f64>,
    pub trade_pending_stop: Option<f64>,
    pub trade_pending_target: Option<f64>,
    pub trade_pending_direction: Option<i8>,
    pub trade_pending_max_hold: Option<usize>,
    pub trade_pending_size: Option<f64>,
    pub trade_pending_entry_ts: Option<i64>,
    pub trade_pending_valid: usize,
    /// Setup signals already acted on by the broker (durable dedupe key).
    pub seen_setups: HashSet<String>,
    /// Cached enricher output for the last processed bar, used for durable replay.
    pub last_enrichment: Option<Vec<(String, Event)>>,
}

impl Context {
    pub fn new(instrument: Symbol, timeframe: Timeframe) -> Self {
        Self { instrument, timeframe, latest: None, processors: HashMap::new(), indicators: HashMap::new(), open_trade: false,
               trade_entry: None, trade_stop: None, trade_target: None, trade_direction: None,
               trade_max_hold: None, trade_bars: 0, trade_entry_ts: None, trade_size: None, trade_source: None,
               trade_pending: false, trade_pending_market: false, trade_pending_entry: None, trade_pending_stop: None,
               trade_pending_target: None, trade_pending_direction: None, trade_pending_max_hold: None,
               trade_pending_size: None, trade_pending_entry_ts: None, trade_pending_valid: 0,
               seen_setups: HashSet::new(), last_enrichment: None }
    }

    pub fn register(&mut self, processor: Box<dyn Processor>) {
        self.processors.insert(processor.name().to_string(), processor);
    }

    pub fn apply(&mut self, candle: &Candle) {
        for p in self.processors.values_mut() {
            p.apply(candle);
        }
        self.latest = Some(candle.timestamp.timestamp());
    }

    pub fn trade_open(&self) -> bool { self.open_trade }
    pub fn set_trade_open(&mut self, open: bool) { self.open_trade = open; }

    pub fn state_json(&self) -> String {
        let mut map = serde_json::Map::new();
        map.insert("latest".to_string(), serde_json::json!(self.latest));
        for (name, p) in &self.processors {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&p.state_json()) {
                map.insert(name.clone(), v);
            }
        }
        map.insert("open_trade".to_string(), serde_json::Value::Bool(self.open_trade));
        if !self.indicators.is_empty() {
            map.insert(
                "indicators".to_string(),
                serde_json::json!(self.indicators),
            );
        }
        if let Some(v) = self.trade_entry { map.insert("trade_entry".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_stop { map.insert("trade_stop".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_target { map.insert("trade_target".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_direction { map.insert("trade_direction".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_max_hold { map.insert("trade_max_hold".to_string(), serde_json::json!(v)); }
        map.insert("trade_bars".to_string(), serde_json::json!(self.trade_bars));
        if let Some(v) = self.trade_entry_ts { map.insert("trade_entry_ts".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_size { map.insert("trade_size".to_string(), serde_json::json!(v)); }
        if let Some(v) = &self.trade_source { map.insert("trade_source".to_string(), serde_json::json!(v)); }
        map.insert("trade_pending".to_string(), serde_json::Value::Bool(self.trade_pending));
        map.insert("trade_pending_market".to_string(), serde_json::Value::Bool(self.trade_pending_market));
        if let Some(v) = self.trade_pending_entry { map.insert("trade_pending_entry".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_pending_stop { map.insert("trade_pending_stop".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_pending_target { map.insert("trade_pending_target".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_pending_direction { map.insert("trade_pending_direction".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_pending_max_hold { map.insert("trade_pending_max_hold".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_pending_size { map.insert("trade_pending_size".to_string(), serde_json::json!(v)); }
        if let Some(v) = self.trade_pending_entry_ts { map.insert("trade_pending_entry_ts".to_string(), serde_json::json!(v)); }
        map.insert("trade_pending_valid".to_string(), serde_json::json!(self.trade_pending_valid));
        map.insert("seen_setups".to_string(), serde_json::json!(self.seen_setups));
        if let Some(outputs) = &self.last_enrichment {
            let arr: Vec<serde_json::Value> = outputs
                .iter()
                .map(|(topic, event)| serde_json::json!({"topic": topic, "event": event}))
                .collect();
            map.insert("last_enrichment".to_string(), serde_json::json!(arr));
        }
        serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
    }

    pub fn load_json(&mut self, json: &str) {
        if let Ok(obj) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json) {
            for (name, v) in obj {
                if name == "latest" { if let Some(t) = v.as_i64() { self.latest = Some(t); } continue; }
                if name == "open_trade" { if let Some(b) = v.as_bool() { self.open_trade = b; } continue; }
                if name == "indicators" {
                    if let Some(values) = v.as_object() {
                        for (key, value) in values {
                            if let Some(f) = value.as_f64() {
                                self.indicators.insert(key.clone(), f);
                            }
                        }
                    }
                    continue;
                }
                // Legacy snapshots (pre-DSL) stored the two well-known values
                // under fixed keys.
                if name == "ema_value" {
                    if let Some(f) = v.as_f64() { self.indicators.insert("ema20".to_string(), f); }
                    continue;
                }
                if name == "atr_value" {
                    if let Some(f) = v.as_f64() { self.indicators.insert("atr14".to_string(), f); }
                    continue;
                }
                if name == "trade_entry" { self.trade_entry = v.as_f64(); continue; }
                if name == "trade_stop" { self.trade_stop = v.as_f64(); continue; }
                if name == "trade_target" { self.trade_target = v.as_f64(); continue; }
                if name == "trade_direction" { self.trade_direction = v.as_i64().map(|x| x as i8); continue; }
                if name == "trade_max_hold" { self.trade_max_hold = v.as_u64().map(|x| x as usize); continue; }
                if name == "trade_bars" { self.trade_bars = v.as_u64().unwrap_or(0) as usize; continue; }
        if name == "trade_entry_ts" { self.trade_entry_ts = v.as_i64(); continue; }
        if name == "trade_size" { self.trade_size = v.as_f64(); continue; }
        if name == "trade_source" { self.trade_source = v.as_str().map(|s| s.to_string()); continue; }
        if name == "trade_pending" { if let Some(b) = v.as_bool() { self.trade_pending = b; } continue; }
        if name == "trade_pending_market" { if let Some(b) = v.as_bool() { self.trade_pending_market = b; } continue; }
        if name == "trade_pending_entry" { self.trade_pending_entry = v.as_f64(); continue; }
        if name == "trade_pending_stop" { self.trade_pending_stop = v.as_f64(); continue; }
        if name == "trade_pending_target" { self.trade_pending_target = v.as_f64(); continue; }
        if name == "trade_pending_direction" { self.trade_pending_direction = v.as_i64().map(|x| x as i8); continue; }
        if name == "trade_pending_max_hold" { self.trade_pending_max_hold = v.as_u64().map(|x| x as usize); continue; }
        if name == "trade_pending_size" { self.trade_pending_size = v.as_f64(); continue; }
        if name == "trade_pending_entry_ts" { self.trade_pending_entry_ts = v.as_i64(); continue; }
        if name == "trade_pending_valid" { self.trade_pending_valid = v.as_u64().unwrap_or(0) as usize; continue; }
        if name == "seen_setups" {
            if let Some(arr) = v.as_array() {
                for item in arr {
                    if let Some(k) = item.as_str() { self.seen_setups.insert(k.to_string()); }
                }
            }
            continue;
        }
        if name == "last_enrichment" {
            if let Some(arr) = v.as_array() {
                let mut out = Vec::new();
                for item in arr {
                    if let (Some(topic), Some(ev_val)) =
                        (item.get("topic").and_then(|x| x.as_str()), item.get("event"))
                    {
                        if let Ok(ev) = serde_json::from_value(ev_val.clone()) {
                            out.push((topic.to_string(), ev));
                        }
                    }
                }
                self.last_enrichment = Some(out);
            }
            continue;
        }
                if let Some(p) = self.processors.get_mut(&name) {
                    p.load_json(&v.to_string());
                }
            }
        }
    }
}

pub struct MarketContext {
    pub contexts: HashMap<(Symbol, Timeframe), Context>,
}

impl MarketContext {
    pub fn new() -> Self {
        Self { contexts: HashMap::new() }
    }

    pub fn register(&mut self, instrument: Symbol, timeframe: Timeframe) {
        let key = (instrument.clone(), timeframe.clone());
        self.contexts.insert(key, Context::new(instrument, timeframe));
    }

    pub fn get(&mut self, instrument: &Symbol, timeframe: &Timeframe) -> Option<&mut Context> {
        self.contexts.get_mut(&(instrument.clone(), timeframe.clone()))
    }

    pub fn get_or_create(&mut self, instrument: Symbol, timeframe: Timeframe) -> &mut Context {
        let key = (instrument.clone(), timeframe.clone());
        self.contexts.entry(key).or_insert_with(|| Context::new(instrument, timeframe))
    }
}

impl crate::StateMachine for MarketContext {
    fn handle_event(&mut self, event: &Event) {
        match event {
            Event::CandleClosed { symbol, timeframe, candle } => {
                if let Some(ctx) = self.contexts.get_mut(&(symbol.clone(), timeframe.clone())) {
                    ctx.apply(candle);
                }
            }
            _ => {}
        }
    }
}
