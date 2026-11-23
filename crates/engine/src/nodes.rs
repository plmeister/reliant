use crate::expr;
use crate::expr::Expr;
use crate::graph::Node;
use crate::context::MarketContext;
use reliant_datamodel::{Candle, Event, Symbol, Timeframe};
use reliant_indicators::{Atr, Ema, Indicator, SwingDetector, SwingPivot};

pub struct CandleSource {
    pub id: String,
    pub symbol: Symbol,
    pub timeframe: Timeframe,
    pub candles: Vec<Candle>,
    emitted: bool,
}

impl CandleSource {
    pub fn new(id: &str, symbol: Symbol, timeframe: Timeframe, candles: Vec<Candle>) -> Self {
        Self { id: id.to_string(), symbol, timeframe, candles, emitted: false }
    }
}

impl Node for CandleSource {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscriptions(&self) -> &[&'static str] {
        &["control"]
    }

    fn on_event(&mut self, _topic: &str, event: &Event, _ctx: &mut MarketContext) -> Vec<(String, Event)> {
        match event {
            Event::Start => {
                if self.emitted {
                    return Vec::new();
                }
                self.emitted = true;
                self.candles
                    .iter()
                    .map(|c| ("ingress".to_string(), Event::CandleClosed {
                        symbol: self.symbol.clone(),
                        timeframe: self.timeframe.clone(),
                        candle: c.clone(),
                    }))
                    .collect()
            }
            _ => Vec::new(),
        }
    }
}

pub struct AtrNode {
    pub id: String,
    /// Processor / fact key inside the market context. Defaults to `atr14`
    /// (legacy), set from the DSL box name by the builder.
    pub key: String,
    pub symbol: Symbol,
    pub timeframe: Timeframe,
    pub period: usize,
}

impl AtrNode {
    pub fn new(id: &str, symbol: Symbol, timeframe: Timeframe, period: usize) -> Self {
        Self::with_key(id, "atr14", symbol, timeframe, period)
    }

    pub fn with_key(
        id: &str,
        key: &str,
        symbol: Symbol,
        timeframe: Timeframe,
        period: usize,
    ) -> Self {
        Self { id: id.to_string(), key: key.to_string(), symbol, timeframe, period }
    }
}

impl Node for AtrNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscriptions(&self) -> &[&'static str] {
        &["ingress"]
    }

    fn init_context(&self, ctx: &mut MarketContext) {
        let c = ctx.get_or_create(self.symbol.clone(), self.timeframe.clone());
        if !c.processors.contains_key(&self.key) {
            c.register(Box::new(Atr::new(&self.key, self.period)));
        }
    }

    fn on_event(&mut self, _topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)> {
        if let Event::CandleClosed { symbol, timeframe, candle } = event {
            if symbol != &self.symbol || timeframe != &self.timeframe {
                return Vec::new();
            }
            let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
            if !c.processors.contains_key(&self.key) {
                c.register(Box::new(Atr::new(&self.key, self.period)));
            }
            if let Some(p) = c.processors.get_mut(&self.key) { p.apply(candle); }
            if let Some(proc) = c.processors.get(&self.key) {
                if let Some(atr) = proc.as_any().downcast_ref::<Atr>() {
                    if let Some(v) = atr.value() {
                        c.indicators.insert(self.key.clone(), v);
                        return vec![("enrichment".to_string(), Event::IndicatorUpdated { symbol: symbol.clone(), timeframe: timeframe.clone(), name: self.key.clone(), value: v })];
                    }
                }
            }
        }
        Vec::new()
    }
}

pub struct EmaNode {
    pub id: String,
    /// Processor / fact key inside the market context. Defaults to `ema20`
    /// (legacy), set from the DSL box name by the builder.
    pub key: String,
    pub symbol: Symbol,
    pub timeframe: Timeframe,
    pub period: usize,
}

impl EmaNode {
    pub fn new(id: &str, symbol: Symbol, timeframe: Timeframe, period: usize) -> Self {
        Self::with_key(id, "ema20", symbol, timeframe, period)
    }

    pub fn with_key(
        id: &str,
        key: &str,
        symbol: Symbol,
        timeframe: Timeframe,
        period: usize,
    ) -> Self {
        Self { id: id.to_string(), key: key.to_string(), symbol, timeframe, period }
    }
}

impl Node for EmaNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscriptions(&self) -> &[&'static str] {
        &["ingress"]
    }

    fn init_context(&self, ctx: &mut MarketContext) {
        let c = ctx.get_or_create(self.symbol.clone(), self.timeframe.clone());
        if !c.processors.contains_key(&self.key) {
            c.register(Box::new(Ema::new(&self.key, self.period)));
        }
    }

    fn on_event(&mut self, _topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)> {
        if let Event::CandleClosed { symbol, timeframe, candle } = event {
            if symbol != &self.symbol || timeframe != &self.timeframe {
                return Vec::new();
            }
            let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
            if !c.processors.contains_key(&self.key) {
                c.register(Box::new(Ema::new(&self.key, self.period)));
            }
            if let Some(p) = c.processors.get_mut(&self.key) { p.apply(candle); }
            if let Some(proc) = c.processors.get(&self.key) {
                if let Some(ema) = proc.as_any().downcast_ref::<Ema>() {
                    if let Some(v) = ema.value() {
                        c.indicators.insert(self.key.clone(), v);
                        return vec![("enrichment".to_string(), Event::IndicatorUpdated { symbol: symbol.clone(), timeframe: timeframe.clone(), name: self.key.clone(), value: v })];
                    }
                }
            }
        }
        Vec::new()
    }
}

pub struct SwingNode {
    pub id: String,
    /// Processor / fact key inside the market context. Defaults to `swing`
    /// (legacy), set from the DSL box name by the builder.
    pub key: String,
    pub symbol: Symbol,
    pub timeframe: Timeframe,
    pub k: f64,
    pub lookback: usize,
}

impl SwingNode {
    pub fn new(id: &str, symbol: Symbol, timeframe: Timeframe, k: f64, lookback: usize) -> Self {
        Self::with_key(id, "swing", symbol, timeframe, k, lookback)
    }

    pub fn with_key(
        id: &str,
        key: &str,
        symbol: Symbol,
        timeframe: Timeframe,
        k: f64,
        lookback: usize,
    ) -> Self {
        Self {
            id: id.to_string(),
            key: key.to_string(),
            symbol,
            timeframe,
            k,
            lookback,
        }
    }
}

impl Node for SwingNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscriptions(&self) -> &[&'static str] {
        &["ingress"]
    }

    fn init_context(&self, ctx: &mut MarketContext) {
        let c = ctx.get_or_create(self.symbol.clone(), self.timeframe.clone());
        if !c.processors.contains_key(&self.key) {
            c.register(Box::new(SwingDetector::new(&self.key, self.k, self.lookback)));
        }
    }

    fn on_event(&mut self, _topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)> {
        if let Event::CandleClosed { symbol, timeframe, candle } = event {
            if symbol != &self.symbol || timeframe != &self.timeframe {
                return Vec::new();
            }
            let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
            if !c.processors.contains_key(&self.key) {
                c.register(Box::new(SwingDetector::new(&self.key, self.k, self.lookback)));
            }
            let signals = if let Some(proc) = c.processors.get_mut(&self.key) {
                if let Some(det) = proc.as_any_mut().downcast_mut::<SwingDetector>() {
                    det.apply_with_signals(candle)
                } else { Vec::new() }
            } else { Vec::new() };
            return signals
                .into_iter()
                .map(|sp: SwingPivot| ("enrichment".to_string(), Event::SwingPivotEvent {
                    symbol: self.symbol.clone(),
                    timeframe: self.timeframe.clone(),
                    name: self.key.clone(),
                    index: sp.index,
                    timestamp: candle.timestamp.timestamp(),
                    kind: match sp.kind {
                        reliant_indicators::PivotKind::High => 1,
                        reliant_indicators::PivotKind::Low => -1,
                    },
                    price: sp.price,
                    atr: sp.atr_confirm,
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                }))
                .collect();
        }
        Vec::new()
    }
}

pub struct StrategyNode {
    pub id: String,
    pub k: f64,
    pub stop_atr_mult: f64,
    pub target_atr_mult: f64,
    pub direction_sign: f64,
    pub max_hold_bars: usize,
    pub htf_ema_key: String,
    pub htf_filter: String,
    pub timeframe: Timeframe,
    /// Names of the fact slots this box is wired to. The legacy constructor
    /// keeps the pre-DSL defaults; the DSL builder resolves them from the
    /// input edges, so `ema: ema_fast` really reads `ema_fast`.
    pub ema_key: String,
    pub atr_key: String,
    pub swing_key: String,
}

impl StrategyNode {
    pub fn new(id: &str, k: f64) -> Self {
        Self::with_params(id, k, 1.5, 3.0, -1.0, 15, String::new(), "none".to_string(), "ema20", "atr14", "swing", Timeframe::D1)
    }

    pub fn with_inputs(
        id: &str,
        k: f64,
        ema_key: impl Into<String>,
        atr_key: impl Into<String>,
        swing_key: impl Into<String>,
    ) -> Self {
        Self::with_params(id, k, 1.5, 3.0, -1.0, 15, String::new(), "none".to_string(), ema_key, atr_key, swing_key, Timeframe::D1)
    }

    pub fn with_params(
        id: &str,
        k: f64,
        stop_atr_mult: f64,
        target_atr_mult: f64,
        direction_sign: f64,
        max_hold_bars: usize,
        htf_ema_key: String,
        htf_filter: String,
        ema_key: impl Into<String>,
        atr_key: impl Into<String>,
        swing_key: impl Into<String>,
        timeframe: Timeframe,
    ) -> Self {
        Self {
            id: id.to_string(),
            k,
            stop_atr_mult,
            target_atr_mult,
            direction_sign,
            max_hold_bars,
            htf_ema_key,
            htf_filter,
            ema_key: ema_key.into(),
            atr_key: atr_key.into(),
            swing_key: swing_key.into(),
            timeframe,
        }
    }
}

impl Node for StrategyNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscriptions(&self) -> &[&'static str] {
        &["enrichment"]
    }

    fn on_event(&mut self, _topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)> {
        match event {
            Event::IndicatorUpdated { symbol, timeframe: _, name, value } => {
                // Store under the strategy's own (trade) timeframe so that
                // HTF indicators (e.g. ema_1w) are visible alongside trade-TF
                // ones when a trade-TF swing arrives.
                let c = ctx.get_or_create(symbol.clone(), self.timeframe.clone());
                c.indicators.insert(name.clone(), *value);
                Vec::new()
            }
            Event::SwingPivotEvent { symbol, timeframe, name, kind, price, atr, index, timestamp, .. } => {
                // Older audit files carry no pivot name; accept those, but when
                // a name is present it must be the wired swing producer.
                if !name.is_empty() && name != &self.swing_key {
                    return Vec::new();
                }
                // Read EMA/ATR from context (may have been populated by
                // enricher-side indicators or strategy-side IndicatorUpdated).
                let (ema, atr_v) = {
                    let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
                    let ema = c.indicators.get(&self.ema_key).copied().or_else(|| {
                        c.processors.get(&self.ema_key)
                            .and_then(|p| p.as_any().downcast_ref::<Ema>())
                            .and_then(|e| e.value())
                    });
                    let atr_cached = c.indicators.get(&self.atr_key).copied().or_else(|| {
                        c.processors.get(&self.atr_key)
                            .and_then(|p| p.as_any().downcast_ref::<Atr>())
                            .and_then(|a| a.value())
                    });
                    let ema = match ema { Some(v) => v, None => return Vec::new() };
                    let atr_v = if *atr > 0.0 { *atr } else { atr_cached.unwrap_or(1.0) };
                    (ema, atr_v)
                };
                let deviation = (*price - ema).abs();
                if deviation < self.k * atr_v {
                    return Vec::new();
                }
                let direction: i8 = (self.direction_sign * *kind as i8 as f64) as i8;
                // Higher-timeframe trend alignment filter (optional). The HTF
                // indicator arrives via the enrichment bus (e.g. an `ema_1w`
                // box) and is stored in this context under `htf_ema_key`.
                if !self.htf_ema_key.is_empty() && self.htf_filter == "align" {
                    let htf_ema = ctx.get_or_create(symbol.clone(), timeframe.clone()).indicators.get(&self.htf_ema_key).copied();
                    if let Some(htf_ema) = htf_ema {
                        let aligned = if direction > 0 {
                            *price > htf_ema
                        } else {
                            *price < htf_ema
                        };
                        if !aligned {
                            return Vec::new();
                        }
                    }
                }
                let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
                if c.open_trade || c.trade_pending || c.trade_pending_market {
                    return Vec::new();
                }
                let entry = *price;
                let stop = entry - (direction as f64) * self.stop_atr_mult * atr_v;
                let target = entry + (direction as f64) * self.target_atr_mult * atr_v;
                vec![("signal".to_string(), Event::SetupSignal {
                    symbol: symbol.clone(),
                    timeframe: timeframe.clone(),
                    direction,
                    entry,
                    stop,
                    target,
                    max_hold_secs: self.max_hold_bars as f64 * timeframe.bar_seconds() as f64,
                    timestamp: *timestamp,
                    source: self.id.clone(),
                })]
            }
            _ => Vec::new(),
        }
    }
}

pub struct GeometryNode {
    pub id: String,
    pub balance: f64,
    pub risk_pct: f64,
    /// If non-empty, only size setups whose `source` matches this value.
    /// Empty = size all setups (default).
    pub source_filter: String,
}

impl GeometryNode {
    pub fn new(id: &str, balance: f64, risk_pct: f64) -> Self {
        Self { id: id.to_string(), balance, risk_pct, source_filter: String::new() }
    }

    pub fn with_source(id: &str, balance: f64, risk_pct: f64, source_filter: String) -> Self {
        Self { id: id.to_string(), balance, risk_pct, source_filter }
    }
}

impl Node for GeometryNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscriptions(&self) -> &[&'static str] {
        &["signal"]
    }

    fn on_event(&mut self, _topic: &str, event: &Event, _ctx: &mut MarketContext) -> Vec<(String, Event)> {
        if let Event::SetupSignal { symbol, timeframe, direction, entry, stop, target, max_hold_secs, timestamp, source, .. } = event {
            if !self.source_filter.is_empty() && source != &self.source_filter {
                return Vec::new();
            }
            let risk = self.balance * self.risk_pct;
            let stop_dist = (*entry - *stop).abs();
            let size = if stop_dist > 0.0 { risk / stop_dist } else { 0.0 };
            return vec![("trade".to_string(), Event::TradeSignal {
                symbol: symbol.clone(),
                timeframe: timeframe.clone(),
                direction: *direction,
                entry: *entry,
                stop: *stop,
                target: *target,
                max_hold_secs: *max_hold_secs,
                size,
                timestamp: *timestamp,
                source: source.clone(),
            })];
        }
        Vec::new()
    }
}

// ---- DummyBroker: param-driven order management ----

pub struct DummyBroker {
    pub id: String,
    pub symbol: Symbol,
    pub timeframe: Timeframe,
    pub entry_mode: String,
    pub limit_valid_bars: usize,
}

impl DummyBroker {
    pub fn new(id: &str, symbol: Symbol, timeframe: Timeframe) -> Self {
        Self::with_params(id, symbol, timeframe, "limit".to_string(), 3)
    }

    pub fn with_params(
        id: &str,
        symbol: Symbol,
        timeframe: Timeframe,
        entry_mode: String,
        limit_valid_bars: usize,
    ) -> Self {
        Self { id: id.to_string(), symbol, timeframe, entry_mode, limit_valid_bars }
    }
}

impl Node for DummyBroker {
    fn id(&self) -> &str { &self.id }
    fn subscriptions(&self) -> &[&'static str] { &["ingress", "trade"] }

    fn on_event(&mut self, topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)> {
        match event {
            Event::TradeSignal { symbol, timeframe, direction, entry, stop, target, max_hold_secs, size, timestamp, source, .. } => {
                if symbol == &self.symbol && timeframe == &self.timeframe && topic == "trade" {
                    let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
                    if c.open_trade || c.trade_pending || c.trade_pending_market {
                        // Already busy: ignore.
                    } else {
                        let bars = (*max_hold_secs / timeframe.bar_seconds() as f64).ceil().max(1.0) as usize;
                        if self.entry_mode == "immediate" {
                            c.open_trade = true;
                            c.trade_entry = Some(*entry);
                            c.trade_stop = Some(*stop);
                            c.trade_target = Some(*target);
                            c.trade_direction = Some(*direction);
                            c.trade_max_hold = Some(bars);
                            c.trade_bars = 0;
                            c.trade_entry_ts = Some(if *timestamp != 0 { *timestamp } else { 0 });
                            c.trade_size = Some(*size);
                            c.trade_source = Some(source.clone());
                        } else if self.entry_mode == "next_open" {
                            c.trade_pending_market = true;
                            c.trade_pending_entry = Some(*entry);
                            c.trade_pending_stop = Some(*stop);
                            c.trade_pending_target = Some(*target);
                            c.trade_pending_direction = Some(*direction);
                            c.trade_pending_max_hold = Some(bars);
                            c.trade_pending_size = Some(*size);
                            c.trade_pending_entry_ts = Some(if *timestamp != 0 { *timestamp } else { 0 });
                            c.trade_pending_valid = 1;
                            c.trade_source = Some(source.clone());
                        } else {
                            c.trade_pending = true;
                            c.trade_pending_entry = Some(*entry);
                            c.trade_pending_stop = Some(*stop);
                            c.trade_pending_target = Some(*target);
                            c.trade_pending_direction = Some(*direction);
                            c.trade_pending_max_hold = Some(bars);
                            c.trade_pending_size = Some(*size);
                            c.trade_pending_entry_ts = Some(if *timestamp != 0 { *timestamp } else { 0 });
                            c.trade_pending_valid = self.limit_valid_bars.max(1);
                            c.trade_source = Some(source.clone());
                        }
                    }
                }
                Vec::new()
            }
            Event::CandleClosed { symbol, timeframe, candle } => {
                if symbol != &self.symbol || timeframe != &self.timeframe || topic != "ingress" {
                    return Vec::new();
                }
                let mut out: Vec<(String, Event)> = Vec::new();
                // Resolve a carried position first.
                {
                    let c = match ctx.get(symbol, timeframe) { Some(c) => c, None => return Vec::new() };
                    if c.open_trade {
                        c.trade_bars += 1;
                        let entry = c.trade_entry.unwrap();
                        let stop = c.trade_stop.unwrap();
                        let target = c.trade_target.unwrap();
                        let direction = c.trade_direction.unwrap();
                        let max_hold = c.trade_max_hold.unwrap();
                        let bars = c.trade_bars;
                        let (exit_price, reason) = if direction > 0 {
                            if candle.low <= stop { (stop, "stop") }
                            else if candle.high >= target { (target, "target") }
                            else if bars >= max_hold { (candle.close, "timeout") }
                            else { return out; }
                        } else {
                            if candle.high >= stop { (stop, "stop") }
                            else if candle.low <= target { (target, "target") }
                            else if bars >= max_hold { (candle.close, "timeout") }
                            else { return out; }
                        };
                        let r_dist = (entry - stop).abs();
                        let pnl_r = if r_dist > 0.0 { (direction as f64) * (exit_price - entry) / r_dist } else { 0.0 };
                        let entry_ts = c.trade_entry_ts.unwrap_or(0);
                        let size = c.trade_size.unwrap_or(0.0);
                        let source = c.trade_source.clone().unwrap_or_default();
                        let c = ctx.get(symbol, timeframe).unwrap();
                        c.open_trade = false;
                        c.trade_entry = None;
                        c.trade_stop = None;
                        c.trade_target = None;
                        c.trade_direction = None;
                        c.trade_max_hold = None;
                        c.trade_bars = 0;
                        c.trade_entry_ts = None;
                        c.trade_size = None;
                        c.trade_source = None;
                        out.push(("trade".to_string(), Event::TradeResult {
                            pnl_r,
                            reason: reason.to_string(),
                            symbol: Some(symbol.0.clone()),
                            timeframe: Some(timeframe.as_str().to_string()),
                            direction,
                            entry,
                            stop,
                            target,
                            entry_ts,
                            exit_ts: candle.timestamp.timestamp(),
                            exit_price,
                            size,
                            source,
                        }));
                    }
                }

                // Resting limit / pending market: fill when conditions met.
                let c = match ctx.get(symbol, timeframe) { Some(c) => c, None => return out };
                if !c.open_trade && (c.trade_pending || c.trade_pending_market) {
                    let is_market = c.trade_pending_market;
                    let entry = c.trade_pending_entry.unwrap();
                    let stop = c.trade_pending_stop.unwrap();
                    let target = c.trade_pending_target.unwrap();
                    let direction = c.trade_pending_direction.unwrap();
                    let max_hold = c.trade_pending_max_hold.unwrap();
                    let size = c.trade_pending_size.unwrap_or(0.0);

                    let touched = if is_market { true } else if direction > 0 {
                        candle.low <= entry
                    } else {
                        candle.high >= entry
                    };
                    let gap_through = if is_market { true } else if direction > 0 {
                        candle.open < entry
                    } else {
                        candle.open > entry
                    };
                    if touched {
                        let fill = if is_market { candle.open } else if gap_through { candle.open } else { entry };
                        let c = ctx.get(symbol, timeframe).unwrap();
                        c.trade_pending = false;
                        c.trade_pending_market = false;
                        c.trade_pending_entry = None;
                        c.trade_pending_stop = None;
                        c.trade_pending_target = None;
                        c.trade_pending_direction = None;
                        c.trade_pending_max_hold = None;
                        c.trade_pending_size = None;
                        c.trade_pending_entry_ts = None;
                        c.trade_pending_valid = 0;

                        c.open_trade = true;
                        c.trade_entry = Some(fill);
                        c.trade_stop = Some(stop);
                        c.trade_target = Some(target);
                        c.trade_direction = Some(direction);
                        c.trade_max_hold = Some(max_hold);
                        c.trade_bars = 1;
                        c.trade_entry_ts = Some(candle.timestamp.timestamp());
                        c.trade_size = Some(size);

                        let entry = c.trade_entry.unwrap();
                        let stop = c.trade_stop.unwrap();
                        let target = c.trade_target.unwrap();
                        let direction = c.trade_direction.unwrap();
                        let max_hold = c.trade_max_hold.unwrap();
                        let bars = c.trade_bars;
                        let (exit_price, reason) = if direction > 0 {
                            if candle.low <= stop { (stop, "stop") }
                            else if candle.high >= target { (target, "target") }
                            else if bars >= max_hold { (candle.close, "timeout") }
                            else { (0.0, "") }
                        } else {
                            if candle.high >= stop { (stop, "stop") }
                            else if candle.low <= target { (target, "target") }
                            else if bars >= max_hold { (candle.close, "timeout") }
                            else { (0.0, "") }
                        };
                        if reason.is_empty() {
                            return out;
                        }
                        let r_dist = (entry - stop).abs();
                        let pnl_r = if r_dist > 0.0 { (direction as f64) * (exit_price - entry) / r_dist } else { 0.0 };
                        let entry_ts = c.trade_entry_ts.unwrap_or(0);
                        let size = c.trade_size.unwrap_or(0.0);
                        let source = c.trade_source.clone().unwrap_or_default();
                        let c = ctx.get(symbol, timeframe).unwrap();
                        c.open_trade = false;
                        c.trade_entry = None;
                        c.trade_stop = None;
                        c.trade_target = None;
                        c.trade_direction = None;
                        c.trade_max_hold = None;
                        c.trade_bars = 0;
                        c.trade_entry_ts = None;
                        c.trade_size = None;
                        c.trade_source = None;
                        out.push(("trade".to_string(), Event::TradeResult {
                            pnl_r,
                            reason: reason.to_string(),
                            symbol: Some(symbol.0.clone()),
                            timeframe: Some(timeframe.as_str().to_string()),
                            direction,
                            entry,
                            stop,
                            target,
                            entry_ts,
                            exit_ts: candle.timestamp.timestamp(),
                            exit_price,
                            size,
                            source,
                        }));
                    } else {
                        let c = ctx.get(symbol, timeframe).unwrap();
                        if c.trade_pending {
                            c.trade_pending_valid = c.trade_pending_valid.saturating_sub(1);
                            if c.trade_pending_valid == 0 {
                                c.trade_pending = false;
                                c.trade_pending_market = false;
                                c.trade_pending_entry = None;
                                c.trade_pending_stop = None;
                                c.trade_pending_target = None;
                                c.trade_pending_direction = None;
                                c.trade_pending_max_hold = None;
                                c.trade_pending_size = None;
                                c.trade_pending_entry_ts = None;
                            }
                        }
                    }
                }
                out
            }
            _ => Vec::new(),
        }
    }
}

// ---- FilterNode ----

pub struct FilterNode {
    pub id: String,
    pub when: String,
    parsed: Option<Expr>,
    seen: std::collections::HashSet<(String, i64)>,
}

impl FilterNode {
    pub fn new(id: &str, when: &str) -> Self {
        Self { id: id.to_string(), when: when.to_string(), parsed: expr::parse(when).ok(), seen: std::collections::HashSet::new() }
    }
}

impl Node for FilterNode {
    fn id(&self) -> &str { &self.id }
    fn subscriptions(&self) -> &[&'static str] { &["enrichment"] }

    fn on_event(&mut self, _topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)> {
        if let Event::SwingPivotEvent { symbol, timeframe, name, kind, price, atr, index, timestamp, open, high, low, close } = event {
            if !self.seen.insert((symbol.0.clone(), *index)) {
                return Vec::new();
            }
            let mut vars: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
            if let Some(c) = ctx.get(symbol, timeframe) {
                for (k, v) in &c.indicators {
                    vars.insert(k.clone(), *v);
                }
            }
            vars.insert("price".to_string(), *price);
            vars.insert("open".to_string(), *open);
            vars.insert("high".to_string(), *high);
            vars.insert("low".to_string(), *low);
            vars.insert("close".to_string(), *close);
            vars.insert("atr".to_string(), *atr);
            vars.insert("kind".to_string(), *kind as f64);
            vars.insert("index".to_string(), *index as f64);

            let pass = match &self.parsed {
                Some(e) => expr::eval_bool(e, &vars),
                None => true,
            };
            if pass {
                return vec![("enrichment".to_string(), Event::SwingPivotEvent {
                    symbol: symbol.clone(),
                    timeframe: timeframe.clone(),
                    name: self.id.clone(),
                    kind: *kind,
                    price: *price,
                    atr: *atr,
                    index: *index,
                    timestamp: *timestamp,
                    open: *open,
                    high: *high,
                    low: *low,
                    close: *close,
                })];
            }
        }
        Vec::new()
    }
}

// ---- LevelDetector ----

pub struct LevelDetector {
    pub id: String,
    pub lookback: usize,
    highs: std::collections::VecDeque<f64>,
    lows: std::collections::VecDeque<f64>,
}

impl LevelDetector {
    pub fn new(id: &str, lookback: usize) -> Self {
        Self { id: id.to_string(), lookback: lookback.max(2), highs: std::collections::VecDeque::new(), lows: std::collections::VecDeque::new() }
    }
}

impl Node for LevelDetector {
    fn id(&self) -> &str { &self.id }
    fn subscriptions(&self) -> &[&'static str] { &["ingress"] }

    fn on_event(&mut self, _topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)> {
        if let Event::CandleClosed { symbol, timeframe, candle } = event {
            // Compute from PRIOR bars so a breakout is "current high exceeds
            // the prior rolling max" (resistance excludes the current bar).
            let resistance = if self.highs.len() >= 1 {
                Some(self.highs.iter().cloned().fold(f64::MIN, f64::max))
            } else { None };
            let support = if self.lows.len() >= 1 {
                Some(self.lows.iter().cloned().fold(f64::MAX, f64::min))
            } else { None };

            self.highs.push_back(candle.high);
            self.lows.push_back(candle.low);
            while self.highs.len() > self.lookback { self.highs.pop_front(); }
            while self.lows.len() > self.lookback { self.lows.pop_front(); }

            if let (Some(resistance), Some(support)) = (resistance, support) {
                let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
                c.indicators.insert("support".to_string(), support);
                c.indicators.insert("resistance".to_string(), resistance);
                return vec![
                    ("enrichment".to_string(), Event::IndicatorUpdated {
                        symbol: symbol.clone(),
                        timeframe: timeframe.clone(),
                        name: "support".to_string(),
                        value: support,
                    }),
                    ("enrichment".to_string(), Event::IndicatorUpdated {
                        symbol: symbol.clone(),
                        timeframe: timeframe.clone(),
                        name: "resistance".to_string(),
                        value: resistance,
                    }),
                ];
            }
        }
        Vec::new()
    }
}

// ---- CandleSignal: signal on a condition, no deviation check ----

pub struct CandleSignal {
    pub id: String,
    pub when: String,
    parsed: Option<Expr>,
    pub direction: i8,
    pub stop_atr_mult: f64,
    pub target_atr_mult: f64,
    pub max_hold_bars: usize,
    pub ema_key: String,
    pub atr_key: String,
    pub swing_key: String,
    pub trigger: String,
}

impl CandleSignal {
    pub fn new(id: &str, when: &str, direction: i8) -> Self {
        Self {
            id: id.to_string(),
            when: when.to_string(),
            parsed: expr::parse(when).ok(),
            direction,
            stop_atr_mult: 1.5,
            target_atr_mult: 3.0,
            max_hold_bars: 15,
            ema_key: "ema".to_string(),
            atr_key: "atr".to_string(),
            swing_key: "swing".to_string(),
            trigger: "swing".to_string(),
        }
    }
    pub fn with_params(
        id: &str,
        when: &str,
        direction: i8,
        stop_atr_mult: f64,
        target_atr_mult: f64,
        max_hold_bars: usize,
        ema_key: impl Into<String>,
        atr_key: impl Into<String>,
        swing_key: impl Into<String>,
        trigger: impl Into<String>,
    ) -> Self {
        Self {
            id: id.to_string(),
            when: when.to_string(),
            parsed: expr::parse(when).ok(),
            direction,
            stop_atr_mult,
            target_atr_mult,
            max_hold_bars,
            ema_key: ema_key.into(),
            atr_key: atr_key.into(),
            swing_key: swing_key.into(),
            trigger: trigger.into(),
        }
    }
}

impl Node for CandleSignal {
    fn id(&self) -> &str { &self.id }
    fn subscriptions(&self) -> &[&'static str] {
        if self.trigger == "bar" { &["ingress", "enrichment"] } else { &["enrichment"] }
    }

    fn on_event(&mut self, _topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)> {
        if self.trigger == "bar" {
            if let Event::CandleClosed { symbol, timeframe, candle } = event {
                let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
                let ema = c.indicators.get(&self.ema_key).copied();
                let atr_cached = c.indicators.get(&self.atr_key).copied();
                let atr_v = atr_cached.unwrap_or(1.0);
                let ema_v = ema.unwrap_or(candle.close);
                let resistance = c.indicators.get("resistance").copied().unwrap_or(candle.close);
                let support = c.indicators.get("support").copied().unwrap_or(candle.close);

                let mut vars: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
                for (k, v) in &c.indicators { vars.insert(k.clone(), *v); }
                vars.insert("price".to_string(), candle.close);
                vars.insert("open".to_string(), candle.open);
                vars.insert("high".to_string(), candle.high);
                vars.insert("low".to_string(), candle.low);
                vars.insert("close".to_string(), candle.close);
                vars.insert("atr".to_string(), atr_v);
                vars.insert("ema".to_string(), ema_v);
                vars.insert("resistance".to_string(), resistance);
                vars.insert("support".to_string(), support);
                vars.insert("kind".to_string(), 0.0);
                vars.insert("index".to_string(), candle.timestamp.timestamp() as f64);

                let pass = match &self.parsed {
                    Some(e) => expr::eval_bool(e, &vars),
                    None => false
                };
                let _ = ();
                if !pass { return Vec::new(); }

                let direction = if self.direction == 0 {
                    if candle.high > resistance { 1 }
                    else if candle.low < support { -1 }
                    else { return Vec::new(); }
                } else { self.direction };

                let entry = candle.close;
                let stop = entry - (direction as f64) * self.stop_atr_mult * atr_v;
                let target = entry + (direction as f64) * self.target_atr_mult * atr_v;
                return vec![("signal".to_string(), Event::SetupSignal {
                    symbol: symbol.clone(),
                    timeframe: timeframe.clone(),
                    direction,
                    entry,
                    stop,
                    target,
                    max_hold_secs: self.max_hold_bars as f64 * timeframe.bar_seconds() as f64,
                    timestamp: candle.timestamp.timestamp(),
                    source: self.id.clone(),
                })];
            }
        }
        if let Event::IndicatorUpdated { symbol, timeframe, name, value } = event {
            let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
            c.indicators.insert(name.clone(), *value);
            return Vec::new();
        }
        if let Event::SwingPivotEvent { symbol, timeframe, name, kind, price, atr, index, timestamp, open, high, low, close } = event {
            if !name.is_empty() && name != &self.swing_key {
                return Vec::new();
            }
            let c = ctx.get_or_create(symbol.clone(), timeframe.clone());
            let ema = c.indicators.get(&self.ema_key).copied();
            let atr_cached = c.indicators.get(&self.atr_key).copied();
            let atr_v = if *atr > 0.0 { *atr } else { atr_cached.unwrap_or(1.0) };
            let ema_v = ema.unwrap_or(*price);

            let mut vars: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
            for (k, v) in &c.indicators { vars.insert(k.clone(), *v); }
            vars.insert("price".to_string(), *price);
            vars.insert("open".to_string(), *open);
            vars.insert("high".to_string(), *high);
            vars.insert("low".to_string(), *low);
            vars.insert("close".to_string(), *close);
            vars.insert("atr".to_string(), atr_v);
            vars.insert("ema".to_string(), ema_v);
            vars.insert("kind".to_string(), *kind as f64);
            vars.insert("index".to_string(), *index as f64);

            let pass = match &self.parsed {
                Some(e) => expr::eval_bool(e, &vars),
                None => false,
            };
            if !pass {
                return Vec::new();
            }
            // Auto-direction (direction==0): derive from which level was breached.
            let direction = if self.direction == 0 {
                if *price > vars.get("resistance").copied().unwrap_or(*price) { 1 }
                else if *price < vars.get("support").copied().unwrap_or(*price) { -1 }
                else { return Vec::new(); }
            } else {
                self.direction
            };
            let entry = *price;
            let stop = entry - (direction as f64) * self.stop_atr_mult * atr_v;
            let target = entry + (direction as f64) * self.target_atr_mult * atr_v;
            return vec![("signal".to_string(), Event::SetupSignal {
                symbol: symbol.clone(),
                timeframe: timeframe.clone(),
                direction,
                entry,
                stop,
                target,
                max_hold_secs: self.max_hold_bars as f64 * timeframe.bar_seconds() as f64,
                timestamp: *timestamp,
                source: self.id.clone(),
            })];
        }
        Vec::new()
    }
}
