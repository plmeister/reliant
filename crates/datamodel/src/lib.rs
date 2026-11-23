use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Timeframe {
    M1,
    M5,
    H1,
    D1,
    W1,
}

impl Timeframe {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "1m" | "m1" => Some(Timeframe::M1),
            "5m" | "m5" => Some(Timeframe::M5),
            "1h" | "h1" => Some(Timeframe::H1),
            "1d" | "d1" => Some(Timeframe::D1),
            "1w" | "w1" => Some(Timeframe::W1),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Timeframe::M1 => "1m",
            Timeframe::M5 => "5m",
            Timeframe::H1 => "1h",
            Timeframe::D1 => "1d",
            Timeframe::W1 => "1w",
        }
    }

    pub fn priority(&self) -> u8 {
        match self {
            Timeframe::M1 => 0,
            Timeframe::M5 => 1,
            Timeframe::H1 => 2,
            Timeframe::D1 => 3,
            Timeframe::W1 => 4,
        }
    }

    pub fn bar_seconds(&self) -> u64 {
        match self {
            Timeframe::M1 => 60,
            Timeframe::M5 => 300,
            Timeframe::H1 => 3600,
            Timeframe::D1 => 86_400,
            Timeframe::W1 => 604_800,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Event {
    Start,
    CandleClosed {
        symbol: Symbol,
        timeframe: Timeframe,
        candle: Candle,
    },
    IndicatorUpdated {
        symbol: Symbol,
        timeframe: Timeframe,
        name: String,
        value: f64,
    },
    Tick {
        timestamp: i64,
    },
    /// Aligned multi-instrument snapshot for a single bar (future: pair trading / dollar strength).
    Snapshot {
        timestamp: i64,
        candles: Vec<(Symbol, Timeframe, Candle)>,
    },
    SetupSignal {
        symbol: Symbol,
        timeframe: Timeframe,
        direction: i8,
        entry: f64,
        stop: f64,
        target: f64,
        /// Maximum hold as a timespan (seconds), not a bar count.
        max_hold_secs: f64,
        /// Bar timestamp when the signal fired (for entry_ts tracking).
        #[serde(default)]
        timestamp: i64,
        /// Which strategy box produced this setup (for per-strategy sizing).
        #[serde(default)]
        source: String,
    },
    TradeSignal {
        symbol: Symbol,
        timeframe: Timeframe,
        direction: i8,
        entry: f64,
        stop: f64,
        target: f64,
        max_hold_secs: f64,
        size: f64,
        /// Bar timestamp when the signal fired.
        #[serde(default)]
        timestamp: i64,
        /// Which strategy box produced this signal.
        #[serde(default)]
        source: String,
    },
    TradeResult {
        /// Realised outcome in R multiples.
        pnl_r: f64,
        reason: String,
        /// Lifecycle detail (additive, backward compatible with older audit files).
        #[serde(default)]
        symbol: Option<String>,
        #[serde(default)]
        timeframe: Option<String>,
        #[serde(default)]
        direction: i8,
        #[serde(default)]
        entry: f64,
        #[serde(default)]
        stop: f64,
        #[serde(default)]
        target: f64,
        #[serde(default)]
        entry_ts: i64,
        #[serde(default)]
        exit_ts: i64,
        #[serde(default)]
        exit_price: f64,
        #[serde(default)]
        size: f64,
        #[serde(default)]
        source: String,
    },
    SwingPivotEvent {
        symbol: Symbol,
        timeframe: Timeframe,
        /// Processor key of the swing box that emitted this pivot. Empty when
        /// replayed from an older audit file; a strategy treats empty as
        /// "unqualified" and accepts it.
        #[serde(default)]
        name: String,
        index: i64,
        /// Confirmation timestamp (bar when the pivot was actually confirmed).
        #[serde(default)]
        timestamp: i64,
        kind: i8,
        price: f64,
        atr: f64,
        #[serde(default)]
        open: f64,
        #[serde(default)]
        high: f64,
        #[serde(default)]
        low: f64,
        #[serde(default)]
        close: f64,
    },
    FetchRequested {
        symbols: Vec<String>,
        timeframes: Vec<String>,
    },
    FetchCompleted {
        symbols: Vec<String>,
        timeframes: Vec<String>,
    },
    /// A component's engine-context snapshot. Components never write their own
    /// state row: they publish this and a single `state-writer` component
    /// coalesces and serialises the writes (write-behind). On startup a
    /// component reads its own key directly.
    ContextState {
        /// Owning component (`ingress`, `enricher`, `strategy`, `broker`).
        role: String,
        /// Bus context, e.g. `test`.
        context: String,
        instrument: String,
        timeframe: String,
        /// Monotonic version of the context (last applied bar). Lets the writer
        /// drop a snapshot that arrives after a newer one.
        version: i64,
        /// Serialised context state, opaque to the bus.
        state: String,
    },
}


pub trait Processor: Send {
    fn name(&self) -> &str;
    fn apply(&mut self, candle: &Candle);
    fn state_json(&self) -> String;
    fn load_json(&mut self, json: &str);
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}
