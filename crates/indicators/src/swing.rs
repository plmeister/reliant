use reliant_datamodel::{Candle, Processor};
use crate::atr::Atr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PivotKind {
    High,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Pivot {
    pub index: i64,
    pub kind: PivotKind,
    pub price: f64,
    pub atr_confirm: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SwingPivot {
    pub index: i64,
    pub kind: PivotKind,
    pub price: f64,
    pub atr_confirm: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwingDetector {
    name: String,
    k: f64,
    lookback: usize,
    buf: std::collections::VecDeque<(f64, f64, f64, i64, f64)>,
    kept: Vec<Pivot>,
    atr: Atr,
}

impl SwingDetector {
    pub fn new(name: &str, k: f64, lookback: usize) -> Self {
        Self {
            name: name.to_string(),
            k,
            lookback,
            buf: std::collections::VecDeque::new(),
            kept: Vec::new(),
            atr: Atr::new("atr", 14),
        }
    }

    pub fn apply_with_signals(&mut self, candle: &Candle) -> Vec<SwingPivot> {
        let atr = self.atr.update_ohlc(candle.high, candle.low, candle.close).unwrap_or(1.0);
        self.push(candle.high, candle.low, candle.close, candle.timestamp.timestamp(), atr)
    }

    pub fn k(&self) -> f64 {
        self.k
    }

    pub fn push(&mut self, high: f64, low: f64, close: f64, index: i64, atr_confirm: f64) -> Vec<SwingPivot> {
        // Store the bar with its ATR. A pivot is only confirmed once we have
        // `lookback` bars on BOTH sides, so we keep a sliding window of
        // 2*lookback+1 bars and evaluate the middle bar when the window is full.
        self.buf.push_back((high, low, close, index, atr_confirm));
        let need = 2 * self.lookback + 1;
        if self.buf.len() < need {
            return Vec::new();
        }
        let mid = self.lookback;
        let cand = self.buf[mid];
        let mut is_high = true;
        let mut is_low = true;
        for (i, b) in self.buf.iter().enumerate() {
            if i == mid { continue; }
            if b.0 > cand.0 { is_high = false; }
            if b.1 < cand.1 { is_low = false; }
        }
        // Slide the window.
        self.buf.pop_front();

        let mut signals = Vec::new();
        if is_high {
            let p = Pivot { index: cand.3, kind: PivotKind::High, price: cand.0, atr_confirm: cand.4 };
            if let Some(sig) = self.confirm(p) { signals.push(sig); }
        }
        if is_low {
            let p = Pivot { index: cand.3, kind: PivotKind::Low, price: cand.1, atr_confirm: cand.4 };
            if let Some(sig) = self.confirm(p) { signals.push(sig); }
        }
        // Keep only the last two confirmed pivots (current + prior opposite).
        if self.kept.len() > 2 {
            let excess = self.kept.len() - 2;
            self.kept.drain(..excess);
        }
        signals
    }

    fn confirm(&mut self, p: Pivot) -> Option<SwingPivot> {
        let last = self.kept.last().copied();
        match last {
            None => {
                self.kept.push(p);
                None
            }
            Some(l) => {
                if p.kind == l.kind {
                    // Same kind: replace if this pivot is more extreme.
                    if (p.kind == PivotKind::High && p.price > l.price)
                        || (p.kind == PivotKind::Low && p.price < l.price)
                    {
                        self.kept.pop();
                        self.kept.push(p);
                    }
                    None
                } else {
                    self.kept.push(p);
                    let prior = self.kept[self.kept.len() - 2];
                    if (p.price - prior.price).abs() >= self.k * p.atr_confirm {
                        Some(SwingPivot { index: p.index, kind: p.kind, price: p.price, atr_confirm: p.atr_confirm })
                    } else {
                        None
                    }
                }
            }
        }
    }
}

impl Processor for SwingDetector {
    fn name(&self) -> &str {
        &self.name
    }

    fn apply(&mut self, candle: &Candle) {
        let atr = self.atr.update_ohlc(candle.high, candle.low, candle.close).unwrap_or(1.0);
        self.push(candle.high, candle.low, candle.close, candle.timestamp.timestamp(), atr);
    }

    fn state_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn load_json(&mut self, json: &str) {
        if let Ok(s) = serde_json::from_str::<SwingDetector>(json) {
            *self = s;
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
