use reliant_datamodel::{Candle, Processor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Atr {
    name: String,
    period: usize,
    prev_close: Option<f64>,
    current_atr: Option<f64>,
    count: usize,
    tr_sum: f64,
}

impl Atr {
    pub fn new(name: &str, period: usize) -> Self {
        Self { name: name.to_string(), period, prev_close: None, current_atr: None, count: 0, tr_sum: 0.0 }
    }

    pub fn update_ohlc(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
        let tr = match self.prev_close {
            None => high - low,
            Some(pc) => {
                let hl = high - low;
                let hc = (high - pc).abs();
                let lc = (low - pc).abs();
                hl.max(hc).max(lc)
            }
        };
        self.prev_close = Some(close);
        self.count += 1;

        if self.count <= self.period {
            self.tr_sum += tr;
            if self.count == self.period {
                let atr = self.tr_sum / self.period as f64;
                self.current_atr = Some(atr);
            }
        } else if let Some(prev) = self.current_atr {
            let atr = (prev * (self.period as f64 - 1.0) + tr) / self.period as f64;
            self.current_atr = Some(atr);
        }

        self.current_atr
    }

    pub fn value(&self) -> Option<f64> {
        self.current_atr
    }
}

impl Processor for Atr {
    fn name(&self) -> &str {
        &self.name
    }

    fn apply(&mut self, candle: &Candle) {
        self.update_ohlc(candle.high, candle.low, candle.close);
    }

    fn state_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn load_json(&mut self, json: &str) {
        if let Ok(s) = serde_json::from_str::<Atr>(json) {
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
