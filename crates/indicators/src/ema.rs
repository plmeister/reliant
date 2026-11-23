use crate::Indicator;
use reliant_datamodel::{Candle, Processor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ema {
    name: String,
    period: usize,
    multiplier: f64,
    current: Option<f64>,
    count: usize,
}

impl Ema {
    pub fn new(name: &str, period: usize) -> Self {
        let multiplier = 2.0 / (period as f64 + 1.0);
        Self { name: name.to_string(), period, multiplier, current: None, count: 0 }
    }
}

impl Indicator for Ema {
    type Output = f64;

    fn update(&mut self, val: f64) -> Option<f64> {
        self.count += 1;
        match self.current {
            None => {
                if self.count >= self.period {
                    self.current = Some(val);
                }
            }
            Some(prev) => {
                let next = (val - prev) * self.multiplier + prev;
                self.current = Some(next);
            }
        }
        self.current
    }

    fn value(&self) -> Option<f64> {
        self.current
    }
}

impl Processor for Ema {
    fn name(&self) -> &str {
        &self.name
    }

    fn apply(&mut self, candle: &Candle) {
        self.count += 1;
        let val = candle.close;
        match self.current {
            None => {
                if self.count >= self.period {
                    self.current = Some(val);
                }
            }
            Some(prev) => {
                let next = (val - prev) * self.multiplier + prev;
                self.current = Some(next);
            }
        }
    }

    fn state_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn load_json(&mut self, json: &str) {
        if let Ok(s) = serde_json::from_str::<Ema>(json) {
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
