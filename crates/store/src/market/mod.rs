use std::collections::HashMap;
use reliant_datamodel::{Candle, Symbol, Timeframe};

pub trait MarketDataSource {
    fn candles(&self, symbol: &Symbol, timeframe: &Timeframe) -> Vec<Candle>;
}

pub struct InMemorySource {
    data: HashMap<(String, String), Vec<Candle>>,
}

impl InMemorySource {
    pub fn new() -> Self {
        Self { data: HashMap::new() }
    }

    pub fn insert(&mut self, symbol: &Symbol, timeframe: &Timeframe, candles: Vec<Candle>) {
        self.data.insert((symbol.0.clone(), timeframe.as_str().to_string()), candles);
    }
}

impl Default for InMemorySource {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketDataSource for InMemorySource {
    fn candles(&self, symbol: &Symbol, timeframe: &Timeframe) -> Vec<Candle> {
        self.data
            .get(&(symbol.0.clone(), timeframe.as_str().to_string()))
            .cloned()
            .unwrap_or_default()
    }
}
