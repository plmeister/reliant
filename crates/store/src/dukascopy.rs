use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use reliant_datamodel::{Candle, Symbol, Timeframe};

/// Map a canonical instrument name (as used in portfolio.json) to the
/// Dukascopy datafeed instrument code. Dukascopy uses its own internal codes
/// (e.g. "DEUIDXEUR" for the DAX), not the canonical names, so an explicit
/// mapping is required. Codes marked UNVERIFIED have not been confirmed
/// against the live datafeed and may need adjustment.
pub fn canonical_to_dukascopy(sym: &str) -> String {
    match sym.to_uppercase().as_str() {
        "GER30" => "DEUIDXEUR".to_string(),   // verified
        "SPX500" => "USA500IDXUSD".to_string(), // unverified
        "UK100" => "UKIDXGBP".to_string(),     // unverified
        "UKOIL" => "XBRUSD".to_string(),        // unverified (Brent; use XTIUSD for WTI)
        "COPPER" => "XCUUSD".to_string(),       // unverified
        "AAPL" => "AAPL".to_string(),           // unverified (stocks may not be on Dukascopy)
        other => other.replace("/", "").replace("-", ""),
    }
}

pub fn timeframe_minutes(tf: &Timeframe) -> i64 {
    match tf {
        Timeframe::M1 => 1,
        Timeframe::M5 => 5,
        Timeframe::H1 => 60,
        Timeframe::D1 => 1440,
        Timeframe::W1 => 10080,
    }
}

pub fn min1_url(instrument: &str, date: NaiveDate) -> String {
    let (y, m, d) = (date.year(), date.month() as i32 - 1, date.day());
    format!(
        "https://datafeed.dukascopy.com/datafeed/{}/{}/{:02}/{:02}/BID_candles_min_1.bi5",
        instrument, y, m, d
    )
}

pub fn parse_bi5(bytes: &[u8], date: NaiveDate, price_scale: f64) -> Result<Vec<Candle>, String> {
    let mut blob = Vec::new();
    let mut input = std::io::Cursor::new(bytes);
    lzma_rs::lzma_decompress(&mut input, &mut blob)
        .map_err(|e| format!("lzma decompress failed: {:?}", e))?;
    let record_size = 24;
    let day_start = date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
    let mut out = Vec::new();
    for chunk in blob.chunks(record_size) {
        if chunk.len() < record_size {
            break;
        }
        let sec = u32::from_be_bytes(chunk[0..4].try_into().unwrap()) as i64;
        let o = u32::from_be_bytes(chunk[4..8].try_into().unwrap()) as f64 / price_scale;
        let c = u32::from_be_bytes(chunk[8..12].try_into().unwrap()) as f64 / price_scale;
        let l = u32::from_be_bytes(chunk[12..16].try_into().unwrap()) as f64 / price_scale;
        let h = u32::from_be_bytes(chunk[16..20].try_into().unwrap()) as f64 / price_scale;
        let v = f32::from_be_bytes(chunk[20..24].try_into().unwrap()) as f64;
        let ts = day_start + sec;
        out.push(Candle {
            timestamp: Utc.timestamp_opt(ts, 0).unwrap(),
            open: o,
            high: h,
            low: l,
            close: c,
            volume: v,
        });
    }
    Ok(out)
}

pub fn aggregate(candles: Vec<Candle>, period_minutes: i64) -> Vec<Candle> {
    if candles.is_empty() {
        return Vec::new();
    }
    let period_secs = period_minutes * 60;
    let mut out = Vec::new();
    let mut bucket: Option<(i64, Candle)> = None;
    for c in candles {
        let ts = c.timestamp.timestamp();
        let b = ts / period_secs * period_secs;
        match &mut bucket {
            Some((bt, agg)) if *bt == b => {
                agg.high = agg.high.max(c.high);
                agg.low = agg.low.min(c.low);
                agg.close = c.close;
                agg.volume += c.volume;
            }
            _ => {
                if let Some((_, agg)) = bucket.take() {
                    out.push(agg);
                }
                bucket = Some((
                    b,
                    Candle {
                        timestamp: Utc.timestamp_opt(b, 0).unwrap(),
                        open: c.open,
                        high: c.high,
                        low: c.low,
                        close: c.close,
                        volume: c.volume,
                    },
                ));
            }
        }
    }
    if let Some((_, agg)) = bucket {
        out.push(agg);
    }
    out
}

pub struct DukascopyProvider {
    pub price_scale: f64,
    client: reqwest::blocking::Client,
    /// Delay between day requests to stay under the datafeed's rate limit.
    pub request_delay: std::time::Duration,
    pub max_attempts: u32,
}

impl DukascopyProvider {
    pub fn new(price_scale: f64) -> Self {
        Self::with_delay(price_scale, std::time::Duration::from_millis(150))
    }

    pub fn with_delay(price_scale: f64, request_delay: std::time::Duration) -> Self {
        // The public datafeed throttles/drops clients without a browser-like UA and referer.
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            )
            .default_headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(
                    reqwest::header::REFERER,
                    reqwest::header::HeaderValue::from_static("https://www.dukascopy.com/"),
                );
                h
            })
            .build()
            .unwrap_or_else(|_| reqwest::blocking::Client::new());
        Self { price_scale, client, request_delay, max_attempts: 5 }
    }

    /// Fetch one day, retrying on 429/5xx/network errors with exponential backoff.
    /// Returns `Ok(None)` for days with no data (holidays, not yet published).
    fn fetch_day(&self, instrument: &str, date: NaiveDate) -> Result<Option<Vec<Candle>>, String> {
        let url = min1_url(instrument, date);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self.client.get(&url).send() {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let bytes = resp.bytes().map_err(|e| e.to_string())?;
                        if bytes.is_empty() {
                            return Ok(None);
                        }
                        return parse_bi5(&bytes, date, self.price_scale).map(Some);
                    }
                    let retryable = status.as_u16() == 429 || status.is_server_error();
                    if !retryable || attempt >= self.max_attempts {
                        if retryable {
                            return Err(format!("rate limited (http {}) for {}", status, url));
                        }
                        return Ok(None);
                    }
                }
                Err(e) => {
                    if attempt >= self.max_attempts {
                        return Err(format!("{} for {}", e, url));
                    }
                }
            }
            let backoff = std::time::Duration::from_millis(500 * 2u64.pow(attempt.min(5)));
            std::thread::sleep(backoff);
        }
    }

    pub fn fetch(
        &self,
        symbol: &Symbol,
        timeframe: &Timeframe,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<Candle>, String> {
        let instrument = canonical_to_dukascopy(&symbol.0);
        let period = timeframe_minutes(timeframe);
        let mut all = Vec::new();
        let mut date = from;
        let mut skipped = 0usize;
        let mut failed = 0usize;
        while date < to {
            match self.fetch_day(&instrument, date) {
                Ok(Some(mins)) => all.extend(aggregate(mins, period)),
                Ok(None) => skipped += 1,
                Err(e) => {
                    eprintln!("dukascopy: {} {}: {}", instrument, date, e);
                    failed += 1;
                }
            }
            std::thread::sleep(self.request_delay);
            date = date.succ_opt().ok_or_else(|| "date overflow".to_string())?;
        }
        if skipped > 0 || failed > 0 {
            eprintln!(
                "dukascopy: {} skipped {} empty days, {} failed days",
                instrument, skipped, failed
            );
        }
        Ok(all)
    }
}
