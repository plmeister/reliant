use crate::context::MarketContext;
use crate::graph::Node;
use reliant_datamodel::{Event, Symbol, Timeframe};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;

/// Complete trade record written to the JSONL log.
#[derive(Debug, Serialize)]
struct TradeRecord {
    symbol: String,
    timeframe: String,
    direction: i8,
    entry: f64,
    stop: f64,
    target: f64,
    entry_ts: i64,
    exit_ts: i64,
    exit_price: f64,
    reason: String,
    pnl_r: f64,
    size: f64,
    #[serde(default)]
    source: String,
}

/// A node that subscribes to the `trade` topic and appends complete trade
/// lifecycle records to a JSONL file. Write-behind: buffers records in memory
/// and flushes every `flush_every` records or on explicit `flush()`.
///
/// Open trades are tracked in memory; closed trades are correlated with the
/// most recent pending open for that (symbol, timeframe) pair.
pub struct TradeLogger {
    id: String,
    path: PathBuf,
    flush_every: usize,
    buffer: Mutex<Vec<TradeRecord>>,
    writer: Mutex<Option<BufWriter<File>>>,

}

impl TradeLogger {
    pub fn new(id: &str, path: PathBuf) -> Self {
        Self::with_flush(id, path, 10)
    }

    pub fn with_flush(id: &str, path: PathBuf, flush_every: usize) -> Self {
        Self {
            id: id.to_string(),
            path,
            flush_every,
            buffer: Mutex::new(Vec::new()),
            writer: Mutex::new(None),
        }
    }

    /// Explicitly flush the buffer to disk.
    pub fn flush(&self) {
        let mut buf = self.buffer.lock().unwrap();
        if buf.is_empty() {
            return;
        }
        let mut w = self.writer.lock().unwrap();
        if w.is_none() {
            if let Some(parent) = self.path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Ok(f) = OpenOptions::new().create(true).append(true).open(&self.path) {
                *w = Some(BufWriter::new(f));
            }
        }
        if let Some(writer) = w.as_mut() {
            for rec in buf.drain(..) {
                if let Ok(json) = serde_json::to_string(&rec) {
                    let _ = writeln!(writer, "{}", json);
                }
            }
            let _ = writer.flush();
        }
    }
}

impl Drop for TradeLogger {
    fn drop(&mut self) {
        self.flush();
    }
}

impl Node for TradeLogger {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscriptions(&self) -> &[&'static str] {
        &["trade"]
    }

    fn on_event(&mut self, _topic: &str, event: &Event, _ctx: &mut MarketContext) -> Vec<(String, Event)> {
        match event {
            Event::TradeResult { symbol, timeframe, direction, entry, stop, target, entry_ts, exit_ts, exit_price, reason, pnl_r, size, source, .. } => {
                let sym = symbol.clone().unwrap_or_default();
                let tf = timeframe.clone().unwrap_or_default();
                let rec = TradeRecord {
                    symbol: sym,
                    timeframe: tf,
                    direction: *direction,
                    entry: *entry,
                    stop: *stop,
                    target: *target,
                    entry_ts: *entry_ts,
                    exit_ts: *exit_ts,
                    exit_price: *exit_price,
                    reason: reason.clone(),
                    pnl_r: *pnl_r,
                    size: *size,
                    source: source.clone(),
                };
                let mut buf = self.buffer.lock().unwrap();
                buf.push(rec);
                if buf.len() >= self.flush_every {
                    drop(buf);
                    self.flush();
                }
            }
            _ => {}
        }
        Vec::new()
    }
}