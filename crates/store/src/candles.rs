use reliant_datamodel::{Candle, Symbol, Timeframe};
use rusqlite::Connection;

pub trait CandleStore {
    fn insert(&mut self, symbol: &Symbol, timeframe: &Timeframe, candles: &[Candle]) -> Result<(), String>;
    fn range(&self, symbol: &Symbol, timeframe: &Timeframe, from_ts: Option<i64>, to_ts: Option<i64>) -> Result<Vec<Candle>, String>;
    fn last_ts(&self, symbol: &Symbol, timeframe: &Timeframe) -> Result<Option<i64>, String>;
    fn symbols(&self) -> Result<Vec<String>, String>;
    fn watermark(&self, symbol: &Symbol, timeframe: &Timeframe) -> Result<Option<(i64, i64)>, String>;
    fn watermarks(&self) -> Result<Vec<(String, String, i64, i64)>, String>;
}

pub struct SqliteCandleStore {
    conn: Connection,
}

impl SqliteCandleStore {
    pub fn new(path: &str) -> Result<Self, String> {
        let conn = Connection::open(path).map_err(|e| e.to_string())?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                PRIMARY KEY (symbol, timeframe, timestamp)
            )",
            [],
        ).map_err(|e| e.to_string())?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_candles_range ON candles (symbol, timeframe, timestamp)",
            [],
        ).map_err(|e| e.to_string())?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS watermarks (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                earliest_ts INTEGER NOT NULL,
                latest_ts INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (symbol, timeframe)
            )",
            [],
        ).map_err(|e| e.to_string())?;
        Ok(Self { conn })
    }

    pub fn in_memory() -> Result<Self, String> {
        Self::new(":memory:")
    }

    /// Open an existing store without running DDL and without ever taking a
    /// write lock. Replay and audit tools use this so they can read a store
    /// that is owned by another uid (the container runtime) and is also open
    /// for writes by the fetcher.
    pub fn open_read_only(path: &str) -> Result<Self, String> {
        let conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| e.to_string())?;
        Ok(Self { conn })
    }
}

impl CandleStore for SqliteCandleStore {
    fn insert(&mut self, symbol: &Symbol, timeframe: &Timeframe, candles: &[Candle]) -> Result<(), String> {
        let tx = self.conn.transaction().map_err(|e| e.to_string())?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
            ).map_err(|e| e.to_string())?;
            for c in candles {
                stmt.execute(rusqlite::params![
                    symbol.0, timeframe.as_str(), c.timestamp.timestamp(),
                    c.open, c.high, c.low, c.close, c.volume
                ]).map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())?;
        // update watermark
        if !candles.is_empty() {
            let earliest = candles.iter().map(|c| c.timestamp.timestamp()).min().unwrap();
            let latest = candles.iter().map(|c| c.timestamp.timestamp()).max().unwrap();
            let now = chrono::Utc::now().timestamp();
            self.conn.execute(
                "INSERT INTO watermarks (symbol, timeframe, earliest_ts, latest_ts, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (symbol, timeframe) DO UPDATE SET
                    earliest_ts = MIN(earliest_ts, excluded.earliest_ts),
                    latest_ts = MAX(latest_ts, excluded.latest_ts),
                    updated_at = excluded.updated_at",
                rusqlite::params![symbol.0, timeframe.as_str(), earliest, latest, now],
            ).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn range(&self, symbol: &Symbol, timeframe: &Timeframe, from_ts: Option<i64>, to_ts: Option<i64>) -> Result<Vec<Candle>, String> {
        let mut sql = String::from(
            "SELECT timestamp, open, high, low, close, volume FROM candles WHERE symbol=?1 AND timeframe=?2"
        );
        if from_ts.is_some() { sql.push_str(" AND timestamp >= ?3"); }
        if to_ts.is_some() { sql.push_str(" AND timestamp < ?4"); }
        sql.push_str(" ORDER BY timestamp");
        let mut stmt = self.conn.prepare(&sql).map_err(|e| e.to_string())?;
        let rows = if from_ts.is_some() && to_ts.is_some() {
            stmt.query_map(rusqlite::params![symbol.0, timeframe.as_str(), from_ts.unwrap(), to_ts.unwrap()], row_to_candle)
        } else if from_ts.is_some() {
            stmt.query_map(rusqlite::params![symbol.0, timeframe.as_str(), from_ts.unwrap()], row_to_candle)
        } else if to_ts.is_some() {
            stmt.query_map(rusqlite::params![symbol.0, timeframe.as_str(), to_ts.unwrap()], row_to_candle)
        } else {
            stmt.query_map(rusqlite::params![symbol.0, timeframe.as_str()], row_to_candle)
        }.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for r in rows { if let Ok(c) = r { out.push(c); } }
        Ok(out)
    }

    fn last_ts(&self, symbol: &Symbol, timeframe: &Timeframe) -> Result<Option<i64>, String> {
        let mut stmt = self.conn.prepare(
            "SELECT MAX(timestamp) FROM candles WHERE symbol=?1 AND timeframe=?2"
        ).map_err(|e| e.to_string())?;
        let v: Option<i64> = stmt.query_row(rusqlite::params![symbol.0, timeframe.as_str()], |row| row.get(0)).map_err(|e| e.to_string())?;
        Ok(v)
    }

    fn symbols(&self) -> Result<Vec<String>, String> {
        let mut stmt = self.conn.prepare("SELECT DISTINCT symbol FROM candles ORDER BY symbol").map_err(|e| e.to_string())?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0)).map_err(|e| e.to_string())?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    fn watermark(&self, symbol: &Symbol, timeframe: &Timeframe) -> Result<Option<(i64, i64)>, String> {
        let mut stmt = self.conn.prepare(
            "SELECT earliest_ts, latest_ts FROM watermarks WHERE symbol=?1 AND timeframe=?2"
        ).map_err(|e| e.to_string())?;
        let res = stmt.query_row(rusqlite::params![symbol.0, timeframe.as_str()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        });
        match res {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    fn watermarks(&self) -> Result<Vec<(String, String, i64, i64)>, String> {
        let mut stmt = self.conn.prepare(
            "SELECT symbol, timeframe, earliest_ts, latest_ts FROM watermarks ORDER BY symbol, timeframe"
        ).map_err(|e| e.to_string())?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?))
        }).map_err(|e| e.to_string())?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }
}

fn row_to_candle(row: &rusqlite::Row) -> rusqlite::Result<Candle> {
    let ts: i64 = row.get(0)?;
    let open: f64 = row.get(1)?;
    let high: f64 = row.get(2)?;
    let low: f64 = row.get(3)?;
    let close: f64 = row.get(4)?;
    let volume: f64 = row.get(5)?;
    Ok(Candle {
        timestamp: chrono::DateTime::from_timestamp(ts, 0).unwrap_or_default(),
        open, high, low, close, volume,
    })
}