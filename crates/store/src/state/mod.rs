pub trait StateStore {
    fn save(&mut self, key: &str, value: &str) -> Result<(), String>;
    fn load(&mut self, key: &str) -> Result<Option<String>, String>;
}

pub struct InMemoryStateStore {
    data: std::collections::HashMap<String, String>,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self { data: std::collections::HashMap::new() }
    }
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for InMemoryStateStore {
    fn save(&mut self, key: &str, value: &str) -> Result<(), String> {
        self.data.insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn load(&mut self, key: &str) -> Result<Option<String>, String> {
        Ok(self.data.get(key).cloned())
    }
}

pub struct SqliteStateStore {
    conn: rusqlite::Connection,
}

impl SqliteStateStore {
    pub fn new(path: &str) -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open(path)?;
        // Several components share one state file. WAL lets them queue writes
        // instead of failing immediately with SQLITE_BUSY, and the timeout makes
        // a contended write wait rather than error out silently.
        let _ = conn.busy_timeout(std::time::Duration::from_secs(30));
        let _ = conn.pragma_update(None, "journal_mode", "WAL");
        let _ = conn.pragma_update(None, "synchronous", "NORMAL");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            [],
        )?;
        Ok(Self { conn })
    }
}

impl StateStore for SqliteStateStore {
    fn save(&mut self, key: &str, value: &str) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                [key, value],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn load(&mut self, key: &str) -> Result<Option<String>, String> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM kv WHERE key = ?1")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([key], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?;
        let mut out = None;
        for r in rows {
            if let Ok(v) = r {
                out = Some(v);
            }
        }
        Ok(out)
    }
}

impl SqliteStateStore {
    /// Read-only handle for a component that only needs a cursor (ingress).
    /// Never writes, so it cannot contend with the single writer; a short busy
    /// timeout keeps a reader from stalling a publishing loop for long.
    pub fn open_readonly(path: &str) -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let _ = conn.busy_timeout(std::time::Duration::from_millis(500));
        Ok(Self { conn })
    }

    /// Write a batch of snapshots in one transaction. The write-behind writer
    /// flushes this way so a flush costs one fsync instead of one per row.
    pub fn save_batch(&mut self, rows: &[(String, String)]) -> Result<usize, String> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction().map_err(|e| e.to_string())?;
        {
            let mut stmt = tx
                .prepare(
                    "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                )
                .map_err(|e| e.to_string())?;
            for (k, v) in rows {
                stmt.execute([k, v]).map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(rows.len())
    }
}
