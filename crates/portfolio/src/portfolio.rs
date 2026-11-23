use reliant_datamodel::Timeframe;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// On-disk shape of `portfolio.json` (camelCase, tolerant of missing optionals).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioFile {
    pub version: u32,
    pub id: String,
    #[serde(default)]
    pub instruments: Vec<String>,
    #[serde(default)]
    pub timeframes: Vec<String>,
    #[serde(default)]
    pub trade_timeframe: Option<String>,
    #[serde(default)]
    pub context_timeframes: Vec<String>,
    #[serde(default)]
    pub strategy: Option<StrategyConfig>,
    #[serde(default)]
    pub account: Option<AccountConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StrategyConfig {
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub params: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AccountConfig {
    #[serde(default)]
    pub starting_balance: Option<f64>,
    #[serde(default)]
    pub risk_pct: Option<f64>,
}

/// A validated portfolio: identity + analysis universe + strategy binding +
/// capital defaults.
///
/// Ownership split (see `deploy/PORTFOLIOS.md`):
/// - strategy params live here, bound from the portfolio file (not from the
///   strategy itself), so the DSL can later publish parameter slots that
///   portfolios reference and configure.
/// - account fields (starting_balance, risk_pct) are portfolio-level and never
///   come from strategy params.
#[derive(Debug, Clone)]
pub struct Portfolio {
    pub id: String,
    pub instruments: Vec<String>,
    pub timeframes: Vec<Timeframe>,
    pub trade_timeframe: Timeframe,
    pub context_timeframes: Vec<Timeframe>,
    pub strategy_file: String,
    pub strategy_params: BTreeMap<String, serde_json::Value>,
    pub starting_balance: f64,
    pub risk_pct: f64,
    pub path: PathBuf,
}

impl Portfolio {
    pub fn from_file(path: &std::path::Path, dir_name: &str) -> Result<Self, String> {
        let raw: PortfolioFile = serde_json::from_str(
            &std::fs::read_to_string(path).map_err(|e| format!("read {}: {}", path.display(), e))?,
        )
        .map_err(|e| format!("parse {}: {}", path.display(), e))?;

        if raw.version != 1 {
            return Err(format!("unsupported version {} (expected 1)", raw.version));
        }
        if !is_valid_id(&raw.id) {
            return Err(format!("invalid id {:?}", raw.id));
        }
        if raw.id != dir_name {
            return Err(format!(
                "id {:?} does not match directory name {:?}",
                raw.id, dir_name
            ));
        }
        if raw.instruments.is_empty() {
            return Err("instruments must be a non-empty list".to_string());
        }
        if raw.trade_timeframe.is_none() {
            return Err("trade_timeframe is required".to_string());
        }
        let trade_timeframe = Timeframe::parse(
            raw.trade_timeframe
                .as_deref()
                .ok_or("trade_timeframe missing")?,
        )
        .ok_or_else(|| format!("unknown trade_timeframe {:?}", raw.trade_timeframe))?;

        let mut timeframes = Vec::new();
        for t in &raw.timeframes {
            let tf = Timeframe::parse(t).ok_or_else(|| format!("unknown timeframe {}", t))?;
            if !timeframes.contains(&tf) {
                timeframes.push(tf);
            }
        }
        if !timeframes.contains(&trade_timeframe) {
            return Err(format!(
                "trade_timeframe {:?} not present in timeframes {:?}",
                trade_timeframe,
                timeframes
            ));
        }

        let mut context_timeframes = Vec::new();
        for t in &raw.context_timeframes {
            let tf = Timeframe::parse(t).ok_or_else(|| format!("unknown timeframe {}", t))?;
            if !context_timeframes.contains(&tf) {
                context_timeframes.push(tf);
            }
        }

        let strategy = raw.strategy.unwrap_or_default();
        let strategy_file = strategy.file.unwrap_or_default();
        let strategy_params = strategy.params.unwrap_or_default();

        let account = raw.account.unwrap_or_default();
        let starting_balance = account.starting_balance.unwrap_or(10_000.0);
        let risk_pct = account.risk_pct.unwrap_or(0.01);

        Ok(Self {
            id: raw.id,
            instruments: raw.instruments,
            timeframes,
            trade_timeframe,
            context_timeframes,
            strategy_file,
            strategy_params,
            starting_balance,
            risk_pct,
            path: path.to_path_buf(),
        })
    }

    /// All timeframes this portfolio operates on (trade + context).
    pub fn all_timeframes(&self) -> Vec<Timeframe> {
        let mut tf = vec![self.trade_timeframe.clone()];
        for t in &self.context_timeframes {
            if *t != self.trade_timeframe && !tf.contains(t) {
                tf.push(t.clone());
            }
        }
        tf
    }
}

pub fn is_valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

#[derive(Debug, Clone)]
pub struct Registry {
    pub root: std::path::PathBuf,
    portfolios: Vec<Portfolio>,
    by_id: std::collections::HashMap<String, usize>,
    failures: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RegistryError {
    pub loaded: Vec<Portfolio>,
    pub failures: Vec<String>,
}

impl Registry {
    pub fn load(root: &std::path::Path) -> Result<Self, RegistryError> {
        let mut loaded: Vec<Portfolio> = Vec::new();
        let mut failures: Vec<String> = Vec::new();
        let entries = std::fs::read_dir(root).map_err(|e| {
            RegistryError {
                loaded: Vec::new(),
                failures: vec![format!("read {}: {}", root.display(), e)],
            }
        })?;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let dir_name = entry.file_name().to_string_lossy().to_string();
            let file = path.join("portfolio.json");
            if !file.is_file() {
                continue;
            }
            match Portfolio::from_file(&file, &dir_name) {
                Ok(p) => loaded.push(p),
                Err(e) => failures.push(format!("{}: {}", dir_name, e)),
            }
        }
        loaded.sort_by(|a, b| a.id.cmp(&b.id));
        let mut by_id = std::collections::HashMap::new();
        for (i, p) in loaded.iter().enumerate() {
            by_id.insert(p.id.clone(), i);
        }
        Ok(Self { root: root.to_path_buf(), portfolios: loaded, by_id, failures })
    }

    pub fn all(&self) -> &[Portfolio] {
        &self.portfolios
    }

    /// Log-worthy failures from the last load (bad portfolio configs that were
    /// skipped). An unreadable root is an error, not a skip.
    pub fn failures(&self) -> &[String] {
        &self.failures
    }

    pub fn len(&self) -> usize {
        self.portfolios.len()
    }

    pub fn is_empty(&self) -> bool {
        self.portfolios.is_empty()
    }

    pub fn get(&self, id: &str) -> Option<&Portfolio> {
        self.by_id.get(id).map(|i| &self.portfolios[*i])
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut Portfolio> {
        let i = *self.by_id.get(id)?;
        Some(&mut self.portfolios[i])
    }

    /// Union of all portfolios' instruments, sorted + deduped.
    pub fn instrument_universe(&self) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for p in &self.portfolios {
            for i in &p.instruments {
                set.insert(i.clone());
            }
        }
        set.into_iter().collect()
    }

    /// All timeframes a given instrument is analysed under across portfolios.
    pub fn instrument_timeframes(&self, instrument: &str) -> Vec<Timeframe> {
        let mut set = std::collections::HashSet::new();
        for p in &self.portfolios {
            if p.instruments.iter().any(|i| i == instrument) {
                for t in p.all_timeframes() {
                    set.insert(t);
                }
            }
        }
        let mut out: Vec<Timeframe> = set.into_iter().collect();
        out.sort_by_key(|t| t.priority());
        out
    }
}
