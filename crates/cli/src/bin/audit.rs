//! Trade-fill audit.
//!
//! Replays one portfolio's DSL through the same engine graphs the daemons use,
//! then re-prices every recorded trade under attainable-fill assumptions.
//!
//! The question is mechanical, not statistical: *given the information that
//! existed when the signal fired, could that trade have been entered and exited
//! where the engine says it was?* Three things are checked for every trade:
//!
//! 1. **Temporal placement** — the signal is produced by the bar that just
//!    closed, and the position is only resolved from the next bar onwards.
//! 2. **Fill feasibility** — the recorded entry price has to be reachable after
//!    the decision was made, not just somewhere inside the decision bar's range.
//! 3. **Exit resolution** — stops are re-priced against gaps, and bars that
//!    touch both the stop and the target are counted, because the engine always
//!    assumes the stop came first.
//!
//! Variants then re-simulate the same signal stream with attainable entries so
//! the recorded R can be compared against R that a real fill would have earned.

use clap::Parser;
use reliant_datamodel::{Candle, Event, Symbol, Timeframe};
use reliant_dsl::{BuildOptions, Compiled, Role, build_role_nodes};
use reliant_engine::Graph;
use reliant_store::{CandleStore, SqliteCandleStore};
use serde_json::json;

#[derive(Parser)]
#[command(
    name = "audit",
    about = "Replay a portfolio and audit whether its recorded trades were fillable"
)]
struct Cli {
    #[arg(long)]
    candle_db: Option<String>,
    /// Portfolio directory (contains `portfolio.json` and the strategy file).
    #[arg(long)]
    portfolio: String,
    #[arg(long, default_value = "AUDCAD")]
    instrument: String,
    #[arg(long, default_value = "1d")]
    timeframe: String,
    /// Write one JSON object per recorded trade and per signal to this path.
    #[arg(long)]
    jsonl: Option<String>,
    /// How many bars a resting limit at the pivot price stays valid.
    #[arg(long, default_value_t = 3)]
    limit_valid_bars: usize,
}

/// A setup the engine emitted, whether or not the broker could act on it.
#[derive(Debug, Clone)]
struct Signal {
    bar: usize,
    dir: i8,
    /// Pivot extreme of the decision bar: the price the engine records as entry.
    entry: f64,
    stop: f64,
    max_hold: usize,
}

impl Signal {
    fn atr(&self) -> f64 {
        (self.entry - self.stop).abs() / 1.5
    }
}

/// A trade the engine actually opened.
#[derive(Debug, Clone)]
struct Recorded {
    signal_bar: usize,
    dir: i8,
    entry: f64,
    stop: f64,
    target: f64,
    size: f64,
    exit_bar: Option<usize>,
    exit: Option<f64>,
    reason: Option<String>,
    pnl_r: Option<f64>,
}

/// Replay the compiled strategy bar by bar through role-separated graphs, the
/// same way the daemons do (broker resolves a bar before new setups for that
/// bar are offered to it).
fn replay(
    compiled: &Compiled,
    symbol: &Symbol,
    timeframe: &Timeframe,
    bars: &[Candle],
    balance: f64,
    risk_pct: f64,
) -> Result<(Vec<Signal>, Vec<Recorded>), String> {
    let options = BuildOptions { balance, risk_pct };
    let mut enricher = Graph::with_context(
        "audit",
        build_role_nodes(compiled, Role::Enricher, symbol, timeframe, &options)?,
    );
    let mut strategy = Graph::with_context(
        "audit",
        build_role_nodes(compiled, Role::Strategy, symbol, timeframe, &options)?,
    );
    let mut broker = Graph::with_context(
        "audit",
        build_role_nodes(compiled, Role::Broker, symbol, timeframe, &options)?,
    );

    let mut signals: Vec<Signal> = Vec::new();
    let mut trades: Vec<Recorded> = Vec::new();

    for (i, candle) in bars.iter().enumerate() {
        let closed = Event::CandleClosed {
            symbol: symbol.clone(),
            timeframe: timeframe.clone(),
            candle: candle.clone(),
        };

        // 1. Resolve a position that was already open when this bar closed.
        let resolved = broker.derived("ingress", &closed);
        for (_, event) in &resolved {
            if let Event::TradeResult { pnl_r, reason, .. } = event
                && let Some(trade) = trades.iter_mut().rev().find(|t| t.exit.is_none())
            {
                let r_dist = (trade.entry - trade.stop).abs();
                trade.exit_bar = Some(i);
                trade.exit = Some(trade.entry + trade.dir as f64 * pnl_r * r_dist);
                trade.reason = Some(reason.clone());
                trade.pnl_r = Some(*pnl_r);
            }
        }

        // 2. Enrich the closed bar.
        let enriched = enricher.derived("ingress", &closed);

        // 3. Strategy reacts to the enrichment, broker acts on the setup.
        for (stage, event) in &enriched {
            for (_, setup) in strategy.derived(stage, event) {
                let Event::SetupSignal {
                    direction,
                    entry,
                    stop,
                    target,
                    max_hold_secs,
                    ..
                } = &setup
                else {
                    continue;
                };
                let max_hold = (*max_hold_secs / timeframe.bar_seconds() as f64)
                    .ceil()
                    .max(1.0) as usize;
                signals.push(Signal {
                    bar: i,
                    dir: *direction,
                    entry: *entry,
                    stop: *stop,
                    max_hold,
                });

                let was_active = broker
                    .context()
                    .get(symbol, timeframe)
                    .is_some_and(|c| c.open_trade || c.trade_pending);
                let derived = broker.derived("signal", &setup);
                let now_active = broker
                    .context()
                    .get(symbol, timeframe)
                    .is_some_and(|c| c.open_trade || c.trade_pending);
                let _ = derived;

                if !was_active && now_active {
                    let size = derived
                        .iter()
                        .find_map(|(_, e)| match e {
                            Event::TradeSignal { size, .. } => Some(*size),
                            _ => None,
                        })
                        .unwrap_or(0.0);
                    trades.push(Recorded {
                        signal_bar: i,
                        dir: *direction,
                        entry: *entry,
                        stop: *stop,
                        target: *target,
                        size,
                        exit_bar: None,
                        exit: None,
                        reason: None,
                        pnl_r: None,
                    });
                }
            }
        }
    }

    Ok((signals, trades))
}

#[derive(Debug, Default)]
struct Sim {
    trades: usize,
    wins: usize,
    total_r: f64,
    skipped_busy: usize,
    unresolved: usize,
}

impl Sim {
    fn win_rate(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.wins as f64 / self.trades as f64 * 100.0
        }
    }
}

struct Position {
    entry: f64,
    stop: f64,
    target: f64,
    dir: i8,
    held: usize,
    max_hold: usize,
}

impl Position {
    /// Re-anchor the stop/target around an actual fill, keeping the engine's
    /// 1.5 ATR stop and 3.0 ATR target distances.
    fn at(fill: f64, sig: &Signal) -> Self {
        let atr = sig.atr();
        let d = sig.dir as f64;
        Self {
            entry: fill,
            stop: fill - d * 1.5 * atr,
            target: fill + d * 3.0 * atr,
            dir: sig.dir,
            held: 0,
            max_hold: sig.max_hold,
        }
    }

    fn r_multiple(&self, price: f64) -> f64 {
        let r = (self.entry - self.stop).abs();
        if r > 0.0 {
            self.dir as f64 * (price - self.entry) / r
        } else {
            0.0
        }
    }

    /// Resolve the position against one bar. Stop first (pessimistic), then
    /// target, then timeout at the close. A gap through the stop fills at the
    /// open, which is the price that actually existed.
    fn resolve(&self, bar: &Candle) -> Option<(f64, &'static str)> {
        let d = self.dir as f64;
        let stop_hit = if d > 0.0 {
            bar.low <= self.stop
        } else {
            bar.high >= self.stop
        };
        if stop_hit {
            let fill = if d > 0.0 {
                self.stop.min(bar.open)
            } else {
                self.stop.max(bar.open)
            };
            return Some((fill, "stop"));
        }
        let target_hit = if d > 0.0 {
            bar.high >= self.target
        } else {
            bar.low <= self.target
        };
        if target_hit {
            return Some((self.target, "target"));
        }
        if self.held >= self.max_hold {
            return Some((bar.close, "timeout"));
        }
        None
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Entry {
    /// Market at the close of the bar that produced the signal.
    SignalClose,
    /// Market at the open of the following bar.
    NextOpen,
    /// Resting limit at the pivot price, valid for N bars.
    PivotLimit(usize),
}

fn simulate(bars: &[Candle], signals: &[Signal], mode: Entry) -> Sim {
    let mut sim = Sim::default();
    let mut by_bar: std::collections::HashMap<usize, Vec<usize>> = std::collections::HashMap::new();
    for (i, s) in signals.iter().enumerate() {
        by_bar.entry(s.bar).or_default().push(i);
    }

    let mut pos: Option<Position> = None;
    let mut pending_market: Option<&Signal> = None;
    let mut pending_limit: Option<(&Signal, usize)> = None; // signal + expiry bar

    for (i, bar) in bars.iter().enumerate() {
        // 1. Resolve a position that was carried into this bar.
        if let Some(p) = pos.take() {
            let mut p = p;
            p.held += 1;
            match p.resolve(bar) {
                Some((price, reason)) => {
                    let _ = reason;
                    sim.total_r += p.r_multiple(price);
                    sim.trades += 1;
                    if p.r_multiple(price) > 0.0 {
                        sim.wins += 1;
                    }
                }
                None => pos = Some(p),
            }
        }

        // 2. Market order from the previous bar's signal fills at this open.
        if pos.is_none()
            && let Some(sig) = pending_market.take()
        {
            let mut p = Position::at(bar.open, sig);
            // The fill and the bar are simultaneous: a stop/target touch in
            // the same bar is resolved pessimistically (stop first).
            match p.resolve(bar) {
                Some((price, _)) => {
                    sim.total_r += p.r_multiple(price);
                    sim.trades += 1;
                    if p.r_multiple(price) > 0.0 {
                        sim.wins += 1;
                    }
                }
                None => {
                    p.held = 1;
                    pos = Some(p);
                }
            }
        }

        // 3. Resting limit at the pivot price.
        if pos.is_none() {
            if let Some((_sig, expiry)) = pending_limit
                && i > expiry
            {
                pending_limit = None;
            }

            if let Some((sig, expiry)) = pending_limit {
                let d = sig.dir as f64;
                let touched = if d > 0.0 {
                    bar.low <= sig.entry
                } else {
                    bar.high >= sig.entry
                };
                if touched {
                    // A limit fills at its price, or better when the bar gapped
                    // through it.
                    let gap_through = if d > 0.0 {
                        bar.open < sig.entry
                    } else {
                        bar.open > sig.entry
                    };
                    let fill = if gap_through { bar.open } else { sig.entry };
                    let mut p = Position::at(fill, sig);
                    match p.resolve(bar) {
                        Some((price, _)) => {
                            sim.total_r += p.r_multiple(price);
                            sim.trades += 1;
                            if p.r_multiple(price) > 0.0 {
                                sim.wins += 1;
                            }
                        }
                        None => {
                            p.held = 1;
                            pos = Some(p);
                        }
                    }
                    pending_limit = None;
                } else if i == expiry {
                    pending_limit = None;
                }
            }
        }

        // 4. New signals from this bar.
        if let Some(idxs) = by_bar.get(&i) {
            for &idx in idxs {
                let sig = &signals[idx];
                match mode {
                    Entry::SignalClose => {
                        if pos.is_none() && pending_market.is_none() {
                            pos = Some(Position::at(bar.close, sig));
                        } else {
                            sim.skipped_busy += 1;
                        }
                    }
                    Entry::NextOpen => {
                        if pos.is_none() && pending_market.is_none() {
                            pending_market = Some(sig);
                        } else {
                            sim.skipped_busy += 1;
                        }
                    }
                    Entry::PivotLimit(valid) => {
                        if pos.is_none() && pending_limit.is_none() {
                            pending_limit = Some((sig, i + valid));
                        } else {
                            sim.skipped_busy += 1;
                        }
                    }
                }
            }
        }
    }

    if pos.is_some() {
        sim.unresolved += 1;
    }
    sim
}

fn main() {
    let cli = Cli::parse();
    let candle_db = cli.candle_db.clone().unwrap_or_else(|| reliant_cli::candles_db().to_string_lossy().to_string());
    let tf = reliant_cli::parse_timeframe(&cli.timeframe);
    let dir = std::path::Path::new(&cli.portfolio);
    let portfolio = match reliant_portfolio::Portfolio::from_file(
        &dir.join("portfolio.json"),
        dir.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("portfolio"),
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("audit: {}", e);
            std::process::exit(2);
        }
    };
    let strategy_src = match std::fs::read_to_string(dir.join(&portfolio.strategy_file)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("audit: read strategy: {}", e);
            std::process::exit(2);
        }
    };
    let compiled = match reliant_dsl::compile(&strategy_src, &portfolio.id) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("audit: compile strategy: {}", e);
            std::process::exit(2);
        }
    };

    let store = match SqliteCandleStore::open_read_only(&candle_db) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("audit: open {}: {}", candle_db, e);
            std::process::exit(2);
        }
    };
    let symbol = Symbol(cli.instrument.clone());
    let bars = match store.range(&symbol, &tf, None, None) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("audit: read candles: {}", e);
            std::process::exit(2);
        }
    };
    if bars.is_empty() {
        eprintln!("audit: no candles for {} {}", cli.instrument, cli.timeframe);
        std::process::exit(2);
    }

    let (signals, trades) = match replay(
        &compiled,
        &symbol,
        &tf,
        &bars,
        portfolio.starting_balance,
        portfolio.risk_pct,
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("audit: {}", e);
            std::process::exit(2);
        }
    };

    // ---- recorded result (parity with the live stack) ----------------------
    let recorded_r: f64 = trades.iter().filter_map(|t| t.pnl_r).sum();
    let recorded_wins = trades
        .iter()
        .filter(|t| t.pnl_r.is_some_and(|r| r > 0.0))
        .count();
    println!(
        "# {} {} {}  bars={}",
        portfolio.id,
        cli.instrument,
        cli.timeframe,
        bars.len()
    );
    let recorded_trades = trades.iter().filter(|t| t.pnl_r.is_some()).count();
    println!(
        "recorded    trades={} wins={} win_rate={:.1}% total_r={:.3} signals={}",
        recorded_trades,
        recorded_wins,
        if recorded_trades == 0 {
            0.0
        } else {
            recorded_wins as f64 / recorded_trades as f64 * 100.0
        },
        recorded_r,
        signals.len()
    );

    // ---- per-trade fill diagnostics ---------------------------------------
    let mut entry_at_extreme = 0usize;
    let mut entry_beyond_next_open = 0usize;
    let mut gap_through_stop = 0usize;
    let mut both_touched = 0usize;
    let mut limit_filled = 0usize;
    let mut favour_atr = 0.0f64;
    let mut slip_atr = 0.0f64;
    let mut counted = 0usize;
    let mut records = Vec::new();

    for t in &trades {
        let Some(sb) = bars.get(t.signal_bar) else {
            continue;
        };
        let atr = (t.entry - t.stop).abs() / 1.5;
        let d = t.dir as f64;
        // Should always be true: the engine enters at the decision bar extreme.
        let at_extreme = if d > 0.0 {
            (t.entry - sb.low).abs() < 1e-12
        } else {
            (t.entry - sb.high).abs() < 1e-12
        };
        if at_extreme {
            entry_at_extreme += 1;
        }
        // Positive = the recorded entry was better (cheaper to buy / dearer to
        // sell) than a price that actually existed after the decision.
        favour_atr += (sb.close - t.entry) * d / atr;
        if let Some(nb) = bars.get(t.signal_bar + 1) {
            if (t.entry - nb.open) * d < 0.0 {
                entry_beyond_next_open += 1;
            }
            slip_atr += (nb.open - t.entry) * d / atr;
        }
        // Could a resting limit at the pivot price have filled at all?
        let horizon = (t.signal_bar + 1 + cli.limit_valid_bars).min(bars.len());
        let filled = bars[t.signal_bar + 1..horizon].iter().any(|b| {
            if d > 0.0 {
                b.low <= t.entry
            } else {
                b.high >= t.entry
            }
        });
        if filled {
            limit_filled += 1;
        }
        if let Some(xi) = t.exit_bar {
            let xb = &bars[xi];
            if d > 0.0 && xb.open < t.stop {
                gap_through_stop += 1;
            }
            if d < 0.0 && xb.open > t.stop {
                gap_through_stop += 1;
            }
            let stop_touched = if d > 0.0 {
                xb.low <= t.stop
            } else {
                xb.high >= t.stop
            };
            let target_touched = if d > 0.0 {
                xb.high >= t.target
            } else {
                xb.low <= t.target
            };
            if stop_touched && target_touched {
                both_touched += 1;
            }
        }
        counted += 1;
        records.push(json!({
            "signal_bar": t.signal_bar,
            "signal_ts": sb.timestamp.timestamp(),
            "entry": t.entry,
            "stop": t.stop,
            "target": t.target,
            "dir": t.dir,
            "atr": atr,
            "signal_close": sb.close,
            "signal_high": sb.high,
            "signal_low": sb.low,
            "next_open": bars.get(t.signal_bar + 1).map(|b| b.open),
            "entry_at_bar_extreme": at_extreme,
            "entry_better_than_close_atr": (sb.close - t.entry) * d / atr,
            "entry_better_than_next_open_atr": bars.get(t.signal_bar + 1).map(|b| (b.open - t.entry) * d / atr),
            "pivot_limit_filled": filled,
            "exit_bar": t.exit_bar,
            "exit_ts": t.exit_bar.map(|i| bars[i].timestamp.timestamp()),
            "exit": t.exit,
            "reason": t.reason,
            "pnl_r": t.pnl_r,
            "size": t.size,
        }));
    }

    if counted > 0 {
        println!(
            "fills       entry=decision-bar-extreme {}/{}  better-than-next-open {}/{}  \
             mean_favour={:.2}ATR  mean_slip={:.2}ATR  pivot-limit-filled {}/{}",
            entry_at_extreme,
            counted,
            entry_beyond_next_open,
            counted,
            favour_atr / counted as f64,
            slip_atr / counted as f64,
            limit_filled,
            counted
        );
        println!(
            "exits       gap-through-stop {}  stop+target-same-bar {}",
            gap_through_stop, both_touched
        );
    }

    // ---- attainable-fill variants -----------------------------------------
    let variants = [
        ("close", Entry::SignalClose),
        ("next-open", Entry::NextOpen),
        ("pivot-limit", Entry::PivotLimit(cli.limit_valid_bars)),
    ];
    for (name, mode) in variants {
        let sim = simulate(&bars, &signals, mode);
        println!(
            "{:<11} trades={} wins={} win_rate={:.1}% total_r={:.3} avg_r={:.3} skipped_busy={} open_at_end={}",
            name,
            sim.trades,
            sim.wins,
            sim.win_rate(),
            sim.total_r,
            if sim.trades == 0 {
                0.0
            } else {
                sim.total_r / sim.trades as f64
            },
            sim.skipped_busy,
            sim.unresolved
        );
    }

    if let Some(path) = &cli.jsonl {
        let mut out = String::new();
        for r in &records {
            out.push_str(&r.to_string());
            out.push('\n');
        }
        if let Err(e) = std::fs::write(path, out) {
            eprintln!("audit: write {}: {}", path, e);
        }
    }
}
