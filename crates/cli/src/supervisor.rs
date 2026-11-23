//! Multi-portfolio supervisor: one process per role, a bounded worker pool.
//!
//! Each portfolio gets its own `Graph`, its own state file and its own inbound
//! queue. Queues are per portfolio, so work for one portfolio can never block
//! another (no head-of-line stalls). A worker takes a portfolio's queue only if
//! its runtime lock is free — that lock *is* the `busy` flag: `try_lock`, never
//! `lock`, so a busy portfolio is simply skipped and picked up later.
//!
//! Portfolios never talk to each other: every subject is `pf.{id}.{stage}` and
//! every runtime is keyed by id.
//!
//! Nothing in a worker touches the bus. Workers hand `(subject, event)` to a
//! single publisher thread, which is the only owner of the bus for the process.

use crate::{restore_context_state, StateCheckpoint};
use reliant_datamodel::{Event, Symbol, Timeframe};
use reliant_dsl::{BuildOptions, Compiled, Role};
use reliant_engine::{
    AtrNode, DummyBroker, EmaNode, GeometryNode, Graph, Node, StrategyNode, SwingNode,
};
use reliant_messaging::{MessageBus, NatsMessageBus, STATE_SUBJECT};
use reliant_portfolio::{Portfolio, Registry};
use reliant_store::SqliteStateStore;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// How many queued events a worker drains before it releases a portfolio.
const DRAIN_BATCH: usize = 32;

struct Work {
    event: Event,
    /// Warm-up bars advance the enricher's context but are never published.
    warmup: bool,
}

pub struct Runtime {
    pub portfolio: Portfolio,
    pub graph: Graph,
    checkpoints: HashMap<(String, Timeframe), StateCheckpoint>,
    /// Results this portfolio's broker resolved, for the shutdown report.
    pub trades: Vec<(f64, String)>,
    /// Cached enricher output per bar, used to republish without re-running
    /// indicators when a bar is replayed for downstream recovery.
    pub enrichment_cache: HashMap<(String, String, i64), Vec<(String, Event)>>,
}

struct Shared {
    runtimes: Mutex<HashMap<String, Mutex<Runtime>>>,
    queues: Mutex<HashMap<String, Mutex<VecDeque<Work>>>>,
    order: Mutex<Vec<String>>,
    /// Portfolios whose worker panicked. A poisoned runtime would otherwise
    /// look permanently "busy" and silently stall, so it is quarantined and
    /// reported instead.
    failed: Mutex<HashMap<String, AtomicBool>>,
    ready: Mutex<bool>,
    ready_cv: Condvar,
    role: Role,
    root: String,
}

impl Shared {
    fn wake(&self) {
        let mut ready = self.ready.lock().unwrap();
        *ready = true;
        self.ready_cv.notify_one();
    }
}

type Publisher = SyncSender<(String, Event)>;

fn publish(publisher: &Publisher, out: Vec<(String, Event)>) {
    for (topic, event) in out {
        if publisher.send((topic, event)).is_err() {
            return;
        }
    }
}

/// Run one role for every portfolio under `root`, blocking until shutdown.
pub fn run(
    role: Role,
    root: &str,
    workers: usize,
    bus_url: &str,
    shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    let workers = if workers == 0 {
        match role {
            Role::Broker => 2,
            _ => std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        }
    } else {
        workers
    };

    // A stale marker from a previous run must not make this process look ready
    // before it has subscribed (compose healthcheck reads this file).
    let ready_file = std::env::var("RELIANT_READY_FILE").ok();
    if let Some(path) = &ready_file {
        let _ = std::fs::remove_file(path);
    }

    let registry = Registry::load(Path::new(root))
        .map_err(|e| format!("portfolio registry {}: {}", root, e.failures.join("; ")))?;
    for failure in registry.failures() {
        eprintln!(
            "supervisor({}): skipping invalid portfolio: {}",
            role.as_str(),
            failure
        );
    }
    if registry.is_empty() {
        return Err(format!("no portfolios under {}", root));
    }

    let mut runtimes = HashMap::new();
    let mut queues = HashMap::new();
    let mut failed = HashMap::new();
    for portfolio in registry.all() {
        match build_runtime(portfolio, role) {
            Ok(rt) => {
                runtimes.insert(portfolio.id.clone(), Mutex::new(rt));
                queues.insert(portfolio.id.clone(), Mutex::new(VecDeque::new()));
                failed.insert(portfolio.id.clone(), AtomicBool::new(false));
            }
            Err(e) => eprintln!(
                "supervisor({}): portfolio {} not started: {}",
                role.as_str(),
                portfolio.id,
                e
            ),
        }
    }
    if runtimes.is_empty() {
        return Err("no runnable portfolios".to_string());
    }

    let order: Vec<String> = {
        let mut ids: Vec<String> = runtimes.keys().cloned().collect();
        ids.sort();
        ids
    };

    let (pub_tx, pub_rx) = sync_channel::<(String, Event)>(8192);
    let shared = Arc::new(Shared {
        runtimes: Mutex::new(runtimes),
        queues: Mutex::new(queues),
        order: Mutex::new(order),
        failed: Mutex::new(failed),
        ready: Mutex::new(false),
        ready_cv: Condvar::new(),
        role,
        root: root.to_string(),
    });

    // The only bus owner in the process.
    let publisher = {
        let bus_url = bus_url.to_string();
        std::thread::spawn(move || {
            let mut bus: Box<dyn MessageBus> = Box::new(NatsMessageBus::new(&bus_url));
            while let Ok((topic, event)) = pub_rx.recv() {
                bus.publish(&topic, event);
            }
        })
    };

    let mut bus = NatsMessageBus::new(bus_url);
    // Returns only once the server has processed the SUB, so "ready" below is
    // honest: an ingress that starts later cannot lose its first burst.
    let (rx, warmup_rx) = subscribe_role(&mut bus, role);

    let order = shared.order.lock().unwrap();
    for id in order.iter() {
        println!("supervisor({}): portfolio {} ready", role.as_str(), id);
    }
    drop(order);
    // Optional readiness marker for orchestration (compose healthcheck): it is
    // only created after the subscription exists and every runtime is built.
    if let Some(path) = &ready_file {
        if let Err(e) = std::fs::write(path, format!("{} ready\n", role.as_str())) {
            eprintln!(
                "supervisor({}): cannot write ready file {}: {}",
                role.as_str(),
                path,
                e
            );
        }
    }

    // Dispatcher: blocks on the role's stream and enqueues by portfolio.
    let dispatcher = {
        let shared = Arc::clone(&shared);
        let shutdown = shutdown.clone();
        std::thread::spawn(move || dispatch(shared, rx, warmup_rx, shutdown))
    };

    let mut handles = Vec::new();
    for index in 0..workers.max(1) {
        let shared = Arc::clone(&shared);
        let shutdown = shutdown.clone();
        let publisher = pub_tx.clone();
        handles.push(std::thread::spawn(move || {
            worker(index, shared, publisher, shutdown);
        }));
    }

    dispatcher.join().ok();
    for h in handles {
        h.join().ok();
    }

    // Final snapshots: nothing in memory is lost on a clean stop.
    let mut final_out: Vec<(String, Event)> = Vec::new();
    let order = shared.order.lock().unwrap().clone();
    for id in &order {
        let runtimes = shared.runtimes.lock().unwrap();
        if let Some(rt_mutex) = runtimes.get(id) {
            if let Ok(mut rt) = rt_mutex.try_lock() {
                final_out.extend(snapshot_all(&mut rt));
            }
        }
    }
    publish(&pub_tx, final_out);
    drop(pub_tx);
    publisher.join().ok();

    let order = shared.order.lock().unwrap().clone();
    for id in &order {
        let runtimes = shared.runtimes.lock().unwrap();
        if let Some(rt_mutex) = runtimes.get(id) {
            if let Ok(mut rt) = rt_mutex.lock() {
                let wins = rt.trades.iter().filter(|(r, _)| *r > 0.0).count();
                let total_r: f64 = rt.trades.iter().map(|(r, _)| r).sum();
                let last = rt
                    .graph
                    .context()
                    .contexts
                    .values()
                    .filter_map(|c| c.latest)
                    .max()
                    .unwrap_or(0);
                println!(
                    "supervisor({}): portfolio {} trades={} wins={} total_r={:.3} last={}",
                    role.as_str(),
                    id,
                    rt.trades.len(),
                    wins,
                    total_r,
                    last
                );
            }
        }
    }

    Ok(())
}

fn subscribe_role(
    bus: &mut NatsMessageBus,
    role: Role,
) -> (Receiver<(String, Event)>, Option<Receiver<(String, Event)>>) {
    let stage = match role {
        Role::Enricher => "feed",
        Role::Strategy => "enrichment",
        Role::Broker => "exec",
    };
    let rx = bus.subscribe_tagged(&format!("pf.*.{}", stage));
    let warmup = if role == Role::Enricher {
        Some(bus.subscribe_tagged("pf.*.warmup"))
    } else {
        None
    };
    (rx, warmup)
}

fn dispatch(
    shared: Arc<Shared>,
    rx: Receiver<(String, Event)>,
    warmup_rx: Option<Receiver<(String, Event)>>,
    shutdown: Arc<AtomicBool>,
) {
    // The warm-up stream is forwarded onto the same per-portfolio queues so the
    // dispatcher can block on one receiver.
    let warm_handle = warmup_rx.map(|rx| {
        let shared = Arc::clone(&shared);
        let shutdown = shutdown.clone();
        std::thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok((subject, event)) => enqueue(&shared, &subject, event, true),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(_) => break,
                }
            }
            while let Ok((subject, event)) = rx.try_recv() {
                enqueue(&shared, &subject, event, true);
            }
        })
    });

    while !shutdown.load(Ordering::Relaxed) {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok((subject, event)) => enqueue(&shared, &subject, event, false),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Periodically scan for new portfolios (Option A: fs watch via polling).
                scan_and_register(&shared, &shared.root, shared.role);
            }
            Err(_) => break,
        }
    }
    // Drain anything still buffered in the inbound receiver before the
    // workers perform their final queue drain.
    while let Ok((subject, event)) = rx.try_recv() {
        enqueue(&shared, &subject, event, false);
    }
    if let Some(h) = warm_handle {
        h.join().ok();
    }
}

fn enqueue(shared: &Shared, subject: &str, event: Event, warmup: bool) {
    let Some(id) = portfolio_of(subject) else {
        return;
    };
    let queues = shared.queues.lock().unwrap();
    let Some(queue) = queues.get(id) else {
        return;
    };
    queue.lock().unwrap().push_back(Work { event, warmup });
    drop(queues);
    shared.wake();
}

/// `pf.{id}.{stage}` -> `{id}`.
fn portfolio_of(subject: &str) -> Option<&str> {
    let rest = subject.strip_prefix("pf.")?;
    let (id, _stage) = rest.split_once('.')?;
    Some(id)
}

fn worker(index: usize, shared: Arc<Shared>, publisher: Publisher, shutdown: Arc<AtomicBool>) {
    let cursor = AtomicUsize::new(index);
    while !shutdown.load(Ordering::Relaxed) {
        let mut did_work = false;
        let order = shared.order.lock().unwrap().clone();
        let n = order.len();
        if n == 0 {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        for step in 0..n {
            let at = (cursor.load(Ordering::Relaxed) + step) % n;
            let id = order[at].clone();
            if drain(shared.role, &shared, &id, &publisher) {
                did_work = true;
                cursor.store((at + 1) % n, Ordering::Relaxed);
                break;
            }
        }
        if !did_work {
            let ready = shared.ready.lock().unwrap();
            let _ = shared
                .ready_cv
                .wait_timeout(ready, Duration::from_millis(100))
                .unwrap();
        }
    }

    // Graceful stop: the dispatcher has stopped enqueueing, so finishing the
    // queues is bounded. Dropping queued events would silently lose bars —
    // NATS core does not replay and the pipeline cursor has already advanced.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let mut did_work = false;
        let order = shared.order.lock().unwrap().clone();
        for id in &order {
            if drain(shared.role, &shared, id, &publisher) {
                did_work = true;
            }
        }
        if !did_work {
            break;
        }
    }
}

/// Take a portfolio if it is idle, process up to `DRAIN_BATCH` events, release.
/// Returns whether anything was processed.
fn drain(role: Role, shared: &Shared, id: &str, publisher: &Publisher) -> bool {
    {
        let failed = shared.failed.lock().unwrap();
        if let Some(f) = failed.get(id) {
            if f.load(Ordering::Relaxed) {
                return false;
            }
        }
    }
    let queues = shared.queues.lock().unwrap();
    let Some(queue) = queues.get(id) else {
        return false;
    };
    let runtimes = shared.runtimes.lock().unwrap();
    let Some(runtime) = runtimes.get(id) else {
        return false;
    };
    // busy flag: never block on a portfolio another worker owns
    let mut rt = match runtime.try_lock() {
        Ok(rt) => rt,
        Err(std::sync::TryLockError::WouldBlock) => return false,
        Err(std::sync::TryLockError::Poisoned(_)) => {
            // A panic in this portfolio poisoned its runtime. Quarantine it
            // instead of retrying forever: a poisoned lock looks exactly like
            // a busy one, which would stall the portfolio with no log at all.
            eprintln!(
                "supervisor({}): portfolio {} panicked; quarantined until restart",
                role.as_str(),
                id
            );
            let mut failed = shared.failed.lock().unwrap();
            if let Some(f) = failed.get(id) {
                f.store(true, Ordering::Relaxed);
            }
            return false;
        }
    };

    let mut out: Vec<(String, Event)> = Vec::new();
    let mut processed = false;
    for _ in 0..DRAIN_BATCH {
        let work = queue.lock().unwrap().pop_front();
        let Some(work) = work else { break };
        processed = true;
        process(role, &mut rt, work, &mut out);
    }
    // Publish before releasing the portfolio: two workers must never be able
    // to send each other's batches out of order.
    publish(publisher, out);
    drop(rt);
    drop(runtimes);
    drop(queues);
    processed
}

fn process(role: Role, rt: &mut Runtime, work: Work, out: &mut Vec<(String, Event)>) {
    match role {
        Role::Enricher => process_enricher(rt, work, out),
        Role::Strategy => process_strategy(rt, work, out),
        Role::Broker => process_broker(rt, work, out),
    }
}

fn process_enricher(rt: &mut Runtime, work: Work, out: &mut Vec<(String, Event)>) {
    let Event::CandleClosed {
        symbol,
        timeframe,
        candle,
    } = &work.event
    else {
        return;
    };
    let ts = candle.timestamp.timestamp();
    let is_replay = latest(&mut rt.graph, symbol, timeframe).is_some_and(|l| ts <= l);
    let cache_key = (symbol.0.clone(), timeframe.as_str().to_string(), ts);

    if !work.warmup {
        if is_replay {
            // Republish the cached enrichment for this bar instead of
            // re-running indicators, which would mutate already-advanced state.
            if let Some(cached) = rt.enrichment_cache.get(&cache_key) {
                out.extend(cached.clone());
            } else {
                // No cache (e.g. cold start after restart handled as fresh):
                // fall back to re-running, without advancing the cursor.
                out.push((subject(&rt.portfolio.id, "enrichment"), work.event.clone()));
                let derived = rt.graph.derived("ingress", &work.event);
                for (stage, event) in derived {
                    out.push((qualified(rt, &stage), event));
                }
            }
        } else {
            let mut cached = Vec::new();
            cached.push((subject(&rt.portfolio.id, "enrichment"), work.event.clone()));
            out.push(cached[0].clone());
            let derived = rt.graph.derived("ingress", &work.event);
            for (stage, event) in derived {
                let q = qualified(rt, &stage);
                cached.push((q.clone(), event.clone()));
                out.push((q, event));
            }
            rt.enrichment_cache.insert(cache_key, cached);
        }
    } else {
        // Facts from a warm-up bar only advance the enricher's own context.
        let _ = rt.graph.derived("ingress", &work.event);
    }
    if !is_replay {
        mark_latest(&mut rt.graph, symbol, timeframe, ts);
    }
    checkpoint(rt, symbol, timeframe, out);
}

fn process_strategy(rt: &mut Runtime, work: Work, out: &mut Vec<(String, Event)>) {
    match &work.event {
        Event::CandleClosed {
            symbol,
            timeframe,
            candle,
        } => {
            let ts = candle.timestamp.timestamp();
            let is_replay = latest(&mut rt.graph, symbol, timeframe).is_some_and(|l| ts <= l);
            // The closed candle goes out before this bar's setups, on the same
            // subject, so the broker resolves bar N before applying it. Replay
            // must still publish it so downstream roles can catch up.
            out.push((subject(&rt.portfolio.id, "exec"), work.event.clone()));
            let derived = rt.graph.derived("enrichment", &work.event);
            for (stage, event) in derived {
                out.push((qualified(rt, &stage), event));
            }
            if !is_replay {
                mark_latest(&mut rt.graph, symbol, timeframe, ts);
            }
            checkpoint(rt, symbol, timeframe, out);
        }
        Event::SwingPivotEvent {
            symbol,
            timeframe,
            ..
        } => {
            let derived = rt.graph.derived("enrichment", &work.event);
            for (stage, event) in derived {
                // Setups are execution: they go on the exec subject, never on
                // the enrichment subject the strategy also subscribes to.
                let topic = if stage == "signal" {
                    subject(&rt.portfolio.id, "exec")
                } else {
                    qualified(rt, &stage)
                };
                out.push((topic, event));
            }
            checkpoint(rt, symbol, timeframe, out);
        }
        Event::IndicatorUpdated {
            symbol,
            timeframe,
            ..
        } => {
            // Only updates the strategy's indicator slots; derives nothing.
            let _ = rt.graph.derived("enrichment", &work.event);
            checkpoint(rt, symbol, timeframe, out);
        }
        _ => {}
    }
}

fn process_broker(rt: &mut Runtime, work: Work, out: &mut Vec<(String, Event)>) {
    match &work.event {
        Event::CandleClosed {
            symbol,
            timeframe,
            candle,
        } => {
            // Context timeframes travel to the broker on `exec` as well (they
            // are candles the strategy saw), but only the trade timeframe may
            // resolve or count against a position.
            if timeframe != &rt.portfolio.trade_timeframe {
                return;
            }
            let ts = candle.timestamp.timestamp();
            let is_replay = latest(&mut rt.graph, symbol, timeframe).is_some_and(|l| ts <= l);
            let derived = rt.graph.derived("ingress", &work.event);
            if !is_replay {
                mark_latest(&mut rt.graph, symbol, timeframe, ts);
            }
            emit(rt, derived, out);
            checkpoint(rt, symbol, timeframe, out);
        }
        Event::SetupSignal {
            symbol, timeframe, timestamp, entry, stop, ..
        } => {
            let key = format!(
                "{}:{}:{}:{}:{}",
                symbol.0, timeframe.as_str(), timestamp, entry, stop
            );
            let c = rt.graph.context().get_or_create(symbol.clone(), timeframe.clone());
            if !c.seen_setups.insert(key) {
                return;
            }
            let derived = rt.graph.derived("signal", &work.event);
            emit(rt, derived, out);
            checkpoint(rt, symbol, timeframe, out);
        }
        _ => {}
    }
}

fn emit(rt: &mut Runtime, derived: Vec<(String, Event)>, out: &mut Vec<(String, Event)>) {
    for (stage, event) in derived {
        if let Event::TradeResult { pnl_r, reason, .. } = &event {
            rt.trades.push((*pnl_r, reason.clone()));
        }
        out.push((qualified(rt, &stage), event));
    }
}

fn checkpoint(
    rt: &mut Runtime,
    symbol: &Symbol,
    timeframe: &Timeframe,
    out: &mut Vec<(String, Event)>,
) {
    let key = (symbol.0.clone(), timeframe.clone());
    let Some(mut cp) = rt.checkpoints.remove(&key) else {
        return;
    };
    if let Some(event) = cp.snapshot_event(&mut rt.graph) {
        out.push((STATE_SUBJECT.to_string(), event));
    }
    rt.checkpoints.insert(key, cp);
}

fn snapshot_all(rt: &mut Runtime) -> Vec<(String, Event)> {
    let mut out = Vec::new();
    let keys: Vec<(String, Timeframe)> = rt.checkpoints.keys().cloned().collect();
    for key in keys {
        let Some(mut cp) = rt.checkpoints.remove(&key) else {
            continue;
        };
        if let Some(event) = cp.snapshot_event(&mut rt.graph) {
            out.push((STATE_SUBJECT.to_string(), event));
        }
        rt.checkpoints.insert(key, cp);
    }
    out
}

fn qualified(rt: &Runtime, stage: &str) -> String {
    subject(&rt.portfolio.id, stage)
}

fn subject(portfolio: &str, stage: &str) -> String {
    reliant_messaging::subject(portfolio, stage)
}

fn latest(graph: &mut Graph, symbol: &Symbol, timeframe: &Timeframe) -> Option<i64> {
    graph
        .context()
        .contexts
        .get(&(symbol.clone(), timeframe.clone()))
        .and_then(|c| c.latest)
}

fn mark_latest(graph: &mut Graph, symbol: &Symbol, timeframe: &Timeframe, ts: i64) {
    graph
        .context()
        .get_or_create(symbol.clone(), timeframe.clone())
        .latest = Some(ts);
}

fn build_runtime(portfolio: &Portfolio, role: Role) -> Result<Runtime, String> {
    // `Portfolio::path` is the portfolio.json file; the folder holds the
    // strategy and the state directory.
    let dir = portfolio
        .path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| portfolio.path.clone());

    let compiled = if portfolio.strategy_file.is_empty() {
        None
    } else {
        let path = dir.join(&portfolio.strategy_file);
        let source = std::fs::read_to_string(&path)
            .map_err(|e| format!("read strategy {}: {}", path.display(), e))?;
        Some(
            reliant_dsl::compile(&source, &portfolio.id)
                .map_err(|e| format!("strategy {}: {}", path.display(), e))?,
        )
    };

    let nodes = build_nodes(portfolio, compiled.as_ref(), role)?;
    let mut graph = Graph::with_context(&portfolio.id, nodes);

    let tfs = match role {
        Role::Enricher => portfolio.all_timeframes(),
        _ => vec![portfolio.trade_timeframe.clone()],
    };

    let state_dir = dir.join("state");
    std::fs::create_dir_all(&state_dir)
        .map_err(|e| format!("create {}: {}", state_dir.display(), e))?;
    let state_path = state_dir.join(format!("state-{}.sqlite", role.as_str()));
    let mut store = SqliteStateStore::new(
        state_path
            .to_str()
            .ok_or_else(|| format!("non-UTF8 state path {}", state_path.display()))?,
    )
    .map_err(|e| format!("open {}: {}", state_path.display(), e))?;

    let mut checkpoints = HashMap::new();
    for instrument in &portfolio.instruments {
        let symbol = Symbol(instrument.clone());
        for tf in &tfs {
            graph.context().get_or_create(symbol.clone(), tf.clone());
            let checkpoint = StateCheckpoint::new(role.as_str(), &portfolio.id, &symbol, tf);
            restore_context_state(&mut store, checkpoint.key(), &mut graph, &symbol, tf);
            checkpoints.insert((instrument.clone(), tf.clone()), checkpoint);
        }
    }

    Ok(Runtime {
        portfolio: portfolio.clone(),
        graph,
        checkpoints,
        trades: Vec::new(),
        enrichment_cache: HashMap::new(),
    })
}

fn build_nodes(
    portfolio: &Portfolio,
    compiled: Option<&Compiled>,
    role: Role,
) -> Result<Vec<Box<dyn Node>>, String> {
    let dir = portfolio
        .path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| portfolio.path.clone());
    let tfs = match role {
        Role::Enricher => portfolio.all_timeframes(),
        _ => vec![portfolio.trade_timeframe.clone()],
    };

    match compiled {
        Some(compiled) => {
            let options = BuildOptions {
                balance: portfolio.starting_balance,
                risk_pct: portfolio.risk_pct,
            };
            let mut nodes: Vec<Box<dyn Node>> = Vec::new();
            for boxed in compiled.nodes_for(role) {
                let per_instrument =
                    matches!(boxed.kind.as_str(), "ema" | "atr" | "swing" | "broker" | "oanda_broker");
                if per_instrument {
                    for instrument in &portfolio.instruments {
                        for tf in &tfs {
                            // Respect a box's optional `timeframe` restriction
                            // (e.g. an HTF indicator).
                            if let Some(reliant_dsl::ast::Value::Str(tf_param)) = boxed.params.get("timeframe") {
                                if !tf_param.is_empty() && tf_param != tf.as_str() {
                                    continue;
                                }
                            }
                            nodes.push(reliant_dsl::build_box(
                                compiled,
                                &boxed.name,
                                &Symbol(instrument.clone()),
                                tf,
                                &options,
                            )?);
                        }
                    }
                } else {
                    // One instance per portfolio: a duplicated strategy box
                    // would process every event twice.
                    let instrument = portfolio
                        .instruments
                        .first()
                        .ok_or_else(|| "portfolio has no instruments".to_string())?;
                    nodes.push(reliant_dsl::build_box(
                        compiled,
                        &boxed.name,
                        &Symbol(instrument.clone()),
                        &portfolio.trade_timeframe,
                        &options,
                    )?);
                }
            }
            // Sidecar: persist trade lifecycle to trades.jsonl (write-behind).
            if role == Role::Broker {
                let state_dir = dir.join("state");
                let log_path = state_dir.join("trades.jsonl");
                nodes.push(Box::new(reliant_engine::TradeLogger::new(
                    &format!("logger_{}", portfolio.id),
                    log_path,
                )));
            }
            Ok(nodes)
        }
        None => Ok(legacy_nodes(portfolio, role)),
    }
}

/// The pre-DSL graph, kept so a portfolio without `strategy.dsl` still runs.
fn legacy_nodes(portfolio: &Portfolio, role: Role) -> Vec<Box<dyn Node>> {
    let k = portfolio
        .strategy_params
        .get("k")
        .and_then(|v| v.as_f64())
        .unwrap_or(3.5);
    let trade_tf = portfolio.trade_timeframe.clone();
    let tfs = match role {
        Role::Enricher => portfolio.all_timeframes(),
        _ => vec![trade_tf.clone()],
    };

    let mut nodes: Vec<Box<dyn Node>> = Vec::new();
    for instrument in &portfolio.instruments {
        let symbol = Symbol(instrument.clone());
        for tf in &tfs {
            match role {
                Role::Enricher | Role::Strategy => {
                    nodes.push(Box::new(AtrNode::new(
                        &format!("atr_{}", instrument),
                        symbol.clone(),
                        tf.clone(),
                        14,
                    )));
                    nodes.push(Box::new(EmaNode::new(
                        &format!("ema_{}", instrument),
                        symbol.clone(),
                        tf.clone(),
                        20,
                    )));
                    nodes.push(Box::new(SwingNode::new(
                        &format!("swing_{}", instrument),
                        symbol.clone(),
                        tf.clone(),
                        k,
                        5,
                    )));
                }
                Role::Broker => {
                    nodes.push(Box::new(DummyBroker::new(
                        &format!("broker_{}", instrument),
                        symbol.clone(),
                        tf.clone(),
                    )));
                }
            }
        }
    }
    match role {
        Role::Strategy => nodes.push(Box::new(StrategyNode::new("strat", k))),
        Role::Broker => nodes.push(Box::new(GeometryNode::new(
            "geom",
            portfolio.starting_balance,
            portfolio.risk_pct,
        ))),
        Role::Enricher => {}
    }
    nodes
}

/// Register a newly discovered portfolio at runtime (Option A: fs watch).
/// Returns true if the portfolio was successfully registered.
fn register_portfolio(shared: &Shared, root: &str, role: Role, id: &str) -> bool {
    // Check if already registered.
    {
        let order = shared.order.lock().unwrap();
        if order.contains(&id.to_string()) {
            return false;
        }
    }

    // Load the portfolio.
    let portfolio_path = format!("{}/{}", root, id);
    let registry = match Registry::load(Path::new(root)) {
        Ok(r) => r,
        Err(_) => return false,
    };
    let portfolio = match registry.get(id) {
        Some(p) => p.clone(),
        None => return false,
    };

    // Build runtime.
    match build_runtime(&portfolio, role) {
        Ok(rt) => {
            let mut runtimes = shared.runtimes.lock().unwrap();
            let mut queues = shared.queues.lock().unwrap();
            let mut failed = shared.failed.lock().unwrap();
            let mut order = shared.order.lock().unwrap();

            runtimes.insert(id.to_string(), Mutex::new(rt));
            queues.insert(id.to_string(), Mutex::new(VecDeque::new()));
            failed.insert(id.to_string(), AtomicBool::new(false));
            order.push(id.to_string());
            order.sort();

            eprintln!(
                "supervisor({}): dynamically registered portfolio {}",
                role.as_str(),
                id
            );
            true
        }
        Err(e) => {
            eprintln!(
                "supervisor({}): portfolio {} not started: {}",
                role.as_str(),
                id,
                e
            );
            false
        }
    }
}

/// Scan the portfolios root for new portfolios and register them.
fn scan_and_register(shared: &Shared, root: &str, role: Role) {
    if let Ok(rd) = std::fs::read_dir(root) {
        for entry in rd.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let id = entry.file_name().to_string_lossy().to_string();
            if id.starts_with('.') {
                continue;
            }
            register_portfolio(shared, root, role, &id);
        }
    }
}
