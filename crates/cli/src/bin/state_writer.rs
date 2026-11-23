use clap::Parser;
use reliant_datamodel::Event;
use reliant_messaging::{InMemoryMessageBus, MessageBus, NatsMessageBus, STATE_SUBJECT};
use reliant_store::SqliteStateStore;
use std::collections::HashMap;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "reliant-state-writer",
    about = "Write-behind: persist context snapshots published by the components"
)]
struct Cli {
    #[arg(long)]
    bus: Option<String>,
    #[arg(long)]
    state_db: Option<String>,
    /// Route a role's snapshots to a different file, e.g.
    /// `--role-db broker=/data/state-broker.sqlite`. Repeatable. Roles not
    /// listed go to `--state-db`.
    #[arg(long, value_name = "ROLE=PATH")]
    role_db: Vec<String>,
    /// Per-portfolio routing: `{root}/{portfolio}/state/state-{role}.sqlite`.
    /// Supersedes `--role-db` when given.
    #[arg(long)]
    portfolios_root: Option<String>,
    /// How often buffered snapshots are flushed (milliseconds).
    #[arg(long, default_value = "250")]
    flush_ms: u64,
    /// Never exit on idleness.
    #[arg(long)]
    daemon: bool,
    /// Without `--daemon`, exit once nothing has arrived for this long.
    #[arg(long, default_value = "10000")]
    idle_exit_ms: u64,
    /// Log every flush.
    #[arg(long)]
    verbose: bool,
}

/// One buffered snapshot: (version, role, state).
type Snapshot = (i64, String, String);

fn main() {
    let cli = Cli::parse();
    let state_db = cli.state_db.clone().unwrap_or_else(|| reliant_cli::state_db().to_string_lossy().to_string());
    let portfolios_root = cli.portfolios_root.clone().unwrap_or_else(|| reliant_cli::portfolios_root().to_string_lossy().to_string());
    let shutdown = reliant_cli::shutdown_flag();
    let mut bus: Box<dyn MessageBus> = match &cli.bus {
        Some(url) => Box::new(NatsMessageBus::new(url)),
        None => Box::new(InMemoryMessageBus::new()),
    };

    let mut routes: HashMap<String, String> = HashMap::new();
    for spec in &cli.role_db {
        match spec.split_once('=') {
            Some((role, path)) if !role.is_empty() && !path.is_empty() => {
                routes.insert(role.to_string(), path.to_string());
            }
            _ => eprintln!("state-writer: ignoring malformed --role-db '{}'", spec),
        }
    }

    let mut stores: HashMap<String, SqliteStateStore> = HashMap::new();
    if portfolios_root.is_empty() {
        for path in std::iter::once(&state_db).chain(routes.values()) {
            if !stores.contains_key(path) {
                match SqliteStateStore::new(path) {
                    Ok(s) => {
                        stores.insert(path.clone(), s);
                    }
                    Err(e) => {
                        eprintln!("state-writer: cannot open {}: {}", path, e);
                        std::process::exit(1);
                    }
                }
            }
        }
    }

    let flush_every = Duration::from_millis(cli.flush_ms.max(10));
    let idle_exit = Duration::from_millis(cli.idle_exit_ms);
    // A stale marker from a previous run must not make this process look ready
    // before it has subscribed.
    let ready_file = std::env::var("RELIANT_READY_FILE").ok();
    if let Some(path) = &ready_file {
        let _ = std::fs::remove_file(path);
    }
    let rx = bus.subscribe(STATE_SUBJECT);
    if let Some(path) = &ready_file {
        if let Err(e) = std::fs::write(path, "state-writer ready\n") {
            eprintln!("state-writer: cannot write ready file {}: {}", path, e);
        }
    }

    // Latest snapshot per key, coalesced: a burst of updates for one context
    // costs a single row write on the next flush.
    let mut pending: HashMap<String, Snapshot> = HashMap::new();
    let mut written: HashMap<String, i64> = HashMap::new();
    let mut last_recv = std::time::Instant::now();
    let mut flushed_any = false;
    let mut total_rows = 0usize;
    let mut received = 0usize;

    println!("state-writer: listening on {} -> {}", STATE_SUBJECT, state_db);

    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        match rx.recv_timeout(flush_every) {
            Ok(ev) => {
                last_recv = std::time::Instant::now();
                if let Event::ContextState {
                    role,
                    context,
                    instrument,
                    timeframe,
                    version,
                    state,
                } = ev
                {
                    let key = format!("{}:{}:{}:{}:state", context, instrument, timeframe, role);
                    received += 1;
                    // Drop a snapshot older than one already applied, so a late
                    // publish cannot roll the stored state backwards.
                    if let Some(v) = written.get(&key) {
                        if version < *v {
                            continue;
                        }
                    }
                    pending.insert(key, (version, role, state));
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        if !pending.is_empty() {
            total_rows += flush(&mut pending, &mut written, &mut stores, &routes, &state_db, &portfolios_root, cli.verbose, &mut flushed_any);
        }

        if !cli.daemon && flushed_any && last_recv.elapsed() >= idle_exit {
            break;
        }
    }

    // Final flush: nothing buffered survives shutdown.
    total_rows += flush(&mut pending, &mut written, &mut stores, &routes, &state_db, &portfolios_root, cli.verbose, &mut flushed_any);

    println!(
        "state-writer: shutting down: received={} rows_written={}",
        received, total_rows
    );
}

/// Group the buffered snapshots by destination file and write each group in one
/// transaction. Failed groups go back into `pending` so a transient error never
/// loses a snapshot.
fn flush(
    pending: &mut HashMap<String, Snapshot>,
    written: &mut HashMap<String, i64>,
    stores: &mut HashMap<String, SqliteStateStore>,
    routes: &HashMap<String, String>,
    state_db: &str,
    portfolios_root: &str,
    verbose: bool,
    flushed_any: &mut bool,
) -> usize {
    let mut by_path: HashMap<String, Vec<(String, Snapshot)>> = HashMap::new();
    for (key, snap) in pending.drain() {
        let path = destination(&key, &snap.1, routes, state_db, portfolios_root);
        by_path.entry(path).or_default().push((key, snap));
    }

    let mut written_rows = 0usize;
    for (path, rows) in by_path {
        let mut batch: Vec<(String, String)> = Vec::with_capacity(rows.len() * 2);
        for (key, (version, _role, state)) in &rows {
            batch.push((key.clone(), state.clone()));
            // The enricher owns the pipeline cursor: its snapshot version is the
            // last bar it consumed. Pin that under a stable key so ingress can
            // read it without parsing the component's state JSON.
            if let Some(base) = key.strip_suffix(":state") {
                batch.push((format!("{base}:cursor"), version.to_string()));
            }
        }

        if !stores.contains_key(&path) {
            if let Some(parent) = std::path::Path::new(&path).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match SqliteStateStore::new(&path) {
                Ok(s) => {
                    stores.insert(path.clone(), s);
                }
                Err(e) => {
                    eprintln!("state-writer: cannot open {}: {}", path, e);
                    for (k, snap) in rows {
                        pending.insert(k, snap);
                    }
                    continue;
                }
            }
        }

        let Some(store) = stores.get_mut(&path) else {
            eprintln!("state-writer: no store for {}", path);
            continue;
        };
        match store.save_batch(&batch) {
            Ok(n) => {
                written_rows += n;
                *flushed_any = true;
                for (k, (version, _, _)) in &rows {
                    written.insert(k.clone(), *version);
                }
                if verbose {
                    println!("state-writer: flushed {} rows -> {}", n, path);
                }
            }
            Err(e) => {
                eprintln!("state-writer: flush to {} failed: {}", path, e);
                for (k, snap) in rows {
                    pending.insert(k, snap);
                }
            }
        }
    }
    written_rows
}

/// Where one snapshot lives. Keys are `{portfolio}:{instrument}:{tf}:{role}:state`,
/// so the destination is derivable from the key alone — a snapshot can never be
/// misrouted to another portfolio's file.
fn destination(
    key: &str,
    role: &str,
    routes: &HashMap<String, String>,
    state_db: &str,
    portfolios_root: &str,
) -> String {
    if !portfolios_root.is_empty() {
        let portfolio = key.split(':').next().unwrap_or("unknown");
        format!(
            "{}/{}/state/state-{}.sqlite",
            portfolios_root.trim_end_matches('/'),
            portfolio,
            role
        )
    } else {
        routes
            .get(role)
            .cloned()
            .unwrap_or_else(|| state_db.to_string())
    }
}
