use reliant_datamodel::Event;
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, SyncSender};
#[cfg(feature = "nats")]
use futures::StreamExt;

#[cfg(feature = "nats")]
pub struct NatsMessageBus {
    client: std::sync::Arc<async_nats::Client>,
    _rt: tokio::runtime::Runtime,
    handle: tokio::runtime::Handle,
}

#[cfg(feature = "nats")]
impl NatsMessageBus {
    pub fn new(url: &str) -> Self {
        let rt = tokio::runtime::Runtime::new().expect("nats bus: tokio runtime");
        let handle = rt.handle().clone();
        let client = rt.block_on(async { async_nats::connect(url).await.expect("nats connect") });
        Self { client: std::sync::Arc::new(client), _rt: rt, handle }
    }

    /// Bridge the async NATS receive task to the synchronous `Receiver` the
    /// components consume from. The bounded async channel provides real
    /// backpressure; a dedicated thread does the blocking hand-off so no
    /// runtime worker is ever blocked.
    fn channel(&self) -> (tokio::sync::mpsc::Sender<Event>, Receiver<Event>) {
        let (async_tx, mut async_rx) = tokio::sync::mpsc::channel::<Event>(4096);
        let (tx, rx) = mpsc::sync_channel(4096);
        std::thread::spawn(move || {
            while let Some(ev) = async_rx.blocking_recv() {
                if tx.send(ev).is_err() {
                    break;
                }
            }
        });
        (async_tx, rx)
    }
}

#[cfg(feature = "nats")]
impl MessageBus for NatsMessageBus {
    fn publish(&mut self, topic: &str, event: Event) {
        let client = self.client.clone();
        let handle = self.handle.clone();
        let topic = topic.to_string();
        let payload = bytes::Bytes::from(serde_json::to_vec(&event).unwrap_or_default());
        let _ = handle.block_on(async { client.publish(topic, payload).await });
    }

    fn subscribe(&mut self, topic: &str) -> Receiver<Event> {
        let (tx, rx) = self.channel();
        let client = self.client.clone();
        // Only report the subscription as established once the server has
        // processed the SUB. Without the flush, a publisher that starts right
        // after this call can have its first messages discarded (core NATS does
        // not queue for future subscribers), which silently loses bars.
        let sub = self
            .handle
            .block_on(async { subscribe_flushed(&client, topic, None).await });
        let mut sub = match sub {
            Some(s) => s,
            None => return rx,
        };
        self.handle.spawn(async move {
            while let Some(msg) = sub.next().await {
                let ev: Result<Event, _> = serde_json::from_slice(&msg.payload[..]);
                if let Ok(ev) = ev {
                    // await capacity: never drop, never block a runtime worker
                    if tx.send(ev).await.is_err() {
                        return;
                    }
                }
            }
        });
        rx
    }

    /// Tagged subscription: the delivered subject is kept, so a wildcard
    /// subscription (`pf.*.exec`) can route events to the portfolio named in the
    /// subject. Routing is the only reason a component needs the subject back;
    /// the event payload deliberately carries no portfolio id.
    fn subscribe_tagged(&mut self, topic: &str) -> Receiver<(String, Event)> {
        let (async_tx, mut async_rx) = tokio::sync::mpsc::channel::<(String, Event)>(4096);
        let (tx, rx) = mpsc::sync_channel(4096);
        std::thread::spawn(move || {
            while let Some(item) = async_rx.blocking_recv() {
                if tx.send(item).is_err() {
                    break;
                }
            }
        });
        let client = self.client.clone();
        let sub = self
            .handle
            .block_on(async { subscribe_flushed(&client, topic, None).await });
        let mut sub = match sub {
            Some(s) => s,
            None => return rx,
        };
        self.handle.spawn(async move {
            while let Some(msg) = sub.next().await {
                let ev: Result<Event, _> = serde_json::from_slice(&msg.payload[..]);
                if let Ok(ev) = ev {
                    if async_tx.send((msg.subject.to_string(), ev)).await.is_err() {
                        return;
                    }
                }
            }
        });
        rx
    }

    fn queue_subscribe(&mut self, topic: &str, group: &str) -> Receiver<Event> {
        let (tx, rx) = self.channel();
        let client = self.client.clone();
        let sub = self
            .handle
            .block_on(async { subscribe_flushed(&client, topic, Some(group)).await });
        let mut sub = match sub {
            Some(s) => s,
            None => return rx,
        };
        self.handle.spawn(async move {
            while let Some(msg) = sub.next().await {
                let ev: Result<Event, _> = serde_json::from_slice(&msg.payload[..]);
                if let Ok(ev) = ev {
                    if tx.send(ev).await.is_err() {
                        return;
                    }
                }
            }
        });
        rx
    }
}

/// Subscribe and wait until the server has processed the SUB. Returning only
/// after `flush` is what makes "I am subscribed" true for a producer started
/// afterwards: core NATS drops messages sent before a subscription exists, and
/// the feeds are bursts, so an unflushed subscribe silently loses bars.
#[cfg(feature = "nats")]
async fn subscribe_flushed(
    client: &async_nats::Client,
    topic: &str,
    group: Option<&str>,
) -> Option<async_nats::Subscriber> {
    let sub = match group {
        Some(group) => client.queue_subscribe(topic.to_string(), group.to_string()).await,
        None => client.subscribe(topic.to_string()).await,
    };
    let sub = match sub {
        Ok(sub) => sub,
        Err(e) => {
            eprintln!("nats: subscribe {} failed: {}", topic, e);
            return None;
        }
    };
    if let Err(e) = client.flush().await {
        eprintln!("nats: flush after subscribe {} failed: {}", topic, e);
        return None;
    }
    Some(sub)
}

pub trait MessageBus {
    fn publish(&mut self, topic: &str, event: Event);
    fn subscribe(&mut self, topic: &str) -> Receiver<Event>;
    /// Subscribe with a NATS queue group so multiple instances share the load.
    fn queue_subscribe(&mut self, topic: &str, group: &str) -> Receiver<Event> {
        let _ = group;
        self.subscribe(topic)
    }
    /// Subscribe and keep the delivered subject alongside the event. Used by
    /// multi-portfolio components that subscribe with a wildcard and route by
    /// `pf.{id}.{stage}`. The default implementation has no subject to report,
    /// so it echoes the subscribed pattern (fine for exact-topic use).
    fn subscribe_tagged(&mut self, topic: &str) -> Receiver<(String, Event)> {
        let rx = self.subscribe(topic);
        let tag = topic.to_string();
        let (tx, out) = mpsc::sync_channel(4096);
        std::thread::spawn(move || {
            while let Ok(ev) = rx.recv() {
                if tx.send((tag.clone(), ev)).is_err() {
                    break;
                }
            }
        });
        out
    }
}

#[derive(Default)]
pub struct InMemoryMessageBus {
    subscribers: HashMap<String, Vec<SyncSender<Event>>>,
}

impl InMemoryMessageBus {
    pub fn new() -> Self {
        Self { subscribers: HashMap::new() }
    }
}

impl MessageBus for InMemoryMessageBus {
    fn publish(&mut self, topic: &str, event: Event) {
        if let Some(senders) = self.subscribers.get(topic) {
            for s in senders {
                // block rather than drop: backpressure instead of silent event loss
                let _ = s.send(event.clone());
            }
        }
    }

    fn subscribe(&mut self, topic: &str) -> Receiver<Event> {
        let (tx, rx) = mpsc::sync_channel(4096);
        self.subscribers.entry(topic.to_string()).or_default().push(tx);
        rx
    }
}

/// Components publish context snapshots on this single global subject. The
/// `state-writer` component is its only subscriber and the only process that
/// opens the state database for writing, so no two components ever contend for
/// the SQLite write lock.
pub const STATE_SUBJECT: &str = "state";

pub fn stage_for_event(event: &Event) -> &'static str {
    match event {
        Event::CandleClosed { .. } | Event::Tick { .. } | Event::Start | Event::Snapshot { .. } => "ingress",
        Event::IndicatorUpdated { .. } | Event::SwingPivotEvent { .. } => "enrichment",
        Event::SetupSignal { .. } => "signal",
        Event::TradeSignal { .. } | Event::TradeResult { .. } => "trade",
        Event::FetchRequested { .. } | Event::FetchCompleted { .. } => "fetch",
        Event::ContextState { .. } => "state",
    }
}

/// Fully-qualified bus subject for an event: `pf.{context}.{stage}`.
/// An empty context yields the bare stage (backtest / single-context use).
pub fn subject(context: &str, stage: &str) -> String {
    if context.is_empty() {
        stage.to_string()
    } else {
        format!("pf.{}.{}", context, stage)
    }
}

pub fn topic_for_event(context: &str, event: &Event) -> String {
    // Context snapshots are cross-context: one writer serves every portfolio
    // and every role, so they go to a single global subject rather than
    // `{context}.state`.
    if matches!(event, Event::ContextState { .. }) {
        return STATE_SUBJECT.to_string();
    }
    subject(context, stage_for_event(event))
}
