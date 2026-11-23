use crate::bus::{MessageBus};
use crate::context::MarketContext;
use reliant_datamodel::Event;

pub trait Node: Send {
    fn id(&self) -> &str;
    fn subscriptions(&self) -> &[&'static str];
    fn on_event(&mut self, topic: &str, event: &Event, ctx: &mut MarketContext) -> Vec<(String, Event)>;

    /// Register the processors this node keeps in the market context. Called
    /// once when the graph is built, before any event arrives, so a restored
    /// state snapshot has somewhere to load into on a cold start.
    fn init_context(&self, _ctx: &mut MarketContext) {}
}

pub struct Graph {
    nodes: Vec<Box<dyn Node>>,
    pub context: MarketContext,
    context_id: String,
}

impl Graph {
    pub fn new(nodes: Vec<Box<dyn Node>>) -> Self {
        let mut context = MarketContext::new();
        for n in &nodes {
            n.init_context(&mut context);
        }
        Self { nodes, context, context_id: String::new() }
    }

    pub fn with_context(context_id: &str, nodes: Vec<Box<dyn Node>>) -> Self {
        Self { context_id: context_id.to_string(), ..Self::new(nodes) }
    }

    pub fn context_id(&self) -> &str {
        &self.context_id
    }

    pub fn context(&mut self) -> &mut MarketContext {
        &mut self.context
    }

    fn qualified(&self, stage: &str) -> String {
        reliant_messaging::subject(&self.context_id, stage)
    }

    pub fn run(&mut self, initial: Vec<(String, Event)>) -> Vec<Event> {
        let mut all: Vec<(String, Event)> = Vec::new();
        for (topic, e) in initial {
            self.collect(&topic, &e, &mut all);
        }
        all.into_iter().map(|(_, e)| e).collect()
    }

    pub fn run_with_bus(&mut self, initial: Vec<(String, Event)>, bus: &mut dyn MessageBus) -> Vec<Event> {
        let mut all: Vec<(String, Event)> = Vec::new();
        for (topic, e) in initial {
            bus.publish(&self.qualified(&topic), e.clone());
            all.push((topic.clone(), e.clone()));
            let derived = self.derived_from(&topic, &e);
            for (t, e2) in derived {
                bus.publish(&self.qualified(&t), e2.clone());
                all.push((t, e2));
            }
        }
        all.into_iter().map(|(_, e)| e).collect()
    }

    /// Derive events from a single event without bus round-trip.
    /// Returns only newly derived events (input event excluded).
    pub fn compute(&mut self, stage: &str, event: &Event) -> Vec<(String, Event)> {
        self.derived_from(stage, event)
    }

    /// Process a single event through the stage-matched nodes, publishing
    /// derived events onto the bus. Used by long-running component daemons.
    pub fn process(&mut self, stage: &str, event: &Event, bus: &mut dyn MessageBus) -> Vec<(String, Event)> {
        let derived = self.derived_from(stage, event);
        let mut out: Vec<(String, Event)> = Vec::new();
        for (t, e) in derived {
            bus.publish(&self.qualified(&t), e.clone());
            out.push((t, e));
        }
        out
    }

    /// Derive events from a single event without publishing. Used by replay
    /// tools that decide where (and whether) the derived events go.
    pub fn derived(&mut self, stage: &str, event: &Event) -> Vec<(String, Event)> {
        self.derived_from(stage, event)
    }

    fn derived_from(&mut self, stage: &str, event: &Event) -> Vec<(String, Event)> {
        let mut derived = Vec::new();
        for node in self.nodes.iter_mut() {
            if node.subscriptions().contains(&stage) {
                derived.extend(node.on_event(stage, event, &mut self.context));
            }
        }
        let mut out: Vec<(String, Event)> = Vec::new();
        for (t, e) in derived {
            out.push((t.clone(), e.clone()));
            let sub = self.derived_from(&t, &e);
            out.extend(sub);
        }
        out
    }

    fn collect(&mut self, stage: &str, event: &Event, out: &mut Vec<(String, Event)>) {
        out.push((stage.to_string(), event.clone()));
        let derived = self.derived_from(stage, event);
        for (t, e) in derived {
            self.collect(&t, &e, out);
        }
    }
}
