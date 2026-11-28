pub mod context;
pub mod expr;
pub mod graph;
pub mod nodes;
pub mod oanda;
pub mod tradelogger;

pub use context::{Context, MarketContext};
pub use expr::{Expr, eval_bool, parse as parse_expr, extract_numbers, substitute_numbers};
pub use graph::{Graph, Node};
pub use reliant_messaging::*;
pub use nodes::{AtrNode, CandleSignal, CandleSource, DummyBroker, EmaNode, FilterNode, GeometryNode, LevelDetector, StrategyNode, SwingNode};
pub use oanda::OandaBroker;
pub use tradelogger::TradeLogger;

pub trait StateMachine {
    fn handle_event(&mut self, event: &reliant_datamodel::Event);
}

pub struct EventProcessor {
    ctx: MarketContext,
}

impl EventProcessor {
    pub fn new() -> Self {
        Self { ctx: MarketContext::new() }
    }

    pub fn handle(&mut self, event: &reliant_datamodel::Event) {
        self.ctx.handle_event(event);
    }
}

impl Default for EventProcessor {
    fn default() -> Self {
        Self::new()
    }
}
