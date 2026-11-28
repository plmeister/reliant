use crate::ast::Value;
use crate::compile::{Compiled, CompiledNode};
use crate::spec::Role;
use reliant_datamodel::{Symbol, Timeframe};
use reliant_engine::{
    AtrNode, DummyBroker, EmaNode, GeometryNode, Node, StrategyNode, SwingNode,
};

/// Account values are portfolio-level (never DSL parameters), so the builder
/// takes them from the caller.
#[derive(Debug, Clone, Copy)]
pub struct BuildOptions {
    pub balance: f64,
    pub risk_pct: f64,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self { balance: 10_000.0, risk_pct: 0.01 }
    }
}

/// Instantiate the boxes owned by one component. The returned nodes keep the
/// DSL names as their processor keys, which is what makes the strategy's
/// `ema: ema20` wiring resolvable from the context bag.
pub fn build_role_nodes(
    compiled: &Compiled,
    role: Role,
    symbol: &Symbol,
    timeframe: &Timeframe,
    options: &BuildOptions,
) -> Result<Vec<Box<dyn Node>>, String> {
    let mut out = Vec::new();
    for node in compiled.nodes_for(role) {
        // A box may restrict itself to a specific timeframe (e.g. an HTF
        // indicator). Skip it for other timeframes.
        if let Some(Value::Str(tf)) = node.params.get("timeframe") {
            if !tf.is_empty() && tf != timeframe.as_str() {
                continue;
            }
        }
        out.push(build_node(node, symbol, timeframe, options)?);
    }
    Ok(out)
}

/// Instantiate the whole strategy in execution order. Used by backtests and
/// tests that drive the graph in-process; deployed components use
/// [`build_role_nodes`].
pub fn build_all_nodes(
    compiled: &Compiled,
    symbol: &Symbol,
    timeframe: &Timeframe,
    options: &BuildOptions,
) -> Result<Vec<Box<dyn Node>>, String> {
    compiled
        .nodes
        .iter()
        .map(|node| build_node(node, symbol, timeframe, options))
        .collect()
}

/// Instantiate a single named box. Multi-portfolio components need to control
/// multiplicity (one strategy box per portfolio, one broker per instrument), so
/// they build box-by-box rather than role-by-role.
pub fn build_box(
    compiled: &Compiled,
    name: &str,
    symbol: &Symbol,
    timeframe: &Timeframe,
    options: &BuildOptions,
) -> Result<Box<dyn Node>, String> {
    let node = compiled
        .node(name)
        .ok_or_else(|| format!("strategy has no box named {:?}", name))?;
    build_node(node, symbol, timeframe, options)
}

fn build_node(
    node: &CompiledNode,
    symbol: &Symbol,
    timeframe: &Timeframe,
    options: &BuildOptions,
) -> Result<Box<dyn Node>, String> {
    let id = node.name.as_str();
    Ok(match node.kind.as_str() {
        "ema" => Box::new(EmaNode::with_key(
            id,
            id,
            symbol.clone(),
            timeframe.clone(),
            usize_param(node, "period")?,
        )),
        "atr" => Box::new(AtrNode::with_key(
            id,
            id,
            symbol.clone(),
            timeframe.clone(),
            usize_param(node, "period")?,
        )),
        "swing" => Box::new(SwingNode::with_key(
            id,
            id,
            symbol.clone(),
            timeframe.clone(),
            float_param(node, "k")?,
            usize_param(node, "lookback")?,
        )),
        "filter" => Box::new(reliant_engine::FilterNode::new(
            id,
            &string_param(node, "when")?,
        )),
        "level" => Box::new(reliant_engine::LevelDetector::with_params(
            id,
            usize_param(node, "lookback")?,
            usize_param(node, "min_tests")?,
            float_param(node, "cluster_pct")?,
        )),
        "strategy" => Box::new(StrategyNode::with_params(
            id,
            float_param(node, "k")?,
            float_param(node, "stop_atr_mult")?,
            float_param(node, "target_atr_mult")?,
            float_param(node, "direction_sign")?,
            usize_param(node, "max_hold_bars")?,
            node.inputs.get("htf_ema").map(|s| s.as_str()).unwrap_or("").to_string(),
            string_param(node, "htf_filter")?,
            input(node, "ema")?,
            input(node, "atr")?,
            input(node, "swing")?,
            timeframe.clone(),
        )),
        "candle_signal" => Box::new(reliant_engine::CandleSignal::with_params(
            id,
            &string_param(node, "when")?,
            int_param(node, "direction")? as i8,
            float_param(node, "stop_atr_mult")?,
            float_param(node, "target_atr_mult")?,
            usize_param(node, "max_hold_bars")?,
            input(node, "ema")?,
            input(node, "atr")?,
            input(node, "swing")?,
            string_param(node, "trigger")?,
        ).with_filters(
            float_param(node, "margin").unwrap_or(1.0),
            float_param(node, "vol_mult").unwrap_or(1.5),
            float_param(node, "body_min").unwrap_or(0.5),
            float_param(node, "close_pos_min").unwrap_or(0.7),
        )),
        "sizing" => {
            let sf = string_param(node, "source_filter").unwrap_or_default();
            let sf = if sf == "__auto__" {
                input(node, "setups").unwrap_or("").to_string()
            } else if sf == "*" {
                String::new()  // merge: size all setups
            } else {
                sf
            };
            let risk_raw = float_param(node, "risk_pct").unwrap_or(0.0);
            let risk = if risk_raw > 0.0 { risk_raw } else { options.risk_pct };
            Box::new(GeometryNode::with_source(
                id,
                options.balance,
                risk,
                sf,
            ))
        },
        "broker" => Box::new(DummyBroker::with_params(
            id,
            symbol.clone(),
            timeframe.clone(),
            string_param(node, "entry_mode")?,
            usize_param(node, "limit_valid_bars")?,
        )),
        "oanda_broker" => Box::new(reliant_engine::OandaBroker::new(
            id,
            symbol.clone(),
            timeframe.clone(),
            string_param(node, "entry_mode")?,
        )),
        other => return Err(format!("box {:?}: no runtime builder for kind {:?}", node.name, other)),
    })
}

fn input<'a>(node: &'a CompiledNode, port: &str) -> Result<&'a str, String> {
    node.inputs.get(port).map(|s| s.as_str()).ok_or_else(|| {
        format!("box {:?} is missing resolved input {:?}", node.name, port)
    })
}

fn float_param(node: &CompiledNode, name: &str) -> Result<f64, String> {
    node.params
        .get(name)
        .and_then(|v| v.as_f64())
        .ok_or_else(|| format!("box {:?} parameter {:?} is not numeric", node.name, name))
}

fn usize_param(node: &CompiledNode, name: &str) -> Result<usize, String> {
    node.params
        .get(name)
        .and_then(|v| v.as_i64())
        .and_then(|v| usize::try_from(v).ok())
        .ok_or_else(|| {
            format!("box {:?} parameter {:?} is not a non-negative integer", node.name, name)
        })
}

fn int_param(node: &CompiledNode, name: &str) -> Result<i64, String> {
    node.params
        .get(name)
        .and_then(|v| v.as_i64())
        .ok_or_else(|| format!("box {:?} parameter {:?} is not an integer", node.name, name))
}

fn string_param(node: &CompiledNode, name: &str) -> Result<String, String> {
    node.params
        .get(name)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("box {:?} parameter {:?} is not a string", node.name, name))
}
