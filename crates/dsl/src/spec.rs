use crate::ast::Value;

/// Which process owns a box. Roles map 1:1 to deployed components (plus the
/// ingress infrastructure that is not a DSL box) and therefore to subjects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    Enricher,
    Strategy,
    Broker,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Enricher => "enricher",
            Role::Strategy => "strategy",
            Role::Broker => "broker",
        }
    }
}

/// What a box emits. Connections are type-checked against this, so a swing
/// producer cannot be wired into the EMA slot by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Producer {
    Indicator,
    Swing,
    Setup,
    Trade,
}

impl Producer {
    pub fn as_str(&self) -> &'static str {
        match self {
            Producer::Indicator => "indicator",
            Producer::Swing => "swing",
            Producer::Setup => "setup",
            Producer::Trade => "trade",
        }
    }
}

/// A boundary between components. The topic is derived from the portfolio id,
/// never hard-coded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Channel {
    Feed,
    Enrichment,
    Exec,
}

impl Channel {
    pub fn stage(&self) -> &'static str {
        match self {
            Channel::Feed => "feed",
            Channel::Enrichment => "enrichment",
            Channel::Exec => "exec",
        }
    }

    /// Same spelling as `reliant_messaging::subject`: `pf.{id}.{stage}`,
    /// or the bare stage when no portfolio is bound (single-context tests).
    pub fn subject(&self, portfolio: &str) -> String {
        if portfolio.is_empty() {
            self.stage().to_string()
        } else {
            format!("pf.{}.{}", portfolio, self.stage())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    Int,
    Float,
    String,
    Bool,
}

impl ParamType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ParamType::Int => "integer",
            ParamType::Float => "number",
            ParamType::String => "string",
            ParamType::Bool => "boolean",
        }
    }
}

/// Const-friendly default so kind specs can live in a static table.
#[derive(Debug, Clone, Copy)]
pub enum DefaultValue {
    Int(i64),
    Float(f64),
    Str(&'static str),
    Bool(bool),
}

impl DefaultValue {
    pub fn to_value(self) -> Value {
        match self {
            DefaultValue::Int(v) => Value::Int(v),
            DefaultValue::Float(v) => Value::Float(v),
            DefaultValue::Str(v) => Value::Str(v.to_string()),
            DefaultValue::Bool(v) => Value::Bool(v),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParamSpec {
    pub name: &'static str,
    pub ty: ParamType,
    pub required: bool,
    pub default: Option<DefaultValue>,
}

#[derive(Debug, Clone, Copy)]
pub struct InputSpec {
    pub name: &'static str,
    pub accepts: Producer,
    pub required: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct KindSpec {
    pub name: &'static str,
    pub role: Role,
    /// `None` for sinks (a broker consumes but emits only side effects).
    pub output: Option<Producer>,
    pub params: &'static [ParamSpec],
    pub inputs: &'static [InputSpec],
}

impl KindSpec {
    pub fn param(&self, name: &str) -> Option<&'static ParamSpec> {
        self.params.iter().find(|p| p.name == name)
    }

    pub fn input(&self, name: &str) -> Option<&'static InputSpec> {
        self.inputs.iter().find(|i| i.name == name)
    }
}

const TIMEFRAME_PARAM: ParamSpec = ParamSpec {
    name: "timeframe",
    ty: ParamType::String,
    required: false,
    default: Some(DefaultValue::Str("")),
};

const PERIOD_20: &[ParamSpec] = &[
    ParamSpec {
        name: "period",
        ty: ParamType::Int,
        required: false,
        default: Some(DefaultValue::Int(20)),
    },
    TIMEFRAME_PARAM,
];

const PERIOD_14: &[ParamSpec] = &[
    ParamSpec {
        name: "period",
        ty: ParamType::Int,
        required: false,
        default: Some(DefaultValue::Int(14)),
    },
    TIMEFRAME_PARAM,
];

const SWING_PARAMS: &[ParamSpec] = &[
    ParamSpec {
        name: "k",
        ty: ParamType::Float,
        required: false,
        default: Some(DefaultValue::Float(3.5)),
    },
    ParamSpec {
        name: "lookback",
        ty: ParamType::Int,
        required: false,
        default: Some(DefaultValue::Int(5)),
    },
    TIMEFRAME_PARAM,
];

const FILTER_PARAMS: &[ParamSpec] = &[ParamSpec {
    name: "when",
    ty: ParamType::String,
    required: false,
    default: Some(DefaultValue::Str("true")),
}];

const LEVEL_PARAMS: &[ParamSpec] = &[
    ParamSpec {
        name: "lookback",
        ty: ParamType::Int,
        required: false,
        default: Some(DefaultValue::Int(20)),
    },
    TIMEFRAME_PARAM,
];

const CANDLE_SIGNAL_PARAMS: &[ParamSpec] = &[
    ParamSpec { name: "when", ty: ParamType::String, required: false, default: Some(DefaultValue::Str("true")) },
    ParamSpec { name: "direction", ty: ParamType::Int, required: false, default: Some(DefaultValue::Int(1)) },
    ParamSpec { name: "stop_atr_mult", ty: ParamType::Float, required: false, default: Some(DefaultValue::Float(1.5)) },
    ParamSpec { name: "target_atr_mult", ty: ParamType::Float, required: false, default: Some(DefaultValue::Float(3.0)) },
    ParamSpec { name: "max_hold_bars", ty: ParamType::Int, required: false, default: Some(DefaultValue::Int(15)) },
    ParamSpec { name: "trigger", ty: ParamType::String, required: false, default: Some(DefaultValue::Str("swing")) },
];

const STRATEGY_PARAMS: &[ParamSpec] = &[
    ParamSpec {
        name: "k",
        ty: ParamType::Float,
        required: false,
        default: Some(DefaultValue::Float(3.5)),
    },
    ParamSpec {
        name: "stop_atr_mult",
        ty: ParamType::Float,
        required: false,
        default: Some(DefaultValue::Float(1.5)),
    },
    ParamSpec {
        name: "target_atr_mult",
        ty: ParamType::Float,
        required: false,
        default: Some(DefaultValue::Float(3.0)),
    },
    ParamSpec {
        name: "direction_sign",
        ty: ParamType::Float,
        required: false,
        default: Some(DefaultValue::Float(-1.0)),
    },
    ParamSpec {
        name: "max_hold_bars",
        ty: ParamType::Int,
        required: false,
        default: Some(DefaultValue::Int(15)),
    },
    ParamSpec {
        name: "htf_filter",
        ty: ParamType::String,
        required: false,
        default: Some(DefaultValue::Str("none")),
    },
];

const STRATEGY_INPUTS: &[InputSpec] = &[
    InputSpec { name: "ema", accepts: Producer::Indicator, required: true },
    InputSpec { name: "atr", accepts: Producer::Indicator, required: true },
    InputSpec { name: "swing", accepts: Producer::Swing, required: true },
    InputSpec { name: "htf_ema", accepts: Producer::Indicator, required: false },
];

const SIZING_INPUTS: &[InputSpec] =
    &[InputSpec { name: "setups", accepts: Producer::Setup, required: true }];

const BROKER_PARAMS: &[ParamSpec] = &[
    ParamSpec {
        name: "entry_mode",
        ty: ParamType::String,
        required: false,
        default: Some(DefaultValue::Str("limit")),
    },
    ParamSpec {
        name: "limit_valid_bars",
        ty: ParamType::Int,
        required: false,
        default: Some(DefaultValue::Int(3)),
    },
];

const BROKER_INPUTS: &[InputSpec] =
    &[InputSpec { name: "trades", accepts: Producer::Trade, required: true }];

const SIZING_PARAMS: &[ParamSpec] = &[ParamSpec {
    name: "source_filter",
    ty: ParamType::String,
    required: false,
    default: Some(DefaultValue::Str("__auto__")),
}];

/// Built-in box catalogue. This is the whole vocabulary: adding a box means
/// adding a spec plus a construction arm in `build`.
pub static KINDS: &[KindSpec] = &[
    KindSpec {
        name: "ema",
        role: Role::Enricher,
        output: Some(Producer::Indicator),
        params: PERIOD_20,
        inputs: &[],
    },
    KindSpec {
        name: "atr",
        role: Role::Enricher,
        output: Some(Producer::Indicator),
        params: PERIOD_14,
        inputs: &[],
    },
    KindSpec {
        name: "swing",
        role: Role::Enricher,
        output: Some(Producer::Swing),
        params: SWING_PARAMS,
        inputs: &[],
    },
    KindSpec {
        name: "filter",
        role: Role::Enricher,
        output: Some(Producer::Swing),
        params: FILTER_PARAMS,
        inputs: &[InputSpec { name: "swing", accepts: Producer::Swing, required: true }],
    },
    KindSpec {
        name: "level",
        role: Role::Enricher,
        output: Some(Producer::Indicator),
        params: LEVEL_PARAMS,
        inputs: &[],
    },
    KindSpec {
        name: "candle_signal",
        role: Role::Strategy,
        output: Some(Producer::Setup),
        params: CANDLE_SIGNAL_PARAMS,
        inputs: STRATEGY_INPUTS,
    },
    KindSpec {
        name: "strategy",
        role: Role::Strategy,
        output: Some(Producer::Setup),
        params: STRATEGY_PARAMS,
        inputs: STRATEGY_INPUTS,
    },
    KindSpec {
        name: "sizing",
        role: Role::Broker,
        output: Some(Producer::Trade),
        params: SIZING_PARAMS,
        inputs: SIZING_INPUTS,
    },
    KindSpec {
        name: "broker",
        role: Role::Broker,
        output: None,
        params: BROKER_PARAMS,
        inputs: BROKER_INPUTS,
    },
    KindSpec {
        name: "oanda_broker",
        role: Role::Broker,
        output: None,
        params: BROKER_PARAMS,
        inputs: BROKER_INPUTS,
    },
];

pub fn kind(name: &str) -> Option<&'static KindSpec> {
    KINDS.iter().find(|k| k.name == name)
}

pub fn kind_names() -> Vec<&'static str> {
    KINDS.iter().map(|k| k.name).collect()
}

/// Legal cross-role transport for an edge. Same-role edges stay inside one
/// process and use the engine's internal stage names instead.
pub fn channel_between(from: Role, to: Role) -> Option<Channel> {
    match (from, to) {
        (Role::Enricher, Role::Strategy) => Some(Channel::Enrichment),
        (Role::Strategy, Role::Broker) => Some(Channel::Exec),
        _ => None,
    }
}
