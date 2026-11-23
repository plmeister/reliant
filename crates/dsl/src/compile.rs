use crate::ast::{Definition, Value};
use crate::error::{DslError, DslResult, Pos};
use crate::spec::{self, channel_between, Channel, KindSpec, ParamType, Producer, Role};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone)]
pub struct CompiledNode {
    pub name: String,
    pub kind: String,
    pub role: Role,
    pub params: BTreeMap<String, Value>,
    /// Input port -> producer box name.
    pub inputs: BTreeMap<String, String>,
    pub pos: Pos,
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub port: String,
    pub produces: Producer,
    /// `None` for same-role edges (executed inside one process); otherwise the
    /// bus channel carrying the edge.
    pub channel: Option<Channel>,
}

#[derive(Debug, Clone)]
pub struct RolePlan {
    pub role: Role,
    pub nodes: Vec<String>,
    pub subscribes: Vec<Channel>,
    pub publishes: Vec<Channel>,
}

impl RolePlan {
    pub fn subscribe_topics(&self, portfolio: &str) -> Vec<String> {
        self.subscribes.iter().map(|c| c.subject(portfolio)).collect()
    }

    pub fn publish_topics(&self, portfolio: &str) -> Vec<String> {
        self.publishes.iter().map(|c| c.subject(portfolio)).collect()
    }
}

/// A validated strategy: boxes in execution order plus the subjects each role
/// must listen to and publish on.
#[derive(Debug, Clone)]
pub struct Compiled {
    pub portfolio: String,
    pub nodes: Vec<CompiledNode>,
    pub edges: Vec<Edge>,
    pub roles: BTreeMap<Role, RolePlan>,
}

impl Compiled {
    pub fn node(&self, name: &str) -> Option<&CompiledNode> {
        self.nodes.iter().find(|n| n.name == name)
    }

    pub fn role_plan(&self, role: Role) -> Option<&RolePlan> {
        self.roles.get(&role)
    }

    pub fn nodes_for(&self, role: Role) -> Vec<&CompiledNode> {
        self.nodes.iter().filter(|n| n.role == role).collect()
    }

    /// Subjects this role subscribes to, in stable channel order.
    pub fn subscribe_topics(&self, role: Role) -> Vec<String> {
        self.role_plan(role)
            .map(|p| p.subscribe_topics(&self.portfolio))
            .unwrap_or_default()
    }

    pub fn publish_topics(&self, role: Role) -> Vec<String> {
        self.role_plan(role)
            .map(|p| p.publish_topics(&self.portfolio))
            .unwrap_or_default()
    }
}

struct Working {
    name: String,
    kind: &'static KindSpec,
    params: BTreeMap<String, Value>,
    raw_inputs: BTreeMap<String, (String, Pos)>,
    pos: Pos,
}

pub fn compile_program(program: &crate::ast::Program, portfolio: &str) -> DslResult<Compiled> {
    if program.definitions.is_empty() {
        return Err(DslError::new("strategy is empty: define at least one box"));
    }

    let mut working: Vec<Working> = Vec::with_capacity(program.definitions.len());
    let mut by_name: HashMap<String, usize> = HashMap::new();

    for (index, def) in program.definitions.iter().enumerate() {
        if by_name.insert(def.name.clone(), index).is_some() {
            return Err(DslError::at(def.pos, format!("duplicate box name {:?}", def.name)));
        }
        working.push(check_definition(def)?);
    }

    let mut edges: Vec<Edge> = Vec::new();
    let mut resolved_inputs: Vec<BTreeMap<String, String>> =
        (0..working.len()).map(|_| BTreeMap::new()).collect();

    for (index, node) in working.iter().enumerate() {
        for port in node.kind.inputs {
            let (target_name, pos) = match node.raw_inputs.get(port.name).cloned() {
                Some(v) => v,
                None if !port.required => continue,
                None => {
                    return Err(DslError::at(
                        node.pos,
                        format!(
                            "box {:?} ({}) is missing input {:?}",
                            node.name, node.kind.name, port.name
                        ),
                    ));
                }
            };

            let target_index = *by_name.get(&target_name).ok_or_else(|| {
                DslError::at(
                    pos,
                    format!("input {:?} references unknown box {:?}", port.name, target_name),
                )
            })?;

            if target_index == index {
                return Err(DslError::at(
                    pos,
                    format!("box {:?} cannot be wired to itself", node.name),
                ));
            }

            let target = &working[target_index];
            let Some(produces) = target.kind.output else {
                return Err(DslError::at(
                    pos,
                    format!(
                        "box {:?} is a sink ({}) and cannot feed {:?}",
                        target.name, target.kind.name, node.name
                    ),
                ));
            };
            if produces != port.accepts {
                return Err(DslError::at(
                    pos,
                    format!(
                        "input {:?} of {:?} expects producer type {}, but {:?} produces {}",
                        port.name,
                        node.name,
                        port.accepts.as_str(),
                        target.name,
                        produces.as_str()
                    ),
                ));
            }

            let channel = if target.kind.role == node.kind.role {
                None
            } else {
                match channel_between(target.kind.role, node.kind.role) {
                    Some(c) => Some(c),
                    None => {
                        return Err(DslError::at(
                            pos,
                            format!(
                                "no transport from {} ({}) to {} ({})",
                                target.kind.role.as_str(),
                                target.name,
                                node.kind.role.as_str(),
                                node.name
                            ),
                        ));
                    }
                }
            };

            resolved_inputs[index].insert(port.name.to_string(), target_name.clone());
            edges.push(Edge {
                from: target_name,
                to: node.name.clone(),
                port: port.name.to_string(),
                produces,
                channel,
            });
        }
    }

    let order = topological_order(&working, &edges)?;

    let nodes: Vec<CompiledNode> = order
        .iter()
        .map(|&i| {
            let w = &working[i];
            CompiledNode {
                name: w.name.clone(),
                kind: w.kind.name.to_string(),
                role: w.kind.role,
                params: w.params.clone(),
                inputs: resolved_inputs[i].clone(),
                pos: w.pos,
            }
        })
        .collect();

    let mut roles: BTreeMap<Role, RolePlan> = BTreeMap::new();
    for node in &nodes {
        roles
            .entry(node.role)
            .or_insert_with(|| RolePlan {
                role: node.role,
                nodes: Vec::new(),
                subscribes: match node.role {
                    Role::Enricher => vec![Channel::Feed],
                    Role::Strategy => vec![Channel::Enrichment],
                    Role::Broker => vec![Channel::Exec],
                },
                publishes: match node.role {
                    Role::Enricher => vec![Channel::Enrichment],
                    Role::Strategy => vec![Channel::Exec],
                    Role::Broker => Vec::new(),
                },
            })
            .nodes
            .push(node.name.clone());
    }

    // Lint pass: warn about suspicious wiring.
    let mut warnings: Vec<String> = Vec::new();
    let consumed: std::collections::HashSet<&str> = edges.iter().map(|e| e.to.as_str()).collect();
    let producing: std::collections::HashSet<&str> = edges.iter().map(|e| e.from.as_str()).collect();
    for node in &nodes {
        let kind = spec::kind(&node.kind);
        let has_output = kind.map(|k| k.output.is_some()).unwrap_or(false);
        // Dead producer: a box with an output that nothing consumes.
        if has_output && !producing.contains(node.name.as_str()) {
            warnings.push(format!(
                "warning: box {:?} ({}) produces {} but nothing consumes it",
                node.name, node.kind, kind.unwrap().output.unwrap().as_str()
            ));
        }
        // Orphaned consumer: a box with required inputs but no incoming edges.
        if !node.inputs.is_empty() && !consumed.contains(node.name.as_str()) {
            warnings.push(format!(
                "warning: box {:?} ({}) has inputs but no producer is wired to it",
                node.name, node.kind
            ));
        }
    }
    // Sink (broker) with no upstream sizing.
    for node in &nodes {
        if node.kind == "broker" && !consumed.contains(node.name.as_str()) {
            warnings.push(format!(
                "warning: broker {:?} has no sizing input — it will never trade",
                node.name
            ));
        }
    }
    for w in &warnings {
        eprintln!("{}", w);
    }

    Ok(Compiled { portfolio: portfolio.to_string(), nodes, edges, roles })
}

fn check_definition(def: &Definition) -> DslResult<Working> {
    let kind = spec::kind(&def.kind).ok_or_else(|| {
        DslError::at(
            def.pos,
            format!(
                "unknown box kind {:?} (known: {})",
                def.kind,
                spec::kind_names().join(", ")
            ),
        )
    })?;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    let mut raw_inputs: BTreeMap<String, (String, Pos)> = BTreeMap::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for field in &def.fields {
        if !seen.insert(field.name.clone()) {
            return Err(DslError::at(
                field.pos,
                format!("duplicate field {:?} on box {:?}", field.name, def.name),
            ));
        }

        if let Some(port) = kind.input(&field.name) {
            match &field.value {
                Value::Ref(target) => {
                    raw_inputs.insert(port.name.to_string(), (target.clone(), field.pos));
                }
                other => {
                    return Err(DslError::at(
                        field.pos,
                        format!(
                            "input {:?} of {} expects a box reference, found {}",
                            field.name,
                            kind.name,
                            other.type_name()
                        ),
                    ));
                }
            }
            continue;
        }

        if let Some(param) = kind.param(&field.name) {
            if matches!(field.value, Value::Ref(_)) {
                return Err(DslError::at(
                    field.pos,
                    format!(
                        "parameter {:?} of {} expects a literal, found a reference (is it an input port?)",
                        field.name, kind.name
                    ),
                ));
            }
            let value = coerce_param(param.ty, &field.value).ok_or_else(|| {
                DslError::at(
                    field.pos,
                    format!(
                        "parameter {:?} of {} expects {}, found {}",
                        field.name,
                        kind.name,
                        param.ty.as_str(),
                        field.value.type_name()
                    ),
                )
            })?;
            params.insert(param.name.to_string(), value);
            continue;
        }

        let mut expected: Vec<&str> = kind.params.iter().map(|p| p.name).collect();
        expected.extend(kind.inputs.iter().map(|i| i.name));
        return Err(DslError::at(
            field.pos,
            format!(
                "unknown field {:?} on {} (expected one of: {})",
                field.name,
                kind.name,
                expected.join(", ")
            ),
        ));
    }

    for param in kind.params {
        if params.contains_key(param.name) {
            continue;
        }
        if let Some(default) = param.default {
            params.insert(param.name.to_string(), default.to_value());
        } else if param.required {
            return Err(DslError::at(
                def.pos,
                format!("box {:?} ({}) requires parameter {:?}", def.name, kind.name, param.name),
            ));
        } else {
            return Err(DslError::at(
                def.pos,
                format!(
                    "box {:?} ({}) has no value or default for parameter {:?}",
                    def.name, kind.name, param.name
                ),
            ));
        }
    }

    Ok(Working {
        name: def.name.clone(),
        kind,
        params,
        raw_inputs,
        pos: def.pos,
    })
}

fn coerce_param(ty: ParamType, value: &Value) -> Option<Value> {
    match (ty, value) {
        (ParamType::Int, Value::Int(v)) => Some(Value::Int(*v)),
        (ParamType::Float, Value::Int(v)) => Some(Value::Float(*v as f64)),
        (ParamType::Float, Value::Float(v)) => Some(Value::Float(*v)),
        (ParamType::String, Value::Str(v)) => Some(Value::Str(v.clone())),
        (ParamType::Bool, Value::Bool(v)) => Some(Value::Bool(*v)),
        _ => None,
    }
}

fn topological_order(nodes: &[Working], edges: &[Edge]) -> DslResult<Vec<usize>> {
    let index: HashMap<&str, usize> =
        nodes.iter().enumerate().map(|(i, n)| (n.name.as_str(), i)).collect();
    let mut indegree = vec![0usize; nodes.len()];
    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];

    for edge in edges {
        let from = index[edge.from.as_str()];
        let to = index[edge.to.as_str()];
        outgoing[from].push(to);
        indegree[to] += 1;
    }

    // Alphabetical tie-break keeps compilation deterministic regardless of
    // source order for independent boxes.
    let mut ready: BTreeSet<usize> = BTreeSet::new();
    for (i, degree) in indegree.iter().enumerate() {
        if *degree == 0 {
            ready.insert(i);
        }
    }

    let mut order = Vec::with_capacity(nodes.len());
    while let Some(&i) = ready.iter().next() {
        ready.remove(&i);
        order.push(i);
        for &to in &outgoing[i] {
            indegree[to] -= 1;
            if indegree[to] == 0 {
                ready.insert(to);
            }
        }
    }

    if order.len() != nodes.len() {
        let stuck: Vec<&str> = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| !order.contains(i))
            .map(|(_, n)| n.name.as_str())
            .collect();
        return Err(DslError::new(format!(
            "cycle in strategy graph involving: {}",
            stuck.join(", ")
        )));
    }
    Ok(order)
}
