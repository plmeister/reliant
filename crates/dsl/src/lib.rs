//! Declarative strategy DSL for the Rust reimplementation.

pub mod ast;
pub mod build;
pub mod compile;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod spec;

pub use ast::{Definition, Field, Import, Program, Value};
pub use build::{build_all_nodes, build_box, build_role_nodes, BuildOptions};
pub use compile::{compile_program, Compiled, CompiledNode, Edge, RolePlan};
pub use error::{DslError, DslResult, Pos};
pub use spec::{channel_between, kind, kind_names, Channel, Producer, Role};

use std::collections::HashSet;
use std::path::PathBuf;

/// Parse and compile a strategy source for a portfolio id.
pub fn compile(source: &str, portfolio: &str) -> DslResult<Compiled> {
    compile_with_base(source, None, portfolio)
}

/// Parse and compile with a base directory for resolving `import` statements.
pub fn compile_with_base(source: &str, base_dir: Option<&std::path::Path>, portfolio: &str) -> DslResult<Compiled> {
    let program = parser::parse(source)?;
    let resolved = resolve_imports(program, base_dir)?;
    compile_program(&resolved, portfolio)
}

/// Read, resolve imports and compile a strategy file.
pub fn compile_file(path: &str, portfolio: &str) -> DslResult<Compiled> {
    let source = std::fs::read_to_string(path)
        .map_err(|e| DslError::new(format!("read strategy {}: {}", path, e)))?;
    let base = std::path::Path::new(path).parent();
    compile_with_base(&source, base, portfolio)
}

/// Parse only.
pub fn parse(source: &str) -> DslResult<Program> {
    parser::parse(source)
}

fn lib_dir() -> PathBuf {
    if let Ok(v) = std::env::var("RELIANT_LIB") {
        if !v.is_empty() { return PathBuf::from(v); }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(format!("{}/.config/reliant/lib", home))
}

fn resolve_path(p: &str, base: Option<&std::path::Path>, lib: &PathBuf) -> PathBuf {
    let path = std::path::Path::new(p);
    if path.is_absolute() || p.starts_with("./") || p.starts_with("../") {
        if let Some(b) = base {
            return b.join(p);
        }
        return PathBuf::from(p);
    }
    // try lib dir
    let mut cand = lib.join(p);
    if !cand.exists() && !p.ends_with(".dsl") {
        cand = lib.join(format!("{}.dsl", p));
    }
    if cand.exists() { return cand; }
    if let Some(b) = base {
        let c = b.join(p);
        if c.exists() { return c; }
        if !p.ends_with(".dsl") {
            let c2 = b.join(format!("{}.dsl", p));
            if c2.exists() { return c2; }
        }
    }
    if !p.ends_with(".dsl") { lib.join(format!("{}.dsl", p)) } else { lib.join(p) }
}

fn namespace_value(v: crate::ast::Value, names: &HashSet<String>, alias: &str) -> crate::ast::Value {
    use crate::ast::Value;
    match v {
        Value::Ref(name) => {
            if names.contains(&name) {
                Value::Ref(format!("{}.{}", alias, name))
            } else {
                Value::Ref(name)
            }
        }
        Value::List(items) => Value::List(items.into_iter().map(|x| namespace_value(x, names, alias)).collect()),
        other => other,
    }
}

fn namespace_program(prog: crate::ast::Program, alias: &str) -> crate::ast::Program {
    let names: HashSet<String> = prog.definitions.iter().map(|d| d.name.clone()).collect();
    let mut defs = Vec::new();
    for d in prog.definitions {
        let new_name = format!("{}.{}", alias, d.name);
        let new_fields = d.fields.into_iter().map(|f| {
            crate::ast::Field {
                name: f.name,
                value: namespace_value(f.value, &names, alias),
                pos: f.pos,
            }
        }).collect();
        defs.push(crate::ast::Definition { name: new_name, kind: d.kind, fields: new_fields, pos: d.pos });
    }
    crate::ast::Program { imports: vec![], exports: vec![], components: vec![], instances: vec![], definitions: defs }
}

fn resolve(path: &str, base: Option<&std::path::Path>, lib: &PathBuf, visited: &mut HashSet<PathBuf>) -> DslResult<crate::ast::Program> {
    let full = resolve_path(path, base, lib);
    if visited.contains(&full) {
        return Err(DslError::new(format!("cyclic import: {}", full.display())));
    }
    let source = std::fs::read_to_string(&full)
        .map_err(|e| DslError::new(format!("read import {}: {}", full.display(), e)))?;
    let parsed = parser::parse(&source)?;
    let imported_base = full.parent();
    let mut visited = visited.clone();
    visited.insert(full.clone());
    let mut defs = Vec::new();
    let mut comps = Vec::new();
    let mut instances = Vec::new();
    for imp in &parsed.imports {
        let imp_prog = resolve(&imp.path, imported_base, lib, &mut visited)?;
        let alias = if imp.alias.is_empty() {
            std::path::Path::new(&imp.path).file_stem().and_then(|s| s.to_str()).unwrap_or("imported").to_string()
        } else {
            imp.alias.clone()
        };
        let ns = namespace_program(imp_prog.clone(), &alias);
        defs.extend(ns.definitions);
        for c in imp_prog.components {
            let mut nc = c.clone();
            nc.name = format!("{}.{}", alias, c.name);
            nc.body = namespace_program(c.body.clone(), &alias);
            nc.exports = c.exports.iter().map(|e| format!("{}.{}", alias, e)).collect();
            comps.push(nc);
        }
        for inst in imp_prog.instances {
            // namespace instance name and component ref
            instances.push(crate::ast::Instance {
                name: format!("{}.{}", alias, inst.name),
                component: format!("{}.{}", alias, inst.component),
                args: inst.args.into_iter().map(|f| crate::ast::Field {
                    name: f.name,
                    value: namespace_value(f.value, &std::collections::HashSet::new(), &alias),
                    pos: f.pos,
                }).collect(),
                pos: inst.pos,
            });
        }
    }
    // collect components from this file
    comps.extend(parsed.components.clone());
    defs.extend(parsed.definitions.clone());
    instances.extend(parsed.instances.clone());
    // expand instances using comps
    let expanded = expand_instances(&comps, &instances, &mut defs);
    defs.extend(expanded);
    Ok(crate::ast::Program { imports: vec![], exports: vec![], components: comps, instances: vec![], definitions: defs })
}

fn resolve_imports(program: crate::ast::Program, base_dir: Option<&std::path::Path>) -> DslResult<crate::ast::Program> {
    let lib = lib_dir();
    let base_ref = base_dir;
    let mut visited = HashSet::new();
    let mut defs = Vec::new();
    let mut comps = Vec::new();
    for imp in &program.imports {
        let imp_prog = resolve(&imp.path, base_ref, &lib, &mut visited)?;
        let alias = if imp.alias.is_empty() {
            std::path::Path::new(&imp.path).file_stem().and_then(|s| s.to_str()).unwrap_or("imported").to_string()
        } else {
            imp.alias.clone()
        };
        let ns = namespace_program(imp_prog.clone(), &alias);
        defs.extend(ns.definitions);
        for c in imp_prog.components {
            let mut nc = c.clone();
            nc.name = format!("{}.{}", alias, c.name);
            nc.body = namespace_program(c.body.clone(), &alias);
            nc.exports = c.exports.iter().map(|e| format!("{}.{}", alias, e)).collect();
            comps.push(nc);
        }
    }
    comps.extend(program.components);
    defs.extend(program.definitions);
    // Expand instances
    let expanded = expand_instances(&comps, &program.instances, &mut defs);
    defs.extend(expanded);
    Ok(crate::ast::Program { imports: vec![], exports: vec![], components: comps, instances: vec![], definitions: defs })
}

fn expand_instances(comps: &[crate::ast::Component], instances: &[crate::ast::Instance], defs: &mut Vec<crate::ast::Definition>) -> Vec<crate::ast::Definition> {
    use crate::ast::{Component, Definition, Field, Value};
    use std::collections::HashMap;
    let comp_map: HashMap<String, &Component> = comps.iter().map(|c| (c.name.clone(), c)).collect();
    let mut out = Vec::new();
    for inst in instances {
        let comp = match comp_map.get(&inst.component) {
            Some(c) => c,
            None => continue, // unknown component, skip
        };
        // Build param substitution map
        let mut subst: HashMap<String, Value> = HashMap::new();
        for p in &comp.params {
            if let Some(d) = &p.default {
                subst.insert(p.name.clone(), d.clone());
            }
        }
        for arg in &inst.args {
            subst.insert(arg.name.clone(), arg.value.clone());
        }
        // Determine which body defs to include (exports or all)
        let export_names: Vec<String> = if comp.exports.is_empty() {
            comp.body.definitions.iter().map(|d| d.name.clone()).collect()
        } else {
            comp.exports.clone()
        };
        // Collect deps of exports
        let by_name: HashMap<String, &Definition> = comp.body.definitions.iter().map(|d| (d.name.clone(), d)).collect();
        let mut included: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut stack: Vec<String> = export_names.clone();
        while let Some(name) = stack.pop() {
            if included.contains(&name) { continue; }
            included.insert(name.clone());
            if let Some(def) = by_name.get(&name) {
                for f in &def.fields {
                    if let Value::Ref(r) = &f.value {
                        if by_name.contains_key(r) && !included.contains(r) {
                            stack.push(r.clone());
                        }
                    }
                }
            }
        }
        // Generate definitions with param substitution and instance prefix
        for d in &comp.body.definitions {
            if !included.contains(&d.name) { continue; }
            // The exported box gets the instance name; deps get instance_name.dep_name
            let new_name = if export_names.contains(&d.name) && export_names.len() == 1 {
                inst.name.clone()
            } else if export_names.contains(&d.name) {
                format!("{}.{}", inst.name, d.name)
            } else {
                format!("{}.{}", inst.name, d.name)
            };
            let new_fields = d.fields.iter().map(|f| {
                let new_value = subst_value(&f.value, &subst);
                Field { name: f.name.clone(), value: new_value, pos: f.pos }
            }).collect();
            out.push(Definition { name: new_name, kind: d.kind.clone(), fields: new_fields, pos: d.pos });
        }
    }
    out
}

fn subst_value(v: &crate::ast::Value, subst: &std::collections::HashMap<String, crate::ast::Value>) -> crate::ast::Value {
    use crate::ast::Value;
    match v {
        Value::Ref(name) => {
            if let Some(repl) = subst.get(name) {
                repl.clone()
            } else {
                Value::Ref(name.clone())
            }
        }
        Value::List(items) => Value::List(items.iter().map(|x| subst_value(x, subst)).collect()),
        other => other.clone(),
    }
}
