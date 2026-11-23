use crate::ast::{Component, Definition, Export, Field, Import, Instance, Param, Program, Value};
use crate::error::{DslError, DslResult, Pos};
use crate::lexer::{tokenize, Tok, Token};

pub fn parse(source: &str) -> DslResult<Program> {
    let tokens = tokenize(source)?;
    Parser { tokens, at: 0 }.program()
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.at.min(self.tokens.len() - 1)]
    }

    fn pos(&self) -> Pos {
        self.peek().pos
    }

    fn bump(&mut self) -> Token {
        let t = self.peek().clone();
        if self.at < self.tokens.len() - 1 {
            self.at += 1;
        }
        t
    }

    fn eat(&mut self, tok: &Tok) -> bool {
        if std::mem::discriminant(&self.peek().tok) == std::mem::discriminant(tok) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, tok: &Tok) -> DslResult<Token> {
        if std::mem::discriminant(&self.peek().tok) == std::mem::discriminant(tok) {
            Ok(self.bump())
        } else {
            Err(DslError::at(
                self.pos(),
                format!("expected {} but found {}", tok.describe(), self.peek().tok.describe()),
            ))
        }
    }

    fn ident(&mut self) -> DslResult<(String, Pos)> {
        let pos = self.pos();
        match self.bump().tok {
            Tok::Ident(v) => Ok((v, pos)),
            other => Err(DslError::at(pos, format!("expected identifier but found {}", other.describe()))),
        }
    }

    fn program(&mut self) -> DslResult<Program> {
        let mut imports = Vec::new();
        let mut exports = Vec::new();
        let mut components = Vec::new();
        let mut instances = Vec::new();
        let mut definitions = Vec::new();
        while !matches!(self.peek().tok, Tok::Eof) {
            if let Tok::Ident(n) = self.peek().tok.clone() {
                if n == "import" {
                    imports.push(self.import()?);
                    continue;
                }
                if n == "export" {
                    exports.push(self.export()?);
                    continue;
                }
                if n == "component" {
                    components.push(self.component()?);
                    continue;
                }
            }
            let (name, _) = self.ident()?;
            self.expect(&Tok::Assign)?;
            let (kind, _) = self.ident()?;
            if self.peek().tok == Tok::LBrace {
                self.expect(&Tok::LBrace)?;
                let mut fields: Vec<Field> = Vec::new();
                loop {
                    if self.eat(&Tok::RBrace) { break; }
                    let (field_name, field_pos) = self.ident()?;
                    let value = if self.eat(&Tok::Colon) {
                        self.value()?
                    } else {
                        Value::Ref(field_name.clone())
                    };
                    fields.push(Field { name: field_name, value, pos: field_pos });
                    if self.eat(&Tok::Comma) { continue; }
                    if self.eat(&Tok::RBrace) { break; }
                    return Err(DslError::at(
                        self.pos(),
                        format!("expected ',' or '}}' but found {}", self.peek().tok.describe()),
                    ));
                }
                definitions.push(Definition { name, kind, fields, pos: self.pos() });
            } else if self.peek().tok == Tok::LParen {
                self.expect(&Tok::LParen)?;
                let mut args: Vec<Field> = Vec::new();
                loop {
                    if self.eat(&Tok::RParen) { break; }
                    let (arg_name, arg_pos) = self.ident()?;
                    let value = if self.eat(&Tok::Colon) { self.value()? } else { Value::Ref(arg_name.clone()) };
                    args.push(Field { name: arg_name, value, pos: arg_pos });
                    if self.eat(&Tok::Comma) { continue; }
                    if self.eat(&Tok::RParen) { break; }
                    return Err(DslError::at(
                        self.pos(),
                        format!("expected ',' or ')' but found {}", self.peek().tok.describe()),
                    ));
                }
                instances.push(Instance { name, component: kind, args, pos: self.pos() });
            } else {
                return Err(DslError::at(self.pos(), format!("expected '{{' or '(' but found {}", self.peek().tok.describe())));
            }
        }
        Ok(Program { imports, exports, components, instances, definitions })
    }

    fn import(&mut self) -> DslResult<Import> {
        let pos = self.pos();
        match self.bump().tok {
            Tok::Ident(n) if n == "import" => {}
            _ => return Err(DslError::at(pos, "expected 'import'".to_string())),
        }
        // Form: `import <alias> from <path>`
        if let Tok::Ident(_) = self.peek().tok {
            let saved = self.at;
            let alias_candidate = match self.peek().tok.clone() {
                Tok::Ident(n) => n,
                _ => String::new(),
            };
            self.bump();
            if let Tok::Ident(kw) = self.peek().tok.clone() {
                if kw == "from" {
                    self.bump();
                    let path = self.read_import_path()?;
                    let _ = self.eat(&Tok::Comma);
                    return Ok(Import { path, alias: alias_candidate, pos });
                }
            }
            self.at = saved;
        }
        // Form: `import "path"` or `import "path" as alias`
        let path = self.read_import_path()?;
        let alias = if let Tok::Ident(n) = &self.peek().tok {
            if n == "as" {
                self.bump();
                let (a, _) = self.ident()?;
                a
            } else {
                std::path::Path::new(&path).file_stem().and_then(|s| s.to_str()).unwrap_or("imported").to_string()
            }
        } else {
            std::path::Path::new(&path).file_stem().and_then(|s| s.to_str()).unwrap_or("imported").to_string()
        };
        let _ = self.eat(&Tok::Comma);
        Ok(Import { path, alias, pos })
    }

    fn read_import_path(&mut self) -> DslResult<String> {
        match self.peek().tok.clone() {
            Tok::Str(v) => { self.bump(); Ok(v) }
            other => Err(DslError::at(self.pos(), format!("expected string path but found {}", other.describe()))),
        }
    }

    fn export(&mut self) -> DslResult<Export> {
        let pos = self.pos();
        match self.bump().tok {
            Tok::Ident(n) if n == "export" => {}
            _ => return Err(DslError::at(pos, "expected 'export'".to_string())),
        }
        let (name, _) = self.ident()?;
        let _ = self.eat(&Tok::Comma);
        Ok(Export { name, pos })
    }

    fn component(&mut self) -> DslResult<Component> {
        let pos = self.pos();
        match self.bump().tok {
            Tok::Ident(n) if n == "component" => {}
            _ => return Err(DslError::at(pos, "expected 'component'".to_string())),
        }
        let (name, _) = self.ident()?;
        self.expect(&Tok::LParen)?;
        let mut params = Vec::new();
        loop {
            if self.eat(&Tok::RParen) { break; }
            let (pname, ppos) = self.ident()?;
            let default = if self.eat(&Tok::Colon) { Some(self.value()?) } else { None };
            params.push(Param { name: pname, default, pos: ppos });
            if self.eat(&Tok::Comma) { continue; }
            if self.eat(&Tok::RParen) { break; }
            return Err(DslError::at(self.pos(), format!("expected ',' or ')' but found {}", self.peek().tok.describe())));
        }
        self.expect(&Tok::LBrace)?;
        // parse body until matching RBrace
        let mut imports = Vec::new();
        let mut exports = Vec::new();
        let mut components = Vec::new();
        let mut instances = Vec::new();
        let mut definitions = Vec::new();
        while !matches!(self.peek().tok, Tok::RBrace) && !matches!(self.peek().tok, Tok::Eof) {
            if let Tok::Ident(n) = self.peek().tok.clone() {
                if n == "import" { imports.push(self.import()?); continue; }
                if n == "export" { exports.push(self.export()?); continue; }
                if n == "component" { components.push(self.component()?); continue; }
            }
            let (name, _) = self.ident()?;
            self.expect(&Tok::Assign)?;
            let (kind, _) = self.ident()?;
            if self.peek().tok == Tok::LBrace {
                self.expect(&Tok::LBrace)?;
                let mut fields: Vec<Field> = Vec::new();
                loop {
                    if self.eat(&Tok::RBrace) { break; }
                    let (field_name, field_pos) = self.ident()?;
                    let value = if self.eat(&Tok::Colon) { self.value()? } else { Value::Ref(field_name.clone()) };
                    fields.push(Field { name: field_name, value, pos: field_pos });
                    if self.eat(&Tok::Comma) { continue; }
                    if self.eat(&Tok::RBrace) { break; }
                    return Err(DslError::at(self.pos(), format!("expected ',' or '}}' but found {}", self.peek().tok.describe())));
                }
                definitions.push(Definition { name, kind, fields, pos: self.pos() });
            } else if self.peek().tok == Tok::LParen {
                self.expect(&Tok::LParen)?;
                let mut args: Vec<Field> = Vec::new();
                loop {
                    if self.eat(&Tok::RParen) { break; }
                    let (arg_name, arg_pos) = self.ident()?;
                    let value = if self.eat(&Tok::Colon) { self.value()? } else { Value::Ref(arg_name.clone()) };
                    args.push(Field { name: arg_name, value, pos: arg_pos });
                    if self.eat(&Tok::Comma) { continue; }
                    if self.eat(&Tok::RParen) { break; }
                    return Err(DslError::at(self.pos(), format!("expected ',' or ')' but found {}", self.peek().tok.describe())));
                }
                instances.push(Instance { name, component: kind, args, pos: self.pos() });
            } else {
                return Err(DslError::at(self.pos(), format!("expected '{{' or '(' but found {}", self.peek().tok.describe())));
            }
        }
        self.expect(&Tok::RBrace)?;
        let body = Program { imports, exports, components, instances, definitions };
        let exports = body.exports.iter().map(|e| e.name.clone()).collect();
        Ok(Component { name, params, body, exports, pos })
    }

    fn value(&mut self) -> DslResult<Value> {
        let pos = self.pos();
        match self.peek().tok.clone() {
            Tok::Int(v) => { self.bump(); Ok(Value::Int(v)) }
            Tok::Float(v) => { self.bump(); Ok(Value::Float(v)) }
            Tok::Str(v) => { self.bump(); Ok(Value::Str(v)) }
            Tok::True => { self.bump(); Ok(Value::Bool(true)) }
            Tok::False => { self.bump(); Ok(Value::Bool(false)) }
            Tok::Null => { self.bump(); Ok(Value::Null) }
            Tok::Ident(name) => { self.bump(); Ok(Value::Ref(name)) }
            Tok::LBracket => self.list(),
            other => Err(DslError::at(pos, format!("expected a value but found {}", other.describe()))),
        }
    }

    fn list(&mut self) -> DslResult<Value> {
        self.expect(&Tok::LBracket)?;
        let mut items = Vec::new();
        loop {
            if self.eat(&Tok::RBracket) { break; }
            let pos = self.pos();
            let item = self.value()?;
            if matches!(item, Value::Ref(_)) {
                return Err(DslError::at(pos, "lists are plain data: references are not allowed inside a list".to_string()));
            }
            items.push(item);
            if self.eat(&Tok::Comma) { continue; }
            if self.eat(&Tok::RBracket) { break; }
            return Err(DslError::at(
                self.pos(),
                format!("expected ',' or ']' but found {}", self.peek().tok.describe()),
            ));
        }
        Ok(Value::List(items))
    }
}
