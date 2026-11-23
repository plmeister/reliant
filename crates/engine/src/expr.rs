//! Minimal expression language for filter conditions.
//!
//! Supports arithmetic (+,-,*,/), comparisons (>,<,>=,<=,==,!=) and logical
//! (and, or, not) over named variables. Variables resolve against a context
//! map (indicator values, price, atr, ...). Unknown variables evaluate to 0.0.

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum Expr {
    Number(f64),
    Var(String),
    BinOp(Op, Box<Expr>, Box<Expr>),
    Unary(Op, Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Add, Sub, Mul, Div,
    Gt, Lt, Ge, Le, Eq, Ne,
    And, Or,
    Not, Neg, Abs,
    Min, Max,
}

#[derive(Debug)]
pub enum ParseError {
    Unexpected(String),
    Eof,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Unexpected(s) => write!(f, "unexpected: {}", s),
            ParseError::Eof => write!(f, "unexpected end of expression"),
        }
    }
}

struct Parser<'a> {
    s: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self { Self { s, pos: 0 } }

    fn skip_ws(&mut self) {
        while self.pos < self.s.len() && self.s.as_bytes()[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;
        loop {
            self.skip_ws();
            if self.match_word("or") {
                let right = self.parse_and()?;
                left = Expr::BinOp(Op::Or, Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_cmp()?;
        loop {
            self.skip_ws();
            if self.match_word("and") {
                let right = self.parse_cmp()?;
                left = Expr::BinOp(Op::And, Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_cmp(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_add()?;
        self.skip_ws();
        if self.consume(">=") { let r = self.parse_add()?; return Ok(Expr::BinOp(Op::Ge, Box::new(left), Box::new(r))); }
        if self.consume("<=") { let r = self.parse_add()?; return Ok(Expr::BinOp(Op::Le, Box::new(left), Box::new(r))); }
        if self.consume("==") { let r = self.parse_add()?; return Ok(Expr::BinOp(Op::Eq, Box::new(left), Box::new(r))); }
        if self.consume("!=") { let r = self.parse_add()?; return Ok(Expr::BinOp(Op::Ne, Box::new(left), Box::new(r))); }
        if self.consume(">") { let r = self.parse_add()?; return Ok(Expr::BinOp(Op::Gt, Box::new(left), Box::new(r))); }
        if self.consume("<") { let r = self.parse_add()?; return Ok(Expr::BinOp(Op::Lt, Box::new(left), Box::new(r))); }
        Ok(left)
    }

    fn parse_add(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_mul()?;
        loop {
            self.skip_ws();
            if self.consume("+") { let r = self.parse_mul()?; left = Expr::BinOp(Op::Add, Box::new(left), Box::new(r)); }
            else if self.consume("-") { let r = self.parse_mul()?; left = Expr::BinOp(Op::Sub, Box::new(left), Box::new(r)); }
            else { break; }
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary()?;
        loop {
            self.skip_ws();
            if self.consume("*") { let r = self.parse_unary()?; left = Expr::BinOp(Op::Mul, Box::new(left), Box::new(r)); }
            else if self.consume("/") { let r = self.parse_unary()?; left = Expr::BinOp(Op::Div, Box::new(left), Box::new(r)); }
            else { break; }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        self.skip_ws();
        if self.match_word("not") {
            let e = self.parse_unary()?;
            return Ok(Expr::Unary(Op::Not, Box::new(e)));
        }
        if self.consume("-") {
            let e = self.parse_unary()?;
            return Ok(Expr::Unary(Op::Neg, Box::new(e)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        self.skip_ws();
        if self.pos >= self.s.len() { return Err(ParseError::Eof); }
        let b = self.s.as_bytes()[self.pos];
        if b == b'(' {
            self.pos += 1;
            let e = self.parse_expr()?;
            self.skip_ws();
            if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b')' {
                self.pos += 1;
            }
            return Ok(e);
        }
        if b.is_ascii_digit() || b == b'.' {
            let start = self.pos;
            while self.pos < self.s.len() {
                let c = self.s.as_bytes()[self.pos];
                if c.is_ascii_digit() || c == b'.' || c == b'e' || c == b'E' || c == b'+' || c == b'-' {
                    // only allow +/- inside exponent
                    self.pos += 1;
                } else {
                    break;
                }
            }
            let num: f64 = self.s[start..self.pos].parse().map_err(|_| ParseError::Unexpected(self.s[start..self.pos].to_string()))?;
            return Ok(Expr::Number(num));
        }
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = self.pos;
            while self.pos < self.s.len() {
                let c = self.s.as_bytes()[self.pos];
                if c.is_ascii_alphanumeric() || c == b'_' { self.pos += 1; } else { break; }
            }
            let word = &self.s[start..self.pos];
            // boolean literals
            if word == "true" { return Ok(Expr::Number(1.0)); }
            if word == "false" { return Ok(Expr::Number(0.0)); }
            // function calls
            self.skip_ws();
            if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b'(' {
                self.pos += 1;
                let arg1 = self.parse_expr()?;
                self.skip_ws();
                if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b',' {
                    self.pos += 1;
                    let arg2 = self.parse_expr()?;
                    self.skip_ws();
                    if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b')' { self.pos += 1; }
                    let f = match word {
                        "min" => Expr::BinOp(Op::Min, Box::new(arg1), Box::new(arg2)),
                        "max" => Expr::BinOp(Op::Max, Box::new(arg1), Box::new(arg2)),
                        _ => Expr::Var(word.to_string()),
                    };
                    return Ok(f);
                }
                if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b')' { self.pos += 1; }
                if word == "abs" {
                    // abs(x) = x if x>=0 else -x  ->  use (x) * sign; emulate via conditional? 
                    // Encode as: if x<0 then -x else x. Use a unary Abs op.
                    return Ok(Expr::Unary(Op::Abs, Box::new(arg1)));
                }
                // Unknown single-arg function: treat as variable.
                return Ok(Expr::Var(word.to_string()));
            }
            return Ok(Expr::Var(word.to_string()));
        }
        Err(ParseError::Unexpected(format!("{}", b as char)))
    }

    fn consume(&mut self, tok: &str) -> bool {
        self.skip_ws();
        if self.s[self.pos..].starts_with(tok) {
            // ensure not part of a longer identifier
            let after = self.pos + tok.len();
            if tok.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                if after < self.s.len() && (self.s.as_bytes()[after].is_ascii_alphanumeric() || self.s.as_bytes()[after] == b'_') {
                    return false;
                }
            }
            self.pos = after;
            true
        } else {
            false
        }
    }

    fn match_word(&mut self, word: &str) -> bool {
        self.skip_ws();
        if self.s[self.pos..].starts_with(word) {
            let after = self.pos + word.len();
            if after < self.s.len() && (self.s.as_bytes()[after].is_ascii_alphanumeric() || self.s.as_bytes()[after] == b'_') {
                return false;
            }
            self.pos = after;
            true
        } else {
            false
        }
    }
}

pub fn parse(s: &str) -> Result<Expr, ParseError> {
    let mut p = Parser::new(s);
    let e = p.parse_expr()?;
    Ok(e)
}

pub fn eval(expr: &Expr, ctx: &HashMap<String, f64>) -> f64 {
    match expr {
        Expr::Number(n) => *n,
        Expr::Var(name) => ctx.get(name).copied().unwrap_or(0.0),
        Expr::Unary(Op::Not, e) => if eval(e, ctx) == 0.0 { 1.0 } else { 0.0 },
        Expr::Unary(Op::Neg, e) => -eval(e, ctx),
        Expr::Unary(Op::Abs, e) => eval(e, ctx).abs(),
        Expr::Unary(_, e) => eval(e, ctx),
        Expr::BinOp(op, l, r) => {
            let lv = eval(l, ctx);
            let rv = eval(r, ctx);
            match op {
                Op::Add => lv + rv,
                Op::Sub => lv - rv,
                Op::Mul => lv * rv,
                Op::Div => if rv != 0.0 { lv / rv } else { 0.0 },
                Op::Gt => if lv > rv { 1.0 } else { 0.0 },
                Op::Lt => if lv < rv { 1.0 } else { 0.0 },
                Op::Ge => if lv >= rv { 1.0 } else { 0.0 },
                Op::Le => if lv <= rv { 1.0 } else { 0.0 },
                Op::Eq => if (lv - rv).abs() < f64::EPSILON { 1.0 } else { 0.0 },
                Op::Ne => if (lv - rv).abs() >= f64::EPSILON { 1.0 } else { 0.0 },
                Op::And => if lv != 0.0 && rv != 0.0 { 1.0 } else { 0.0 },
                Op::Or => if lv != 0.0 || rv != 0.0 { 1.0 } else { 0.0 },
                Op::Min => lv.min(rv),
                Op::Max => lv.max(rv),
                _ => 0.0,
            }
        }
    }
}

pub fn eval_bool(expr: &Expr, ctx: &HashMap<String, f64>) -> bool {
    eval(expr, ctx) != 0.0
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn basic() {
        let e = parse("atr < 5 and price > ema").unwrap();
        let mut ctx = HashMap::new();
        ctx.insert("atr".into(), 3.0);
        ctx.insert("price".into(), 100.0);
        ctx.insert("ema".into(), 90.0);
        assert!(eval_bool(&e, &ctx));
    }
    #[test]
    fn arith() {
        let e = parse("(a + b) * 2 > 10").unwrap();
        let mut ctx = HashMap::new();
        ctx.insert("a".into(), 2.0);
        ctx.insert("b".into(), 4.0);
        assert!(eval_bool(&e, &ctx));
    }
}