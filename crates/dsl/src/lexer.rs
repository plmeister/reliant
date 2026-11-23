use crate::error::{DslError, DslResult, Pos};

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),
    Assign,   // :=
    LBrace,   // {
    RBrace,   // }
    LParen,   // (
    RParen,   // )
    Colon,    // :
    Comma,    // ,
    LBracket, // [
    RBracket, // ]
    True,
    False,
    Null,
    Eof,
}

impl Tok {
    pub fn describe(&self) -> String {
        match self {
            Tok::Ident(v) => format!("identifier {:?}", v),
            Tok::Int(v) => format!("integer {}", v),
            Tok::Float(v) => format!("float {}", v),
            Tok::Str(_) => "string".to_string(),
            Tok::Assign => "':='".to_string(),
            Tok::LBrace => "'{'".to_string(),
            Tok::RBrace => "'}'".to_string(),
            Tok::LParen => "'('".to_string(),
            Tok::RParen => "')'".to_string(),
            Tok::Colon => "':'".to_string(),
            Tok::Comma => "','".to_string(),
            Tok::LBracket => "'['".to_string(),
            Tok::RBracket => "']'".to_string(),
            Tok::True => "'true'".to_string(),
            Tok::False => "'false'".to_string(),
            Tok::Null => "'null'".to_string(),
            Tok::Eof => "end of input".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Token {
    pub tok: Tok,
    pub pos: Pos,
}

struct Lexer<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    line: usize,
    col: usize,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
        Self { chars: source.chars().peekable(), line: 1, col: 1 }
    }

    fn pos(&self) -> Pos {
        Pos::new(self.line, self.col)
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.chars.next()?;
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(c)
    }

    fn skip_trivia(&mut self) -> DslResult<()> {
        loop {
            match self.chars.peek().copied() {
                Some(c) if c.is_whitespace() => {
                    self.bump();
                }
                Some('/') => {
                    self.bump();
                    match self.chars.peek().copied() {
                        Some('/') => {
                            while let Some(c) = self.chars.peek().copied() {
                                if c == '\n' {
                                    break;
                                }
                                self.bump();
                            }
                        }
                        _ => {
                            return Err(DslError::at(
                                Pos::new(self.line, self.col - 1),
                                "unexpected '/' (only '//' comments are supported)",
                            ));
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    fn lex(mut self) -> DslResult<Vec<Token>> {
        let mut out = Vec::new();
        loop {
            self.skip_trivia()?;
            let pos = self.pos();
            let Some(c) = self.chars.peek().copied() else {
                out.push(Token { tok: Tok::Eof, pos });
                return Ok(out);
            };
            match c {
                ':' => {
                    self.bump();
                    if self.chars.peek().copied() == Some('=') {
                        self.bump();
                        out.push(Token { tok: Tok::Assign, pos });
                    } else {
                        out.push(Token { tok: Tok::Colon, pos });
                    }
                }
                '=' => {
                    return Err(DslError::at(pos, "truncated ':=' operator"));
                }
                '{' => {
                    self.bump();
                    out.push(Token { tok: Tok::LBrace, pos });
                }
                '}' => {
                    self.bump();
                    out.push(Token { tok: Tok::RBrace, pos });
                }
                '(' => {
                    self.bump();
                    out.push(Token { tok: Tok::LParen, pos });
                }
                ')' => {
                    self.bump();
                    out.push(Token { tok: Tok::RParen, pos });
                }
                ',' => {
                    self.bump();
                    out.push(Token { tok: Tok::Comma, pos });
                }
                '[' => {
                    self.bump();
                    out.push(Token { tok: Tok::LBracket, pos });
                }
                ']' => {
                    self.bump();
                    out.push(Token { tok: Tok::RBracket, pos });
                }
                '<' | '|' | '>' => {
                    return Err(DslError::at(
                        pos,
                        format!("choice syntax '{}' is not supported", c),
                    ));
                }
                '"' => out.push(Token { tok: Tok::Str(self.lex_string(pos)?), pos }),
                c if c.is_ascii_digit() || c == '-' => {
                    out.push(self.lex_number(pos)?);
                }
                c if c.is_ascii_alphabetic() || c == '_' => {
                    out.push(self.lex_ident(pos));
                }
                other => {
                    return Err(DslError::at(pos, format!("unexpected character {:?}", other)));
                }
            }
        }
    }

    fn lex_string(&mut self, start: Pos) -> DslResult<String> {
        self.bump(); // opening quote
        let mut out = String::new();
        loop {
            match self.bump() {
                Some('"') => return Ok(out),
                Some('\\') => match self.bump() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some(other) => out.push(other),
                    None => return Err(DslError::at(start, "unterminated string literal")),
                },
                Some(other) => out.push(other),
                None => return Err(DslError::at(start, "unterminated string literal")),
            }
        }
    }

    fn lex_number(&mut self, start: Pos) -> DslResult<Token> {
        let mut raw = String::new();
        if self.chars.peek().copied() == Some('-') {
            raw.push('-');
            self.bump();
        }
        let mut saw_digit = false;
        while let Some(c) = self.chars.peek().copied() {
            if c.is_ascii_digit() {
                saw_digit = true;
                raw.push(c);
                self.bump();
            } else {
                break;
            }
        }
        if !saw_digit {
            return Err(DslError::at(start, "expected a digit after '-'"));
        }

        let mut is_float = false;
        if self.chars.peek().copied() == Some('.') {
            // A '.' only belongs to the number when a digit follows; otherwise it
            // is left for the main loop to reject with a positioned error.
            let mut clone = self.chars.clone();
            clone.next();
            if clone.peek().copied().is_some_and(|c| c.is_ascii_digit()) {
                is_float = true;
                raw.push('.');
                self.bump();
                while let Some(c) = self.chars.peek().copied() {
                    if c.is_ascii_digit() {
                        raw.push(c);
                        self.bump();
                    } else {
                        break;
                    }
                }
            }
        }

        let tok = if is_float {
            Tok::Float(raw.parse::<f64>().map_err(|_| DslError::at(start, format!("invalid float {:?}", raw)))?)
        } else {
            Tok::Int(raw.parse::<i64>().map_err(|_| DslError::at(start, format!("invalid integer {:?}", raw)))?)
        };
        Ok(Token { tok, pos: start })
    }

    fn lex_ident(&mut self, start: Pos) -> Token {
        let mut raw = String::new();
        while let Some(c) = self.chars.peek().copied() {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
                raw.push(c);
                self.bump();
            } else {
                break;
            }
        }
        let tok = match raw.as_str() {
            "true" => Tok::True,
            "false" => Tok::False,
            "null" => Tok::Null,
            _ => Tok::Ident(raw),
        };
        Token { tok, pos: start }
    }
}

pub fn tokenize(source: &str) -> DslResult<Vec<Token>> {
    Lexer::new(source).lex()
}
