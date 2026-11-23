use std::fmt;

/// 1-based source position (line, column) used by every DSL error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Pos {
    pub line: usize,
    pub col: usize,
}

impl Pos {
    pub fn new(line: usize, col: usize) -> Self {
        Self { line, col }
    }
}

impl fmt::Display for Pos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.col)
    }
}

/// A positioned DSL diagnostic. Compilation never panics on bad input: every
/// failure is a `DslError` carrying the best position available.
#[derive(Debug, Clone)]
pub struct DslError {
    pub message: String,
    pub pos: Option<Pos>,
}

impl DslError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), pos: None }
    }

    pub fn at(pos: Pos, message: impl Into<String>) -> Self {
        Self { message: message.into(), pos: Some(pos) }
    }
}

impl fmt::Display for DslError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.pos {
            Some(p) => write!(f, "{} ({}:{})", self.message, p.line, p.col),
            None => write!(f, "{}", self.message),
        }
    }
}

impl std::error::Error for DslError {}

pub type DslResult<T> = Result<T, DslError>;
