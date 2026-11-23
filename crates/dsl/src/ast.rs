use crate::error::Pos;

/// Parsed value on the right-hand side of a field.
///
/// `Ref` is the wiring form: the field names an input port, the value names the
/// producer box. Literals are parameters. Keeping both in one `Value` type
/// means the parser does not need the node registry; the compiler decides
/// whether a field is a parameter or a connection.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
    Ref(String),
    List(Vec<Value>),
}

impl Value {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Int(_) => "integer",
            Value::Float(_) => "float",
            Value::Str(_) => "string",
            Value::Bool(_) => "boolean",
            Value::Null => "null",
            Value::Ref(_) => "reference",
            Value::List(_) => "list",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub value: Value,
    pub pos: Pos,
}

#[derive(Debug, Clone)]
pub struct Definition {
    pub name: String,
    pub kind: String,
    pub fields: Vec<Field>,
    pub pos: Pos,
}

#[derive(Debug, Clone)]
pub struct Import {
    pub path: String,
    pub alias: String,
    pub pos: Pos,
}

#[derive(Debug, Clone)]
pub struct Export {
    pub name: String,
    pub pos: Pos,
}

#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub default: Option<Value>,
    pub pos: Pos,
}

#[derive(Debug, Clone)]
pub struct Component {
    pub name: String,
    pub params: Vec<Param>,
    pub body: Program,
    pub exports: Vec<String>,
    pub pos: Pos,
}

#[derive(Debug, Clone)]
pub struct Instance {
    pub name: String,
    pub component: String,
    pub args: Vec<Field>,
    pub pos: Pos,
}

/// A parsed strategy source: an unordered collection of boxes. References may
/// point forward; the compiler resolves the whole set.
#[derive(Debug, Clone)]
pub struct Program {
    pub imports: Vec<Import>,
    pub exports: Vec<Export>,
    pub components: Vec<Component>,
    pub instances: Vec<Instance>,
    pub definitions: Vec<Definition>,
}
