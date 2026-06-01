use crate::parser::ParseError;

#[derive(Debug, PartialEq, Eq)]
pub enum ErrorKind {
    // protocol layer
    ProtocolError(String),

    // dispatch layer
    UnknownCommand(String),
    WrongArity(String),

    // semantic layer
    NotAnInteger,
    OutOfRange,
    WrongType,
    SyntaxError,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::ProtocolError(s) => write!(f, "protocol error: {s}"),
            ErrorKind::UnknownCommand(cmd) => write!(f, "unknown command: {cmd}"),
            ErrorKind::WrongArity(name) => write!(f, "wrong arity: {name}"),
            ErrorKind::NotAnInteger => write!(f, "not an integer"),
            ErrorKind::OutOfRange => write!(f, "out of range"),
            ErrorKind::WrongType => write!(f, "wrong type"),
            ErrorKind::SyntaxError => write!(f, "syntax error"),
        }
    }
}

impl From<ParseError> for ErrorKind {
    fn from(value: ParseError) -> Self {
        ErrorKind::ProtocolError(value.to_string())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Null,
    Error(ErrorKind),
    Integer(i64),
    BulkString(String),
    SimpleString(String),
}