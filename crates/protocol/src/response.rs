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

impl ErrorKind {
    pub fn message(&self) -> String {
        match self {
            ErrorKind::UnknownCommand(msg) => format!("unknown command: {msg}"),
            ErrorKind::WrongArity(msg) => {
                format!("wrong arity: {msg}")
            },
            ErrorKind::ProtocolError(msg) => format!("protocol error: {msg}"),
            ErrorKind::NotAnInteger => "not an integer".to_string(),
            ErrorKind::OutOfRange => "out of range".to_string(),
            ErrorKind::WrongType => "wrong type".to_string(),
            ErrorKind::SyntaxError => "syntax error".to_string(),
        }
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