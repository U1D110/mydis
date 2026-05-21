#[derive(Debug)]
pub enum Response {
    Null,
    Error(String),
    Integer(i64),
    BulkString(String),
    SimpleString(String),
}