pub enum Response {
    Ok,
    Value(String),
    Error(String),
    Null,
}