use crate::Response;

pub fn serialize(response: Response) -> Vec<u8> {
    match response {
        Response::Null => b"$-1\r\n".to_vec(),
        Response::Error(err) => format!("-ERR {}\r\n", err.to_string()).into_bytes(),
        Response::Integer(n) => format!(":{n}\r\n").into_bytes(),
        Response::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
        Response::SimpleString(s) => format!("+{s}\r\n").into_bytes(),
    }
}