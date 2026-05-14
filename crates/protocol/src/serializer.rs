use crate::Response;

pub fn serialize(reponse: Response) -> Vec<u8> {
    match reponse {
        Response::Ok => b"OK\n".to_vec(),
        Response::Value(val) => format!("{val}\n").into_bytes(),
        Response::Error(err) => format!("ERR {err}\n").into_bytes(),
        Response::Null => b"(nil)\n".to_vec(),
    }
}