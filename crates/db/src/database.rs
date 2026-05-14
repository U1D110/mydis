use std::collections::HashMap;
use protocol::{Command, Response};

pub struct Database {
    data: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Database {
        Database { data: HashMap::new() }
    }

    // TODO: The execution of commands feels like a separate concern from the
    //      owning of data. Should we have an `executor` module? 
    pub fn execute(&mut self, cmd: Command) -> Response {
        match cmd.name.as_str() {
            "GET" => self.get(cmd.args),
            "SET" => self.set(cmd.args),
            "DEL" => self.delete(cmd.args),
            _ => Response::Error("Unknown Command".to_string()),
        }
    }

    fn get(&self, args: Vec<String>) -> Response {
        if args.len() != 1 {
            Response::Error("GET requires exactly one argument".to_string())
        } else {
            match self.data.get(&args[0]) {
                Some(v) => Response::Value(v.clone()),
                None => Response::Null,
            }
        }
    }

    fn set(&mut self, args: Vec<String>) -> Response {
        if args.len() != 2 {
            Response::Error("SET requires exactly two arguments".to_string())
        } else {
            let _ = self.data.insert(args[0].clone(), args[1].clone());
            Response::Ok
        }
    }

    fn delete(&mut self, args: Vec<String>) -> Response {
        if args.len() != 1 {
            Response::Error("DEL requires exactly one argument".to_string())
        } else {
            let _ = self.data.remove(&args[0]);
            Response::Ok
        }
    }
}