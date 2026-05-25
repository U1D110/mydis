use std::{
    collections::HashMap, 
    time::{
        Duration,
        Instant
    }
};
use protocol::{Command, Response};

struct Entry {
    value: String,
    expiration: Option<Instant>,
}

enum ExpirationUnits {
    Seconds,
    Milliseconds,
}

pub struct Database {
    data: HashMap<String, Entry>,
    //expiry: BTreeSet<(Instant, String)>,
}

impl Database {
    pub fn new() -> Database {
        Database { 
            data: HashMap::new(),
            //expiry: BTreeSet::new(),
        }
    }

    pub fn execute(&mut self, cmd: Command) -> Response {
        match cmd.name.as_str() {
            "GET" => {
                if cmd.args.len() != 1 {
                    return Response::Error("GET requires exactly one argument".to_string());
                } 

                self.get(&cmd.args[0])
            },
            "SET" => {
                if cmd.args.len() == 2 {
                    self.set(cmd.args[0].clone(), cmd.args[1].clone())
                } else if cmd.args.len() == 4 {
                    let units = match cmd.args[2].to_uppercase().as_str() {
                        "EX" => ExpirationUnits::Seconds,
                        "PX" => ExpirationUnits::Milliseconds,
                        _ => return Response::Error("Invalid units".to_string()),
                    };
                    self.set_with_expiration(
                        cmd.args[0].clone(),
                        cmd.args[1].clone(),
                        units,
                        cmd.args[3].clone(),
                    )
                } else {
                    Response::Error("SET requires either 2 or 4 arguments".to_string())
                }
            },
            "DEL" => {
                if cmd.args.len() != 1 {
                    return Response::Error("DEL requires exactly one argument".to_string());
                } 
        
                self.delete(&cmd.args[0])
            },
            "EXPIRE" => {
                if cmd.args.len() != 2 {
                    return Response::Error("EXPIRE requires two arguments".to_string())
                }

                self.set_expire(&cmd.args[0], &cmd.args[1], ExpirationUnits::Seconds)
            },
            "PEXPIRE" => {
                if cmd.args.len() != 2 {
                    return Response::Error("PEXPIRE requires two arguments".to_string())
                }

                self.set_expire(&cmd.args[0], &cmd.args[1], ExpirationUnits::Milliseconds)
            },
            "TTL" => {
                if cmd.args.len() != 1 {
                    return Response::Error("TTL requires exactly one argument".to_string());
                }

                self.time_to_live(&cmd.args[0], ExpirationUnits::Seconds)
            },
            "PTTL" => {
                if cmd.args.len() != 1 {
                    return Response::Error("PTTL requires exactly one argument".to_string());
                }

                self.time_to_live(&cmd.args[0], ExpirationUnits::Milliseconds)
            },
            "PERSIST" => {
                if cmd.args.len() != 1 {
                    return Response::Error("PERSIST requires one argument".to_string())
                }

                self.make_persistent(&cmd.args[0])
            },
            "PING" => Response::SimpleString("PONG".to_string()),
            _ => Response::Error("Unknown Command".to_string()),
        }
    }

    fn get(&mut self, key: &str) -> Response {
        match self.data.get(key) {
            Some(v) => {
                if let Some(t) = v.expiration 
                    && t <= Instant::now() {
                        // Remove expired key
                        self.data.remove(key);
                        return Response::Null;
                    }

                Response::BulkString(v.value.clone())
            },
            None => Response::Null,
        }
    }

    fn set(&mut self, key: String, value: String) -> Response {
        let _ = self.data.insert(
            key,
            Entry { value, expiration: None },
        );
        Response::SimpleString("OK".to_string())
    }

    fn set_with_expiration(
        &mut self,
        key: String,
        value: String,
        units: ExpirationUnits,
        duration: String
    ) -> Response {
        let time = match duration.parse::<u64>() {
            Ok(t) => t,
            Err(err) => return Response::Error(err.to_string()),
        };

        let duration = match units {
            ExpirationUnits::Seconds => Duration::from_secs(time),
            ExpirationUnits::Milliseconds => Duration::from_millis(time),
        };

        let expiration = match Instant::now().checked_add(duration) {
            Some(d) => d,
            None => return Response::Error("Duration out of bounds".to_string()),
        };

        let entry = Entry { 
            value,
            expiration: Some(expiration),
        };
        let _ = self.data.insert(key, entry);
        Response::SimpleString("OK".to_string())
    }

    fn delete(&mut self, key: &str) -> Response {
        self.remove_if_expired(key);
        
        match self.data.remove(key) {
            Some(_) => Response::Integer(1),
            None => Response::Integer(0),
        }
    }

    fn set_expire(&mut self, key: &str, duration: &str, units: ExpirationUnits) -> Response {
        self.remove_if_expired(key);

        if let Some(value) = self.data.get_mut(key) {
            let new_expiration = match duration.parse::<i64>() {
                Ok(n) if n <= 0 => return Response::Error("Expire time must be positive".to_string()),
                Ok(n) => n,
                Err(err) => return Response::Error(format!("EXPIRE: {}", err)),
            };

            let d = match units {
                ExpirationUnits::Seconds => Duration::from_secs(new_expiration as u64),
                ExpirationUnits::Milliseconds => Duration::from_millis(new_expiration as u64),
            };

            let instant = match Instant::now().checked_add(d) {
                Some(i) => i,
                None => return Response::Error("duration out of bounds".to_string()),
            };

            value.expiration = Some(instant);
            Response::Integer(1)
        } else {
            Response::Integer(0)
        }
    }

    fn time_to_live(&mut self, key: &str, units: ExpirationUnits) -> Response {
        // returns remaining time to live in seconds as an integer
        // Positive integer -> seconds remaining
        // -1 -> key exists but has no expiry
        // -2 -> key does not exist
        self.remove_if_expired(key);

        let ttl = match self.data.get(key) {
            Some(value) => {
                match value.expiration {
                    Some(instant) => {
                        match units {
                            ExpirationUnits::Seconds => {
                                instant
                                    .duration_since(Instant::now())
                                    .as_secs() as i64
                            },
                            ExpirationUnits::Milliseconds => {
                                instant
                                    .duration_since(Instant::now())
                                    .as_millis() as i64
                            },
                        }
                    },
                    None => -1,
                }
            },
            None => -2,
        };

        Response::Integer(ttl)
    }

    fn make_persistent(&mut self, key: &str) -> Response {
        // remove expiration from key (set to None) but do not remove Entry unless
        // this key was already expired.
        // return 1 if expiry was removed
        // return 0 if key does not exist or has no expiry
        self.remove_if_expired(key);

        if let Some(value) = self.data.get_mut(key) 
            && value.expiration.is_some() {
                value.expiration = None;
                return Response::Integer(1);
            }
        
        Response::Integer(0)
    }

    fn is_expired(&self, key: &str) -> bool {
        match self.data.get(key) {
            Some(entry) => match entry.expiration {
                Some(expiration) => Instant::now() > expiration,
                None => false,
            },
            None => false,
        }
    }

    fn remove_if_expired(&mut self, key: &str) {
        if self.is_expired(key) {
            self.data.remove(key);
        }
    }
}