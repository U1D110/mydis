use std::{
    collections::{
        BTreeSet,
        HashMap,
    }, num::IntErrorKind, time::{
        Duration,
        Instant
    }
};
use protocol::{
    Command,
    ErrorKind,
    Response,
};
use crate::clock::{Clock, SystemClock};

struct Entry {
    value: String,
    expiration: Option<Instant>,
}

enum ExpirationUnits {
    Seconds,
    Milliseconds,
}

pub struct Database {
    inner: InnerDatabase<SystemClock>,
}

impl Database {
    pub fn new() -> Self {
        Self { 
            inner: InnerDatabase {
                data: HashMap::new(),
                expiring_keys: BTreeSet::new(),
                clock: SystemClock,
            }
        }
    }

    pub fn execute(&mut self, cmd: Command) -> Response {
        self.inner.execute(cmd)
    }

    pub fn purge_expired_keys(&mut self) {
        self.inner.purge_expired_keys()
    }

    pub fn next_expiration_timeout(&self) -> i32 {
        self.inner.next_expiration_timeout()
    }
}

struct InnerDatabase<C: Clock = SystemClock> {
    data: HashMap<String, Entry>,
    expiring_keys: BTreeSet<(Instant, String)>,
    clock: C,
}

#[cfg(test)]
impl InnerDatabase<crate::clock::TestClock> {
    fn for_test() -> Self {
        Self { 
            data: HashMap::new(),
            expiring_keys: BTreeSet::new(),
            clock: crate::clock::TestClock::new(),
        }
    }
}

impl<C: Clock> InnerDatabase<C> {
    fn execute(&mut self, cmd: Command) -> Response {
        match cmd.name.as_str() {
            "GET" => {
                if cmd.args.len() != 1 {
                    return Response::Error(
                        ErrorKind::WrongArity("GET".to_string())
                    );
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
                        _ => return Response::Error(ErrorKind::SyntaxError),
                    };
                    self.set_with_expiration(
                        cmd.args[0].clone(),
                        cmd.args[1].clone(),
                        units,
                        cmd.args[3].clone(),
                    )
                } else {
                    Response::Error(
                        ErrorKind::WrongArity("SET".to_string())
                    )
                }
            },

            "DEL" => {
                if cmd.args.len() != 1 {
                    return Response::Error(
                        ErrorKind::WrongArity("DEL".to_string())
                    );
                } 
        
                self.delete(&cmd.args[0])
            },

            "EXPIRE" => {
                if cmd.args.len() != 2 {
                    return Response::Error(
                        ErrorKind::WrongArity("EXPIRE".to_string())
                    );
                }

                self.set_expire(&cmd.args[0], &cmd.args[1], ExpirationUnits::Seconds)
            },

            "PEXPIRE" => {
                if cmd.args.len() != 2 {
                    return Response::Error(
                        ErrorKind::WrongArity("PEXPIRE".to_string())
                    );
                }

                self.set_expire(&cmd.args[0], &cmd.args[1], ExpirationUnits::Milliseconds)
            },
            
            "TTL" => {
                if cmd.args.len() != 1 {
                    return Response::Error(
                        ErrorKind::WrongArity("TTL".to_string())
                    );
                }

                self.time_to_live(&cmd.args[0], ExpirationUnits::Seconds)
            },

            "PTTL" => {
                if cmd.args.len() != 1 {
                    return Response::Error(
                        ErrorKind::WrongArity("PTTL".to_string())
                    );
                }

                self.time_to_live(&cmd.args[0], ExpirationUnits::Milliseconds)
            },

            "PERSIST" => {
                if cmd.args.len() != 1 {
                    return Response::Error(
                        ErrorKind::WrongArity("PERSIST".to_string())
                    );
                }

                self.make_persistent(&cmd.args[0])
            },

            "PING" => Response::SimpleString("PONG".to_string()),

            _ => Response::Error(ErrorKind::UnknownCommand(cmd.name))
        }
    }

    fn purge_expired_keys(&mut self) {
        let now = self.clock.now();
        while let Some(kv) = self.expiring_keys.first() {
            if kv.0 > now {
                break;
            }

            let (_, key) = self.expiring_keys.pop_first().unwrap();
            let _ = self.data.remove(&key);
        }
    }

    fn next_expiration_timeout(&self) -> i32 {
        let now = self.clock.now();

        match self.expiring_keys.first() {
            Some((expiration, _)) => {
                if expiration < &now {
                    0
                } else {
                    let ms = expiration.duration_since(now).as_millis();
                    std::cmp::min(ms, 100) as i32
                }
            },
            None => -1,
        }
    }

    fn get(&mut self, key: &str) -> Response {
        match self.data.get(key) {
            Some(v) => {
                if let Some(t) = v.expiration 
                    && t <= self.clock.now() {
                        // Remove expired key
                        self.data.remove(key);
                        self.expiring_keys.remove(&(t, key.to_string()));
                        return Response::Null;
                    }

                Response::BulkString(v.value.clone())
            },
            None => Response::Null,
        }
    }

    fn set(&mut self, key: String, value: String) -> Response {
        if let Some(entry) = self.data.get(&key) {          
            if let Some(expiration) = entry.expiration {
                self.expiring_keys.remove(&(expiration, key.clone()));
            }
        }

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
        duration: String,
    ) -> Response {
        let new_expiration = match Self::instant_from_string(&self.clock, &duration, units, false) {
            Ok(instant) => instant,
            Err(kind) => return Response::Error(kind),
        };

        if let Some(entry) = self.data.get(&key) {
            if let Some(existing_expiration) = entry.expiration {
                self.expiring_keys.remove(&(existing_expiration, key.clone()));
            }
        }

        let entry = Entry { 
            value,
            expiration: Some(new_expiration),
        };
        let _ = self.data.insert(key.clone(), entry);
        let _ = self.expiring_keys.insert((new_expiration, key));
        Response::SimpleString("OK".to_string())
    }

    fn delete(&mut self, key: &str) -> Response {
        self.remove_if_expired(key);
        
        match self.data.remove(key) {
            Some(entry) => {
                if let Some(expiration) = entry.expiration {
                    self.expiring_keys.remove(&(expiration, key.to_string()));
                }

                Response::Integer(1)
            },
            None => Response::Integer(0),
        }
    }

    fn set_expire(&mut self, key: &str, duration: &str, units: ExpirationUnits) -> Response {
        self.remove_if_expired(key);

        if let Some(entry) = self.data.get_mut(key) {
            let new_expiration = match Self::instant_from_string(&self.clock, duration, units, true) {
                Ok(instant) => instant,
                Err(kind) => return Response::Error(kind),
            }; 

            if let Some(existing_expiration) = entry.expiration {
                self.expiring_keys.remove(&(existing_expiration, key.to_string()));
            }

            self.expiring_keys.insert((new_expiration, key.to_string()));
            entry.expiration = Some(new_expiration);

            Response::Integer(1)
        } else {
            Response::Integer(0)
        }
    }

    fn instant_from_string(clock: &C, duration: &str, units: ExpirationUnits, allow_zero: bool) -> Result<Instant, ErrorKind> {
        let new_expiration = match duration.parse::<i64>() {
            Ok(n) => n,
            Err(err) => {
                let kind = match err.kind() {
                    IntErrorKind::Empty | IntErrorKind::InvalidDigit => {
                        ErrorKind::NotAnInteger
                    },
                    IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => {
                        ErrorKind::OutOfRange
                    },
                    IntErrorKind::Zero => ErrorKind::OutOfRange,
                    _ => ErrorKind::NotAnInteger,
                };
                
                return Err(kind);
            },
        };

        if !allow_zero && new_expiration == 0 || new_expiration < 0 {
            return Err(ErrorKind::OutOfRange);
        }

        let d = match units {
            ExpirationUnits::Seconds => Duration::from_secs(new_expiration as u64),
            ExpirationUnits::Milliseconds => Duration::from_millis(new_expiration as u64),
        };

        match clock.now().checked_add(d) {
            Some(instant) => Ok(instant),
            None => Err(ErrorKind::OutOfRange),
        }
    }

    fn time_to_live(&mut self, key: &str, units: ExpirationUnits) -> Response {
        // returns remaining time to live as an integer
        // Positive integer -> seconds remaining
        // -1 -> key exists but has no expiration
        // -2 -> key does not exist
        self.remove_if_expired(key);

        let ttl = match self.data.get(key) {
            Some(value) => {
                match value.expiration {
                    Some(instant) => {
                        match units {
                            ExpirationUnits::Seconds => {
                                instant
                                    .duration_since(self.clock.now())
                                    .as_secs() as i64
                            },
                            ExpirationUnits::Milliseconds => {
                                instant
                                    .duration_since(self.clock.now())
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
                self.expiring_keys.remove(                
                    &(
                        value.expiration.unwrap(), 
                        key.to_string()
                    )
                );
                value.expiration = None;
                return Response::Integer(1);
            }
        
        Response::Integer(0)
    }

    fn remove_if_expired(&mut self, key: &str) {
        if let Some(entry) = self.data.get(key) {
            if let Some(expiration) = entry.expiration 
                && self.clock.now() > expiration {
                    let _ = self.expiring_keys.remove(&(expiration, key.to_string()));
                    let _ = self.data.remove(key);
            }
        } 
    }
}

//impl Default for Database {
//    fn default() -> Self {
//        Self { 
//            data: Default::default(),
//            expiring_keys: Default::default()
//        }
//    }
//}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn command(name: &str, args: &[&str]) -> Command {
        Command {
            name: name.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn set_get_del_workflow() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value"]));
        assert!(matches!(response, Response::SimpleString(ref s) if s == "OK"));

        let response = db.execute(command("GET", &["key"]));
        assert!(matches!(response, Response::BulkString(ref s) if s == "value"));

        let response = db.execute(command("DEL", &["key"]));
        assert!(matches!(response, Response::Integer(1)));

        let response = db.execute(command("GET", &["key"]));
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn should_get_not_an_integer_error() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value", "EX", "umpteen"]));
        assert!(matches!(response, Response::Error(ErrorKind::NotAnInteger)));
        let response = db.execute(command("SET", &["key", "value", "PX", "eleventy"]));
        assert!(matches!(response, Response::Error(ErrorKind::NotAnInteger)));

        db.execute(command("SET", &["key", "value", "EX", "10"]));

        let response = db.execute(command("EXPIRE", &["key", "soon"]));
        assert!(matches!(response, Response::Error(ErrorKind::NotAnInteger)));
        let response = db.execute(command("PEXPIRE", &["key", "soon"]));
        assert!(matches!(response, Response::Error(ErrorKind::NotAnInteger)));
    }

    #[test]
    fn set_does_not_allow_negative_expiration() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value", "EX", "-1"]));
        assert!(matches!(response, Response::Error(ErrorKind::OutOfRange)));
    }

    #[test]
    fn set_validates_expiration_type() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value", "blurg", "1"]));
        assert!(matches!(response, Response::Error(ErrorKind::SyntaxError)));
        let response = db.execute(command("SET", &["key", "value", "AB", "1"]));
        assert!(matches!(response, Response::Error(ErrorKind::SyntaxError)));
    }

    #[test]
    fn set_does_not_allow_zero_expiration() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value", "EX", "0"]));
        assert!(matches!(response, Response::Error(ErrorKind::OutOfRange)));
    }

    #[test]
    fn expire_allows_zero_as_immediate_expiration() {
        let mut db = Database::new();
        db.execute(command("SET", &["key", "value", "EX", "1"]));
        db.execute(command("EXPIRE", &["key", "0"]));
        let response = db.execute(command("GET", &["key"]));
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn missing_key_returns_null_and_zero() {
        let mut db = Database::new();

        let response = db.execute(command("GET", &["missing"]));
        assert!(matches!(response, Response::Null));

        let response = db.execute(command("DEL", &["missing"]));
        assert!(matches!(response, Response::Integer(0)));
    }

    #[test]
    fn invalid_argument_counts_return_errors() {
        let mut db = Database::new();

        let response = db.execute(command("GET", &[]));
        assert!(matches!(response, Response::Error(ErrorKind::WrongArity(_))));

        let response = db.execute(command("SET", &["key", "value", "EX"]));
        assert!(matches!(response, Response::Error(ErrorKind::WrongArity(_))));

        let response = db.execute(command("EXPIRE", &["key"]));
        assert!(matches!(response, Response::Error(ErrorKind::WrongArity(_))));

        let response = db.execute(command("TTL", &["key", "extra"]));
        assert!(matches!(response, Response::Error(ErrorKind::WrongArity(_))));
    }

    #[test]
    fn unknown_command_returns_error() {
        let mut db = Database::new();

        let response = db.execute(command("UNKNOWN", &[]));
        assert!(matches!(response, Response::Error(ErrorKind::UnknownCommand(_))));
    }

    #[test]
    fn set_with_ex_and_px_expires_as_expected() {
        let mut db = InnerDatabase::for_test();

        let response = db.execute(command("SET", &["temp", "one", "EX", "1"]));
        assert!(matches!(response, Response::SimpleString(ref s) if s == "OK"));

        let response = db.execute(command("GET", &["temp"]));
        assert!(matches!(response, Response::BulkString(ref s) if s == "one"));

        let response = db.execute(command("SET", &["temp_ms", "two", "PX", "50"]));
        assert!(matches!(response, Response::SimpleString(ref s) if s == "OK"));

        db.clock.advance(Duration::from_millis(60));
        let response = db.execute(command("GET", &["temp_ms"]));
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn expire_and_persist_behavior() {
        let mut db = InnerDatabase::for_test();

        db.execute(command("SET", &["k", "v"]));
        let response = db.execute(command("PEXPIRE", &["k", "100"]));
        assert!(matches!(response, Response::Integer(1)));

        let response = db.execute(command("PTTL", &["k"]));
        assert!(matches!(response, Response::Integer(ttl) if ttl >= 0));

        let response = db.execute(command("PERSIST", &["k"]));
        assert!(matches!(response, Response::Integer(1)));

        let response = db.execute(command("PTTL", &["k"]));
        assert!(matches!(response, Response::Integer(-1)));

        db.clock.advance(Duration::from_millis(110));
        let response = db.execute(command("GET", &["k"]));
        assert!(matches!(response, Response::BulkString(ref s) if s == "v"));
    }

    #[test]
    fn ttl_and_pttl_missing_and_persistent_cases() {
        let mut db = Database::new();

        let response = db.execute(command("TTL", &["missing"]));
        assert!(matches!(response, Response::Integer(-2)));

        let response = db.execute(command("PTTL", &["missing"]));
        assert!(matches!(response, Response::Integer(-2)));

        db.execute(command("SET", &["persistent", "x"]));
        let response = db.execute(command("TTL", &["persistent"]));
        assert!(matches!(response, Response::Integer(-1)));
        
        let response = db.execute(command("PTTL", &["persistent"]));
        assert!(matches!(response, Response::Integer(-1)));
    }

    #[test]
    fn purge_cleans_up_expired_keys() {
        let mut db = InnerDatabase::for_test();

        db.execute(command("SET", &["short", "s", "PX", "100"]));

        // Verify purge does not remove unexpired keys
        assert_eq!(true, db.data.contains_key("short"));
        db.purge_expired_keys();
        assert_eq!(true, db.data.contains_key("short"));

        // Wait for expiration
        db.clock.advance(Duration::from_millis(110));

        // Verify the expired key still lingers, then purge and verify it
        // has been removed.
        assert_eq!(true, db.data.contains_key("short"));
        db.purge_expired_keys();
        assert_eq!(false, db.data.contains_key("short"));
    }

    #[test]
    fn overwriting_key_removes_old_expiration() {
        let mut db = InnerDatabase::for_test();

        db.execute(command("SET", &["swap", "one", "EX", "1"]));
        db.execute(command("SET", &["swap", "two"]));

        db.clock.advance(Duration::from_millis(1100));
        let response = db.execute(command("GET", &["swap"]));
        assert!(matches!(response, Response::BulkString(ref s) if s == "two"));
    }
}