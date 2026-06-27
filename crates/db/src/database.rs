use crate::clock::{Clock, SystemClock};
use protocol::{Command, ErrorKind, Response};
use std::{
    collections::{BTreeSet, HashMap},
    num::IntErrorKind,
};

const MAX_POLL_INTERVAL_MS: u64 = 100;

struct Entry {
    value: String,
    expiration_ms: Option<u64>,
}

enum ExpirationUnits {
    Seconds,
    Milliseconds,
}

enum ExpirationType {
    Absolute,
    Relative,
}

pub struct CommandResult {
    pub response: Response,
    pub persist: Option<Command>,
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
            },
        }
    }

    pub fn execute(&mut self, cmd: Command) -> CommandResult {
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
    // For a production grade project, BinaryHeap would
    // possibly be the better choice along with a tombstone
    // strategy for dead key removal.
    expiring_keys: BTreeSet<(u64, String)>,
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
    fn execute(&mut self, cmd: Command) -> CommandResult {
        let Command { name, args } = cmd;

        match name.as_str() {
            "GET" => {
                let response = if args.len() == 1 {
                    self.get(&args[0])
                } else {
                    Response::Error(ErrorKind::WrongArity("GET".to_string()))
                };

                CommandResult { response, persist: None }
            }

            "SET" => {
                if args.len() == 2 {
                    CommandResult {
                        response: self.set(&args[0], &args[1]),
                        persist: Some(Command { name, args }), 
                    }
                } else if args.len() == 4 {
                    let [key, value, flag, duration] = args.try_into().unwrap();
                    let (units, exp_type) = match flag.to_uppercase().as_str() {
                        "EX" => (ExpirationUnits::Seconds, ExpirationType::Relative),
                        "PX" => (ExpirationUnits::Milliseconds, ExpirationType::Relative),
                        "EXAT" => (ExpirationUnits::Seconds, ExpirationType::Absolute),
                        "PXAT" => (ExpirationUnits::Milliseconds, ExpirationType::Absolute),
                        _ => return CommandResult { 
                                response: Response::Error(ErrorKind::SyntaxError),
                                persist: None,
                        }
                    };

                    let response = self.set_with_expiration(&key, &value, units, exp_type, &duration);

                    let persist = if matches!(response, Response::SimpleString(ref s) if s == "OK") {
                        let ms = self.data.get(&key).and_then(|e| e.expiration_ms).unwrap();
                        Some(Command { name, args: vec![key, value, "PXAT".to_string(), ms.to_string()] })
                    } else { 
                        None 
                    };

                    CommandResult { response, persist }
                } else {
                    CommandResult {
                        response: Response::Error(ErrorKind::WrongArity("SET".to_string())),
                        persist: None,
                    }
                }
            }

            "DEL" => {
                let response = if args.len() == 1 {
                    self.delete(&args[0])
                } else {
                    Response::Error(ErrorKind::WrongArity("DEL".to_string()))
                };

                let persist = if matches!(response, Response::Integer(1)) {
                    Some(Command { name, args })
                } else {
                    None
                };

                CommandResult { response, persist }
            }

            "EXPIRE" => {
                let response = if args.len() == 2 {
                    self.set_expire(&args[0], &args[1], ExpirationUnits::Seconds, ExpirationType::Relative)
                } else {
                    Response::Error(ErrorKind::WrongArity("EXPIRE".to_string()))
                };

                let persist = if matches!(response, Response::Integer(1)) {
                    match self.data.get(&args[0]).and_then(|e| e.expiration_ms) {
                        Some(ms) => Some(Command { name: "PEXPIREAT".to_string(), args: vec![args[0].clone(), ms.to_string()] }),
                        None => Some(Command { name: "DEL".to_string(), args: vec![args[0].clone()]}),
                    }
                } else {
                    None
                };

                CommandResult { response, persist }
            }

            "PEXPIRE" => {
                let response = if args.len() == 2 {
                    self.set_expire(
                        &args[0],
                        &args[1],
                        ExpirationUnits::Milliseconds,
                        ExpirationType::Relative,
                    )
                } else {
                    Response::Error(ErrorKind::WrongArity("PEXPIRE".to_string()))
                };

                let persist = if matches!(response, Response::Integer(1)) {
                    match self.data.get(&args[0]).and_then(|e| e.expiration_ms) {
                        Some(ms) => Some( Command { name: "PEXPIREAT".to_string(), args: vec![args[0].clone(), ms.to_string()]}),
                        None => Some(Command { name: "DEL".to_string(), args: vec![args[0].clone()]}),
                    }
                } else {
                    None
                };

                CommandResult { response, persist }
            }

            "EXPIREAT" => {
                let response = if args.len() == 2 {
                    self.set_expire(&args[0], &args[1], ExpirationUnits::Seconds, ExpirationType::Absolute)
                } else {
                    Response::Error(ErrorKind::WrongArity("EXPIREAT".to_string()))
                };

                let persist = if matches!(response, Response::Integer(1)) {
                    let ms = self.data.get(&args[0]).and_then(|e| e.expiration_ms);
                    match ms {
                        Some(ms) => Some(Command { name: "PEXPIREAT".to_string(), args: vec![args[0].clone(), ms.to_string()] }),
                        None => Some(Command { name: "DEL".to_string(), args: vec![args[0].clone()]}),
                    }
                } else {
                    None
                };

                CommandResult { response, persist }
            }

            "PEXPIREAT" => {
                let response = if args.len() == 2 {
                    self.set_expire(
                        &args[0],
                        &args[1],
                        ExpirationUnits::Milliseconds,
                        ExpirationType::Absolute,
                    )
                } else {
                    Response::Error(ErrorKind::WrongArity("PEXPIREAT".to_string()))
                };

                let persist = if matches!(response, Response::Integer(1)) {
                    Some(Command { name, args })
                } else {
                    None
                };

                CommandResult { response, persist }
            }

            "TTL" => {
                let response = if args.len() == 1 {
                    self.time_to_live(&args[0], ExpirationUnits::Seconds)
                } else {
                    Response::Error(ErrorKind::WrongArity("TTL".to_string()))
                };
                
                CommandResult { response, persist: None }
            }

            "PTTL" => {
                let response = if args.len() == 1 {
                    self.time_to_live(&args[0], ExpirationUnits::Milliseconds)
                } else {
                    Response::Error(ErrorKind::WrongArity("PTTL".to_string()))
                };

                CommandResult { response, persist: None }
            }

            "PERSIST" => {
                let response = if args.len() == 1 {
                    self.make_persistent(&args[0])
                } else {
                    Response::Error(ErrorKind::WrongArity("PERSIST".to_string()))
                };

                let persist = if matches!(response, Response::Integer(1)) {
                    Some(Command { name, args })
                } else {
                    None
                };

                CommandResult { response, persist }
            }

            "PING" => CommandResult { response: Response::SimpleString("PONG".to_string()), persist: None },

            _ => CommandResult { response: Response::Error(ErrorKind::UnknownCommand(name)), persist: None },
        }
    }

    fn purge_expired_keys(&mut self) {
        let now = self.clock.now().as_millis() as u64;
        while let Some(kv) = self.expiring_keys.first() {
            if kv.0 > now {
                break;
            }

            let (_, key) = self.expiring_keys.pop_first().unwrap();
            let _ = self.data.remove(&key);
        }
    }

    fn next_expiration_timeout(&self) -> i32 {
        let now = self.clock.now().as_millis() as u64;

        match self.expiring_keys.first() {
            Some((expiration, _)) => {
                let ms = expiration.saturating_sub(now);
                std::cmp::min(ms, MAX_POLL_INTERVAL_MS) as i32
            }
            None => -1,
        }
    }

    fn get(&mut self, key: &str) -> Response {
        match self.data.get(key) {
            Some(v) => {
                if let Some(t) = v.expiration_ms
                {
                    let now = self.clock.now().as_millis() as u64;

                    if t <= now {
                        // Remove expired key
                        self.data.remove(key);
                        self.expiring_keys.remove(&(t, key.to_string()));
                        return Response::Null;
                    }
                }

                Response::BulkString(v.value.clone())
            }
            None => Response::Null,
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Response {
        if let Some(entry) = self.data.get(key) {
            if let Some(expiration_ms) = entry.expiration_ms {
                self.expiring_keys.remove(&(expiration_ms, key.to_string()));
            }
        }

        let _ = self.data.insert(
            key.to_string(),
            Entry {
                value: value.to_string(),
                expiration_ms: None,
            },
        );

        Response::SimpleString("OK".to_string())
    }

    fn set_with_expiration(
        &mut self,
        key: &str,
        value: &str,
        units: ExpirationUnits,
        exp_type: ExpirationType,
        duration: &str,
    ) -> Response {
        let new_expiration =
            match Self::expiration_from_string(&self.clock, duration, units, exp_type, false) {
                Ok(Some(instant)) => instant,
                Ok(None) => unreachable!(),
                Err(kind) => return Response::Error(kind),
            };

        if let Some(entry) = self.data.get(key) {
            if let Some(existing_expiration) = entry.expiration_ms {
                self.expiring_keys
                    .remove(&(existing_expiration, key.to_string()));
            }
        }

        let entry = Entry {
            value: value.to_string(),
            expiration_ms: Some(new_expiration),
        };
        let _ = self.data.insert(key.to_string(), entry);
        let _ = self.expiring_keys.insert((new_expiration, key.to_string()));
        Response::SimpleString("OK".to_string())
    }

    fn delete(&mut self, key: &str) -> Response {
        self.remove_if_expired(key);

        match self.data.remove(key) {
            Some(entry) => {
                if let Some(expiration_ms) = entry.expiration_ms {
                    self.expiring_keys.remove(&(expiration_ms, key.to_string()));
                }

                Response::Integer(1)
            }
            None => Response::Integer(0),
        }
    }

    fn set_expire(
        &mut self,
        key: &str,
        duration: &str,
        units: ExpirationUnits,
        exp_type: ExpirationType,
    ) -> Response {
        self.remove_if_expired(key);

        let existing_expiration = match self.data.get(key) {
            Some(entry) => entry.expiration_ms,
            None => return Response::Integer(0),
        };

        let new_expiration =
            match Self::expiration_from_string(&self.clock, duration, units, exp_type, true) {
                Ok(opt) => opt,
                Err(kind) => return Response::Error(kind),
            };

        if let Some(exp) = existing_expiration {
            self.expiring_keys.remove(&(exp, key.to_string()));
        }

        match new_expiration {
            None => {
                self.data.remove(key);
            }
            Some(expiration) => {
                self.data.get_mut(key).unwrap().expiration_ms = Some(expiration); // key verified above; no data mutations between check and here
                self.expiring_keys.insert((expiration, key.to_string()));
            }
        }

        Response::Integer(1)
    }

    fn expiration_from_string(
        clock: &C,
        duration: &str,
        units: ExpirationUnits,
        exp_type: ExpirationType,
        allow_non_positive: bool,
    ) -> Result<Option<u64>, ErrorKind> {
        let parsed_int = match duration.parse::<i64>() {
            Ok(n) => n,
            Err(err) => {
                let kind = match err.kind() {
                    IntErrorKind::Empty | IntErrorKind::InvalidDigit => ErrorKind::NotAnInteger,
                    IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => ErrorKind::OutOfRange,
                    //IntErrorKind::Zero => ErrorKind::OutOfRange,
                    _ => ErrorKind::NotAnInteger,
                };

                return Err(kind);
            }
        };

        match exp_type {
            ExpirationType::Absolute => {
                let now = match units {
                    ExpirationUnits::Seconds => clock.now().as_secs(),
                    ExpirationUnits::Milliseconds => clock.now().as_millis() as u64,
                };

                if parsed_int as u64 <= now {
                    if allow_non_positive {
                        return Ok(None);
                    } else {
                        return Err(ErrorKind::OutOfRange);
                    }
                }

                let new_expiration_ms = match units {
                    ExpirationUnits::Seconds => {
                        match (parsed_int as u64).checked_mul(1000) {
                            Some(value) => value,
                            None => return Err(ErrorKind::OutOfRange),
                        }
                    }
                    ExpirationUnits::Milliseconds => parsed_int as u64,
                };

                Ok(Some(new_expiration_ms))
            }
            ExpirationType::Relative => {
                if parsed_int <= 0 {
                    if allow_non_positive {
                        return Ok(None);
                    } else {
                        return Err(ErrorKind::OutOfRange);
                    }
                }

                let new_expiration_ms = match units {
                    ExpirationUnits::Seconds => {
                        match (parsed_int as u64).checked_mul(1000) {
                            Some(value) => value,
                            None => return Err(ErrorKind::OutOfRange),
                        }
                    }
                    ExpirationUnits::Milliseconds => parsed_int as u64,
                };

                let now_ms = clock.now().as_millis() as u64;
                Ok(Some(now_ms + new_expiration_ms))
            }
        }
    }

    fn time_to_live(&mut self, key: &str, units: ExpirationUnits) -> Response {
        // returns remaining time to live as an integer
        // Positive integer -> seconds remaining
        // -1 -> key exists but has no expiration
        // -2 -> key does not exist
        self.remove_if_expired(key);

        let ttl = match self.data.get(key) {
            Some(value) => match value.expiration_ms {
                Some(expiration_ms) => {
                    let remaining_ms = expiration_ms.saturating_sub(self.clock.now().as_millis() as u64);
                    match units {
                        ExpirationUnits::Seconds => (remaining_ms / 1000) as i64,
                        ExpirationUnits::Milliseconds => remaining_ms as i64,
                    }
                },
                None => -1,
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
            && value.expiration_ms.is_some()
        {
            self.expiring_keys
                .remove(&(value.expiration_ms.unwrap(), key.to_string()));
            value.expiration_ms = None;
            return Response::Integer(1);
        }

        Response::Integer(0)
    }

    fn remove_if_expired(&mut self, key: &str) {
        if let Some(entry) = self.data.get(key) {
            if let Some(expiration_ms) = entry.expiration_ms
                && (self.clock.now().as_millis() as u64) > expiration_ms
            {
                let _ = self.expiring_keys.remove(&(expiration_ms, key.to_string()));
                let _ = self.data.remove(key);
            }
        }
    }
}

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

        let response = db.execute(command("SET", &["key", "value"])).response;
        assert!(matches!(response, Response::SimpleString(ref s) if s == "OK"));

        let response = db.execute(command("GET", &["key"])).response;
        assert!(matches!(response, Response::BulkString(ref s) if s == "value"));

        let response = db.execute(command("DEL", &["key"])).response;
        assert!(matches!(response, Response::Integer(1)));

        let response = db.execute(command("GET", &["key"])).response;
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn should_get_not_an_integer_error() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value", "EX", "umpteen"])).response;
        assert!(matches!(response, Response::Error(ErrorKind::NotAnInteger)));
        let response = db.execute(command("SET", &["key", "value", "PX", "eleventy"])).response;
        assert!(matches!(response, Response::Error(ErrorKind::NotAnInteger)));

        db.execute(command("SET", &["key", "value", "EX", "10"]));

        let response = db.execute(command("EXPIRE", &["key", "soon"])).response;
        assert!(matches!(response, Response::Error(ErrorKind::NotAnInteger)));
        let response = db.execute(command("PEXPIRE", &["key", "soon"])).response;
        assert!(matches!(response, Response::Error(ErrorKind::NotAnInteger)));
    }

    #[test]
    fn set_does_not_allow_negative_expiration() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value", "EX", "-1"])).response;
        assert!(matches!(response, Response::Error(ErrorKind::OutOfRange)));
    }

    #[test]
    fn set_validates_expiration_type() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value", "blurg", "1"])).response;
        assert!(matches!(response, Response::Error(ErrorKind::SyntaxError)));
        let response = db.execute(command("SET", &["key", "value", "AB", "1"])).response;
        assert!(matches!(response, Response::Error(ErrorKind::SyntaxError)));
    }

    #[test]
    fn set_does_not_allow_zero_expiration() {
        let mut db = Database::new();

        let response = db.execute(command("SET", &["key", "value", "EX", "0"])).response;
        assert!(matches!(response, Response::Error(ErrorKind::OutOfRange)));
    }

    #[test]
    fn expire_allows_zero_as_immediate_expiration() {
        let mut db = Database::new();
        db.execute(command("SET", &["key", "value", "EX", "1"]));
        db.execute(command("EXPIRE", &["key", "0"]));
        let response = db.execute(command("GET", &["key"])).response;
        assert!(matches!(response, Response::Null));
        
        db.execute(command("SET", &["key", "value", "PX", "1000"]));
        db.execute(command("PEXPIRE", &["key", "0"]));
        let response = db.execute(command("GET", &["key"])).response;
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn missing_key_returns_null_and_zero() {
        let mut db = Database::new();

        let response = db.execute(command("GET", &["missing"])).response;
        assert!(matches!(response, Response::Null));

        let response = db.execute(command("DEL", &["missing"])).response;
        assert!(matches!(response, Response::Integer(0)));
    }

    #[test]
    fn invalid_argument_counts_return_errors() {
        let mut db = Database::new();

        let response = db.execute(command("GET", &[])).response;
        assert!(matches!(
            response,
            Response::Error(ErrorKind::WrongArity(_))
        ));

        let response = db.execute(command("SET", &["key", "value", "EX"])).response;
        assert!(matches!(
            response,
            Response::Error(ErrorKind::WrongArity(_))
        ));

        let response = db.execute(command("EXPIRE", &["key"])).response;
        assert!(matches!(
            response,
            Response::Error(ErrorKind::WrongArity(_))
        ));

        let response = db.execute(command("TTL", &["key", "extra"])).response;
        assert!(matches!(
            response,
            Response::Error(ErrorKind::WrongArity(_))
        ));
    }

    #[test]
    fn unknown_command_returns_error() {
        let mut db = Database::new();

        let response = db.execute(command("UNKNOWN", &[])).response;
        assert!(matches!(
            response,
            Response::Error(ErrorKind::UnknownCommand(_))
        ));
    }

    #[test]
    fn set_with_ex_and_px_expires_as_expected() {
        let mut db = InnerDatabase::for_test();

        let response = db.execute(command("SET", &["temp", "one", "EX", "1"])).response;
        assert!(matches!(response, Response::SimpleString(ref s) if s == "OK"));

        let response = db.execute(command("GET", &["temp"])).response;
        assert!(matches!(response, Response::BulkString(ref s) if s == "one"));

        let response = db.execute(command("SET", &["temp_ms", "two", "PX", "50"])).response;
        assert!(matches!(response, Response::SimpleString(ref s) if s == "OK"));

        db.clock.advance(Duration::from_millis(60));
        let response = db.execute(command("GET", &["temp_ms"])).response;
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn expire_and_persist_behavior() {
        let mut db = InnerDatabase::for_test();

        db.execute(command("SET", &["k", "v"]));
        let response = db.execute(command("PEXPIRE", &["k", "100"])).response;
        assert!(matches!(response, Response::Integer(1)));

        let response = db.execute(command("PTTL", &["k"])).response;
        assert!(matches!(response, Response::Integer(ttl) if ttl == 100));

        let response = db.execute(command("PERSIST", &["k"])).response;
        assert!(matches!(response, Response::Integer(1)));

        let response = db.execute(command("PTTL", &["k"])).response;
        assert!(matches!(response, Response::Integer(-1)));

        db.clock.advance(Duration::from_millis(110));
        let response = db.execute(command("GET", &["k"])).response;
        assert!(matches!(response, Response::BulkString(ref s) if s == "v"));
    }

    #[test]
    fn expireat_behavior() {
        let mut db = InnerDatabase::for_test();

        db.execute(command("SET", &["k", "v"]));

        let deadline = db.clock.now().as_millis() as u64 + 100;
        let response = db.execute(command("PEXPIREAT", &["k", &deadline.to_string()])).response;
        assert!(matches!(response, Response::Integer(1)));

        let response = db.execute(command("PTTL", &["k"])).response;
        assert!(matches!(response, Response::Integer(ttl) if ttl == 100));

        db.clock.advance(Duration::from_millis(50));
        let response = db.execute(command("GET", &["k"])).response;
        assert!(matches!(response, Response::BulkString(ref s) if s == "v"));

        db.clock.advance(Duration::from_millis(50));
        let response = db.execute(command("GET", &["k"])).response;
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn expireat_seconds_behavior() {
        let mut db = InnerDatabase::for_test();

        db.execute(command("SET", &["k", "v"]));

        let deadline = db.clock.now().as_secs() + 1;
        let response = db.execute(command("EXPIREAT", &["k", &deadline.to_string()])).response;
        assert!(matches!(response, Response::Integer(1)));

        let response = db.execute(command("TTL", &["k"])).response;
        assert!(matches!(response, Response::Integer(n) if n >= 0 && n < 2));

        db.clock.advance(Duration::from_secs(1));
        let response = db.execute(command("GET", &["k"])).response;
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn set_exat_behavior() {
        let mut db = InnerDatabase::for_test();

        let deadline = db.clock.now().as_secs() + 1;
        let response = db.execute(command(
            "SET",
            &["k", "v", "EXAT", &deadline.to_string()],
        )).response;
        assert!(matches!(response, Response::SimpleString(ref s) if s == "OK"));

        let response = db.execute(command("GET", &["k"])).response;
        assert!(matches!(response, Response::BulkString(ref s) if s == "v"));

        db.clock.advance(Duration::from_secs(1));
        let response = db.execute(command("GET", &["k"])).response;
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn set_pxat_behavior() {
        let mut db = InnerDatabase::for_test();

        let deadline = db.clock.now().as_millis() as u64 + 100;
        let response = db.execute(command(
            "SET",
            &["k", "v", "PXAT", &deadline.to_string()],
        )).response;
        assert!(matches!(response, Response::SimpleString(ref s) if s == "OK"));

        db.clock.advance(Duration::from_millis(50));
        let response = db.execute(command("GET", &["k"])).response;
        assert!(matches!(response, Response::BulkString(ref s) if s == "v"));

        db.clock.advance(Duration::from_millis(50));
        let response = db.execute(command("GET", &["k"])).response;
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn ttl_and_pttl_missing_and_persistent_cases() {
        let mut db = Database::new();

        let response = db.execute(command("TTL", &["missing"])).response;
        assert!(matches!(response, Response::Integer(-2)));

        let response = db.execute(command("PTTL", &["missing"])).response;
        assert!(matches!(response, Response::Integer(-2)));

        db.execute(command("SET", &["persistent", "x"]));
        let response = db.execute(command("TTL", &["persistent"])).response;
        assert!(matches!(response, Response::Integer(-1)));

        let response = db.execute(command("PTTL", &["persistent"])).response;
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
        let response = db.execute(command("GET", &["swap"])).response;
        assert!(matches!(response, Response::BulkString(ref s) if s == "two"));
    }

    #[test]
    fn next_expiration_is_negative_one_when_no_expiring_keys() {
        let mut db = InnerDatabase::for_test();
        db.execute(command("SET", &["key", "value"]));
        assert_eq!(db.next_expiration_timeout(), -1, "Next expiration should have been -1");
    }

    #[test]
    fn next_expiration_is_capped() {
        let mut db = InnerDatabase::for_test();
        let exp = MAX_POLL_INTERVAL_MS * 2;
        db.execute(command("SET", &["key", "value", "PX", &exp.to_string()]));
        assert_eq!(db.next_expiration_timeout(), MAX_POLL_INTERVAL_MS as i32);
    }

    #[test]
    fn next_expiration_is_below_cap() {
        let mut db = InnerDatabase::for_test();
        let exp = MAX_POLL_INTERVAL_MS / 2;
        db.execute(command("SET", &["key", "value", "PX", &exp.to_string()]));
        assert_eq!(db.next_expiration_timeout(), exp as i32);
    }

    #[test]
    fn next_expiration_is_zero_when_key_already_expired() {
        let mut db = InnerDatabase::for_test();
        db.execute(command("SET", &["key", "value", "PX", "50"]));
        db.clock.advance(Duration::from_millis(60));
        assert_eq!(db.next_expiration_timeout(), 0);
    }
}
