use std::{fs::{File, OpenOptions}, io::{self, Write}, path::Path};

use db::Database;
use protocol::{Command, ParseResult, Response};

pub struct Aof {
    file: File,
}

impl Aof {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Aof> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)?;

        file.sync_data()?;

        Ok(Aof { file })
    }

    pub fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)?;
        self.file.sync_data()
    }

    pub fn truncate(&self, length: u64) -> io::Result<()> {
        self.file.set_len(length)
    }
}

pub fn should_persist(command: &Command, response: &Response) -> bool {
    match command.name.as_str() {
        "SET" => matches!(response, Response::SimpleString(s) if s == "OK"),
        "DEL" => matches!(response, Response::Integer(1)),
        "EXPIRE" => matches!(response, Response::Integer(1)),
        "PEXPIRE" => matches!(response, Response::Integer(1)),
        "EXPIREAT" => matches!(response, Response::Integer(1)),
        "PEXPIREAT" => matches!(response, Response::Integer(1)),
        "PERSIST" => matches!(response, Response::Integer(1)),
        _ => false
    }
}

pub fn replay<P: AsRef<Path>>(path: P, database: &mut Database) -> io::Result<u64> {
    // Read file into memory. Fine for the scale of this project.
    let bytes = match std::fs::read(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut offset = 0;
    while offset < bytes.len() {
        match protocol::parse(&bytes[offset..]) {
            ParseResult::Complete(command, consumed) => {
                let _ = database.execute(&command);
                offset += consumed;
            }
            ParseResult::Incomplete => break, // truncated tail - crashed mid write
            ParseResult::Error(_) => break, // corrupted
        }
    }

    Ok(offset as u64)
}