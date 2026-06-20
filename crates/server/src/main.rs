mod aof;
mod connection;
mod handler;

use crate::{aof::Aof, connection::Connection, handler::handle_events};

use std::{collections::HashMap, io};

use db::Database;
use net::{Events, Interests, Poll, TcpListener};

const EVENT_BUF_SIZE: usize = 1024;
const PORT: &str = "3490";

fn main() -> io::Result<()> {
    let mut database = Database::new();

    let listener = TcpListener::bind(PORT)?;
    let poll = Poll::new()?;

    poll.register(listener.as_raw_fd(), Interests::read_only())?;

    let mut connections: HashMap<i32, Connection> = HashMap::new();
    let mut events = Events::with_capacity(EVENT_BUF_SIZE);

    let aof_path = std::env::var("MYDIS_AOF_PATH")
        .unwrap_or_else(|_| "appendonly.aof".to_string());

    let valid_length = aof::replay(&aof_path, &mut database)?;
    let mut aof = Aof::open(&aof_path)?;
    aof.truncate(valid_length)?;

    println!("Server waiting for connections...");

    loop {
        let timeout_ms = database.next_expiration_timeout();

        poll.wait(&mut events, timeout_ms)?;

        handle_events(&events, &mut connections, &listener, &poll, &mut database, &mut aof)?;

        database.purge_expired_keys();
    }
}
