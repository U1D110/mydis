mod aof;
mod connection;
mod handler;

use crate::{aof::Aof, connection::Connection, handler::handle_events};

use std::{collections::HashMap, io, os::fd::{AsFd, RawFd}};

use db::Database;
use net::{Events, Interests, Poll, Signals, TcpListener};

const EVENT_BUF_SIZE: usize = 1024;
const PORT: &str = "3490";

fn main() -> io::Result<()> {
    let signals = Signals::new()?;
    let mut database = Database::new();

    let listener = TcpListener::bind(PORT)?;
    let poll = Poll::new()?;

    poll.register(listener.as_fd(), Interests::read_only())?;

    let mut connections: HashMap<RawFd, Connection> = HashMap::new();
    let mut events = Events::with_capacity(EVENT_BUF_SIZE);

    let aof_path = std::env::var("MYDIS_AOF_PATH")
        .unwrap_or_else(|_| "appendonly.aof".to_string());

    let valid_length = aof::replay(&aof_path, &mut database)?;
    let mut aof = Aof::open(&aof_path, valid_length)?;

    poll.register(aof.notify_fd(), Interests::read_only())?;
    poll.register(signals.as_fd(), Interests::read_only())?;

    println!("Server waiting for connections...");

    let mut next_gen = 0;
    let mut running = true;
    while running {
        let timeout_ms = database.next_expiration_timeout();

        poll.wait(&mut events, timeout_ms)?;

        running = handle_events(
            &events,
            &mut connections,
            &mut next_gen,
            &listener,
            &poll,
            &mut database,
            &mut aof,
            &signals
        )?;

        database.purge_expired_keys();
    }

    println!("Shutting down. Draining AOF worker...");
    aof.shutdown();
    Ok(())
}
