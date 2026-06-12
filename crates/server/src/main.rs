mod connection;
mod handler;

use crate::{connection::Connection, handler::handle_events};

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

    println!("Server waiting for connections...");

    loop {
        let timeout_ms = database.next_expiration_timeout();

        poll.wait(&mut events, timeout_ms)?;

        handle_events(&events, &mut connections, &listener, &poll, &mut database)?;

        database.purge_expired_keys();
    }
}
