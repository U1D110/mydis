mod connection;
mod handler;

use crate::{
    connection::Connection,
    handler::handle_events,
};

use std::{
    collections::HashMap, 
    io,
};

use net::{
    Events, Interests, Poll, TcpListener
};

const EVENT_BUF_SIZE: usize = 1024;
const PORT: &str = "3490";

fn main() -> io::Result<()> {
    let listener = TcpListener::bind(PORT)?;
    let poll = Poll::new()?;

    poll.register(listener.as_raw_fd(), Interests::read_only())?;
    
    let mut connections: HashMap<i32, Connection> = HashMap::new();
    let mut events = Events::with_capacity(EVENT_BUF_SIZE);

    println!("Server waiting for connections...");

    loop {
        let _ = poll.wait(&mut events)?;
        handle_events(&events, &mut connections, &listener, &poll)?;
    }
}
