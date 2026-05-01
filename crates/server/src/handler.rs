use std::{
    collections::{
        HashMap,
        hash_map::Entry,
    },
    io,
};

use net::{Events, Interests, Poll, TcpListener};

use crate::connection::{
    Connection,
    ConnectionStatus,
};

const READ_BUF_SIZE: usize = 4096;

pub fn handle_events(
    events: &Events,
    connections: &mut HashMap<i32, Connection>,
    listener: &TcpListener,
    poll: &Poll,
) -> io::Result<()> {
    for event in events.iter() {
        if event.fd() == listener.as_raw_fd() {
            loop {
                match listener.accept() {
                    Ok(stream) => {
                        println!("Accepted a connection on fd {}", stream.as_raw_fd());

                        poll.register(stream.as_raw_fd(), Interests::read_only())?;

                        let conn = Connection::new(stream);

                        connections.insert(conn.id(), conn);
                    },
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // kernel accept queue is empty, time to wait again
                        break;
                    },
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                        break;
                    }
                }
            }
        } else {
            if let Entry::Occupied(mut entry) = connections.entry(event.fd()) {
                let mut connection_closed = false;

                let connection = entry.get_mut();
            
                if event.readable() {
                    let mut read_buf = [0u8; READ_BUF_SIZE];

                    loop {
                        match connection.read(&mut read_buf) {
                            Ok(status) => {
                                if status == ConnectionStatus::Closed {
                                    connection_closed = true;
                                    break;
                                }
                                // else we are still draining the read buffer
                            },
                            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                                // Done reading, but connection still active
                                if connection.has_pending_writes() {
                                    poll.reregister(event.fd(), Interests::read_write())?;
                                }
                                break;
                            },
                            Err(err) => {
                                eprintln!("Error reading from stream {}: {err}", event.fd());
                                connection_closed = true;
                                break;
                            },
                        }
                    }

                    // Opportunistic write to save context switch overhead/latency
                    match connection.pump() {
                        // Write buffer drained, switch to read_only
                        Ok(()) => poll.reregister(event.fd(), Interests::read_only())?,
                        // OS send buffer full, bytes still waiting in write buffer
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            poll.reregister(event.fd(), Interests::read_write())?;
                        },
                        Err(err) => {
                            eprintln!("Write error after read: {err}");
                            connection_closed = true;
                        }
                    }
                }

                if event.rdhup() {
                    connection_closed = true;
                }

                if event.writable() && !connection_closed {
                    match connection.pump() {
                        Ok(()) => poll.reregister(event.fd(), Interests::read_only())?,
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            // Send buffer is full. No change to registered interests.
                        },
                        Err(err) => {
                            eprintln!("Write error on writable wakeup: {err}");
                            connection_closed = true;
                        },
                    }
                }

                if event.error() || event.hang_up() {
                    connection_closed = true;
                }
            
                if connection_closed {
                    entry.remove();
                }
            }
        }   
    }

    Ok(())
}