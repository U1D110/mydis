use std::{
    collections::{HashMap, hash_map::Entry},
    io,
};

use db::Database;
use net::{Events, Interests, Poll, Signals, TcpListener};
use protocol::{ErrorKind, ParseResult, Response};

use crate::{aof::{Aof, FlushRequest}, connection::{Connection, ConnectionStatus}};

pub fn handle_events(
    events: &Events,
    connections: &mut HashMap<i32, Connection>,
    listener: &TcpListener,
    poll: &Poll,
    database: &mut Database,
    aof: &mut Aof,
    signals: &Signals,
) -> io::Result<bool> {
    for event in events.iter() {
        if event.fd() == listener.as_raw_fd() {
            loop {
                match listener.accept() {
                    Ok(stream) => {
                        println!("Accepted a connection on fd {}", stream.as_raw_fd());

                        poll.register(stream.as_raw_fd(), Interests::read_only())?;

                        let conn = Connection::new(stream);

                        connections.insert(conn.id(), conn);
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // kernel accept queue is empty, time to wait again
                        break;
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                        break;
                    }
                }
            }
        } else if event.fd() == aof.notify_fd() {
            for completion in aof.drain_completions()? {
                let close = if let Some(connection) = connections.get_mut(&completion.fd) {
                    match completion.result {
                        Ok(()) => {
                            connection.resume_after_flush();
                            let process_says_close = process_commands(connection, database, aof, completion.fd)?;
                            let flush_says_close = flush(connection, poll, completion.fd)?;
                            process_says_close || flush_says_close
                        }
                        Err(_) => {
                            // Discard pending flush because we never want to acknowledge a lost write
                            connection.discard_pending_flush();
                            true
                        }
                    }
                } else {
                    // client already disconnected, write is still durable so nothing to resume.
                    false
                };

                if close {
                    connections.remove(&completion.fd);
                }
            }
        } else if let Entry::Occupied(mut entry) = connections.entry(event.fd()) {
            let mut connection_closed = false;

            let connection = entry.get_mut();

            if event.readable() {
                loop {
                    match connection.read() {
                        Ok(status) => {
                            if status == ConnectionStatus::Closed {
                                connection_closed = true;
                                break;
                            }
                            // else we are still draining the read buffer
                        }
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            // Done reading, but connection still active
                            if connection.has_pending_writes() {
                                poll.reregister(event.fd(), Interests::read_write())?;
                            }
                            break;
                        }
                        Err(err) => {
                            eprintln!("Error reading from stream {}: {err}", event.fd());
                            connection_closed = true;
                            break;
                        }
                    }
                }

                if !connection.is_blocked() {
                    // Parse read buffer into commands
                    connection_closed |= process_commands(connection, database, aof, event.fd())?;
                }

                // Opportunistic write to save context switch overhead/latency
                connection_closed |= flush(connection, poll, event.fd())?;
            }

            if event.rdhup() {
                connection_closed = true;
            }

            if event.writable() && !connection_closed {
                // Calling flush here can create a minor redundancy in that on WouldBlock
                // flush will reregister the fd with read_write when it is already read_write.
                // However, choosing to accept this in favor of the simplicity of calling flush
                // instead of reproducing the same match block with the one difference of not 
                // calling reregister on WouldBlock.
                connection_closed |= flush(connection, poll, event.fd())?;
            }

            if event.error() || event.hang_up() {
                connection_closed = true;
            }

            if connection_closed {
                entry.remove();
            }
        } else if event.fd() == signals.as_raw_fd() {
            return Ok(false);
        }
    }

    Ok(true)
}

fn flush(connection: &mut Connection, poll: &Poll, fd: i32) -> io::Result<bool> {
    let close_connection = match connection.pump() {
        // Write buffer drained, switch to read_only
        Ok(()) => {
            poll.reregister(fd, Interests::read_only())?;
            false
        }
        // OS send buffer full, bytes still waiting in write buffer
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
            poll.reregister(fd, Interests::read_write())?;
            false
        }
        Err(err) => {
            eprintln!("Write error: {err}");
            true
        }
    };

    Ok(close_connection)
}

fn process_commands(
    connection: &mut Connection,
    database: &mut Database,
    aof: &Aof,
    fd: i32,
) -> io::Result<bool> {
    loop {
        match protocol::parse(connection.read_buf()) {
            ParseResult::Complete(command, to_consume) => {
                connection.drain_read_bytes(to_consume);

                let result = database.execute(command);
                let response_bytes = protocol::serialize(result.response);

                match result.persist {
                    // Push the bytes we want to persist to Aof, then hold on to our
                    // response bytes in the connection until it is no longer blocked
                    // (until the Aof worker signals it is finished).
                    Some(command_to_persist) => {
                        let bytes = command_to_persist.to_resp_bytes();
                        aof.submit(FlushRequest { fd, bytes })?;
                        connection.block_on_flush(response_bytes);
                        break;
                    }

                    // Nothing to persist, so queue response and continue parsing
                    None => connection.queue_bytes(&response_bytes),
                }
            }
            ParseResult::Incomplete => break,
            ParseResult::Error(err) => {
                let bytes = protocol::serialize(Response::Error(ErrorKind::from(err)));
                connection.queue_bytes(&bytes);
                return Ok(true);
            }
        }
    }

    Ok(false)
}