use std::{
    collections::{HashMap, hash_map::Entry}, io, os::fd::{AsFd, AsRawFd, RawFd},
};

use db::Database;
use net::{Events, Interests, Poll, Signals, TcpListener};
use protocol::{ErrorKind, ParseResult, Response};

use crate::{aof::{Aof, FlushRequest}, connection::{Connection, ConnectionStatus}};

pub fn handle_events(
    events: &Events,
    connections: &mut HashMap<RawFd, Connection>,
    next_gen: &mut u32,
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

                        poll.register(stream.as_fd(), Interests::read_only())?;

                        let generation = *next_gen;
                        *next_gen += 1;

                        let conn = Connection::new(stream, generation);

                        connections.insert(conn.as_fd().as_raw_fd(), conn);
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
        } else if event.fd() == aof.notify_fd().as_raw_fd() {
            for completion in aof.drain_completions()? {
                let (fd, result, generation) = completion.into_parts();

                let close = if let Some(connection) = connections.get_mut(&fd) {
                    if connection.generation() != generation {
                        // Even though we have the same `fd` between `Connection` and `Completion`
                        // their generation does not match, and so this is a re-used file descriptor.
                        continue;
                    }

                    connection.complete_flush(result);

                    match connection.poll_flush() {
                        runtime::Poll::Ready(Ok(())) => {
                            let process_says_close = process_commands(connection, database, aof)?;
                            let flush_says_close = flush(connection, poll)?;
                            process_says_close || flush_says_close
                        }
                        runtime::Poll::Ready(Err(_)) => true,
                        runtime::Poll::Pending => false,
                    }
                } else {
                    // client already disconnected, write is still durable so nothing to resume.
                    false
                };

                if close {
                    connections.remove(&fd);
                }
            }
        } else if let Entry::Occupied(mut entry) = connections.entry(event.fd()) {
            let connection = entry.get_mut();
            let mut connection_closed = false;

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
                                poll.reregister(connection.as_fd(), Interests::read_write())?;
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
                    connection_closed |= process_commands(connection, database, aof)?;
                }

                // Opportunistic write to save context switch overhead/latency
                connection_closed |= flush(connection, poll)?;
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
                connection_closed |= flush(connection, poll)?;
            }

            if event.error() || event.hang_up() {
                connection_closed = true;
            }

            if connection_closed {
                entry.remove();
            }
        } else if event.fd() == signals.as_raw_fd() {
            signals.drain()?;
            return Ok(false);
        }
    }

    Ok(true)
}

fn flush(connection: &mut Connection, poll: &Poll) -> io::Result<bool> {
    let close_connection = match connection.pump() {
        // Write buffer drained, switch to read_only
        Ok(()) => {
            poll.reregister(connection.as_fd(), Interests::read_only())?;
            false
        }
        // OS send buffer full, bytes still waiting in write buffer
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
            poll.reregister(connection.as_fd(), Interests::read_write())?;
            false
        }
        Err(err) => {
            eprintln!("Write error: {err}");
            true
        }
    };

    Ok(close_connection)
}

const PERSIST_BATCH_LIMIT: usize = 1024 * 1024;

fn process_commands(
    connection: &mut Connection,
    database: &mut Database,
    aof: &Aof,
) -> io::Result<bool> {
    let mut aof_buf: Vec<u8> = Vec::new();
    let mut response_buf: Vec<u8> = Vec::new();

    loop {
        match protocol::parse(connection.read_buf()) {
            ParseResult::Complete(command, to_consume) => {
                connection.drain_read_bytes(to_consume);

                let result = database.execute(command);
                let mut response_bytes = protocol::serialize(result.response);

                if let Some(command_to_persist) = result.persist {
                    aof_buf.append(&mut command_to_persist.to_resp_bytes());
                }

                if aof_buf.is_empty() {
                    connection.queue_bytes(&response_bytes)
                } else {
                    response_buf.append(&mut response_bytes);
                }

                if aof_buf.len() >= PERSIST_BATCH_LIMIT {
                    break;
                }
            }
            ParseResult::Incomplete => break,
            ParseResult::Error(err) => {
                // The stream is corrupt, so this connection is closing. Commands already
                // parsed in this batch have run and mutated the database, so their AOF
                // bytes must still be persisted -- dropping them would leave the log
                // disagreeing with the keyspace across a restart.
                //
                // We deliberately do not `begin_flush` here, because there is nothing to
                // resume into. That means `response_buf` is dropped: a client that sent
                // `SET a`, `SET b`, <garbage> receives only the error and never the two
                // `+OK`s, even though both writes are durable. Acceptable -- a client that
                // corrupted its own framing cannot trust the reply stream anyway, and it
                // matches Redis, which answers a protocol error with an error and a close.
                //
                // The completion for this submit arrives after the connection has been
                // removed, and is discarded by the `else` branch in `handle_events`.
                if !aof_buf.is_empty() {
                    aof.submit(
                        FlushRequest::new(
                            connection.as_fd().as_raw_fd(), 
                            aof_buf, 
                            connection.generation()
                        )
                    )?;
                }
                let bytes = protocol::serialize(Response::Error(ErrorKind::from(err)));
                connection.queue_bytes(&bytes);
                return Ok(true);
            }
        }
    }

    if !aof_buf.is_empty() {
        aof.submit(
            FlushRequest::new(
                    connection.as_fd().as_raw_fd(),
                    aof_buf,
                    connection.generation()
                )
            )?;
        connection.begin_flush(response_buf);
    }

    Ok(false)
}