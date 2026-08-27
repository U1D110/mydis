use crate::aof::Aof;

use db::Database;
use protocol::{ErrorKind, ParseResult, Response};
use net::TcpStream;
use runtime::Reactor;
use std::{cell::RefCell, io, os::fd::{AsFd, BorrowedFd}, rc::Rc};

const READ_BUF_SIZE: usize = 4096;
const WRITE_BUF_SIZE: usize = 4096;
const MAX_READ_BUF: usize = 64*1024*1024;
const PERSIST_BATCH_LIMIT: usize = 1024 * 1024;

pub async fn handle_connection(
    stream: TcpStream,
    reactor: Rc<Reactor>,
    database: Rc<RefCell<Database>>,
    aof: Rc<Aof>
) {
    let mut connection = Connection {
        stream,
        read_buf: Vec::with_capacity(READ_BUF_SIZE),
        write_buf: Vec::with_capacity(WRITE_BUF_SIZE),
    };

    let registered = reactor.register(connection.as_fd()).expect("failed to register connection");

    let mut processing_state = ProcessingState::Idle;

    loop {
        // if there aren't commands left to process from the previous iteration
        if processing_state != ProcessingState::InProgress {
            registered.readable().await;
        }

        let connection_closed = loop {
            match connection.read() {
                Ok(ConnectionStatus::Closed) => break true,
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break false,
                Err(_) => return,
            }
        };

        processing_state = match process_commands(&mut connection, &database, &aof).await {
            Ok(state) => state,
            Err(_) => return,
        };

        if processing_state == ProcessingState::Close { return; }

        while !connection.write_buf.is_empty() {
            match connection.pump() {
                Ok(()) => break,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => registered.writable().await,
                Err(_) => return,
            }
        }

        if connection_closed && processing_state != ProcessingState::InProgress {
            return;
        }
    }
}


#[derive(PartialEq)]
enum ProcessingState {
    Idle,
    InProgress,
    Close,
}

async fn process_commands(
    connection: &mut Connection,
    database: &Rc<RefCell<Database>>,
    aof: &Rc<Aof>,
) -> io::Result<ProcessingState> {
    let mut aof_buf: Vec<u8> = Vec::new();
    let mut response_buf: Vec<u8> = Vec::new();
    let mut state = ProcessingState::Idle;

    loop {
        match protocol::parse(&connection.read_buf) {
            ParseResult::Complete(command, to_consume) => {
                connection.read_buf.drain(..to_consume);

                let result = database.borrow_mut().execute(command);
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
                    state = ProcessingState::InProgress;
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
                // The completion for this flush arrives after the connection has been
                // removed, and is discarded because `Flush::drop` removes the entry and 
                // `Aof::complete` finds nothing to wake.
                if !aof_buf.is_empty() {
                    let _ = aof.flush(aof_buf);
                }
                let bytes = protocol::serialize(Response::Error(ErrorKind::from(err)));
                connection.queue_bytes(&bytes);
                return Ok(ProcessingState::Close);
            }
        }
    }

    if !aof_buf.is_empty() {
        aof.flush(aof_buf)?.await?;
        connection.queue_bytes(&response_buf);
    }

    Ok(state)
}

#[derive(PartialEq)]
enum ConnectionStatus {
    Active,
    Closed,
}

struct Connection {
    stream: TcpStream,
    read_buf: Vec<u8>, // Maybe we use BytesMut from `bytes` crate later on
    write_buf: Vec<u8>,
}

impl Connection {
    fn read(&mut self) -> io::Result<ConnectionStatus> {
        let mut buf = [0u8; READ_BUF_SIZE];
        let bytes_read = self.stream.read(&mut buf)?;

        // client disconnected
        if bytes_read == 0 { 
            return Ok(ConnectionStatus::Closed); 
        }
        
        self.read_buf.extend_from_slice(&buf[..bytes_read]);

        if self.read_buf.len() >= MAX_READ_BUF {
            return Err(io::Error::other("exceeded query buffer limit"));
        }

        Ok(ConnectionStatus::Active)
    }

    fn queue_bytes(&mut self, bytes: &[u8]) {
        self.write_buf.extend_from_slice(bytes);
    }

    fn pump(&mut self) -> io::Result<()> {
        if self.write_buf.is_empty() {
            return Ok(());
        }

        let bytes_sent = self.stream.write(&self.write_buf)?;

        self.write_buf.drain(..bytes_sent);

        if !self.write_buf.is_empty() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        Ok(())
    }
}

impl AsFd for Connection {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.stream.as_fd()
    }
}