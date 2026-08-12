use net::TcpStream;
use runtime::Poll;
use std::{io, os::fd::{AsFd, BorrowedFd}};

const READ_BUF_SIZE: usize = 4096;
const WRITE_BUF_SIZE: usize = 4096;
const MAX_READ_BUF: usize = 64*1024*1024;

enum FlushState {
    Pending { response: Vec<u8> },
    Ready { result: io::Result<()>, response: Vec<u8> },
}

impl FlushState {
    pub fn poll(&mut self) -> Poll<io::Result<()>> {
        match self {
            FlushState::Pending { .. } => Poll::Pending,
            FlushState::Ready { result, .. } => {
                Poll::Ready(std::mem::replace(result, Ok(())))
            }
        }
    }

    pub fn complete(&mut self, result: io::Result<()>) {
        if let FlushState::Pending { response } = self {
            let response = std::mem::take(response);
            *self = FlushState::Ready { result, response };
        }
    }
}

#[derive(PartialEq)]
pub enum ConnectionStatus {
    Active,
    Closed,
}

pub struct Connection {
    stream: TcpStream,
    read_buf: Vec<u8>, // Maybe we use BytesMut from `bytes` crate later on
    write_buf: Vec<u8>,
    flush: Option<FlushState>,
    generation: u32,
}

impl Connection {
    pub fn new(stream: TcpStream, generation: u32) -> Self {
        Connection {
            stream,
            read_buf: Vec::with_capacity(READ_BUF_SIZE),
            write_buf: Vec::with_capacity(WRITE_BUF_SIZE),
            flush: None,
            generation,
        }
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn is_blocked(&self) -> bool {
        self.flush.is_some()
    }

    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.is_empty()
    }

    pub fn read_buf(&self) -> &[u8] {
        &self.read_buf
    }

    pub fn poll_flush(&mut self) -> Poll<io::Result<()>> {
        match self.flush.as_mut() {
            Some(state) => match state.poll() {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    if let Some(FlushState::Ready { response, ..}) = self.flush.take() {
                        if result.is_ok() {
                            // resume: send the reply we've been holding on to.
                            self.queue_bytes(&response); 
                        }
                        // Err: response dropped after we exit current scope because we just took ownership
                        //      of the FlushState in flush. (never ack a lost write)
                    }
                    Poll::Ready(result)
                }
            },
            None => Poll::Ready(Ok(())),
        }
    }

    pub fn begin_flush(&mut self, response: Vec<u8>) {
        self.flush = Some(FlushState::Pending { response });
    }

    pub fn complete_flush(&mut self, result: io::Result<()>) {
        if let Some(state) = self.flush.as_mut() {
            state.complete(result);
        }
    }

    pub fn drain_read_bytes(&mut self, num_bytes: usize) {
        self.read_buf.drain(..num_bytes);
    }

    pub fn read(&mut self) -> io::Result<ConnectionStatus> {
        let mut buf = [0u8; READ_BUF_SIZE];
        match self.stream.read(&mut buf) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    // client disconnected
                    Ok(ConnectionStatus::Closed)
                } else {
                    self.read_buf.extend_from_slice(&buf[..bytes_read]);

                    if self.read_buf().len() >= MAX_READ_BUF {
                        Err(io::Error::other("exceeded query buffer limit"))
                    } else {
                        Ok(ConnectionStatus::Active)
                    }
                }
            }
            Err(err) => Err(err),
        }
    }

    pub fn queue_bytes(&mut self, bytes: &[u8]) {
        self.write_buf.extend_from_slice(bytes);
    }

    fn write(&self) -> io::Result<usize> {
        self.stream.write(&self.write_buf)
    }

    pub fn pump(&mut self) -> io::Result<()> {
        if self.write_buf.is_empty() {
            return Ok(());
        }

        match self.write() {
            Ok(bytes_sent) => {
                self.write_buf.drain(..bytes_sent);
                if self.write_buf.is_empty() {
                    Ok(())
                } else {
                    // Partial write - treat like WouldBlock
                    Err(io::Error::from(io::ErrorKind::WouldBlock))
                }
            }
            Err(err) => Err(err),
        }
    }
}

impl AsFd for Connection {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.stream.as_fd()
    }
}