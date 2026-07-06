use net::TcpStream;
use std::{io, os::fd::{AsFd, BorrowedFd}};

const READ_BUF_SIZE: usize = 4096;
const WRITE_BUF_SIZE: usize = 4096;

#[derive(PartialEq)]
pub enum ConnectionStatus {
    Active,
    Closed,
}

pub struct Connection {
    stream: TcpStream,
    read_buf: Vec<u8>, // Maybe we use BytesMut from `bytes` crate later on
    write_buf: Vec<u8>,
    pending_flush: Option<Vec<u8>>,
    generation: u32,
}

impl Connection {
    pub fn new(stream: TcpStream, generation: u32) -> Self {
        Connection {
            stream,
            read_buf: Vec::with_capacity(READ_BUF_SIZE),
            write_buf: Vec::with_capacity(WRITE_BUF_SIZE),
            pending_flush: None,
            generation,
        }
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn is_blocked(&self) -> bool {
        self.pending_flush.is_some()
    }

    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.is_empty()
    }

    pub fn read_buf(&self) -> &[u8] {
        &self.read_buf
    }

    pub fn block_on_flush(&mut self, bytes: Vec<u8>) {
        self.pending_flush = Some(bytes);
    }

    pub fn resume_after_flush(&mut self) {
        if let Some(bytes) = self.pending_flush.take() {
            self.queue_bytes(&bytes);
        }
    }

    pub fn discard_pending_flush(&mut self) {
        self.pending_flush = None
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
                    // TODO: Maximum buffer size check
                    self.read_buf.extend_from_slice(&buf[..bytes_read]);
                    //println!(
                    //    "read_buf after read: {:?}",
                    //    String::from_utf8_lossy(&self.read_buf)
                    //);
                    Ok(ConnectionStatus::Active)
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