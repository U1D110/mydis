use net::TcpStream;
use std::io;

const WRITE_BUF_SIZE: usize = 4096;

#[derive(PartialEq)]
pub enum ConnectionStatus {
    Active,
    Closed,
}

pub struct Connection {
    stream: TcpStream,
    // TODO: Probably want a separate read_buf
    write_buf: Vec<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection { 
            stream,
            write_buf: Vec::with_capacity(WRITE_BUF_SIZE),
        }
    }

    pub fn id(&self) -> i32 {
        self.stream.as_raw_fd()
    }

    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.is_empty()
    }

    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<ConnectionStatus> {
        match self.stream.read(buf) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    // client disconnected
                    Ok(ConnectionStatus::Closed)
                } else {
                    // This is really only a write buffer because we are just echoing
                    self.write_buf.extend_from_slice("Received: ".as_bytes());
                    self.write_buf.extend_from_slice(&buf[..bytes_read]);
                    Ok(ConnectionStatus::Active)
                }
            },
            Err(err) => Err(err),
        }
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
            },
            Err(err) => Err(err)
        }
    }
}