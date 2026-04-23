use std::{
    collections::{
        hash_map::Entry,
        HashMap,
    },
    io,
};

use net::{
    Events,
    Poll,
    TcpListener,
    TcpStream,
};

const EVENT_BUF_SIZE: usize = 1024;
const PORT: &str = "3490";

fn main() -> io::Result<()> {
    let listener = TcpListener::bind(PORT)?;
    let poll = Poll::new()?;

    poll.register(listener.as_raw_fd())?;
    
    let mut connections: HashMap<i32, TcpStream> = HashMap::new();

    println!("Server waiting for connections...");

    let mut events = Events::with_capacity(EVENT_BUF_SIZE);

    loop {
        let _ = poll.wait(&mut events)?;

        for event in events.iter() {
            if event.fd == listener.as_raw_fd() {
                loop {
                    match listener.accept() {
                        Ok(stream) => {
                            println!("Accepted a connection on fd {}", stream.as_raw_fd());

                            poll.register(stream.as_raw_fd())?;
                            connections.insert(stream.as_raw_fd(), stream);
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
                // TODO: handle EPOLLHUP (event.hang_up) and EPOLLERR (event.error) by closing the associated fd (so drop the TcpStream or TcpListener)
                if event.readable {
                    if let Entry::Occupied(mut entry) = connections.entry(event.fd) {
                        let mut connection_closed = false;

                        let stream = entry.get_mut();

                        loop {
                            match stream.read() {
                                Ok(bs) => {
                                    // Echo data back
                                    // if bs is 0 the client disconnected, so remove it from connections
                                    if bs.len() == 0 {
                                        connection_closed = true;
                                        break;
                                    } else {
                                        if let Err(err) = stream.write(&bs) {
                                            eprintln!("Write error from stream {}: {}", event.fd, err);
                                        }
                                    }
                                },
                                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                    // kernel buffer drained, so done reading for now
                                    break;
                                },
                                Err(err) => {
                                    eprintln!("Error reading from stream {}: {}", event.fd, err);
                                    break;
                                }
                            }
                        }

                        if connection_closed {
                            entry.remove();
                        }
                    }
                }
            }   
        }
    }
}
