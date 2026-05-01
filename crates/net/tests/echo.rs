use net::{Events, Interests, TcpListener, TcpStream};
use std::io;

#[test]
fn tcp_listener_accepts_and_echoes() -> io::Result<()> {
    let listener = TcpListener::bind("0")?;
    let port = listener.local_port()?;

    let poll = net::Poll::new()?;
    poll.register(listener.as_raw_fd(), Interests::read_only())?;

    let handle = std::thread::spawn(move || {
        let client = TcpStream::connect(
            "127.0.0.1", 
            port.to_string().as_str()
        ).expect("Failed to connect to server");
        client.write(b"Hello, Manuel! This is Jeff.").expect("Jeff failed to write to server");
        let mut buf = [0u8; 28];
        client.read(&mut buf).expect("Jeff failed to read from server");
        assert_eq!(&buf, b"Hello, Manuel! This is Jeff.");
        drop(client);

        let client = TcpStream::connect(
            "127.0.0.1", 
            port.to_string().as_str()
        ).expect("Failed to connect to server");
        client.write(b"Hello, Manuel! This is Bort.").expect("Bort failed to write to server");
        let mut buf = [0u8; 28];
        client.read(&mut buf).expect("Bort failed to read from server");
        assert_eq!(&buf, b"Hello, Manuel! This is Bort.");
        //drop(client); // Not really necessary
    });

    let mut events = Events::with_capacity(4);
    
    for i in 0..2 {
        // Wait for first client to connect
        poll.wait(&mut events)?;
        let stream = listener.accept()?;
        assert_eq!(events.iter().count(), 1, "Expected exactly one event on accept {i}");
        poll.register(stream.as_raw_fd(), Interests::read_only())?;

        // Wait for client to write
        poll.wait(&mut events)?;
        assert_eq!(events.iter().count(), 1, "Expected exactly one readable event {i}");
        let mut buf = [0u8; 28];
        let n = stream.read(&mut buf)?;
        assert_eq!(n, 28, "Expected to read exactly 28 bytes");
        stream.write(&buf)?;

        // First client drops
        poll.wait(&mut events)?;
        assert!(events.iter().any(|e| e.rdhup()), "Expected EPOLLRDHUP from client {i} disconnect");
    }

    handle.join().expect("thread panicked");

    Ok(())
}
