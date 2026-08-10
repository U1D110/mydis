use std::{
    io::{Read, Write}, net::{TcpListener, TcpStream}, process::{Child, Command, ExitStatus}, time::{Duration, Instant}
};

struct ServerProcess {
    child: Child,
    port: u16,
    aof_path: String,
}

impl ServerProcess {
    fn start() -> ServerProcess {
        let port = free_port();
        let aof_path = format!(
            "{}/mydis-test-{}-{port}.aof",
            std::env::temp_dir().display(),
            std::process::id(),
        );
        let _ = std::fs::remove_file(&aof_path);

        // Cargo builds the binary before the test runs and hands you its path.
        let child = Command::new(env!("CARGO_BIN_EXE_server"))
            .env("MYDIS_PORT", port.to_string())
            .env("MYDIS_AOF_PATH", &aof_path)
            .spawn()
            .expect("spawn server");

        let server = ServerProcess { child, port, aof_path };
        server.wait_until_listening();
        server
    }

    fn restart(&mut self) {
        // kill, wait, respawn with same port and aof_path
        // make Drop the only thing that deletes the file
        self.child.kill().expect("Failed to kill child on attempted restart ... you monster.");
        self.child.wait().expect("Wait, what?");
        self.child = Command::new(env!("CARGO_BIN_EXE_server"))
            .env("MYDIS_PORT", self.port.to_string())
            .env("MYDIS_AOF_PATH", &self.aof_path)
            .spawn()
            .expect("Child did not spawn");

        self.wait_until_listening();
    }

    fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(("127.0.0.1", self.port)).expect("connect");
        // A hung server fails the test instead of hanging the entire run.
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        stream
    }

    fn wait_until_listening(&self) {
        for _ in 0..100 {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("server never listened on port {}", self.port);
    }

    fn terminate(&self) {
        let result = unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
        assert_eq!(result, 0, "failed to kill: {}", std::io::Error::last_os_error());
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait().expect("try_wait_") {
                Some(status) => return Some(status),
                None if Instant::now() >= deadline => return None,
                None => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.aof_path);
    }
}

fn free_port() -> u16 {
    // Bind port 0, let the OS pick.
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn expect(stream: &mut TcpStream, want: &[u8]) {
    let mut got = vec![0u8; want.len()];
    stream.read_exact(&mut got).expect("read reply");
    assert_eq!(String::from_utf8_lossy(&got), String::from_utf8_lossy(want));
}


#[test]
fn set_get_del_get() {
    let server = ServerProcess::start();
    let mut client = server.connect();

    client.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
    expect(&mut client, b"+OK\r\n");

    client.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").unwrap();
    expect(&mut client, b"$3\r\nbar\r\n");

    client.write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n").unwrap();
    expect(&mut client, b":1\r\n");

    client.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").unwrap();
    expect(&mut client, b"$-1\r\n");
}

#[test]
fn pipelines_commands_reply_in_order() {
    let server = ServerProcess::start();
    let mut client = server.connect();

    client.write_all(
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n\
          *3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n\
          *2\r\n$3\r\nGET\r\n$1\r\na\r\n"
    ).unwrap();
    
    expect(&mut client, b"+OK\r\n+OK\r\n$1\r\n1\r\n");
}

#[test]
fn command_split_across_writes() {
    let server = ServerProcess::start();
    let mut client = server.connect();

    client.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
    expect(&mut client, b"+OK\r\n");

    client.write_all(b"*2\r\n$3\r\nGET\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.write_all(b"$3\r\nfoo\r\n").unwrap();

    expect(&mut client, b"$3\r\nbar\r\n");
}

#[test]
fn aof_replay_across_restart() {
    let mut server = ServerProcess::start();
    let mut client = server.connect();

    client.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
    expect(&mut client, b"+OK\r\n");

    server.restart();

    let mut client = server.connect();

    client.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").unwrap();
    expect(&mut client, b"$3\r\nbar\r\n");
}

#[test]
fn graceful_shutdown() {
    let mut server = ServerProcess::start();
    server.terminate();

    let status = server
        .wait_for_exit(Duration::from_secs(5))
        .expect("server did not exit within 5 seconds of SIGTERM");

    assert!(status.success(), "Did not exit cleanly: {status:?}");
}

#[test]
fn partial_write_buffering() {
    let server = ServerProcess::start();
    let mut client = server.connect();

    let value = vec![b'x'; 1 << 20];
    let mut buf = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1048576\r\n".to_vec();
    buf.extend_from_slice(&value[..]);
    buf.extend_from_slice(b"\r\n");

    client.write_all(&mut buf).unwrap();
    expect(&mut client, b"+OK\r\n");

    let mut want = b"$1048576\r\n".to_vec();
    want.extend_from_slice(&value);
    want.extend_from_slice(b"\r\n");

    client.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n").unwrap();

    let mut got = vec![0u8; want.len()];
    client.read_exact(&mut got).expect("read reply");
    
    for idx in 0..want.len() {
        assert_eq!(got[idx], want[idx], "byte mismatch at offset {idx}")
    }
}

#[test]
fn expiry() {
    let server = ServerProcess::start();
    let mut client = server.connect();

    client.write_all(b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n100\r\n").unwrap();
    expect(&mut client, b"+OK\r\n");

    client.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n").unwrap();
    expect(&mut client, b"$5\r\nvalue\r\n");

    std::thread::sleep(Duration::from_millis(250));

    client.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n").unwrap();
    expect(&mut client, b"$-1\r\n");

    client.write_all(b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n").unwrap();
    expect(&mut client, b":-2\r\n");
}

#[test]
fn tag_team() {
    let server = ServerProcess::start();
    let mut cecil = server.connect();
    let mut steve = server.connect();

    cecil.write_all(b"*3\r\n$3\r\nSET\r\n$6\r\nWhoomp\r\n$11\r\nThere it is\r\n").unwrap();
    cecil.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n").unwrap();
    cecil.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n").unwrap();
    cecil.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n").unwrap();

    steve.write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nspell\r\n$6\r\nwhoomp\r\n").unwrap();
    steve.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nd\r\n$1\r\n4\r\n").unwrap();
    steve.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\ne\r\n$1\r\n5\r\n").unwrap();
    steve.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nf\r\n$1\r\n6\r\n").unwrap();

    expect(&mut cecil, b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n");
    expect(&mut steve, b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n");

    cecil.write_all(
        b"*2\r\n$3\r\nGET\r\n$6\r\nWhoomp\r\n\
        *2\r\n$3\r\nGET\r\n$1\r\nb\r\n"
    ).unwrap();
    steve.write_all(
        b"*2\r\n$3\r\nGET\r\n$5\r\nspell\r\n\
        *2\r\n$3\r\nGET\r\n$1\r\nd\r\n"
    ).unwrap();

    expect(&mut cecil, b"$11\r\nThere it is\r\n$1\r\n2\r\n");
    expect(&mut steve, b"$6\r\nwhoomp\r\n$1\r\n4");
}

#[test]
fn mid_flush_disconnect() {
    let mut server = ServerProcess::start();
    let mut guru = server.connect();
    let mut premier = server.connect();

    guru.write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nup\r\n$10\r\nin the sky\r\n").unwrap();
    drop(guru);

    premier.write_all(b"*3\r\n$3\r\nSET\r\n$6\r\nfaking\r\n$8\r\nthe funk\r\n").unwrap();
    expect(&mut premier, b"+OK\r\n");

    premier.write_all(b"*2\r\n$3\r\nGET\r\n$6\r\nfaking\r\n").unwrap();
    expect(&mut premier, b"$8\r\nthe funk\r\n");

    premier.write_all(b"*2\r\n$3\r\nGET\r\n$2\r\nup\r\n").unwrap();
    expect(&mut premier, b"$10\r\nin the sky\r\n");

    server.restart();

    let mut guru = server.connect();

    guru.write_all(b"*2\r\n$3\r\nGET\r\n$2\r\nup\r\n").unwrap();
    expect(&mut guru, b"$10\r\nin the sky\r\n");
}