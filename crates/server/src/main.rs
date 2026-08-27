mod aof;
mod connection;

use crate::{aof::Aof, connection::handle_connection};

use std::{cell::RefCell, io::self, os::fd::{AsFd, AsRawFd}, rc::Rc, time::Duration};

use db::Database;
use net::{Signals, TcpListener};
use runtime::{Reactor, Runner, Spawner};

async fn accept_loop(
    listener: TcpListener,
    reactor: Rc<Reactor>,
    spawner: Spawner,
    database: Rc<RefCell<Database>>,
    aof: Rc<Aof>,
) {
    let registered = reactor
        .register(listener.as_fd())
        .expect("failed to register listener");

    loop {
        registered.readable().await;

        loop {
            match listener.accept() {
                Ok(stream) => {
                    println!("Accepted a connection on fd {}", stream.as_raw_fd());
                    spawner.spawn(
                        handle_connection(
                            stream,
                            Rc::clone(&reactor),
                            Rc::clone(&database),
                            Rc::clone(&aof),
                        )
                    );
                }
                
                // kernel accept queue is empty, time to wait again
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,

                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    break;
                }
            }
        }
    }
}

async fn signal_task(signals: Signals, reactor: Rc<Reactor>, spawner: Spawner) {
    let registered = reactor.register(signals.as_fd()).expect("failed to register signal handler");
    registered.readable().await;
    let _ = signals.drain();
    spawner.shutdown();
}

async fn aof_completions(aof: Rc<Aof>, reactor: Rc<Reactor>) {
    let registered = reactor
        .register(aof.notify_fd())
        .expect("failed to register aof");

    loop {
        registered.readable().await;
        aof.drain_and_complete().expect("failed to drain completions");
    }
}

async fn expiry(database: Rc<RefCell<Database>>, reactor: Rc<Reactor>) {
    loop {
        let timeout_ms = database.borrow().next_expiration_timeout();

        let snooze = if timeout_ms < 0 {
            Duration::from_millis(100)
        } else {
            Duration::from_millis(timeout_ms.max(1) as u64)
        };

        reactor.sleep(snooze).await;
        database.borrow_mut().purge_expired_keys();
    }
}

fn main() -> io::Result<()> {
    let signals = Signals::new()?;
    let database = Rc::new(RefCell::new(Database::new()));

    // Replay append-only file, if one exists, to repopulate the database. Then open
    // or create the append-only file so we can write to it as we run.
    let aof_path = std::env::var("MYDIS_AOF_PATH")
        .unwrap_or_else(|_| "appendonly.aof".to_string());

    let valid_length = aof::replay(&aof_path, Rc::clone(&database))?;
    let aof = Rc::new(Aof::open(&aof_path, valid_length)?);

    // Get our port and create a listener.
    let port = std::env::var("MYDIS_PORT")
        .unwrap_or_else(|_| "3490".to_string());

    let listener = TcpListener::bind(&port)?;

    let runner = Runner::new()?;
    let reactor = runner.reactor();
    let spawner = runner.spawner();

    runner.spawn(
        accept_loop(
            listener,
            Rc::clone(&reactor),
            spawner.clone(),
            Rc::clone(&database),
            Rc::clone(&aof)
        )
    );

    runner.spawn(signal_task(signals, Rc::clone(&reactor), spawner.clone()));
    runner.spawn(aof_completions(Rc::clone(&aof), Rc::clone(&reactor)));
    runner.spawn(expiry(Rc::clone(&database), Rc::clone(&reactor)));

    runner.run()?;
    aof.shutdown();
    Ok(())
}
