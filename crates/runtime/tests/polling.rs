use net::{TcpListener, TcpStream, Wakeup};
use runtime::{Runner, block_on, yield_now};
use std::{
    cell::{Cell, RefCell}, io, os::fd::AsFd, pin::Pin, rc::Rc, task::{Context, Poll, Waker}, thread, time::{Duration, Instant},
};

#[test]
fn with_simple_closure() {
    let result = block_on(async { 40 + 2 });
    assert_eq!(result, 42);
}

#[test]
fn with_trivial_future() {
    struct UnitFuture;
    
    impl Future for UnitFuture {
        type Output = ();
    
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(())
        }
    }

    let result = block_on(UnitFuture);
    assert_eq!(result, ());
}

struct Deadline {
    when: Instant,
    poll_count: u32,
    waker: Option<Waker>,
}

impl Future for Deadline {
    type Output = u32;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Self::Output> {
        let this = self.get_mut();

        this.poll_count += 1;

        if Instant::now() >= this.when {
            this.waker = None;
            Poll::Ready(this.poll_count)
        } else {
            if this.waker.is_none() {
                let waker = cx.waker().clone();
                this.waker = Some(waker.clone());
                let when = this.when;

                thread::spawn(move || {
                    let now = Instant::now();
                    if now < when {
                        thread::sleep(when - now);
                    }
                    waker.wake();
                });
            }

            Poll::Pending
        }
    }
}

#[test]
fn parks_until_woken() {
    let count = block_on(Deadline {
        when: Instant::now() + Duration::from_millis(50),
        poll_count: 0,
        waker: None,
    });

    println!("Polled {count} times");
    assert!(count < 10);
}

#[test]
fn context_forwarded() {
    let deadline = Deadline {
        when: Instant::now() + Duration::from_millis(50),
        poll_count: 0,
        waker: None,
    };

    let count = block_on(async {
        deadline.await
    });

    println!("Polled {count} times");
    assert!(count < 10);
}

#[test]
fn tasks_interleave() -> io::Result<()> {
    let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let log_for_a = Rc::clone(&log);
    let log_for_b = Rc::clone(&log);

    let runner = Runner::new()?;

    runner.spawn(async move {
        for _ in 0..3 {
            log_for_a.borrow_mut().push("a");
            yield_now().await;
        }
    });

    runner.spawn(async move {
        for _ in 0..3 {
            log_for_b.borrow_mut().push("b");
            yield_now().await;
        }
    });

    runner.run()?;

    assert_eq!(*log.borrow(), ["a", "b", "a", "b", "a", "b"]);

    Ok(())
}

struct Flipper {
    value: Rc<Cell<bool>>,
}

impl Drop for Flipper {
    fn drop(&mut self) {
        let old = self.value.get();
        self.value.set(!old);
    }
}

struct KeepsWaker {
    _flipper: Flipper,
    waker: Rc<RefCell<Option<Waker>>>,
}

impl Future for KeepsWaker {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if this.waker.borrow().is_some() {
            return Poll::Ready(());
        }
                
        *this.waker.borrow_mut() = Some(cx.waker().clone());
        cx.waker().wake_by_ref();

        Poll::Pending
    }
}


#[test]
fn task_future_dropped() -> io::Result<()> {
    let switch = Rc::new(Cell::new(false));
    let flipped_switch = Rc::clone(&switch);

    let waker = Rc::new(RefCell::new(None));

    let runner = Runner::new()?;

    let keeps_waker_fut = KeepsWaker {
        _flipper: Flipper { value: flipped_switch },
        waker: Rc::clone(&waker),
    };

    runner.spawn(keeps_waker_fut);

    runner.run()?;

    assert!(waker.borrow().is_some());
    assert!(switch.get());

    Ok(())
}

#[test]
fn task_awaits_readable() -> io::Result<()> {
    let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let log_for_a = Rc::clone(&log);
    let log_for_b = Rc::clone(&log);

    let runner = Runner::new()?;
    let reactor = runner.reactor();

    let wakeup = Wakeup::new()?;
    let notifier = wakeup.notifier();
    let registered = reactor.register(wakeup.as_fd())?;

    runner.spawn(async move {
        registered.readable().await;
        log_for_a.borrow_mut().push("readable");
    });

    runner.spawn(async move {
        yield_now().await;
        notifier.notify().unwrap();
        log_for_b.borrow_mut().push("notify");
    });

    runner.run()?;
    assert_eq!(*log.borrow(), ["notify", "readable"]);

    Ok(())
}

#[test]
fn task_with_no_waker_not_lost() -> io::Result<()> {
    let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let log_for_a = Rc::clone(&log);

    let runner = Runner::new()?;
    let reactor = runner.reactor();

    let wakeup = Wakeup::new()?;
    let registered = reactor.register(wakeup.as_fd())?;
    let notifier = wakeup.notifier();
    notifier.notify().unwrap();

    runner.spawn(async move {
        yield_now().await;
        registered.readable().await;
        log_for_a.borrow_mut().push("readable");
    });

    runner.run()?;
    assert_eq!(*log.borrow(), ["readable"]);

    Ok(())
}

#[test]
fn sleepers_wake_and_overlap() -> io::Result<()> {
    let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let log_for_a = Rc::clone(&log);
    let log_for_b = Rc::clone(&log);

    let runner = Runner::new()?;
    let reactor_a = runner.reactor();
    let reactor_b = runner.reactor();

    runner.spawn(async move {
        reactor_a.sleep(Duration::from_millis(80)).await;
        log_for_a.borrow_mut().push("long");
    });

    runner.spawn(async move {
        reactor_b.sleep(Duration::from_millis(20)).await;
        log_for_b.borrow_mut().push("short");
    });

    let now = Instant::now();
    runner.run()?;
    let elapsed = now.elapsed().as_millis();

    assert!(elapsed >= 80 && elapsed < 100);
    assert_eq!(*log.borrow(), ["short", "long"]);

    Ok(())
}

#[test]
fn handle_readable_and_writable() -> io::Result<()> {
    let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let log_for_rable = Rc::clone(&log);
    let log_for_wable = Rc::clone(&log);

    let listener = TcpListener::bind("0")?;
    let port = listener.local_port()?;

    let client = TcpStream::connect("127.0.0.1", &port.to_string())?;
    let server = listener.accept()?;
    client.write(b"Greetings and salutations.")?;

    let runner = Runner::new()?;
    let registered = Rc::new(runner.reactor().register(server.as_fd())?);
    let reg_r = Rc::clone(&registered);
    let reg_w = Rc::clone(&registered);

    runner.spawn(async move {
        reg_r.readable().await;
        log_for_rable.borrow_mut().push("read");
    });

    runner.spawn(async move {
        reg_w.writable().await;
        log_for_wable.borrow_mut().push("write");
    });

    runner.run()?;

    assert_eq!(*log.borrow(), ["read", "write"]);

    Ok(())
}

#[test]
fn spawner_shuts_down_runner() -> io::Result<()> {
    let runner = Runner::new()?;
    let wakeup = Wakeup::new()?;
    let notifier = wakeup.notifier();
    let reg = runner.reactor().register(wakeup.as_fd())?;
    let wakes = Rc::new(Cell::new(0));

    let ws = Rc::clone(&wakes);
    runner.spawn(async move {
        loop {
            reg.readable().await;
            ws.update(|n| n + 1);
        }
    });

    let spawner = runner.spawner();
    runner.spawn(async move {
        notifier.notify().unwrap();
        yield_now().await;
        spawner.shutdown();
    });

    runner.run()?;

    assert_eq!(wakes.get(), 1);

    Ok(())
}

#[test]
fn spawner_spawns() -> io::Result<()> {
    let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let log_a = Rc::clone(&log);
    let log_b = Rc::clone(&log);

    let runner = Runner::new()?;
    let spawner = runner.spawner();
    runner.spawn(async move {
        log_a.borrow_mut().push("parent");
        spawner.spawn(async move {
            log_b.borrow_mut().push("child");
        });
    });

    runner.run()?;

    assert_eq!(*log.borrow(), ["parent", "child"]);
    
    Ok(())
}