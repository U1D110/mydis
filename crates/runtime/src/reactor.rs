use std::{
    cell::{Cell, RefCell}, collections::{BTreeMap, HashMap}, io, os::fd::{AsRawFd, BorrowedFd, RawFd}, pin::Pin, rc::Rc, task::{Context, Poll, Waker}, time::{Duration, Instant},
};
use net::{Events, Interests, Poll as Epoll};

const EVENT_BUF_SIZE: usize = 1024;

pub struct Sleep {
    when: Instant,
    id: Option<u64>,
    reactor: Rc<Reactor>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if Instant::now() >= this.when {
            return Poll::Ready(());
        } 
        
        if this.id.is_none() {
            let id = this.reactor.insert_timer(this.when, cx.waker().clone());
            this.id = Some(id);
        }

        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.reactor.remove_timer(self.when, id);
        }
    }
}

pub struct Readable {
    fd: RawFd,
    reactor: Rc<Reactor>,
}

impl Future for Readable {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.reactor.poll_readable(self.fd, cx)
    }
}

pub struct Writable {
    fd: RawFd,
    reactor: Rc<Reactor>,
}

impl Future for Writable {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.reactor.poll_writable(self.fd, cx)
    }
}

pub struct Registered {
    fd: RawFd,
    reactor: Rc<Reactor>,
}

impl Registered {
    pub fn readable(&self) -> Readable {
        Readable { fd: self.fd, reactor: Rc::clone(&self.reactor) }
    }

    pub fn writable(&self) -> Writable {
        Writable { fd: self.fd, reactor: Rc::clone(&self.reactor) }
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        // Any failure means the registration is already gone, which is what
        // we are trying to do here.
        let _ = self.reactor.deregister(self.fd);
    }
}

#[derive(Default)]
struct Direction {
    waker: Option<Waker>,
    ready: bool,
}

impl Direction {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.ready {
            self.ready = false;
            Poll::Ready(())
        } else {
            self.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn signal(&mut self) -> Option<Waker> {
        self.ready = true;
        self.waker.take()
    }
}

/// Consider that `Registration` has a single `Waker` slot, which means if
/// two `Tasks`s were to be waiting for readability on the same fd the second
/// of those would overwrite the first, losing that first `Task`. 
/// This project is one `Task` per connection, so this is fine.
#[derive(Default)]
struct Registration {
    read: Direction,
    write: Direction,
}

pub struct Reactor {
    epoll: Epoll,
    events: RefCell<Events>,
    registry: RefCell<HashMap<RawFd, Registration>>,
    timers: RefCell<BTreeMap<(Instant, u64), Waker>>,
    next_timer_id: Cell<u64>,
}

impl Reactor {
    pub fn new() -> io::Result<Self> {
        let epoll = Epoll::new()?;
        Ok(Self { 
            epoll,
            events: RefCell::new(Events::with_capacity(EVENT_BUF_SIZE)),
            registry: RefCell::new(HashMap::new()),
            timers: RefCell::new(BTreeMap::new()),
            next_timer_id: Cell::new(0),
        })
    }

    pub fn sleep_until(self: &Rc<Self>, when: Instant) -> Sleep {
        Sleep { when, id: None, reactor: Rc::clone(self) }
    }

    pub fn sleep(self: &Rc<Self>, duration: Duration) -> Sleep {
        self.sleep_until(Instant::now() + duration)
    }

    /// The `fd` will always be registered with both read and write interests. This means
    /// in cases where you are only concerned with reading, there will still be an initial
    /// spurious writable event though it will be harmless. Since `net::Poll` uses 
    /// egde-triggered mode, there will be no further writable events unless the fd actually
    /// becomes writable.
    pub fn register(self: &Rc<Self>, fd: BorrowedFd<'_>) -> io::Result<Registered> {
        self.epoll.register(fd, Interests::read_write())?;
        self.registry.borrow_mut().insert(fd.as_raw_fd(), Registration::default());
        Ok(Registered { fd: fd.as_raw_fd(), reactor: Rc::clone(self) })
    }

    pub fn deregister(&self, fd: RawFd) -> io::Result<()> {
        self.registry.borrow_mut().remove(&fd);
        self.epoll.deregister(fd)?;
        Ok(())
    }

    pub fn poll_readable(&self, fd: RawFd, cx: &mut Context<'_>) -> Poll<()> {
        match self.registry.borrow_mut().get_mut(&fd) {
            Some(reg) => reg.read.poll(cx),
            None => panic!("No key in registry matching fd: {fd}"),
        }
    }

    pub fn poll_writable(&self, fd: RawFd, cx: &mut Context<'_>) -> Poll<()> {
        match self.registry.borrow_mut().get_mut(&fd) {
            Some(reg) => reg.write.poll(cx),
            None => panic!("No key in registry matching fd: {fd}"),
        }
    }

    pub fn turn(&self, max_timeout_ms: i32) -> io::Result<()> {
        debug_assert!(
            max_timeout_ms >= 0 || !self.registry.borrow().is_empty() || !self.timers.borrow().is_empty(),
            "turn() would block forever because nothing is registered, there are no timers, and there is no timeout"
        );

        let timeout = match self.timers.borrow().keys().next() {
            Some((when, _)) => {
                let ms = when
                                .saturating_duration_since(Instant::now())
                                .as_millis()
                                .min(i32::MAX as u128) as i32;
                if max_timeout_ms < 0 { ms } else { max_timeout_ms.min(ms) }
            }
            None => max_timeout_ms,
        };

        let mut events = self.events.borrow_mut();
        self.epoll.wait(&mut events, timeout)?;

        for event in events.iter() {
            let (
                    read_waker, 
                    write_waker
                ) = {
                match self.registry.borrow_mut().get_mut(&event.fd()) {
                    Some(reg) => {
                        let fatal = event.error() || event.hang_up();
                        let read = event.readable() || event.rdhup();
                        let write = event.writable();
                    
                        (
                            if read  || fatal { reg.read.signal()  } else { None },
                            if write || fatal { reg.write.signal() } else { None }
                        )
                    }
                    None => (None, None),
                }
            };

            if let Some(waker) = read_waker  { waker.wake(); }
            if let Some(waker) = write_waker { waker.wake(); }
        }

        // pop entries off the front of timers while their "when" <= Instant::now()
        let mut expired: Vec<Waker> = Vec::new();
        {
            // Keep our timers borrow scoped
            let now = Instant::now();
            let mut timers = self.timers.borrow_mut();
            while let Some((&(when, _), _)) = timers.iter().next() {
                if when > now { break; }
                if let Some((_, waker)) = timers.pop_first() {
                    expired.push(waker);
                }
            }
        }
        
        for waker in expired {
            waker.wake();
        }
        
        Ok(())
    }

    fn insert_timer(&self, when: Instant, waker: Waker) -> u64 {
        let id = self.next_timer_id.get();
        self.next_timer_id.set(id + 1);
        self.timers.borrow_mut().insert((when, id), waker);
        id
    }

    fn remove_timer(&self, when: Instant, id: u64) {
        self.timers.borrow_mut().remove(&(when, id));
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropped_sleep_removes_timer_entry() -> io::Result<()> {
        let reactor = Rc::new(Reactor::new()?);
        let mut sleep = reactor.sleep(Duration::from_secs(60));

        // polling sleep should invoke insert_timer
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut sleep).poll(&mut cx).is_pending());
        assert_eq!(1, reactor.timers.borrow().len());

        // dropping here should invoke remove_timer
        drop(sleep);

        assert_eq!(0, reactor.timers.borrow().len());

        Ok(())
    }

    #[test]
    fn direction_readiness() {
        let mut direction = Direction::default();
        let mut cx = Context::from_waker(Waker::noop());

        // Ready before wait
        assert!(direction.signal().is_none());              // signal ready without stored waker
        assert!(direction.poll(&mut cx).is_ready());        // confirm ready without needing waker

        // Wait before ready
        assert!(direction.poll(&mut cx).is_pending());      // now we park and store the waker
        assert!(direction.signal().is_some());              // signal takes the waker
        assert!(direction.poll(&mut cx).is_ready());        // confirmation signal set ready
    }
}