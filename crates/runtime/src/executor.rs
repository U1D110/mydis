use std::cell::Cell;
use std::io;
use std::mem::ManuallyDrop;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread::{self, Thread};

use crate::task::{RunQueue, Task};
use crate::Reactor;

static VTABLE: RawWakerVTable = 
    RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
    unsafe { Arc::increment_strong_count(ptr.cast::<Thread>()) };
    RawWaker::new(ptr, &VTABLE)
}

unsafe fn wake_waker(ptr: *const ()) {
    // Consuming:
    // We take ownership, but the ref count remains unchanged with the `from_raw` call.
    // We do our unpark and then let `thread` drop naturally when it goes out of scope.
    let thread = unsafe { Arc::from_raw(ptr.cast::<Thread>()) };
    thread.unpark();
}

unsafe fn wake_by_ref_waker(ptr: *const ()) {
    // Borrowing: reconstruct so we can use it, but ManuallyDrop stops the
    // count from being released; the Waker still exists and still owns it.
    let thread = ManuallyDrop::new(
        unsafe { Arc::from_raw(ptr.cast::<Thread>()) }
    );
    thread.unpark();
}

unsafe fn drop_waker(ptr: *const ()) {
    unsafe { Arc::decrement_strong_count(ptr.cast::<Thread>()); }
}

fn waker_for(thread: Arc<Thread>) -> Waker {
    let ptr = Arc::into_raw(thread) as *const ();
    // SAFETY: `Waker` is Send + Sync, so the vtable must be thread-safe.
    // The payload is an `Arc<Thread>`. `Thread` is Send + Sync, so `Arc<Thread>`
    // is too. Every vtable function manipulates only the atomic strong count.
    // `ptr` came from `Arc::into_raw` and every vtable function casts it back to
    // `*const Thread`, the same type it was created from.
    unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }    
}

pub fn block_on<F>(fut: F) -> F::Output
where 
    F: Future,
{
    let mut fut = pin!(fut);
    let waker = waker_for(Arc::new(thread::current()));
    let mut cx = Context::from_waker(&waker);

    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => thread::park(),
        } 
    }
}

pub struct Runner {
    queue: Rc<RunQueue>,
    reactor: Rc<Reactor>,
    alive_count: Cell<usize>,
}

impl Runner {
    pub fn new() -> io::Result<Self> {
        let reactor = Reactor::new()?;
        Ok(Self { 
            queue: Rc::new(RunQueue::new()), 
            reactor: Rc::new(reactor),
            alive_count: Cell::new(0),
        })
    }

    pub fn reactor(&self) -> Rc<Reactor> {
        Rc::clone(&self.reactor)
    }

    pub fn spawn<F>(&self, future: F) 
    where
        F: Future<Output = ()> + 'static
    {
        let task = Rc::new(Task::new(future, Rc::downgrade(&self.queue)));
        task.schedule();
        self.alive_count.update(|n| n + 1);
    }

    pub fn run(&self) -> io::Result<()> {
        loop {
            for task in self.queue.take() {
                if task.run() {
                    self.alive_count.update(|n| n.saturating_sub(1));
                }
            }

            if self.alive_count.get() == 0 { return Ok(()); }

            // If we have more tasks queued after processing the last batch, use a 0 timeout
            // so as not to block and immediately process the new batch.
            // Otherwise, for now, use -1 (eventually we should be calculating a timeout)
            let timeout = if self.queue.is_empty() {
                -1
            } else {
                0
            };
            self.reactor.turn(timeout)?;
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn honors_arc_wake_contracts() {
        let thread = Arc::new(thread::current());
        let waker = waker_for(Arc::clone(&thread));

        // one for `thread` and one for `waker`
        assert_eq!(Arc::strong_count(&thread), 2);

        let a = waker.clone();
        let b = waker.clone();
        assert_eq!(Arc::strong_count(&thread), 4);

        drop(a);
        drop(b);
        assert_eq!(Arc::strong_count(&thread), 2);

        let third = waker.clone();
        assert_eq!(Arc::strong_count(&thread), 3);

        third.wake_by_ref();
        assert_eq!(Arc::strong_count(&thread), 3);

        third.wake();
        assert_eq!(Arc::strong_count(&thread), 2);
    }
}