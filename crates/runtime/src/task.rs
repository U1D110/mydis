use std::{cell::{Cell, RefCell}, collections::VecDeque, mem::ManuallyDrop, pin::Pin, rc::{Rc, Weak}, task::{Context, Poll, RawWaker, RawWakerVTable, Waker}};

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_task, wake_task, wake_by_ref_task, drop_task);

unsafe fn clone_task(ptr: *const ()) -> RawWaker {
    unsafe { Rc::increment_strong_count(ptr.cast::<Task>()) };
    RawWaker::new(ptr, &VTABLE)
}

unsafe fn wake_task(ptr: *const ()) {
    // Consume
    let task: Rc<Task> = unsafe { Rc::from_raw(ptr.cast::<Task>()) };
    task.schedule();
}

unsafe fn wake_by_ref_task(ptr: *const ()) {
    // Borrow
    let task = ManuallyDrop::new(
        unsafe { Rc::from_raw(ptr.cast::<Task>()) }
    );
    task.schedule();
}

unsafe fn drop_task(ptr: *const ()) {
    unsafe { Rc::decrement_strong_count(ptr.cast::<Task>()) };
}

fn waker_from_task(task: Rc<Task>) -> Waker {
    // SAFETY: This is not thread safe. Perhaps obvious since our data pointer
    // is `Rc`, but since `Waker` is `Send` and `Sync` unconditionally, the 
    // compiler will not stop you from sending this `Waker` across thread
    // boundaries.
    // Currently, this is safe because this runtime is single-threaded, so 
    // no `Waker` will ever be sent to another thread.
    let ptr = Rc::into_raw(task) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
}

#[derive(Default)]
pub struct RunQueue {
    tasks: RefCell<VecDeque<Rc<Task>>>,
}

impl RunQueue {
    pub fn new() -> Self {
        Self {
            tasks: RefCell::new(VecDeque::new())
        }
    }

    pub fn push(&self, task: Rc<Task>) {
        self.tasks.borrow_mut().push_back(task);
    }

    #[allow(dead_code)]
    pub fn pop(&self) -> Option<Rc<Task>> {
        self.tasks.borrow_mut().pop_front()
    }

    pub fn take(&self) -> VecDeque<Rc<Task>> {
        std::mem::take(&mut *self.tasks.borrow_mut())
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.borrow().is_empty()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.tasks.borrow().len()
    }
}

pub struct Task {
    future: RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,
    queue: Weak<RunQueue>,
    queued: Cell<bool>,
    done: Cell<bool>,
}

impl Task {
    pub fn new<F>(future: F, queue: Weak<RunQueue>) -> Self
    where
        F: Future<Output = ()> + 'static
    {
        Self {
            future: RefCell::new(Some(Box::pin(future))),
            queue,
            queued: Cell::new(false),
            done: Cell::new(false),
        }
    }

    pub fn run(self: &Rc<Self>) -> bool {
        self.queued.set(false);
        if self.done.get() { return false; }

        // If `None` this `Task` has already completed, so we simply return.
        let mut future = match self.future.borrow_mut().take(){
            Some(f) => f,
            None => return false,
        };

        let waker = waker_from_task(Rc::clone(self));
        let mut cx = Context::from_waker(&waker);

        // If this future is Ready then set `Task`s done flag and let `future` drop.
        // And if it is pending, put it back in the task's future slot.
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(()) => {
                self.done.set(true);
                true
            }
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                false
            }
        }
    }

    pub fn schedule(self: &Rc<Self>) -> bool {
        if self.done.get() || self.queued.get() { return false; }

        let Some(run_queue) = self.queue.upgrade() else {
            return false;
        };

        self.queued.set(true);
        run_queue.push(Rc::clone(self));
        true
    }
}

pub struct Yield {
    yielded: bool,
}

impl Future for Yield {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if this.yielded {
            return Poll::Ready(());
        }

        this.yielded = true;

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}

#[must_use]
pub fn yield_now() -> Yield {
    Yield { yielded: false }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn honors_rc_wake_contracts() {
        let task = {
            let run_queue = Rc::new(RunQueue::new());
            let task = Task::new(async {}, Rc::downgrade(&run_queue));
            // Pretend this task is already queued so that calls to `schedule()` triggered by
            // waking do not increase its ref count. Test could be reworked to accomodate the 
            // count increments, but this test is about the waker contracts, not the scheduling
            // mechanism.
            task.queued.set(true);
            Rc::new(task)
        };
        let waker = waker_from_task(Rc::clone(&task));

        assert_eq!(Rc::strong_count(&task), 2);

        let a = waker.clone();
        let b = waker.clone();
        assert_eq!(Rc::strong_count(&task), 4);

        drop(a);
        drop(b);
        assert_eq!(Rc::strong_count(&task), 2);

        let third = waker.clone();
        assert_eq!(Rc::strong_count(&task), 3);

        third.wake_by_ref();
        assert_eq!(Rc::strong_count(&task), 3);

        third.wake();
        assert_eq!(Rc::strong_count(&task), 2);
    }

    #[test]
    fn scheduling_dedups() {
        let run_queue = Rc::new(RunQueue::new());
        let task = Rc::new(Task::new(async {}, Rc::downgrade(&run_queue)));
        let waker = waker_from_task(Rc::clone(&task));

        assert!(run_queue.is_empty());

        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake_by_ref();

        assert_eq!(run_queue.len(), 1);
    }
}