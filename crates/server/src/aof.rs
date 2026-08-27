use std::{
    cell::{Cell, RefCell}, 
    collections::HashMap, 
    fs::{File, OpenOptions}, 
    io::{self, Write}, 
    os::fd::{AsFd, BorrowedFd}, 
    path::Path, 
    pin::Pin, 
    rc::Rc, 
    sync::mpsc::{Receiver, Sender, channel}, 
    task::{Context, Poll, Waker}, 
    thread::{self, JoinHandle},
};

use db::Database;
use net::{Notifier, Wakeup};
use protocol::ParseResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlushId(u64);

#[derive(Default)]
struct PendingFlush {
    waker: Option<Waker>,
    result: Option<io::Result<()>>,
}

pub struct Flush {
    id: FlushId,
    aof: Rc<Aof>,
}

impl Future for Flush {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.aof.poll_flush(self.id, cx)
    }
}

impl Drop for Flush {
    fn drop(&mut self) {
       self.aof.remove_pending_flush(self.id);
    }
}

struct FlushRequest {
    id: FlushId,
    bytes: Vec<u8>,
}

pub struct Completion {
    id: FlushId,
    result: io::Result<()>, // did worker succeed (write_all + sync_data)?
}

impl Completion {
    pub fn new(id: FlushId, result: io::Result<()>) -> Completion {
        Completion { id, result }
    }

    fn into_parts(self) -> (FlushId, io::Result<()>) {
        (self.id, self.result)
    }
}

pub struct Aof {
    tx: RefCell<Option<Sender<FlushRequest>>>,
    worker: RefCell<Option<JoinHandle<()>>>,
    wakeup: Wakeup,
    completions: Receiver<Completion>,
    counter: Cell<u64>,
    pending_flushes: RefCell<HashMap<FlushId, PendingFlush>>,
}

impl Aof {
    pub fn open<P: AsRef<Path>>(path: P, valid_len: u64) -> io::Result<Aof> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)?;

        file.set_len(valid_len)?;
        file.sync_data()?;

        let wakeup = Wakeup::new()?;
        let notifier = wakeup.notifier();

        let (request_tx, request_rx) = channel::<FlushRequest>();
        let (completion_tx, completion_rx) = channel::<Completion>();
        let worker = thread::spawn(move || worker_loop(file, request_rx, completion_tx, notifier));

        Ok(Aof { 
            tx: RefCell::new(Some(request_tx)),
            worker: RefCell::new(Some(worker)),
            wakeup,
            completions: completion_rx, 
            counter: Cell::new(1),
            pending_flushes: RefCell::new(HashMap::new()),
        })
    }

    pub fn flush(self: &Rc<Self>, bytes: Vec<u8>) -> io::Result<Flush> {
        if let Some(sender) = self.tx.borrow().as_ref() {
            let id = FlushId(self.counter.get());

            let req = FlushRequest { id, bytes };

            // Sending before inserting to avoid a stranded PendingFlush in the case of a failed
            // send. This is safe because flush runs synchronously on the main thread and the PendingFlush
            // will not be accessed until the returned Flush is polled.
            sender.send(req).map_err(|_| io::Error::other("aof worker has stopped"))?;
            self.pending_flushes.borrow_mut().insert(id, PendingFlush::default());

            self.counter.update(|n| n + 1);
            Ok(Flush { id, aof: Rc::clone(self) })
        } else {
            Err(io::Error::other("aof worker has stopped"))
        }
    }

    pub fn remove_pending_flush(&self, id: FlushId) {
        self.pending_flushes.borrow_mut().remove(&id);
    }

    pub fn poll_flush(&self, id: FlushId, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(pending) = self.pending_flushes.borrow_mut().get_mut(&id) {
            match pending.result.take() {
                Some(res) => Poll::Ready(res),
                None => {
                    let waker = cx.waker().clone();
                    pending.waker = Some(waker);
                    Poll::Pending
                }
            }
        } else {
            panic!("flush id does not exist");
        }
    }

    pub fn shutdown(&self) {
        self.tx.borrow_mut().take(); // let sender be dropped so the channel is closed
        if let Some(handle) = self.worker.borrow_mut().take() {
            let _ = handle.join();
        }
    }

    pub fn notify_fd(&self) -> BorrowedFd<'_> {
        self.wakeup.as_fd()
    }

    pub fn drain_and_complete(&self) -> io::Result<()> {
        self.wakeup.drain()?;
        while let Ok(c) = self.completions.try_recv() {
            self.complete(c);
        }
        Ok(())
    }

    fn complete(&self, completion: Completion) {
        let (id, result) = completion.into_parts();

        let waker = match self.pending_flushes.borrow_mut().get_mut(&id) {
            Some(pending) => {
                pending.result = Some(result);
                pending.waker.take()
            }

            None => None,
        };

        if let Some(waker) = waker { waker.wake(); }
    }
}

fn worker_loop(
    mut file: File,
    request_receiver: Receiver<FlushRequest>,
    completion_sender: Sender<Completion>,
    notifier: Notifier,
) {
    for req in request_receiver {
        let result = file
            .write_all(&req.bytes)
            .and_then(|_| file.sync_data());

        // Let Aof know we completed.
        let _ = completion_sender.send(Completion::new(req.id, result));
        // Let event loop (epoll) know that we have Completions to process.
        let _ = notifier.notify();
    }
}

pub fn replay<P: AsRef<Path>>(path: P, database: Rc<RefCell<Database>>) -> io::Result<u64> {
    // Read file into memory. Fine for the scale of this project.
    let bytes = match std::fs::read(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut offset = 0;
    while offset < bytes.len() {
        match protocol::parse(&bytes[offset..]) {
            ParseResult::Complete(command, consumed) => {
                let _ = database.borrow_mut().execute(command);
                offset += consumed;
            }
            ParseResult::Incomplete => break, // truncated tail - crashed mid write
            ParseResult::Error(_) => break, // corrupted
        }
    }

    Ok(offset as u64)
}