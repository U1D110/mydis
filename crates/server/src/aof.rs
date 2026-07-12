use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    os::fd::{AsFd, BorrowedFd, RawFd},
    path::Path,
    sync::mpsc::{Receiver, Sender, channel},
    thread::{self, JoinHandle}
};

use db::Database;
use net::{Notifier, Wakeup};
use protocol::ParseResult;

pub struct Aof {
    tx: Sender<FlushRequest>,
    worker: JoinHandle<()>,
    wakeup: Wakeup,
    completions: Receiver<Completion>,
}

pub struct FlushRequest {
    fd: RawFd,
    bytes: Vec<u8>,
    generation: u32,
}

impl FlushRequest {
    pub fn new(fd: RawFd, bytes: Vec<u8>, generation: u32) -> FlushRequest {
        FlushRequest { fd, bytes, generation }
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }
}

pub struct Completion {
    fd: RawFd,    // the connection whose write finished
    result: io::Result<()>, // did worker succeed (write_all + sync_data)?
    generation: u32,
}

impl Completion {
    pub fn new(fd: RawFd, result: io::Result<()>, generation: u32) -> Completion {
        Completion { fd, result, generation }
    }

    pub fn into_parts(self) -> (RawFd, io::Result<()>, u32) {
        (self.fd, self.result, self.generation)
    }
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
            tx: request_tx,
            worker,
            wakeup,
            completions: completion_rx, 
        })
    }

    pub fn submit(&self, req: FlushRequest) -> io::Result<()> {
        self.tx
            .send(req)
            .map_err(|_| io::Error::other("aof worker has stopped"))
    }

    pub fn shutdown(self) {
        let Aof { 
            tx, 
            worker,
            // Using actual binding with prepended underscore both to avoid warnings
            // and so that these are not dropped until they go out of scope with
            // the end of the function. Using a plain underscore would result in the 
            // values being immediately dropped, and we do not want the fd of the 
            // Wakeup to be closed until after we have joined the worker because it 
            // could otherwise notify epoll using reused fd.
            wakeup: _wakeup,
            completions: _completions,
        } = self;
        drop(tx);
        let _ = worker.join();
    }

    pub fn notify_fd(&self) -> BorrowedFd<'_> {
        self.wakeup.as_fd()
    }

    pub fn drain_completions(&self) -> io::Result<Vec<Completion>> {
        self.wakeup.drain()?;
        let mut out = Vec::new();
        while let Ok(c) = self.completions.try_recv() {
            out.push(c);
        }
        Ok(out)
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
        let _ = completion_sender.send(Completion::new(req.fd, result, req.generation()));
        // Let event loop (epoll) know that we have Completions to process.
        let _ = notifier.notify();
    }
}

pub fn replay<P: AsRef<Path>>(path: P, database: &mut Database) -> io::Result<u64> {
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
                let _ = database.execute(command);
                offset += consumed;
            }
            ParseResult::Incomplete => break, // truncated tail - crashed mid write
            ParseResult::Error(_) => break, // corrupted
        }
    }

    Ok(offset as u64)
}