use std::io;

use libc::{sigaddset, sigemptyset, signalfd, sigprocmask, sigset_t};

pub struct Signals {
    fd: i32,
}

impl Signals {
    pub fn new() -> io::Result<Signals> {
        let mut mask: sigset_t = unsafe { std::mem::zeroed() };
        unsafe {
            sigemptyset(&mut mask);
            sigaddset(&mut mask, libc::SIGINT);
            sigaddset(&mut mask, libc::SIGTERM);

            // Block these so their default action to terminate never runs and so
            // signalfd is the only delivery path.
            // Must run before any thread is spawned so spawned threads inherit
            // this mask.
            if sigprocmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut()) > 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = signalfd(-1, &mask, libc::SFD_NONBLOCK | libc::SFD_CLOEXEC);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Signals { fd })
        }
    }

    pub fn as_raw_fd(&self) -> i32 {
        self.fd
    }

    pub fn drain(&self) -> io::Result<()> {
        todo!()
    }
}

impl Drop for Signals {
    fn drop(&mut self) {
        if self.fd != -1 {
            unsafe { libc::close(self.fd) };
        }
    }
}