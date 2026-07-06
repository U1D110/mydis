use std::{io, os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd}};

use libc::{sigaddset, sigemptyset, signalfd, sigprocmask, sigset_t};

pub struct Signals {
    fd: OwnedFd,
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
            let fd = OwnedFd::from_raw_fd(fd);
            Ok(Signals { fd })
        }
    }

    pub fn drain(&self) -> io::Result<()> {
        todo!()
    }
}

impl AsFd for Signals {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for Signals {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}