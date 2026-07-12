use std::{io, os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd}};

use libc::{c_void, sigaddset, sigemptyset, signalfd, sigprocmask, sigset_t};

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
        const SIGINFO_SIZE: usize = std::mem::size_of::<libc::signalfd_siginfo>();
        let mut buf = [0u8; SIGINFO_SIZE];

        loop {
            let n = unsafe { 
                libc::read(
                    self.fd.as_raw_fd(),
                    &mut buf as *mut _ as *mut c_void, 
                    SIGINFO_SIZE
                ) 
            };

            match n {
                -1 => {
                    let e = io::Error::last_os_error();
                    match e.raw_os_error() {
                        Some(libc::EINTR) => continue,
                        Some(libc::EAGAIN) => return Ok(()),
                        _ => return Err(e),
                    }
                }

                n if n as usize == SIGINFO_SIZE => continue,

                _ => {
                    return Err(io::Error::other("unexpected signalfd read size: {n}"));
                }
            }
        }
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