use libc::{EFD_CLOEXEC, EFD_NONBLOCK, c_void, epoll_create1, epoll_ctl, epoll_event, epoll_wait};

use std::{io, os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd}};

pub struct Event {
    flags: u32,
    fd: RawFd,
}

impl Event {
    pub fn from_epoll(evt: epoll_event) -> Self {
        Event {
            flags: evt.events,
            fd: evt.u64 as RawFd,
        }
    }

    pub fn readable(&self) -> bool {
        self.flags & libc::EPOLLIN as u32 != 0
    }

    pub fn writable(&self) -> bool {
        self.flags & libc::EPOLLOUT as u32 != 0
    }

    pub fn error(&self) -> bool {
        self.flags & libc::EPOLLERR as u32 != 0
    }

    pub fn hang_up(&self) -> bool {
        self.flags & libc::EPOLLHUP as u32 != 0
    }

    pub fn rdhup(&self) -> bool {
        self.flags & libc::EPOLLRDHUP as u32 != 0
    }

    pub fn fd(&self) -> RawFd {
        self.fd
    }
}

pub struct Events {
    inner: Vec<epoll_event>,
}

impl Events {
    pub fn with_capacity(capacity: usize) -> Self {
        Events {
            inner: Vec::with_capacity(capacity),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.inner.iter().map(|&event| Event::from_epoll(event))
    }
}

pub struct Interests {
    read: bool,
    write: bool,
}

impl Interests {
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
        }
    }

    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
        }
    }
}

pub struct Poll {
    epoll_fd: OwnedFd,
}

impl Poll {
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { epoll_create1(0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let epoll_fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Poll { epoll_fd })
    }

    pub fn register(&self, fd: BorrowedFd<'_>, interests: Interests) -> io::Result<()> {
        self.poll_ctl(libc::EPOLL_CTL_ADD, fd, interests)
    }

    pub fn reregister(&self, fd: BorrowedFd<'_>, interests: Interests) -> io::Result<()> {
        self.poll_ctl(libc::EPOLL_CTL_MOD, fd, interests)
    }

    pub fn deregister(&self, fd: RawFd) -> io::Result<()> {
        let mut empty_event: epoll_event = unsafe { std::mem::zeroed() };

        let res = unsafe {
            epoll_ctl(
                self.epoll_fd.as_raw_fd(),
                libc::EPOLL_CTL_DEL,
                fd,
                &mut empty_event,
            )
        };

        if res < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn poll_ctl(&self, op: i32, fd: BorrowedFd<'_>, interests: Interests) -> io::Result<()> {
        let mut flags = (libc::EPOLLET | libc::EPOLLRDHUP) as u32;
        if interests.read {
            flags |= libc::EPOLLIN as u32;
        }
        if interests.write {
            flags |= libc::EPOLLOUT as u32;
        }

        let mut event = epoll_event {
            events: flags,
            u64: fd.as_raw_fd() as u64,
        };

        let res = unsafe { epoll_ctl(self.epoll_fd.as_raw_fd(), op, fd.as_raw_fd(), &mut event) };

        if res < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn wait(&self, events: &mut Events, timeout: i32) -> io::Result<()> {
        events.inner.clear();

        let num_events = unsafe {
            epoll_wait(
                self.epoll_fd.as_raw_fd(),
                events.inner.as_mut_ptr(),
                events.inner.capacity() as i32,
                timeout,
            )
        };

        if num_events < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                Ok(())
            } else {
                Err(err)
            }
        } else {
            unsafe { events.inner.set_len(num_events as usize) };
            Ok(())
        }
    }

    pub fn wait_no_timeout(&self, events: &mut Events) -> io::Result<usize> {
        events.inner.clear();

        loop {
            let num_events = unsafe {
                epoll_wait(
                    self.epoll_fd.as_raw_fd(),
                    events.inner.as_mut_ptr(),
                    events.inner.capacity() as i32,
                    -1,
                )
            };

            if num_events < 0 {
                let err = io::Error::last_os_error();

                // System calls can be interrupted by OS signals. So if this
                // happens we just keep looping and try epoll_wait again.
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(err);
            } else if num_events == 0 {
                return Err(io::Error::other("wait timed out"));
            } else {
                unsafe { events.inner.set_len(num_events as usize) };
                return Ok(num_events as usize);
            }
        }
    }
}

// Copy used by worker thread to signal. Does not own the fd.
#[derive(Clone, Copy)]
pub struct Notifier {
    fd: RawFd,
}

impl Notifier {
    pub fn notify(&self) -> io::Result<()> {
        let one: u64 = 1;
        let n = unsafe { libc::write(self.fd, &one as *const _ as *const c_void, 8) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

pub struct Wakeup {
    fd: OwnedFd,
}

impl Wakeup {
    pub fn new() -> io::Result<Wakeup> {
        let fd = unsafe { libc::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC) };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            Ok(Wakeup { fd })
        }
    }

    pub fn notifier(&self) -> Notifier {
        Notifier { fd: self.fd.as_raw_fd() }
    }

    pub fn drain(&self) -> io::Result<()> {
        loop {
            let mut buf: u64 = 0;
            let n = unsafe { libc::read(self.fd.as_raw_fd(), &mut buf as *mut _ as *mut c_void, 8) };
            match n {
                8 => return Ok(()),
                
                -1 => {
                    let e = io::Error::last_os_error();
                    match e.raw_os_error() {
                        Some(libc::EINTR) => continue,
                        Some(libc::EAGAIN) => return Ok(()),
                        _ => return Err(e),
                    }
                }

                _ => {
                    return Err(io::Error::other("unexpected eventfd read size: {n}"));
                }
            } 
        }
    }
}

impl AsFd for Wakeup {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for Wakeup {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}