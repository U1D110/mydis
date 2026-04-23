use libc::{
    close,
    epoll_create1,
    epoll_ctl,
    epoll_event,
    epoll_wait,
};

use std::io;

// TODO: remove `pub` for all and add getters
pub struct Event {
    pub readable: bool,
    pub writable: bool,
    pub error: bool,
    pub hang_up: bool,
    pub fd: i32,
}

impl Event {
    pub fn from_epoll(evt: epoll_event) -> Self {
        let flags = evt.events;

        Event {
            readable: (flags & libc::EPOLLIN as u32) != 0,
            writable: (flags & libc::EPOLLOUT as u32) != 0,
            error:    (flags & libc::EPOLLERR as u32) != 0,
            hang_up:  (flags & libc::EPOLLHUP as u32) != 0,
            fd:       evt.u64 as i32,
        }
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

pub struct Poll {
    epoll_fd: i32,
}

impl Poll {
    pub fn new() -> io::Result<Self> {
        let epoll_fd = unsafe { epoll_create1(0) };
        if epoll_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Poll { epoll_fd })
    }

    pub fn register(&self, fd: i32) -> io::Result<()> {
        let mut event = epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLET) as u32,
            u64: fd as u64,
        };

        let res = unsafe {
            epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut event)
        };

        if res < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn wait(&self, events: &mut Events) -> io::Result<usize> {
        events.inner.clear();

        loop {
            let num_events = unsafe {
                epoll_wait(
                    self.epoll_fd,
                    events.inner.as_mut_ptr(),
                    events.inner.capacity() as i32,
                    -1
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

impl Drop for Poll {
    fn drop(&mut self) {
        if self.epoll_fd != -1 {
            unsafe { close(self.epoll_fd) };
        }
    }
}