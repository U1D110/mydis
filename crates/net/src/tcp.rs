use libc::{
    accept, addrinfo, bind, c_void, close, connect, fcntl, freeaddrinfo, gai_strerror, getaddrinfo,
    listen, recv, send, setsockopt, sockaddr, sockaddr_storage, socket, socklen_t,
};

use std::{
    ffi::{CStr, CString}, io, os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd}, ptr,
};

pub struct TcpListener {
    fd: OwnedFd,
}

impl TcpListener {
    pub fn bind(port: &str) -> io::Result<Self> {
        let mut hints: addrinfo = unsafe { std::mem::zeroed() };
        hints.ai_family = libc::AF_UNSPEC; // IPv4 or IPv6
        hints.ai_socktype = libc::SOCK_STREAM;
        hints.ai_flags = libc::AI_PASSIVE;

        let mut res: *mut addrinfo = ptr::null_mut();

        let port = CString::new(port).unwrap();
        let status = unsafe { getaddrinfo(ptr::null(), port.as_ptr(), &hints, &mut res) };
        if status != 0 {
            // CStr is a borrowed string we receive from a C function. Immutable and null-terminated.
            let err = unsafe { CStr::from_ptr(gai_strerror(status)) };
            return Err(io::Error::other(err.to_string_lossy()));
        }

        let mut p = res;
        let mut sockfd = -1; // Should we try using Option here?
        let yes = 1;
        let yes: *const c_void = &yes as *const _ as *const c_void;
        let option_len = std::mem::size_of::<i32>() as socklen_t;

        while !p.is_null() {
            let ai = unsafe { &*p };

            sockfd = unsafe { socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol) };
            if sockfd == -1 {
                eprintln!("server: socket");
                p = ai.ai_next;
                continue;
            }

            let status = unsafe {
                setsockopt(
                    sockfd,
                    libc::SOL_SOCKET,
                    libc::SO_REUSEADDR,
                    yes,
                    option_len,
                )
            };
            if status == -1 {
                return Err(io::Error::other("setsockopt"));
            }

            let status = unsafe { bind(sockfd, ai.ai_addr, ai.ai_addrlen) };
            if status != 0 {
                let err = std::io::Error::last_os_error();
                eprintln!("bind error: {}", err);
                unsafe { close(sockfd) };
                p = ai.ai_next;
                continue;
            }

            break;
        }

        unsafe { freeaddrinfo(res) };

        if p.is_null() {
            return Err(io::Error::other("Server failed to bind"));
        }

        let status = unsafe { listen(sockfd, 10) };
        if status == -1 {
            let err = std::io::Error::last_os_error();
            unsafe { close(sockfd) };
            return Err(err);
        }

        let fd = unsafe { OwnedFd::from_raw_fd(sockfd) };

        set_nonblocking(fd.as_fd())?;

        Ok(TcpListener { fd })
    }

    pub fn accept(&self) -> io::Result<TcpStream> {
        let mut their_addr: sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut sin_size = std::mem::size_of::<sockaddr_storage>() as socklen_t;

        let new_fd = unsafe {
            accept(
                self.fd.as_raw_fd(),
                &mut their_addr as *mut _ as *mut sockaddr,
                &mut sin_size,
            )
        };

        if new_fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let fd = unsafe { OwnedFd::from_raw_fd(new_fd) };

        set_nonblocking(fd.as_fd())?;

        Ok(TcpStream { fd })
    }

    pub fn local_port(&self) -> io::Result<u16> {
        let mut addr: sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut addr_len = std::mem::size_of::<sockaddr_storage>() as socklen_t;

        let status = unsafe {
            libc::getsockname(self.fd.as_raw_fd(), &mut addr as *mut _ as *mut sockaddr, &mut addr_len)
        };

        if status != 0 {
            return Err(io::Error::last_os_error());
        }

        match addr.ss_family as i32 {
            libc::AF_INET => {
                let sockaddr = (&addr) as *const _ as *const libc::sockaddr_in;
                Ok(u16::from_be(unsafe { (*sockaddr).sin_port }))
            }
            libc::AF_INET6 => {
                let sockaddr = (&addr) as *const _ as *const libc::sockaddr_in6;
                Ok(u16::from_be(unsafe { (*sockaddr).sin6_port }))
            }
            _ => Err(io::Error::other("Unknown address family")),
        }
    }
}

impl AsFd for TcpListener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

pub struct TcpStream {
    fd: OwnedFd,
}

impl TcpStream {
    pub fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        let bytes_received =
            unsafe { recv(self.fd.as_raw_fd(), buffer.as_mut_ptr() as *mut c_void, buffer.len(), 0) };

        if bytes_received < 0 {
            return Err(io::Error::last_os_error());
        } else if bytes_received == 0 {
            println!("server: client disconnected gracefully");
            return Ok(0);
        }

        Ok(bytes_received as usize)
    }

    pub fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        let bytes_sent = unsafe { send(self.fd.as_raw_fd(), bytes.as_ptr() as *const c_void, bytes.len(), 0) };

        if bytes_sent < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(bytes_sent as usize)
        }
    }

    pub fn connect(addr: &str, port: &str) -> io::Result<TcpStream> {
        let mut hints: addrinfo = unsafe { std::mem::zeroed() };
        hints.ai_family = libc::AF_UNSPEC; // IPv4 or IPv6
        hints.ai_socktype = libc::SOCK_STREAM;

        let mut res: *mut addrinfo = ptr::null_mut();

        let addr_c = CString::new(addr).map_err(|_| io::Error::other("Invalid address"))?;
        let port_c = CString::new(port).map_err(|_| io::Error::other("Invalid port"))?;

        let status = unsafe { getaddrinfo(addr_c.as_ptr(), port_c.as_ptr(), &hints, &mut res) };
        if status != 0 {
            // CStr is a borrowed string we receive from a C function. Immutable and null-terminated.
            let err = unsafe { CStr::from_ptr(gai_strerror(status)) };
            return Err(io::Error::other(err.to_string_lossy()));
        }

        let mut last_error: Option<io::Error> = None;
        let mut current = res;

        while !current.is_null() {
            let ai = unsafe { &*current };

            let sockfd = unsafe { socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol) };
            if sockfd == -1 {
                last_error = Some(io::Error::last_os_error());
                current = ai.ai_next;
                continue;
            }

            let status = unsafe { connect(sockfd, ai.ai_addr, ai.ai_addrlen) };
            if status == -1 {
                last_error = Some(io::Error::last_os_error());
                unsafe { close(sockfd) };
                current = ai.ai_next;
                continue;
            }

            unsafe { freeaddrinfo(res) };
            let fd = unsafe { OwnedFd::from_raw_fd(sockfd) };
            return Ok(TcpStream { fd });
        }

        unsafe { freeaddrinfo(res) };

        match last_error {
            Some(err) => Err(err),
            None => Err(io::Error::other("Failed to connect to any address")),
        }
    }
}

impl AsFd for TcpStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

fn set_nonblocking(fd: BorrowedFd<'_>) -> io::Result<()> {
    let flags = unsafe { fcntl(fd.as_raw_fd(), libc::F_GETFL, 0) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }

    let result = unsafe { fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}
