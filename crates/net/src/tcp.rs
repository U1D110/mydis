use libc::{
    accept,
    addrinfo,
    bind,
    close,
    connect,
    c_void,
    fcntl,
    freeaddrinfo,
    gai_strerror,
    getaddrinfo,
    listen,
    recv,
    send,
    setsockopt,
    sockaddr,
    sockaddr_storage,
    socket,
    socklen_t,
};

use std::{
    ffi::{
        CStr,
        CString,
    },
    io,
    ptr,
};

pub struct TcpListener {
    fd: i32,
}

impl TcpListener {
    pub fn bind(port: &str) -> io::Result<Self> {
        let mut hints: addrinfo = unsafe { std::mem::zeroed() };
        hints.ai_family = libc::AF_UNSPEC;  // IPv4 or IPv6
        hints.ai_socktype = libc::SOCK_STREAM;
        hints.ai_flags = libc::AI_PASSIVE;

        let mut res: *mut addrinfo = ptr::null_mut();

        let port = CString::new(port).unwrap();
        let status = unsafe {
            getaddrinfo(ptr::null(), port.as_ptr(), &hints, &mut res)
        };
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

            sockfd = unsafe {
                socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol)
            };
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

        set_nonblocking(sockfd)?;

        Ok(TcpListener {
            fd: sockfd,
        })
    }

    pub fn accept(&self) -> io::Result<TcpStream> {
        let mut their_addr: sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut sin_size = std::mem::size_of::<sockaddr_storage>() as socklen_t;

        let new_fd = unsafe {
            accept(
                self.fd,
                &mut their_addr as *mut _ as *mut sockaddr,
                &mut sin_size
            )
        };

        if new_fd < 0 {
            return Err(io::Error::last_os_error());
        }

        set_nonblocking(new_fd)?;

        // TODO: This should be logged
        let ai = match their_addr.ss_family as i32 {
            libc::AF_INET => {
                let sockaddr = (&their_addr) as *const _ as *const libc::sockaddr_in;
                let ip_ptr = unsafe { &(*sockaddr).sin_addr as *const libc::in_addr };
                ipv4_to_string(ip_ptr)
            },
            libc::AF_INET6 => {
                let sockaddr = (&their_addr) as *const _ as *const libc::sockaddr_in6;
                let ip_ptr = unsafe { &(*sockaddr).sin6_addr as *const libc::in6_addr };
                ipv6_to_string(ip_ptr)
            },
            _ => String::from("Unknown"),
        };

        println!("server: got connection from {}", ai);

        return Ok(TcpStream { fd: new_fd });
    }

    pub fn local_port(&self) -> io::Result<u16> {
        let mut addr: sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut addr_len = std::mem::size_of::<sockaddr_storage>() as socklen_t;

        let status = unsafe {
            libc::getsockname(
                self.fd,
                &mut addr as *mut _ as *mut sockaddr,
                &mut addr_len,
            )
        };

        if status != 0 {
            return Err(io::Error::last_os_error());
        }

        match addr.ss_family as i32 {
            libc::AF_INET => {
                let sockaddr = (&addr) as *const _ as *const libc::sockaddr_in;
                Ok(u16::from_be(unsafe { (*sockaddr).sin_port }))
            },
            libc::AF_INET6 => {
                let sockaddr = (&addr) as *const _ as *const libc::sockaddr_in6;
                Ok(u16::from_be(unsafe { (*sockaddr).sin6_port }))
            },
            _ => Err(io::Error::other("Unknown address family")),
        }
    }

    // Why use the word "raw" here? We're just returning an i32 ...
    pub fn as_raw_fd(&self) -> i32 {
        self.fd
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        if self.fd != -1 {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}

pub struct TcpStream {
    fd: i32,
}

impl TcpStream {
    pub fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        let bytes_received = unsafe {
            recv(
                self.fd,
                buffer.as_mut_ptr() as *mut c_void,
                buffer.len(),
                0,
            )
        };

        if bytes_received < 0 {
            return Err(io::Error::last_os_error());
        } else if bytes_received == 0 {
            println!("server: client disconnected gracefully");
            return Ok(0);
        }

        Ok(bytes_received as usize)
    }

    pub fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        let bytes_sent = unsafe {
            send(
                self.fd,
                bytes.as_ptr() as *const c_void,
                bytes.len(),
                0,
            )
        };

        if bytes_sent < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(bytes_sent as usize)
        }
    }

    pub fn as_raw_fd(&self) -> i32 {
        self.fd
    }

    pub fn connect(addr: &str, port: &str) -> io::Result<TcpStream> {
        let mut hints: addrinfo = unsafe { std::mem::zeroed() };
        hints.ai_family = libc::AF_UNSPEC;  // IPv4 or IPv6
        hints.ai_socktype = libc::SOCK_STREAM;

        let mut res: *mut addrinfo = ptr::null_mut();

        let addr_c = CString::new(addr)
            .map_err(|_| io::Error::other("Invalid address"))?;
        let port_c = CString::new(port)
            .map_err(|_| io::Error::other("Invalid port"))?;

        let status = unsafe {
            getaddrinfo(addr_c.as_ptr(), port_c.as_ptr(), &hints, &mut res)
        };
        if status != 0 {
            // CStr is a borrowed string we receive from a C function. Immutable and null-terminated.
            let err = unsafe { CStr::from_ptr(gai_strerror(status)) };
            return Err(io::Error::other(err.to_string_lossy()));
        }

        let mut last_error: Option<io::Error> = None;
        let mut current = res;

        while !current.is_null() {
            let ai = unsafe { &*current };

            let sockfd = unsafe {
                socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol)
            };
            if sockfd == -1 {
                last_error = Some(io::Error::last_os_error());
                current = ai.ai_next;
                continue;
            }

            let status = unsafe { 
                connect(sockfd, ai.ai_addr, ai.ai_addrlen) 
            };
            if status == -1 {
                last_error = Some(io::Error::last_os_error());
                unsafe { close(sockfd) };
                current = ai.ai_next;
                continue;
            }

            unsafe { freeaddrinfo(res) };
            return Ok(TcpStream { fd: sockfd });
        }

        unsafe { freeaddrinfo(res) };

        match last_error {
            Some(err) => Err(err),
            None => Err(io::Error::other("Failed to connect to any address")),
        }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        if self.fd != -1 {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}


fn ipv4_to_string(addr: *const libc::in_addr) -> String {
    let addr = u32::from_be(unsafe { (*addr).s_addr });
    std::net::Ipv4Addr::from(addr).to_string()
}

fn ipv6_to_string(addr: *const libc::in6_addr) -> String {
    let segments = unsafe { (*addr).s6_addr };
    std::net::Ipv6Addr::from(segments).to_string()
}

fn set_nonblocking(fd: i32) -> io::Result<()> {
    let flags = unsafe { fcntl(fd, libc::F_GETFL, 0) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }

    let result = unsafe { 
        fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK)
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}
