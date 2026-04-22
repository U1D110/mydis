use std::{
    ffi::{
        CStr,
        CString,
    },
    ptr
};

use libc::{
    accept, addrinfo, c_char, c_void, bind, close, freeaddrinfo, 
    gai_strerror, getaddrinfo, gethostname, listen, perror, setsockopt, signal, 
    sockaddr, sockaddr_storage, socket, socklen_t,
};

fn main() {
    //let host = String::from("duckduckgo.com"); //env::args().nth(1).expect("usage: playground <hostname>");
    // CString is an owned (and null-terminated) string we can pass to a C function.
    //let host_c = CString::new(host).unwrap();

    unsafe {
        // Set up signal handler to reap zombie processes automatically (Beej-style)
        // This ignores SIGCHLD, letting the system clean up child processes without waitpid calls.
        signal(libc::SIGCHLD, libc::SIG_IGN);

        let mut hints: addrinfo = std::mem::zeroed();
        hints.ai_family = libc::AF_UNSPEC;  // IPv4 or IPv6
        hints.ai_socktype = libc::SOCK_STREAM;
        hints.ai_flags = libc::AI_PASSIVE;

        let mut res: *mut addrinfo = ptr::null_mut();

        //let status = getaddrinfo(host_c.as_ptr(), ptr::null(), &hints, &mut res);
        let mut hostname = [0u8; 256];
        let status = gethostname(hostname.as_mut_ptr() as *mut i8, hostname.len());
        if status != 0 {
            // CStr is a borrowed string we receive from a C function. Immutable and null-terminated.
            let err = std::io::Error::last_os_error();
            eprintln!("gethostname error: {}", err);
        }

        // Ensure null termination
        hostname[hostname.len() - 1] = 0;

        let hostname = CStr::from_ptr(hostname.as_ptr() as *const i8).to_string_lossy();
        println!("Hostname: {}", hostname);

        let port = CString::new("3490").unwrap();
        let status = getaddrinfo(ptr::null(), port.as_ptr(), &hints, &mut res);
        if status != 0 {
            // CStr is a borrowed string we receive from a C function. Immutable and null-terminated.
            let err = CStr::from_ptr(gai_strerror(status));
            eprintln!("getaddrinfo error: {}", err.to_string_lossy());
            return;
        }

        let mut p = res;
        let mut sockfd = -1; // Should we try using Option here?
        let yes = 1;
        let yes: *const c_void = &yes as *const _ as *const c_void;
        let option_len = std::mem::size_of::<i32>() as socklen_t;

        while !p.is_null() {
            let ai = &*p;

            // Make a socket
            sockfd = socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol);
            if sockfd == -1 {
                eprintln!("server: socket");
                p = ai.ai_next;
                continue;
            }

            // Add setsockopt call to reuse socket?
            let status = setsockopt(
                sockfd,
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                yes,
                option_len,
            );
            if status == -1 {
                perror(b"setsockopt\0".as_ptr() as *const c_char);
                return;
            }

            // Bind to our port
            let status = bind(sockfd, ai.ai_addr, ai.ai_addrlen);
            if status != 0 {
                let err = std::io::Error::last_os_error();
                eprintln!("bind error: {}", err);
                close(sockfd); // close on failure. Double check that this is needed.
                p = ai.ai_next;
                continue;
            }

            break;
        }

        freeaddrinfo(res);

        if p.is_null() {
            eprintln!("server: failed to bind");
            return;
        }

        // listen on sockfd
        let status = listen(sockfd, 10);
        if status == -1 {
            let err = std::io::Error::last_os_error();
            eprintln!("listen error: {}", err);
            close(sockfd);
            return;
        }

        println!("server: waiting for connections...");

        // accept loop
        let mut their_addr: sockaddr_storage = std::mem::zeroed();
        let mut sin_size = std::mem::size_of::<sockaddr_storage>() as socklen_t;
        loop {
            let new_fd = accept(
                sockfd,
                &mut their_addr as *mut _ as *mut sockaddr,
                &mut sin_size
            );

            if new_fd == -1 {
                continue;
            }

            let ai = match their_addr.ss_family as i32 {
                libc::AF_INET => {
                    let sockaddr = (&their_addr) as *const _ as *const libc::sockaddr_in;
                    let ip_ptr = &(*sockaddr).sin_addr as *const libc::in_addr;
                    ipv4_to_string(ip_ptr)
                },
                libc::AF_INET6 => {
                    let sockaddr = (&their_addr) as *const _ as *const libc::sockaddr_in6;
                    let ip_ptr = &(*sockaddr).sin6_addr as *const libc::in6_addr;
                    ipv6_to_string(ip_ptr)
                },
                _ => String::from("Unknown"),
            };

            println!("server: got connection from {}", ai);

            // TODO: replace this with a loop that continuously calls recv()
            // Fork to handle the connection in a child process
            //let pid = fork();
            //if pid == -1 {
            //    eprintln!("fork error");
            //    close(new_fd);
            //    continue;
            //} else if pid == 0 {
            //    // Child process: handle the connection
            //    close(sockfd); // Child doesn't need the listener socket

            //    // Respond
            //    let response = CString::new("Hello and welcome to my server. Please ignore the smell.\n").unwrap();
            //    let bytes_sent = libc::send(new_fd, response.as_ptr() as *const c_void, response.as_bytes().len(), 0);
            //    if bytes_sent == -1 {
            //        perror(b"send\0".as_ptr() as *const c_char);
            //    }

            //    close(new_fd);
            //    std::process::exit(0); // Exit child process
            //} else {
            //    // Parent process: close the accepted socket and continue listening
            //    close(new_fd);
            //}

            // Allocate a fixed-size buffer to hold incoming bytes
            let mut buffer = [0u8; 1024];

            // Echo loop
            loop {
                // 1. Read data from client
                let bytes_received = libc::recv(
                    new_fd,
                    buffer.as_mut_ptr() as *mut c_void,
                    buffer.len(),
                    0,
                );

                if bytes_received < 0 {
                    perror(b"recv\0".as_ptr() as *const c_char);
                    break;
                    // Error happened, so drop the connection
                } else if bytes_received == 0 {
                    println!("server: client disconnected gracefully");
                    break;
                }

                // 2. Send the same data back
                // Note: the amount of bytes we send is equal to bytes_received, not the entire buffer
                let bytes_sent = libc::send(
                    new_fd,
                    buffer.as_ptr() as *const c_void,
                    bytes_received as usize,
                    0,
                );

                if bytes_sent == -1 {
                    perror(b"send\0".as_ptr() as *const c_char);
                    break; // Send error, drop connection
                }
            }

            close(new_fd);
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
