use net::{TcpListener, TcpStream};

const PORT: &str = "3490";

fn main() {
    let listener = TcpListener::bind(PORT).expect("Failed to bind");
    
    println!("Server waiting for connections...");

    if let Ok(stream) = TcpListener::accept(&listener) {
        println!("We have a connection.");

        if let Ok(bytes) = &stream.read() {
            if let Err(err) = &stream.write(bytes) {
                eprintln!("Write error: {}", err);
            }
        }
    }
}
