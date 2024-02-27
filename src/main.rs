// Uncomment this block to pass the first stage
use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                handle_stream(&mut _stream);
                _stream.write_all(b"+PONG\r\n").expect("Failed to write.");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(stream: &mut std::net::TcpStream) {
    const PONG: &[u8] = "+PONG\r\n".as_bytes();
    let mut buffer = [0; 1024];

    while let Ok(bytes_read) = stream.read(&mut buffer) {
        println!(
            "received: {}",
            String::from_utf8_lossy(&buffer[..bytes_read])
        );
        stream.write(PONG).expect("write failed");
    }
}
