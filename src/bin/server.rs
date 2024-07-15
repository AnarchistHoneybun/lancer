use std::net::TcpListener;
use std::io::{Read, Write};
use std::thread;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let port = &args[1];
    let server_name = &args[2];

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).unwrap();
    println!("{} listening on {}", server_name, addr);

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let server_name = server_name.clone();

        thread::spawn(move || {
            handle_connection(stream, &server_name);
        });
    }
}

fn handle_connection(mut stream: std::net::TcpStream, server_name: &str) {
    let mut buffer = [0; 1024];
    stream.read(&mut buffer).unwrap();

    let request = String::from_utf8_lossy(&buffer[..]);
    let first_line = request.lines().next().unwrap_or("");

    if first_line.starts_with("GET /health ") {
        let response = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nOK";
        stream.write(response.as_bytes()).unwrap();
        stream.flush().unwrap();
        return;
    }

    let number = first_line
        .split_whitespace().nth(1)
        .and_then(|path| path.trim_start_matches("/").parse::<u64>().ok())
        .unwrap_or(1);  // Default to 1 if no valid number is provided

    let factor_count = count_factors(number);

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nHello from {}, your factors are {}",
        server_name, factor_count,
    );

    stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}

fn count_factors(n: u64) -> u64 {
    if n == 0 {
        return 0;
    }
    let mut count = 0;
    for i in 1..=n {
        if n % i == 0 {
            count += 1;
        }
    }
    count
}