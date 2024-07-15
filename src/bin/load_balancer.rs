use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write, Error as IoError};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::collections::HashMap;

enum PooledConnection {
    Idle(TcpStream),
    InUse,
}

struct ConnectionPool {
    connections: HashMap<String, Vec<PooledConnection>>,
}

impl ConnectionPool {

    fn new() -> Self {
        ConnectionPool {
            connections: HashMap::new(),
        }
    }

    fn get_connection(&mut self, server: &str) -> Result<TcpStream, IoError> {
        let connections = self.connections.entry(server.to_string()).or_insert_with(Vec::new);

        let mut i = 0;

        while i < connections.len() {
            if let PooledConnection::Idle(socket) = &mut connections[i] {
                if Self::check_connection_health(socket) {
                    let conn = std::mem::replace(&mut connections[i], PooledConnection::InUse);

                    if let PooledConnection::Idle(socket) = conn {
                        return Ok(socket);
                    } else {
                        // This should never happen, but we need to handle it for completeness
                        unreachable!("Connection state changed unexpectedly");
                    }
                } else {
                    connections.remove(i);
                    continue;
                }
            }
            i += 1;
        }

        // If no available connection, create a new one
        let stream = TcpStream::connect_timeout(&server.parse().unwrap(), Duration::from_secs(5))?;
        connections.push(PooledConnection::InUse);
        Ok(stream)
    }

    fn release_connection(&mut self, server: &str, stream: TcpStream) {
        if let Some(connections) = self.connections.get_mut(server) {
            if let Ok(addr) = stream.peer_addr() {
                if let Some(connection) = connections.iter_mut().find(|c| {
                    if let PooledConnection::InUse = c {
                        true
                    } else {
                        false
                    }
                }) {
                    *connection = PooledConnection::Idle(stream);
                }
            }
        }
    }

    fn check_connection_health(stream: &mut TcpStream) -> bool {
        if stream.set_write_timeout(Some(Duration::from_secs(5))).is_err() {
            return false;
        }
        if stream.set_read_timeout(Some(Duration::from_secs(5))).is_err() {
            return false;
        }

        if stream.write_all(b"GET /health HTTP/1.1\r\n\r\n").is_err() {
            return false;
        }

        let mut response = [0; 1024];
        match stream.read(&mut response) {
            Ok(size) if size > 0 => {
                let response = String::from_utf8_lossy(&response[..size]);
                response.contains("200 OK") && response.contains("OK")
            }
            _ => false,
        }
    }
}

fn main() -> Result<(), IoError> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    println!("Load balancer listening on port 8080");

    let servers = Arc::new(Mutex::new(vec![
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
        "127.0.0.1:8083".to_string(),
    ]));

    let counter = Arc::new(Mutex::new(0));
    let pool = Arc::new(Mutex::new(ConnectionPool::new()));

    for stream in listener.incoming() {
        let stream = stream?;
        let servers = Arc::clone(&servers);
        let counter = Arc::clone(&counter);
        let pool = Arc::clone(&pool);

        thread::spawn(move || {
            if let Err(e) = handle_connection(stream, servers, counter, pool) {
                eprintln!("Error handling connection: {:?}", e);
            }
        });
    }

    Ok(())
}

fn handle_connection(
    mut client_stream: TcpStream,
    servers: Arc<Mutex<Vec<String>>>,
    counter: Arc<Mutex<usize>>,
    pool: Arc<Mutex<ConnectionPool>>
) -> Result<(), IoError> {
    let mut buffer = [0; 1024];
    client_stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let bytes_read = client_stream.read(&mut buffer)?;

    if bytes_read == 0 {
        return Err(IoError::new(std::io::ErrorKind::UnexpectedEof, "Client closed connection"));
    }

    let server = find_available_server(&servers, &counter, &pool);

    match server {
        Some(server_addr) => {
            let mut server_stream = pool.lock().unwrap().get_connection(&server_addr)?;

            server_stream.set_write_timeout(Some(Duration::from_secs(5)))?;
            server_stream.write_all(&buffer[..bytes_read])?;

            let mut response = Vec::new();
            server_stream.set_read_timeout(Some(Duration::from_secs(30)))?;
            server_stream.read_to_end(&mut response)?;

            if response.is_empty() {
                return Err(IoError::new(std::io::ErrorKind::UnexpectedEof, "Empty response from server"));
            }

            client_stream.set_write_timeout(Some(Duration::from_secs(5)))?;
            client_stream.write_all(&response)?;
            client_stream.flush()?;

            // Release the connection back to the pool
            pool.lock().unwrap().release_connection(&server_addr, server_stream);
        }
        None => {
            send_error_response(&mut client_stream, "All servers are currently unavailable")?;
        }
    }

    Ok(())
}

fn find_available_server(
    servers: &Arc<Mutex<Vec<String>>>,
    counter: &Arc<Mutex<usize>>,
    pool: &Arc<Mutex<ConnectionPool>>
) -> Option<String> {
    let servers = servers.lock().unwrap();
    let mut counter = counter.lock().unwrap();
    let mut pool = pool.lock().unwrap();
    let start_index = *counter % servers.len();

    for i in 0..servers.len() {
        let index = (start_index + i) % servers.len();
        let server = &servers[index];

        match pool.get_connection(server) {
            Ok(_) => {
                *counter = index + 1;
                return Some(server.clone());
            }
            Err(e) => {
                eprintln!("Failed to connect to server {}: {:?}", server, e);
            }
        }
    }

    None
}

fn send_error_response(client_stream: &mut TcpStream, message: &str) -> Result<(), IoError> {
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\n{}",
        message
    );
    client_stream.write_all(response.as_bytes())?;
    client_stream.flush()?;
    Ok(())
}