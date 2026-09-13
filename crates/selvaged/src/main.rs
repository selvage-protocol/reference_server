use std::env;
use std::io;
use std::net::SocketAddr;
use std::process::exit;

use selvaged::{Server, ServerConfig};

const DEFAULT_ADDRESS: &str = "127.0.0.1:8080";

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr = match listen_address() {
        Ok(addr) => addr,
        Err(message) => {
            eprintln!("{message}");
            exit(2);
        }
    };

    let config = ServerConfig::default();
    let server = Server::bind(addr, config.clone()).await?;
    let local = server.local_addr();
    println!(
        "selvaged listening on ws://{local}/session (meta at http://{local}/meta)"
    );
    println!(
        "rooms are destroyed {}s after their host disconnects",
        config.room_grace.as_secs()
    );
    server.run().await;
    Ok(())
}

/// Reads `--listen ADDR`, which defaults to localhost.
fn listen_address() -> Result<SocketAddr, String> {
    let mut addr: SocketAddr = DEFAULT_ADDRESS
        .parse()
        .map_err(|e| format!("bad default address: {e}"))?;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg != "--listen" {
            return Err(format!(
                "usage: selvaged [--listen ADDR]\nunknown argument: {arg}"
            ));
        }
        let Some(value) = args.next() else {
            return Err(
                "--listen wants an address, e.g. 127.0.0.1:8080".to_string()
            );
        };
        addr = value.parse().map_err(|e| {
            format!("--listen wants an address, e.g. 127.0.0.1:8080\n{e}")
        })?;
    }
    Ok(addr)
}
