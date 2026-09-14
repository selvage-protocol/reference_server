use std::env;
use std::io;
use std::net::SocketAddr;
use std::process::exit;
use std::time::Duration;

use selvaged::{Server, ServerConfig};

const DEFAULT_ADDRESS: &str = "127.0.0.1:8080";
const USAGE: &str = "usage: selvaged [--listen ADDR] [--room-grace-ms MS]";

#[tokio::main]
async fn main() -> io::Result<()> {
    let (addr, room_grace) = match arguments() {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}");
            exit(2);
        }
    };

    let config = ServerConfig {
        room_grace,
        ..ServerConfig::default()
    };
    let server = Server::bind(addr, config).await?;
    let local = server.local_addr();
    println!(
        "selvaged listening on ws://{local}/session (meta at http://{local}/meta)"
    );
    println!(
        "rooms are destroyed {}ms after their host disconnects",
        room_grace.as_millis()
    );
    server.run().await;
    Ok(())
}

/// Reads `--listen ADDR` and `--room-grace-ms MS`, which default to localhost and the
/// reference grace period. A vector that depends on the grace period is replayed by a
/// process that can set it.
fn arguments() -> Result<(SocketAddr, Duration), String> {
    let mut addr: SocketAddr = DEFAULT_ADDRESS
        .parse()
        .map_err(|e| format!("bad default address: {e}"))?;
    let mut room_grace = ServerConfig::default().room_grace;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                let value = args
                    .next()
                    .ok_or("--listen wants an address, e.g. 127.0.0.1:8080")?;
                addr = value.parse().map_err(|e| {
                    format!("--listen wants an address, e.g. 127.0.0.1:8080\n{e}")
                })?;
            }
            "--room-grace-ms" => {
                let value = args
                    .next()
                    .ok_or("--room-grace-ms wants a number of milliseconds")?;
                let ms: u64 = value.parse().map_err(|e| {
                    format!("--room-grace-ms wants a number of milliseconds\n{e}")
                })?;
                room_grace = Duration::from_millis(ms);
            }
            _ => return Err(format!("{USAGE}\nunknown argument: {arg}")),
        }
    }
    Ok((addr, room_grace))
}
