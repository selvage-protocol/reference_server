use std::net::SocketAddr;

use selvaged::{Server, ServerConfig};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut addr: SocketAddr = "127.0.0.1:8080".parse().expect("valid default address");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                addr = args
                    .next()
                    .and_then(|a| a.parse().ok())
                    .unwrap_or_else(|| {
                        eprintln!("--listen wants an address, e.g. 127.0.0.1:8080");
                        std::process::exit(2)
                    });
            }
            other => {
                eprintln!("usage: selvaged [--listen ADDR]\nunknown argument: {other}");
                std::process::exit(2);
            }
        }
    }

    let config = ServerConfig::default();
    let server = Server::bind(addr, config.clone()).await?;
    let local = server.local_addr()?;
    println!("selvaged listening on ws://{local}/session (meta at http://{local}/meta)");
    println!(
        "rooms are destroyed {}s after their host disconnects",
        config.room_grace.as_secs()
    );
    server.run().await;
    Ok(())
}
