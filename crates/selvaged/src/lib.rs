//! `selvaged` — memory-only reference server for the Selvage Session Protocol.
//!
//! One WebSocket endpoint (`/session`) minting and relaying rooms, plus an HTTP
//! `GET /meta` negotiation endpoint on the same listener. No persistence, no accounts,
//! no file access: the token is the permission and the room dies with its host.

mod net;
pub mod room;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use selvage_protocol::Keepalive;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use room::Registry;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// How long a room survives its host disconnecting.
    pub room_grace: Duration,
    /// WebSocket ping interval (protocol-level keepalive).
    pub ping_interval: Duration,
    /// How long a connection may stay silent before sending `session.hello`.
    pub hello_timeout: Duration,
    /// Keepalive values advertised to clients.
    pub keepalive: Keepalive,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            room_grace: Duration::from_secs(30),
            ping_interval: Duration::from_secs(30),
            hello_timeout: Duration::from_secs(10),
            keepalive: Keepalive::default(),
        }
    }
}

pub struct Server {
    listener: TcpListener,
    config: ServerConfig,
    registry: Arc<Mutex<Registry>>,
}

impl Server {
    /// Binds the listener. Use port 0 to let the OS pick one, then read
    /// [`Server::local_addr`].
    pub async fn bind(addr: SocketAddr, config: ServerConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            config,
            registry: Arc::new(Mutex::new(Registry::new())),
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// The `ws://` base URL clients connect to (without the `/session` path).
    pub fn ws_base(&self) -> String {
        format!("ws://{}", self.listener.local_addr().expect("bound listener"))
    }

    pub fn http_base(&self) -> String {
        format!("http://{}", self.listener.local_addr().expect("bound listener"))
    }

    pub async fn run(self) {
        net::serve(self.listener, self.config, self.registry).await;
    }
}

pub(crate) fn random_hex(bytes: usize) -> String {
    use rand::RngExt;
    let mut buf = vec![0u8; bytes];
    rand::rng().fill(&mut buf[..]);
    let mut out = String::with_capacity(bytes * 2);
    for b in buf {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

pub(crate) fn mint_room_id() -> String {
    format!("r-{}", random_hex(6))
}

pub(crate) fn mint_token() -> String {
    random_hex(16)
}
