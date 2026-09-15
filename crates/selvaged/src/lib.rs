//! `selvaged` — memory-only reference server for the Selvage Session Protocol.
//!
//! One WebSocket endpoint (`/session`) minting and relaying rooms, plus an HTTP
//! `GET /meta` negotiation endpoint on the same listener. No persistence, no accounts,
//! no file access: the token is the permission and the room dies with its host.

mod net;
pub mod room;

use std::fmt::Write as _;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
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
    /// How long a connection may take to send its HTTP request head. A client that never
    /// finishes one is dropped rather than held open for ever.
    pub head_timeout: Duration,
    /// Keepalive values advertised to clients.
    pub keepalive: Keepalive,
    /// How many rooms the server holds at once. Past it, minting is refused.
    pub max_rooms: usize,
    /// How many peers one room seats at once. Past it, joining is refused — unless the
    /// newcomer reclaims a host-less room as its host, which always seats.
    pub max_peers_per_room: usize,
    /// How many paths one room's open-document set holds at once. Past it, opening a
    /// new path is refused.
    pub max_documents_per_room: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            room_grace: Duration::from_secs(30),
            ping_interval: Duration::from_secs(30),
            hello_timeout: Duration::from_secs(10),
            head_timeout: Duration::from_secs(5),
            keepalive: Keepalive::default(),
            max_rooms: 1024,
            max_peers_per_room: 128,
            max_documents_per_room: 1024,
        }
    }
}

pub struct Server {
    listener: TcpListener,
    /// The address the listener is actually bound to, resolved once at bind time.
    addr: SocketAddr,
    config: ServerConfig,
    registry: Arc<Mutex<Registry>>,
}

impl Server {
    /// Binds the listener. Use port 0 to let the OS pick one, then read
    /// [`Server::local_addr`].
    ///
    /// # Errors
    ///
    /// Returns the bind error, for instance when the address is already in use.
    pub async fn bind(
        addr: SocketAddr,
        config: ServerConfig,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        let registry = Arc::new(Mutex::new(Registry::default()));
        Ok(Self {
            listener,
            addr: bound,
            config,
            registry,
        })
    }

    /// The address the server is listening on.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The `ws://` base URL clients connect to (without the `/session` path).
    #[must_use]
    pub fn ws_base(&self) -> String {
        format!("ws://{}", self.addr)
    }

    #[must_use]
    pub fn http_base(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub async fn run(self) {
        let shared = net::Shared::new(self.config, self.registry);
        net::serve(self.listener, shared).await;
    }
}

pub(crate) fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rng().fill(buf.as_mut_slice());
    let mut out = String::with_capacity(bytes.saturating_mul(2));
    for byte in buf {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub(crate) fn mint_room_id() -> String {
    format!("r-{}", random_hex(6))
}

pub(crate) fn mint_token() -> String {
    random_hex(16)
}
