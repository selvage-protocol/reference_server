//! `selvaged` — memory-only reference server for the Selvage Session Protocol.
//!
//! One WebSocket endpoint (`/session`) minting and relaying rooms, plus an HTTP
//! `GET /meta` negotiation endpoint on the same listener. No persistence, no accounts,
//! no file access: the token is the permission and the room dies with its host.

pub mod budget;
mod net;
pub mod page;
pub mod room;

use std::fmt::Write as _;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use selvage_protocol::Keepalive;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use room::Registry;

#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// How many connections the server holds at once, seated or not. Past it, a new
    /// connection is turned away with a signal — `503` for plain HTTP, a `1013`
    /// close for a WebSocket upgrade — once its head is read. Silence after the
    /// handshake is not policed here: `PROTOCOL.md` §2.1 forbids timing a session
    /// out for inactivity, so a deployment that needs an idle deadline puts it in
    /// front (§12).
    ///
    /// The process needs file descriptors for more than these sockets: keep
    /// `RLIMIT_NOFILE` at `max_connections + 64` or above (listener, stdio and a
    /// margin), or a full server sits in `EMFILE` on its listener. The accept loop
    /// backs off rather than spinning there, but descriptors below the cap still
    /// refuse connections that would otherwise fit.
    pub max_connections: usize,
    /// How many rooms the server holds at once. Past it, minting is refused.
    pub max_rooms: usize,
    /// How many peers one room seats at once. Past it, joining is refused — unless the
    /// newcomer reclaims a host-less room as its host, which always seats.
    pub max_peers_per_room: usize,
    /// How many paths one room's open-document set holds at once. Past it, opening a
    /// new path is refused.
    pub max_documents_per_room: usize,
    /// How many payload bytes one connection may have queued but unwritten before it is
    /// disconnected as a peer that stopped reading. The dominant term in what a full
    /// server can hold, so a small host sizes it rather than its peer count.
    pub max_queue_bytes: usize,
    /// The largest inbound text envelope this server will parse, in bytes. Checked on
    /// the frame's length *before* `serde_json` sees it: parsing megabytes of
    /// attacker-chosen JSON to find out it was too big is the cost this bound removes.
    /// It sits below the transport's frame bound, so an envelope past it is refused
    /// `bad_message` on the frame's own vocabulary with the connection left open, rather
    /// than ending the connection the way an over-bound frame does.
    ///
    /// Its unit is wire bytes, which is not the unit a listing is measured in:
    /// `doc.grant`'s listing is capped in path bytes by
    /// [`net::MAX_GRANT_BYTES`](crate::net), and JSON writes a `"` or a `\` as two
    /// bytes, so a listing written in either carries half the path bytes it could.
    pub max_envelope_bytes: usize,
    /// The bytes per second one connection may send, refilled continuously. Past it the
    /// connection is told so and ended: the peer's session is over, and its reconnect
    /// starts with a fresh budget.
    pub inbound_bytes_per_sec: usize,
    /// How much of that rate one connection may spend at once. A fresh session starts
    /// with a whole burst, so a newcomer syncing a room is not throttled before it has
    /// sent anything; a flooder that spends it is held to the rate.
    pub inbound_burst_bytes: usize,
    /// Whether to seat `selvage/1` alone (`PROTOCOL.md` §10).
    ///
    /// The default seats both versions (`--serve-version-1-only` is what narrows it): a
    /// version-1-only server is the shape the specification's version-1 corpus pins — its
    /// `/meta` advertises one version and a `selvage/2` hello is refused — and the shape a
    /// client that must be shown a refusal is pointed at. A room is pinned to the version
    /// its minting connection speaks either way, so a server that seats both refuses a
    /// connection speaking the other version to a room rather than seating it.
    pub serve_version_1_only: bool,
    /// Serve a static page from this directory on `GET /` and every other plain
    /// path, from the same origin as `/session` and `/meta`. `None` keeps the
    /// server a server alone: an unknown plain path answers `404`.
    pub page_root: Option<PathBuf>,
}

impl ServerConfig {
    /// The wire versions this configuration seats, in the order `/meta` writes them.
    #[must_use]
    pub fn wire_versions(&self) -> Vec<selvage_protocol::Version> {
        if self.serve_version_1_only {
            vec![selvage_protocol::Version::V1]
        } else {
            vec![selvage_protocol::Version::V1, selvage_protocol::Version::V2]
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            room_grace: Duration::from_secs(30),
            ping_interval: Duration::from_secs(30),
            hello_timeout: Duration::from_secs(10),
            head_timeout: Duration::from_secs(5),
            keepalive: Keepalive::default(),
            max_connections: 1024,
            max_rooms: 1024,
            max_peers_per_room: 128,
            max_documents_per_room: 1024,
            max_queue_bytes: room::MAX_QUEUE_BYTES,
            // 5 MiB, inside the 8 MiB frame bound so that an over-bound envelope is
            // refused on the frame's own vocabulary and not by ending the connection.
            // The 4 MiB a full grant's paths may occupy does not always fit here: a path
            // whose bytes all escape wires to twice its length, and a listing heavy in
            // them is refused with the bound named rather than parsed, the same way a
            // listing past the byte budget is.
            max_envelope_bytes: 5 * 1024 * 1024,
            // 2 MiB/s sustained, 64 MiB bursting. Above anything an editor does — a
            // keystroke is tens of bytes, a presence update one a quiescent 100 ms —
            // and above the largest legitimate burst, a newcomer's initial sync of a
            // multi-megabyte document.
            inbound_bytes_per_sec: 2 * 1024 * 1024,
            inbound_burst_bytes: 64 * 1024 * 1024,
            serve_version_1_only: false,
            page_root: None,
        }
    }
}

impl ServerConfig {
    /// The smallest outbound queue that can hold every frame this configuration can put
    /// in one: the largest frame a peer may relay, the room's whole open-document set as
    /// the events echo it, and the whole grant. A queue below this does not bound memory,
    /// it breaks sessions — a frame the queue refuses is a frame nobody receives, so the
    /// handshake that cannot be delivered seats nothing and a host whose own grant is
    /// larger than its queue is dropped by publishing it.
    ///
    /// What is counted is the frame, and a frame is not measured in paths. JSON writes a
    /// `"` or a `\` as two bytes, and a path carries no control character
    /// (`PROTOCOL.md` §5), so a legal path wires to at most twice its length and a set of
    /// them to twice its path bytes. A grant cannot reach that: a host publishes one as a
    /// single `doc.grant`, so the largest listing a room can hold is one whose own frame
    /// fit the envelope bound, and the event echoing it is the same listing written
    /// again.
    ///
    /// [`ServerConfig::default`] clears this; the arithmetic is what a deployment lowers
    /// `--max-documents-per-room` for, since the document set is echoed whole to every
    /// peer on every change.
    #[must_use]
    pub fn smallest_queue_bytes(&self) -> usize {
        // The peers list, the path a `doc.*` event names and the envelope around them.
        const ENVELOPE_HEADROOM: usize = 64 * 1024;
        // The most JSON adds to one byte: `"` and `\` are written as two, and every
        // other byte a path may carry is written as one.
        const MAX_ESCAPE: usize = 2;
        let documents = self
            .max_documents_per_room
            .saturating_mul(net::MAX_DOC_PATH_BYTES)
            .saturating_mul(MAX_ESCAPE)
            .saturating_add(ENVELOPE_HEADROOM);
        // A listing is bounded in path bytes and, being one frame, in wire bytes; the
        // smaller of the two is all the room can hold and therefore all it can echo.
        let grant = net::MAX_GRANT_BYTES
            .saturating_mul(MAX_ESCAPE)
            .min(net::MAX_FRAME_BYTES.min(self.max_envelope_bytes))
            .saturating_add(ENVELOPE_HEADROOM);
        net::MAX_FRAME_BYTES.max(documents).max(grant)
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
