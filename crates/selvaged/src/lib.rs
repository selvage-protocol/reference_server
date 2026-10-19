//! `selvaged` — memory-only reference server for the Selvage Session Protocol.
//!
//! One WebSocket endpoint (`/session`) minting and relaying rooms, plus an HTTP
//! `GET /meta` endpoint on the same listener. No persistence, no accounts,
//! no file access: the token is the permission and a room lives while it has
//! connections and for the grace period after its last one ends.

pub mod budget;
mod net;
pub mod page;
pub mod room;

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use selvage_protocol::Keepalive;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use room::Registry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// How long a room survives its last connection ending.
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
    /// How many peers one room seats at once. Past it, joining is refused.
    pub max_peers_per_room: usize,
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
    /// JSON writes a `"` or a `\` as two bytes, so a listing written in either
    /// carries half the path bytes it could.
    pub max_envelope_bytes: usize,
    /// The bytes per second one connection may send, refilled continuously. Past it the
    /// connection is told so and ended: the peer's session is over, and its reconnect
    /// starts with a fresh budget.
    pub inbound_bytes_per_sec: usize,
    /// How much of that rate one connection may spend at once. A fresh session starts
    /// with a whole burst, so a newcomer syncing a room is not throttled before it has
    /// sent anything; a flooder that spends it is held to the rate.
    pub inbound_burst_bytes: usize,
    /// Whether to serve a static page.
    pub page_root: Option<PathBuf>,
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
            max_queue_bytes: room::MAX_QUEUE_BYTES,
            // 5 MiB, inside the 8 MiB frame bound so that an over-bound envelope is
            // refused on the frame's own vocabulary and not by ending the connection.
            max_envelope_bytes: 5 * 1024 * 1024,
            // 2 MiB/s sustained, 64 MiB bursting. Above anything an editor does — a
            // keystroke is tens of bytes, a presence update one a quiescent 100 ms —
            // and above the largest legitimate burst, a newcomer's initial sync of a
            // multi-megabyte document.
            inbound_bytes_per_sec: 2 * 1024 * 1024,
            inbound_burst_bytes: 64 * 1024 * 1024,
            page_root: None,
        }
    }
}

impl ServerConfig {
    /// The smallest outbound queue that can hold every frame this configuration can put
    /// in one: the largest frame a peer may relay, and the membership an event names.
    /// A queue below this does not bound memory, it breaks sessions — a frame the queue
    /// refuses is a frame nobody receives, so the handshake that cannot be delivered
    /// seats nothing.
    #[must_use]
    pub const fn smallest_queue_bytes(&self) -> usize {
        // The peers list and the envelope around it.
        const ENVELOPE_HEADROOM: usize = 64 * 1024;
        net::MAX_FRAME_BYTES.saturating_add(ENVELOPE_HEADROOM)
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

/// `bytes` random bytes as lowercase hex.
///
/// The platform's CSPRNG is the one source of a room's token (`PROTOCOL.md` §12),
/// so there is no refusal to build when it cannot be read: a process that has no
/// randomness has nothing to mint a room with.
#[expect(
    clippy::expect_used,
    reason = "a platform with no CSPRNG has no token to mint and no refusal to send"
)]
pub(crate) fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    getrandom::fill(&mut buf).expect("the platform CSPRNG is available");
    let mut out = String::with_capacity(bytes.saturating_mul(2));
    for byte in buf.drain(..) {
        out.push(hex_digit(byte >> 4));
        out.push(hex_digit(byte & 0x0f));
    }
    out
}

/// One lowercase hex digit, for the four bits it names.
fn hex_digit(nibble: u8) -> char {
    char::from_digit(u32::from(nibble), 16).unwrap_or('0')
}

pub(crate) fn mint_room_id() -> String {
    format!("r-{}", random_hex(6))
}

pub(crate) fn mint_token() -> String {
    random_hex(16)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a minted identifier may contain: lowercase hex, and nothing else.
    fn is_lower_hex(text: &str) -> bool {
        text.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    /// A mint is two characters per random byte, drawn from the lowercase hex alphabet.
    #[test]
    fn a_mint_is_lowercase_hex_of_two_characters_a_byte() {
        for bytes in [0, 1, 6, 8, 16] {
            let text = random_hex(bytes);
            assert_eq!(
                text.len(),
                bytes.saturating_mul(2),
                "length of {bytes} bytes"
            );
            assert!(is_lower_hex(&text), "{text:?} is not lowercase hex");
        }
    }

    /// The two identifiers the server mints are the sizes the protocol names.
    #[test]
    fn the_minted_identifiers_are_the_sizes_the_protocol_names() {
        let room = mint_room_id();
        assert_eq!(room.len(), 2 + 12);
        assert!(room.starts_with("r-"));
        assert!(is_lower_hex(room.trim_start_matches("r-")));

        let token = mint_token();
        assert_eq!(token.len(), 32);
        assert!(is_lower_hex(&token));
    }

    /// Two mints of the same size are different identifiers, so the bytes are drawn rather
    /// than left as the buffer's zeros.
    #[test]
    fn two_mints_of_the_same_size_differ() {
        let a = mint_token();
        let b = mint_token();
        assert_ne!(a, b, "two tokens of 16 bytes came out the same");
    }
}
