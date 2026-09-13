//! Listener, `GET /meta`, the WebSocket upgrade and the per-connection task.
//!
//! The server is session-aware and payload-opaque: it parses the JSON session envelope,
//! owns rooms, membership and the open-document set, but never decodes a document or
//! awareness payload — binary frames are routed to the rest of the room untouched.

use std::io;
use std::mem::take;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{
    UnboundedReceiver, UnboundedSender, unbounded_channel,
};
use tokio::task::JoinHandle;
use tokio::time::{Interval, MissedTickBehavior, interval};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;

use selvage_protocol as proto;
use selvage_protocol::event;

use crate::room::{Outbound, Registry};
use crate::{ServerConfig, random_hex};

mod session;

use session::{Applicant, Session, handshake};

const MAX_HEAD_BYTES: usize = 16 * 1024;
const NOT_FOUND: &str = r#"{"error":"not found"}"#;

/// A WebSocket session from a client, upgraded off a plain TCP stream.
pub type SessionSocket =
    tokio_tungstenite::WebSocketStream<PrefixedStream<TcpStream>>;
pub type SessionSink = SplitSink<SessionSocket, Message>;
pub type SessionStream = SplitStream<SessionSocket>;

/// Everything a connection needs from the server: its configuration and its rooms.
#[derive(Clone)]
pub struct Shared {
    pub config: ServerConfig,
    pub registry: Arc<Mutex<Registry>>,
}

impl Shared {
    #[must_use]
    pub const fn new(
        config: ServerConfig,
        registry: Arc<Mutex<Registry>>,
    ) -> Self {
        Self { config, registry }
    }
}

/// Serves connections until the listener itself fails.
pub async fn serve(listener: TcpListener, shared: Shared) {
    loop {
        let Ok((tcp, _addr)) = listener.accept().await else {
            continue;
        };
        let connection = shared.clone();
        tokio::spawn(connection.accept(tcp));
    }
}

impl Shared {
    /// Reads the request head and either answers it in place or upgrades to a session.
    async fn accept(self, mut tcp: TcpStream) -> io::Result<()> {
        let Some(mut head) = read_http_head(&mut tcp).await? else {
            return Ok(());
        };
        if !head.is_websocket_upgrade {
            return respond_plain(&mut tcp, &head).await;
        }
        if head.path != proto::ENDPOINT_PATH {
            return respond_status(&mut tcp, Status::NotFound, NOT_FOUND).await;
        }
        // The head we consumed while routing has to go back in front of the socket.
        let prefixed = PrefixedStream::new(take(&mut head.raw), tcp);
        let Ok(ws) = tokio_tungstenite::accept_async(prefixed).await else {
            return Ok(());
        };
        self.serve_session(ws, proto::parse_join_query(&head.query))
            .await;
        Ok(())
    }

    /// Runs one connection: handshake, seat, then relay frames until it ends.
    async fn serve_session(&self, ws: SessionSocket, join: proto::JoinQuery) {
        let mut wire = Wire::new(ws);
        let claims_host = join.room.is_none();
        let greeted =
            handshake(&mut wire.stream, claims_host, self.config.hello_timeout)
                .await;
        let seated = match greeted {
            Ok(hello) => {
                let applicant = Applicant {
                    peer_id: format!("p-{}", random_hex(8)),
                    join,
                    hello,
                    tx: wire.tx.clone(),
                };
                applicant.seat(&self.registry, &self.config).await
            }
            Err(refusal) => Err(refusal),
        };
        match seated {
            Ok(session) => self.drive(session, wire).await,
            Err((code, message)) => refuse(wire, code, message).await,
        }
    }

    /// Runs a seated session until the connection ends, then lets the peers know.
    async fn drive(&self, session: Session, wire: Wire) {
        let mut ping = interval(self.config.ping_interval);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ping.tick().await;

        let mut live = Live {
            session,
            wire,
            ping,
        };
        pump(&mut live, self).await;
        live.session.leave(self).await;
        // The registry held a sender for this peer too; `leave` dropped it, so this is
        // the last one and the writer loop can finish.
        drop(live.wire.tx);
        let _ = live.wire.writer.await;
    }
}

/// The transport half of a connection: what is read, what is queued and who drains it.
pub struct Wire {
    pub stream: SessionStream,
    pub tx: UnboundedSender<Outbound>,
    writer: JoinHandle<()>,
}

impl Wire {
    fn new(ws: SessionSocket) -> Self {
        let (sink, stream) = ws.split();
        let (tx, rx) = unbounded_channel::<Outbound>();
        Self {
            stream,
            tx,
            writer: tokio::spawn(write_outbound(sink, rx)),
        }
    }
}

/// A seated session plus the plumbing that keeps it alive.
struct Live {
    session: Session,
    wire: Wire,
    ping: Interval,
}

/// The turn loop: protocol pings, session methods and inbound frames.
async fn pump(live: &mut Live, shared: &Shared) {
    loop {
        let keep_going = tokio::select! {
            _ = live.ping.tick() => live.heartbeat(),
            _ = &mut live.wire.writer => false,
            incoming = live.wire.stream.next() => live.session.handle_frame(incoming, shared).await,
        };
        if !keep_going {
            break;
        }
    }
}

impl Live {
    /// Sends one protocol-level ping. The keepalive never ends the session.
    fn heartbeat(&self) -> bool {
        let _ = self.wire.tx.send(Outbound::Ping(Vec::new()));
        true
    }
}

/// Writes queued frames until the last sender is dropped or the socket fails.
async fn write_outbound(
    mut sink: SessionSink,
    mut rx: UnboundedReceiver<Outbound>,
) {
    while let Some(out) = rx.recv().await {
        let closing = matches!(out, Outbound::Close(..));
        if sink.send(frame_of_outbound(out)).await.is_err() {
            return;
        }
        if closing {
            let _ = sink.close().await;
            return;
        }
    }
}

/// Tells a connection why it was refused, then closes it.
async fn refuse(wire: Wire, code: &'static str, message: String) {
    let event = proto::ServerMessage::event(
        event::SESSION_ERROR,
        serde_json::json!({ "code": code, "message": message }),
    );
    if let Some(frame) = frame_of(&event) {
        let _ = wire.tx.send(frame);
    }
    let _ = wire
        .tx
        .send(Outbound::Close(proto::close_code_for(code), message));
    drop(wire.tx);
    let _ = wire.writer.await;
}

/// Serializes a server message into an outbound text frame.
fn frame_of(msg: &proto::ServerMessage) -> Option<Outbound> {
    msg.to_text().ok().map(Outbound::Text)
}

/// Builds an event frame ready to write.
pub fn event_frame(name: &str, params: Value) -> Option<Outbound> {
    frame_of(&proto::ServerMessage::event(name, params))
}

fn frame_of_outbound(out: Outbound) -> Message {
    match out {
        Outbound::Text(t) => Message::text(t),
        Outbound::Binary(b) => Message::binary(b),
        Outbound::Ping(b) => Message::Ping(b.into()),
        Outbound::Close(code, reason) => Message::Close(Some(CloseFrame {
            code: code.into(),
            reason: reason.into(),
        })),
    }
}

// --- HTTP ---------------------------------------------------------------------

/// The statuses this server answers plain HTTP requests with.
#[derive(Debug, Clone, Copy)]
enum Status {
    Ok,
    NotFound,
}

impl Status {
    const fn code(self) -> u16 {
        match self {
            Self::Ok => 200,
            Self::NotFound => 404,
        }
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::NotFound => "Not Found",
        }
    }
}

struct Head {
    raw: Vec<u8>,
    path: String,
    query: String,
    is_websocket_upgrade: bool,
}

async fn read_http_head(tcp: &mut TcpStream) -> io::Result<Option<Head>> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let end = loop {
        let n = tcp.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        let Some(part) = chunk.get(..n) else {
            return Ok(None);
        };
        buf.extend_from_slice(part);
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos.saturating_add(4);
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Ok(None);
        }
    };

    let Some(raw) = buf.get(..end) else {
        return Ok(None);
    };
    let head_text = String::from_utf8_lossy(raw).into_owned();
    let request_line = head_text.lines().next().unwrap_or_default();
    let target = request_line.split_whitespace().nth(1).unwrap_or_default();
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };

    Ok(Some(Head {
        raw: raw.to_vec(),
        path: path.to_string(),
        query: query.to_string(),
        is_websocket_upgrade: head_text
            .to_ascii_lowercase()
            .contains("upgrade: websocket"),
    }))
}

async fn respond_plain(tcp: &mut TcpStream, head: &Head) -> io::Result<()> {
    if head.path != proto::META_PATH {
        return respond_status(tcp, Status::NotFound, NOT_FOUND).await;
    }
    let meta = serde_json::to_string_pretty(&proto::Meta::reference())
        .map_err(io::Error::other)?;
    respond_status(tcp, Status::Ok, &meta).await
}

async fn respond_status(
    tcp: &mut TcpStream,
    status: Status,
    body: &str,
) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        status.code(),
        status.reason(),
        body.len()
    );
    tcp.write_all(response.as_bytes()).await?;
    tcp.shutdown().await
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// A socket with the already-read HTTP head pushed back in front of it, so the
/// WebSocket upgrade sees the request bytes we consumed while routing.
pub struct PrefixedStream<T> {
    prefix: Vec<u8>,
    pos: usize,
    inner: T,
}

impl<T> PrefixedStream<T> {
    const fn new(prefix: Vec<u8>, inner: T) -> Self {
        Self {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for PrefixedStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(remaining) = this.prefix.get(this.pos..) else {
            return Poll::Ready(Ok(()));
        };
        if remaining.is_empty() {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        }
        let n = remaining.len().min(buf.remaining());
        let Some(chunk) = remaining.get(..n) else {
            return Poll::Ready(Ok(()));
        };
        buf.put_slice(chunk);
        this.pos = this.pos.saturating_add(n);
        Poll::Ready(Ok(()))
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for PrefixedStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
