//! Listener, `GET /meta`, the WebSocket upgrade and the per-connection task.
//!
//! The server is session-aware and payload-opaque: it parses the JSON session envelope,
//! owns rooms, membership and the open-document set, but never decodes a document or
//! awareness payload — binary frames are routed to the rest of the room untouched.

use std::fmt::Write as _;
use std::io;
use std::mem::take;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Interval, MissedTickBehavior, interval, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};

use selvage_protocol as proto;
use selvage_protocol::event;

use crate::room::{Outbound, Queue, Registry};
use crate::{ServerConfig, random_hex};

mod session;

use session::{Applicant, Session, handshake};

const MAX_HEAD_BYTES: usize = 16 * 1024;
const NOT_FOUND: &str = r#"{"error":"not found","hint":"try /session (WebSocket) or /meta (HTTP)"}"#;
const METHOD_NOT_ALLOWED: &str = r#"{"error":"method not allowed","hint":"GET answers, HEAD answers headers-only"}"#;
const SERVER_FULL_BODY: &str =
    r#"{"error":"server full","hint":"try again later"}"#;
const HEAD_TOO_LARGE_BODY: &str = r#"{"error":"request head too large","hint":"send a shorter request head"}"#;

/// The most one inbound WebSocket frame or message may carry, well under the
/// library's 64 MiB default. A frame over the bound is a transport failure, not a
/// session fault: the connection ends the way a dropped socket ends, and the room
/// learns of it as `peer.left` (`PROTOCOL.md` §2.1). Ending rather than refusing is
/// structural, not policy: past the bound the transport cannot resync mid-message,
/// so there is no session left to refuse on.
///
/// 8 MiB clears measured real use with headroom: a single 4 MiB insert encodes to
/// 4,194,338 wired bytes (update bytes track text bytes one-for-one plus ~34 B),
/// and a tombstone-heavy document (9 KB live after 2000 inserts with 90% deleted)
/// encodes to 34,663 wired bytes, ~3.9× its live text — so the bound clears bare
/// pastes to ~8 MiB and history-amplified documents to a few megabytes live. Shapes
/// measured in `crates/harness/tests/bounds.rs`, clearance pinned in
/// `crates/harness/tests/session.rs`.
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// RFC 6455 allows 125 bytes in a control-frame payload, and a close frame spends two of
/// them on its status code.
const MAX_CLOSE_REASON: usize = 123;

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
    /// How many connections the server holds right now, seated or not. Past the
    /// configured cap a new connection is turned away with a signal — `503` for
    /// plain HTTP, a `1013` close for a WebSocket upgrade — rather than silence.
    pub connections: Arc<AtomicUsize>,
}

impl Shared {
    #[must_use]
    pub fn new(config: ServerConfig, registry: Arc<Mutex<Registry>>) -> Self {
        Self {
            config,
            registry,
            connections: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Serves connections until the listener itself fails. Each accepted socket gets
/// its own task; past the configured cap a connection is turned away with a
/// signal (`503` for plain HTTP, a `1013` close for a WebSocket upgrade) rather
/// than silence, once its head is read. The count covers handshakes as well as
/// seats, so half-open sockets cannot pile up past it.
pub async fn serve(listener: TcpListener, shared: Shared) {
    let mut errors: u32 = 0;
    loop {
        let tcp = if let Ok(tcp) = accept_nodelay(&listener).await {
            errors = 0;
            tcp
        } else {
            // A failing listener — `EMFILE` at the connection cap — must not
            // hot-spin: rest with a capped backoff before retrying.
            errors = errors.saturating_add(1);
            sleep(accept_backoff(errors)).await;
            continue;
        };
        let connection = shared.clone();
        tokio::spawn(async move {
            let _ = connection.accept(tcp).await;
        });
    }
}

/// How long the accept loop rests after `errors` consecutive listener failures:
/// 50 ms, doubling to a cap of 800 ms. A full server still reclaims a freed slot
/// within a tick, without burning a core while there is none.
const fn accept_backoff(errors: u32) -> Duration {
    let millis: u64 = match errors {
        0 | 1 => 50,
        2 => 100,
        3 => 200,
        4 => 400,
        _ => 800,
    };
    Duration::from_millis(millis)
}

/// A held connection slot: admitted past the head, released when the connection
/// ends. Releasing on drop keeps every early return after admission honest.
struct Slot {
    count: Arc<AtomicUsize>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Accepts one connection with Nagle's algorithm off.
///
/// A reply and the event that follows it are two small writes back to back — `doc.open`'s
/// result and its `doc.opened` — and so is a burst of relayed frames. Nagle holds the second
/// write until the first is acknowledged: latency the protocol earns nothing for, since its
/// frames are whole messages that cannot usefully coalesce. Loopback acknowledges too quickly
/// for it to show; a wide-area link does not.
async fn accept_nodelay(listener: &TcpListener) -> io::Result<TcpStream> {
    let (tcp, _addr) = listener.accept().await?;
    tcp.set_nodelay(true)?;
    Ok(tcp)
}

impl Shared {
    /// Reads the request head and either answers it in place or upgrades to a session.
    async fn accept(self, mut tcp: TcpStream) -> io::Result<()> {
        // A half-sent head must not hold a connection open for ever.
        let reading =
            timeout(self.config.head_timeout, read_http_head(&mut tcp));
        let Ok(request) = reading.await else {
            return Ok(());
        };
        let mut head = match request? {
            // The client went away before finishing: nothing to answer.
            HeadRead::Gone => return Ok(()),
            // Past the head bound with no blank line: the request is unanswerable,
            // and the method never parsed, so the answer carries a body.
            HeadRead::TooLarge => {
                return respond_status(
                    &mut tcp,
                    Status::RequestHeaderFieldsTooLarge,
                    HEAD_TOO_LARGE_BODY,
                    "GET",
                )
                .await;
            }
            HeadRead::Ready(head) => head,
        };
        // Admitted past the head: the count covers handshakes as well as seats, so
        // half-open sockets cannot pile up past it, and a refused connection hears
        // why instead of silence.
        if self.connections.fetch_add(1, Ordering::SeqCst)
            >= self.config.max_connections
        {
            self.connections.fetch_sub(1, Ordering::SeqCst);
            return self.refuse_full(tcp, head).await;
        }
        let _slot = Slot {
            count: Arc::clone(&self.connections),
        };
        // The WebSocket handshake is a `GET` (RFC 6455 §4.1): anything else, upgrade
        // headers or not, is a plain request and answered as one.
        if !head.is_websocket_upgrade || head.method != "GET" {
            return respond_plain(&mut tcp, &head).await;
        }
        if head.path != proto::ENDPOINT_PATH {
            return respond_status(
                &mut tcp,
                Status::NotFound,
                NOT_FOUND,
                &head.method,
            )
            .await;
        }
        // The head we consumed while routing has to go back in front of the socket.
        let prefixed = PrefixedStream::new(take(&mut head.request), tcp);
        let framing = WebSocketConfig::default()
            .max_message_size(Some(MAX_FRAME_BYTES))
            .max_frame_size(Some(MAX_FRAME_BYTES));
        let Ok(mut ws) = tokio_tungstenite::accept_async_with_config(
            prefixed,
            Some(framing),
        )
        .await
        else {
            return Ok(());
        };
        // The upgrade refuses a request with anything behind it, so bytes that arrived
        // in the same read as the head wait here until the frame parser asks for them.
        ws.get_mut().push_back(take(&mut head.tail));
        ws.get_mut().drop_head();
        self.serve_session(ws, proto::parse_join_query(&head.query))
            .await;
        Ok(())
    }

    /// Turns a connection away past the cap, with a signal instead of silence:
    /// plain HTTP gets `503` plus `retry-after`; a WebSocket upgrade gets its
    /// handshake answered and a `1013` close. A reconnect storm can tell "full"
    /// from "dead" either way.
    async fn refuse_full(
        self,
        mut tcp: TcpStream,
        head: Head,
    ) -> io::Result<()> {
        let upgrade = head.method == "GET"
            && head.is_websocket_upgrade
            && head.path == proto::ENDPOINT_PATH;
        if !upgrade {
            return respond_status(
                &mut tcp,
                Status::ServiceUnavailable,
                SERVER_FULL_BODY,
                &head.method,
            )
            .await;
        }
        let prefixed = PrefixedStream::new(head.request, tcp);
        let framing = WebSocketConfig::default()
            .max_message_size(Some(MAX_FRAME_BYTES))
            .max_frame_size(Some(MAX_FRAME_BYTES));
        let Ok(mut ws) = tokio_tungstenite::accept_async_with_config(
            prefixed,
            Some(framing),
        )
        .await
        else {
            return Ok(());
        };
        ws.get_mut().push_back(head.tail);
        // 1013: try again later. The reason fits a control frame many times over.
        let _ = ws
            .send(Message::Close(Some(CloseFrame {
                code: 1013u16.into(),
                reason: "server full, try again later".to_string().into(),
            })))
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
                let (poison_tx, poison_rx) = oneshot::channel();
                let applicant = Applicant {
                    peer_id: format!("p-{}", random_hex(8)),
                    join,
                    hello,
                    queue: wire.queue.clone(),
                    poison: poison_tx,
                };
                applicant
                    .seat(self)
                    .await
                    .map(|session| (session, poison_rx))
            }
            Err(refusal) => Err(refusal),
        };
        match seated {
            Ok((session, poison)) => self.drive(session, wire, poison).await,
            Err((code, message)) => refuse(wire, code, message).await,
        }
    }

    /// Runs a seated session until the connection ends, then lets the peers know.
    #[expect(
        clippy::too_many_arguments,
        reason = "a drive names its session, transport and poison channel; all three move into the turn loop"
    )]
    async fn drive(
        &self,
        session: Session,
        wire: Wire,
        poison: oneshot::Receiver<()>,
    ) {
        let mut ping = interval(self.config.ping_interval);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ping.tick().await;

        let mut live = Live {
            session,
            wire,
            ping,
            poison,
        };
        pump(&mut live, self).await;
        live.session.leave(self).await;
        // The registry held a sender for this peer too; `leave` dropped it, so this is
        // the last one and the writer loop can finish.
        let Wire {
            queue, mut writer, ..
        } = live.wire;
        drop(queue);
        join_writer(&mut writer).await;
    }
}

/// The transport half of a connection: what is read, what is queued and who drains it.
pub struct Wire {
    pub stream: SessionStream,
    pub queue: Queue,
    writer: JoinHandle<()>,
}

impl Wire {
    fn new(ws: SessionSocket) -> Self {
        let (sink, stream) = ws.split();
        let (queue, rx) = Queue::channel();
        let queued = queue.queued_counter();
        Self {
            stream,
            queue,
            writer: tokio::spawn(write_outbound(sink, rx, queued)),
        }
    }
}

/// How long a connection's writer may take to drain once the session is over. A peer
/// that stopped reading holds the writer in its send; the frames it would have written
/// were already dropped with the queue, so the wait is bounded and the socket goes
/// with the writer.
const WRITER_GRACE: Duration = Duration::from_secs(2);

/// Waits for a writer task, unless the turn loop already observed it finish: a
/// `JoinHandle` whose output the loop has taken panics when it is polled again. The
/// wait is bounded: a writer stuck on a peer that stopped reading is stopped, and the
/// socket with it.
async fn join_writer(writer: &mut JoinHandle<()>) {
    if writer.is_finished() {
        return;
    }
    if timeout(WRITER_GRACE, &mut *writer).await.is_err() {
        writer.abort();
    }
}

/// A seated session plus the plumbing that keeps it alive.
struct Live {
    session: Session,
    wire: Wire,
    ping: Interval,
    /// Ends the turn loop when the registry drops its sender: the peer was removed,
    /// so there is nothing left to serve.
    poison: oneshot::Receiver<()>,
}

/// The turn loop: protocol pings, session methods and inbound frames.
async fn pump(live: &mut Live, shared: &Shared) {
    loop {
        let keep_going = tokio::select! {
            _ = live.ping.tick() => {
                live.heartbeat();
                true
            }
            _ = &mut live.wire.writer => false,
            _ = &mut live.poison => false,
            incoming = live.wire.stream.next() => live.session.handle_frame(incoming, shared).await,
        };
        if !keep_going {
            break;
        }
    }
}

impl Live {
    /// Sends one protocol-level ping. The keepalive never ends the session.
    fn heartbeat(&self) {
        let _ = self.wire.queue.try_queue(Outbound::Ping(Vec::new()));
    }
}

/// Writes queued frames until the last sender is dropped or the socket fails. Each
/// frame's bytes are released after its send completes — written or not — so the byte
/// count tracks what is still held for the peer.
async fn write_outbound(
    mut sink: SessionSink,
    mut rx: Receiver<Outbound>,
    queued: Arc<AtomicUsize>,
) {
    while let Some(out) = rx.recv().await {
        let len = out.payload_len();
        let closing = matches!(out, Outbound::Close(..));
        if sink.send(frame_of_outbound(out)).await.is_err() {
            queued.fetch_sub(len, Ordering::Relaxed);
            return;
        }
        queued.fetch_sub(len, Ordering::Relaxed);
        if closing {
            let _ = sink.close().await;
            return;
        }
    }
}

/// Tells a connection why it was refused, then closes it. The writer drains under
/// the same grace as a seated session: a client that never reads must not hold the
/// connection slot past it.
async fn refuse(wire: Wire, code: &'static str, message: String) {
    let event = proto::ServerMessage::event(
        event::SESSION_ERROR,
        serde_json::json!({ "code": code, "message": message }),
    );
    if let Some(frame) = frame_of(&event) {
        let _ = wire.queue.try_queue(frame);
    }
    let _ = wire
        .queue
        .try_queue(Outbound::Close(proto::close_code_for(code), message));
    drop(wire.queue);
    let mut writer = wire.writer;
    join_writer(&mut writer).await;
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
            reason: truncate_reason(reason).into(),
        })),
    }
}

/// Cuts a close reason down to what a control frame can carry. Reasons are built from
/// client input, and a client is owed the close code even when its own input cannot be
/// quoted back in full.
fn truncate_reason(mut reason: String) -> String {
    if reason.len() <= MAX_CLOSE_REASON {
        return reason;
    }
    let mut end = MAX_CLOSE_REASON;
    while !reason.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    reason.truncate(end);
    reason
}

// --- HTTP ---------------------------------------------------------------------

/// The statuses this server answers plain HTTP requests with.
#[derive(Debug, Clone, Copy)]
enum Status {
    Ok,
    NotFound,
    MethodNotAllowed,
    ServiceUnavailable,
    RequestHeaderFieldsTooLarge,
}

impl Status {
    const fn code(self) -> u16 {
        match self {
            Self::Ok => 200,
            Self::NotFound => 404,
            Self::MethodNotAllowed => 405,
            Self::ServiceUnavailable => 503,
            Self::RequestHeaderFieldsTooLarge => 431,
        }
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::NotFound => "Not Found",
            Self::MethodNotAllowed => "Method Not Allowed",
            Self::ServiceUnavailable => "Service Unavailable",
            Self::RequestHeaderFieldsTooLarge => {
                "Request Header Fields Too Large"
            }
        }
    }

    /// `retry-after` seconds for the statuses that ask the client to come back.
    const fn retry_after_secs(self) -> Option<u64> {
        match self {
            Self::ServiceUnavailable => Some(1),
            Self::Ok
            | Self::NotFound
            | Self::MethodNotAllowed
            | Self::RequestHeaderFieldsTooLarge => None,
        }
    }

    /// The methods a refusal takes: `405` names what the server implements.
    const fn allow(self) -> Option<&'static str> {
        match self {
            Self::MethodNotAllowed => Some("GET, HEAD"),
            Self::Ok
            | Self::NotFound
            | Self::ServiceUnavailable
            | Self::RequestHeaderFieldsTooLarge => None,
        }
    }
}

struct Head {
    /// The request head, up to and including the blank line that ends it.
    request: Vec<u8>,
    /// Whatever the same read returned behind the head, which a client that pipelines
    /// its handshake puts there.
    tail: Vec<u8>,
    /// The request method, verbatim: only `GET` answers with a body.
    method: String,
    path: String,
    query: String,
    is_websocket_upgrade: bool,
}

/// What reading the request head produced: the head itself, the client going
/// away before finishing, or a head that ran past the bound — no blank line in
/// the first bound bytes, or the blank line ending past it. The last two look
/// alike on the socket, but only the first is silence: an oversize head is
/// refused `431`.
enum HeadRead {
    Ready(Head),
    Gone,
    TooLarge,
}

async fn read_http_head(tcp: &mut TcpStream) -> io::Result<HeadRead> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let end = loop {
        let n = tcp.read(&mut chunk).await?;
        if n == 0 {
            return Ok(HeadRead::Gone);
        }
        let Some(part) = chunk.get(..n) else {
            return Ok(HeadRead::Gone);
        };
        buf.extend_from_slice(part);
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos.saturating_add(4);
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Ok(HeadRead::TooLarge);
        }
    };
    // The terminator was found, but past the bound its head is still oversize:
    // the bound limits the head, not just the hunt for its end. An end exactly
    // at the limit is accepted.
    if end > MAX_HEAD_BYTES {
        return Ok(HeadRead::TooLarge);
    }

    let tail = buf.split_off(end);
    let head_text = String::from_utf8_lossy(&buf).into_owned();
    let request_line = head_text.lines().next().unwrap_or_default();
    let mut words = request_line.split_whitespace();
    let method = words.next().unwrap_or_default();
    let target = words.next().unwrap_or_default();
    let origin = origin_form(target);
    let (raw_path, query) = match origin.split_once('?') {
        Some((path, query)) => (path, query),
        None => (origin, ""),
    };
    // An absolute URI with no path carries an empty one; `?` alone still split.
    let path = if raw_path.is_empty() { "/" } else { raw_path };

    Ok(HeadRead::Ready(Head {
        // Everything the read returned, split where the head ends: bytes behind it are
        // frames a client pipelined, and they still have to reach the frame parser.
        request: buf,
        tail,
        method: method.to_string(),
        path: path.to_string(),
        query: query.to_string(),
        is_websocket_upgrade: head_text
            .to_ascii_lowercase()
            .contains("upgrade: websocket"),
    }))
}

/// Strips an absolute-form request target down to its origin form: a proxy
/// forwarding a `GET` with an absolute URI is legal HTTP, and §12 puts one in
/// front of any public deployment, so comparing the target verbatim 404s it.
/// A target with no path keeps its query (a query-only absolute target still
/// joins once routed): the boundary is the first `/` or `?`, whichever comes
/// first.
fn origin_form(target: &str) -> &str {
    let Some((_, after_scheme)) = target.split_once("://") else {
        return target;
    };
    let boundary = match (after_scheme.find('/'), after_scheme.find('?')) {
        (Some(slash), Some(query)) => slash.min(query),
        (Some(slash), None) => slash,
        (None, Some(query)) => query,
        (None, None) => return "/",
    };
    after_scheme.get(boundary..).unwrap_or("/")
}

async fn respond_plain(tcp: &mut TcpStream, head: &Head) -> io::Result<()> {
    if head.method != "GET" && head.method != "HEAD" {
        return respond_status(
            tcp,
            Status::MethodNotAllowed,
            METHOD_NOT_ALLOWED,
            &head.method,
        )
        .await;
    }
    if head.path != proto::META_PATH {
        return respond_status(tcp, Status::NotFound, NOT_FOUND, &head.method)
            .await;
    }
    // Canonical (`CANONICAL.md`), so that the negotiation body has the same bytes for
    // every implementation.
    let meta = serde_json::to_string(&proto::Meta::reference())
        .map_err(io::Error::other)?;
    respond_status(tcp, Status::Ok, &meta, &head.method).await
}

/// Answers a plain HTTP request. `HEAD` gets the status line and the headers a
/// `GET` would have — including the body's length — with no body after them;
/// anything else gets the body too.
#[expect(
    clippy::too_many_arguments,
    reason = "a plain answer names its socket, status, body and the method that decides the body"
)]
async fn respond_status(
    tcp: &mut TcpStream,
    status: Status,
    body: &str,
    method: &str,
) -> io::Result<()> {
    let mut response = format!(
        "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
        status.code(),
        status.reason(),
        body.len()
    );
    if let Some(secs) = status.retry_after_secs() {
        let _ = write!(response, "retry-after: {secs}\r\n");
    }
    if let Some(methods) = status.allow() {
        let _ = write!(response, "allow: {methods}\r\n");
    }
    response.push_str("\r\n");
    if method != "HEAD" {
        response.push_str(body);
    }
    tcp.write_all(response.as_bytes()).await?;
    tcp.shutdown().await
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// A socket with bytes queued in front of it: the HTTP head the routing consumed, and
/// any frames that arrived in the same read and still have to reach the WebSocket
/// parser.
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

    /// Queues more bytes to be read after everything already queued.
    fn push_back(&mut self, mut bytes: Vec<u8>) {
        self.prefix.append(&mut bytes);
    }

    /// Drops the HTTP head bytes the upgrade consumed, keeping any frames that
    /// arrived behind them. The head lingers otherwise — up to ~17 KiB per
    /// connection, ~17 MiB at the cap — for no reader that will ever ask again.
    fn drop_head(&mut self) {
        self.prefix.drain(..self.pos.min(self.prefix.len()));
        self.prefix.shrink_to_fit();
        self.pos = 0;
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    /// Listener failures rest before retrying: 50 ms first, doubling to a cap.
    #[test]
    fn accept_failures_rest_before_retrying() {
        assert_eq!(accept_backoff(0), Duration::from_millis(50));
        assert_eq!(accept_backoff(1), Duration::from_millis(50));
        assert_eq!(accept_backoff(2), Duration::from_millis(100));
        assert_eq!(accept_backoff(3), Duration::from_millis(200));
        assert_eq!(accept_backoff(4), Duration::from_millis(400));
        assert_eq!(accept_backoff(5), Duration::from_millis(800));
        assert_eq!(accept_backoff(u32::MAX), Duration::from_millis(800));
    }

    /// An absolute-form target names the same resource: the scheme and authority
    /// fall away before routing, and the query still splits off after. (Built with
    /// `format!`: a literal URL here would send the link checker fetching.)
    #[test]
    fn absolute_form_targets_route_like_origin_form() {
        assert_eq!(origin_form("/meta"), "/meta");
        assert_eq!(origin_form("/session?room=r-1"), "/session?room=r-1");
        let host = "localhost:8080";
        assert_eq!(origin_form(&format!("http://{host}/meta")), "/meta");
        assert_eq!(
            origin_form(&format!("http://{host}/session?room=r-1&token=t")),
            "/session?room=r-1&token=t"
        );
        assert_eq!(origin_form(&format!("http://{host}")), "/");
        assert_eq!(
            origin_form(&format!("http://{host}?room=r-1&token=t")),
            "?room=r-1&token=t"
        );
        assert_eq!(origin_form("*"), "*");
    }

    #[tokio::test]
    async fn an_accepted_socket_writes_without_nagle() {
        let listener =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, served) =
            tokio::join!(TcpStream::connect(addr), accept_nodelay(&listener));
        client.unwrap();
        assert!(served.unwrap().nodelay().unwrap());
    }
}
