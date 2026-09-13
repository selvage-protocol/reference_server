//! Listener, `GET /meta`, the WebSocket upgrade and the per-connection task.
//!
//! The server is session-aware and payload-opaque: it parses the JSON session envelope,
//! owns rooms, membership and the open-document set, but never decodes a document or
//! awareness payload — binary frames are routed to the rest of the room untouched.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

use selvage_protocol as proto;

use crate::room::{Outbound, Peer, Registry, Seat, SeatError};
use crate::{mint_room_id, mint_token, random_hex, ServerConfig};

const MAX_HEAD_BYTES: usize = 16 * 1024;

pub(crate) async fn serve(
    listener: TcpListener,
    config: ServerConfig,
    registry: Arc<Mutex<Registry>>,
) {
    loop {
        let Ok((stream, addr)) = listener.accept().await else {
            continue;
        };
        let config = config.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, addr, config, registry).await;
        });
    }
}

async fn handle_connection(
    mut tcp: TcpStream,
    _addr: SocketAddr,
    config: ServerConfig,
    registry: Arc<Mutex<Registry>>,
) -> io::Result<()> {
    let Some(mut head) = read_http_head(&mut tcp).await? else {
        return Ok(());
    };

    if !head.is_websocket_upgrade {
        return respond_plain(&mut tcp, &head).await;
    }
    if head.path != proto::ENDPOINT_PATH {
        return respond_status(&mut tcp, 404, "Not Found", r#"{"error":"not found"}"#).await;
    }

    let prefixed = PrefixedStream::new(std::mem::take(&mut head.raw), tcp);
    let ws = match tokio_tungstenite::accept_async(prefixed).await {
        Ok(ws) => ws,
        Err(_) => return Ok(()),
    };

    serve_session(ws, proto::parse_join_query(&head.query), config, registry).await;
    Ok(())
}

async fn serve_session(
    ws: tokio_tungstenite::WebSocketStream<PrefixedStream<TcpStream>>,
    join: proto::JoinQuery,
    config: ServerConfig,
    registry: Arc<Mutex<Registry>>,
) {
    let (mut sink, mut stream) = ws.split();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Outbound>();

    let mut writer = tokio::spawn(async move {
        while let Some(out) = rx.recv().await {
            let closing = matches!(out, Outbound::Close(..));
            let msg = match out {
                Outbound::Text(t) => Message::text(t),
                Outbound::Binary(b) => Message::binary(b),
                Outbound::Ping(b) => Message::Ping(b.into()),
                Outbound::Close(code, reason) => Message::Close(Some(CloseFrame {
                    code: code.into(),
                    reason: reason.into(),
                })),
            };
            if sink.send(msg).await.is_err() {
                break;
            }
            if closing {
                let _ = sink.close().await;
                break;
            }
        }
    });

    let claims_host = join.room.is_none();
    match handshake(&mut stream, claims_host, config.hello_timeout).await {
        Ok(hello) => {
            let peer_id = format!("p-{}", random_hex(8));
            match seat(&registry, &config, peer_id, join, hello, tx.clone()).await {
                Ok(session) => session.run(stream, &registry, &config, &mut writer).await,
                Err((code, message)) => {
                    let _ = tx.send(Outbound::Text(
                        proto::ServerMessage::event(
                            proto::event::SESSION_ERROR,
                            serde_json::json!({ "code": code, "message": message }),
                        )
                        .to_text(),
                    ));
                    let _ = tx.send(Outbound::Close(proto::close_code_for(code), message));
                    let _ = writer.await;
                }
            }
        }
        Err((code, message)) => {
            let _ = tx.send(Outbound::Text(
                proto::ServerMessage::event(
                    proto::event::SESSION_ERROR,
                    serde_json::json!({ "code": code, "message": message }),
                )
                .to_text(),
            ));
            let _ = tx.send(Outbound::Close(proto::close_code_for(code), message));
            let _ = writer.await;
        }
    }
}

/// Waits for the `session.hello` envelope that opens every session.
async fn handshake<S>(
    stream: &mut S,
    claims_host: bool,
    timeout: Duration,
) -> Result<Hello, (&'static str, String)>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let text = match tokio::time::timeout(timeout, next_text(stream)).await {
        Ok(Ok(text)) => text,
        Ok(Err(reason)) => return Err((proto::code::BAD_MESSAGE, reason)),
        Err(_) => {
            return Err((
                proto::code::HELLO_REQUIRED,
                "session.hello was not sent in time".to_string(),
            ))
        }
    };
    let msg: proto::ClientMessage = serde_json::from_str(&text).map_err(|e| {
        (
            proto::code::BAD_MESSAGE,
            format!("first message is not a session envelope: {e}"),
        )
    })?;
    if msg.method != proto::method::SESSION_HELLO {
        return Err((
            proto::code::HELLO_REQUIRED,
            format!(
                "first method must be {}, got {}",
                proto::method::SESSION_HELLO,
                msg.method
            ),
        ));
    }
    if !proto::is_compatible(&msg.v) {
        return Err((
            proto::code::UNSUPPORTED_VERSION,
            format!("unsupported wire version {}", msg.v),
        ));
    }
    let params: proto::HelloParams = serde_json::from_value(msg.params).map_err(|e| {
        (
            proto::code::BAD_PARAMS,
            format!("bad session.hello params: {e}"),
        )
    })?;
    if params.display_name.trim().is_empty() {
        return Err((
            proto::code::BAD_PARAMS,
            "session.hello requires a display_name".to_string(),
        ));
    }
    Ok(Hello { params, claims_host })
}

struct Hello {
    params: proto::HelloParams,
    /// A connection without a room in the URL is minting one.
    claims_host: bool,
}

/// Seats a connection: mints a room or admits it to an existing one, then sends the
/// handshake response. The response is queued while the registry lock is held so it is
/// the first frame on the connection's channel.
async fn seat(
    registry: &Arc<Mutex<Registry>>,
    config: &ServerConfig,
    peer_id: String,
    join: proto::JoinQuery,
    hello: Hello,
    tx: UnboundedSender<Outbound>,
) -> Result<Session, (&'static str, String)> {
    let role = match hello.params.role {
        Some(role) => role,
        None if hello.claims_host => proto::Role::Host,
        None => proto::Role::Guest,
    };
    let info = proto::PeerInfo {
        peer_id: peer_id.clone(),
        display_name: hello.params.display_name.clone(),
        role,
        awareness_client_id: hello.params.awareness_client_id,
    };
    let peer = Peer {
        info: info.clone(),
        tx: tx.clone(),
    };

    let mut registry = registry.lock().await;
    let (event, params) = match join.room {
        None => {
            let room_id = mint_room_id();
            let token = mint_token();
            registry.create(room_id.clone(), token.clone(), config.keepalive, peer);
            (
                proto::event::ROOM_CREATED,
                proto::SessionParams {
                    room_id,
                    token: Some(token),
                    self_peer: info.clone(),
                    peers: Vec::new(),
                    documents: Vec::new(),
                    capabilities: capabilities(),
                    keepalive: config.keepalive,
                },
            )
        }
        Some(room_id) => {
            let host_was_present = registry.room(&room_id).is_some_and(|room| room.host_present());
            match registry.admit(&room_id, join.token.as_deref(), role, peer) {
                Ok(Seat::Joined) => {}
                Ok(Seat::Created { .. }) => unreachable!("admit never mints a room"),
                Err(SeatError::Unknown) => {
                    return Err((proto::code::ROOM_UNKNOWN, format!("no such room: {room_id}")))
                }
                Err(SeatError::TokenMismatch) => {
                    return Err((proto::code::TOKEN_INVALID, "invalid room token".into()))
                }
                Err(SeatError::HostPresent) => {
                    return Err((proto::code::HOST_PRESENT, "the room already has a host".into()))
                }
            }
            if role == proto::Role::Host && !host_was_present {
                let out = proto::ServerMessage::event(
                    proto::event::HOST_ATTACHED,
                    serde_json::json!({ "peer": info }),
                );
                if let Some(room) = registry.room(&room_id) {
                    room.broadcast(Some(&peer_id), Outbound::Text(out.to_text()));
                }
            }
            match registry.room(&room_id) {
                Some(room) => (
                    proto::event::ROOM_JOINED,
                    proto::SessionParams {
                        room_id: room.id.clone(),
                        token: None,
                        self_peer: info.clone(),
                        peers: room.peers_except(&peer_id),
                        documents: room.documents.clone(),
                        capabilities: capabilities(),
                        keepalive: room.keepalive,
                    },
                ),
                None => return Err((proto::code::ROOM_GONE, "the room is gone".into())),
            }
        }
    };

    let response = proto::ServerMessage::event(
        event,
        serde_json::to_value(&params).expect("session params serialize"),
    );
    let _ = tx.send(Outbound::Text(response.to_text()));

    // Late arrivals must be announced to the peers already in the room.
    if params.token.is_none() {
        let out = proto::ServerMessage::event(
            proto::event::PEER_JOINED,
            serde_json::json!({ "peer": info }),
        );
        if let Some(room) = registry.room(&params.room_id) {
            room.broadcast(
                Some(&peer_id),
                Outbound::Text(out.to_text()),
            );
        }
    }
    drop(registry);

    Ok(Session {
        peer_id,
        room_id: params.room_id.clone(),
        tx,
    })
}

fn capabilities() -> Vec<String> {
    proto::CAPABILITIES.iter().map(|c| c.to_string()).collect()
}

/// A seated connection. Binary frames are relayed; JSON methods are session-level.
struct Session {
    peer_id: String,
    room_id: String,
    tx: UnboundedSender<Outbound>,
}

impl Session {
    async fn run<S>(
        mut self,
        mut stream: S,
        registry: &Arc<Mutex<Registry>>,
        config: &ServerConfig,
        writer: &mut tokio::task::JoinHandle<()>,
    ) where
        S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        let mut ping = tokio::time::interval(config.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await;

        loop {
            tokio::select! {
                _ = ping.tick() => {
                    let _ = self.tx.send(Outbound::Ping(Vec::new()));
                }
                _ = &mut *writer => break,
                incoming = stream.next() => match incoming {
                    Some(Ok(Message::Binary(frame))) => {
                        let guard = registry.lock().await;
                        if let Some(room) = guard.room(&self.room_id) {
                            room.broadcast(
                                Some(&self.peer_id),
                                Outbound::Binary(frame.to_vec()),
                            );
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        self.handle_text(&text, registry).await;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                },
            }
        }

        self.leave(registry, config).await;
        // Dropping the outbound sender ends the writer loop, which is what lets a
        // connection that was closed by the peer finish instead of hanging on `recv`.
        drop(std::mem::replace(&mut self.tx, inert_sender()));
        let _ = writer.await;
    }

    async fn handle_text(&self, text: &str, registry: &Arc<Mutex<Registry>>) {
        let reply = |msg: proto::ServerMessage| {
            let _ = self.tx.send(Outbound::Text(msg.to_text()));
        };
        let event = |name: &str, params: serde_json::Value| {
            let _ = self.tx.send(Outbound::Text(proto::ServerMessage::event(name, params).to_text()));
        };

        let msg: proto::ClientMessage = match serde_json::from_str(text) {
            Ok(msg) => msg,
            Err(e) => {
                event(
                    proto::event::SESSION_ERROR,
                    serde_json::json!({ "code": proto::code::BAD_MESSAGE, "message": e.to_string() }),
                );
                return;
            }
        };
        let Some(id) = msg.id else {
            event(
                proto::event::SESSION_ERROR,
                serde_json::json!({
                    "code": proto::code::BAD_MESSAGE,
                    "message": "a request needs an id"
                }),
            );
            return;
        };

        if !proto::is_compatible(&msg.v) {
            reply(proto::ServerMessage::error(
                id,
                proto::code::UNSUPPORTED_VERSION,
                format!("unsupported wire version {}", msg.v),
            ));
            let _ = self
                .tx
                .send(Outbound::Close(proto::close::UNSUPPORTED_VERSION, "version".into()));
            return;
        }

        match msg.method.as_str() {
            proto::method::DOC_OPEN => {
                let path = match serde_json::from_value::<proto::DocOpenParams>(msg.params) {
                    Ok(p) if !p.path.trim().is_empty() => p.path,
                    Ok(_) => {
                        return reply(proto::ServerMessage::error(
                            id,
                            proto::code::BAD_PARAMS,
                            "path is required",
                        ))
                    }
                    Err(e) => {
                        return reply(proto::ServerMessage::error(
                            id,
                            proto::code::BAD_PARAMS,
                            e.to_string(),
                        ))
                    }
                };
                {
                    let mut guard = registry.lock().await;
                    if let Some(room) = guard.room_mut(&self.room_id) {
                        room.open_document(&path);
                    }
                }
                reply(proto::ServerMessage::response(id, serde_json::json!({})));
                self.announce(
                    registry,
                    proto::event::DOC_OPENED,
                    serde_json::json!({ "peer_id": self.peer_id, "path": path }),
                )
                .await;
            }
            proto::method::DOC_CLOSE => {
                let path = match serde_json::from_value::<proto::DocCloseParams>(msg.params) {
                    Ok(p) => p.path,
                    Err(e) => {
                        return reply(proto::ServerMessage::error(
                            id,
                            proto::code::BAD_PARAMS,
                            e.to_string(),
                        ))
                    }
                };
                {
                    let mut guard = registry.lock().await;
                    if let Some(room) = guard.room_mut(&self.room_id) {
                        room.close_document(&path);
                    }
                }
                reply(proto::ServerMessage::response(id, serde_json::json!({})));
                self.announce(
                    registry,
                    proto::event::DOC_CLOSED,
                    serde_json::json!({ "peer_id": self.peer_id, "path": path }),
                )
                .await;
            }
            proto::method::SESSION_HELLO => reply(proto::ServerMessage::error(
                id,
                proto::code::ALREADY_SEATED,
                "this connection already completed the handshake",
            )),
            other => reply(proto::ServerMessage::error(
                id,
                proto::code::UNKNOWN_METHOD,
                format!("no such method: {other}"),
            )),
        }
    }

    async fn announce(
        &self,
        registry: &Arc<Mutex<Registry>>,
        event: &str,
        params: serde_json::Value,
    ) {
        let guard = registry.lock().await;
        if let Some(room) = guard.room(&self.room_id) {
            room.broadcast(
                Some(&self.peer_id),
                Outbound::Text(proto::ServerMessage::event(event, params).to_text()),
            );
        }
    }

    async fn leave(&mut self, registry: &Arc<Mutex<Registry>>, config: &ServerConfig) {
        let mut guard = registry.lock().await;
        let Some(detach) = guard.detach(&self.room_id, &self.peer_id, config.room_grace) else {
            return;
        };
        let left = proto::ServerMessage::event(
            proto::event::PEER_LEFT,
            serde_json::json!({ "peer_id": self.peer_id }),
        );
        if let Some(room) = guard.room(&self.room_id) {
            room.broadcast(None, Outbound::Text(left.to_text()));
        }
        if !detach.was_host {
            return;
        }

        let grace_ms = config.room_grace.as_millis() as u64;
        if let Some(room) = guard.room(&self.room_id) {
            room.broadcast(
                None,
                Outbound::Text(
                    proto::ServerMessage::event(
                        proto::event::HOST_DETACHED,
                        serde_json::json!({ "grace_ms": grace_ms }),
                    )
                    .to_text(),
                ),
            );
        }

        drop(guard);
        let registry = registry.clone();
        let room_id = self.room_id.clone();
        let generation = detach.generation;
        let grace = config.room_grace;
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let peers = registry.lock().await.reap_if_host_absent(&room_id, generation);
            if peers.is_empty() {
                return;
            }
            let gone = proto::ServerMessage::event(
                proto::event::ROOM_GONE,
                serde_json::json!({ "room_id": room_id, "reason": "host did not return" }),
            );
            for peer in peers {
                peer.send(Outbound::Text(gone.to_text()));
                peer.send(Outbound::Close(
                    proto::close::ROOM_GONE,
                    "room gone".into(),
                ));
            }
        });
    }
}

/// A sender whose receiver has been dropped: every send fails, so it is inert.
fn inert_sender() -> UnboundedSender<Outbound> {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    tx
}

async fn next_text<S>(stream: &mut S) -> Result<String, String>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
            Some(Ok(Message::Binary(_))) => {
                return Err("a binary frame arrived before session.hello".into())
            }
            Some(Ok(Message::Close(_))) => return Err("connection closed during handshake".into()),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e.to_string()),
            None => return Err("connection closed during handshake".into()),
        }
    }
}

// --- HTTP ---------------------------------------------------------------------

struct Head {
    raw: Vec<u8>,
    path: String,
    query: String,
    is_websocket_upgrade: bool,
}

async fn read_http_head(tcp: &mut TcpStream) -> io::Result<Option<Head>> {
    use tokio::io::AsyncReadExt;

    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let end = loop {
        let n = tcp.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Ok(None);
        }
    };

    let head_text = String::from_utf8_lossy(&buf[..end]).to_string();
    let request_line = head_text.lines().next().unwrap_or_default().to_string();
    let target = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };

    Ok(Some(Head {
        raw: buf[..end].to_vec(),
        path,
        query,
        is_websocket_upgrade: head_text
            .to_ascii_lowercase()
            .contains("upgrade: websocket"),
    }))
}

async fn respond_plain(tcp: &mut TcpStream, head: &Head) -> io::Result<()> {
    if head.path == proto::META_PATH {
        let body =
            serde_json::to_string_pretty(&proto::Meta::reference()).expect("meta serializes");
        return respond_status(tcp, 200, "OK", &body).await;
    }
    respond_status(tcp, 404, "Not Found", r#"{"error":"not found"}"#).await
}

async fn respond_status(tcp: &mut TcpStream, status: u16, reason: &str, body: &str) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
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
struct PrefixedStream<T> {
    prefix: Vec<u8>,
    pos: usize,
    inner: T,
}

impl<T> PrefixedStream<T> {
    fn new(prefix: Vec<u8>, inner: T) -> Self {
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
        if this.pos < this.prefix.len() {
            let remaining = &this.prefix[this.pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.pos += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
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

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
