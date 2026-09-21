//! Session layer: version negotiation, unknown methods, membership and the room
//! lifecycle. The raw-WireSocket tests speak the protocol by hand, so the spec in
//! `PROTOCOL.md` is checked against the bytes on the wire rather than against the
//! client library.

use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_harness::{
    EngineEvent, Error, Harness, Role, Room, SelectionOffsets, ServerConfig,
    SyncEngine, WAIT, wait_for, wait_for_convergence, wait_for_described,
    wait_for_described_within, wait_for_event, wait_for_peer,
};
use selvage_protocol as proto;
use selvage_protocol::{close, code, event, method};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;

const PATH: &str = "src/main.rs";

/// A path only the guest in the document-set test holds open.
const GUEST_ONLY: &str = "only/guest.rs";

/// A peer is ejected only after the room relays tens of mebibytes into a socket that
/// never drains, so the wait for the announcement is I/O under coverage, not a local
/// engine turn. [`WAIT`] bounds the latter; this bounds the former, and still fails a
/// run that never ejects at all.
const EJECTION_WAIT: Duration = Duration::from_secs(30);

/// Anything these tests can fail with.
type Failure = Box<dyn StdError>;

/// A hand-rolled WebSocket session: no client library involved.
type Raw = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect(url: &str) -> Result<Raw, Failure> {
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    Ok(ws)
}

async fn send_json(ws: &mut Raw, value: &Value) -> Result<(), Failure> {
    ws.send(Message::text(value.to_string())).await?;
    Ok(())
}

async fn hello(ws: &mut Raw, params: &Value) -> Result<(), Failure> {
    send_json(
        ws,
        &serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": 1,
            "method": method::SESSION_HELLO,
            "params": params,
        }),
    )
    .await
}

/// The next text frame that is not a binary relay.
async fn next_json(ws: &mut Raw) -> Result<Value, Failure> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                return Ok(serde_json::from_str(&text)?);
            }
            Some(Ok(Message::Close(frame))) => {
                return Err(format!(
                    "connection closed while waiting for JSON: {frame:?}"
                )
                .into());
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.into()),
            None => {
                return Err("connection ended while waiting for JSON".into());
            }
        }
    }
}

/// The next binary frame relayed by the server.
async fn next_binary(ws: &mut Raw) -> Result<Vec<u8>, Failure> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(frame))) => return Ok(frame.to_vec()),
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.into()),
            None => {
                return Err(
                    "connection ended while waiting for a binary frame".into(),
                );
            }
        }
    }
}

// --- a WebSocket client written by hand -----------------------------------------
//
// A library client cannot write the HTTP upgrade and the first frame in one go, and
// cannot watch the socket close. These tests can, so they speak the protocol over a
// plain TCP socket.
/// One frame as it appeared on the wire.
struct Frame {
    opcode: u8,
    payload: Vec<u8>,
}

impl Frame {
    /// The close code, for a close frame.
    fn close_code(&self) -> Option<u16> {
        let bytes: [u8; 2] = self.payload.get(..2)?.try_into().ok()?;
        Some(u16::from_be_bytes(bytes))
    }

    /// The payload as text, for a text frame.
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.payload).into_owned()
    }

    /// The reason of a close frame: everything after the two-byte status code.
    fn reason(&self) -> Option<String> {
        let rest = self.payload.get(2..)?;
        Some(String::from_utf8_lossy(rest).into_owned())
    }
}

/// A WebSocket connection the test drives byte by byte.
struct RawSocket {
    stream: TcpStream,
}

impl RawSocket {
    /// Performs the upgrade by hand, writing `extra` — frames the client already has —
    /// in the same write as the request head.
    async fn open(
        harness: &Harness,
        target: &str,
        extra: &[u8],
    ) -> Result<Self, Failure> {
        let addr = harness.ws_base().trim_start_matches("ws://").to_string();
        let mut stream = TcpStream::connect(&addr).await?;
        // A fixed zero nonce: the handshake needs sixteen bytes, not secrecy, and a
        // random-looking fixture trips the secret scanner.
        let mut request = format!(
            "GET {target} HTTP/1.1\r\nhost: {addr}\r\nupgrade: websocket\r\n\
             connection: Upgrade\r\nsec-websocket-key: AAAAAAAAAAAAAAAAAAAAAA==\r\n\
             sec-websocket-version: 13\r\n\r\n"
        )
        .into_bytes();
        request.extend_from_slice(extra);
        stream.write_all(&request).await?;
        let head = read_http_head(&mut stream).await?;
        if !head.starts_with("HTTP/1.1 101") {
            return Err(format!("the upgrade was refused: {head}").into());
        }
        Ok(Self { stream })
    }

    /// Sends one masked frame, as a client must.
    async fn send(
        &mut self,
        opcode: u8,
        payload: &[u8],
    ) -> Result<(), Failure> {
        let frame = client_frame(opcode, payload);
        self.stream.write_all(&frame).await?;
        Ok(())
    }

    /// Sends one frame with the FIN bit chosen by the caller: fragments carry it
    /// clear until the last, so the server reassembles before the bound bites.
    #[expect(
        clippy::too_many_arguments,
        reason = "a fragment names its opcode, FIN bit and payload; the shape is the pin"
    )]
    async fn send_fragment(
        &mut self,
        opcode: u8,
        fin: bool,
        payload: &[u8],
    ) -> Result<(), Failure> {
        let frame = client_fragment(opcode, fin, payload);
        self.stream.write_all(&frame).await?;
        Ok(())
    }

    /// Sends a text frame carrying JSON.
    async fn send_json(&mut self, value: &Value) -> Result<(), Failure> {
        self.send(0x1, value.to_string().as_bytes()).await
    }

    /// Sends `session.hello` with these params.
    async fn hello(&mut self, params: &Value) -> Result<(), Failure> {
        self.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": 1,
            "method": method::SESSION_HELLO,
            "params": params,
        }))
        .await
    }

    /// Reads frames until a text frame arrives, returning its JSON.
    async fn next_json(&mut self) -> Result<Value, Failure> {
        let frame = read_matching(&mut self.stream, 0x1).await?;
        Ok(serde_json::from_str(&frame.text())?)
    }
    /// Reads frames until the server closes, returning the close frame.
    async fn read_to_close(&mut self) -> Result<Frame, Failure> {
        read_matching(&mut self.stream, 0x8).await
    }

    /// Reads until the peer closes the socket, so the connection is gone for good.
    async fn read_to_eof(&mut self) -> Result<(), Failure> {
        let mut scratch = [0u8; 64];
        while self.stream.read(&mut scratch).await? > 0 {}
        Ok(())
    }

    /// Reads until the socket ends, returning how: `None` for a clean FIN, else the
    /// error that ended it. A writer stopped with unsent bytes still queued resets
    /// the connection instead of closing it cleanly; either way nothing more arrives.
    /// One read off the socket: `None` for more data, `Some` for how it ended.
    async fn read_outcome(&mut self) -> Option<Option<ErrorKind>> {
        let mut scratch = [0u8; 64];
        match self.stream.read(&mut scratch).await {
            Ok(0) => Some(None),
            Ok(_) => None,
            Err(error) => Some(Some(error.kind())),
        }
    }

    async fn read_to_end(&mut self) -> Option<ErrorKind> {
        loop {
            match self.read_outcome().await {
                None => (),
                Some(how) => return how,
            }
        }
    }
}

/// Whether this failure is the socket ending underfoot: the server closes on an
/// over-bound header, so a send whose payload outgrows the socket buffers can fail
/// with a reset instead of completing.
fn socket_ended(failed: &Failure) -> bool {
    let Some(error) = failed.downcast_ref::<IoError>() else {
        return false;
    };
    matches!(
        error.kind(),
        ErrorKind::ConnectionReset
            | ErrorKind::BrokenPipe
            | ErrorKind::ConnectionAborted
    )
}

/// The payload length of a frame, reading the extended form when the short one says to.
async fn frame_length(
    stream: &mut TcpStream,
    short: u8,
) -> Result<u64, Failure> {
    match short {
        126 => {
            let mut ext = [0u8; 2];
            stream.read_exact(&mut ext).await?;
            Ok(u64::from(u16::from_be_bytes(ext)))
        }
        127 => {
            let mut ext = [0u8; 8];
            stream.read_exact(&mut ext).await?;
            Ok(u64::from_be_bytes(ext))
        }
        length => Ok(u64::from(length)),
    }
}

/// Reads one frame off the socket.
async fn read_frame(stream: &mut TcpStream) -> Result<Frame, Failure> {
    let mut header = [0u8; 2];
    stream.read_exact(&mut header).await?;
    let opcode = header[0] & 0x0f;
    let length = frame_length(stream, header[1] & 0x7f).await?;
    let mut payload = vec![0u8; usize::try_from(length)?];
    stream.read_exact(&mut payload).await?;
    Ok(Frame { opcode, payload })
}

/// Reads frames until one has this opcode.
async fn read_matching(
    stream: &mut TcpStream,
    opcode: u8,
) -> Result<Frame, Failure> {
    loop {
        let frame = read_frame(stream).await?;
        if frame.opcode == opcode {
            return Ok(frame);
        }
    }
}

/// Waits for the engine to report an undecodable frame, ignoring everything else.
async fn wait_for_bad_frame(
    events: &mut broadcast::Receiver<EngineEvent>,
) -> Result<String, Failure> {
    loop {
        match timeout(Duration::from_millis(50), events.recv()).await {
            Ok(Ok(EngineEvent::SessionError { code, .. }))
                if code == code::BAD_MESSAGE =>
            {
                return Ok(code);
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return Err("the event stream closed".into());
            }
            _ => {}
        }
    }
}

/// Reads binary frames until `wanted` arrives, skipping whatever else the room
/// relays: a watcher that joined after an engine hears the engine's own sync first.
async fn read_binary_until(
    stream: &mut TcpStream,
    wanted: &[u8],
) -> Result<Frame, Failure> {
    loop {
        let frame = read_matching(stream, 0x2).await?;
        match (frame.payload == wanted).then_some(frame) {
            None => (),
            Some(relayed) => return Ok(relayed),
        }
    }
}

/// One masked client frame with the FIN bit chosen by the caller, for the
/// fragmented-bound pin below.
fn client_fragment(opcode: u8, fin: bool, payload: &[u8]) -> Vec<u8> {
    let mut frame = client_frame(opcode, payload);
    if let Some(first) = frame.first_mut() {
        *first = if fin { 0x80 | opcode } else { opcode };
    }
    frame
}

/// One masked client frame, in whichever length form fits: seven bits, sixteen, or
/// sixty-four. The server accepts frames far larger than a session envelope, and the
/// grant's count bound is only reachable with one.
fn client_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    /// The 7-bit length that means the real one follows in two more bytes.
    const EXTENDED: u8 = 126;
    /// ... and the one that means the next eight.
    const LONG: u8 = 127;
    let mask = [0x21, 0x42, 0x63, 0x84];
    let mut frame = vec![0x80 | opcode];
    // A payload longer than a `u64` cannot exist, so the widening below never saturates.
    let length = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    match u8::try_from(length) {
        Ok(short) if length < u64::from(EXTENDED) => frame.push(0x80 | short),
        _ => match u16::try_from(length) {
            Ok(medium) if length < u64::from(u16::MAX) => {
                frame.push(0x80 | EXTENDED);
                frame.extend_from_slice(&medium.to_be_bytes());
            }
            _ => {
                frame.push(0x80 | LONG);
                frame.extend_from_slice(&length.to_be_bytes());
            }
        },
    }
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .zip(mask.iter().cycle())
            .map(|(byte, key)| byte ^ key),
    );
    frame
}

/// Reads an HTTP response head, returning it as text.
async fn read_http_head(stream: &mut TcpStream) -> Result<String, Failure> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await?;
        head.push(byte[0]);
    }
    Ok(String::from_utf8(head)?)
}

async fn open(ws: &mut Raw, id: u64, path: &str) -> Result<(), Failure> {
    send_json(
        ws,
        &serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": method::DOC_OPEN,
            "params": {"path": path},
        }),
    )
    .await?;
    // A peer's `doc.opened` event may arrive before our own response.
    loop {
        let response = next_json(ws).await?;
        if response.get("id").and_then(Value::as_u64) == Some(id) {
            let result = response.get("result").ok_or("a result")?;
            let documents = result
                .get("documents")
                .and_then(Value::as_array)
                .ok_or("the open set")?;
            assert!(
                documents.contains(&Value::from(path)),
                "the result reports the room's open set: {result}"
            );
            return Ok(());
        }
    }
}

/// The next response to `id`, skipping the events the server sends in between.
async fn response_for(ws: &mut Raw, id: u64) -> Result<Value, Failure> {
    loop {
        let frame = next_json(ws).await?;
        if frame.get("id").and_then(Value::as_u64) == Some(id) {
            return Ok(frame);
        }
    }
}

/// The session target a guest joins a room with.
fn guest_target(room_id: &str, token: &str) -> String {
    format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(room_id),
        proto::percent_encode(token)
    )
}

/// The next JSON frame on a raw connection, under the harness's deadline. A raw socket
/// has no timeout of its own, and a test that waits for ever is not a test that passed.
#[expect(
    clippy::panic,
    reason = "a frame that never arrives is a test failure, not a value to recover"
)]
async fn next_json_within(raw: &mut RawSocket, label: &str) -> Value {
    match timeout(WAIT, raw.next_json()).await {
        Ok(Ok(value)) => value,
        Ok(Err(e)) => panic!("{label}: {e}"),
        Err(elapsed) => panic!("{label}: {elapsed}"),
    }
}

/// The next response to `id` on a raw connection, skipping the events in between.
async fn raw_response_for(raw: &mut RawSocket, id: u64) -> Value {
    loop {
        let frame = next_json_within(raw, "a response").await;
        if frame.get("id").and_then(Value::as_u64) == Some(id) {
            return frame;
        }
    }
}

/// Reads until the connection closes, returning the close code.
async fn close_code(ws: &mut Raw) -> Result<u16, Failure> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Close(Some(frame)))) => {
                return Ok(u16::from(frame.code));
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.into()),
            None => {
                return Err("connection dropped without a close frame".into());
            }
        }
    }
}

#[tokio::test]
async fn meta_negotiates_the_wire_version() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let body = http_get(&format!("{}/meta", harness.http_base()))
        .await
        .expect("meta answers");
    let meta: proto::Meta = serde_json::from_str(&body).expect("meta is JSON");

    assert!(
        meta.wire_versions
            .contains(&proto::WIRE_VERSION.to_string())
    );
    assert!(meta.capabilities.iter().any(|c| c == "y-protocols/1"));
    assert_eq!(meta.keepalive.ping_interval_ms, 30_000);
    assert_eq!(meta.keepalive.awareness_renew_ms, 15_000);
    assert_eq!(meta.keepalive.awareness_expire_ms, 30_000);
    assert_eq!(meta.roles, vec!["host", "guest"]);
}

/// A host whose socket stops answering is ended, so the room can move on: a peer that
/// answers no ping is not idle but gone — a roaming client whose TCP end died unobserved,
/// a hung relay — and leaving it seated strands the room, since it stays the host, no grace
/// is armed, and every guest waits for a `host.detached` that cannot come. Here the host is
/// a raw socket that completes the handshake and then never reads, so it never answers the
/// server's pings (`PROTOCOL.md` §2.1).
#[tokio::test]
async fn a_host_that_stops_answering_its_pings_is_detached()
-> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig {
        ping_interval: Duration::from_millis(20),
        room_grace: Duration::from_secs(30),
        ..ServerConfig::default()
    })
    .await;
    let mut host = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    host.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    let created = next_json_within(&mut host, "room.created").await;
    assert_eq!(created["event"], event::ROOM_CREATED);
    let room = Room {
        id: created["params"]["room_id"]
            .as_str()
            .expect("a room id")
            .to_string(),
        token: created["params"]["token"]
            .as_str()
            .expect("a token")
            .to_string(),
        invite_url: String::new(),
    };

    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    let detached = wait_for_event(&guest, "host.detached", |event| {
        matches!(event, EngineEvent::HostDetached { .. })
    });
    let EngineEvent::HostDetached { grace_ms } = detached.await else {
        panic!("host.detached");
    };
    assert_eq!(grace_ms, 30_000, "the grace starts as it does on any drop");

    // The room is alive, not destroyed: a host that comes back inside the grace reclaims it.
    let _reclaimed = harness
        .reclaim(&room, "Ada")
        .await
        .expect("the room waits for its host");
    Ok(())
}

/// A merely slow link is untouched. The server's ping interval here is 20 ms and the
/// session runs for fifteen of them; a client that answers its pings (every conforming one
/// does, as it reads) is never mistaken for a dead socket, and no reconnect happens at all.
#[tokio::test]
async fn a_slow_link_is_not_killed_by_the_liveness_bound() -> Result<(), Failure>
{
    let harness = Harness::start_with(ServerConfig {
        ping_interval: Duration::from_millis(20),
        room_grace: Duration::from_secs(30),
        ..ServerConfig::default()
    })
    .await;
    let (host, room) = harness.host("Ada").await?;
    let guest = harness.join(&room, "Bob").await?;
    let before = guest.session().peer.peer_id.clone();

    sleep(Duration::from_millis(300)).await;

    guest.open(PATH).await.expect("the session is still seated");
    assert_eq!(
        guest.session().peer.peer_id,
        before,
        "no reconnect stands in for a session that never dropped"
    );
    assert_eq!(
        host.documents().await?,
        vec![PATH.to_string()],
        "the room still holds the guest's document"
    );
    Ok(())
}

/// The room's grace period is advertised before a session exists (`PROTOCOL.md` §2): a host
/// learns it from `host.detached` only if it is still connected, and the connection that
/// has to size its retry budget to the grace is the one that is gone. The value here is the
/// server's configured one, so a deployment that moves the clock says so.
#[tokio::test]
async fn meta_advertises_the_configured_room_grace() {
    let configured = Duration::from_millis(4_321);
    let harness = Harness::start_with(ServerConfig {
        room_grace: configured,
        ..ServerConfig::default()
    })
    .await;
    let body = http_get(&format!("{}/meta", harness.http_base()))
        .await
        .expect("meta answers");
    let meta: proto::Meta = serde_json::from_str(&body).expect("meta is JSON");
    assert_eq!(meta.keepalive.room_grace_ms, 4_321);

    // The grace a peer actually gets is the same number: the default host detach carries
    // it, and a server whose two answers disagreed would be lying to one of them.
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let mut invited =
        RawSocket::open(&harness, &guest_target(&room.id, &room.token), &[])
            .await
            .expect("the upgrade succeeds");
    invited
        .hello(&serde_json::json!({"display_name": "Bob"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut invited, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    drop(host);
    // The room announces `peer.left` before `host.detached`, so the wait is for the event
    // rather than for the next frame.
    let mut frame =
        next_json_within(&mut invited, "a frame after the host left").await;
    while frame["event"] != event::HOST_DETACHED {
        frame = next_json_within(&mut invited, "host.detached").await;
    }
    assert_eq!(frame["params"]["grace_ms"], 4_321);
}

#[tokio::test]
async fn unknown_methods_return_an_error_and_unknown_fields_are_ignored() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let url = proto::session_url(&harness.ws_base(), None, None);
    let mut raw = connect(&url).await.expect("connects");

    // Unknown fields, unknown capabilities: ignored.
    hello(
        &mut raw,
        &serde_json::json!({
            "display_name": "Raw",
            "capabilities": ["no.such.capability"],
            "future_field": {"nested": true},
        }),
    )
    .await
    .expect("says hello");
    let created = next_json(&mut raw).await.expect("room.created");
    assert_eq!(created["event"], event::ROOM_CREATED);
    assert!(created["params"]["token"].is_string());
    let room_id = created["params"]["room_id"].as_str().unwrap().to_string();

    // A known method: a normal response.
    send_json(
        &mut raw,
        &serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": 2,
            "method": method::DOC_OPEN,
            "params": {"path": PATH},
        }),
    )
    .await
    .expect("sends");
    let opened = response_for(&mut raw, 2).await.expect("the response");
    assert_eq!(opened["result"]["documents"], serde_json::json!([PATH]));

    // An unknown method: an error response, not silence.
    send_json(
        &mut raw,
        &serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": 3,
            "method": "cursor.teleport",
            "params": {},
        }),
    )
    .await
    .expect("sends");
    let refused = response_for(&mut raw, 3).await.expect("the refusal");
    assert_eq!(refused["error"]["code"], code::UNKNOWN_METHOD);
    assert!(refused["result"].is_null());

    // An incompatible version: refused, with the documented close code.
    let url = proto::session_url(&harness.ws_base(), Some(&room_id), Some("t"));
    let mut stale = connect(&url).await.expect("connects");
    send_json(
        &mut stale,
        &serde_json::json!({
            "v": "selvage/9",
            "id": 1,
            "method": method::SESSION_HELLO,
            "params": {"display_name": "Stale"},
        }),
    )
    .await
    .expect("sends");
    assert_eq!(
        next_json(&mut stale).await.expect("refusal")["params"]["code"],
        code::UNSUPPORTED_VERSION
    );
    assert_eq!(
        close_code(&mut stale).await.expect("close frame"),
        close::UNSUPPORTED_VERSION
    );
}

#[tokio::test]
async fn joining_needs_the_room_and_the_token() {
    let harness = Harness::start(Duration::from_secs(5)).await;

    // No such room.
    let url = proto::session_url(&harness.ws_base(), Some("r-nope"), Some("t"));
    let mut missing = connect(&url).await.expect("connects");
    hello(&mut missing, &serde_json::json!({"display_name": "Ghost"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json(&mut missing).await.expect("refusal")["params"]["code"],
        code::ROOM_UNKNOWN
    );
    assert_eq!(
        close_code(&mut missing).await.expect("close frame"),
        close::ROOM_UNKNOWN
    );

    // A real room, wrong token.
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let url =
        proto::session_url(&harness.ws_base(), Some(&room.id), Some("wrong"));
    let mut wrong = connect(&url).await.expect("connects");
    hello(&mut wrong, &serde_json::json!({"display_name": "Mallory"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json(&mut wrong).await.expect("refusal")["params"]["code"],
        code::TOKEN_INVALID
    );
    assert_eq!(
        close_code(&mut wrong).await.expect("close frame"),
        close::TOKEN_INVALID
    );

    // The room keeps serving the host.
    assert_eq!(host.session().role, Role::Host);
    assert_eq!(host.peers().await.unwrap().len(), 0);
}

/// A first frame that is wrong in two ways is refused for the first one §11 orders: the
/// envelope and its id, then `v`, then the method. The handshake used to judge the method
/// first, so a frame that was both a non-hello method and an incompatible version was
/// answered `hello_required` — a client that reads the code rather than the message would
/// try again with a hello it still could not seat.
#[tokio::test]
async fn an_incompatible_first_frame_is_refused_for_its_version() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.send_json(&serde_json::json!({
        "v": "selvage/2",
        "id": 1,
        "method": method::DOC_OPEN,
        "params": {"path": PATH},
    }))
    .await
    .expect("sends");
    let refused = next_json_within(&mut raw, "the refusal").await;
    assert_eq!(refused["event"], event::SESSION_ERROR);
    assert_eq!(
        refused["params"]["code"],
        code::UNSUPPORTED_VERSION,
        "the version is judged before the method: {refused}"
    );
    assert_eq!(
        raw.read_to_close().await.expect("a close").close_code(),
        Some(close::UNSUPPORTED_VERSION)
    );

    // A compatible version with a non-hello method is still `hello_required`, and a frame
    // with no id is still `bad_message` before either of them.
    let mut second = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    second
        .send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": 1,
            "method": method::DOC_OPEN,
            "params": {"path": PATH},
        }))
        .await
        .expect("sends");
    assert_eq!(
        next_json_within(&mut second, "the refusal").await["params"]["code"],
        code::HELLO_REQUIRED
    );
}

/// `display_name` is bounded at 32 UTF-16 code units (`PROTOCOL.md` §5), so an astral
/// character costs two. A name of exactly 32 code units — one of them astral — seats; one code
/// unit over is refused `bad_params` before seating. Counting bytes or code points answers both
/// the wrong way: the at-limit name is 34 bytes, and the over-limit one is 32 code points.
#[tokio::test]
async fn display_name_is_bounded_in_utf16_code_units() {
    let harness = Harness::start(Duration::from_secs(5)).await;

    let at_limit = format!("{}𝄞", "a".repeat(30));
    assert_eq!(at_limit.encode_utf16().count(), 32);
    let mut host = connect(&proto::session_url(&harness.ws_base(), None, None))
        .await
        .expect("connects");
    hello(
        &mut host,
        &serde_json::json!({ "display_name": at_limit.as_str(), "role": "host" }),
    )
    .await
    .expect("says hello");
    let created = next_json(&mut host).await.expect("room.created");
    assert_eq!(created["event"], event::ROOM_CREATED);
    assert_eq!(created["params"]["self"]["display_name"], at_limit.as_str());

    let over_limit = format!("{}𝄞", "a".repeat(31));
    assert_eq!(over_limit.encode_utf16().count(), 33);
    assert_eq!(over_limit.chars().count(), 32);
    let mut refused =
        connect(&proto::session_url(&harness.ws_base(), None, None))
            .await
            .expect("connects");
    hello(
        &mut refused,
        &serde_json::json!({ "display_name": over_limit.as_str(), "role": "host" }),
    )
    .await
    .expect("says hello");
    assert_eq!(
        next_json(&mut refused).await.expect("refusal")["params"]["code"],
        code::BAD_PARAMS
    );
    assert_eq!(
        close_code(&mut refused).await.expect("close frame"),
        close::PROTOCOL_ERROR
    );
}

/// A `display_name` carrying a control character is refused `bad_params`, like a blank or
/// over-long one (`PROTOCOL.md` §5): the name is echoed into every peer's roster, the
/// `peers` list of every later joiner and whatever terminal an adapter prints it to, so an
/// ANSI escape in it is a sequence this protocol never agreed to carry. A raw socket sends
/// the bytes a client library would not, which is what makes the shape reachable here.
///
/// The value judged is the one received, before the trim: `"\tAda"` and `"Ada\n"` carry a
/// `Cc` character that `trim` removes, and seating one would store `"Ada"` — a name its
/// owner did not choose, the rewrite §5 refuses.
#[tokio::test]
async fn a_display_name_with_control_characters_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    for name in [
        "\u{1b}[31mAda\u{1b}[0m",
        "\tAda",
        "Ada\n",
        "\u{85}Ada",
        "Ada\u{0}",
        "Cyd\u{9b}",
    ] {
        assert!(name.chars().any(char::is_control));
        let mut refused =
            connect(&proto::session_url(&harness.ws_base(), None, None))
                .await
                .expect("connects");
        hello(
            &mut refused,
            &serde_json::json!({ "display_name": name, "role": "host" }),
        )
        .await
        .expect("says hello");
        let refusal = next_json(&mut refused).await.expect("refusal");
        assert_eq!(
            refusal["event"],
            event::SESSION_ERROR,
            "{name:?} was seated instead of refused: {refusal}"
        );
        assert_eq!(refusal["params"]["code"], code::BAD_PARAMS);
        let message = refusal["params"]["message"]
            .as_str()
            .expect("a message")
            .to_string();
        assert!(
            message.contains("control"),
            "the refusal of {name:?} names the reason: {message}"
        );
        assert!(
            !message.contains(name),
            "the refusal does not echo {name:?} back: {message:?}"
        );
        assert_eq!(
            close_code(&mut refused).await.expect("close frame"),
            close::PROTOCOL_ERROR
        );
    }
}

/// The name a seated peer is stored and echoed under is the bytes its owner sent: §5's
/// refusals are about values a server will not carry, and a server that rewrites an
/// accepted one leaves a peer drawn under a name its person did not choose. Whitespace
/// inside the name is what `trim` must leave alone.
#[tokio::test]
async fn an_accepted_display_name_is_stored_and_echoed_verbatim() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    for name in ["Ada", "Ada Lovelace", "Ada  Lovelace"] {
        let (host, room) = harness.host(name).await.expect("host connects");
        assert_eq!(
            host.session().peer.display_name,
            name,
            "the seated name is the one sent"
        );
        let guest = harness.join(&room, "Bob").await.expect("guest connects");
        assert_eq!(guest.session().peer.display_name, "Bob");
        let seen = wait_for_peer(&guest, name).await;
        assert_eq!(
            seen.display_name, name,
            "the guest reads the host's name verbatim"
        );
    }
}

/// A seated `session.rename` carrying one is the error response `bad_params` and the
/// connection stays open, exactly as a blank rename is: a seated fault is not a close
/// (`PROTOCOL.md` §5, §9.2, §11). A control character is judged on the value received,
/// before the trim, so an edge padding cannot hide one.
#[tokio::test]
async fn a_rename_with_control_characters_is_refused_and_the_session_survives()
{
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({ "display_name": "Ada" }))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    for (id, name) in [
        (2_u64, "Ada\u{1b}[31m"),
        (3, " Ada\u{7} "),
        (4, "Bob\u{0}"),
        (5, "Cyd\u{9b}"),
        (6, "\tAda"),
        (7, "Ada\n"),
        (8, "\u{85}Ada"),
    ] {
        raw.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": method::SESSION_RENAME,
            "params": {"display_name": name},
        }))
        .await
        .expect("sends");
        let refused = raw_response_for(&mut raw, id).await;
        assert_eq!(
            refused["error"]["code"],
            code::BAD_PARAMS,
            "a control character was accepted in {name:?}: {refused}"
        );
    }

    // The connection is still seated, and a name without one still moves — the bytes
    // its owner sent, with no whitespace inside it rewritten.
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 9,
        "method": method::SESSION_RENAME,
        "params": {"display_name": "Ada  Lovelace"},
    }))
    .await
    .expect("sends");
    let answered = raw_response_for(&mut raw, 9).await;
    assert!(answered["error"].is_null(), "the rename stands: {answered}");
    assert_eq!(
        raw.next_json().await.expect("the announcement")["params"]["display_name"],
        "Ada  Lovelace"
    );
}

/// A `doc.open` or `doc.close` path carrying a control character is refused `bad_params`
/// with the connection open, and nothing is announced: the path used to be stored in the
/// room's set and broadcast verbatim to every peer's document set — and, through it, to a
/// file tree and a terminal (`PROTOCOL.md` §5, §12). `..` and an absolute path stay legal:
/// §5 leaves confinement to whoever reads a name, and this rule is about bytes a surface
/// cannot render, not about traversal. A grant path is the same kind of value and is
/// refused the same way.
#[tokio::test]
async fn a_path_with_control_characters_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let mut raw =
        RawSocket::open(&harness, &guest_target(&room.id, &room.token), &[])
            .await
            .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({ "display_name": "Bob" }))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.joined").await["event"],
        event::ROOM_JOINED
    );

    for (id, name, path) in [
        (2_u64, method::DOC_OPEN, "src/\u{0}main.rs"),
        (3, method::DOC_OPEN, "src/main.rs\n"),
        (4, method::DOC_CLOSE, "src/\u{7}main.rs"),
        (5, method::DOC_CLOSE, "src/\u{9b}main.rs"),
    ] {
        raw.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": name,
            "params": {"path": path},
        }))
        .await
        .expect("sends");
        let refused = raw_response_for(&mut raw, id).await;
        assert_eq!(
            refused["error"]["code"],
            code::BAD_PARAMS,
            "{name} accepted {path:?}: {refused}"
        );
    }

    // A host's grant carries paths of the same kind, and an escape in one is refused too.
    let refused = host
        .grant(vec!["src/main.rs\u{1b}[0m".to_string()])
        .await
        .expect_err("a control character is refused");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected bad_params, got {refused}");
    };
    assert_eq!(code, code::BAD_PARAMS);

    // Nothing was stored or announced on the guest's connection: `..` and an absolute path
    // are still carried whole, so the next frame it sees is the grant for them.
    let listed = paths(&["..", "/etc/passwd"]);
    host.grant(listed.clone())
        .await
        .expect("the host publishes");
    let announced = next_json_within(&mut raw, "doc.granted").await;
    assert_eq!(announced["event"], event::DOC_GRANTED);
    assert_eq!(announced["params"]["paths"], serde_json::json!(listed));
}

/// A seated connection renames itself mid-session (`session.rename`, `PROTOCOL.md` §5): the
/// mover and every other peer are told with `peer.renamed`, an out-of-bound name is a
/// non-fatal `bad_params` that leaves the session usable, and a rename to the name already
/// in force is still announced.
#[tokio::test]
async fn a_peer_renames_itself_mid_session() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    let guest_id = guest.session().peer.peer_id;
    assert_eq!(wait_for_peer(&host, "Bob").await.peer_id, guest_id);

    // The room is told, and so is the mover: `peer.renamed` is addressed like `doc.opened`
    // (to everyone), not like `peer.joined` (to the others).
    guest
        .rename("Robert")
        .await
        .expect("the rename is accepted");
    assert_eq!(wait_for_peer(&host, "Robert").await.peer_id, guest_id);
    let mine = wait_for("the mover's own name", || async {
        let peer = guest.session().peer;
        (peer.display_name == "Robert").then_some(peer)
    })
    .await;
    assert_eq!(mine.display_name, "Robert");

    // 32 code points but 33 UTF-16 units, one over the handshake's bound (§5): an error
    // response, not a close. The seated connection keeps serving.
    let over_limit = format!("{}𝄞", "a".repeat(31));
    assert_eq!(over_limit.encode_utf16().count(), 33);
    let refused = guest.rename(over_limit).await.expect_err("over the bound");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected bad_params, got {refused}");
    };
    assert_eq!(code, code::BAD_PARAMS);
    assert_eq!(
        guest.session().peer.display_name,
        "Robert",
        "a refused rename changes nothing"
    );

    // The connection is still seated, and the mover's own record has the new name too.
    guest.rename("Rob").await.expect("still seated");
    let rob = wait_for("the mover's own name", || async {
        let peer = guest.session().peer;
        (peer.display_name == "Rob").then_some(peer)
    })
    .await;
    assert_eq!(rob.display_name, "Rob");
    assert_eq!(wait_for_peer(&host, "Rob").await.peer_id, guest_id);

    // A rename to the name already in force is announced like any other: the server MUST
    // NOT suppress it. The subscription is taken before the request because the engine can
    // emit the event as it reads the frame, before the response this call waits for; the
    // wait above has already drained the previous rename's event.
    let mut events = guest.subscribe();
    guest
        .rename("Rob")
        .await
        .expect("an unchanged name is a rename");
    let announced = timeout(WAIT, events.recv())
        .await
        .expect("the no-op rename is announced within the deadline")
        .expect("the engine stream stays open");
    assert!(
        matches!(announced, EngineEvent::PeersChanged { .. }),
        "a no-op rename is announced, got {announced:?}"
    );
}

/// Display names are stored trimmed: a padded `session.hello` seats as the name
/// without its padding, and a padded `session.rename` is announced trimmed —
/// padding would otherwise sit in `peers` and every surface quoting it. Blank stays
/// `bad_params`: a blank hello closes, a blank rename answers with the connection
/// open.
#[tokio::test]
async fn display_names_are_stored_trimmed() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": " Ada "}))
        .await
        .expect("says hello");
    let created = next_json_within(&mut raw, "room.created").await;
    assert_eq!(created["event"], event::ROOM_CREATED);
    assert_eq!(
        created["params"]["self"]["display_name"], "Ada",
        "the padded hello seats trimmed"
    );

    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 2,
        "method": method::SESSION_RENAME,
        "params": {"display_name": " Bob "},
    }))
    .await
    .expect("sends");
    let answered = raw_response_for(&mut raw, 2).await;
    assert!(answered["error"].is_null(), "the rename stands: {answered}");
    let renamed = raw.next_json().await.expect("the announcement");
    assert_eq!(renamed["event"], event::PEER_RENAMED);
    assert_eq!(
        renamed["params"]["display_name"], "Bob",
        "the padded rename is announced trimmed"
    );

    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 3,
        "method": method::SESSION_RENAME,
        "params": {"display_name": "   "},
    }))
    .await
    .expect("sends");
    let refused = raw_response_for(&mut raw, 3).await;
    assert_eq!(
        refused["error"]["code"],
        code::BAD_PARAMS,
        "a blank rename is refused: {refused}"
    );
}

/// The paths of a listing, as a client sends them.
fn paths(list: &[&str]) -> Vec<String> {
    list.iter().map(|path| (*path).to_string()).collect()
}

/// Waits until a client's view of the room's grant is `wanted`.
async fn wait_for_paths(engine: &SyncEngine, wanted: &[&str]) -> Vec<String> {
    let listed = paths(wanted);
    wait_for("the room's grant", || async {
        let held = engine.granted_paths().await.ok()?;
        (held == listed).then_some(held)
    })
    .await
}

/// A host publishes the room's grant, a guest that joins afterwards receives it, and a
/// republish replaces it wholesale rather than adding to it (`PROTOCOL.md` §5, §6.3). The
/// listing is the room's, not the connection's: the publisher is told by the event like
/// every other peer, and a joiner inherits it without a round trip.
#[tokio::test]
async fn a_joiner_receives_the_rooms_grant_and_a_republish_replaces_it() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");

    let listed = paths(&["README.md", "src/main.rs"]);
    host.grant(listed.clone())
        .await
        .expect("the host publishes");
    assert_eq!(
        wait_for_paths(&host, &["README.md", "src/main.rs"]).await,
        listed
    );

    // A joiner is told the listing the room holds — this one arrives as its own
    // `doc.granted`, straight from the room's grant and not from the publisher's echo.
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    assert_eq!(
        wait_for_paths(&guest, &["README.md", "src/main.rs"]).await,
        listed
    );

    // A snapshot, not a delta: the second publication drops a path by not naming it, and the
    // next joiner inherits the shorter listing.
    host.grant(paths(&["src/main.rs"]))
        .await
        .expect("the host republishes");
    assert_eq!(
        wait_for_paths(&host, &["src/main.rs"]).await,
        paths(&["src/main.rs"])
    );
    assert_eq!(
        wait_for_paths(&guest, &["src/main.rs"]).await,
        paths(&["src/main.rs"])
    );
    let late = harness
        .join(&room, "Cyd")
        .await
        .expect("a second guest joins");
    assert_eq!(
        wait_for_paths(&late, &["src/main.rs"]).await,
        paths(&["src/main.rs"])
    );

    // An empty listing is a listing: the room grants nothing, and that is announced too.
    host.grant(Vec::new()).await.expect("the host empties it");
    assert_eq!(wait_for_paths(&host, &[]).await, Vec::<String>::new());
    assert_eq!(wait_for_paths(&guest, &[]).await, Vec::<String>::new());
    assert_eq!(wait_for_paths(&late, &[]).await, Vec::<String>::new());
}
/// A joiner hears its reply before the room's grant, in that order on the wire: the
/// seat path queues both in one step under the registry lock, and reordering them
/// would hand a client room state before its own identity. The grant here is wide
/// enough that its serialization is the shape under test, not a degenerate one.
#[tokio::test]
async fn a_joiner_hears_its_reply_before_the_rooms_grant() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let listed: Vec<String> =
        (0..5_000).map(|n| format!("src/file{n:05}.rs")).collect();
    host.grant(listed.clone())
        .await
        .expect("the host publishes");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(&room.id),
        proto::percent_encode(&room.token)
    );
    let mut raw = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({ "display_name": "Zoe" }))
        .await
        .expect("says hello");
    let first = raw.next_json().await.expect("the reply");
    assert_eq!(first["event"], event::ROOM_JOINED);
    let second = raw.next_json().await.expect("the grant");
    assert_eq!(second["event"], event::DOC_GRANTED);
    assert_eq!(
        second["params"]["paths"],
        serde_json::json!(listed),
        "the joiner inherits the whole listing"
    );
}

/// Only the room's host may publish a grant. §11 has no code for "not permitted", so a
/// grant from a guest is refused `bad_params`, it changes nothing, and the connection
/// stays seated (`PROTOCOL.md` §5).
#[tokio::test]
async fn a_grant_from_a_guest_is_refused_and_changes_nothing() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    host.grant(paths(&["src/main.rs"]))
        .await
        .expect("the host publishes");
    assert_eq!(
        wait_for_paths(&guest, &["src/main.rs"]).await,
        paths(&["src/main.rs"])
    );

    let refused = guest
        .grant(paths(&["only/guest.rs"]))
        .await
        .expect_err("only the room's host publishes");
    let Error::Protocol { code, message } = refused else {
        panic!("expected bad_params, got {refused}");
    };
    assert_eq!(code, code::BAD_PARAMS);
    assert!(
        message.contains("may publish"),
        "the refusal names the permission, not just the params: {message}"
    );

    // The refusal changed nothing, and the refused connection is still usable.
    assert_eq!(
        wait_for_paths(&guest, &["src/main.rs"]).await,
        paths(&["src/main.rs"])
    );
    guest
        .open(PATH)
        .await
        .expect("the connection is still seated");
}

/// The server carries the listing in the order it was given: it does not sort,
/// deduplicate or normalise it (`PROTOCOL.md` §5, `CANONICAL.md` §2.7). Neither order nor
/// repetition here is what a publisher should write — an astral `😀` precedes the
/// fullwidth `ｆ` as UTF-16 code units and follows it as code points, `b` precedes `a`,
/// and `a` appears twice — and the room holds exactly that.
#[tokio::test]
async fn the_server_carries_the_grant_in_the_order_it_was_given() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");

    let listed = ["😀.txt", "ｆ.txt", "b.txt", "a.txt", "a.txt"];
    host.grant(paths(&listed))
        .await
        .expect("the host publishes");

    // The joiner is told the listing the room *holds*: a server that sorted or deduplicated
    // on the way in would be caught here, where the publisher's own echo would not show it.
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    assert_eq!(wait_for_paths(&guest, &listed).await, paths(&listed));
}

/// A joining connection learns the grant without a round trip, and it is told nothing when
/// the room grants nothing: `doc.granted` follows the join reply if and only if the
/// listing is non-empty (`PROTOCOL.md` §6.3). The guest's next frame here is the
/// `doc.opened` the host then produces, so a server that sent an empty listing would put a
/// frame in front of it and fail.
/// The server holds `..` and absolute paths without resolving them: §5 names them
/// as values the server carries, and confinement is the clients' mirror business
/// (§12). A grant naming them publishes whole, and a late joiner inherits them.
#[tokio::test]
async fn a_grant_naming_dotdot_and_absolute_paths_publishes_whole() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let wanted = ["..", "/etc/passwd", "../outside.rs", "src/main.rs"];
    let listed = paths(&wanted);
    host.grant(listed.clone())
        .await
        .expect("the host publishes");
    assert_eq!(wait_for_paths(&host, &wanted).await, listed);
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    assert_eq!(wait_for_paths(&guest, &wanted).await, listed);
}

#[tokio::test]
async fn a_joiner_of_a_room_that_grants_nothing_gets_no_granted_event() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let mut guest =
        RawSocket::open(&harness, &guest_target(&room.id, &room.token), &[])
            .await
            .expect("the upgrade succeeds");
    guest
        .hello(&serde_json::json!({"display_name": "Raw guest"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut guest, "room.joined").await["event"],
        event::ROOM_JOINED
    );

    host.open(PATH).await.expect("the host opens a document");
    assert_eq!(
        next_json_within(&mut guest, "doc.opened").await["event"],
        event::DOC_OPENED
    );
}

/// A grant over the server's bounds is `bad_params` with the connection left open, exactly
/// as a malformed one is: the count and the per-path length are this server's policy, not
/// the protocol's (`PROTOCOL.md` §2.1, §5). The numbers are `MAX_GRANT_PATHS` and
/// `MAX_GRANT_PATH_BYTES` in `selvaged`; a test built from the constants would only show
/// that they agree with themselves, so they are written out here.
#[tokio::test]
async fn a_grant_over_the_servers_bounds_is_refused_with_the_connection_open() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Raw host"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    let too_many = vec!["a"; 100_001];
    let too_long = "a".repeat(4097);
    let refused = [
        (2_u64, serde_json::json!({"paths": "src/main.rs"})),
        (3_u64, serde_json::json!({"paths": ["src/main.rs", 7]})),
        (4_u64, serde_json::json!({"paths": ["src/main.rs", "   "]})),
        (5_u64, serde_json::json!({})),
        (6_u64, serde_json::json!({"paths": [too_long]})),
        (7_u64, serde_json::json!({"paths": too_many})),
    ];
    for (id, params) in &refused {
        raw.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": method::DOC_GRANT,
            "params": params,
        }))
        .await
        .expect("sends");
        let refusal = raw_response_for(&mut raw, *id).await;
        assert_eq!(
            refusal["error"]["code"],
            code::BAD_PARAMS,
            "id {id}: {refusal}"
        );
        assert!(refusal["result"].is_null());
    }

    // Every refusal left the connection seated, and a listing within the bounds is
    // accepted: the response precedes the event, as a result precedes its doc.opened.
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 8,
        "method": method::DOC_GRANT,
        "params": {"paths": ["src/main.rs"]},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 8).await["result"],
        serde_json::json!({})
    );
    assert_eq!(
        next_json_within(&mut raw, "doc.granted").await["event"],
        event::DOC_GRANTED
    );
}

/// §10's compatibility rule is applied to the grammar §10 and `CANONICAL.md` §2.5 write,
/// not to whatever a number parser happens to accept: a version outside it is refused at
/// the handshake rather than seated. `selvage/1.2.3` is the spelling the TypeScript
/// engine refuses, and `selvage/01` one it accepts — both are outside the grammar, and
/// both must be refused here or the same `v` is seated by one implementation and
/// refused by the other.
#[tokio::test]
async fn a_version_outside_the_grammar_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    for version in ["selvage/1.2.3", "selvage/01"] {
        let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
            .await
            .expect("the upgrade succeeds");
        raw.send_json(&serde_json::json!({
            "v": version,
            "id": 1,
            "method": method::SESSION_HELLO,
            "params": {"display_name": "Off-grammar"},
        }))
        .await
        .expect("sends");

        let refusal = next_json_within(&mut raw, "the refusal").await;
        assert_eq!(refusal["event"], event::SESSION_ERROR);
        assert_eq!(
            refusal["params"]["code"],
            code::UNSUPPORTED_VERSION,
            "{version} is not the grammar of §10"
        );
        assert_eq!(
            raw.read_to_close()
                .await
                .expect("a close frame")
                .close_code(),
            Some(close::UNSUPPORTED_VERSION)
        );
    }
}

/// A request without an `id` is `bad_message` (§4.1, §11) whichever frame it is: the
/// rule is not only for a connection that is already seated, or a conforming server
/// refuses a `session.hello` this one seats.
#[tokio::test]
async fn a_hello_without_an_id_is_refused_before_seating() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "method": method::SESSION_HELLO,
        "params": {"display_name": "Anonymous"},
    }))
    .await
    .expect("sends");

    let refusal = next_json_within(&mut raw, "the refusal").await;
    assert_eq!(refusal["event"], event::SESSION_ERROR);
    assert_eq!(refusal["params"]["code"], code::BAD_MESSAGE);
    assert_eq!(
        raw.read_to_close()
            .await
            .expect("a close frame")
            .close_code(),
        Some(close::PROTOCOL_ERROR)
    );
}

/// The server routes document and awareness payloads and never decodes them: a frame
/// it cannot possibly understand comes out byte-identical on the other side.
#[tokio::test]
async fn document_payloads_are_relayed_verbatim() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let url = proto::session_url(&harness.ws_base(), None, None);
    let mut host = connect(&url).await.expect("connects");
    hello(&mut host, &serde_json::json!({"display_name": "Raw host"}))
        .await
        .expect("says hello");
    let created = next_json(&mut host).await.expect("room.created");
    let room_id = created["params"]["room_id"].as_str().unwrap().to_string();
    let token = created["params"]["token"].as_str().unwrap().to_string();

    let url =
        proto::session_url(&harness.ws_base(), Some(&room_id), Some(&token));
    let mut guest = connect(&url).await.expect("connects");
    hello(
        &mut guest,
        &serde_json::json!({"display_name": "Raw guest"}),
    )
    .await
    .expect("says hello");
    assert_eq!(
        next_json(&mut guest).await.expect("room.joined")["event"],
        event::ROOM_JOINED
    );
    // The host is told the guest arrived, but nothing about what it will say.
    assert_eq!(
        next_json(&mut host).await.expect("peer.joined")["event"],
        event::PEER_JOINED
    );

    open(&mut host, 2, PATH).await.expect("host opens");
    open(&mut guest, 2, PATH).await.expect("guest opens");
    assert_eq!(
        next_json(&mut host).await.expect("doc.opened")["event"],
        event::DOC_OPENED
    );

    // Not a y-protocols frame, not valid UTF-8, not valid JSON.
    let opaque =
        vec![0xff, 0x00, 0xfe, 0x7f, 0x00, 0x01, 0xde, 0xad, 0xbe, 0xef];
    host.send(Message::binary(opaque.clone()))
        .await
        .expect("sends an opaque payload");
    assert_eq!(
        next_binary(&mut guest).await.expect("relayed frame"),
        opaque
    );

    let reply = vec![0x00, 0x02, b'n', b'o', b'p', b'e'];
    guest
        .send(Message::binary(reply.clone()))
        .await
        .expect("sends an opaque payload");
    assert_eq!(next_binary(&mut host).await.expect("relayed frame"), reply);
}

#[tokio::test]
async fn a_second_host_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (_host, room) = harness.host("Ada").await.expect("host connects");

    let refused = harness
        .reclaim(&room, "Impostor")
        .await
        .expect_err("only one host at a time");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected host_present, got {refused}");
    };
    assert_eq!(code, code::HOST_PRESENT);
}

#[tokio::test]
async fn host_reconnect_within_the_grace_period_keeps_the_room() {
    let harness = Harness::start(Duration::from_secs(10)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");

    host.open(PATH).await.unwrap();
    guest.open(PATH).await.unwrap();
    host.insert(PATH, 0, "shared\n").await.unwrap();
    let seeded = wait_for("the guest to receive the seed", || async {
        let text = guest.text(PATH).await.ok()?;
        (text == "shared\n").then_some(text)
    })
    .await;
    assert_eq!(seeded, "shared\n");

    let detached = wait_for_event(&guest, "host.detached", |event| {
        matches!(event, EngineEvent::HostDetached { .. })
    });
    host.disconnect().await.expect("the host leaves cleanly");
    let EngineEvent::HostDetached { grace_ms } = detached.await else {
        panic!("host.detached");
    };
    assert_eq!(grace_ms, 10_000);

    // The room is still there for the guest, and now has no host.
    let reattached = wait_for_event(&guest, "host.attached", |event| {
        matches!(event, EngineEvent::HostAttached { .. })
    });
    let host = harness.reclaim(&room, "Ada").await.expect("host returns");
    let EngineEvent::HostAttached { peer } = reattached.await else {
        panic!("host.attached");
    };
    assert_eq!(peer.display_name, "Ada");
    assert_eq!(peer.role, Role::Host);

    // Same room, same document, same content.
    assert_eq!(host.session().room_id, guest.session().room_id);
    let documents = host.documents().await.unwrap();
    assert!(documents.contains(&PATH.to_string()), "got {documents:?}");
    assert_eq!(guest.text(PATH).await.unwrap(), "shared\n");
}

/// `host.attached` means a host reclaimed the room (§6, §9.1), so a guest that joins
/// while the room is between hosts must not produce one: a conforming peer told
/// `host.attached` concludes the room has a host again and can then let it die under it.
/// The guest's own `doc.open` is a barrier — one connection's frames are answered in
/// order — so what arrives between the join and that answer is pinned, not timed.
#[tokio::test]
async fn a_guest_joining_during_the_grace_period_is_not_announced_as_the_host()
{
    let harness = Harness::start(Duration::from_secs(10)).await;
    let mut host = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    host.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    let created = next_json_within(&mut host, "room.created").await;
    let room_id = created["params"]["room_id"].as_str().unwrap().to_string();
    let token = created["params"]["token"].as_str().unwrap().to_string();
    let target = guest_target(&room_id, &token);

    let mut watcher = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the upgrade succeeds");
    watcher
        .hello(&serde_json::json!({"display_name": "Bob"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut watcher, "room.joined").await["event"],
        event::ROOM_JOINED
    );

    // The host leaves; the room survives between hosts, which is the whole grace period.
    drop(host);
    assert_eq!(
        next_json_within(&mut watcher, "peer.left").await["event"],
        event::PEER_LEFT
    );
    assert_eq!(
        next_json_within(&mut watcher, "host.detached").await["event"],
        event::HOST_DETACHED
    );

    let mut late = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the upgrade succeeds");
    late.hello(&serde_json::json!({"display_name": "Cleo"}))
        .await
        .expect("says hello");
    let joined = next_json_within(&mut watcher, "peer.joined").await;
    assert_eq!(
        joined["event"],
        event::PEER_JOINED,
        "a guest joining a hostless room is a peer, not a host: {joined}"
    );
    assert_eq!(joined["params"]["peer"]["role"], "guest");
    assert_eq!(joined["params"]["peer"]["display_name"], "Cleo");

    late.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 2,
        "method": method::DOC_OPEN,
        "params": {"path": PATH},
    }))
    .await
    .expect("sends");
    assert_eq!(
        next_json_within(&mut watcher, "doc.opened").await["event"],
        event::DOC_OPENED,
        "nothing else may be announced between the join and the next event"
    );
}

#[tokio::test]
async fn the_room_dies_when_the_host_does_not_come_back() {
    let harness = Harness::start(Duration::from_millis(300)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    guest.open(PATH).await.unwrap();

    let gone = wait_for_event(&guest, "room.gone", |event| {
        matches!(event, EngineEvent::RoomGone { .. })
    });
    host.disconnect().await.expect("the host leaves cleanly");
    let EngineEvent::RoomGone { reason } = gone.await else {
        panic!("room.gone");
    };
    assert_eq!(reason, "host did not return");

    // The room really is gone: the id cannot be reused, even with its token.
    let refused = harness
        .join(&room, "Late")
        .await
        .expect_err("the room is gone");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected room_unknown, got {refused}");
    };
    assert_eq!(code, code::ROOM_UNKNOWN);

    // And the guest's connection is closed by the server.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && guest.text(PATH).await.is_ok() {
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        guest.text(PATH).await.is_err(),
        "the server should have closed the abandoned guest's connection"
    );
}

/// The room dies with a guest still connected: the server closes that guest, and the
/// connection task ends having written its own close frame. Ending it must not panic —
/// the harness fails this test if any task panics while it runs, so waiting for the
/// socket to close for good is the assertion that matters.
#[tokio::test]
async fn a_guest_left_in_a_dead_room_is_closed_cleanly() {
    let harness = Harness::start(Duration::from_millis(300)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(&room.id),
        proto::percent_encode(&room.token)
    );
    let mut guest = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the upgrade succeeds");
    guest
        .hello(&serde_json::json!({"display_name": "Raw guest"}))
        .await
        .expect("says hello");
    assert_eq!(
        guest.next_json().await.expect("room.joined")["event"],
        event::ROOM_JOINED
    );

    host.disconnect().await.expect("the host leaves");
    let closing = timeout(WAIT, guest.read_to_close())
        .await
        .expect("the room dies within its grace period")
        .expect("the server sends a close frame");
    assert_eq!(closing.close_code(), Some(close::ROOM_GONE));
    // The socket closes for good once the connection task is done, which is the point
    // after which nothing it did can still be running.
    timeout(WAIT, guest.read_to_eof())
        .await
        .expect("the server closes the socket")
        .expect("a clean close");
}

/// The upgrade request and the first frame in one write, which is what a client that
/// pipelines its handshake sends. The bytes behind the HTTP head belong to the
/// WebSocket parser: dropping them costs the connection its `session.hello`, and the
/// session an unexplained timeout.
#[tokio::test]
async fn a_frame_behind_the_http_head_reaches_the_session() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let hello = serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 1,
        "method": method::SESSION_HELLO,
        "params": {"display_name": "Coalesced"},
    })
    .to_string();
    let frame = client_frame(0x1, hello.as_bytes());
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &frame)
        .await
        .expect("the upgrade succeeds");

    let created = timeout(WAIT, raw.next_json())
        .await
        .expect("the server answers the hello that arrived with the head")
        .expect("room.created");
    assert_eq!(created["event"], event::ROOM_CREATED);
}

#[tokio::test]
async fn a_guest_leaving_is_announced_and_its_presence_is_cleaned_up() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    guest
        .set_selection(PATH, SelectionOffsets::caret(3))
        .await
        .unwrap();

    let seen = wait_for("Bob's cursor", || async {
        host.presence()
            .await
            .ok()?
            .into_iter()
            .find(|p| p.display_name() == Some("Bob"))
    })
    .await;
    assert!(seen.state.is_some());

    let left = wait_for_event(
        &host,
        "peer.left",
        |event| matches!(event, EngineEvent::PeersChanged { peers } if peers.is_empty()),
    );
    guest.disconnect().await.expect("guest leaves cleanly");
    left.await;

    let presence = wait_for("Bob's presence to be forgotten", || async {
        let presence = host.presence().await.ok()?;
        let remote = presence
            .iter()
            .filter(|p| p.display_name() == Some("Bob"))
            .count();
        (remote == 0).then_some(presence.len())
    })
    .await;
    assert_eq!(presence, 1, "only the host's own awareness is left");
}

/// The open-document set is the room's, but a document is open because peers hold it
/// open: one peer closing a document the host still has must not close it for the room.
#[tokio::test]
async fn closing_a_document_leaves_the_peers_that_still_hold_it() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    host.open(PATH).await.expect("the host opens the document");
    guest
        .open(PATH)
        .await
        .expect("the guest opens the document");

    // The guest closes the document the host still holds open.
    guest
        .close(PATH)
        .await
        .expect("the guest closes the document");

    // A document only the guest holds bounds the frames before it: one connection's
    // frames are processed in order, so the room knowing this one means it has already
    // processed the close. The document stays open, so the barrier is observable.
    guest
        .open(GUEST_ONLY)
        .await
        .expect("the guest opens its own document");
    let on_host =
        wait_for("the room to know the guest's own document", || async {
            let documents = host.documents().await.ok()?;
            documents
                .contains(&GUEST_ONLY.to_string())
                .then_some(documents)
        })
        .await;
    assert!(
        on_host.contains(&PATH.to_string()),
        "the host still holds the document open: {on_host:?}"
    );

    // The client that closed it agrees, and so does one that joins afterwards.
    assert!(guest.documents().await.unwrap().contains(&PATH.to_string()));
    let late = harness
        .join(&room, "Cleo")
        .await
        .expect("a late joiner connects");
    assert!(
        late.documents().await.unwrap().contains(&PATH.to_string()),
        "a late joiner is still told the document is open"
    );
}

/// A connection with no room in its URL mints one, so it is that room's host whatever
/// `session.hello` claims: a claimed `guest` role must not produce a hostless room, and
/// the room must not then reject the host that made it.
#[tokio::test]
async fn a_connection_that_mints_a_room_is_seated_as_its_host() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut minter = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    minter
        .hello(&serde_json::json!({
            "display_name": "Minter",
            "role": "guest",
        }))
        .await
        .expect("says hello");
    let created = minter.next_json().await.expect("room.created");
    assert_eq!(created["event"], event::ROOM_CREATED);
    assert_eq!(
        created["params"]["self"]["role"], "host",
        "the connection that minted the room hosts it"
    );

    // And the room really has a host: a second claim on the role is refused.
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        created["params"]["room_id"].as_str().unwrap(),
        created["params"]["token"].as_str().unwrap()
    );
    let mut claimant = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the upgrade succeeds");
    claimant
        .hello(&serde_json::json!({
            "display_name": "Claimant",
            "role": "host",
        }))
        .await
        .expect("says hello");
    assert_eq!(
        claimant.next_json().await.expect("refusal")["params"]["code"],
        code::HOST_PRESENT
    );
}

/// A request the server refuses is an error at the caller, and local state only ever
/// reflects what the server accepted: a refused document is not one this client thinks
/// it has open.
#[tokio::test]
async fn a_refused_request_fails_and_leaves_local_state_alone() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");

    // The server rejects an empty path with `bad_params`.
    let refused = host
        .open("")
        .await
        .expect_err("the server refuses an empty path");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected bad_params, got {refused}");
    };
    assert_eq!(code, code::BAD_PARAMS);
    assert!(
        host.open_documents().await.unwrap().is_empty(),
        "the refusal must not leave a document open locally"
    );
    assert!(host.documents().await.unwrap().is_empty());

    // An accepted request is reflected locally, and the peer is told.
    host.open(PATH).await.expect("the host opens a document");
    assert!(
        host.open_documents()
            .await
            .unwrap()
            .contains(&PATH.to_string())
    );
    wait_for("the guest to hear about the document", || async {
        let documents = guest.documents().await.ok()?;
        documents.contains(&PATH.to_string()).then_some(())
    })
    .await;
}

/// `doc.open` and `doc.close` carry the same `path` field, so they validate it the same
/// way: a path that is empty and a path that is all whitespace are both `bad_params` at
/// both methods, and the connection survives either. The answer the request got is the
/// evidence — a blank path that is accepted shows up as a result, and one that is
/// refused as an `error` with the request's own id.
#[tokio::test]
async fn doc_open_and_doc_close_validate_the_path_the_same_way() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    for (id, name, path) in [
        (2_u64, method::DOC_OPEN, ""),
        (3, method::DOC_OPEN, "   "),
        (4, method::DOC_CLOSE, ""),
        (5, method::DOC_CLOSE, "   "),
    ] {
        raw.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": name,
            "params": {"path": path},
        }))
        .await
        .expect("sends");
        let refused = raw_response_for(&mut raw, id).await;
        assert_eq!(
            refused["error"]["code"],
            code::BAD_PARAMS,
            "{name} accepted the path {path:?}"
        );
    }

    // A real path is still a real path: the check refuses a bad one, not every one.
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 6,
        "method": method::DOC_OPEN,
        "params": {"path": PATH},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 6).await["result"]["documents"],
        serde_json::json!([PATH])
    );
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 7,
        "method": method::DOC_CLOSE,
        "params": {"path": PATH},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 7).await["result"]["documents"],
        serde_json::json!([])
    );
}

/// A frame that repeats a member name anywhere in it is `bad_message` (`PROTOCOL.md` §4).
/// The envelope's own members were always refused by the struct parse, but a member of
/// `params`, or of anything nested below it, is last-wins to a JSON reader: one frame
/// meant two things and two implementations could take different ones. A seated fault is
/// a `session.error` with the connection open, and the connection still serves afterwards.
#[tokio::test]
async fn a_frame_with_a_repeated_member_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    for body in [
        // A member of `params` twice.
        r#"{"v":"selvage/1","id":2,"method":"doc.open","params":{"path":"a.rs","path":"b.rs"}}"#,
        // A member of an object nested below `params`.
        r#"{"v":"selvage/1","id":3,"method":"doc.open","params":{"path":"a.rs","extra":{"x":1,"x":2}}}"#,
        // The envelope's own member twice, which `serde` refuses as well.
        r#"{"v":"selvage/1","id":4,"method":"doc.open","params":{"path":"a.rs"},"v":"selvage/1"}"#,
    ] {
        raw.send(0x1, body.as_bytes()).await.expect("sends");
        let refused = next_json_within(&mut raw, "the refusal").await;
        assert_eq!(
            refused["event"],
            event::SESSION_ERROR,
            "one frame with two readings is refused: {refused}"
        );
        assert_eq!(refused["params"]["code"], code::BAD_MESSAGE);
        let message = refused["params"]["message"].as_str().expect("a message");
        assert!(
            message.contains("duplicate"),
            "the refusal names the repetition: {message}"
        );
    }

    // The connection is still seated and still answers: a refused frame changed nothing.
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 5,
        "method": method::DOC_OPEN,
        "params": {"path": PATH},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 5).await["result"]["documents"],
        serde_json::json!([PATH])
    );
}

/// A join query that names `room` or `token` twice is refused (`PROTOCOL.md` §5.1): a
/// connection whose room depended on which of two values was read last is not one a server
/// may seat, and the client cannot tell which room it asked for. It is a pre-seat fault, so
/// it is the refusal the spec names for a malformed URL — the join refusal `token_invalid`,
/// the code a room whose named token is not the room's already gets — and close 4002 (§11).
#[tokio::test]
async fn a_join_query_naming_a_parameter_twice_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    assert_eq!(host.session().room_id, room.id);

    for target in [
        format!(
            "{}?room={}&room={}&token={}",
            proto::ENDPOINT_PATH,
            room.id,
            proto::percent_encode("r-other"),
            proto::percent_encode(&room.token)
        ),
        format!(
            "{}?room={}&token={}&token={}",
            proto::ENDPOINT_PATH,
            proto::percent_encode(&room.id),
            proto::percent_encode(&room.token),
            proto::percent_encode(&room.token)
        ),
    ] {
        let mut raw = RawSocket::open(&harness, &target, &[])
            .await
            .expect("the upgrade succeeds");
        let refused = next_json_within(&mut raw, "the refusal").await;
        assert_eq!(refused["event"], event::SESSION_ERROR);
        assert_eq!(
            refused["params"]["code"],
            code::TOKEN_INVALID,
            "a malformed join URL takes the join refusal §5.1 names: {refused}"
        );
        assert_eq!(
            raw.read_to_close().await.expect("a close").close_code(),
            Some(close::TOKEN_INVALID)
        );
    }

    // A query naming each once is still the invite it looks like.
    let mut guest =
        RawSocket::open(&harness, &guest_target(&room.id, &room.token), &[])
            .await
            .expect("the upgrade succeeds");
    guest
        .hello(&serde_json::json!({"display_name": "Bob"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut guest, "room.joined").await["event"],
        event::ROOM_JOINED
    );
}

/// A `doc.open` path is bounded like a grant path: 4096 bytes, the grant's own bound
/// (`PROTOCOL.md` §5). A megabyte path was accepted, stored in the room's set and
/// broadcast whole to every peer; both methods now refuse it `bad_params` with the
/// connection open. The numbers are written out: a test built from the constant would
/// only show that it agrees with itself.
#[tokio::test]
async fn doc_open_and_doc_close_bound_the_path_like_a_grant() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    let huge = "a".repeat(1024 * 1024);
    let over = "a".repeat(4097);
    for (id, name, path) in [
        (2_u64, method::DOC_OPEN, huge.as_str()),
        (3, method::DOC_CLOSE, huge.as_str()),
        (4, method::DOC_OPEN, over.as_str()),
        (5, method::DOC_CLOSE, over.as_str()),
    ] {
        raw.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": name,
            "params": {"path": path},
        }))
        .await
        .expect("sends");
        let refused = raw_response_for(&mut raw, id).await;
        assert_eq!(
            refused["error"]["code"],
            code::BAD_PARAMS,
            "{name} with id {id} accepted an over-long path"
        );
    }

    // The boundary still seats: 4096 bytes open and close like any other path.
    let at = "a".repeat(4096);
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 6,
        "method": method::DOC_OPEN,
        "params": {"path": at},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 6).await["result"]["documents"],
        serde_json::json!([at])
    );
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 7,
        "method": method::DOC_CLOSE,
        "params": {"path": at},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 7).await["result"]["documents"],
        serde_json::json!([])
    );
}

/// A close reason is control-frame payload: RFC 6455 allows 125 bytes, two of which the
/// status code takes. A reason built from client input — here a two-thousand-byte wire
/// version — has to be cut down, or a conforming client rejects the frame instead of
/// learning the close code.
#[tokio::test]
async fn a_long_close_reason_is_cut_down_to_fit_a_control_frame() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    let version = format!("selvage/{}", "1".repeat(2000));
    raw.send_json(&serde_json::json!({
        "v": version,
        "id": 1,
        "method": method::SESSION_HELLO,
        "params": {"display_name": "Long"},
    }))
    .await
    .expect("says hello");

    let refusal = raw.next_json().await.expect("the refusal");
    assert_eq!(refusal["params"]["code"], code::UNSUPPORTED_VERSION);
    let closing = raw.read_to_close().await.expect("a close frame");
    assert_eq!(closing.close_code(), Some(close::UNSUPPORTED_VERSION));
    let reason = closing.reason().expect("a reason");
    assert!(
        reason.len() <= 123,
        "a close reason must fit a control frame, got {} bytes",
        reason.len()
    );
    assert!(
        reason.starts_with("unsupported wire version"),
        "the reason still says what went wrong: {reason}"
    );
}
/// `HEAD /meta` answers like `GET` but headers-only: the status line and headers —
/// including the body's length — arrive with no body after them (RFC 9110 §9.3.2).
/// Anything besides `GET` and `HEAD` is `405`: `POST` creates nothing, so `200`
/// with a body would be a lie.
#[tokio::test]
async fn the_negotiation_endpoint_checks_the_request_method() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let addr = harness.ws_base().trim_start_matches("ws://").to_string();
    let get_body = http_get(&format!("{}/meta", harness.http_base()))
        .await
        .expect("meta answers");

    let mut head_stream = TcpStream::connect(&addr).await.expect("connects");
    head_stream
        .write_all(b"HEAD /meta HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
        .await
        .expect("sends a HEAD request");
    let mut head_response = String::new();
    head_stream
        .read_to_string(&mut head_response)
        .await
        .expect("reads the response");
    assert!(
        head_response.starts_with("HTTP/1.1 200"),
        "got {head_response:?}"
    );
    let (head, body) =
        head_response.split_once("\r\n\r\n").expect("headers end");
    assert!(body.is_empty(), "HEAD carries no body: {head_response:?}");
    assert!(
        head.contains(&format!("content-length: {}", get_body.len())),
        "HEAD advertises the GET length: {head_response:?}"
    );

    let mut post_stream = TcpStream::connect(&addr).await.expect("connects");
    post_stream
        .write_all(b"POST /meta HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await
        .expect("sends a POST request");
    let mut post_response = String::new();
    post_stream
        .read_to_string(&mut post_response)
        .await
        .expect("reads the response");
    assert!(
        post_response.starts_with("HTTP/1.1 405"),
        "POST is refused: {post_response:?}"
    );
    assert!(
        post_response.contains("allow: GET, HEAD"),
        "the refusal names what it takes: {post_response:?}"
    );
}

/// An absolute-form request target names the same resource: a proxy forwarding
/// a `GET` with an absolute URI is legal HTTP, and §12 puts one in front of any
/// public deployment, so the origin form it carries must route.
#[tokio::test]
async fn absolute_form_targets_route_like_origin_form() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let addr = harness.ws_base().trim_start_matches("ws://").to_string();
    let mut stream = TcpStream::connect(&addr).await.expect("connects");
    stream
        .write_all(
            format!("GET http://{addr}/meta HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("sends an absolute-form request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("reads the response");
    assert!(response.starts_with("HTTP/1.1 200"), "got {response:?}");
    let (_, body) = response.split_once("\r\n\r\n").expect("a body");
    assert!(body.contains("wire_versions"), "got {body:?}");
}

/// A binary first frame is a fault in the *shape* of the frame, so it is reported as
/// `bad_message` and closes with 4000 — not as `hello_required`, which is for a text frame
/// that is not `session.hello`.
#[tokio::test]
async fn a_binary_first_frame_is_a_bad_message() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.send(0x2, &[0x00, 0x01, 0x02])
        .await
        .expect("sends a binary frame");

    assert_eq!(
        raw.next_json().await.expect("the refusal")["params"]["code"],
        code::BAD_MESSAGE
    );
    assert_eq!(
        raw.read_to_close()
            .await
            .expect("a close frame")
            .close_code(),
        Some(close::PROTOCOL_ERROR)
    );
}

/// A connection that never finishes its HTTP request head is dropped: a client cannot hold
/// a connection open by sending half a request and stopping.
#[tokio::test]
async fn a_half_written_request_head_is_dropped() {
    let harness = Harness::start_with(ServerConfig {
        head_timeout: Duration::from_millis(100),
        ..ServerConfig::default()
    })
    .await;
    let addr = harness.ws_base().trim_start_matches("ws://").to_string();
    let mut stream = TcpStream::connect(&addr).await.expect("connects");
    stream
        .write_all(b"GET /session HTTP/1.1\r\nhost: localhost\r\n")
        .await
        .expect("writes half a head");

    let mut scratch = [0u8; 16];
    let read = timeout(WAIT, stream.read(&mut scratch))
        .await
        .expect("the server gives up on the head")
        .expect("a read");
    assert_eq!(read, 0, "the connection is closed, not held open");
}

async fn http_get(url: &str) -> Result<String, Failure> {
    let address = url.trim_start_matches("http://");
    let (host, path) = address.split_once('/').ok_or("the URL has no path")?;
    let mut stream = TcpStream::connect(host).await?;
    let request = format!(
        "GET /{path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or("an HTTP body follows the headers")?;
    Ok(body.to_string())
}

/// A wrong HTTP path names the two paths that exist, so a newcomer pointing a
/// browser at the server learns where to go instead of seeing a bare 404.
#[tokio::test]
async fn unknown_http_paths_name_the_two_real_ones() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let body = http_get(&format!("{}/nope", harness.http_base()))
        .await
        .expect("a wrong path still answers");
    assert!(body.contains("/session"), "the WebSocket path: {body}");
    assert!(body.contains("/meta"), "the HTTP path: {body}");
}

/// A peer that stops reading is disconnected, not buffered forever. Its queue is
/// bounded, so the first broadcast past a full queue removes it: the room is told
/// `peer.left`, its task is stopped, and everyone else keeps being served. Filling the
/// queue past the socket takes megabytes — 32 frames plus the kernel's — so the flood
/// is 1 MiB binary frames, which stay well under the frame bound and stop just past the
/// cap. Past that the room is flooding the surviving peers, not the slow one, and under
/// coverage a peer that only reads is overtaken and ejected in turn.
#[tokio::test]
async fn a_peer_that_stops_reading_is_disconnected_and_announced() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(&room.id),
        proto::percent_encode(&room.token)
    );

    // The slow peer joins, proves it is seated, and then never reads again.
    let mut slow = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the slow peer joins");
    slow.hello(&serde_json::json!({"display_name": "Bob"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut slow, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    wait_for_peer(&host, "Bob").await;

    // A second raw peer floods the room with binary the slow one never reads.
    let mut flood = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the flooding peer joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut flood, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    let mut progress = host.subscribe();
    let big = vec![0xA5u8; 1024 * 1024];
    for _ in 0..48 {
        flood.send(0x2, &big).await.expect("floods");
        // Wait for the host to read the frame before sending the next: the flood races
        // the host, the host is the peer that must survive it, and unpaced the host is
        // overtaken and ejected for falling behind like the slow peer.
        pace(&mut progress)
            .await
            .expect("the observer reads the flood frame");
    }

    // The room announces the removal, and the slow socket ends.
    wait_for_described_within(
        EJECTION_WAIT,
        "the host to see the slow peer leave",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Bob")).then_some(())
        },
    )
    .await;
    match timeout(EJECTION_WAIT, slow.read_to_end())
        .await
        .expect("the server ends the slow connection")
    {
        None | Some(ErrorKind::ConnectionReset) => {}
        Some(kind) => {
            panic!("the slow socket ends; it does not linger: {kind:?}")
        }
    }

    // The room keeps serving everyone else.
    host.open(PATH).await.expect("the host still opens");
}

/// A reply the queue will not take ends the session the same way a broadcast past a
/// full queue does: the peer stopped reading, so the server removes it rather than
/// stack more behind the unread frames. Replies echo the room's document set, so a few
/// hundred opens of long paths overflow any socket past the bounded queue.
#[tokio::test]
async fn a_reply_past_a_full_queue_ends_the_session() {
    let harness = Harness::start(Duration::from_millis(300)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    let created = next_json_within(&mut raw, "room.created").await;
    let room_id = created["params"]["room_id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let token = created["params"]["token"]
        .as_str()
        .unwrap_or("")
        .to_string();

    // Hundreds of opens, none of them read: the replies alone exceed any socket.
    let path = "a".repeat(4000);
    for id in 2..302_u64 {
        raw.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": method::DOC_OPEN,
            "params": {"path": format!("{path}-{id}")},
        }))
        .await
        .expect("sends");
    }
    match timeout(WAIT, raw.read_to_end())
        .await
        .expect("the server ends the session")
    {
        None | Some(ErrorKind::ConnectionReset) => {}
        Some(kind) => panic!("the socket ends; it does not linger: {kind:?}"),
    }

    // The room died with its host: the grace period ran out long before the socket did,
    // so the id refuses like an id that was never minted.
    let url =
        proto::session_url(&harness.ws_base(), Some(&room_id), Some(&token));
    let mut late = connect(&url).await.expect("connects");
    hello(&mut late, &serde_json::json!({"display_name": "Late"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json(&mut late).await.expect("refusal")["params"]["code"],
        code::ROOM_UNKNOWN
    );
}

/// Minting past the room cap is refused, on an engine connection and on a raw one.
/// The refusal is the server's own `x.server_full`: capacity is policy, not contract
/// (`PROTOCOL.md` §2.1), and the close is the generic 4000.
#[tokio::test]
async fn minting_past_the_room_cap_is_refused() {
    let harness = Harness::start_with(ServerConfig {
        max_rooms: 1,
        ..ServerConfig::default()
    })
    .await;
    let (_ada, _first) = harness.host("Ada").await.expect("mints");
    let refused = harness.host("Bob").await.expect_err("no second room");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected x.server_full, got {refused}");
    };
    assert_eq!(code, "x.server_full");

    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Raw"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "refusal").await["params"]["code"],
        "x.server_full"
    );
    assert_eq!(
        raw.read_to_close().await.expect("close frame").close_code(),
        Some(close::PROTOCOL_ERROR)
    );
}

/// A full room refuses guests but not its host: the room's owner reclaims past the cap,
/// since a host that cannot come back to a full room loses the room.
#[tokio::test]
async fn a_full_room_refuses_guests_but_not_its_host() {
    let harness = Harness::start_with(ServerConfig {
        max_peers_per_room: 2,
        room_grace: Duration::from_secs(30),
        ..ServerConfig::default()
    })
    .await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let _bob = harness.join(&room, "Bob").await.expect("a guest joins");
    // The room is observably full before the refusal is attempted: two seated peers
    // against a cap of two, so a seat for Mallory would mean the cap did not apply.
    wait_for_peer(&host, "Bob").await;
    let refused = harness.join(&room, "Mallory").await.expect_err("full");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected x.room_full, got {refused}");
    };
    assert_eq!(code, "x.room_full");

    // The host drops; a guest takes the freed seat; the host still reclaims past it.
    // Every engine stays bound: dropping one disconnects it, and the freed seat would
    // let the next join in for the wrong reason.
    host.disconnect().await.expect("the host leaves");
    let _mallory = harness
        .join(&room, "Mallory")
        .await
        .expect("the seat freed");
    let back = harness
        .reclaim(&room, "Ada")
        .await
        .expect("the host reclaims");
    assert_eq!(back.session().role, Role::Host);
    assert_eq!(back.session().room_id, room.id);
}

/// Opening past the document cap is refused with the connection open: the set is the
/// room's, and a freed entry opens again.
#[tokio::test]
async fn opening_past_the_document_cap_is_refused_and_frees_again() {
    let harness = Harness::start_with(ServerConfig {
        max_documents_per_room: 2,
        ..ServerConfig::default()
    })
    .await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    for (id, path, wanted) in [
        (2_u64, "d1", serde_json::json!(["d1"])),
        (3, "d2", serde_json::json!(["d1", "d2"])),
    ] {
        raw.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": method::DOC_OPEN,
            "params": {"path": path},
        }))
        .await
        .expect("sends");
        assert_eq!(
            raw_response_for(&mut raw, id).await["result"]["documents"],
            wanted
        );
    }
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 4,
        "method": method::DOC_OPEN,
        "params": {"path": "d3"},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 4).await["error"]["code"],
        "x.room_full"
    );

    // The refusal changed nothing and the connection is still seated: closing frees an
    // entry, and the refused path opens into it.
    for (id, name, path, wanted) in [
        (5_u64, method::DOC_CLOSE, "d1", serde_json::json!(["d2"])),
        (6, method::DOC_OPEN, "d3", serde_json::json!(["d2", "d3"])),
    ] {
        raw.send_json(&serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": name,
            "params": {"path": path},
        }))
        .await
        .expect("sends");
        assert_eq!(
            raw_response_for(&mut raw, id).await["result"]["documents"],
            wanted
        );
    }
}

/// A grant is bounded in total bytes, not just count and per-path length. 1100 paths of
/// 4096 bytes (4,505,600 in total, past the 4 MiB budget) are refused `bad_params`,
/// while 20,000 short paths (~200,000 bytes) publish whole and reach a late joiner
/// whole — bounded by what could be published.
#[tokio::test]
async fn a_grant_over_the_total_byte_budget_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    let created = next_json_within(&mut raw, "room.created").await;
    assert_eq!(created["event"], event::ROOM_CREATED);
    let room_id = created["params"]["room_id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let token = created["params"]["token"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let oversized = vec!["a".repeat(4096); 1100];
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 2,
        "method": method::DOC_GRANT,
        "params": {"paths": oversized},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 2).await["error"]["code"],
        code::BAD_PARAMS
    );

    // The refusal left the connection seated: a listing within every bound publishes.
    let listed: Vec<String> =
        (0..20_000).map(|n| format!("src/{n:05}.rs")).collect();
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 3,
        "method": method::DOC_GRANT,
        "params": {"paths": listed},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 3).await["result"],
        serde_json::json!({})
    );
    assert_eq!(
        next_json_within(&mut raw, "doc.granted").await["event"],
        event::DOC_GRANTED
    );

    // A late joiner inherits the whole listing — bounded, like the publish, by what
    // the budget lets a host send.
    let invite =
        proto::session_url(&harness.ws_base(), Some(&room_id), Some(&token));
    let late = harness.join_url(&invite, "Late").await.expect("late joins");
    let held = wait_for("the late joiner to inherit the grant", || async {
        let held = late.granted_paths().await.ok()?;
        (held == listed).then_some(held)
    })
    .await;
    let total: usize = held.iter().map(String::len).sum();
    assert!(
        total <= 4 * 1024 * 1024,
        "the delivery stays within the budget: {total}"
    );
}

/// A frame over the bound ends the connection the way a dropped socket does: no
/// `session.error`, and the room learns of it as `peer.left` (`PROTOCOL.md` §2.1). A
/// 1 MiB binary still relays whole and byte-identical — the bound is 8 MiB, written
/// out here.
#[tokio::test]
async fn a_frame_over_the_bound_ends_the_connection() -> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(&room.id),
        proto::percent_encode(&room.token)
    );
    let mut flood = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the sender joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    let joined = next_json_within(&mut flood, "room.joined").await;
    assert_eq!(joined["event"], event::ROOM_JOINED);
    let flo = joined["params"]["self"]["peer_id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    wait_for_peer(&host, "Flo").await;

    let mut watched = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the watcher joins");
    watched
        .hello(&serde_json::json!({"display_name": "Watch"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut watched, "room.joined").await["event"],
        event::ROOM_JOINED
    );

    // The control: 1 MiB relays to the watcher byte-identical. Binary frames the
    // host engine sent on joining come first, so the watcher reads past those.
    let big = vec![0xA5u8; 1024 * 1024];
    flood.send(0x2, &big).await.expect("sends 1 MiB");
    let relayed = timeout(WAIT, read_binary_until(&mut watched.stream, &big))
        .await
        .expect("the 1 MiB relay arrives")?;
    assert_eq!(relayed.payload, big);

    // 9 MiB ends the sender's connection with nothing on the wire to say why. The
    // server closes on the over-bound header, so the send itself can fail once the
    // payload outgrows the socket buffers; either way the socket ends and lingers
    // nowhere.
    let huge = vec![0xA5u8; 9 * 1024 * 1024];
    if let Err(failed) = flood.send(0x2, &huge).await {
        assert!(
            socket_ended(&failed),
            "the oversize send ends the socket: {failed}"
        );
    }
    match timeout(WAIT, flood.read_to_end())
        .await
        .expect("the server ends the oversize connection")
    {
        None | Some(ErrorKind::ConnectionReset) => {}
        Some(kind) => panic!("the socket ends; it does not linger: {kind:?}"),
    }

    // The room learned of it as a drop: the watcher reads `peer.left` for Flo, and the
    // host stops seeing them.
    let left = next_json_within(&mut watched, "peer.left").await;
    assert_eq!(left["event"], event::PEER_LEFT);
    assert_eq!(left["params"]["peer_id"], flo.as_str());
    wait_for_described(
        "the host to stop seeing the dropped sender",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Flo")).then_some(())
        },
    )
    .await;
    Ok(())
}

/// Fragments reassemble before the bound bites: nine 1 MiB fragments of one binary
/// message are 9 MiB past the 8 MiB bound, and the connection ends the way a
/// single over-bound frame ends it — both knobs are set, but only the single
/// frame was pinned. Binary, so no UTF-8 fault ends it first; the room learns of
/// it as a drop, like any transport end.
#[tokio::test]
async fn fragments_past_the_bound_end_the_connection() -> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(&room.id),
        proto::percent_encode(&room.token)
    );
    let mut flood = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the sender joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    let joined = next_json_within(&mut flood, "room.joined").await;
    assert_eq!(joined["event"], event::ROOM_JOINED);
    wait_for_peer(&host, "Flo").await;

    // Nine 1 MiB fragments, FIN clear but the last: the message is 9 MiB, past the
    // bound no single frame breaks. The final send can fail once the payload
    // outgrows the socket buffers, like the single-frame pin.
    let big = vec![0xA5u8; 1024 * 1024];
    flood
        .send_fragment(0x2, false, &big)
        .await
        .expect("the first fragment");
    for _ in 0..7 {
        flood
            .send_fragment(0x0, false, &big)
            .await
            .expect("a middle fragment");
    }
    if let Err(failed) = flood.send_fragment(0x0, true, &big).await {
        assert!(
            socket_ended(&failed),
            "the oversize send ends the socket: {failed}"
        );
    }
    match timeout(WAIT, flood.read_to_end())
        .await
        .expect("the server ends the fragmented connection")
    {
        None | Some(ErrorKind::ConnectionReset) => {}
        Some(kind) => panic!("the socket ends; it does not linger: {kind:?}"),
    }

    // The room learned of it as a drop, and the host stops seeing the sender.
    wait_for_described(
        "the host to stop seeing the dropped sender",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Flo")).then_some(())
        },
    )
    .await;
    Ok(())
}

/// The grant byte budget is exact: 1024 paths of 4096 bytes are exactly 4 MiB and
/// publish, one byte more is refused `bad_params`, and the connection stays seated
/// throughout. Written out, not built from the constants, so the test pins the
/// boundary instead of agreeing with it.
#[tokio::test]
async fn grant_byte_boundaries_are_exact() {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    // Exactly 4 MiB of path bytes publishes: the response precedes the event.
    let at_limit = vec!["a".repeat(4096); 1024];
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 2,
        "method": method::DOC_GRANT,
        "params": {"paths": at_limit},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 2).await["result"],
        serde_json::json!({})
    );
    assert_eq!(
        next_json_within(&mut raw, "doc.granted").await["event"],
        event::DOC_GRANTED
    );

    // One byte more is refused, and the refusal leaves the connection seated: a
    // listing within every bound publishes right after.
    let mut over = vec!["a".repeat(4096); 1024];
    over.push("b".to_string());
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 3,
        "method": method::DOC_GRANT,
        "params": {"paths": over},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 3).await["error"]["code"],
        code::BAD_PARAMS
    );
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 4,
        "method": method::DOC_GRANT,
        "params": {"paths": ["src/main.rs"]},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 4).await["result"],
        serde_json::json!({})
    );
}

/// A large-but-legitimate listing publishes whole. 25,000 working-tree paths carry
/// 893,750 path bytes — the shape a large checkout shares — and 100,000 typical
/// paths carry ~3.5 MiB, the most the count cap admits; both publish, and a late
/// joiner inherits each whole. Past the old 1 MiB budget the second half fails.
#[tokio::test]
async fn a_large_checkout_listing_publishes_whole() {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let typical = [
        "src/main.rs",
        "crates/selvaged/src/net/session.rs",
        "packages/foo/src/components/Thing.tsx",
        "docs/studies/client-command-parity.md",
    ];
    let listing = |count: usize| {
        (0..count)
            .map(|n| format!("{}{n:06}", typical[n % typical.len()]))
            .collect::<Vec<String>>()
    };
    let invite = proto::session_url(
        &harness.ws_base(),
        Some(&room.id),
        Some(&room.token),
    );

    let large = listing(25_000);
    host.grant(large.clone())
        .await
        .expect("the large listing publishes");
    let late = harness.join_url(&invite, "Late").await.expect("late joins");
    let held =
        wait_for("the late joiner to inherit the large listing", || async {
            let held = late.granted_paths().await.ok()?;
            (held == large).then_some(held)
        })
        .await;
    let total: usize = held.iter().map(String::len).sum();
    assert!(
        total <= 4 * 1024 * 1024,
        "the delivery stays within the budget: {total}"
    );

    let widest = listing(100_000);
    host.grant(widest.clone())
        .await
        .expect("the widest listing publishes");
    let later = harness
        .join_url(&invite, "Later")
        .await
        .expect("later joins");
    let held =
        wait_for("the later joiner to inherit the widest listing", || async {
            let held = later.granted_paths().await.ok()?;
            (held == widest).then_some(held)
        })
        .await;
    let total: usize = held.iter().map(String::len).sum();
    assert!(
        total <= 4 * 1024 * 1024,
        "the delivery stays within the budget: {total}"
    );
}

/// The frame bound is exact: an 8 MiB binary relays to the room byte-identical, and
/// 8 MiB plus one byte ends the sender with nothing on the wire — the room learning
/// of it as `peer.left`. Written out, not built from the constant.
#[tokio::test]
async fn frame_boundaries_are_exact() -> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(&room.id),
        proto::percent_encode(&room.token)
    );
    let mut flood = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the sender joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    let joined = next_json_within(&mut flood, "room.joined").await;
    assert_eq!(joined["event"], event::ROOM_JOINED);
    let flo = joined["params"]["self"]["peer_id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    wait_for_peer(&host, "Flo").await;

    let mut watched = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the watcher joins");
    watched
        .hello(&serde_json::json!({"display_name": "Watch"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut watched, "room.joined").await["event"],
        event::ROOM_JOINED
    );

    // At the bound: 8 MiB relays byte-identical. Frames the host engine sent on
    // joining come first, so the watcher reads past those.
    let at_limit = vec![0xA5u8; 8 * 1024 * 1024];
    flood.send(0x2, &at_limit).await.expect("sends 8 MiB");
    let relayed =
        timeout(WAIT, read_binary_until(&mut watched.stream, &at_limit))
            .await
            .expect("the 8 MiB relay arrives")?;
    assert_eq!(relayed.payload, at_limit);

    // One byte over ends the sender with nothing on the wire to say why. The server
    // closes on the over-bound header, so the send itself can fail once the payload
    // outgrows the socket buffers.
    let over = vec![0xA5u8; 8 * 1024 * 1024 + 1];
    if let Err(failed) = flood.send(0x2, &over).await {
        assert!(
            socket_ended(&failed),
            "the oversize send ends the socket: {failed}"
        );
    }
    match timeout(WAIT, flood.read_to_end())
        .await
        .expect("the server ends the oversize connection")
    {
        None | Some(ErrorKind::ConnectionReset) => {}
        Some(kind) => panic!("the socket ends; it does not linger: {kind:?}"),
    }

    // The room learned of it as a drop.
    let left = next_json_within(&mut watched, "peer.left").await;
    assert_eq!(left["event"], event::PEER_LEFT);
    assert_eq!(left["params"]["peer_id"], flo.as_str());
    wait_for_described(
        "the host to stop seeing the dropped sender",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Flo")).then_some(())
        },
    )
    .await;
    Ok(())
}

/// A large-but-legitimate document syncs whole. A 4 MiB single insert converges — one
/// ~4 MiB delta frame — and after a 1 MiB delete a late joiner still syncs the full
/// ~3 MiB state in one full-state frame. Both shapes fit the 8 MiB bound, and neither
/// fit the old 2 MiB one, which killed the sender instead.
#[tokio::test]
async fn a_large_document_syncs_whole() -> Result<(), Failure> {
    const LARGE: &str = "large.dat";
    let harness = Harness::start(Duration::from_secs(30)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    host.open(LARGE).await.expect("the host opens");
    guest.open(LARGE).await.expect("the guest opens");

    let big = "x".repeat(4 * 1024 * 1024);
    host.insert(LARGE, 0, big.clone())
        .await
        .expect("the host writes big");
    assert_eq!(wait_for_convergence(&host, &guest, LARGE).await, big);

    host.delete(LARGE, 0, 1024 * 1024)
        .await
        .expect("the host trims");
    let trimmed = big[1024 * 1024..].to_string();
    assert_eq!(wait_for_convergence(&host, &guest, LARGE).await, trimmed);

    let invite = proto::session_url(
        &harness.ws_base(),
        Some(&room.id),
        Some(&room.token),
    );
    let late = harness.join_url(&invite, "Late").await.expect("late joins");
    late.open(LARGE).await.expect("the late joiner opens");
    assert_eq!(wait_for_convergence(&guest, &late, LARGE).await, trimmed);
    Ok(())
}

/// Queued bytes past the cap eject a slow peer before the frame cap could: six 8 MiB
/// frames are nowhere near 32 frames, but past 32 MiB the peer is slow all the same.
/// The room is told `peer.left`, its task is stopped, and everyone else keeps being
/// served.
#[tokio::test]
async fn queued_bytes_past_the_cap_eject_a_slow_peer() {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(&room.id),
        proto::percent_encode(&room.token)
    );

    // The slow peer joins, proves it is seated, and then never reads again.
    let mut slow = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the slow peer joins");
    slow.hello(&serde_json::json!({"display_name": "Bob"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut slow, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    wait_for_peer(&host, "Bob").await;

    // A second raw peer floods the room: six 8 MiB frames are 48 MiB past the 32 MiB
    // cap in 6 frames, where the 32-frame cap would see nothing wrong.
    let mut flood = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the flooding peer joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut flood, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    let mut progress = host.subscribe();
    let big = vec![0xA5u8; 8 * 1024 * 1024];
    for _ in 0..6 {
        flood.send(0x2, &big).await.expect("floods");
        // Paced on the host's own reading, so the flood cannot overtake the peer that
        // has to survive it and eject the host with the slow one.
        pace(&mut progress)
            .await
            .expect("the observer reads the flood frame");
    }

    // The room announces the removal, and the slow socket ends.
    wait_for_described_within(
        EJECTION_WAIT,
        "the host to see the slow peer leave",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Bob")).then_some(())
        },
    )
    .await;
    match timeout(EJECTION_WAIT, slow.read_to_end())
        .await
        .expect("the server ends the slow connection")
    {
        None | Some(ErrorKind::ConnectionReset) => {}
        Some(kind) => {
            panic!("the slow socket ends; it does not linger: {kind:?}")
        }
    }

    // The room keeps serving everyone else.
    host.open(PATH).await.expect("the host still opens");
}

/// Ejecting a slow host announces `peer.left` before `host.detached`, like a clean
/// leave: `eject_into` queues host-first so the LIFO drain delivers the departure
/// first. A watcher reading the wire in order sees the removal before the grace.
#[tokio::test]
async fn a_slow_host_is_ejected_departure_first() {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let mut slow = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    slow.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    let created = next_json_within(&mut slow, "room.created").await;
    let room_id = created["params"]["room_id"].as_str().unwrap_or("");
    let token = created["params"]["token"].as_str().unwrap_or("");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(room_id),
        proto::percent_encode(token),
    );

    // A watching guest on the client library: it reads in the background, so the
    // flood cannot eject it for not reading, and its event stream orders the
    // ejection announcements the way the room sent them.
    let invite =
        proto::session_url(&harness.ws_base(), Some(room_id), Some(token));
    let watcher = harness.join_url(&invite, "Wendy").await.expect("watches");

    // The flood joins after the watcher, and the event subscription starts after
    // both joins, so the only peer events it can see are the ejection pair.
    let mut flood = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the flooding peer joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut flood, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    wait_for_peer(&watcher, "Flo").await;
    let mut events = watcher.subscribe();
    let mut progress = watcher.subscribe();

    // Six 8 MiB frames are 48 MiB past the 32 MiB cap in 6 frames, where the
    // 32-frame cap would see nothing wrong: the never-reading host is ejected.
    let big = vec![0xA5u8; 8 * 1024 * 1024];
    for _ in 0..6 {
        flood.send(0x2, &big).await.expect("floods");
        // Paced on the watcher's own reading, so the flood cannot overtake the peer that
        // has to observe the order and eject it with the host it was watching.
        pace(&mut progress)
            .await
            .expect("the observer reads the flood frame");
    }

    // Departure first, grace second — the clean-leave order. Relay noise is
    // ignored; the wait stops at the grace announcement, reporting whether the
    // removal announcement came before it, and prints what it read if it runs out.
    let mut observed = Vec::new();
    let removal_first = timeout(
        EJECTION_WAIT,
        removal_before_grace(&mut events, &mut observed),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the watcher hears the ejection within {EJECTION_WAIT:?}; saw {observed:?}"
        )
    });
    assert!(removal_first, "peer.left is announced before host.detached");
}

/// Reads engine events until the grace announcement, reporting whether the removal
/// announcement came before it. Every event read lands in `observed`, so a wait that
/// runs out can print the order it actually saw.
async fn removal_before_grace(
    events: &mut broadcast::Receiver<EngineEvent>,
    observed: &mut Vec<EngineEvent>,
) -> bool {
    let mut saw_removal = false;
    while let Some(event) = next_event(events).await {
        if let EngineEvent::PeersChanged { peers } = &event
            && !peers.iter().any(|peer| peer.display_name == "Ada")
        {
            saw_removal = true;
        }
        let detached = matches!(event, EngineEvent::HostDetached { .. });
        observed.push(event);
        if detached {
            return saw_removal;
        }
    }
    false
}

/// The next engine event, skipping a receive that fell behind: a lagging receiver has
/// lost events it cannot ask for again, but the stream still holds the ones after.
async fn next_event(
    events: &mut broadcast::Receiver<EngineEvent>,
) -> Option<EngineEvent> {
    loop {
        match events.recv().await {
            Ok(event) => return Some(event),
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

/// Waits for the observer to process one undecodable flood frame.
///
/// Subscribe before sending the first frame. Each garbage binary frame produces one
/// `SessionError` in the incoming-frame handler, so consuming it acknowledges a socket
/// read rather than just a command turn. Keep this separate from ejection subscriptions
/// so pacing cannot consume the departure announcements they need to inspect.
async fn pace(
    progress: &mut broadcast::Receiver<EngineEvent>,
) -> Result<(), Failure> {
    timeout(EJECTION_WAIT, read_flood_frame(progress)).await??;
    Ok(())
}

/// Consumes events through the next flood acknowledgement without hiding receive errors.
async fn read_flood_frame(
    progress: &mut broadcast::Receiver<EngineEvent>,
) -> Result<(), broadcast::error::RecvError> {
    loop {
        let event = progress.recv().await?;
        if matches!(event, EngineEvent::SessionError { code, message }
            if code == code::BAD_MESSAGE
                && message == "a binary frame could not be decoded")
        {
            return Ok(());
        }
    }
}

/// Opening documents stays linear in the set and stops at the cap: 1024 short paths
/// open, and the 1025th is refused `x.room_full`. Every response carries the whole set,
/// so the run is quadratic in it — bounded by the cap, not by a delta the wire does
/// not have. The elapsed time is reported for the record, not asserted on.
#[tokio::test]
async fn open_cost_grows_with_the_set_and_stops_at_the_cap() {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let (host, _room) = harness.host("Ada").await.expect("host connects");
    let start = Instant::now();
    for n in 0..1024 {
        host.open(&format!("d{n:04}")).await.expect("opens");
    }
    eprintln!("1024 doc.open round trips in {:?}", start.elapsed());
    assert_eq!(host.documents().await.expect("the set").len(), 1024);
    let refused = host.open("d1024").await.expect_err("the set is capped");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected x.room_full, got {refused}");
    };
    assert_eq!(code, "x.room_full");
}

/// Past the connection cap a new connection is turned away with a signal, not
/// silence: plain HTTP gets `503` plus `retry-after`, a WebSocket upgrade gets its
/// handshake answered and a `1013` close — so a reconnect storm can tell "full"
/// from "dead". The count covers handshakes as well as seats; what it does not
/// cover is silence after the handshake, which the protocol forbids policing
/// (`PROTOCOL.md` §2.1).
#[tokio::test]
async fn connections_past_the_cap_are_turned_away() {
    let harness = Harness::start_with(ServerConfig {
        max_connections: 2,
        ..ServerConfig::default()
    })
    .await;
    let (ada, room) = harness.host("Ada").await.expect("connects");
    let _bob = harness.join(&room, "Bob").await.expect("joins");

    // Plain HTTP hears 503 with a retry hint.
    let addr = harness.ws_base().trim_start_matches("ws://").to_string();
    let mut plain = TcpStream::connect(&addr).await.expect("connects");
    plain
        .write_all(
            format!(
                "GET /meta HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("sends a request");
    let mut refused = String::new();
    plain
        .read_to_string(&mut refused)
        .await
        .expect("reads the refusal");
    assert!(refused.starts_with("HTTP/1.1 503"), "got {refused:?}");
    assert!(
        refused.to_ascii_lowercase().contains("retry-after"),
        "the refusal says when to come back: {refused:?}"
    );

    // A WebSocket upgrade hears 1013 after its handshake is answered.
    let mut upgrade =
        connect(&proto::session_url(&harness.ws_base(), None, None))
            .await
            .expect("the handshake is answered");
    let outcome = timeout(WAIT, upgrade.next())
        .await
        .expect("the server answers the upgrade");
    let Some(Ok(Message::Close(Some(frame)))) = outcome else {
        panic!("past the cap the upgrade ends in a close: {outcome:?}");
    };
    assert_eq!(u16::from(frame.code), 1013, "try again later");

    // The seated pair is undisturbed by the refusals.
    ada.open(PATH).await.expect("the room keeps serving");
}

/// The outbound queue's byte cap is the configured one rather than a constant: a few
/// kilobytes of relay eject a peer that stopped reading when the server is sized that way,
/// where the reference 32 MiB would need tens of mebibytes before noticing. This is the
/// knob a small host sizes its memory with, so the number it passes has to be the number
/// enforced.
#[tokio::test]
async fn the_configured_outbound_queue_cap_is_the_one_enforced() {
    let harness = Harness::start_with(ServerConfig {
        max_queue_bytes: 4096,
        room_grace: Duration::from_secs(30),
        ..ServerConfig::default()
    })
    .await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let target = guest_target(&room.id, &room.token);

    // A peer that is seated and then never reads again.
    let mut slow = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the slow peer joins");
    slow.hello(&serde_json::json!({"display_name": "Bob"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut slow, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    wait_for_peer(&host, "Bob").await;

    // A second peer relays frames the slow one never drains: four kilobytes are the whole
    // configured cap, so the fifth frame is past it where the reference cap would see
    // nothing wrong.
    let mut flood = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the flooding peer joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut flood, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    let chunk = vec![0xA5u8; 1024];
    for _ in 0..5 {
        flood.send(0x2, &chunk).await.expect("relays a frame");
    }

    // The room is told, and the slow socket ends.
    wait_for_described_within(
        EJECTION_WAIT,
        "the host to see the slow peer leave",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Bob")).then_some(())
        },
    )
    .await;
    assert!(
        matches!(
            timeout(EJECTION_WAIT, slow.read_to_end())
                .await
                .expect("the slow socket ends"),
            None | Some(ErrorKind::ConnectionReset)
        ),
        "the slow socket ends; it does not linger"
    );
    // The peer that relayed is untouched, and the room still serves.
    host.open(PATH).await.expect("the room keeps serving");
}

/// This server's own code for a connection that sent past its inbound budget: capacity
/// is policy rather than contract (`PROTOCOL.md` §2.1, §10.1), so it is named in the
/// reserved `x.` namespace like the room and server caps.
const RATE_LIMITED: &str = "x.rate_limited";

/// The standard WebSocket code for "try again later": what a connection past its budget
/// is closed with, because its session is over and a reconnect starts fresh.
const CLOSE_TRY_AGAIN_LATER: u16 = 1013;

/// The inbound budget these tests configure: a megabyte of burst and nothing refilled,
/// so the burst is the whole budget and no test here has a clock in it. The rate is a
/// deployment's knob, not a guard's; what is under test is the ceiling, and its refusing
/// at all. A live peer's own traffic (a presence frame on the client's timer is a
/// kilobyte of budget) does not reach a megabyte inside a test that lasts seconds.
fn flooded() -> ServerConfig {
    ServerConfig {
        inbound_bytes_per_sec: 0,
        inbound_burst_bytes: 1024 * 1024,
        room_grace: Duration::from_secs(30),
        ..ServerConfig::default()
    }
}

/// Floods a raw connection with `count` small requests, reading each answer as it
/// arrives: what ends this peer is the inbound budget rather than the outbound queue
/// behind a socket that never reads. Each request names an unknown method, which the
/// server answers once and announces to nobody, so the room pays for none of this.
/// `padding` widens the request, which is how one test prices payloads and another prices
/// frames.
///
/// Returns how many requests were answered and what the refusal said. Writing stops once
/// the requests are sent, and the refusal is read after that: a socket that is still
/// writing when the server closes can be reset by its own writes, which would discard the
/// refusal it had not read yet.
async fn flood_answered_requests(
    raw: &mut RawSocket,
    padding: usize,
    count: u64,
) -> (usize, Value) {
    let mut answers = 0_usize;
    for id in 2..count.saturating_add(2) {
        let frame = serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": format!("no.such.method{}", "x".repeat(padding)),
        });
        if raw.send_json(&frame).await.is_err() {
            break;
        }
        let arrived =
            next_json_within(raw, "an answer or the budget refusal").await;
        if arrived.get("event").and_then(Value::as_str)
            == Some(event::SESSION_ERROR)
        {
            return (answers, arrived);
        }
        if arrived.get("error").is_some() {
            answers = answers.saturating_add(1);
        }
    }
    // What is left after writing stops: the answers to the last frames sent, and then the
    // refusal itself.
    loop {
        let arrived = next_json_within(raw, "the budget refusal").await;
        if arrived.get("event").and_then(Value::as_str)
            == Some(event::SESSION_ERROR)
        {
            return (answers, arrived);
        }
        if arrived.get("error").is_some() {
            answers = answers.saturating_add(1);
        }
    }
}

/// Asserts that a connection ends promptly rather than lingering, and reports how it
/// ended when it does not: a peer left holding a seat while its client waits is the
/// failure this shape looks for.
#[expect(
    clippy::panic,
    reason = "a socket that lingers is a test failure, not a value to recover"
)]
async fn ends_promptly(raw: &mut RawSocket) {
    match timeout(WAIT, raw.read_to_end()).await {
        Ok(None | Some(ErrorKind::ConnectionReset)) => {}
        Ok(Some(kind)) => {
            panic!("the socket ends; it does not linger: {kind:?}");
        }
        Err(elapsed) => panic!("the server ends the connection: {elapsed}"),
    }
}

/// Sends `count` one-byte Ping frames, stopping when the server ends the socket: a write
/// that fails means the budget was already spent and the refusal is on its way.
async fn ping_flood(raw: &mut RawSocket, count: u32) {
    for _ in 0..count {
        if raw.send(0x9, &[0u8]).await.is_err() {
            return;
        }
    }
}

/// Reads the close of a connection the server has refused, returning its status code.
async fn refused_close_code(raw: &mut RawSocket) -> Result<u16, Failure> {
    let frame = timeout(WAIT, raw.read_to_close()).await??;
    frame
        .close_code()
        .ok_or_else(|| "the close frame carries no status code".into())
}

/// A handshake frame the outbound queue will not take seats nobody: the connection is
/// refused promptly instead of leaving a client waiting for a `room.created` that is
/// already lost, and the server keeps serving everyone else. The bookkeeping that takes
/// the placement back is pinned in `crates/selvaged/src/net/session.rs`, where the sizes
/// are exact.
#[tokio::test]
async fn a_handshake_that_cannot_be_delivered_ends_the_connection() {
    let harness = Harness::start_with(ServerConfig {
        // A queue not even a fresh room's `room.created` fits in.
        max_queue_bytes: 128,
        max_rooms: 1,
        ..ServerConfig::default()
    })
    .await;
    let mut first = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    first
        .hello(&serde_json::json!({"display_name": "Ada"}))
        .await
        .expect("says hello");
    // Nothing fits, so nothing arrives, and the socket ends rather than holding a seat
    // while its client waits.
    ends_promptly(&mut first).await;

    // Two more connections are accepted and answered the same way: a refused handshake
    // leaves the server serving.
    for _ in 0..2 {
        let mut next = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
            .await
            .expect("the upgrade still succeeds");
        next.hello(&serde_json::json!({"display_name": "Bob"}))
            .await
            .expect("says hello");
        ends_promptly(&mut next).await;
    }
    let addr = harness.ws_base().trim_start_matches("ws://").to_string();
    let mut plain = TcpStream::connect(&addr).await.expect("connects");
    plain
        .write_all(
            format!("GET /meta HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("sends a request");
    let head = read_http_head(&mut plain).await.expect("an answer");
    assert!(head.starts_with("HTTP/1.1 200"), "got {head:?}");
}

/// What a peer sends before it is seated is charged too: the frames a peer never reads an
/// answer to are the cheapest ones to flood with, and a connection that spends its budget
/// there is refused like a seated one, with the room never learning it existed.
#[tokio::test]
async fn a_pre_seat_flood_is_budgeted_too() {
    let harness = Harness::start_with(flooded()).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    // One-byte Ping frames before hello: nothing else rejects them — a control frame
    // carries at most 125 bytes — so nothing but the budget stops a peer that sends them
    // back to back.
    ping_flood(&mut raw, 1200).await;
    let refusal =
        next_json_within(&mut raw, "the pre-seat budget refusal").await;
    assert_eq!(refusal["params"]["code"], RATE_LIMITED);
    assert_eq!(
        refused_close_code(&mut raw).await.expect("the close"),
        close::PROTOCOL_ERROR,
        "every fault before seating closes, and 4xxx is this server's own range"
    );
}

/// A text envelope past the configured bound is refused on its length, and the
/// connection stays open. The request inside it is a `doc.open` this server answers
/// happily on its own terms — its path is well inside the path bound — so what is
/// refused is the frame's size and nothing about the request in it.
#[tokio::test]
async fn an_oversized_text_envelope_is_refused_on_its_length() {
    let harness = Harness::start_with(ServerConfig {
        max_envelope_bytes: 1024,
        ..ServerConfig::default()
    })
    .await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    // A 2000-byte path is legal — the path bound is 4096 — and its envelope is over the
    // 1024-byte bound, which is the fault the server answers.
    let path = "p".repeat(2000);
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 2,
        "method": method::DOC_OPEN,
        "params": {"path": path},
    }))
    .await
    .expect("sends");
    let refused = next_json_within(&mut raw, "the envelope refusal").await;
    assert_eq!(refused["event"], event::SESSION_ERROR);
    assert_eq!(refused["params"]["code"], code::BAD_MESSAGE);
    let message = refused["params"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("1024"), "the bound is named: {message}");
    assert!(
        message.contains("was not parsed"),
        "the frame, not the request in it, is the fault: {message}"
    );

    // Still seated, and still served: a frame inside the bound is answered normally.
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 3,
        "method": method::DOC_OPEN,
        "params": {"path": PATH},
    }))
    .await
    .expect("sends");
    assert_eq!(
        raw_response_for(&mut raw, 3).await["result"]["documents"],
        serde_json::json!([PATH])
    );
}

/// The bound is measured before the parser is handed the frame: a frame past it that is
/// not JSON at all is refused for its length rather than for the parse error megabytes
/// of it would raise, which is the difference between bounding the parse and paying for
/// it first.
#[tokio::test]
async fn an_oversized_envelope_is_measured_before_it_is_parsed() {
    let harness = Harness::start_with(ServerConfig {
        max_envelope_bytes: 1024,
        ..ServerConfig::default()
    })
    .await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    // Unterminated JSON, four times the bound: a parser would answer with where it gave
    // up, and the server answers with the bound it never handed over.
    raw.send(0x1, &vec![b'{'; 4096]).await.expect("sends");
    let refused = next_json_within(&mut raw, "the envelope refusal").await;
    let message = refused["params"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("1024") && message.contains("was not parsed"),
        "the bound answers, not the parser: {message}"
    );
}

/// Before the handshake completes every fault closes the connection (`PROTOCOL.md` §11),
/// and the envelope bound is judged there too: a first frame past it is refused with the
/// bound named and closed 4000, like any other fault that arrives unseated.
#[tokio::test]
async fn an_oversized_first_frame_is_refused_and_closed() {
    let harness = Harness::start_with(ServerConfig {
        max_envelope_bytes: 1024,
        ..ServerConfig::default()
    })
    .await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    // A `session.hello` whose display name is far past the name bound: over the envelope
    // bound it is never judged as a name at all, and the refusal says so.
    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 1,
        "method": method::SESSION_HELLO,
        "params": {"display_name": "n".repeat(2000)},
    }))
    .await
    .expect("sends");
    let refused = next_json_within(&mut raw, "the unseated refusal").await;
    assert_eq!(refused["params"]["code"], code::BAD_MESSAGE);
    let message = refused["params"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("1024"), "the bound is named: {message}");
    assert_eq!(
        raw.read_to_close().await.expect("the close").close_code(),
        Some(close::PROTOCOL_ERROR)
    );
}

/// A peer that sends past its inbound budget is told why and ended, and nothing about
/// the room moves: the requests it sent before the refusal were answered, the room's
/// document set is what it was, and the seat it held is free for the next peer, whose
/// budget is its own.
#[tokio::test]
async fn a_peer_past_its_inbound_budget_is_told_why_and_exiled() {
    let harness = Harness::start_with(flooded()).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    host.open(PATH).await.expect("the host opens a document");
    let mut flood =
        RawSocket::open(&harness, &guest_target(&room.id, &room.token), &[])
            .await
            .expect("the flooder joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut flood, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    wait_for_peer(&host, "Flo").await;

    // Eight-kilobyte requests: the budget pays for the bytes, and the peer spends its
    // megabyte in a hundred and twenty-seven of them.
    let (answered, refused) =
        flood_answered_requests(&mut flood, 8 * 1024, 130).await;
    assert!(answered > 0, "requests were answered before the refusal");
    assert_eq!(refused["params"]["code"], RATE_LIMITED);
    let message = refused["params"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("1048576"),
        "the refusal names the budget it passed: {message}"
    );
    assert_eq!(
        refused_close_code(&mut flood).await.expect("the close"),
        CLOSE_TRY_AGAIN_LATER
    );

    // The room is whole: the set is what the host left it as, and the host still serves
    // it while the flooder leaves.
    wait_for_described_within(
        EJECTION_WAIT,
        "the host to see the flooder leave",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Flo")).then_some(())
        },
    )
    .await;
    assert_eq!(
        host.documents().await.expect("the set"),
        vec![PATH.to_string()]
    );
    host.open(GUEST_ONLY).await.expect("the host still opens");

    // The seat is free, and the next peer starts with a budget of its own.
    let late = harness
        .join(&room, "Bob")
        .await
        .expect("the freed seat takes a guest");
    late.open(PATH).await.expect("the newcomer is served");
    assert_eq!(
        late.documents().await.expect("the set"),
        vec![PATH.to_string(), GUEST_ONLY.to_string()]
    );
}

/// A flood of tiny frames is priced like the work it costs rather than like the bytes it
/// moves: sixty-byte requests spend a megabyte of budget, because each frame costs the
/// per-frame floor. Without that floor the same flood would cost sixty kilobytes — a
/// sixteenth of the budget — and this peer would keep the server busy for as long as it
/// liked.
#[tokio::test]
async fn a_flood_of_tiny_frames_spends_the_per_frame_floor() {
    let harness = Harness::start_with(flooded()).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let mut flood =
        RawSocket::open(&harness, &guest_target(&room.id, &room.token), &[])
            .await
            .expect("the flooder joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut flood, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    wait_for_peer(&host, "Flo").await;

    // Sixty-byte requests, a thousand and twenty-four of which spend the megabyte.
    let (answered, refused) =
        flood_answered_requests(&mut flood, 0, 1100).await;
    assert!(answered > 0, "requests were answered before the refusal");
    assert_eq!(refused["params"]["code"], RATE_LIMITED);
    let message = refused["params"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("1048576"),
        "the budget, not the bytes sent, is what ran out: {message}"
    );
    // A little over a thousand frames of sixty-odd bytes each: the flood's own weight is
    // a small fraction of the megabyte it spent.
    let sent = answered.saturating_mul(64);
    assert!(
        sent < 256 * 1024,
        "the frames sent are far below the budget they spent: {sent} bytes"
    );
}

/// The budget counts frames the session never parses, too: a relayed payload is copied
/// once per peer, so a flood of them is what turns a room into an amplifier. Nothing is
/// answered here — a relay is not a request — and the burst is gone after two frames.
#[tokio::test]
async fn a_relayed_frame_flood_is_budgeted_too() {
    let harness = Harness::start_with(flooded()).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let mut flood =
        RawSocket::open(&harness, &guest_target(&room.id, &room.token), &[])
            .await
            .expect("the flooder joins");
    flood
        .hello(&serde_json::json!({"display_name": "Flo"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut flood, "room.joined").await["event"],
        event::ROOM_JOINED
    );
    wait_for_peer(&host, "Flo").await;

    // Two 512 KiB frames are the whole megabyte, so the third is the one refused. They
    // are sent before the refusal is read, because a relay is not answered and the server
    // reads faster than the socket takes the writes.
    let chunk = vec![0xA5u8; 512 * 1024];
    for _ in 0..3 {
        flood.send(0x2, &chunk).await.expect("relays a frame");
    }
    let refused = next_json_within(&mut flood, "the budget refusal").await;
    assert_eq!(refused["event"], event::SESSION_ERROR);
    assert_eq!(refused["params"]["code"], RATE_LIMITED);
    assert_eq!(
        refused_close_code(&mut flood).await.expect("the close"),
        CLOSE_TRY_AGAIN_LATER
    );
    // The host is undisturbed, and what the flooder relayed before it was stopped is
    // what the room saw, not a half-written frame.
    host.open(PATH).await.expect("the host still opens");
}

/// Saturation is bounded rather than fatal. Several peers spend their budgets at once,
/// every one of them is exiled with a reason, the host's session and the room's state
/// are the same as they were, and the server is still a server afterwards: it seats a
/// newcomer in the room and mints another room.
#[tokio::test]
async fn saturation_exiles_every_flooder_and_leaves_the_room_whole() {
    let harness = Harness::start_with(flooded()).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    host.open(PATH).await.expect("the host opens a document");

    let mut floods = Vec::new();
    for n in 0..4 {
        let mut flood = RawSocket::open(
            &harness,
            &guest_target(&room.id, &room.token),
            &[],
        )
        .await
        .expect("a flooder joins");
        flood
            .hello(&serde_json::json!({"display_name": format!("Flo{n}")}))
            .await
            .expect("says hello");
        assert_eq!(
            next_json_within(&mut flood, "room.joined").await["event"],
            event::ROOM_JOINED
        );
        floods.push(flood);
    }
    wait_for_peer(&host, "Flo3").await;

    for flood in &mut floods {
        let (_, refused) = flood_answered_requests(flood, 8 * 1024, 130).await;
        assert_eq!(refused["params"]["code"], RATE_LIMITED);
    }

    wait_for_described_within(
        EJECTION_WAIT,
        "the room to see every flooder leave",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers
                .iter()
                .any(|peer| peer.display_name.starts_with("Flo")))
            .then_some(())
        },
    )
    .await;

    // The room is whole: the set is what it was, and the host still edits it.
    assert_eq!(
        host.documents().await.expect("the set"),
        vec![PATH.to_string()]
    );
    host.open(GUEST_ONLY).await.expect("the host still opens");

    // And the server is still a server.
    let late = harness.join(&room, "Late").await.expect("a seat is free");
    late.open(PATH).await.expect("the newcomer is served");
    assert_eq!(
        late.documents().await.expect("the set"),
        vec![PATH.to_string(), GUEST_ONLY.to_string()]
    );
    let (_bob, second) = harness.host("Bob").await.expect("another room mints");
    assert_ne!(second.id, room.id);
}

/// A request head that runs past the bound with no blank line is refused `431`,
/// not dropped silently: the client hears that its head — not the server — is the
/// problem. One write past the 16 KiB bound always fits the socket buffers, so the
/// server always has the bytes it needs to decide before the client waits.
#[tokio::test]
async fn an_oversized_request_head_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let addr = harness.ws_base().trim_start_matches("ws://").to_string();
    let mut stream = TcpStream::connect(&addr).await.expect("connects");
    stream
        .write_all(&vec![b'x'; 17 * 1024])
        .await
        .expect("sends a headless flood");
    let head = read_http_head(&mut stream).await.expect("an answer");
    assert!(head.starts_with("HTTP/1.1 431"), "got {head:?}");
}

// A head whose blank line ends past the bound is oversize too, even though it
// terminates: the bound limits the head, not just the hunt for its end.
#[tokio::test]
async fn a_terminated_head_past_the_bound_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let addr = harness.ws_base().trim_start_matches("ws://").to_string();
    // The terminator lands inside the old fuzz window: past 16 KiB, within one
    // 1 KiB server read past it — where the hunt accepted the head without
    // judging its length.
    let prefix = format!("GET /meta HTTP/1.1\r\nhost: {addr}\r\nx-pad: ");
    let padded = format!("{} {}\r\n\r\n", prefix, "a".repeat(16_400));
    assert!(padded.len() > 16 * 1024, "the head clears the bound");
    assert!(padded.len() < 17 * 1024, "within one read past it");
    let mut stream = TcpStream::connect(&addr).await.expect("connects");
    stream
        .write_all(padded.as_bytes())
        .await
        .expect("sends a fat head");
    let head = read_http_head(&mut stream).await.expect("an answer");
    assert!(head.starts_with("HTTP/1.1 431"), "got {head:?}");
}

/// Closing a path nobody holds is a no-op success: the set is unchanged, and no
/// `doc_not_open` arrives, because the protocol reserves that code without producing
/// it (`PROTOCOL.md` §11). A client written from the vocabulary list must not wait for
/// an event that cannot arrive, and the server must not start emitting one.
#[tokio::test]
async fn closing_a_path_nobody_holds_is_a_no_op_success() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = RawSocket::open(&harness, proto::ENDPOINT_PATH, &[])
        .await
        .expect("the upgrade succeeds");
    raw.hello(&serde_json::json!({"display_name": "Ada", "role": "host"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.created").await["event"],
        event::ROOM_CREATED
    );

    raw.send_json(&serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 2,
        "method": method::DOC_CLOSE,
        "params": {"path": "never/opened.rs"},
    }))
    .await
    .expect("sends");
    let answered = raw_response_for(&mut raw, 2).await;
    assert!(answered["error"].is_null(), "no error: {answered}");
    assert_eq!(
        answered["result"]["documents"],
        serde_json::json!([]),
        "the set is unchanged"
    );
}

/// A binary frame no replica decodes is reported, not dropped: a raw peer sends bytes
/// that fail decoding, the relay carries them opaquely, and the receiving engine says
/// `session.error` instead of losing the sender's content silently. The session stays
/// open — `PROTOCOL.md` §11 keeps a seated connection on a `bad_message`.
#[tokio::test]
async fn an_undecodable_binary_relay_is_reported_and_survived()
-> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    let target = format!(
        "{}?room={}&token={}",
        proto::ENDPOINT_PATH,
        proto::percent_encode(&room.id),
        proto::percent_encode(&room.token)
    );
    let mut raw = RawSocket::open(&harness, &target, &[])
        .await
        .expect("the sender joins");
    raw.hello(&serde_json::json!({"display_name": "Mallory"}))
        .await
        .expect("says hello");
    assert_eq!(
        next_json_within(&mut raw, "room.joined").await["event"],
        event::ROOM_JOINED
    );

    // Subscribed before the garbage is sent: the event must not slip past the wait.
    let mut events = guest.subscribe();
    raw.send(0x2, &[0x02, 0x00, 0x02, 0xFF, 0xFF])
        .await
        .expect("sends undecodable bytes");
    let code = timeout(WAIT, wait_for_bad_frame(&mut events))
        .await
        .expect("a session error arrives")?;
    assert_eq!(code, code::BAD_MESSAGE);

    // The session survived the garbage: both engines still open and agree.
    host.open(PATH).await.expect("the host opens");
    guest.open(PATH).await.expect("the guest opens");
    host.insert(PATH, 0, "kept\n")
        .await
        .expect("the host writes");
    wait_for("the guest to receive the seed", || async {
        (guest.text(PATH).await.ok()? == "kept\n").then_some(())
    })
    .await;
    Ok(())
}
