//! Session layer: version negotiation, unknown methods, membership and the room
//! lifecycle. The raw-WireSocket tests speak the protocol by hand, so the spec in
//! `PROTOCOL.md` is checked against the bytes on the wire rather than against the
//! client library.

use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_harness::{
    EngineEvent, Error, Harness, Role, SelectionOffsets, ServerConfig,
    SyncEngine, WAIT, wait_for, wait_for_convergence, wait_for_described,
    wait_for_event, wait_for_peer,
};
use selvage_protocol as proto;
use selvage_protocol::{close, code, event, method};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::task::yield_now;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;

const PATH: &str = "src/main.rs";

/// A path only the guest in the document-set test holds open.
const GUEST_ONLY: &str = "only/guest.rs";

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
/// seat path serializes both after the registry lock is dropped, and reordering them
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
/// is 1 MiB binary frames, which stay well under the frame bound.
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
    let big = vec![0xA5u8; 1024 * 1024];
    for _ in 0..150 {
        flood.send(0x2, &big).await.expect("floods");
        // Let the room drain between sends: the flood must fill the slow peer's
        // queue, not win a scheduling race against the host's. Unpaced, a loaded
        // machine ejects the host too and the final open fails — flakily, under
        // coverage, on unmodified main as well as here.
        yield_now().await;
    }

    // The room announces the removal, and the slow socket ends.
    wait_for_described(
        "the host to see the slow peer leave",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Bob")).then_some(())
        },
    )
    .await;
    match timeout(WAIT, slow.read_to_end())
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
    let big = vec![0xA5u8; 8 * 1024 * 1024];
    for _ in 0..6 {
        flood.send(0x2, &big).await.expect("floods");
    }

    // The room announces the removal, and the slow socket ends.
    wait_for_described(
        "the host to see the slow peer leave",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            (!peers.iter().any(|peer| peer.display_name == "Bob")).then_some(())
        },
    )
    .await;
    match timeout(WAIT, slow.read_to_end())
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

    // Six 8 MiB frames are 48 MiB past the 32 MiB cap in 6 frames, where the
    // 32-frame cap would see nothing wrong: the never-reading host is ejected.
    let big = vec![0xA5u8; 8 * 1024 * 1024];
    for _ in 0..6 {
        flood.send(0x2, &big).await.expect("floods");
    }

    // Departure first, grace second — the clean-leave order. Relay noise is
    // ignored; the wait stops at the grace announcement, reporting whether the
    // removal announcement came before it.
    let removal_first = timeout(WAIT, removal_before_grace(&mut events))
        .await
        .expect("the watcher hears the ejection");
    assert!(removal_first, "peer.left is announced before host.detached");
}

/// Reads engine events until the grace announcement, reporting whether the removal
/// announcement came before it.
async fn removal_before_grace(
    events: &mut broadcast::Receiver<EngineEvent>,
) -> bool {
    let mut saw_removal = false;
    while let Ok(event) = events.recv().await {
        if let EngineEvent::PeersChanged { peers } = &event
            && !peers.iter().any(|peer| peer.display_name == "Ada")
        {
            saw_removal = true;
        }
        if matches!(event, EngineEvent::HostDetached { .. }) {
            return saw_removal;
        }
    }
    false
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
