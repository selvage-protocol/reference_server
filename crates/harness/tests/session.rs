//! Session layer: version negotiation, unknown methods, membership and the room
//! lifecycle. The raw-WireSocket tests speak the protocol by hand, so the spec in
//! `spec/PROTOCOL.md` is checked against the bytes on the wire rather than against the
//! client library.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_harness::{wait_for, wait_for_event, EngineEvent, Harness, Role};
use selvage_protocol as proto;
use tokio_tungstenite::tungstenite::Message;

const PATH: &str = "src/main.rs";

/// A hand-rolled session: no client library involved.
struct Raw {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl Raw {
    async fn connect(url: &str) -> Self {
        let (ws, _) = tokio_tungstenite::connect_async(url).await.expect("connects");
        Self { ws }
    }

    async fn send_json(&mut self, value: serde_json::Value) {
        self.ws
            .send(Message::text(value.to_string()))
            .await
            .expect("sends");
    }

    async fn hello(&mut self, params: serde_json::Value) {
        self.send_json(serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": 1,
            "method": proto::method::SESSION_HELLO,
            "params": params,
        }))
        .await;
    }

    /// The next text frame that is not a binary relay.
    async fn next_json(&mut self) -> serde_json::Value {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    return serde_json::from_str(&text).expect("server sends JSON")
                }
                Some(Ok(Message::Binary(_))) => continue,
                Some(Ok(Message::Close(frame))) => {
                    panic!("connection closed while waiting for JSON: {frame:?}")
                }
                Some(Ok(_)) => continue,
                other => panic!("connection ended while waiting for JSON: {other:?}"),
            }
        }
    }

    /// The next binary frame relayed by the server.
    async fn next_binary(&mut self) -> Vec<u8> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Binary(frame))) => return frame.to_vec(),
                Some(Ok(_)) => continue,
                other => panic!("connection ended while waiting for a binary frame: {other:?}"),
            }
        }
    }

    async fn open(&mut self, id: u64, path: &str) {
        self.send_json(serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": id,
            "method": proto::method::DOC_OPEN,
            "params": {"path": path},
        }))
        .await;
        // A peer's `doc.opened` event may arrive before our own response.
        loop {
            let response = self.next_json().await;
            if response["id"] == id {
                assert_eq!(response["result"], serde_json::json!({}));
                return;
            }
        }
    }

    /// Reads until the connection closes, returning the close code.
    async fn close_code(&mut self) -> u16 {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Close(Some(frame)))) => return u16::from(frame.code),
                Some(Ok(_)) => continue,
                Some(Err(_)) | None => panic!("connection dropped without a close frame"),
            }
        }
    }
}

#[tokio::test]
async fn meta_negotiates_the_wire_version() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let body = http_get(&format!("{}/meta", harness.http_base())).await;
    let meta: proto::Meta = serde_json::from_str(&body).expect("meta is JSON");

    assert!(meta.wire_versions.contains(&proto::WIRE_VERSION.to_string()));
    assert!(meta.capabilities.iter().any(|c| c == "y-protocols/1"));
    assert_eq!(meta.keepalive.ping_interval_ms, 30_000);
    assert_eq!(meta.keepalive.awareness_renew_ms, 15_000);
    assert_eq!(meta.keepalive.awareness_expire_ms, 30_000);
    assert_eq!(meta.roles, vec!["host", "guest"]);
}

#[tokio::test]
async fn unknown_methods_return_an_error_and_unknown_fields_are_ignored() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut raw = Raw::connect(&proto::session_url(&harness.ws_base(), None, None)).await;

    // Unknown fields, unknown capabilities: ignored.
    raw.hello(serde_json::json!({
        "display_name": "Raw",
        "capabilities": ["no.such.capability"],
        "future_field": {"nested": true},
    }))
    .await;
    let created = raw.next_json().await;
    assert_eq!(created["event"], proto::event::ROOM_CREATED);
    assert!(created["params"]["token"].is_string());
    let room_id = created["params"]["room_id"].as_str().unwrap().to_string();

    // A known method: a normal response.
    raw.send_json(serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 2,
        "method": proto::method::DOC_OPEN,
        "params": {"path": PATH},
    }))
    .await;
    let opened = raw.next_json().await;
    assert_eq!(opened["id"], 2);
    assert_eq!(opened["result"], serde_json::json!({}));

    // An unknown method: an error response, not silence.
    raw.send_json(serde_json::json!({
        "v": proto::WIRE_VERSION,
        "id": 3,
        "method": "cursor.teleport",
        "params": {},
    }))
    .await;
    let refused = raw.next_json().await;
    assert_eq!(refused["id"], 3);
    assert_eq!(refused["error"]["code"], proto::code::UNKNOWN_METHOD);
    assert!(refused["result"].is_null());

    // An incompatible version: refused, with the documented close code.
    let mut stale = Raw::connect(&proto::session_url(&harness.ws_base(), Some(&room_id), Some("t")))
        .await;
    stale
        .send_json(serde_json::json!({
            "v": "selvage/9",
            "id": 1,
            "method": proto::method::SESSION_HELLO,
            "params": {"display_name": "Stale"},
        }))
        .await;
    assert_eq!(
        stale.next_json().await["params"]["code"],
        proto::code::UNSUPPORTED_VERSION
    );
    assert_eq!(stale.close_code().await, proto::close::UNSUPPORTED_VERSION);
}

#[tokio::test]
async fn joining_needs_the_room_and_the_token() {
    let harness = Harness::start(Duration::from_secs(5)).await;

    // No such room.
    let mut missing =
        Raw::connect(&proto::session_url(&harness.ws_base(), Some("r-nope"), Some("t"))).await;
    missing.hello(serde_json::json!({"display_name": "Ghost"})).await;
    assert_eq!(
        missing.next_json().await["params"]["code"],
        proto::code::ROOM_UNKNOWN
    );
    assert_eq!(missing.close_code().await, proto::close::ROOM_UNKNOWN);

    // A real room, wrong token.
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let mut wrong =
        Raw::connect(&proto::session_url(&harness.ws_base(), Some(&room.id), Some("wrong")))
            .await;
    wrong.hello(serde_json::json!({"display_name": "Mallory"})).await;
    assert_eq!(
        wrong.next_json().await["params"]["code"],
        proto::code::TOKEN_INVALID
    );
    assert_eq!(wrong.close_code().await, proto::close::TOKEN_INVALID);

    // The room keeps serving the host.
    assert_eq!(host.session().role, Role::Host);
    assert_eq!(host.peers().await.unwrap().len(), 0);
}

/// The server routes document and awareness payloads and never decodes them: a frame
/// it cannot possibly understand comes out byte-identical on the other side.
#[tokio::test]
async fn document_payloads_are_relayed_verbatim() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let mut host = Raw::connect(&proto::session_url(&harness.ws_base(), None, None)).await;
    host.hello(serde_json::json!({"display_name": "Raw host"})).await;
    let created = host.next_json().await;
    let room_id = created["params"]["room_id"].as_str().unwrap().to_string();
    let token = created["params"]["token"].as_str().unwrap().to_string();

    let mut guest =
        Raw::connect(&proto::session_url(&harness.ws_base(), Some(&room_id), Some(&token))).await;
    guest.hello(serde_json::json!({"display_name": "Raw guest"})).await;
    assert_eq!(
        guest.next_json().await["event"],
        proto::event::ROOM_JOINED
    );
    // The host is told the guest arrived, but nothing about what it will say.
    assert_eq!(host.next_json().await["event"], proto::event::PEER_JOINED);

    host.open(2, PATH).await;
    guest.open(2, PATH).await;
    assert_eq!(host.next_json().await["event"], proto::event::DOC_OPENED);

    // Not a y-protocols frame, not valid UTF-8, not valid JSON.
    let opaque = vec![0xff, 0x00, 0xfe, 0x7f, 0x00, 0x01, 0xde, 0xad, 0xbe, 0xef];
    host.ws
        .send(Message::binary(opaque.clone()))
        .await
        .expect("sends an opaque payload");
    assert_eq!(guest.next_binary().await, opaque);

    let reply = vec![0x00, 0x02, b'n', b'o', b'p', b'e'];
    guest
        .ws
        .send(Message::binary(reply.clone()))
        .await
        .expect("sends an opaque payload");
    assert_eq!(host.next_binary().await, reply);
}

#[tokio::test]
async fn a_second_host_is_refused() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (_host, room) = harness.host("Ada").await.expect("host connects");

    let error = harness
        .reclaim(&room, "Impostor")
        .await
        .expect_err("only one host at a time");
    match error {
        selvage_harness::Error::Protocol { code, .. } => {
            assert_eq!(code, proto::code::HOST_PRESENT)
        }
        other => panic!("expected host_present, got {other}"),
    }
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
        panic!("host.detached")
    };
    assert_eq!(grace_ms, 10_000);

    // The room is still there for the guest, and now has no host.
    let reattached = wait_for_event(&guest, "host.attached", |event| {
        matches!(event, EngineEvent::HostAttached { .. })
    });
    let host = harness.reclaim(&room, "Ada").await.expect("host returns");
    let EngineEvent::HostAttached { peer } = reattached.await else {
        panic!("host.attached")
    };
    assert_eq!(peer.display_name, "Ada");
    assert_eq!(peer.role, Role::Host);

    // Same room, same document, same content.
    assert_eq!(host.session().room_id, guest.session().room_id);
    let documents = host.documents().await.unwrap();
    assert!(documents.contains(&PATH.to_string()), "got {documents:?}");
    assert_eq!(guest.text(PATH).await.unwrap(), "shared\n");
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
        panic!("room.gone")
    };
    assert_eq!(reason, "host did not return");

    // The room really is gone: the id cannot be reused, even with its token.
    let error = harness
        .join(&room, "Late")
        .await
        .expect_err("the room is gone");
    match error {
        selvage_harness::Error::Protocol { code, .. } => assert_eq!(code, proto::code::ROOM_UNKNOWN),
        other => panic!("expected room_unknown, got {other}"),
    }

    // And the guest's connection is closed by the server.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if guest.text(PATH).await.is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the server should have closed the abandoned guest's connection");
}

#[tokio::test]
async fn a_guest_leaving_is_announced_and_its_presence_is_cleaned_up() {
    use selvage_harness::{wait_for, Selection};

    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    guest.set_selection(PATH, Selection::caret(3)).await.unwrap();

    let seen = wait_for("Bob's cursor", || async {
        host.presence()
            .await
            .ok()?
            .into_iter()
            .find(|p| p.display_name() == Some("Bob"))
    })
    .await;
    assert!(seen.state.is_some());

    let left = wait_for_event(&host, "peer.left", |event| {
        matches!(event, EngineEvent::PeersChanged { peers } if peers.is_empty())
    });
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

async fn http_get(url: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let address = url.trim_start_matches("http://");
    let (host, path) = address.split_once('/').expect("host/path");
    let mut stream = tokio::net::TcpStream::connect(host).await.expect("connects");
    stream
        .write_all(format!("GET /{path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n").as_bytes())
        .await
        .expect("writes the request");
    let mut response = String::new();
    stream.read_to_string(&mut response).await.expect("reads");
    response
        .split_once("\r\n\r\n")
        .expect("an HTTP body follows the headers")
        .1
        .to_string()
}
