//! Session layer: version negotiation, unknown methods, membership and the room
//! lifecycle. The raw-WireSocket tests speak the protocol by hand, so the spec in
//! `spec/PROTOCOL.md` is checked against the bytes on the wire rather than against the
//! client library.

use std::error::Error as StdError;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_harness::{
    EngineEvent, Error, Harness, Role, Selection, wait_for, wait_for_event,
};
use selvage_protocol as proto;
use selvage_protocol::{close, code, event, method};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;

const PATH: &str = "src/main.rs";

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
            assert_eq!(response.get("result"), Some(&serde_json::json!({})));
            return Ok(());
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
    let opened = next_json(&mut raw).await.expect("response");
    assert_eq!(opened["id"], 2);
    assert_eq!(opened["result"], serde_json::json!({}));

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
    let refused = next_json(&mut raw).await.expect("refusal");
    assert_eq!(refused["id"], 3);
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

#[tokio::test]
async fn a_guest_leaving_is_announced_and_its_presence_is_cleaned_up() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    guest
        .set_selection(PATH, Selection::caret(3))
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
