//! `selvage/2` on the wire: what a connection gets from the server.
//!
//! This drives a server on its defaults with raw sockets, because the relay library is
//! better exercised by `relay_selvaged.rs`. What is asserted is what `PROTOCOL.md` §3,
//! §6.1 and §9 say the server is: membership, a relay, and nothing a room's contents could
//! be read from.

use std::error::Error as StdError;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_harness::{Harness, ServerConfig, WAIT};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;

type Raw = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Anything this test can fail with.
type Failure = Box<dyn StdError>;

/// A raw connection, and the JSON frames it has read.
struct Peer {
    ws: Raw,
}

impl Peer {
    /// Opens a socket at `base` with `query`, sends `session.hello` and reads the reply.
    async fn join(
        base: &str,
        query: &str,
        name: &str,
    ) -> Result<(Self, Value), Failure> {
        let url = if query.is_empty() {
            format!("{base}/session")
        } else {
            format!("{base}/session?{query}")
        };
        let (ws, _) = tokio_tungstenite::connect_async(url).await?;
        let mut peer = Self { ws };
        let frame = serde_json::json!({
            "id": 1,
            "method": "session.hello",
            "params": { "display_name": name },
            "v": "selvage/2",
        });
        peer.send_text(&frame.to_string()).await?;
        let reply = peer.recv_json().await?;
        Ok((peer, reply))
    }

    async fn send_text(&mut self, text: &str) -> Result<(), Failure> {
        self.ws.send(Message::text(text)).await?;
        Ok(())
    }

    async fn recv_json(&mut self) -> Result<Value, Failure> {
        let message = self.recv_message().await?;
        let Message::Text(text) = message else {
            return Err(
                format!("expected a text frame, got {message:?}").into()
            );
        };
        Ok(serde_json::from_str(&text)?)
    }

    async fn recv_message(&mut self) -> Result<Message, Failure> {
        timeout(WAIT, self.ws.next())
            .await
            .map_err(|_| "no frame arrived")?
            .ok_or("the socket closed")?
            .map_err(Into::into)
    }

    async fn close(mut self) {
        let _ = self.ws.close(None).await;
    }
}

/// A `GET`, reading the whole response body.
async fn http_get(base: &str, path: &str) -> Result<String, Failure> {
    let authority = base.trim_start_matches("http://");
    let mut stream = TcpStream::connect(authority).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nhost: {authority}\r\nconnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response
        .split_once("\r\n\r\n")
        .map_or_else(|| response.clone(), |(_, body)| body.to_string()))
}

/// The query a join needs, from a `room.created` reply.
fn join_query(created: &Value) -> Result<String, Failure> {
    let params = created.get("params").ok_or("a mint has params")?;
    let room = params
        .get("room_id")
        .and_then(Value::as_str)
        .ok_or("a mint names its room")?;
    let token = params
        .get("token")
        .and_then(Value::as_str)
        .ok_or("a mint carries its token")?;
    Ok(format!("room={room}&token={token}"))
}

/// A mint is seated by its reply, and the reply carries membership and nothing else.
#[tokio::test]
async fn a_mint_is_seated_by_its_reply() -> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig::default()).await;
    let (host, created) = Peer::join(&harness.ws_base(), "", "Ada").await?;

    assert_eq!(created["v"], "selvage/2");
    assert_eq!(created["event"], "room.created");
    let params = &created["params"];
    assert!(params.get("token").is_some(), "a mint carries its token");
    assert_eq!(
        params["capabilities"],
        serde_json::json!(["y-protocols/1", "awareness"]),
        "a server advertises the two names the version defines"
    );
    // §6.1: `documents` is not a member, and `PeerInfo` carries no `role`.
    assert!(
        params.get("documents").is_none(),
        "no server-held document set"
    );
    assert!(
        params["self"].get("role").is_none(),
        "the server seats nobody"
    );
    assert!(params["self"]["peer_id"].as_str().is_some());
    host.close().await;
    Ok(())
}

/// The relay: a frame arrives byte for byte, and no frame the server authors names a path
/// or a role (`PROTOCOL.md` §3).
#[tokio::test]
async fn a_room_relays_bytes_and_authors_nothing_about_them()
-> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig::default()).await;
    let (mut host, created) = Peer::join(&harness.ws_base(), "", "Ada").await?;
    let query = join_query(&created)?;
    let (mut guest, joined) =
        Peer::join(&harness.ws_base(), &query, "Bob").await?;
    assert_eq!(joined["event"], "room.joined");
    assert!(joined["params"].get("documents").is_none());
    assert!(joined["params"]["peers"][0].get("role").is_none());

    // The host hears `peer.joined`, and its `peer` carries no role either.
    let announced = host.recv_json().await?;
    assert_eq!(announced["event"], "peer.joined");
    assert_eq!(announced["v"], "selvage/2");
    assert!(
        announced["params"]["peer"].get("role").is_none(),
        "a `PeerInfo` has no role: {announced}"
    );

    // A sealed frame is opaque: the bytes that arrive are the bytes that were sent.
    let payload = vec![0x03u8, 0x23, 0x18, 0xee, 0x00, 0xff, 0x7f];
    guest.ws.send(Message::binary(payload.clone())).await?;
    let relayed = host.recv_message().await?;
    let Message::Binary(bytes) = relayed else {
        return Err(
            format!("expected the relayed bytes, got {relayed:?}").into()
        );
    };
    assert_eq!(bytes.to_vec(), payload);

    // Every frame the server authors carries neither a path nor a role.
    for frame in [&created, &joined, &announced] {
        assert_no_path_or_role(frame)?;
    }
    host.close().await;
    guest.close().await;
    Ok(())
}

/// No server-authored frame names a path or a role (`PROTOCOL.md` §3).
fn assert_no_path_or_role(frame: &Value) -> Result<(), Failure> {
    let text = frame.to_string();
    for forbidden in ["\"path\"", "\"documents\"", "\"role\"", "\"paths\""] {
        if text.contains(forbidden) {
            return Err(format!(
                "a server-authored frame carries {forbidden}: {text}"
            )
            .into());
        }
    }
    Ok(())
}

/// The method surface is two methods: a `doc.*` request is `unknown_method`, and the
/// connection stays open.
#[tokio::test]
async fn the_doc_methods_do_not_exist() -> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig::default()).await;
    let (mut host, _created) =
        Peer::join(&harness.ws_base(), "", "Ada").await?;
    let request = serde_json::json!({
        "id": 2,
        "method": "doc.open",
        "params": { "path": "src/main.rs" },
        "v": "selvage/2",
    });
    host.send_text(&request.to_string()).await?;
    let refused = host.recv_json().await?;
    assert_eq!(refused["error"]["code"], "unknown_method");

    // The connection is still open, and a rename — the version's other method — works.
    let rename = serde_json::json!({
        "id": 3,
        "method": "session.rename",
        "params": { "display_name": "Ada Lovelace" },
        "v": "selvage/2",
    });
    host.send_text(&rename.to_string()).await?;
    let answered = host.recv_json().await?;
    assert_eq!(answered["id"], 3);
    assert!(answered.get("result").is_some());
    let announced = host.recv_json().await?;
    assert_eq!(announced["event"], "peer.renamed");
    assert_eq!(announced["v"], "selvage/2");
    host.close().await;
    Ok(())
}

/// A room lives while it has connections and for `room_grace_ms` after its last one ends;
/// the destruction is silent, and the next join learns it as `room_unknown`
/// (`PROTOCOL.md` §6, §9).
#[tokio::test]
async fn a_room_is_destroyed_after_its_last_connection() -> Result<(), Failure>
{
    let harness = Harness::start_with(ServerConfig {
        room_grace: Duration::from_millis(200),
        ..ServerConfig::default()
    })
    .await;
    let (host, created) = Peer::join(&harness.ws_base(), "", "Ada").await?;
    let query = join_query(&created)?;
    host.close().await;

    sleep(Duration::from_millis(600)).await;
    let (_late, reply) = Peer::join(&harness.ws_base(), &query, "Bob").await?;
    assert_eq!(reply["event"], "session.error");
    assert_eq!(reply["params"]["code"], "room_unknown");
    Ok(())
}

/// A timer armed by one empty window cannot reap a room that emptied again later: the
/// grace is measured from the **last** connection's leave, and a stale timer that fired
/// early would destroy a room whose grace had only just started.
#[tokio::test]
async fn a_stale_grace_timer_does_not_reap_a_room_that_emptied_later()
-> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig {
        room_grace: Duration::from_millis(400),
        ..ServerConfig::default()
    })
    .await;
    let (host, created) = Peer::join(&harness.ws_base(), "", "Ada").await?;
    let query = join_query(&created)?;
    host.close().await;

    sleep(Duration::from_millis(120)).await;
    let (guest, _joined) =
        Peer::join(&harness.ws_base(), &query, "Bob").await?;
    guest.close().await;

    // Past the first timer's deadline and before the second's: the room is still here.
    sleep(Duration::from_millis(380)).await;
    let (again, joined) =
        Peer::join(&harness.ws_base(), &query, "Cara").await?;
    assert_eq!(
        joined["event"], "room.joined",
        "the stale timer reaped a room whose grace had only just started: {joined}"
    );
    again.close().await;

    // And the timer the last leave armed does reap it.
    sleep(Duration::from_millis(500)).await;
    let (_late, reply) = Peer::join(&harness.ws_base(), &query, "Dan").await?;
    assert_eq!(reply["params"]["code"], "room_unknown");
    Ok(())
}

/// `/meta` is what the configuration advertises: the one wire version, the two capability
/// names and the server's identity, on a listener that takes its sockets
/// (`PROTOCOL.md` §2).
#[tokio::test]
async fn meta_advertises_the_one_wire_version() -> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig::default()).await;
    let body = http_get(&harness.http_base(), "/meta").await?;
    let meta: Value = serde_json::from_str(&body)?;
    assert_eq!(meta["wire_versions"], serde_json::json!(["selvage/2"]));
    assert_eq!(
        meta["capabilities"],
        serde_json::json!(["y-protocols/1", "awareness"])
    );
    assert!(
        meta.get("roles").is_none(),
        "the server seats nobody, so nothing says which roles it seats: {meta}"
    );
    assert!(
        meta["server"]
            .as_str()
            .is_some_and(|name| name.starts_with("selvaged/")),
        "the server names itself: {meta}"
    );
    Ok(())
}
