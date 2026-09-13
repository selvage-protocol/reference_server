//! Presence: the parts of awareness that need a second, independent speaker.
//!
//! The raw client here speaks the session handshake by hand, builds its awareness frame
//! with `yrs` directly, and then stops renewing — which is what makes the expiry path
//! observable in under a second.

use std::error::Error as StdError;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_client::{ConnectOptions, Role, SyncEngine};
use selvage_harness::{Harness, Presence, Room, ServerConfig, wait_for};
use selvage_protocol as proto;
use selvage_protocol::{event, method};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;

use yrs::Doc;
use yrs::sync::{Awareness, Message as YMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};

/// Anything this test can fail with.
type Failure = Box<dyn StdError>;

/// True when this presence record is Cleo's cursor.
fn is_cleo_cursor(presence: &Presence) -> bool {
    presence.display_name() == Some("Cleo") && presence.path().is_some()
}

type Raw = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Joins `room` as an observer that runs the awareness clock the server advertises.
async fn observer_on_the_servers_clock(
    harness: &Harness,
    room: &Room,
) -> Result<SyncEngine, Failure> {
    let options =
        ConnectOptions::guest(harness.ws_base(), "Bob", room.invite());
    Ok(SyncEngine::connect(options).await?)
}

/// Joins `room` as an observer whose awareness clock is compressed locally: renew often so
/// the expiry check runs promptly, expire quickly so the test costs no time.
async fn observer_on_a_local_clock(
    harness: &Harness,
    room: &Room,
) -> Result<SyncEngine, Failure> {
    let options =
        ConnectOptions::guest(harness.ws_base(), "Bob", room.invite())
            .with_keepalive(
                Duration::from_millis(40),
                Duration::from_millis(250),
            );
    Ok(SyncEngine::connect(options).await?)
}

/// A peer that publishes awareness once and then goes quiet: it never renews, so the
/// observer is the one that has to expire the state.
async fn silent_peer(
    harness: &Harness,
    room: &Room,
) -> Result<(Raw, Awareness), Failure> {
    let url = proto::session_url(
        &harness.ws_base(),
        Some(&room.id),
        Some(&room.token),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await?;
    let mut awareness = Awareness::new(Doc::new());
    awareness.set_local_state_raw(
        r#"{"path":"src/main.rs","selection":{"anchor":1,"head":1}}"#,
    );
    ws.send(Message::text(
        serde_json::json!({
            "v": proto::WIRE_VERSION,
            "id": 1,
            "method": method::SESSION_HELLO,
            "params": {
                "display_name": "Cleo",
                // How the session layer attributes this cursor to a peer.
                "awareness_client_id": awareness.client_id().get(),
            },
        })
        .to_string(),
    ))
    .await?;
    wait_for_room_joined(&mut ws).await?;

    let mut encoder = EncoderV1::new();
    YMessage::Awareness(awareness.update()?).encode(&mut encoder);
    ws.send(Message::binary(encoder.to_vec())).await?;
    Ok((ws, awareness))
}

/// Waits for the silent peer's cursor on `observer` and then for it to expire, returning
/// what presence is left afterwards.
///
/// # Panics
///
/// Panics when the cursor never appears or never expires.
async fn wait_for_cursor_to_expire(observer: &SyncEngine) -> Vec<Presence> {
    let seen = wait_for("the silent peer's cursor to appear", || async {
        observer
            .presence()
            .await
            .ok()?
            .into_iter()
            .find(is_cleo_cursor)
    })
    .await;
    assert_eq!(seen.selection().map(|s| s.anchor), Some(1));

    wait_for("the stale state to expire", || async {
        let presence = observer.presence().await.ok()?;
        let stale = presence.iter().any(is_cleo_cursor);
        (!stale).then_some(presence)
    })
    .await
}

/// Waits until the server has seated this connection, so the next frame is relayed.
async fn wait_for_room_joined(ws: &mut Raw) -> Result<(), Failure> {
    while let Some(frame) = ws.next().await {
        let Ok(Message::Text(text)) = frame else {
            return Err(format!("expected room.joined, got {frame:?}").into());
        };
        let msg: Value = serde_json::from_str(&text)?;
        if msg.get("event").and_then(Value::as_str) == Some(event::ROOM_JOINED)
        {
            return Ok(());
        }
    }
    Err("connection ended before room.joined".into())
}

#[tokio::test]
async fn a_silent_peer_disappears_when_its_awareness_expires() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (watcher, room) = harness.host("Ada").await.expect("host connects");
    let observer = observer_on_a_local_clock(&harness, &room)
        .await
        .expect("the observer joins");
    let (silent, _) = silent_peer(&harness, &room)
        .await
        .expect("the silent peer connects and publishes");

    // Only the observer's own awareness is left, and Cleo is still a member: the host
    // renews on the y-protocols default, so its cursor has gone stale too.
    let left = wait_for_cursor_to_expire(&observer).await;
    assert_eq!(left.len(), 1, "only the observer's own awareness is left");
    assert_eq!(observer.peers().await.expect("peers").len(), 2);
    assert_eq!(watcher.session().role, Role::Host);
    drop(silent);
}

/// The server advertises the session's awareness clock and a client runs on it. Here the
/// *advertised* window is compressed, so a client that ignored it would still be waiting
/// fifteen seconds for a state that should have expired in a quarter of one.
#[tokio::test]
async fn the_client_runs_on_the_awareness_clock_the_server_advertises() {
    let advertised = proto::Keepalive {
        ping_interval_ms: 30_000,
        awareness_renew_ms: 40,
        awareness_expire_ms: 250,
    };
    let harness = Harness::start_with(ServerConfig {
        keepalive: advertised,
        ..ServerConfig::default()
    })
    .await;
    let (watcher, room) = harness.host("Ada").await.expect("host connects");
    let observer = observer_on_the_servers_clock(&harness, &room)
        .await
        .expect("the observer joins");
    assert_eq!(observer.session().keepalive, advertised);
    let (silent, _) = silent_peer(&harness, &room)
        .await
        .expect("the silent peer connects and publishes");

    // The host runs on the advertised clock too, so it renews while Cleo goes quiet:
    // exactly the silent peer's state is forgotten.
    let left = wait_for_cursor_to_expire(&observer).await;
    assert_eq!(
        left.len(),
        2,
        "the renewing peers are still there: {left:?}"
    );
    assert!(left.iter().any(|p| p.display_name() == Some("Ada")));
    assert_eq!(watcher.session().role, Role::Host);
    drop(silent);
}
