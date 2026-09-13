//! Presence: the parts of awareness that need a second, independent speaker.
//!
//! The raw client here speaks the session handshake by hand, builds its awareness frame
//! with `yrs` directly, and then stops renewing — which is what makes the expiry path
//! observable in under a second.

use std::error::Error as StdError;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_client::{ConnectOptions, Role, SyncEngine};
use selvage_harness::{Harness, Presence, wait_for};
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

    // Renew often so the expiry check runs promptly; expire quickly so the test does.
    let observer = SyncEngine::connect(
        ConnectOptions::guest(harness.ws_base(), "Bob", room.invite())
            .with_keepalive(
                Duration::from_millis(40),
                Duration::from_millis(250),
            ),
    )
    .await
    .expect("the observer joins");

    // A peer that publishes awareness once and then goes quiet.
    let url = proto::session_url(
        &harness.ws_base(),
        Some(&room.id),
        Some(&room.token),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("the silent peer connects");
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
    .await
    .expect("says hello");
    wait_for_room_joined(&mut ws)
        .await
        .expect("the server seats the silent peer");

    let frame = {
        let mut encoder = EncoderV1::new();
        YMessage::Awareness(awareness.update().expect("an awareness update"))
            .encode(&mut encoder);
        encoder.to_vec()
    };
    ws.send(Message::binary(frame))
        .await
        .expect("publishes awareness");

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

    // Cleo never renews, so the observer must expire its state while keeping the peer.
    let remaining = wait_for("the stale state to expire", || async {
        let presence = observer.presence().await.ok()?;
        let stale = presence.iter().any(is_cleo_cursor);
        (!stale).then_some(presence.len())
    })
    .await;
    // Only the observer's own awareness is left, and Cleo is still a member.
    assert_eq!(remaining, 1);
    let peers = observer.peers().await.expect("peers");
    assert_eq!(peers.len(), 2, "got {peers:?}");
    assert_eq!(watcher.session().role, Role::Host);
}
