//! Presence: the parts of awareness that need a second, independent speaker.
//!
//! The raw client here speaks the session handshake by hand, builds its awareness frame
//! with `yrs` directly, and then stops renewing — which is what makes the expiry path
//! observable in under a second.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_client::{ConnectOptions, SyncEngine};
use selvage_harness::Harness;
use tokio_tungstenite::tungstenite::Message;

use yrs::sync::{Awareness, Message as YMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::Doc;

#[tokio::test]
async fn a_silent_peer_disappears_when_its_awareness_expires() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (watcher, room) = harness.host("Ada").await.expect("host connects");

    // Renew often so the expiry check runs promptly; expire quickly so the test does.
    let observer = SyncEngine::connect(
        ConnectOptions::guest(
            harness.ws_base(),
            "Bob",
            room.id.clone(),
            room.token.clone(),
        )
        .with_keepalive(Duration::from_millis(40), Duration::from_millis(250)),
    )
    .await
    .expect("the observer joins");

    // A peer that publishes awareness once and then goes quiet.
    let (mut ws, _) = tokio_tungstenite::connect_async(selvage_protocol::session_url(
        &harness.ws_base(),
        Some(&room.id),
        Some(&room.token),
    ))
    .await
    .expect("the silent peer connects");
    let mut awareness = Awareness::new(Doc::new());
    awareness.set_local_state_raw(r#"{"path":"src/main.rs","selection":{"anchor":1,"head":1}}"#);
    ws.send(Message::text(
        serde_json::json!({
            "v": selvage_protocol::WIRE_VERSION,
            "id": 1,
            "method": selvage_protocol::method::SESSION_HELLO,
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

    let frame = {
        let mut encoder = EncoderV1::new();
        YMessage::Awareness(awareness.update().expect("an awareness update")).encode(&mut encoder);
        encoder.to_vec()
    };
    // Publish only once the server has seated this connection, so the frame is relayed.
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                let msg: serde_json::Value = serde_json::from_str(&text).expect("JSON");
                if msg["event"] == selvage_protocol::event::ROOM_JOINED {
                    break;
                }
            }
            other => panic!("expected room.joined, got {other:?}"),
        }
    }
    ws.send(Message::binary(frame))
        .await
        .expect("publishes awareness");

    let seen = selvage_harness::wait_for("the silent peer's cursor to appear", || async {
        observer
            .presence()
            .await
            .ok()?
            .into_iter()
            .find(|p| p.display_name() == Some("Cleo") && p.path().is_some())
    })
    .await;
    assert_eq!(seen.selection().map(|s| s.anchor), Some(1));

    // Cleo never renews, so the observer must expire its state while keeping the peer.
    let remaining = selvage_harness::wait_for("the stale state to expire", || async {
        let presence = observer.presence().await.ok()?;
        let stale = presence
            .iter()
            .any(|p| p.display_name() == Some("Cleo") && p.path().is_some());
        (!stale).then_some(presence.len())
    })
    .await;
    // Only the observer's own awareness is left, and Cleo is still a member.
    assert_eq!(remaining, 1);
    let peers = observer.peers().await.expect("peers");
    assert_eq!(peers.len(), 2, "got {peers:?}");
    assert_eq!(watcher.session().role, selvage_client::Role::Host);
}
