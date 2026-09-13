//! Presence: the parts of awareness that need a second, independent speaker.
//!
//! The raw client here speaks the session handshake by hand, builds its awareness frame
//! with `yrs` directly, and then stops renewing — which is what makes the expiry path
//! observable in under a second. Publishing a hand-written JSON state is the other half of
//! its job: it is how these tests put a frame on the wire that the reference client would
//! never produce, which is the only way to test what a receiver does with a bad one.

use std::error::Error as StdError;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_client::{ConnectOptions, Role, SyncEngine};
use selvage_harness::{
    Harness, Presence, Room, SelectionOffsets, ServerConfig, WAIT, wait_for,
    wait_for_convergence, wait_for_described,
};
use selvage_protocol as proto;
use selvage_protocol::{event, method};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;

use yrs::Doc;
use yrs::sync::{Awareness, Message as YMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};

const PATH: &str = "src/main.rs";
const OTHER: &str = "src/other.rs";

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

/// A host and a guest, both holding `PATH`, agreed on `seed`.
async fn seeded(
    harness: &Harness,
    seed: &str,
) -> Result<(SyncEngine, Room, SyncEngine), Failure> {
    let (host, room) = harness.host("Ada").await?;
    let guest = harness.join(&room, "Bob").await?;
    host.open(PATH).await?;
    guest.open(PATH).await?;
    host.insert(PATH, 0, seed).await?;
    wait_for_convergence(&host, &guest, PATH).await;
    Ok((host, room, guest))
}

/// Joins `room` by hand as Cleo and publishes exactly `state`, then goes quiet.
async fn raw_peer(
    harness: &Harness,
    room: &Room,
    state: &str,
) -> Result<(Raw, Awareness), Failure> {
    let url = proto::session_url(
        &harness.ws_base(),
        Some(&room.id),
        Some(&room.token),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await?;
    let mut awareness = Awareness::new(Doc::new());
    awareness.set_local_state_raw(state);
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

/// The client id of the replica that wrote the seed, which is what an `item` anchor into that
/// seed has to name.
fn writer_id(engine: &SyncEngine) -> Option<u64> {
    engine.session().peer.awareness_client_id
}

/// A state whose two endpoints are both `anchor`, as a caret is.
fn caret_state(anchor: &str) -> String {
    format!(
        r#"{{"path":"{PATH}","selection":{{"anchor":{anchor},"head":{anchor}}}}}"#
    )
}

/// A peer that publishes awareness once and then goes quiet: it never renews, so the
/// observer is the one that has to expire the state.
async fn silent_peer(
    harness: &Harness,
    room: &Room,
) -> Result<(Raw, Awareness), Failure> {
    let anchor = format!(r#"{{"tname":"{PATH}","assoc":0}}"#);
    raw_peer(harness, room, &caret_state(&anchor)).await
}

/// Waits until `observer` resolves `name`'s anchors to exactly `offsets`.
async fn wait_for_caret(
    observer: &SyncEngine,
    name: &str,
    offsets: SelectionOffsets,
) -> Presence {
    wait_for_described(
        &format!("{name}'s cursor to resolve to {offsets:?}"),
        || describe_presence(observer, name),
        || async {
            observer
                .presence()
                .await
                .ok()?
                .into_iter()
                .find(|p| {
                    p.display_name() == Some(name)
                        && p.selection() == Some(offsets)
                })
        },
    )
    .await
}

/// Waits until `observer` holds the state `name` published, whether or not it resolves.
async fn wait_for_state(observer: &SyncEngine, name: &str) -> Presence {
    wait_for_described(
        &format!("the state {name} published"),
        || describe_presence(observer, name),
        || async {
            observer
                .presence()
                .await
                .ok()?
                .into_iter()
                .find(|p| {
                    p.display_name() == Some(name) && p.state.is_some()
                })
        },
    )
    .await
}

/// Waits until `observer` holds the state `name` published for `path` specifically.
async fn wait_for_state_at(
    observer: &SyncEngine,
    name: &str,
    path: &str,
) -> Presence {
    wait_for_described(
        &format!("the state {name} published for {path}"),
        || describe_presence(observer, name),
        || async {
            observer
                .presence()
                .await
                .ok()?
                .into_iter()
                .find(|p| p.display_name() == Some(name) && p.path() == Some(path))
        },
    )
    .await
}

/// What an awareness wait reports when it times out: what presence held instead.
async fn describe_presence(observer: &SyncEngine, name: &str) -> String {
    match observer.presence().await {
        Err(error) => format!("the engine stopped: {error}"),
        Ok(presence) => {
            let held: Vec<&Presence> = presence
                .iter()
                .filter(|p| p.display_name() == Some(name))
                .collect();
            format!("{name}: {held:?}")
        }
    }
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
    // The observer never opened the document, so there is nothing here to resolve against.
    // What arrived is the anchors themselves, which is all this test needs to see.
    assert_eq!(
        seen.anchors().map(|s| s.anchor.tname.as_deref()),
        Some(Some(PATH)),
        "the cursor arrived as a `tname` anchor, not an offset"
    );

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

/// An absolute offset drifts by the length of every edit landing before it. An anchor does
/// not: the caret keeps naming the same element, so the *offset* it resolves to moves and the
/// position in the text stays put. This is the whole reason §8.1 carries anchors.
#[tokio::test]
async fn a_caret_follows_its_element_when_an_edit_lands_before_it() {
    let harness = Harness::start(WAIT).await;
    let (host, _room, guest) =
        seeded(&harness, "abc").await.expect("a seeded room");

    guest
        .set_selection(PATH, SelectionOffsets::caret(2))
        .await
        .expect("the guest puts a caret before `c`");
    let before = wait_for_caret(&host, "Bob", SelectionOffsets::caret(2)).await;
    let text_before = host.text(PATH).await.expect("the text");
    assert_eq!(text_before.chars().nth(2), Some('c'));

    host.insert(PATH, 0, "XYZ")
        .await
        .expect("the host types above the caret");

    // Three characters went in above it, so the resolved offset is three further along.
    let after = wait_for_caret(&host, "Bob", SelectionOffsets::caret(5)).await;
    let text_after = host.text(PATH).await.expect("the text");
    assert_eq!(text_after, "XYZabc");
    assert_eq!(
        text_after.chars().nth(5),
        Some('c'),
        "the caret still sits before the same character it did"
    );
    // Nothing was republished: the same anchors resolved to a different index.
    assert_eq!(before.anchors(), after.anchors());
}

/// The end of a text has no element to name, so §8.1 requires the `tname` form — and that
/// form follows appends forever, which is what a caret at the end of a file should do.
#[tokio::test]
async fn a_caret_at_the_end_stays_at_the_end_when_the_document_grows() {
    let harness = Harness::start(WAIT).await;
    let (host, _room, guest) =
        seeded(&harness, "abc").await.expect("a seeded room");

    guest
        .set_selection(PATH, SelectionOffsets::caret(3))
        .await
        .expect("the guest puts its caret at the end");
    let seen = wait_for_caret(&host, "Bob", SelectionOffsets::caret(3)).await;
    assert_eq!(
        seen.anchors().map(|s| s.anchor.tname.as_deref()),
        Some(Some(PATH)),
        "a position with no element to name is published as `tname`"
    );

    host.insert(PATH, 3, "de").await.expect("the host appends");

    wait_for_caret(&host, "Bob", SelectionOffsets::caret(5)).await;
    assert_eq!(host.text(PATH).await.expect("the text"), "abcde");
}

/// The seam's unit is UTF-16 code units, and an anchor is computed *from* an offset, so a
/// client counting bytes would anchor this caret to the wrong element entirely.
#[tokio::test]
async fn a_caret_after_a_non_ascii_character_is_a_utf16_offset() {
    let harness = Harness::start(WAIT).await;
    let (host, _room, guest) =
        seeded(&harness, "héllo").await.expect("a seeded room");

    // `é` is one UTF-16 code unit and two UTF-8 bytes, so offset 2 is the first `l`.
    guest
        .set_selection(PATH, SelectionOffsets::caret(2))
        .await
        .expect("the guest puts a caret after `hé`");
    wait_for_caret(&host, "Bob", SelectionOffsets::caret(2)).await;

    // `😀` is two UTF-16 code units and four UTF-8 bytes, so a byte count would say 4.
    host.insert(PATH, 0, "😀").await.expect("the host prepends");

    wait_for_caret(&host, "Bob", SelectionOffsets::caret(4)).await;
    let text = host.text(PATH).await.expect("the text");
    assert_eq!(text, "😀héllo");
    assert_eq!(
        text.chars().nth(3),
        Some('l'),
        "UTF-16 offset 4 is the fourth character, because `😀` is two units"
    );
}

/// A never-seen element cannot be resolved, and §8.1 forbids falling back to an offset or
/// clamping to a guess: the peer is present, with no cursor.
#[tokio::test]
async fn an_anchor_naming_an_unknown_element_shows_no_selection() {
    let harness = Harness::start(WAIT).await;
    let (host, room, _guest) =
        seeded(&harness, "abc").await.expect("a seeded room");

    let state =
        caret_state(r#"{"item":{"client":424242,"clock":7},"assoc":0}"#);
    let (raw, _) = raw_peer(&harness, &room, &state)
        .await
        .expect("the hand-built peer publishes");

    let seen = wait_for_state(&host, "Cleo").await;
    assert_eq!(seen.path(), Some(PATH), "the state itself arrived");
    assert!(seen.anchors().is_some(), "the anchors are still readable");
    assert_eq!(
        seen.selection(),
        None,
        "an element this replica has never seen is no cursor"
    );
    drop(raw);
}

/// `tname` *is* the document path, so one naming another document resolves into another type.
/// §8.1 makes that a failure, and the branch check is what catches it — the receiver holds
/// that other document here, so the anchor resolves and only the check rejects it.
#[tokio::test]
async fn an_anchor_naming_another_document_shows_no_selection() {
    let harness = Harness::start(WAIT).await;
    let (host, room, _guest) =
        seeded(&harness, "abc").await.expect("a seeded room");
    host.open(OTHER)
        .await
        .expect("the host opens a second document");

    let state = caret_state(&format!(r#"{{"tname":"{OTHER}","assoc":0}}"#));
    let (raw, _) = raw_peer(&harness, &room, &state)
        .await
        .expect("the hand-built peer publishes");

    let seen = wait_for_state(&host, "Cleo").await;
    assert_eq!(seen.path(), Some(PATH));
    assert_eq!(
        seen.selection(),
        None,
        "a `tname` that is not the state's path is a mismatch"
    );
    drop(raw);
}

/// A sender must not manufacture a position. §8.1 forbids a *receiver* from clamping to a
/// guess, but the sender is where the guess was being made: `sticky_index` returns `None` for
/// an offset past the end as well as for the end itself, so the scope-only fallback turned
/// "there is no such position here" into "the caret is at the end of the text" — a state that
/// every peer resolves and that nothing on the wire distinguishes from the real thing.
#[tokio::test]
async fn a_sender_publishes_no_selection_it_cannot_anchor() {
    let harness = Harness::start(WAIT).await;
    let (host, _room, guest) =
        seeded(&harness, "abc").await.expect("a seeded room");

    // Opened, but nobody has written to it: this replica holds no `Y.Text` for the path, so
    // it cannot say where an offset in it is.
    host.open(OTHER).await.expect("the host opens a second document");
    host.set_selection(OTHER, SelectionOffsets::caret(0))
        .await
        .expect("the host names the document");
    let unwritten = wait_for_state_at(&guest, "Ada", OTHER).await;
    assert_eq!(
        unwritten.anchors(),
        None,
        "a document this replica has not received is not a position in it"
    );

    // And an offset past the end of a text it does hold is not one either.
    host.set_selection(PATH, SelectionOffsets::caret(99))
        .await
        .expect("the host names an offset past the end");
    let past = wait_for_state_at(&guest, "Ada", PATH).await;
    assert_eq!(past.anchors(), None, "there is no element at offset 99");

    // The contrast: a position that does exist is published, as an anchor.
    host.set_selection(PATH, SelectionOffsets::caret(3))
        .await
        .expect("the host puts its caret at the end of the text");
    let held = wait_for_caret(&guest, "Ada", SelectionOffsets::caret(3)).await;
    assert_eq!(
        held.anchors().map(|s| s.anchor.tname.as_deref()),
        Some(Some(PATH)),
        "the end of the text is the scope-only form §8.1 requires"
    );
}

/// The exact shape a yjs peer puts on the wire for a position inside a root type: `tname`
/// naming the type *and* `item` naming the element in it. `yrs` collapses the two and emits
/// `item` alone, so this pair only ever arrives from the other implementation — and treating
/// it as malformed would show no cursor at all for every yjs peer, with nothing on the wire
/// to say why. `item` is authoritative; `tname` is a check on it.
#[tokio::test]
async fn a_yjs_anchor_carrying_both_tname_and_item_resolves() {
    let harness = Harness::start(WAIT).await;
    let (host, room, _guest) =
        seeded(&harness, "abc").await.expect("a seeded room");
    let writer = writer_id(&host).expect("the host's replica has a client id");

    let anchor = format!(
        r#"{{"tname":"{PATH}","item":{{"client":{writer},"clock":1}},"assoc":0}}"#
    );
    let (raw, _) = raw_peer(&harness, &room, &caret_state(&anchor))
        .await
        .expect("the hand-built peer publishes");

    // Clock 1 is the second character the host wrote, so the caret sits before `b`.
    let seen = wait_for_caret(&host, "Cleo", SelectionOffsets::caret(1)).await;
    assert_eq!(seen.path(), Some(PATH));
    drop(raw);
}

/// The scope is still checked when an element sits beside it: a `tname` naming another
/// document is a mismatch even though the `item` next to it would have resolved on its own.
#[tokio::test]
async fn a_yjs_anchor_whose_tname_is_not_the_path_shows_no_selection() {
    let harness = Harness::start(WAIT).await;
    let (host, room, _guest) =
        seeded(&harness, "abc").await.expect("a seeded room");
    let writer = writer_id(&host).expect("the host's replica has a client id");

    let anchor = format!(
        r#"{{"tname":"{OTHER}","item":{{"client":{writer},"clock":1}},"assoc":0}}"#
    );
    let (raw, _) = raw_peer(&harness, &room, &caret_state(&anchor))
        .await
        .expect("the hand-built peer publishes");

    let seen = wait_for_state(&host, "Cleo").await;
    assert_eq!(seen.path(), Some(PATH));
    assert_eq!(
        seen.selection(),
        None,
        "the scope must be the document the state was published for"
    );
    drop(raw);
}

/// Adding a member must never be a protocol break (§4.1, §8.1). A receiver ignores what it
/// does not know — in the state object and inside an anchor alike — and resolves the rest.
#[tokio::test]
async fn unknown_keys_are_ignored_and_the_selection_still_resolves() {
    let harness = Harness::start(WAIT).await;
    let (host, room, _guest) =
        seeded(&harness, "abc").await.expect("a seeded room");

    let anchor =
        format!(r#"{{"tname":"{PATH}","assoc":0,"nudge":"a later version"}}"#);
    let state = format!(
        r#"{{"path":"{PATH}","selection":{{"anchor":{anchor},"head":{anchor}}},"mood":"calm","visibleRanges":[[0,3]]}}"#
    );
    let (raw, _) = raw_peer(&harness, &room, &state)
        .await
        .expect("the hand-built peer publishes");

    // `tname` with `assoc` 0 is the end of the text, which is 3 characters long here.
    let seen = wait_for_caret(&host, "Cleo", SelectionOffsets::caret(3)).await;
    assert_eq!(seen.path(), Some(PATH));
    drop(raw);
}
