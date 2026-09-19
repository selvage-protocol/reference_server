//! Reconnection (`PROTOCOL.md` §9.1): a dropped socket is re-helloed by the client
//! itself, with a bounded backoff, as a fresh peer. The drop is a relay cut in front of
//! the server, so the server and the room stay up — aborting its accept loop would take
//! the room with it, and no session method closes one peer's socket.

use std::error::Error as StdError;
use std::time::{Duration, Instant};

use selvage_client::{ConnectOptions, ReconnectPolicy, SyncEngine};
use selvage_harness::{
    DropProxy, EngineEvent, Error, Harness, Presence, Role, SelectionOffsets,
    ServerConfig, WAIT, wait_for, wait_for_described, wait_for_event,
    wait_for_peer,
};
use selvage_protocol::code;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::timeout;

const PATH: &str = "src/main.rs";
const GUEST_ONLY: &str = "only/guest.rs";

/// Anything these tests can fail with.
type Failure = Box<dyn StdError>;

/// The next room-documents announcement containing `wanted`: the proof that a
/// reconnecting client re-opened its document. The room's set keeps the path while its
/// socket is gone — claims are forgotten, paths stay — so the set itself proves
/// nothing; the `doc.opened` the room announces on re-open does.
async fn wait_for_reopen(
    events: &mut broadcast::Receiver<EngineEvent>,
    wanted: &str,
) -> Result<(), Failure> {
    loop {
        match events.recv().await {
            Ok(EngineEvent::DocumentsChanged { documents })
                if documents.iter().any(|path| path == wanted) =>
            {
                return Ok(());
            }
            Ok(_) => {}
            Err(_) => return Err("the event stream closed".into()),
        }
    }
}

#[tokio::test]
async fn a_dropped_guest_reconnects_and_reopens_its_document()
-> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let host_proxy = DropProxy::start(&harness.upstream()).await?;
    let guest_proxy = DropProxy::start(&harness.upstream()).await?;
    let (host, room) = harness.host_at(&host_proxy.ws_base(), "Ada").await?;
    let guest = harness
        .join_at(&guest_proxy.ws_base(), &room, "Bob")
        .await?;

    host.open(PATH).await?;
    guest.open(PATH).await?;
    host.insert(PATH, 0, "shared\n").await?;
    wait_for("the guest to receive the seed", || async {
        (guest.text(PATH).await.ok()? == "shared\n").then_some(())
    })
    .await;

    // A document only this guest holds, so the room's set is the evidence that the
    // reconnecting client re-opened it.
    guest.open(GUEST_ONLY).await?;
    guest.insert(GUEST_ONLY, 0, "guest keeps this\n").await?;
    wait_for("the host to see the guest's document", || async {
        host.documents()
            .await
            .ok()?
            .contains(&GUEST_ONLY.to_string())
            .then_some(())
    })
    .await;

    let before = guest.session();
    let old_peer_id = before.peer.peer_id.clone();
    let old_client_id = before.peer.awareness_client_id;

    // `PATH` closes before the drop, so `GUEST_ONLY` is the only document the
    // reconnected client re-opens: the wait below then observes that re-open, not
    // the `PATH` re-open whose full-set announcement already contains `GUEST_ONLY`.
    guest.close(PATH).await?;

    // Subscribed before the drop: the re-open announcement must not slip past the
    // wait below.
    let mut host_events = host.subscribe();
    guest_proxy.drop_all();

    // The set keeps the path while its socket is gone — claims are forgotten, paths
    // stay — so it is already there before the client comes back, and waiting on the
    // set itself would prove nothing.
    assert!(
        host.documents().await?.contains(&GUEST_ONLY.to_string()),
        "the room keeps a dropped peer's paths"
    );

    // It comes back as a new peer with a new awareness client id (spec §9.1: `yrs`
    // tombstones the old one, so reusing it would drop the first republish).
    let after = wait_for_described(
        "the guest to be reseated as a fresh peer",
        || async { format!("{:?}", guest.session()) },
        || async {
            let session = guest.session();
            (session.peer.peer_id != old_peer_id).then_some(session)
        },
    )
    .await;
    assert_ne!(after.peer.awareness_client_id, old_client_id);
    assert_eq!(after.role, Role::Guest);
    assert_eq!(after.room_id, room.id);

    // The host sees the old peer leave and a new one arrive.
    wait_for_described(
        "the host to see the guest rejoin",
        || async { format!("{:?}", host.peers().await) },
        || async {
            let peers = host.peers().await.ok()?;
            let bob = peers.iter().find(|peer| peer.display_name == "Bob")?;
            (bob.peer_id != old_peer_id).then_some(())
        },
    )
    .await;

    // Its own document is open again: the room announced the re-open, which only a
    // fresh `doc.open` from the reconnected client produces.
    timeout(WAIT, wait_for_reopen(&mut host_events, GUEST_ONLY))
        .await
        .expect("the guest to re-open its document")?;

    // Both documents still hold what they held, and the guest's own content was not
    // lost with the socket.
    assert_eq!(guest.text(PATH).await?, "shared\n");
    assert_eq!(guest.text(GUEST_ONLY).await?, "guest keeps this\n");
    assert_eq!(host.text(PATH).await?, "shared\n");
    Ok(())
}

#[tokio::test]
async fn a_dropped_host_reclaims_the_room() -> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let host_proxy = DropProxy::start(&harness.upstream()).await?;
    let (host, room) = harness.host_at(&host_proxy.ws_base(), "Ada").await?;
    let guest = harness.join(&room, "Bob").await?;

    host.open(PATH).await?;
    guest.open(PATH).await?;
    host.insert(PATH, 0, "shared\n").await?;
    wait_for("the guest to receive the seed", || async {
        (guest.text(PATH).await.ok()? == "shared\n").then_some(())
    })
    .await;

    let before = host.session();
    let old_peer_id = before.peer.peer_id.clone();
    let old_client_id = before.peer.awareness_client_id;

    let detached = wait_for_event(&guest, "host.detached", |event| {
        matches!(event, EngineEvent::HostDetached { .. })
    });
    host_proxy.drop_all();
    detached.await;

    // A reclaiming host is a new peer as well as the new host.
    let reattached = wait_for_event(&guest, "host.attached", |event| {
        matches!(event, EngineEvent::HostAttached { .. })
    });
    let EngineEvent::HostAttached { peer } = reattached.await else {
        panic!("host.attached");
    };
    assert_eq!(peer.display_name, "Ada");
    assert_eq!(peer.role, Role::Host);
    assert_ne!(peer.peer_id, old_peer_id);

    // The host is reseated in the same room, under the token it minted with, and is a
    // fresh awareness client.
    let after = wait_for_described(
        "the host to be reseated as a new peer",
        || async { format!("{:?}", host.session()) },
        || async {
            let session = host.session();
            (session.peer.peer_id != old_peer_id).then_some(session)
        },
    )
    .await;
    assert_eq!(after.room_id, room.id);
    assert_eq!(after.role, Role::Host);
    assert_ne!(after.peer.awareness_client_id, old_client_id);

    // Reclaiming rather than minting: the room kept its document set, and the host kept
    // the content that only it held.
    assert_eq!(host.text(PATH).await?, "shared\n");
    assert!(host.documents().await?.contains(&PATH.to_string()));

    host.insert(PATH, 0, "// host again\n").await?;
    let seen = wait_for("the guest to see the host's new edit", || async {
        let text = guest.text(PATH).await.ok()?;
        text.contains("host again").then_some(text)
    })
    .await;
    assert_eq!(seen, "// host again\nshared\n");
    Ok(())
}

#[tokio::test]
async fn a_destroyed_room_is_terminal() -> Result<(), Failure> {
    // The host's grace period is shorter than the client's first retry, so the room is
    // gone by the time either side re-hellos — the refusal is `room_unknown`.
    let harness = Harness::start(Duration::from_millis(300)).await;
    let host_proxy = DropProxy::start(&harness.upstream()).await?;
    let guest_proxy = DropProxy::start(&harness.upstream()).await?;
    let (_host, room) = harness.host_at(&host_proxy.ws_base(), "Ada").await?;
    let guest = harness
        .join_at(&guest_proxy.ws_base(), &room, "Bob")
        .await?;
    guest.open(PATH).await?;

    let gone = wait_for_event(&guest, "room.gone", |event| {
        matches!(event, EngineEvent::RoomGone { .. })
    });
    host_proxy.drop_all();
    let EngineEvent::RoomGone { reason } = gone.await else {
        panic!("room.gone");
    };
    assert_eq!(reason, "host did not return");

    // The refusal is terminal, so the client stops and says so rather than reconnecting
    // into the same refusal. Its engine ends: commands fail rather than wait for a
    // connection that will never come.
    let stopped = wait_for_described(
        "the guest to stop retrying",
        || async { format!("{:?}", guest.text(PATH).await) },
        || async {
            guest.text(PATH).await.err().map(|error| error.to_string())
        },
    )
    .await;
    assert_eq!(stopped, "the session is closed");

    // The id is gone for good, on any URL, with any token.
    let refused = harness
        .join(&room, "Late")
        .await
        .expect_err("the room is gone");
    let Error::Protocol { code, .. } = refused else {
        panic!("expected room_unknown, got {refused}");
    };
    assert_eq!(code, code::ROOM_UNKNOWN);

    // Sanity: `WAIT` is the only deadline any of this leans on.
    assert!(WAIT >= Duration::from_secs(5));
    Ok(())
}

/// A re-seat is a new peer, and its awareness client id is new with it: the peers that were
/// already in the room have never seen this client's state under that id, and the ones that
/// joined while it was away have never seen it at all. An engine that suppressed the seat's publish
/// because the state had not changed would leave every peer blind to this client's cursor
/// until the next renewal — so the seat publishes regardless, and only a caller's path
/// (`set_selection`) suppresses a state it already published.
#[tokio::test]
async fn a_reseated_guest_publishes_its_selection_again() -> Result<(), Failure>
{
    let harness = Harness::start(Duration::from_secs(30)).await;
    let proxy = DropProxy::start(&harness.upstream()).await?;
    let (host, room) = harness.host("Ada").await?;
    let guest = harness.join_at(&proxy.ws_base(), &room, "Bob").await?;

    host.open(PATH).await?;
    guest.open(PATH).await?;
    host.insert(PATH, 0, "shared\n").await?;
    wait_for("the guest to receive the seed", || async {
        (guest.text(PATH).await.ok()? == "shared\n").then_some(())
    })
    .await;

    guest
        .set_selection(PATH, SelectionOffsets::caret(3))
        .await?;
    let old_peer_id = guest.session().peer.peer_id.clone();
    let old_client_id = guest.session().peer.awareness_client_id;
    let before = wait_for_described(
        "the host to see Bob's cursor",
        || async { format!("{:?}", host.presence().await) },
        || async {
            host.presence()
                .await
                .ok()?
                .into_iter()
                .find(|presence| bobs_cursor(presence, old_client_id))
        },
    )
    .await;

    proxy.drop_all();

    let seated = wait_for_described(
        "the guest to be reseated as a fresh peer",
        || async { format!("{:?}", guest.session()) },
        || async {
            let session = guest.session();
            (session.peer.peer_id != old_peer_id).then_some(session)
        },
    )
    .await;
    assert_ne!(
        seated.peer.awareness_client_id, old_client_id,
        "a reseat is a fresh awareness client"
    );

    // The host forgot the old id along with the peer that owned it, so a cursor that is back
    // is the new connection's — published at the seat, with no renewal the test waits for.
    let restored = wait_for_described(
        "Bob's cursor to come back under the new awareness client id",
        || async { format!("{:?}", host.presence().await) },
        || async {
            let wanted = guest.session().peer.awareness_client_id;
            host.presence()
                .await
                .ok()?
                .into_iter()
                .find(|presence| bobs_cursor(presence, wanted))
        },
    )
    .await;
    assert_ne!(restored.client_id, before.client_id);
    Ok(())
}

/// A client that reconnects into a room must learn the grant the room has *now* and must
/// not keep the listing it held before: `doc.granted` reaches a joiner only when the
/// listing is non-empty, so a room whose grant was emptied while the client was away has
/// no frame to correct it (`PROTOCOL.md` §6.3). The listing is carried on the connection,
/// not in `room.joined`, so a seat starts from nothing.
#[tokio::test]
async fn a_reseated_guest_drops_a_grant_the_room_no_longer_has()
-> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let proxy = DropProxy::start(&harness.upstream()).await?;
    let (host, room) = harness.host("Ada").await?;
    let guest = harness.join_at(&proxy.ws_base(), &room, "Bob").await?;

    let listed = vec!["README.md".to_string(), "src/main.rs".to_string()];
    host.grant(listed.clone()).await?;
    wait_for_described(
        "the guest to receive the room's grant",
        || async { format!("{:?}", guest.granted_paths().await) },
        || async {
            let held = guest.granted_paths().await.ok()?;
            (held == listed).then_some(held)
        },
    )
    .await;

    let old_peer_id = guest.session().peer.peer_id.clone();
    proxy.drop_all();
    // The room's grant empties while the guest is away. No event will announce it.
    host.grant(Vec::new()).await?;

    wait_for_described(
        "the guest to be reseated as a fresh peer",
        || async { format!("{:?}", guest.session()) },
        || async {
            let session = guest.session();
            (session.peer.peer_id != old_peer_id).then_some(session)
        },
    )
    .await;
    wait_for_described(
        "the guest to drop the listing the room no longer has",
        || async { format!("{:?}", guest.granted_paths().await) },
        || async {
            let held = guest.granted_paths().await.ok()?;
            held.is_empty().then_some(held)
        },
    )
    .await;
    Ok(())
}

/// A drop the client is going to retry is said out loud (`PROTOCOL.md` §9.1): an adapter
/// shows `reconnecting` rather than inferring the retry from silence, and the re-seat
/// follows as its own event. The subscription is taken before the cut, and the loop reads past
/// the awareness renewals that arrive in between.
#[tokio::test]
async fn a_retrying_drop_is_announced_as_reconnecting() -> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(30)).await;
    let proxy = DropProxy::start(&harness.upstream()).await?;
    let (host, room) = harness.host("Ada").await?;
    let guest = harness.join_at(&proxy.ws_base(), &room, "Bob").await?;
    host.open(PATH).await?;
    guest.open(PATH).await?;
    let old_peer_id = guest.session().peer.peer_id.clone();

    let mut events = guest.subscribe();
    proxy.drop_all();
    let mut announced = timeout(WAIT, events.recv())
        .await?
        .map_err(|_| "the engine stream closed")?;
    while !matches!(announced, EngineEvent::Reconnecting) {
        announced = timeout(WAIT, events.recv())
            .await
            .map_err(|_| "no `reconnecting` arrived before the deadline")?
            .map_err(|_| "the engine stream closed")?;
    }

    // The retry then runs: its outcome is the event that follows a `reconnecting`, and the
    // guest is seated as a fresh peer rather than dropped.
    wait_for_described(
        "the guest to be reseated",
        || async { format!("{:?}", guest.session()) },
        || async {
            let session = guest.session();
            (session.peer.peer_id != old_peer_id).then_some(session)
        },
    )
    .await;
    Ok(())
}

/// Bob's cursor where this test put it, under the awareness client id `wanted` — or under any
/// of them, when the caller has no id in mind yet.
fn bobs_cursor(presence: &Presence, wanted: Option<u64>) -> bool {
    presence.display_name() == Some("Bob")
        && presence.selection() == Some(SelectionOffsets::caret(3))
        && wanted.is_none_or(|client_id| presence.client_id == client_id)
}

/// A handshake refused with an `x.*` code is not re-helloed (`PROTOCOL.md` §9.1): the code
/// is one the document does not define, and every one of them is a stop, whether or not the
/// client knows it. A full room is the reachable case — the room this client left refilled
/// under it — and the count of connections through its relay is what makes "one attempt"
/// observable rather than inferred from a clock.
#[tokio::test]
async fn a_reconnect_into_a_full_room_makes_one_attempt() -> Result<(), Failure>
{
    let harness = Harness::start_with(ServerConfig {
        room_grace: Duration::from_secs(30),
        max_peers_per_room: 2,
        ..ServerConfig::default()
    })
    .await;
    let host_proxy = DropProxy::start(&harness.upstream()).await?;
    let guest_proxy = DropProxy::start(&harness.upstream()).await?;
    let (host, room) = harness.host_at(&host_proxy.ws_base(), "Ada").await?;
    // A first retry later than any join below, so the room is deterministically full when
    // the client re-hellos rather than racing a local join against a 500 ms backoff.
    let options =
        ConnectOptions::guest(guest_proxy.ws_base(), "Bob", room.invite())
            .with_reconnect(ReconnectPolicy {
                initial_delay: Duration::from_secs(2),
                ..ReconnectPolicy::default()
            });
    let guest = SyncEngine::connect(options).await?;
    // The relay's first connection is the client's `/meta` read — §9.1 sizes the retry
    // budget from the grace it carries — and the second is the session itself.
    assert_eq!(
        guest_proxy.accepted(),
        2,
        "the meta read and the session are relayed"
    );
    // Wait for the seat to be taken before taking it away: a roster that has not yet heard
    // `peer.joined` is empty for the same reason one that has heard `peer.left` is, and the
    // wait below would return before the drop had happened at all.
    wait_for_peer(&host, "Bob").await;

    guest_proxy.drop_all();
    // The seat the drop frees is taken before the client retries: the room seats two.
    wait_for("the room to notice the dropped guest", || async {
        host.peers().await.ok()?.is_empty().then_some(())
    })
    .await;
    let _filler = harness.join(&room, "Cyd").await?;

    wait_for_event(
        &guest,
        "the refused reconnect to end the session",
        |event| matches!(event, EngineEvent::Disconnected),
    )
    .await;
    assert_eq!(
        guest_proxy.accepted(),
        3,
        "the meta read, the session, and one reconnect attempt refused `x.room_full`"
    );
    Ok(())
}

/// A fast backoff for a test that counts attempts: 50 ms each, so a budget is a small,
/// known number of connections rather than a wall-clock wait.
fn fast_policy() -> ReconnectPolicy {
    ReconnectPolicy {
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(50),
        ..ReconnectPolicy::default()
    }
}

/// Takes the server away from a client behind `proxy`: the accept loop is aborted and
/// every relayed connection cut, so a retry is refused rather than seated and what the
/// relay counts is the retry budget itself.
///
/// Waiting for the port to refuse is part of it. A retry that landed in the window
/// before the accept loop died would seat, reset the attempts, and leave the count
/// below measuring the race rather than the budget.
async fn server_goes_away(
    harness: &Harness,
    proxy: &DropProxy,
) -> Result<(), Failure> {
    harness.abort();
    let upstream = harness.upstream();
    wait_for("the server's port to refuse connections", || {
        let target = upstream.clone();
        async move { TcpStream::connect(&target).await.err().map(|_error| ()) }
    })
    .await;
    proxy.drop_all();
    Ok(())
}

/// Waits for a session to end, from a subscription taken before whatever ends it: a
/// receiver made afterwards would miss the event it exists to see.
async fn wait_for_giving_up(
    events: &mut broadcast::Receiver<EngineEvent>,
) -> Result<(), Failure> {
    let start = Instant::now();
    let deadline = WAIT.saturating_add(WAIT);
    loop {
        if start.elapsed() >= deadline {
            return Err(
                format!("the session did not end within {deadline:?}").into()
            );
        }
        match timeout(Duration::from_millis(50), events.recv()).await {
            Ok(Ok(EngineEvent::Disconnected)) => return Ok(()),
            Ok(Err(_)) => {
                return Err(
                    "the engine's stream closed before it gave up".into()
                );
            }
            Ok(Ok(_)) | Err(_) => {}
        }
    }
}

/// The grace a server advertises is what a reconnect has to span (`PROTOCOL.md` §9.1), and
/// the five attempts a bare policy makes would give up inside a thirty-second window.
/// `/meta` is where a host reads that number: its own connection is the one that drops,
/// and a `host.detached` reaches only the peers it left behind.
///
/// Ten attempts at 50 ms span the 500 ms advertised here, and the relay counts them.
#[tokio::test]
async fn a_host_sizes_its_retry_budget_from_the_advertised_grace()
-> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig {
        room_grace: Duration::from_millis(500),
        ..ServerConfig::default()
    })
    .await;
    let proxy = DropProxy::start(&harness.upstream()).await?;
    let host = SyncEngine::connect(
        ConnectOptions::host(proxy.ws_base(), "Ada")
            .with_reconnect(fast_policy()),
    )
    .await?;
    assert_eq!(
        proxy.accepted(),
        2,
        "the meta read and the session itself are relayed"
    );

    let mut events = host.subscribe();
    server_goes_away(&harness, &proxy).await?;
    wait_for_giving_up(&mut events).await?;
    assert_eq!(
        proxy.accepted(),
        12,
        "the meta read, the session, and the ten retries that span the advertised grace"
    );
    Ok(())
}

/// A room that advertises no grace leaves the policy its own attempts: there is no window
/// to span, so a client gives up exactly where it did before a grace was read at all.
#[tokio::test]
async fn a_room_without_a_grace_leaves_the_policys_own_attempts()
-> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig {
        room_grace: Duration::ZERO,
        ..ServerConfig::default()
    })
    .await;
    let proxy = DropProxy::start(&harness.upstream()).await?;
    let host = SyncEngine::connect(
        ConnectOptions::host(proxy.ws_base(), "Ada")
            .with_reconnect(fast_policy()),
    )
    .await?;

    let mut events = host.subscribe();
    server_goes_away(&harness, &proxy).await?;
    wait_for_giving_up(&mut events).await?;
    assert_eq!(
        proxy.accepted(),
        7,
        "the meta read, the session, and the policy's own five attempts"
    );
    Ok(())
}

/// A caller that named its own number keeps it, whatever the room advertises: the grace
/// sizes a budget the caller left open, and this one was not — the read that would have
/// found it is not made either, so the relay sees the session and the two attempts alone.
#[tokio::test]
async fn a_callers_own_attempts_win_over_the_advertised_grace()
-> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig {
        room_grace: Duration::from_millis(500),
        ..ServerConfig::default()
    })
    .await;
    let proxy = DropProxy::start(&harness.upstream()).await?;
    let policy = ReconnectPolicy {
        max_attempts: Some(2),
        ..fast_policy()
    };
    let host = SyncEngine::connect(
        ConnectOptions::host(proxy.ws_base(), "Ada").with_reconnect(policy),
    )
    .await?;

    let mut events = host.subscribe();
    server_goes_away(&harness, &proxy).await?;
    wait_for_giving_up(&mut events).await?;
    assert_eq!(
        proxy.accepted(),
        3,
        "the session and the caller's own two attempts, with no meta read at all"
    );
    Ok(())
}
