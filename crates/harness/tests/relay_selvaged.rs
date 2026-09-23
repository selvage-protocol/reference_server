//! The `selvage/2` relay against a real `selvaged` on its defaults, which seat both versions:
//! two relays, one room, a host and a guest, exchanging an edit through a server that never
//! sees a file name or a byte of either replica.
//!
//! This is the proof that the version's two halves (`PROTOCOL.md` §7.1 and §13) can be handed a
//! socket and a room and come out the other side agreeing, which is the wiring the Rust client
//! was written without. Every wait here polls a real predicate with a deadline: a test that slept
//! and hoped would pass on a machine that happened to be fast enough and prove nothing.

use std::error::Error as StdError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

use selvage_client::host::{HostStore, PersistedHost};
use selvage_client::relay::{
    RelayEnding, RelayHostOptions, RelayJoinOptions, RelaySession,
};
use selvage_client::sealed::{RoomKey, SessionKey, encode_key};
use selvage_client::session::KeepaliveConfig;
use selvage_harness::{
    Harness, ServerConfig, wait_for, wait_for_described,
    wait_for_described_within,
};

/// One `GET /meta` body as a server that seats no version-2 client advertises one
/// (`PROTOCOL.md` §2): the capability names the published `selvage/1` server writes, with
/// `wire_versions` naming the one version it can seat. `roles` and `keepalive` are the members
/// this fixture leaves out, and both are optional.
const VERSION_ONE_META: &[u8] = br#"{"capabilities":["y-protocols/1","awareness","open-document-set","host-reclaim"],"server":"selvaged/0.2.1","wire_versions":["selvage/1"]}"#;

/// The request line of one connection, which is all this end needs to tell a `/meta` read from
/// a WebSocket handshake — both begin with `GET`. The whole request is consumed, because a
/// socket closed with bytes still unread in it is reset rather than ended, and the client
/// reading the answer would see the reset instead of the body.
async fn request_line(socket: &mut TcpStream) -> Option<String> {
    let mut request: Vec<u8> = Vec::new();
    let mut whole = false;
    while !whole {
        whole = read_a_chunk(socket, &mut request).await?;
    }
    let headers =
        request.get(..header_end(&request).unwrap_or(request.len()))?;
    String::from_utf8(headers.get(..line_end(headers)?)?.to_vec()).ok()
}

/// One chunk of a request, appended, and whether the headers are now whole.
async fn read_a_chunk(
    socket: &mut TcpStream,
    request: &mut Vec<u8>,
) -> Option<bool> {
    let mut chunk = [0u8; 64];
    let read = timeout(READ_BOUND, socket.read(&mut chunk))
        .await
        .ok()?
        .ok()?;
    request.extend_from_slice(chunk.get(..read)?);
    Some(read == 0 || header_end(request).is_some())
}

/// Where one request's headers end, or `None` while they have not arrived whole.
fn header_end(request: &[u8]) -> Option<usize> {
    request.windows(4).position(|four| four == b"\r\n\r\n")
}

/// Where the first line of one request ends, or `None` while it has not arrived whole.
fn line_end(head: &[u8]) -> Option<usize> {
    head.windows(2).position(|pair| pair == b"\r\n")
}

/// Serves [`VERSION_ONE_META`] to one `GET /meta`, as a server that cannot seat this client
/// does, and closes.
async fn serve_the_version_one_meta(
    socket: &mut TcpStream,
) -> Result<(), Failure> {
    let head = format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        VERSION_ONE_META.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(VERSION_ONE_META).await?;
    Ok(())
}

/// Answers every `/meta` read, and reports the first connection that is not one: a client that
/// dialled a server which cannot seat it, which is the failure this fixture measures.
async fn serve_until_a_dial(
    listener: TcpListener,
    heard: mpsc::UnboundedSender<()>,
) {
    while let Ok((mut tcp, _)) = listener.accept().await {
        let meta = request_line(&mut tcp)
            .await
            .is_some_and(|line| line.starts_with("GET /meta"));
        if !meta {
            let _ = heard.send(());
            return;
        }
        if serve_the_version_one_meta(&mut tcp).await.is_err() {
            return;
        }
    }
}

/// A bound on one read of the fake server, so a client that says nothing cannot hold the test.
const READ_BOUND: Duration = Duration::from_secs(5);

/// Anything this test can fail with.
type Failure = Box<dyn StdError>;

const PATH: &str = "notes.txt";
const OTHER: &str = "src/main.rs";
const SEED: &str = "a room two relays share\n";

/// The guest's own windows are the client keepalive's: §13.8's host-away ending arrives
/// `awareness_expire` after the host's seat left. A wait that means to report which ending it
/// was has to outlast it rather than time out on it.
const HOST_AWAY_WINDOW: Duration = Duration::from_secs(15);

/// A shorter renewal interval than the server advertises, so the session's own clocks run
/// during a test: §7.1's answer to an announcement is bounded by that window, and §13.7's holds
/// are published on the tick that changes them.
const fn keepalive() -> KeepaliveConfig {
    KeepaliveConfig {
        awareness_renew: Duration::from_millis(50),
        awareness_expire: Duration::from_secs(5),
    }
}

/// Where a host keeps the two values §7.1 has it keep: the host key and its `issued` beside it.
///
/// The adapter's is VS Code's state; here it is a box that records the writes, so a test can
/// read the series a host published.
#[derive(Default)]
struct Store {
    saved: Mutex<Option<PersistedHost>>,
}

impl HostStore for Store {
    fn load(&self) -> Option<PersistedHost> {
        match self.saved.lock() {
            Ok(saved) => *saved,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    fn save(&self, persisted: PersistedHost) {
        let mut saved = match self.saved.lock() {
            Ok(saved) => saved,
            Err(poisoned) => poisoned.into_inner(),
        };
        *saved = Some(persisted);
    }
}

/// The listing a host publishes and the way a test changes it, which is one place: a state is
/// sealed from the listing, so a change and the state that follows are one step.
#[derive(Clone, Default)]
struct Listing(Arc<Mutex<Vec<String>>>);

impl Listing {
    fn of(paths: &[&str]) -> Self {
        Self(Arc::new(Mutex::new(
            paths.iter().map(|path| (*path).to_string()).collect(),
        )))
    }

    fn source(&self) -> Arc<dyn Fn() -> Vec<String> + Send + Sync> {
        let held = Arc::clone(&self.0);
        Arc::new(move || match held.lock() {
            Ok(paths) => paths.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        })
    }

    fn add(&self, path: &str) {
        let mut paths = match self.0.lock() {
            Ok(paths) => paths,
            Err(poisoned) => poisoned.into_inner(),
        };
        paths.push(path.to_string());
    }
}

/// A host that mints a room over the real server, with the store a test can read.
async fn host_of(
    server: &Harness,
    listing: &Listing,
    store: Option<Arc<Store>>,
) -> Result<RelaySession, Failure> {
    let kept: Option<Arc<dyn HostStore>> =
        store.map(|kept| Arc::clone(&kept) as Arc<dyn HostStore>);
    Ok(RelaySession::host(RelayHostOptions {
        base_url: server.ws_base(),
        display_name: "Ada".to_string(),
        listing: listing.source(),
        room_key: None,
        host_seed: None,
        store: kept,
        client: Some("selvage-harness/relay-test".to_string()),
        keepalive: Some(keepalive()),
    })
    .await?)
}

/// A guest joining the link a host handed on.
async fn guest_of(invite: &str, name: &str) -> Result<RelaySession, Failure> {
    Ok(RelaySession::join(RelayJoinOptions {
        invite: invite.to_string(),
        display_name: name.to_string(),
        declared_role: None,
        client: None,
        keepalive: Some(keepalive()),
    })
    .await?)
}

/// The listing a session has applied, or `false` while it holds none.
fn listing_of(session: &RelaySession) -> Option<Vec<String>> {
    let listing = session.listing();
    (!listing.is_empty()).then_some(listing)
}

/// A host mints, and a guest joining the wire link applies the state it publishes.
#[tokio::test]
async fn a_version_two_host_mints_and_a_guest_joins_by_the_wire_link()
-> Result<(), Failure> {
    let server = Harness::start_with(ServerConfig::default()).await;
    let listing = Listing::of(&[PATH, OTHER]);
    let host = host_of(&server, &listing, None).await?;
    // §7.1's mint state is folded into the host's own receiver, so the listing it published is
    // the listing it holds.
    assert_eq!(host.listing(), [PATH, OTHER]);
    let invite = host.invite().ok_or("the host is handed a link to send")?;
    // §5.1: the fragment is the two keys, in that order, and the address has none of it.
    assert!(invite.starts_with(&format!("{}/session?room=", server.ws_base())));
    assert!(invite.contains("#k="));
    assert!(invite.contains("&h="));
    assert_eq!(invite.matches('#').count(), 1);
    let address = invite.split_once('#').map(|(before, _)| before.to_string());
    assert!(
        address.is_some_and(|wire| !wire.contains('#')),
        "the socket URL carries no fragment: {invite}"
    );

    let guest = guest_of(&invite, "Bob").await?;
    let applied = wait_for("the guest to apply the host's state", || async {
        listing_of(&guest)
    })
    .await;
    assert_eq!(applied, [PATH, OTHER]);
    assert_eq!(host.session_info().room_id, guest.session_info().room_id);
    assert!(host.is_host());
    assert!(!guest.is_host());
    // The listing and this connection's role do not necessarily arrive in one state: a host
    // publishes its own state on its clock too, and one that does not yet commit this guest
    // carries the listing and no seat for it. The role is what the state §7.1 owes
    // `peer.joined` gives, so it is waited for rather than read off the listing's arrival.
    let role = wait_for_described(
        "the guest to learn its role",
        || async { format!("{:?}", guest.applied_role()) },
        || async { guest.applied_role() },
    )
    .await;
    assert_eq!(role, "guest");

    // §13.4: the guest's seat reaches the host as a `peer.joined`, and the role it is drawn
    // with is the one the applied state gives that seat rather than anything the event claimed.
    let seat = guest.session_info().seat;
    let seen = wait_for_described(
        "the host to see the guest in the room",
        || async { format!("{:?}", host.peers()) },
        || async { host.peers().into_iter().find(|peer| peer.peer_id == seat) },
    )
    .await;
    assert_eq!(seen.display_name, "Bob");
    assert_eq!(seen.awareness_client_id, Some(guest.awareness_client_id()));
    assert_eq!(
        host.roles_by_seat().get(&seat).map(String::as_str),
        Some("guest")
    );

    host.disconnect();
    guest.disconnect();
    Ok(())
}

/// The page form of §5.1's invite joins the same room: the link a browser can open, whose
/// fragment must survive the reading back to the connection URL the relay dials.
#[tokio::test]
async fn a_version_two_guest_joins_by_the_page_link() -> Result<(), Failure> {
    let server = Harness::start_with(ServerConfig::default()).await;
    let listing = Listing::of(&[PATH]);
    let host = host_of(&server, &listing, None).await?;
    let wire = host.invite().ok_or("the host is handed a link to send")?;
    let page = wire.replace("ws://", "http://").replace("/session?", "/?");
    assert!(
        page.contains("#k="),
        "the fragment survives the form change"
    );

    let guest = guest_of(&page, "Bob").await?;
    let applied = wait_for(
        "the guest joining by page link to apply the state",
        || async { listing_of(&guest) },
    )
    .await;
    assert_eq!(applied, [PATH]);
    assert_eq!(host.session_info().room_id, guest.session_info().room_id);

    host.disconnect();
    guest.disconnect();
    Ok(())
}

/// An edit crosses a real server in both directions, and a hold crosses with it.
#[tokio::test]
async fn an_edit_crosses_a_real_server_in_both_directions()
-> Result<(), Failure> {
    let server = Harness::start_with(ServerConfig::default()).await;
    let listing = Listing::of(&[PATH]);
    let host = host_of(&server, &listing, None).await?;
    let invite = host.invite().ok_or("the host is handed a link")?;
    let guest = guest_of(&invite, "Bob").await?;
    let _ = wait_for("the guest to apply the host's state", || async {
        listing_of(&guest)
    })
    .await;

    assert!(host.open(PATH).is_ok());
    assert!(guest.open(PATH).is_ok());

    // §13.7: the hold a joiner takes is the room's, and the host sees the guest held to it.
    let held = wait_for("the guest's hold to reach the host", || async {
        host.peer_holds()
            .into_values()
            .find(|paths| paths.iter().any(|path| path == PATH))
    })
    .await;
    assert!(held.iter().any(|path| path == PATH));

    let seeded = host.insert(PATH, 0, SEED)?;
    assert!(seeded, "the host publishes its own edit");
    let arrived = wait_for("the seeded text to reach the guest", || async {
        let text = guest.text(PATH);
        (text == SEED).then_some(text)
    })
    .await;
    assert_eq!(arrived, SEED);

    // The guest only publishes once a state commits its key, which the host's answer to its
    // announcement is (§13.1's step 4). The wait is on that read, not on the edit: an insert
    // applies to the replica whether or not it may publish, so polling one would edit the text
    // once per attempt.
    let _ = wait_for("the guest to be named by the state", || async {
        guest.applied_role()
    })
    .await;
    assert!(
        guest.insert(PATH, 0, "guest: ")?,
        "the room accepted the guest's edit"
    );
    let back_at_host =
        wait_for("the guest's edit to reach the host", || async {
            let text = host.text(PATH);
            (text == format!("guest: {SEED}")).then_some(text)
        })
        .await;
    assert_eq!(back_at_host, format!("guest: {SEED}"));

    host.disconnect();
    guest.disconnect();
    Ok(())
}

/// §7.1's store: the host key and the `issued` it published, which is what a reload continues.
#[tokio::test]
async fn the_host_store_carries_the_issued_series() -> Result<(), Failure> {
    let server = Harness::start_with(ServerConfig::default()).await;
    let listing = Listing::of(&[PATH]);
    let store = Arc::new(Store::default());
    let host = host_of(&server, &listing, Some(Arc::clone(&store))).await?;

    let minted = {
        let saved = store
            .load()
            .ok_or("the mint state is saved with the key that signed it")?;
        assert_eq!(saved.host_seed.len(), 32);
        assert!(saved.issued >= 1, "§7.1's first state carries `1`");
        saved.issued
    };
    assert_eq!(host.published_issued(), minted);

    listing.add(OTHER);
    host.listing_changed()?;
    let after = wait_for_described(
        "a new edition to be saved",
        || async { format!("the store carries {:?}", store.load()) },
        || async {
            store
                .load()
                .filter(|saved| saved.issued > minted)
                .map(|saved| saved.issued)
        },
    )
    .await;
    assert!(after > minted, "a new edition moves the saved series");
    assert_eq!(after, host.published_issued());

    host.disconnect();
    Ok(())
}

/// §7.1's closing: the host says the room is over, and the guest that holds a state below it
/// ends where §13.10 says.
#[tokio::test]
async fn the_hosts_closing_ends_the_guest() -> Result<(), Failure> {
    let server = Harness::start_with(ServerConfig::default()).await;
    let listing = Listing::of(&[PATH]);
    let host = host_of(&server, &listing, None).await?;
    let invite = host.invite().ok_or("the host is handed a link")?;
    let guest = guest_of(&invite, "Bob").await?;
    // The guest holds a verified state, so the closing applies rather than being ignored.
    let _ = wait_for("the guest to hold the listing", || async {
        listing_of(&guest)
    })
    .await;

    assert!(host.close_room()?, "the host publishes a closing");
    let ended =
        wait_for("the guest to hear the closing", || async { guest.ending() })
            .await;
    assert_eq!(ended, RelayEnding::Closing);
    assert_eq!(guest.ending_sentence(), Some("the room closed"));
    assert_eq!(host.ending(), Some(RelayEnding::Closing));

    host.disconnect();
    guest.disconnect();
    Ok(())
}

/// §7.1's closing, with the disconnect in the same breath: the closing frame is what the socket
/// task has queued when the caller ends the session, and the guest still ends `Closing`. Losing
/// it leaves the guest to §13.8's host-away window, which is a different ending and a slower
/// one.
#[tokio::test]
async fn a_host_that_closes_and_disconnects_at_once_still_ends_the_guest()
-> Result<(), Failure> {
    let server = Harness::start_with(ServerConfig::default()).await;
    let listing = Listing::of(&[PATH]);
    let host = host_of(&server, &listing, None).await?;
    let invite = host.invite().ok_or("the host is handed a link")?;
    let guest = guest_of(&invite, "Bob").await?;
    // The guest holds a verified state, so the closing applies rather than being ignored.
    let _ = wait_for("the guest to hold the listing", || async {
        listing_of(&guest)
    })
    .await;

    assert!(host.close_room()?, "the host publishes a closing");
    host.disconnect();

    let ended = wait_for_described_within(
        HOST_AWAY_WINDOW,
        "the guest to hear the closing",
        || async { format!("{:?}", guest.ending()) },
        || async { guest.ending() },
    )
    .await;
    assert_eq!(ended, RelayEnding::Closing);
    assert_eq!(guest.ending_sentence(), Some("the room closed"));

    guest.disconnect();
    Ok(())
}

/// A link with no fragment carries neither of §5.1's two keys, and a join is refused before a
/// socket exists — measured, not inferred from the refusal's wording: the address the link
/// names is a listener this test owns, and it fails the test if any connection arrives.
#[tokio::test]
async fn a_fragment_less_link_is_refused_before_a_socket_is_opened()
-> Result<(), Failure> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (heard, mut arrival) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let _ = heard.send(listener.accept().await.is_ok());
    });

    let joined = RelaySession::join(RelayJoinOptions {
        invite: format!("ws://{addr}/session?room=r-1&token=t-1"),
        display_name: "Bob".to_string(),
        declared_role: None,
        client: None,
        keepalive: Some(keepalive()),
    })
    .await;
    let error = joined.err().ok_or("a link with no fragment is refused")?;
    assert!(
        error.to_string().contains("fragment"),
        "the refusal asks for the whole link: {error}"
    );
    assert!(
        !matches!(
            timeout(Duration::from_millis(250), arrival.recv()).await,
            Ok(Some(true))
        ),
        "a socket was opened for a link that names no keys"
    );
    Ok(())
}

/// §2's local stop, measured rather than inferred from the refusal's wording. The address the
/// link names is a listener this test owns: it answers `GET /meta` the way a server that seats
/// no version-2 client does, and it reports a connection that arrives after that read. A client
/// that dialled anyway would be seated or refused by the wire; this one must not dial at all.
#[tokio::test]
async fn a_server_that_speaks_only_the_other_version_is_refused_before_a_socket()
-> Result<(), Failure> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (heard, mut arrival) = mpsc::unbounded_channel();
    tokio::spawn(serve_until_a_dial(listener, heard));

    let keys = (
        encode_key(&RoomKey([7; 32]).0),
        SessionKey::from_seed([3; 32]).public().encode(),
    );
    let joined = RelaySession::join(RelayJoinOptions {
        invite: format!(
            "ws://{addr}/session?room=r-1&token=t-1#k={}&h={}",
            keys.0, keys.1
        ),
        display_name: "Bob".to_string(),
        declared_role: None,
        client: None,
        keepalive: Some(keepalive()),
    })
    .await;
    let error = joined
        .err()
        .ok_or("a server that advertises no version-2 is refused")?;
    assert!(
        error.to_string().contains("selvage/2"),
        "the refusal names the version this client needs: {error}"
    );
    assert!(
        error.to_string().contains(&addr.to_string()),
        "and the server it would need it from: {error}"
    );
    assert!(
        matches!(
            &error,
            selvage_client::Error::Protocol { code, .. }
                if code == "unsupported_version"
        ),
        "§10's local stop is an `unsupported_version` refusal: {error}"
    );
    assert!(
        !matches!(
            timeout(Duration::from_millis(250), arrival.recv()).await,
            Ok(Some(()))
        ),
        "a socket was opened for a server that cannot seat this client"
    );
    Ok(())
}

/// A `wss://` base is one this build cannot reach, and the dial says so by name rather than
/// failing somewhere inside a handshake.
#[tokio::test]
async fn a_tls_base_is_refused_by_name() -> Result<(), Failure> {
    let server = Harness::start_with(ServerConfig::default()).await;
    let listing = Listing::of(&[PATH]);
    let minted = RelaySession::host(RelayHostOptions {
        base_url: server.ws_base().replace("ws://", "wss://"),
        display_name: "Ada".to_string(),
        listing: listing.source(),
        room_key: None,
        host_seed: None,
        store: None,
        client: None,
        keepalive: Some(keepalive()),
    })
    .await;
    let error = minted.err().ok_or("a wss:// base is refused")?;
    assert!(
        error.to_string().contains("TLS"),
        "the refusal names what is missing: {error}"
    );
    Ok(())
}

/// A host handed §5.1's two values builds the same fragment from them, which is what a host
/// that means to keep hosting after a reload has to hand back.
#[tokio::test]
async fn a_host_handed_a_room_key_and_a_host_seed_carries_them_in_its_invite()
-> Result<(), Failure> {
    let server = Harness::start_with(ServerConfig::default()).await;
    let listing = Listing::of(&[PATH]);
    let room_key = RoomKey([7; 32]);
    let host_seed = [3; 32];
    let host = RelaySession::host(RelayHostOptions {
        base_url: server.ws_base(),
        display_name: "Ada".to_string(),
        listing: listing.source(),
        room_key: Some(room_key),
        host_seed: Some(host_seed),
        store: None,
        client: None,
        keepalive: Some(keepalive()),
    })
    .await?;
    let invite = host.invite().ok_or("the host is handed a link")?;
    let host_key = SessionKey::from_seed(host_seed).public().encode();
    assert!(
        invite
            .ends_with(&format!("#k={}&h={host_key}", encode_key(&room_key.0))),
        "§5.1's fragment, in the order the version writes it: {invite}"
    );

    host.disconnect();
    Ok(())
}
