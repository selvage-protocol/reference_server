//! The `selvage/2` relay against the real `selvaged`, run with `--serve-version-2`: two relays,
//! one room, a host and a guest, exchanging an edit through a server that never sees a file name
//! or a byte of either replica.
//!
//! This is the proof that the version's two halves (`PROTOCOL.md` §7.1 and §13) can be handed a
//! socket and a room and come out the other side agreeing, which is the wiring the Rust client
//! was written without. Every wait here polls a real predicate with a deadline: a test that slept
//! and hoped would pass on a machine that happened to be fast enough and prove nothing.

use std::error::Error as StdError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use selvage_client::relay::{RelayEnding, RelayHostOptions, RelayJoinOptions, RelaySession};
use selvage_client::host::{HostStore, PersistedHost};
use selvage_client::sealed::{RoomKey, SessionKey, encode_key};
use selvage_client::session::KeepaliveConfig;
use selvage_harness::{Harness, ServerConfig, wait_for, wait_for_described};

/// Anything this test can fail with.
type Failure = Box<dyn StdError>;

const PATH: &str = "notes.txt";
const OTHER: &str = "src/main.rs";
const SEED: &str = "a room two relays share\n";

/// A transitional server: it seats `selvage/2` as well as `selvage/1`.
fn transitional() -> ServerConfig {
    ServerConfig {
        serve_version_2: true,
        ..ServerConfig::default()
    }
}

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
async fn a_version_two_host_mints_and_a_guest_joins_by_the_wire_link() -> Result<(), Failure> {
    let server = Harness::start_with(transitional()).await;
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
    let address = invite
        .split_once('#')
        .map(|(before, _)| before.to_string());
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
    assert_eq!(guest.applied_role().as_deref(), Some("guest"));

    // §13.4: the guest's seat reaches the host as a `peer.joined`, and the role it is drawn
    // with is the one the applied state gives that seat rather than anything the event claimed.
    let seat = guest.session_info().seat;
    let seen = wait_for_described(
        "the host to see the guest in the room",
        || async { format!("{:?}", host.peers()) },
        || async {
            host.peers()
                .into_iter()
                .find(|peer| peer.peer_id == seat)
        },
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
    let server = Harness::start_with(transitional()).await;
    let listing = Listing::of(&[PATH]);
    let host = host_of(&server, &listing, None).await?;
    let wire = host.invite().ok_or("the host is handed a link to send")?;
    let page = wire
        .replace("ws://", "http://")
        .replace("/session?", "/?");
    assert!(page.contains("#k="), "the fragment survives the form change");

    let guest = guest_of(&page, "Bob").await?;
    let applied = wait_for("the guest joining by page link to apply the state", || async {
        listing_of(&guest)
    })
    .await;
    assert_eq!(applied, [PATH]);
    assert_eq!(host.session_info().room_id, guest.session_info().room_id);

    host.disconnect();
    guest.disconnect();
    Ok(())
}

/// An edit crosses a real server in both directions, and a hold crosses with it.
#[tokio::test]
async fn an_edit_crosses_a_real_server_in_both_directions() -> Result<(), Failure> {
    let server = Harness::start_with(transitional()).await;
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
    let back_at_host = wait_for("the guest's edit to reach the host", || async {
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
    let server = Harness::start_with(transitional()).await;
    let listing = Listing::of(&[PATH]);
    let store = Arc::new(Store::default());
    let host = host_of(&server, &listing, Some(Arc::clone(&store))).await?;

    let minted = {
        let saved = store.load().ok_or("the mint state is saved with the key that signed it")?;
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
    let server = Harness::start_with(transitional()).await;
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
    let ended = wait_for("the guest to hear the closing", || async {
        guest.ending()
    })
    .await;
    assert_eq!(ended, RelayEnding::Closing);
    assert_eq!(guest.ending_sentence(), Some("the room closed"));
    assert_eq!(host.ending(), Some(RelayEnding::Closing));

    host.disconnect();
    guest.disconnect();
    Ok(())
}

/// A link with no fragment carries neither of §5.1's two keys, and a join is refused before a
/// socket exists rather than handed a room it cannot read.
#[tokio::test]
async fn a_fragment_less_link_is_refused_before_a_socket_is_opened() -> Result<(), Failure> {
    let server = Harness::start_with(transitional()).await;
    let joined = RelaySession::join(RelayJoinOptions {
        invite: format!("{}/session?room=r-1&token=t-1", server.ws_base()),
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
    Ok(())
}

/// A `wss://` base is one this build cannot reach, and the dial says so by name rather than
/// failing somewhere inside a handshake.
#[tokio::test]
async fn a_tls_base_is_refused_by_name() -> Result<(), Failure> {
    let server = Harness::start_with(transitional()).await;
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
async fn a_host_handed_a_room_key_and_a_host_seed_carries_them_in_its_invite() -> Result<(), Failure> {
    let server = Harness::start_with(transitional()).await;
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
        invite.ends_with(&format!("#k={}&h={host_key}", encode_key(&room_key.0))),
        "§5.1's fragment, in the order the version writes it: {invite}"
    );

    host.disconnect();
    Ok(())
}
