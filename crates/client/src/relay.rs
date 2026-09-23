//! A `selvage/2` session over a socket: the wiring [`PeerSession`] (`PROTOCOL.md` §13) and the
//! host's producer half (§7.1) were written to be handed.
//!
//! [`PeerSession`] decides; this module moves the bytes. It opens the WebSocket, says
//! `session.hello` at `selvage/2`, seats the connection from `room.created`/`room.joined`,
//! hands every binary frame to the session and every frame the session produced to the socket,
//! and runs the session's clocks on a timer of its own (§13.8) — a session renews nothing if
//! only the caller moves it.
//!
//! **What it is not.** It is the relay and nothing above it: it says nothing to an editor. It
//! is also not the version-1 engine ([`crate::SyncEngine`]), which stays where it is; a caller
//! that holds a `selvage/2` invite joins with [`RelaySession::join`] instead.
//!
//! **No TLS.** This workspace builds `tokio-tungstenite` with no TLS feature, so a `wss://`
//! base cannot be reached from here at all: the dial refuses one by name rather than failing
//! somewhere inside the handshake. `ws://` is what this client speaks, and it says so where it
//! bites.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;

use selvage_protocol as proto;
use selvage_protocol::{event, method};

use crate::Error;
use crate::host::{HostOptions, HostStore, ListingSource};
use crate::peer::{Ending, Outcome, PeerInvite, PeerOptions, PeerSession};
use crate::sealed::{
    RoomKey, SealedError, SessionKey, encode_key, fresh_nonce,
};
use crate::session::KeepaliveConfig;

type Socket = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = SplitSink<Socket, Message>;
type Stream = SplitStream<Socket>;

/// The version this module speaks. `selvage/1` is [`crate::SyncEngine`]'s and stays there.
pub const WIRE_VERSION_V2: &str = proto::WIRE_VERSION_V2;

/// How long the upgrade and the handshake may take together before the connection is abandoned.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many events a subscriber that has not read yet is kept.
const EVENT_BACKLOG: usize = 64;

/// A peer as `selvage/2` records it: `PROTOCOL.md` §6.1's `PeerInfo` without `role`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayPeer {
    pub peer_id: String,
    pub display_name: String,
    pub awareness_client_id: Option<u64>,
}

/// What the server said at the end of the `selvage/2` handshake.
#[derive(Debug, Clone)]
pub struct RelaySessionInfo {
    pub room_id: String,
    /// Present only for the connection that minted the room, and never echoed after.
    pub token: Option<String>,
    /// This connection's seat: the `peer_id` the relay showed it under.
    pub seat: String,
    /// The seats already in the room, without this connection.
    pub peers: Vec<RelayPeer>,
    pub capabilities: Vec<String>,
    pub keepalive: KeepaliveConfig,
    /// The server base this connection dialled, without the endpoint path.
    pub base_url: String,
}

/// Why a `selvage/2` session is over. §13.10's three are [`Ending`]'s own; `room-gone` is the
/// relay's, which no peer can sign because the room is not a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayEnding {
    Closing,
    HostAway,
    NoState,
    RoomGone,
}

impl RelayEnding {
    /// The sentence §13.10 asks a client to say, or `None` for the relay's own ending.
    #[must_use]
    pub const fn sentence(self) -> Option<&'static str> {
        match self {
            Self::Closing => Some(Ending::Closing.as_str()),
            Self::HostAway => Some(Ending::HostAway.as_str()),
            Self::NoState => Some(Ending::NoState.as_str()),
            Self::RoomGone => None,
        }
    }
}

/// What a relay reports after it moved something.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayEvent {
    /// The handshake seated this connection.
    Seated,
    /// The roster changed.
    Peers(Vec<RelayPeer>),
    /// The listing of an applied state changed.
    Listing(Vec<String>),
    /// The paths this replica holds text for changed.
    Content(Vec<String>),
    /// A content frame was applied: the replica's text for some path is not what it was.
    Text,
    /// The session ended, by §13.10's rule or the relay's.
    Ended(RelayEnding),
    /// The server reported a fault.
    Failed(String),
}

/// Minting a room (`PROTOCOL.md` §5.1, §7.1).
pub struct RelayHostOptions {
    /// The server base: scheme and authority, without the `/session` path.
    pub base_url: String,
    pub display_name: String,
    /// The room's working tree as this host enumerates it: names, and no content (§7.1).
    pub listing: ListingSource,
    /// The room key, persisted with the host key so a returning host re-derives the same frame
    /// key.
    pub room_key: Option<RoomKey>,
    /// The host key's 32-byte seed, persisted so a returning host can still sign a state.
    pub host_seed: Option<[u8; 32]>,
    /// Where the host key and its `issued` are kept (§7.1); `None` is an in-memory host.
    pub store: Option<Arc<dyn HostStore>>,
    /// Free-form client identifier, for diagnostics (`PROTOCOL.md` §5).
    pub client: Option<String>,
    /// Overrides the clocks the server advertises (`§8.2`). The server's numbers are the room's.
    pub keepalive: Option<KeepaliveConfig>,
}

/// Joining the room a link names (`PROTOCOL.md` §5.1, §13.1).
pub struct RelayJoinOptions {
    /// The invite link, either form, with its fragment: the wire URL or the page link.
    pub invite: String,
    pub display_name: String,
    /// The role this connection declares in its announcement; the state is what assigns it
    /// (`§7.1`). `guest` or `viewer`, never `host`.
    pub declared_role: Option<String>,
    /// Free-form client identifier, for diagnostics (`PROTOCOL.md` §5).
    pub client: Option<String>,
    /// Overrides the clocks the server advertises (`§8.2`). The server's numbers are the room's.
    pub keepalive: Option<KeepaliveConfig>,
}

/// What one change to a session produced: its value, the events the change is reported as, and
/// whether the socket's queue took every frame the change published.
struct Moved<T> {
    value: T,
    events: Vec<RelayEvent>,
    queued: bool,
}

/// One frame on its way to the socket, or the socket's own end.
enum Outbound {
    Text(String),
    Binary(Vec<u8>),
    Close,
}

/// Everything one connection touches, behind one lock.
struct RelayState {
    session: PeerSession,
    info: RelaySessionInfo,
    /// The monotone clock §13.8 reads: elapsed since this connection's seat.
    start: Instant,
    peers: Vec<RelayPeer>,
    invite: Option<String>,
    ending: Option<RelayEnding>,
    fault: Option<String>,
    /// Set when the caller asked for the disconnect, so the close that follows is not the
    /// room's.
    destroyed: bool,
    request_id: u64,
    last_listing: Vec<String>,
    last_documents: Vec<String>,
    last_peers: Vec<RelayPeer>,
}

/// A connection: the session and what a caller reads from it, and the socket underneath.
struct Relay {
    state: Mutex<RelayState>,
    events: broadcast::Sender<RelayEvent>,
    outgoing: mpsc::UnboundedSender<Outbound>,
}

/// One `selvage/2` connection: a socket, a [`PeerSession`] and the clocks between them.
///
/// Every mutation takes the same lock an inbound frame and the clock take, so a frame's arrival
/// and a local edit are decided one at a time in the order they were let in — and the frames
/// each of them produced leave in the order they were produced. Two tasks move the bytes: one
/// owns the socket, and one runs the session's clocks on a timer of its own (§13.8).
///
/// A relay that is dropped without [`RelaySession::disconnect`] leaves its tasks running, the
/// way a timer left behind does anywhere else: ending a session is the caller's to say.
pub struct RelaySession {
    relay: Arc<Relay>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl RelaySession {
    /// Mints a room; this connection is its host by holding the host key's private half.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the socket cannot be opened, the server refuses the session, the
    /// handshake does not finish inside [`HANDSHAKE_TIMEOUT`], or §7.1's first state cannot be
    /// sealed from the invite's two keys.
    pub async fn host(options: RelayHostOptions) -> Result<Self, Error> {
        let base = session_base(&options.base_url).ok_or_else(|| {
            Error::Invite(format!(
                "not a session address: {}",
                link_address(&options.base_url)
            ))
        })?;
        let room_key = match options.room_key {
            Some(key) => key,
            None => random_room_key()?,
        };
        let host_seed = match options.host_seed {
            Some(seed) => seed,
            None => random_seed()?,
        };
        let host_key = SessionKey::from_seed(host_seed).public();
        let awareness_client_id = mint_awareness_client_id()?;
        let url = proto::session_url(&base, None, None);
        let dial = dial(
            &url,
            &hello(
                &options.display_name,
                options.client.as_ref(),
                awareness_client_id,
            ),
            options.keepalive.as_ref(),
        )
        .await?;
        let info = dial.info;
        let session = PeerSession::new(&PeerOptions {
            room_id: info.room_id.clone(),
            room_key,
            host_key,
            renew: info.keepalive.awareness_renew,
            expire: info.keepalive.awareness_expire,
            seat: Some(info.seat.clone()),
            roster: seats_of(&info.peers),
            fixed_session_key: None,
            declared_role: None,
            awareness_client_id: Some(awareness_client_id),
            host: Some(HostOptions {
                host_seed,
                listing: options.listing,
                store: options.store,
            }),
        })
        .map_err(|error| sealed(&error))?;
        // §5.1: the room key and the host's public key in the fragment, in the order the
        // version writes them, and neither ever in the socket URL.
        let invite = info.token.as_ref().map(|token| {
            format!(
                "{}#k={}&h={}",
                proto::session_url(&base, Some(&info.room_id), Some(token)),
                encode_key(&room_key.0),
                host_key.encode()
            )
        });
        Ok(start(dial.socket, session, info, invite))
    }

    /// Joins the room an invite names; the fragment is read here and never reaches the socket.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invite`] when the link is not one this client can join with — including
    /// a link with no fragment, which carries neither of §5.1's two keys — and [`Error`] when
    /// the socket cannot be opened or the server refuses the session.
    pub async fn join(options: RelayJoinOptions) -> Result<Self, Error> {
        let invite =
            PeerInvite::parse(&options.invite).map_err(Error::Invite)?;
        let awareness_client_id = mint_awareness_client_id()?;
        let dial = dial(
            &invite.socket_url,
            &hello(
                &options.display_name,
                options.client.as_ref(),
                awareness_client_id,
            ),
            options.keepalive.as_ref(),
        )
        .await?;
        let info = dial.info;
        if info.room_id != invite.room {
            // The link names the room it joins, and a reply that seats this connection
            // somewhere else means the two readings disagreed about the address the join was
            // sent to. A session that continued would read every frame of the wrong room.
            // The refusal names the address, which carries no fragment: §5.1 keeps the two
            // keys out of every message a client produces.
            return Err(Error::Invite(invite.socket_url));
        }
        let session = PeerSession::new(&PeerOptions {
            room_id: invite.room,
            room_key: invite.room_key,
            host_key: invite.host_key,
            renew: info.keepalive.awareness_renew,
            expire: info.keepalive.awareness_expire,
            seat: Some(info.seat.clone()),
            roster: seats_of(&info.peers),
            fixed_session_key: None,
            declared_role: options.declared_role,
            awareness_client_id: Some(awareness_client_id),
            host: None,
        })
        .map_err(|error| sealed(&error))?;
        Ok(start(dial.socket, session, info, None))
    }

    // --- what a caller reads ---------------------------------------------------

    /// The connection's session description.
    #[must_use]
    pub fn session_info(&self) -> RelaySessionInfo {
        self.read().info.clone()
    }

    /// The invite this connection can hand on: the wire URL, its fragment carrying both keys.
    #[must_use]
    pub fn invite(&self) -> Option<String> {
        self.read().invite.clone()
    }

    /// The seats and names the relay knows, without the roles the applied state assigns.
    #[must_use]
    pub fn peers(&self) -> Vec<RelayPeer> {
        self.read().peers.clone()
    }

    /// The listing of the last state applied, with §5's refused paths dropped.
    #[must_use]
    pub fn listing(&self) -> Vec<String> {
        self.read().session.listing().to_vec()
    }

    /// The paths this replica holds text for: the documents that have arrived.
    #[must_use]
    pub fn documents(&self) -> Vec<String> {
        self.read().session.documents()
    }

    /// The text this replica holds at a path.
    #[must_use]
    pub fn text(&self, path: &str) -> String {
        self.read().session.text(path)
    }

    /// The CRDT state vector of this replica, as `(client id, clock)` pairs.
    #[must_use]
    pub fn state_vector(&self) -> Vec<(u64, u32)> {
        self.read().session.state_vector()
    }

    /// The paths this connection has open: what its own holds message carries (§13.7).
    #[must_use]
    pub fn held_paths(&self) -> Vec<String> {
        self.read().session.held().iter().cloned().collect()
    }

    /// What each peer is held to, by key or seat (§13.4, §13.7).
    #[must_use]
    pub fn peer_holds(&self) -> BTreeMap<String, Vec<String>> {
        self.read().session.peer_holds()
    }

    /// The room's open-document set as §13.7 makes it: the union of the live holds, this
    /// connection's included. It is what a version-1 room's `documents` used to be.
    #[must_use]
    pub fn open_documents(&self) -> Vec<String> {
        let state = self.read();
        let mut paths: Vec<String> =
            state.session.held().iter().cloned().collect();
        for held in state.session.peer_holds().values() {
            paths.extend(held.iter().cloned());
        }
        paths.sort_unstable();
        paths.dedup();
        paths
    }

    /// The role the applied state gives this connection's own key (`§13.4`).
    #[must_use]
    pub fn applied_role(&self) -> Option<String> {
        self.read().session.own_role().map(ToString::to_string)
    }

    /// The roles the applied state assigns, by the seat each committed key is labelled.
    #[must_use]
    pub fn roles_by_seat(&self) -> BTreeMap<String, String> {
        self.read().session.roles_by_seat()
    }

    /// The seat the applied state names as the host connection, if any (`§13.4`).
    #[must_use]
    pub fn named_host_seat(&self) -> Option<String> {
        self.read()
            .session
            .named_host_seat()
            .map(ToString::to_string)
    }

    /// Whether this connection holds the host key, which is the whole of what being the host is.
    #[must_use]
    pub fn is_host(&self) -> bool {
        self.read().session.is_host()
    }

    /// §13.10's ending, or `RoomGone` when the relay said the room is over.
    #[must_use]
    pub fn ending(&self) -> Option<RelayEnding> {
        self.read().ending
    }

    /// The sentence for that ending, where the version has one.
    #[must_use]
    pub fn ending_sentence(&self) -> Option<&'static str> {
        self.ending()?.sentence()
    }

    /// The first thing that went wrong, in this client's own words.
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        self.read().fault.clone()
    }

    /// The awareness client id this connection announced (`§8.4`).
    #[must_use]
    pub fn awareness_client_id(&self) -> u64 {
        self.read().session.awareness_client_id()
    }

    /// The highest `issued` this connection published, which is §7.1's series.
    #[must_use]
    pub fn published_issued(&self) -> u64 {
        self.read().session.published_issued()
    }

    /// The monotone elapsed time from this connection's seat (`§13.8`).
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.read().start.elapsed()
    }

    /// Subscribes to relay events.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<RelayEvent> {
        self.relay.events.subscribe()
    }

    // --- what the caller hands in ----------------------------------------------

    /// §13.1's join order: a hold is taken once a state commits this connection's key, and the
    /// whole set goes out when it changes rather than at the next renewal (§13.7).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] when the connection has ended.
    pub fn open(&self, path: &str) -> Result<(), Error> {
        self.moving(|state| {
            state.session.open(path);
            Ok(())
        })
    }

    /// Releases every path. §13.7 asks for the empty set rather than for silence, so the room
    /// learns in one hop instead of waiting out a lease.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] when the connection has ended.
    pub fn release(&self) -> Result<(), Error> {
        self.moving(|state| {
            state.session.release();
            Ok(())
        })
    }

    /// One local insert. Returns whether it was published — a `viewer`'s is not (`§13.9`), and
    /// nothing is before a state commits this connection's key (§13.1's step 4).
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the offset is past the end of the text, when the frame cannot be
    /// sealed, or when the connection has ended.
    #[expect(
        clippy::too_many_arguments,
        reason = "a path, an offset and the text are the three things an insert is"
    )]
    pub fn insert(
        &self,
        path: &str,
        index: u32,
        text: &str,
    ) -> Result<bool, Error> {
        self.moving(|state| {
            state
                .session
                .insert(path, index, text)
                .map_err(|error| sealed(&error))
        })
    }

    /// Changes this connection's display name (`PROTOCOL.md` §5). The server answers the mover
    /// and the rest of the room with `peer.renamed`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] when the connection has ended.
    pub fn rename(&self, display_name: &str) -> Result<(), Error> {
        let request = self.rename_request(display_name)?;
        self.send(Outbound::Text(request))
    }

    /// `session.rename`'s text frame, at the next request id this connection has used.
    fn rename_request(&self, display_name: &str) -> Result<String, Error> {
        let mut state = self.read();
        if state.destroyed {
            return Err(Error::Closed);
        }
        state.request_id = state.request_id.saturating_add(1);
        let request = serde_json::json!({
            "v": WIRE_VERSION_V2,
            "id": state.request_id,
            "method": method::SESSION_RENAME,
            "params": { "display_name": display_name },
        });
        Ok(request.to_string())
    }

    /// The host's listing changed: the whole tree as it now is (`§7.1`).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] when the connection has ended.
    pub fn listing_changed(&self) -> Result<(), Error> {
        self.moving(|state| {
            let clock = state.start.elapsed();
            state.session.listing_changed(clock);
            Ok(())
        })
    }

    /// §7.1's closing: the host's statement that the room is over. `false` when this connection
    /// is not the host, there is nothing to close, or the frame could not be sealed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] when the connection has ended.
    pub fn close_room(&self) -> Result<bool, Error> {
        self.moving(|state| Ok(state.session.close_room()))
    }

    /// Ends the session: the socket closed, the clock stopped, the session released.
    pub fn disconnect(&self) {
        {
            let mut state = self.read();
            state.destroyed = true;
        }
        let _ = self.relay.outgoing.send(Outbound::Close);
        let mut tasks = lock(&self.tasks);
        for task in tasks.drain(..) {
            task.abort();
        }
    }

    // --- the two paths every mutation takes -------------------------------------

    /// Applies a change to the session, puts what it published on the socket while the state
    /// is still held, and reports what the change is.
    fn moving<T>(
        &self,
        change: impl FnOnce(&mut RelayState) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let moved = self.moved(change)?;
        self.relay.emit(moved.events);
        if !moved.queued {
            return Err(Error::Closed);
        }
        Ok(moved.value)
    }

    /// Runs a change under the lock and takes what the session published with it.
    ///
    /// The frames are handed to the socket's queue by the same lock the session assigned their
    /// counters under ([`Relay::queue`]), so the counter order is the queue order.
    fn moved<T>(
        &self,
        change: impl FnOnce(&mut RelayState) -> Result<T, Error>,
    ) -> Result<Moved<T>, Error> {
        let mut state = self.read();
        if state.destroyed {
            return Err(Error::Closed);
        }
        let value = change(&mut state)?;
        let queued = Relay::queue(&mut state, &self.relay.outgoing);
        let events = report(&mut state);
        drop(state);
        Ok(Moved {
            value,
            events,
            queued,
        })
    }

    fn send(&self, frame: Outbound) -> Result<(), Error> {
        self.relay.outgoing.send(frame).map_err(|_| Error::Closed)
    }

    /// The lock, with a poisoned one treated as held: nothing here leaves shared state half
    /// written, and a session that refused every frame after one panic would be worse.
    fn read(&self) -> MutexGuard<'_, RelayState> {
        lock(&self.relay.state)
    }
}

/// Hands a seated session to a caller, with its socket and its clock started.
#[expect(
    clippy::too_many_arguments,
    reason = "a seated socket, its session, what the handshake said and the invite are the whole of a relay"
)]
fn start(
    socket: Socket,
    session: PeerSession,
    info: RelaySessionInfo,
    invite: Option<String>,
) -> RelaySession {
    let peers = info.peers.clone();
    let listing = session.listing().to_vec();
    let documents = session.documents();
    let (outgoing, incoming) = mpsc::unbounded_channel();
    let relay = Arc::new(Relay {
        state: Mutex::new(RelayState {
            session,
            info,
            start: Instant::now(),
            peers: peers.clone(),
            invite,
            ending: None,
            fault: None,
            destroyed: false,
            request_id: 1,
            last_listing: listing,
            last_documents: documents,
            last_peers: peers,
        }),
        events: broadcast::channel(EVENT_BACKLOG).0,
        outgoing,
    });
    let (sink, stream) = socket.split();
    let clock_task = tokio::spawn(clock(Arc::clone(&relay)));
    let socket_task =
        tokio::spawn(socket_loop(sink, stream, Arc::clone(&relay), incoming));
    let me = RelaySession {
        relay,
        tasks: Mutex::new(vec![clock_task, socket_task]),
    };
    let _ = me.relay.events.send(RelayEvent::Seated);
    // §13.1's step 4: a guest's announcement belongs at the join, and a host's first state is
    // already in the session's outbound queue, so both go out on this tick rather than on a
    // timer the caller would wait a whole renewal window for.
    let _ = me.moving(|state| {
        let clock = state.start.elapsed();
        state.session.tick(clock);
        Ok(())
    });
    me
}

/// The clock task (`§13.8`): the session's own timer, which nothing else moves it on.
///
/// It ends with the session it belongs to. A socket that closed or errored ends the session as
/// `room-gone` without destroying it, and a clock left ticking into an ended session would hold
/// the relay, its state and its event channel alive for the rest of the process while
/// publishing nothing.
async fn clock(relay: Arc<Relay>) {
    let mut ticker = interval(clock_period(&relay.read().info.keepalive));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let queued = relay.tick_once();
        let state = relay.read();
        if state.destroyed || state.ending.is_some() || !queued {
            return;
        }
    }
}

/// The period the clock task runs at.
///
/// `interval` panics on a zero period, and the value is either the room's advertised keepalive
/// — §8.2 asks a server for "anything positive", which a client cannot enforce — or the
/// caller's override, so it is floored here rather than trusted. A renewal that is not a
/// duration at all is not a session quietly ticking nothing.
fn clock_period(keepalive: &KeepaliveConfig) -> Duration {
    keepalive.awareness_renew.max(Duration::from_millis(1))
}

/// The socket task: one inbound frame at a time, and whatever the session produced after it.
#[expect(
    clippy::too_many_arguments,
    reason = "the two halves of the socket, the relay and its inbound queue are what the task owns"
)]
async fn socket_loop(
    mut sink: Sink,
    mut stream: Stream,
    relay: Arc<Relay>,
    mut incoming: mpsc::UnboundedReceiver<Outbound>,
) {
    loop {
        tokio::select! {
            arriving = stream.next() => {
                let Some(message) = arriving else {
                    relay.closed();
                    return;
                };
                let queued = match message {
                    Ok(Message::Binary(frame)) => relay.deliver(&frame),
                    Ok(Message::Text(text)) => relay.hear(&text),
                    Ok(_) => true,
                    Err(_) => {
                        relay.closed();
                        return;
                    }
                };
                if !queued {
                    return;
                }
            }
            next = incoming.recv() => {
                let Some(frame) = next else { return };
                if !send_frame(&mut sink, frame).await {
                    relay.closed();
                    return;
                }
            }
        }
    }
}

/// One frame on its way out. `false` means the socket is gone or the caller asked to close it.
async fn send_frame(sink: &mut Sink, frame: Outbound) -> bool {
    match frame {
        Outbound::Text(text) => sink.send(Message::text(text)).await.is_ok(),
        Outbound::Binary(bytes) => {
            sink.send(Message::binary(bytes)).await.is_ok()
        }
        Outbound::Close => {
            let _ = sink.close().await;
            false
        }
    }
}

/// The lock, with a poisoned one treated as held.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Takes what the session has published, and keeps the first fault it reports.
fn drain(state: &mut RelayState) -> Vec<Vec<u8>> {
    let frames = state.session.take_outbound();
    if let Some(fault) = state.session.fault()
        && state.fault.is_none()
    {
        state.fault = Some(fault.to_string());
    }
    frames
}

impl Relay {
    fn read(&self) -> MutexGuard<'_, RelayState> {
        lock(&self.state)
    }

    /// One tick of the session's own clocks, then whatever it published.
    ///
    /// Returns whether the socket's queue took every frame.
    fn tick_once(&self) -> bool {
        let (queued, events) = self.ticked();
        self.emit(events);
        queued
    }

    fn ticked(&self) -> (bool, Vec<RelayEvent>) {
        let mut state = self.read();
        if state.destroyed || state.ending.is_some() {
            return (true, Vec::new());
        }
        let clock = state.start.elapsed();
        state.session.tick(clock);
        let queued = Self::queue(&mut state, &self.outgoing);
        let events = report(&mut state);
        (queued, events)
    }

    /// One sealed frame, as the relay delivered it. Returns whether the socket's queue took
    /// every frame the delivery answered with.
    fn deliver(&self, frame: &[u8]) -> bool {
        let (queued, events) = self.delivered(frame);
        self.emit(events);
        queued
    }

    fn delivered(&self, frame: &[u8]) -> (bool, Vec<RelayEvent>) {
        let mut state = self.read();
        let clock = state.start.elapsed();
        let outcome = state.session.deliver(clock, frame);
        // §13.5's content, and only content: a state, a holds set and a closing change no text.
        let content = matches!(outcome, Outcome::Applied { kind: 0 });
        let queued = Self::queue(&mut state, &self.outgoing);
        let mut events = report(&mut state);
        if content {
            events.push(RelayEvent::Text);
        }
        (queued, events)
    }

    /// One text frame: the roster's events, a fault, or the room's end. Returns whether the
    /// socket's queue took every frame the event answered with.
    fn hear(&self, text: &str) -> bool {
        let Ok(message) = proto::ServerMessage::from_text(text) else {
            return true;
        };
        if message.event.as_deref() == Some(event::SESSION_ERROR) {
            self.emit(vec![RelayEvent::Failed(self.note_fault(&message))]);
            return true;
        }
        let (queued, events) = self.heard(&message);
        self.emit(events);
        queued
    }

    /// A server-authored frame, folded into the session, with what the two produced.
    fn heard(&self, message: &proto::ServerMessage) -> (bool, Vec<RelayEvent>) {
        let mut state = self.read();
        let clock = state.start.elapsed();
        match message.event.as_deref() {
            Some(event::PEER_JOINED) => {
                state.hear_joined(clock, message.params.as_ref());
            }
            Some(event::PEER_LEFT) => {
                state.hear_left(clock, message.params.as_ref());
            }
            Some(event::PEER_RENAMED) => {
                state.hear_renamed(message.params.as_ref());
            }
            Some(event::ROOM_GONE) => state.end(RelayEnding::RoomGone),
            _ => {}
        }
        let queued = Self::queue(&mut state, &self.outgoing);
        let events = report(&mut state);
        (queued, events)
    }

    /// Hands a session's published frames to the socket's queue, while the caller still holds
    /// the state lock they were published under.
    ///
    /// `CANONICAL.md` §6.1 numbers a frame under the key that signs it, and a receiver refuses
    /// a `kind = 0`, `3` or `4` frame "at or below the mark" it has already advanced. The next
    /// counter is taken under this lock, so the queue push has to happen under it too: a push
    /// after the lock was released can be overtaken by another producer — the socket task, the
    /// clock task, another caller — that took the lock in the gap, and the room then reads the
    /// two frames with their counters the wrong way round and refuses the earlier one
    /// `replayed_counter`.
    ///
    /// Returns whether the queue took every frame. A queue that is gone is a socket task that
    /// has ended.
    fn queue(
        state: &mut RelayState,
        outgoing: &mpsc::UnboundedSender<Outbound>,
    ) -> bool {
        drain(state)
            .into_iter()
            .all(|frame| outgoing.send(Outbound::Binary(frame)).is_ok())
    }

    /// One `session.error`: the server's own words, kept and reported, and never fatal.
    fn note_fault(&self, message: &proto::ServerMessage) -> String {
        let reason = fault_sentence(message);
        let mut state = self.read();
        state.fault.get_or_insert_with(|| reason.clone());
        reason
    }

    /// The socket ended: unless the caller asked for it, the room is what ended the session.
    fn closed(&self) {
        let events = self.ended();
        self.emit(events);
    }

    fn ended(&self) -> Vec<RelayEvent> {
        let mut state = self.read();
        if state.destroyed {
            return Vec::new();
        }
        state.end(RelayEnding::RoomGone);
        state
            .fault
            .get_or_insert_with(|| "the connection ended".to_string());
        report(&mut state)
    }

    fn emit(&self, events: Vec<RelayEvent>) {
        for event in events {
            let _ = self.events.send(event);
        }
    }
}

impl RelayState {
    const fn end(&mut self, ending: RelayEnding) {
        if self.ending.is_none() {
            self.ending = Some(ending);
        }
    }

    fn hear_joined(
        &mut self,
        clock: Duration,
        params: Option<&serde_json::Value>,
    ) {
        let Some(peer) = params.and_then(peer_of) else {
            return;
        };
        self.peers.retain(|held| held.peer_id != peer.peer_id);
        self.peers.push(peer.clone());
        self.session.seat_joined(clock, &peer.peer_id);
    }

    fn hear_left(
        &mut self,
        clock: Duration,
        params: Option<&serde_json::Value>,
    ) {
        let named = params
            .and_then(|record| record.get("peer_id"))
            .and_then(serde_json::Value::as_str);
        let Some(departed) = named else { return };
        self.peers.retain(|held| held.peer_id != departed);
        self.session.seat_left(clock, departed);
    }

    fn hear_renamed(&mut self, params: Option<&serde_json::Value>) {
        let Some(record) = params else { return };
        let seat = record.get("peer_id").and_then(serde_json::Value::as_str);
        let name = record
            .get("display_name")
            .and_then(serde_json::Value::as_str);
        let (Some(peer_id), Some(display_name)) = (seat, name) else {
            return;
        };
        let renamed =
            self.peers.iter_mut().find(|held| held.peer_id == peer_id);
        if let Some(held) = renamed {
            display_name.clone_into(&mut held.display_name);
        }
    }
}

/// What a session's state says now, as the events a change of it produces.
fn report(state: &mut RelayState) -> Vec<RelayEvent> {
    let mut events = Vec::new();
    let listing = state.session.listing().to_vec();
    if listing != state.last_listing {
        state.last_listing.clone_from(&listing);
        events.push(RelayEvent::Listing(listing));
    }
    let documents = state.session.documents();
    if documents != state.last_documents {
        state.last_documents.clone_from(&documents);
        events.push(RelayEvent::Content(documents));
    }
    if state.ending.is_none()
        && let Some(ending) = state.session.ending()
    {
        let reported = relay_ending(ending);
        state.ending = Some(reported);
        events.push(RelayEvent::Ended(reported));
    }
    let peers = state.peers.clone();
    if peers != state.last_peers {
        state.last_peers.clone_from(&peers);
        events.push(RelayEvent::Peers(peers));
    }
    events
}

/// §13.10's ending as the relay names it.
const fn relay_ending(ending: Ending) -> RelayEnding {
    match ending {
        Ending::Closing => RelayEnding::Closing,
        Ending::HostAway => RelayEnding::HostAway,
        Ending::NoState => RelayEnding::NoState,
    }
}

/// A `session.error` event, as the refusal it is: its `params` carry the code and the message.
fn session_error(params: Option<&serde_json::Value>) -> Error {
    let code = params
        .and_then(|refusal| refusal.get("code"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("protocol_error")
        .to_string();
    let message = params
        .and_then(|refusal| refusal.get("message"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("the server refused the session")
        .to_string();
    Error::Protocol { code, message }
}

/// The server's own words for a `session.error`.
fn fault_sentence(message: &proto::ServerMessage) -> String {
    message
        .error
        .as_ref()
        .map_or("the server reported a fault", |error| {
            error.message.as_str()
        })
        .to_string()
}

/// The roster a session is handed: the seats the handshake showed, without this connection.
fn seats_of(peers: &[RelayPeer]) -> BTreeSet<String> {
    peers.iter().map(|peer| peer.peer_id.clone()).collect()
}

/// The part of a link `PROTOCOL.md` §5.1 lets out: everything before its first `#`.
///
/// The fragment carries the room key and the host key, and §5.1 says a client MUST NOT log
/// them, so no message or error here names a whole link — a refusal names the address, which
/// is what the sentence is about anyway.
fn link_address(link: &str) -> &str {
    link.split_once('#')
        .map_or(link, |(address, _fragment)| address)
}

/// One `PeerInfo` from a `peer.joined`'s params, which wraps its record under `peer`.
fn peer_of(params: &serde_json::Value) -> Option<RelayPeer> {
    let record = params.get("peer").unwrap_or(params);
    let peer: proto::PeerInfoV2 =
        serde_json::from_value(record.clone()).ok()?;
    Some(RelayPeer {
        peer_id: peer.peer_id,
        display_name: peer.display_name,
        awareness_client_id: peer.awareness_client_id,
    })
}

/// The server base of a URL, without the endpoint path. It is what a mint dials and what the
/// invite is built from.
///
/// `PROTOCOL.md` §5.1: the fragment carries the room's keys and is never part of a request, so
/// an address with one is not a base. A base is what every URL this connection builds is made
/// from, and a fragment left in it would ride into each of them.
fn session_base(url: &str) -> Option<String> {
    let (address, _) = url.split_once('?').unwrap_or((url, ""));
    if address.contains('#') {
        return None;
    }
    let base = address
        .strip_suffix(proto::ENDPOINT_PATH)
        .unwrap_or(address);
    (!base.is_empty()).then(|| base.to_string())
}

/// `session.hello` at `selvage/2`: no `role`, because the version seats nobody as anything.
fn hello(
    display_name: &str,
    client: Option<&String>,
    awareness_client_id: u64,
) -> String {
    let params = proto::HelloParamsV2 {
        awareness_client_id: Some(awareness_client_id),
        capabilities: proto::CAPABILITIES_V2
            .iter()
            .map(ToString::to_string)
            .collect(),
        client: client.cloned(),
        display_name: display_name.to_string(),
    };
    serde_json::json!({
        "v": WIRE_VERSION_V2,
        "id": 1,
        "method": method::SESSION_HELLO,
        "params": params,
    })
    .to_string()
}

/// A socket seated by its handshake reply.
struct Dialled {
    socket: Socket,
    info: RelaySessionInfo,
}

/// Opens the socket, says `session.hello` at `selvage/2`, and waits to be seated.
///
/// The reply is the connection's first frame after the handshake (`PROTOCOL.md` §5), so
/// nothing between the hello and the seating has to be queued: whatever arrives next is read
/// by the socket task, which starts with the session already built.
async fn dial(
    url: &str,
    hello: &str,
    keepalive: Option<&KeepaliveConfig>,
) -> Result<Dialled, Error> {
    refuse_tls(url)?;
    timeout(HANDSHAKE_TIMEOUT, seating(url, hello, keepalive))
        .await
        .map_err(|_| Error::Closed)?
}

/// The dial itself, bounded by the caller's handshake timeout.
async fn seating(
    url: &str,
    hello: &str,
    keepalive: Option<&KeepaliveConfig>,
) -> Result<Dialled, Error> {
    let (mut socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(Error::Wire)?;
    socket
        .send(Message::text(hello.to_string()))
        .await
        .map_err(Error::Wire)?;
    loop {
        let Some(message) = socket.next().await else {
            return Err(Error::Closed);
        };
        let Ok(Message::Text(text)) = message else {
            // §5: the seating reply is a text frame and the first one after the hello, so
            // anything else arriving before it belongs to a session that does not exist.
            continue;
        };
        let frame = proto::ServerMessage::from_text(&text)?;
        if let Some(error) = frame.error {
            return Err(Error::Protocol {
                code: error.code,
                message: error.message,
            });
        }
        match frame.event.as_deref() {
            // §5: the reply is one `room.created` or `room.joined`, and it is the connection's
            // first frame after the handshake.
            Some(event::ROOM_CREATED | event::ROOM_JOINED) => {}
            // A refusal is an event, and its `params` are the code and the message rather than
            // a room description.
            Some(event::SESSION_ERROR) => {
                return Err(session_error(frame.params.as_ref()));
            }
            // Anything else is not the seating reply: a frame before it belongs to a session
            // that does not exist.
            _ => continue,
        }
        let Some(body) = frame.params else { continue };
        let read: proto::SessionParamsV2 = serde_json::from_value(body)?;
        let base = session_base(url)
            .ok_or_else(|| Error::Invite(link_address(url).to_string()))?;
        let advertised = KeepaliveConfig::from(read.keepalive);
        return Ok(Dialled {
            socket,
            info: RelaySessionInfo {
                room_id: read.room_id,
                token: read.token,
                seat: read.self_peer.peer_id,
                peers: read.peers.iter().map(peer_info).collect(),
                capabilities: read.capabilities,
                keepalive: overridden(advertised, keepalive),
                base_url: base,
            },
        });
    }
}

/// The clocks the session runs on: the server's, overridden by the caller's (`§8.2`).
const fn overridden(
    advertised: KeepaliveConfig,
    given: Option<&KeepaliveConfig>,
) -> KeepaliveConfig {
    match given.copied() {
        Some(chosen) => chosen,
        None => advertised,
    }
}

/// One seat as the handshake reply writes it.
fn peer_info(peer: &proto::PeerInfoV2) -> RelayPeer {
    RelayPeer {
        peer_id: peer.peer_id.clone(),
        display_name: peer.display_name.clone(),
        awareness_client_id: peer.awareness_client_id,
    }
}

/// This build has no TLS, so a `wss://` socket is one no session here can be seated on. Saying
/// so where the dial happens is the difference between a named refusal and a handshake failure
/// a reader has to go looking for.
fn refuse_tls(url: &str) -> Result<(), Error> {
    if url.starts_with("wss://") {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "a wss:// base needs TLS, which this client is not built with: use ws://",
        )));
    }
    Ok(())
}

/// A room key from the platform's CSPRNG (`CANONICAL.md` §6.1).
fn random_room_key() -> Result<RoomKey, Error> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|e| Error::Invite(format!("no randomness: {e}")))?;
    Ok(RoomKey(bytes))
}

/// The host key's seed, from the platform's CSPRNG.
fn random_seed() -> Result<[u8; 32], Error> {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed)
        .map_err(|e| Error::Invite(format!("no randomness: {e}")))?;
    Ok(seed)
}

/// The awareness client id this connection announces, which is the one its session publishes
/// under (`§8.4`).
///
/// # Errors
///
/// Returns [`Error::Yjs`] when the platform's CSPRNG cannot be read.
fn mint_awareness_client_id() -> Result<u64, Error> {
    let nonce = fresh_nonce().map_err(|error| sealed(&error))?;
    let mut id: u64 = 0;
    for byte in nonce.iter().take(4) {
        id = id.saturating_mul(256).saturating_add(u64::from(*byte));
    }
    Ok(id)
}

/// Reports a sealed-layer failure as this crate's own error.
fn sealed(error: &SealedError) -> Error {
    Error::Yjs(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::error::Error as StdError;
    use std::hint::spin_loop;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::thread::{
        JoinHandle as ThreadHandle, available_parallelism, spawn,
    };
    use std::time::{Duration, Instant};

    use futures_util::SinkExt;
    use futures_util::StreamExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::Message;

    use super::{
        RelayHostOptions, RelayJoinOptions, RelaySession, RelaySessionInfo,
        clock_period, lock, session_base, start,
    };
    use crate::host::{HostOptions, ListingSource};
    use crate::peer::{PeerOptions, PeerSession};
    use crate::sealed::{
        Envelope, KeyId, Recipe, RoomKey, SessionKey, encode_key, seal,
    };
    use crate::session::KeepaliveConfig;

    const ROOM: &str = "r-1";
    const SEAT: &str = "p-self";
    const PATH: &str = "notes.txt";
    const HOST_SEED: [u8; 32] = [3; 32];
    const OWN_SEED: [u8; 32] = [5; 32];

    /// How long a test waits for a predicate of its own before it reports what it saw.
    const WAIT: Duration = Duration::from_secs(5);

    /// How long two editors edit one connection during the race below.
    const WINDOW: Duration = Duration::from_millis(600);

    /// The clocks these tests run the session on: a renewal short enough for the clock task to
    /// move during a test, and a lease long enough not to lapse.
    const fn keepalive() -> KeepaliveConfig {
        KeepaliveConfig {
            awareness_renew: Duration::from_millis(1),
            awareness_expire: Duration::from_secs(30),
        }
    }

    /// One frame as it arrived at the server end.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Arrived {
        /// A `selvage/2` frame, as §6.1's clear prefix writes it: the key that signed it, its
        /// kind, and the counter under that key.
        Sealed {
            key_id: KeyId,
            kind: u64,
            counter: u64,
        },
        /// A text frame — the session's own, such as a rename.
        Text,
    }

    /// A relay over a loopback WebSocket, with this test as the server end.
    ///
    /// Nothing dials anywhere and no server is involved, so the relay's own wiring is the
    /// subject rather than a room. The server end reads every frame the client sends, in the
    /// order the socket delivers it, and records §6.1's clear prefix: the sealing covers none
    /// of it, and an end holding no key still reads the order a sender numbered its frames in.
    /// It sends too, through [`Fixture::feed`], which is how a test reaches the socket task's
    /// own inbound paths.
    struct Fixture {
        relay: Arc<RelaySession>,
        arrived: Arc<Mutex<Vec<Arrived>>>,
        feed: mpsc::UnboundedSender<Message>,
        close: Option<oneshot::Sender<()>>,
        clock: Option<JoinHandle<()>>,
    }

    impl Fixture {
        /// The frames that arrived so far, in order.
        fn arrived(&self) -> Vec<Arrived> {
            lock(&self.arrived).clone()
        }

        /// Puts one frame on the wire to the client, as the room's server would. The socket
        /// task reads it through `deliver` or `hear` and publishes whatever §13 owes it, which
        /// is the path a fixture that only ever listened cannot exercise.
        fn feed(&self, frame: Message) -> bool {
            self.feed.send(frame).is_ok()
        }

        /// Ends the room's socket the way a server that goes away does.
        fn end_the_socket(&mut self) {
            let close = self.close.take();
            let _ = close.map(|sender| sender.send(()));
        }

        /// Takes the clock task's handle, so a test can watch it end. Taking the handle does
        /// not stop the task: a dropped handle leaves a task running.
        fn take_clock(&mut self) -> Option<JoinHandle<()>> {
            self.clock.take()
        }
    }

    /// The session a fixture seats: the room, the two keys and this connection's own key, all
    /// from constants. `host` is `None` for a guest, whose key no state commits until one
    /// arrives — so §13.1's step 4 holds everything it edits back.
    fn options(
        keepalive: KeepaliveConfig,
        host: Option<HostOptions>,
    ) -> PeerOptions {
        PeerOptions {
            room_id: ROOM.to_string(),
            room_key: RoomKey([7; 32]),
            host_key: SessionKey::from_seed(HOST_SEED).public(),
            renew: keepalive.awareness_renew,
            expire: keepalive.awareness_expire,
            seat: Some(SEAT.to_string()),
            roster: BTreeSet::new(),
            fixed_session_key: Some(OWN_SEED),
            declared_role: None,
            awareness_client_id: Some(7),
            host,
        }
    }

    /// A seated relay over a loopback socket, whatever session is on it.
    async fn seated(
        keepalive: KeepaliveConfig,
        options: &PeerOptions,
    ) -> Result<Fixture, Box<dyn StdError>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (server, client) = tokio::try_join!(
            async {
                let (tcp, _) = listener.accept().await?;
                let socket = tokio_tungstenite::accept_async(tcp).await?;
                Ok::<_, Box<dyn StdError>>(socket)
            },
            async {
                let url = format!("ws://{addr}/session");
                let (socket, _) = tokio_tungstenite::connect_async(url).await?;
                Ok::<_, Box<dyn StdError>>(socket)
            },
        )?;

        let arrived = Arc::new(Mutex::new(Vec::new()));
        let (close, closed) = oneshot::channel();
        let (feed, sent) = mpsc::unbounded_channel();
        tokio::spawn(serving(server, Arc::clone(&arrived), closed, sent));

        let session = PeerSession::new(options)?;
        let info = RelaySessionInfo {
            room_id: ROOM.to_string(),
            token: Some("t-1".to_string()),
            seat: SEAT.to_string(),
            peers: Vec::new(),
            capabilities: Vec::new(),
            keepalive,
            base_url: format!("ws://{addr}"),
        };
        let relay = start(client, session, info, None);
        // `start` spawns the clock task first and the socket task after it: this end keeps the
        // clock's handle and lets the socket's go, which leaves that task running.
        let mut tasks = {
            let mut held = lock(&relay.tasks);
            held.drain(..).collect::<Vec<_>>()
        };
        let clock = tasks.drain(..1).next();
        Ok(Fixture {
            relay: Arc::new(relay),
            arrived,
            feed,
            close: Some(close),
            clock,
        })
    }

    /// A seated relay over a loopback socket: a host session, seeded from constants, whose own
    /// mint state commits its key — so a local edit is published rather than held back.
    async fn fixture(
        keepalive: KeepaliveConfig,
    ) -> Result<Fixture, Box<dyn StdError>> {
        let listing: ListingSource = Arc::new(Vec::new);
        let host = HostOptions {
            host_seed: HOST_SEED,
            listing,
            store: None,
        };
        seated(keepalive, &options(keepalive, Some(host))).await
    }

    /// The same socket with a guest on it: no host half, and no state has committed its key, so
    /// §13.1's step 4 holds back every edit until one does.
    async fn guest_fixture(
        keepalive: KeepaliveConfig,
    ) -> Result<Fixture, Box<dyn StdError>> {
        seated(keepalive, &options(keepalive, None)).await
    }

    /// The server end: what the client sent, until the socket ends, and what this end puts on
    /// the wire back to it.
    #[expect(
        clippy::too_many_arguments,
        reason = "the socket, its own two ends and the frames this test feeds in are what a server end is"
    )]
    async fn serving(
        socket: WebSocketStream<TcpStream>,
        arrived: Arc<Mutex<Vec<Arrived>>>,
        mut close: oneshot::Receiver<()>,
        mut sending: mpsc::UnboundedReceiver<Message>,
    ) {
        let (mut sink, mut stream) = socket.split();
        loop {
            tokio::select! {
                arriving = stream.next() => match arriving {
                    Some(Ok(Message::Binary(frame))) => {
                        if let Ok(envelope) = Envelope::parse(&frame) {
                            lock(&arrived).push(Arrived::Sealed {
                                key_id: envelope.key_id,
                                kind: envelope.kind,
                                counter: envelope.counter,
                            });
                        }
                    }
                    Some(Ok(Message::Text(_))) => lock(&arrived).push(Arrived::Text),
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => return,
                },
                outgoing = sending.recv() => {
                    let Some(frame) = outgoing else { return };
                    if sink.send(frame).await.is_err() {
                        return;
                    }
                }
                _ = &mut close => {
                    let _ = sink.close().await;
                    return;
                }
            }
        }
    }

    /// Waits for a predicate of this test's own, with a deadline, and says whether it held. The
    /// predicate is polled rather than slept on: what a test waits for is the effect.
    async fn held_within(
        deadline: Duration,
        mut predicate: impl FnMut() -> bool,
    ) -> bool {
        let start = Instant::now();
        while start.elapsed() < deadline && !predicate() {
            sleep(Duration::from_millis(2)).await;
        }
        predicate()
    }

    /// One editor's whole job: local inserts into one connection until its window closes.
    fn edit_for(relay: &RelaySession, window: Duration) {
        let start = Instant::now();
        while start.elapsed() < window {
            let _ = relay.insert(PATH, 0, "x");
        }
    }

    /// A thread that only burns its core, which is what puts a preemption inside the window the
    /// race below needs.
    fn burn_for(window: Duration) {
        let start = Instant::now();
        while start.elapsed() < window {
            spin_loop();
        }
    }

    /// One editor on a thread of its own.
    fn editor(relay: Arc<RelaySession>, window: Duration) -> ThreadHandle<()> {
        spawn(move || edit_for(&relay, window))
    }

    /// One core burned by a thread of its own.
    fn noisy(window: Duration) -> ThreadHandle<()> {
        spawn(move || burn_for(window))
    }

    /// The editors: two connections' worth of local edits, each on its own thread.
    fn editors(
        relay: &Arc<RelaySession>,
        count: usize,
    ) -> Vec<ThreadHandle<()>> {
        (0..count)
            .map(|_| editor(Arc::clone(relay), WINDOW))
            .collect()
    }

    /// One burnt core per core, which is what puts a preemption inside the window the race
    /// below needs.
    fn noise(count: usize) -> Vec<ThreadHandle<()>> {
        (0..count).map(|_| noisy(WINDOW)).collect()
    }

    /// Joins the threads a test started, so a panic inside one is the test's failure and never
    /// a green run.
    fn joined(threads: Vec<ThreadHandle<()>>, what: &str) {
        for thread in threads {
            assert!(thread.join().is_ok(), "a {what} thread panicked");
        }
    }

    /// The counters of the frames one key signed, in the order the socket delivered them.
    fn counters_of(arrived: &[Arrived], key: KeyId) -> Vec<u64> {
        arrived
            .iter()
            .filter_map(|frame| match frame {
                Arrived::Sealed {
                    key_id, counter, ..
                } if *key_id == key => Some(*counter),
                Arrived::Sealed { .. } | Arrived::Text => None,
            })
            .collect()
    }

    /// Whether a frame of this kind reached the server end.
    fn sent_kind(arrived: &Mutex<Vec<Arrived>>, kind: u64) -> bool {
        lock(arrived).iter().any(
            |frame| matches!(frame, Arrived::Sealed { kind: seen, .. } if *seen == kind),
        )
    }

    /// Whether one of the session's own text frames reached the server end.
    fn sent_text(arrived: &Mutex<Vec<Arrived>>) -> bool {
        lock(arrived)
            .iter()
            .any(|frame| matches!(frame, Arrived::Text))
    }

    /// Refuses the first counter that does not advance. The order the counters leave in is what
    /// a receiver's mark is kept on (`CANONICAL.md` §6.1): a `kind = 0`, `3` or `4` frame at or
    /// below the mark is refused whatever else is right about it.
    fn the_counters_advance(counters: &[u64]) {
        let mut previous = 0;
        for (index, counter) in counters.iter().enumerate() {
            assert!(
                *counter > previous,
                "frame {index} of {} carries counter {counter} after {previous}: a later frame \
                 reached the socket first, and a peer refuses the earlier one as \
                 `replayed_counter`",
                counters.len()
            );
            previous = *counter;
        }
    }

    /// M1: a frame's counter is assigned under the state lock, so the order the socket's queue
    /// takes the frames in has to be that same order. Two tasks editing one connection
    /// interleave inside that lock, and what leaves must be the order the session numbered the
    /// frames in: a reordered pair costs the earlier edit until §13.6's re-sync repairs it.
    ///
    /// **This test is a race, and it is built to lose it.** The gap the defect lives in is
    /// between releasing the lock and handing the frame over, and nothing but a preemption
    /// widens it: the editors run on threads of their own and the test burns a core per core
    /// while they do, because on a machine with a core to spare the publisher always reaches
    /// its push first and the arrangement under test — the push after the lock — would pass.
    /// What it measures is the wire order, which is what a peer's mark is kept on.
    ///
    /// That load is the whole reproduction, and what it buys is worth stating: with the push
    /// moved back to after the lock this test passed every run on a quiet host (0 red of 10)
    /// and failed every run with one spinner per core added (10 of 10), and a failing run
    /// carried 60–105 inversions in 5,500–7,000 frames, of which the first is the one reported.
    /// The producers here are two callers and the clock; the socket task's own path is the test
    /// below.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_editors_frames_leave_in_the_order_they_were_numbered()
    -> Result<(), Box<dyn StdError>> {
        let mut fixture = fixture(keepalive()).await?;
        fixture.relay.open(PATH)?;

        joined(editors(&fixture.relay, 2), "editor");
        joined(
            noise(available_parallelism().map_or(4, NonZeroUsize::get)),
            "noise",
        );

        // A text frame the session sends after the last edit is the sentinel that says the
        // queue holds none of theirs any more: everything the editors published is already at
        // this end, in the order the socket delivered it.
        fixture.relay.rename("done")?;
        let arrived = Arc::clone(&fixture.arrived);
        assert!(
            held_within(WAIT, || sent_text(&arrived)).await,
            "the sentinel frame arrives"
        );

        let counters = counters_of(
            &fixture.arrived(),
            SessionKey::from_seed(OWN_SEED).public().id(),
        );
        assert!(
            counters.len() > 100,
            "the two editors published frames: {} arrived",
            counters.len()
        );
        the_counters_advance(&counters);
        fixture.end_the_socket();
        Ok(())
    }

    /// How many edits a guest makes before the state that commits its key arrives. §13.1's step
    /// 4 flushes every one of them as a single report, and a report this size is what makes the
    /// race below decidable: the window a wrong arrangement leaves is the push loop the report
    /// is handed over in, and one frame makes that window a function call wide.
    const HELD_EDITS: usize = 8_000;

    /// One frame this end seals itself, as the session's own `sealed_frame` does: the room's
    /// frame key, the kind, a counter under the key that signs it.
    #[expect(
        clippy::too_many_arguments,
        reason = "what a test seals with: the kind, the counter, the signer and the plaintext"
    )]
    fn sealed(
        kind: u64,
        counter: u64,
        signer: &SessionKey,
        plaintext: &[u8],
    ) -> Vec<u8> {
        let frame_key = RoomKey([7; 32]).frame_key(ROOM);
        let recipe = Recipe {
            room_id: ROOM,
            frame_key: &frame_key,
            kind,
            epoch: 0,
            counter,
            nonce: [9; 12],
            signer,
        };
        seal(&recipe, plaintext).expect("the frame seals").bytes()
    }

    /// A host-signed state that commits this connection's own key: §7.1's mint state, which is
    /// what takes a guest out of §13.1's step 4 and makes it publish everything it held back.
    fn committing_state() -> Vec<u8> {
        let mut peers = serde_json::Map::new();
        peers.insert(
            SessionKey::from_seed(OWN_SEED).public().encode(),
            serde_json::json!({"peer_id": SEAT, "role": "guest"}),
        );
        let payload = serde_json::json!({
            "issued": 1,
            "listing": [PATH],
            "peers": peers,
        });
        sealed(
            1,
            1,
            &SessionKey::from_seed(HOST_SEED),
            &serde_json::to_vec(&payload).expect("a state encodes"),
        )
    }

    /// `count` edits a guest makes before a state commits its key, every one of them held back
    /// by §13.1's step 4 rather than published.
    fn hold_edits(
        relay: &RelaySession,
        count: usize,
    ) -> Result<(), Box<dyn StdError>> {
        for _ in 0..count {
            let _ = relay.insert(PATH, 0, "x")?;
        }
        Ok(())
    }

    /// M1, the socket task's half: a report `deliver` publishes is handed to the socket under
    /// the guard that numbered its frames, exactly as a caller's own edit is.
    ///
    /// The report is a guest's whole held-back set. §13.1's step 4 keeps every edit made before
    /// a state commits this connection's key, and the state that commits it puts all of them on
    /// the wire in one report — published from the socket task's own `delivered`, which is the
    /// side of the lock that a fixture whose server end only ever listens cannot reach. The
    /// editors above race each other; this races an editor against that report, and the
    /// property is the same: a frame the report numbered must not arrive behind one the editor
    /// numbered later.
    ///
    /// **The race is the same race, with a wider window.** Reverting `delivered` alone — the
    /// report drained under the guard and handed over after it — leaves the editors' test above
    /// green and turns this one red on a quiet host: fifteen failing runs in fifteen, the
    /// failure the first counter inversion, which lands between frame 3,400 and frame 10,700 of
    /// runs of 9,700–14,900. The window is the loop the report's frames leave in, thousands of
    /// pushes wide, where a report of one frame leaves a window a function call wide and the
    /// machine's mood decides it: at half this report, that revert was caught in 8 runs of 10.
    /// With the fix it is green, ten runs in ten.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reports_frames_are_handed_over_under_the_lock_that_numbered_them()
    -> Result<(), Box<dyn StdError>> {
        let mut fixture = guest_fixture(keepalive()).await?;
        fixture.relay.open(PATH)?;
        // §13.1's step 4, ahead of the state: none of these is published yet.
        hold_edits(&fixture.relay, HELD_EDITS)?;

        // An editor of its own, editing through the lock while the report is handed over.
        let editor = editor(Arc::clone(&fixture.relay), WINDOW);
        assert!(
            fixture.feed(Message::binary(committing_state())),
            "the committing state reaches the client's socket"
        );
        joined(vec![editor], "editor");

        // The same sentinel as above: a text frame the session sends after the last edit says
        // the queue holds none of theirs any more.
        fixture.relay.rename("done")?;
        let arrived = Arc::clone(&fixture.arrived);
        assert!(
            held_within(WAIT, || sent_text(&arrived)).await,
            "the sentinel frame arrives"
        );

        let counters = counters_of(
            &fixture.arrived(),
            SessionKey::from_seed(OWN_SEED).public().id(),
        );
        assert!(
            counters.len() > HELD_EDITS,
            "the held set and the editor both published: {} arrived",
            counters.len()
        );
        the_counters_advance(&counters);
        fixture.end_the_socket();
        Ok(())
    }

    /// m1: the clock task ends with the session it belongs to. A socket that closed or errored
    /// ends the session as `room-gone` without destroying it, and a clock that watched only
    /// `destroyed` would tick into an ended session for the rest of the process, holding the
    /// relay, its state and its event channel alive while publishing nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_clock_task_ends_with_a_closed_socket()
    -> Result<(), Box<dyn StdError>> {
        let mut fixture = fixture(keepalive()).await?;
        let clock =
            fixture.take_clock().ok_or("the fixture has a clock task")?;
        assert!(
            !clock.is_finished(),
            "the clock runs while the room's socket does"
        );
        fixture.end_the_socket();
        assert!(
            held_within(WAIT, || clock.is_finished()).await,
            "the clock task is still running after the socket closed"
        );
        Ok(())
    }

    /// m4: `interval` panics on a zero period, and that period is either the room's advertised
    /// keepalive — which `PROTOCOL.md` §8.2 lets a server write however it likes — or the
    /// caller's own override, so it is floored before it is used. The assertion is that the
    /// session keeps moving: a spawned panic is not a failed test, and a clock that died is a
    /// session that never renews its holds.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_zero_second_renewal_still_ticks() -> Result<(), Box<dyn StdError>>
    {
        let zero = KeepaliveConfig {
            awareness_renew: Duration::ZERO,
            awareness_expire: Duration::from_secs(30),
        };
        assert_eq!(
            clock_period(&zero),
            Duration::from_millis(1),
            "a zero renewal is floored rather than handed to `interval`"
        );
        let fixture = fixture(zero).await?;
        fixture.relay.open(PATH)?;
        let arrived = Arc::clone(&fixture.arrived);
        assert!(
            held_within(WAIT, || sent_kind(&arrived, 3)).await,
            "the clock task publishes the held set: {} frames arrived",
            fixture.arrived().len()
        );
        Ok(())
    }

    /// §5.1: a base is what every URL this connection builds is made from, and the fragment is
    /// never part of a request — so an address that carries one is not a base.
    #[test]
    fn a_base_with_a_fragment_is_refused() {
        assert_eq!(
            session_base("ws://h:8080/session?room=r-1"),
            Some("ws://h:8080".to_string())
        );
        assert_eq!(
            session_base("ws://h:8080/session#k=cd8&h=cd8"),
            None,
            "the fragment's keys never reach a request"
        );
        assert_eq!(session_base("ws://h:8080#k=cd8"), None);
    }

    /// The two keys a `PROTOCOL.md` §5.1 fragment carries, as a host mints them.
    fn sealed_keys() -> (String, String) {
        (
            encode_key(&RoomKey([7; 32]).0),
            SessionKey::from_seed([3; 32]).public().encode(),
        )
    }

    /// §5.1: a client "MUST NOT log" the invite's fragment, so no refusal may name a whole
    /// link — not the fragment, and not either key it carries.
    fn names_no_key(text: &str, keys: &(String, String)) {
        assert!(!text.contains('#'), "the fragment survived into: {text}");
        assert!(
            !text.contains(&keys.0),
            "the room key survived into: {text}"
        );
        assert!(
            !text.contains(&keys.1),
            "the host key survived into: {text}"
        );
    }

    /// A server that completes the handshake and seats the connection in another room: the
    /// one reply that makes `RelaySession::join` disagree with the address it dialled.
    #[expect(
        clippy::excessive_nesting,
        reason = "a task's handshake is one place: the listener, the accept and the reply belong together"
    )]
    async fn seating_elsewhere(
        room: &str,
    ) -> Result<String, Box<dyn StdError>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let seated = room.to_string();
        tokio::spawn(async move {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = tokio_tungstenite::accept_async(tcp).await
            else {
                return;
            };
            // The client's hello, then the seating reply.
            let _ = socket.next().await;
            let reply = serde_json::json!({
                "v": "selvage/2",
                "event": "room.joined",
                "params": {
                    "room_id": seated,
                    "self": {"peer_id": "p-2", "display_name": "Bob"},
                    "peers": [],
                    "capabilities": [],
                    "keepalive": {
                        "awareness_renew_ms": 15000,
                        "awareness_expire_ms": 30000,
                        "ping_interval_ms": 30000,
                    },
                },
            });
            let _ = socket.send(Message::text(reply.to_string())).await;
            // Hold the socket open so the dial reads the reply before any close.
            let _ = socket.next().await;
        });
        Ok(format!("ws://{addr}"))
    }

    /// R1: a refusal names the address, never the link. §5.1 forbids the fragment leaving a
    /// client, and these are the two refusals a caller reaches with an ordinary paste — a base
    /// that carries a whole link, and a server that seats the connection somewhere else.
    #[tokio::test]
    async fn a_refusal_names_the_address_and_never_the_two_keys()
    -> Result<(), Box<dyn StdError>> {
        let keys = sealed_keys();
        let listing: ListingSource = Arc::new(Vec::new);

        // A base that is a whole link with the fragment in its address: `session_base` refuses
        // it, and the refusal must not repeat what it refused. (A fragment after a `?` is
        // stripped with the query before this point, which is why the shape below carries
        // none.)
        let minted = RelaySession::host(RelayHostOptions {
            base_url: format!("ws://h:8080/session#k={}&h={}", keys.0, keys.1),
            display_name: "Ada".to_string(),
            listing,
            room_key: None,
            host_seed: None,
            store: None,
            client: None,
            keepalive: Some(keepalive()),
        })
        .await;
        let refused =
            minted.err().ok_or("a base with a fragment is refused")?;
        names_no_key(&refused.to_string(), &keys);

        let base = seating_elsewhere("r-OTHER").await?;
        let joined = RelaySession::join(RelayJoinOptions {
            invite: format!(
                "{base}/session?room=r-1&token=t-1#k={}&h={}",
                keys.0, keys.1
            ),
            display_name: "Bob".to_string(),
            declared_role: None,
            client: None,
            keepalive: Some(keepalive()),
        })
        .await;
        let refused = joined.err().ok_or("another room is refused")?;
        assert!(
            refused.to_string().contains("r-1"),
            "the refusal names the address the link gave: {refused}"
        );
        names_no_key(&refused.to_string(), &keys);
        Ok(())
    }
}
