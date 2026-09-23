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

/// The frames a session published while a change was made, in the order it produced them.
type Published = Vec<Vec<u8>>;

/// What one change to a session produced: its value, the frames it published, and the events
/// the change is reported as.
struct Moved<T> {
    value: T,
    frames: Published,
    events: Vec<RelayEvent>,
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
                options.base_url
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
            return Err(Error::Invite(options.invite));
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

    /// Applies a change to the session, then puts what it published on the socket and reports
    /// what the change is.
    fn moving<T>(
        &self,
        change: impl FnOnce(&mut RelayState) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let moved = self.moved(change)?;
        self.relay.emit(moved.events);
        for frame in moved.frames {
            self.send(Outbound::Binary(frame))?;
        }
        Ok(moved.value)
    }

    /// Runs a change under the lock and takes what the session published with it, so that no
    /// frame is put on the socket while the state is held.
    fn moved<T>(
        &self,
        change: impl FnOnce(&mut RelayState) -> Result<T, Error>,
    ) -> Result<Moved<T>, Error> {
        let mut state = self.read();
        if state.destroyed {
            return Err(Error::Closed);
        }
        let value = change(&mut state)?;
        let frames = drain(&mut state);
        let events = report(&mut state);
        drop(state);
        Ok(Moved {
            value,
            frames,
            events,
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
        outgoing: outgoing.clone(),
    });
    let (sink, stream) = socket.split();
    let clock_task = tokio::spawn(clock(Arc::clone(&relay), outgoing.clone()));
    let socket_task = tokio::spawn(socket_loop(
        sink,
        stream,
        Arc::clone(&relay),
        outgoing,
        incoming,
    ));
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
async fn clock(relay: Arc<Relay>, outgoing: mpsc::UnboundedSender<Outbound>) {
    let renew = relay.read().info.keepalive.awareness_renew;
    let mut ticker = interval(renew);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let frames = relay.tick_once();
        if relay.read().destroyed {
            return;
        }
        let sent = frames.into_iter().all(|frame| outgoing.send(frame).is_ok());
        if !sent {
            return;
        }
    }
}

/// The socket task: one inbound frame at a time, and whatever the session produced after it.
#[expect(
    clippy::too_many_arguments,
    reason = "the two halves of the socket, the relay and its two channels are what the task owns"
)]
async fn socket_loop(
    mut sink: Sink,
    mut stream: Stream,
    relay: Arc<Relay>,
    outgoing: mpsc::UnboundedSender<Outbound>,
    mut incoming: mpsc::UnboundedReceiver<Outbound>,
) {
    loop {
        tokio::select! {
            arriving = stream.next() => {
                let Some(message) = arriving else {
                    relay.closed();
                    return;
                };
                let produced = match message {
                    Ok(Message::Binary(frame)) => relay.deliver(&frame),
                    Ok(Message::Text(text)) => relay.hear(&text),
                    Ok(_) => Vec::new(),
                    Err(_) => {
                        relay.closed();
                        return;
                    }
                };
                let sent = produced.into_iter().all(|frame| outgoing.send(frame).is_ok());
                if !sent {
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
    fn tick_once(&self) -> Vec<Outbound> {
        let (frames, events) = self.ticked();
        self.emit(events);
        frames.into_iter().map(Outbound::Binary).collect()
    }

    fn ticked(&self) -> (Vec<Vec<u8>>, Vec<RelayEvent>) {
        let mut state = self.read();
        if state.destroyed || state.ending.is_some() {
            return (Vec::new(), Vec::new());
        }
        let clock = state.start.elapsed();
        state.session.tick(clock);
        let frames = drain(&mut state);
        let events = report(&mut state);
        (frames, events)
    }

    /// One sealed frame, as the relay delivered it.
    fn deliver(&self, frame: &[u8]) -> Vec<Outbound> {
        let (frames, events) = self.delivered(frame);
        self.emit(events);
        frames.into_iter().map(Outbound::Binary).collect()
    }

    fn delivered(&self, frame: &[u8]) -> (Vec<Vec<u8>>, Vec<RelayEvent>) {
        let mut state = self.read();
        let clock = state.start.elapsed();
        let outcome = state.session.deliver(clock, frame);
        // §13.5's content, and only content: a state, a holds set and a closing change no text.
        let content = matches!(outcome, Outcome::Applied { kind: 0 });
        let frames = drain(&mut state);
        let mut events = report(&mut state);
        if content {
            events.push(RelayEvent::Text);
        }
        (frames, events)
    }

    /// One text frame: the roster's events, a fault, or the room's end.
    fn hear(&self, text: &str) -> Vec<Outbound> {
        let Ok(message) = proto::ServerMessage::from_text(text) else {
            return Vec::new();
        };
        if message.event.as_deref() == Some(event::SESSION_ERROR) {
            self.emit(vec![RelayEvent::Failed(self.note_fault(&message))]);
            return Vec::new();
        }
        let (frames, events) = self.heard(&message);
        self.emit(events);
        frames.into_iter().map(Outbound::Binary).collect()
    }

    /// A server-authored frame, folded into the session, with what the two produced.
    fn heard(
        &self,
        message: &proto::ServerMessage,
    ) -> (Vec<Vec<u8>>, Vec<RelayEvent>) {
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
        let frames = drain(&mut state);
        (frames, report(&mut state))
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
fn session_base(url: &str) -> Option<String> {
    let (address, _) = url.split_once('?').unwrap_or((url, ""));
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
        let base =
            session_base(url).ok_or_else(|| Error::Invite(url.to_string()))?;
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
