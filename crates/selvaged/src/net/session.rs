//! The session protocol: the handshake, seating a connection, and the session methods
//! a seated connection answers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Error as WireError;
use tokio_tungstenite::tungstenite::Message;

use selvage_protocol as proto;
use selvage_protocol::{close, code, event, method};

use crate::ServerConfig;
use crate::room::{
    Claim, NewRoom, Outbound, Peer, Queue, Registry, Room, SeatError,
};
use crate::{mint_room_id, mint_token};

use super::{SessionStream, Shared, event_frame};

/// Why a connection was not seated: the error code and the message to send back.
type Refusal = (&'static str, String);

/// The longest grant this server will carry, in paths. A listing is bounded as policy and not
/// as a peer's contract (`PROTOCOL.md` §2.1, §5): the transport already refuses a frame over
/// its own bound, so this is the backstop against a pathologically wide listing that a host
/// should have bounded itself (§5). It sits well above the file count of any working tree a
/// host should be sharing — a checkout that reaches it is one whose build output or dependency
/// tree was not excluded.
pub const MAX_GRANT_PATHS: usize = 100_000;

/// The longest single path in a grant, in bytes. The server does not resolve or normalise
/// paths, so this is the only thing it can say about one; POSIX's own `PATH_MAX` is 4096 bytes
/// for a whole path, and a workspace-relative one is shorter than that (`PROTOCOL.md` §5).
pub const MAX_GRANT_PATH_BYTES: usize = 4096;

/// The most path bytes a grant carries in total. The count and the per-path length cap
/// the shape; this caps the bytes: `100_000` paths of 4096 bytes would otherwise be
/// ~400 MiB of room state, re-serialized on every publish and delivered whole to every
/// late joiner. What a late joiner can be sent is bounded by what could be published.
///
/// 4 MiB clears measured real use with headroom: a 25,000-path working-tree listing is
/// 893,750 path bytes, and 100,000 typical paths are 3,575,000 bytes, both publishing
/// whole; a contaminated tree (123,883 files, ~11.9 MiB of path bytes with build
/// outputs included — a shell count of a working area, not a shape the tests seed) is
/// refused, which is the documented policy — the host excludes what it should not be
/// sharing (`PROTOCOL.md` §5). Shapes measured in
/// `crates/harness/tests/bounds.rs`, clearance pinned in
/// `crates/harness/tests/session.rs`.
pub const MAX_GRANT_BYTES: usize = 4 * 1024 * 1024;

/// Capacity refusals are this server's policy, not the protocol's (`PROTOCOL.md` §2.1,
/// §11): an implementation that needs a code of its own names it in the `x.` namespace
/// rather than inventing a bare name a later version may want. Neither code is in the
/// clients' terminal sets, so a refused client retries with its bounded backoff and
/// then stops — the tolerable shape for a full server.
const SERVER_FULL: &str = "x.server_full";
const ROOM_FULL: &str = "x.room_full";

/// The longest path a `doc.open` or `doc.close` carries, in bytes: the grant's bound,
/// applied to the other path ingestion. A megabyte path was accepted, stored in the
/// room's set and broadcast whole before this bound; §5 allows the same length in both.
pub const MAX_DOC_PATH_BYTES: usize = 4096;

/// One request off the wire: the id to answer and the params to interpret.
struct Request {
    id: u64,
    params: Value,
}

/// A connection that has said hello and is asking for a seat.
pub struct Applicant {
    pub peer_id: String,
    pub join: proto::JoinQuery,
    pub hello: Hello,
    pub queue: Queue,
    /// How to end this connection's task once it is seated: taking it out of the
    /// registry and dropping it completes the task's poison channel.
    pub poison: oneshot::Sender<()>,
}

/// The result of the handshake.
pub struct Hello {
    params: proto::HelloParams,
    /// A connection without a room in the URL is minting one.
    claims_host: bool,
}

/// What seating a newcomer produces: the reply for its own connection, and the frame
/// the peers already in the room get when a host reclaims it — `None` for a mint and
/// for a guest join, which announce nothing beyond `peer.joined`.
type Placement = ((&'static str, proto::SessionParams), Option<Outbound>);

/// Everything that decides where a newcomer is seated.
struct Seating<'a> {
    applicant: &'a Applicant,
    config: &'a ServerConfig,
    role: proto::Role,
    info: &'a proto::PeerInfo,
    /// `None` means "mint a new room".
    room_id: Option<&'a str>,
    peer: Peer,
}

/// Waits for the `session.hello` envelope that opens every session.
///
/// # Errors
///
/// Returns the refusal to send back when the first frame is not a compatible
/// `session.hello`.
pub async fn handshake(
    stream: &mut SessionStream,
    claims_host: bool,
    hello_timeout: Duration,
) -> Result<Hello, Refusal> {
    let text = match timeout(hello_timeout, next_text(stream)).await {
        Ok(Ok(text)) => text,
        Ok(Err(reason)) => return Err((code::BAD_MESSAGE, reason)),
        Err(_) => {
            return Err((
                code::HELLO_REQUIRED,
                "session.hello was not sent in time".to_string(),
            ));
        }
    };
    let msg: proto::ClientMessage =
        serde_json::from_str(&text).map_err(|e| {
            envelope_refusal("first message is not a session envelope", &e)
        })?;
    if msg.id.is_none() {
        return Err((code::BAD_MESSAGE, "a request needs an id".to_string()));
    }
    if msg.method != method::SESSION_HELLO {
        return Err((
            code::HELLO_REQUIRED,
            format!(
                "first method must be {}, got {}",
                method::SESSION_HELLO,
                msg.method
            ),
        ));
    }
    if !proto::is_compatible(&msg.v) {
        return Err((
            code::UNSUPPORTED_VERSION,
            format!("unsupported wire version {}", msg.v),
        ));
    }
    let params: proto::HelloParams = serde_json::from_value(msg.params)
        .map_err(|e| envelope_refusal("bad session.hello params", &e))?;
    if params.display_name.trim().is_empty() {
        return Err((
            code::BAD_PARAMS,
            "session.hello requires a display_name".to_string(),
        ));
    }
    if proto::display_name_over_limit(&params.display_name) {
        return Err((
            code::BAD_PARAMS,
            format!(
                "display_name is longer than {} UTF-16 code units",
                proto::DISPLAY_NAME_MAX_UTF16
            ),
        ));
    }
    Ok(Hello {
        params,
        claims_host,
    })
}

/// The next text frame on a connection that is still in the handshake.
async fn next_text(stream: &mut SessionStream) -> Result<String, String> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
            Some(Ok(Message::Binary(_))) => {
                return Err(
                    "a binary frame arrived before session.hello".to_string()
                );
            }
            Some(Ok(Message::Close(_))) | None => {
                return Err("connection closed during handshake".to_string());
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.to_string()),
        }
    }
}

fn envelope_refusal(what: &str, error: &serde_json::Error) -> Refusal {
    (code::BAD_MESSAGE, format!("{what}: {error}"))
}

fn refusal_for(error: SeatError, room_id: &str) -> Refusal {
    match error {
        SeatError::Unknown => {
            (code::ROOM_UNKNOWN, format!("no such room: {room_id}"))
        }
        SeatError::TokenMismatch => {
            (code::TOKEN_INVALID, "invalid room token".to_string())
        }
        SeatError::HostPresent => (
            code::HOST_PRESENT,
            "the room already has a host".to_string(),
        ),
        SeatError::RoomFull => {
            (ROOM_FULL, "the room seats no more peers".to_string())
        }
    }
}

/// The result of a document method: the room's open-document set after the change.
fn doc_set(documents: &[String]) -> Value {
    serde_json::json!({ "documents": documents })
}

/// The `path` of a `doc.open` or `doc.close` request. The two params are the same shape
/// (§5) — a path and nothing else — so one type reads either and the rule that a path is
/// non-blank lives here for both. Params that do not parse and a path that is empty or
/// all whitespace are both `bad_params`.
fn document_path(raw: Value) -> Result<String, Refusal> {
    let params = serde_json::from_value::<proto::DocOpenParams>(raw)
        .map_err(|e| (code::BAD_PARAMS, e.to_string()))?;
    if params.path.trim().is_empty() {
        return Err((code::BAD_PARAMS, "path is required".to_string()));
    }
    if params.path.len() > MAX_DOC_PATH_BYTES {
        return Err((
            code::BAD_PARAMS,
            format!("a document path is at most {MAX_DOC_PATH_BYTES} bytes"),
        ));
    }
    Ok(params.path)
}

/// The `display_name` of a `session.rename` request, which carries the handshake's bound
/// (`PROTOCOL.md` §5). Params that do not parse and a name that is blank or over-long are
/// all `bad_params`; unlike the handshake the refusal is a response, not a close.
fn rename_name(raw: Value) -> Result<String, Refusal> {
    let params = serde_json::from_value::<proto::RenameParams>(raw)
        .map_err(|e| (code::BAD_PARAMS, e.to_string()))?;
    if params.display_name.trim().is_empty() {
        return Err((code::BAD_PARAMS, "display_name is required".to_string()));
    }
    if proto::display_name_over_limit(&params.display_name) {
        return Err((
            code::BAD_PARAMS,
            format!(
                "display_name is longer than {} UTF-16 code units",
                proto::DISPLAY_NAME_MAX_UTF16
            ),
        ));
    }
    Ok(params.display_name)
}

/// The `paths` of a `doc.grant` request (`PROTOCOL.md` §5): the host's whole listing, in the
/// order it wrote it. The shape is a path's (`document_path`), an empty array is a listing
/// that grants nothing and not a fault, and the listing is rejected `bad_params` with the
/// connection open when it is over this server's bounds. The order is never touched: the
/// server carries what it was given and does not sort, deduplicate or normalise it.
fn grant_paths(raw: Value) -> Result<Vec<String>, Refusal> {
    let params = serde_json::from_value::<proto::GrantParams>(raw)
        .map_err(|e| (code::BAD_PARAMS, e.to_string()))?;
    if params.paths.len() > MAX_GRANT_PATHS {
        return Err((
            code::BAD_PARAMS,
            format!("a grant is at most {MAX_GRANT_PATHS} paths"),
        ));
    }
    for path in &params.paths {
        if path.trim().is_empty() {
            return Err((
                code::BAD_PARAMS,
                "a grant path is required".to_string(),
            ));
        }
        if path.len() > MAX_GRANT_PATH_BYTES {
            return Err((
                code::BAD_PARAMS,
                format!("a grant path is at most {MAX_GRANT_PATH_BYTES} bytes"),
            ));
        }
    }
    let total: usize = params.paths.iter().map(String::len).sum();
    if total > MAX_GRANT_BYTES {
        return Err((
            code::BAD_PARAMS,
            format!("a grant is at most {MAX_GRANT_BYTES} path bytes in total"),
        ));
    }
    Ok(params.paths)
}

/// A peer taken out of the room under the lock: what its announcements need after it.
struct DetachedPeer {
    peer_id: String,
    was_host: bool,
    generation: u64,
    /// Dropping this ends the connection's task. A removed peer is gone either way;
    /// ending its task is what closes its socket.
    poison: Option<oneshot::Sender<()>>,
}

/// Removes a peer under the lock: its seat, its claims and its task handle. The room
/// outlives the announcement, so the frames the departure needs are built by the
/// caller once the lock is dropped.
fn detach_locked(
    registry: &mut Registry,
    room_id: &str,
    peer_id: &str,
) -> Option<DetachedPeer> {
    let detach = registry.detach(room_id, peer_id)?;
    let poison = registry.take_task(peer_id);
    Some(DetachedPeer {
        peer_id: peer_id.to_string(),
        was_host: detach.was_host,
        generation: detach.generation,
        poison,
    })
}

/// The `peer.left` a departure is announced with.
fn peer_left_frame(peer_id: &str) -> Option<Outbound> {
    event_frame(event::PEER_LEFT, serde_json::json!({ "peer_id": peer_id }))
}

/// The `host.detached` a host's departure is announced with.
fn host_detached_frame(grace_ms: u64) -> Option<Outbound> {
    event_frame(
        event::HOST_DETACHED,
        serde_json::json!({ "grace_ms": grace_ms }),
    )
}

/// The room grace period in whole milliseconds, as `host.detached` carries it.
fn grace_ms(config: &ServerConfig) -> u64 {
    u64::try_from(config.room_grace.as_millis()).unwrap_or(u64::MAX)
}

/// Sends a frame to a room's peers, minus one. A peer whose queue is full is not kept
/// and told nothing: it is removed, the room is told `peer.left`, and its task is
/// stopped. Frames the departures themselves need join the same loop, so a burst of
/// slow peers drains without recursing.
#[expect(
    clippy::too_many_arguments,
    reason = "a delivery names its server, room, exclusion and frame; a struct would hide one call site's meaning"
)]
async fn deliver(
    shared: &Shared,
    room_id: &str,
    except: Option<&str>,
    frame: Option<Outbound>,
) {
    let mut pending: Vec<(Option<String>, Outbound)> = Vec::new();
    if let Some(out) = frame {
        pending.push((except.map(str::to_string), out));
    }
    while let Some((skip, out)) = pending.pop() {
        let slow: Vec<String> = {
            let guard = shared.registry.lock().await;
            guard
                .room(room_id)
                .map(|room| room.broadcast(skip.as_deref(), &out))
                .unwrap_or_default()
        };
        for peer_id in slow {
            eject_into(shared, room_id, &peer_id, &mut pending).await;
        }
    }
}

/// Removes one slow peer found by [`deliver`], queuing the frames its departure needs
/// with the ones still unsent. Dropping its poison ends its task.
#[expect(
    clippy::too_many_arguments,
    reason = "an ejection names its server, room, peer and the queue it appends to"
)]
async fn eject_into(
    shared: &Shared,
    room_id: &str,
    peer_id: &str,
    pending: &mut Vec<(Option<String>, Outbound)>,
) {
    let detached = {
        let mut guard = shared.registry.lock().await;
        detach_locked(&mut guard, room_id, peer_id)
    };
    let Some(removed) = detached else {
        return;
    };
    // Queued host-first: `deliver` drains LIFO, so the room hears `peer.left`
    // before `host.detached`, like a clean leave in `remove_peer`.
    if removed.was_host {
        let grace = grace_ms(&shared.config);
        if let Some(gone) = host_detached_frame(grace) {
            pending.push((None, gone));
        }
        reap_later(shared.clone(), room_id.to_string(), removed.generation);
    }
    if let Some(left) = peer_left_frame(&removed.peer_id) {
        pending.push((None, left));
    }
    drop(removed.poison);
}

/// Detaches a peer the server stops serving, tells the room, arms the grace period if
/// the host just left, and ends the connection's task. A clean leave and a slow-peer
/// removal end the same way: the seat is gone, so there is nothing left to serve.
async fn remove_peer(shared: &Shared, room_id: &str, peer_id: &str) {
    let detached = {
        let mut guard = shared.registry.lock().await;
        detach_locked(&mut guard, room_id, peer_id)
    };
    let Some(removed) = detached else {
        return;
    };
    deliver(shared, room_id, None, peer_left_frame(&removed.peer_id)).await;
    if removed.was_host {
        let grace = grace_ms(&shared.config);
        deliver(shared, room_id, None, host_detached_frame(grace)).await;
        reap_later(shared.clone(), room_id.to_string(), removed.generation);
    }
    drop(removed.poison);
}

fn capabilities() -> Vec<String> {
    proto::CAPABILITIES
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// The `doc.granted` a connection seated into a room receives right after its `room.joined`,
/// or `None` when the room has no grant (`PROTOCOL.md` §6.3). It is a snapshot rather than a
/// delta — the room already holds the listing — and a freshly minted room's grant is always
/// empty, so a mint produces no frame.
fn join_grant(room: Option<&Room>) -> Option<Outbound> {
    let paths = room?.grant();
    if paths.is_empty() {
        return None;
    }
    event_frame(event::DOC_GRANTED, serde_json::json!({ "paths": paths }))
}

impl Applicant {
    /// The role this connection is seated as. A connection that arrives without a room
    /// mints that room, so it is its host whatever `session.hello` claims: honouring a
    /// claimed `guest` would create a room with no host, whose real host would then be
    /// refused it.
    const fn role(&self) -> proto::Role {
        if self.hello.claims_host {
            return proto::Role::Host;
        }
        match self.hello.params.role {
            Some(role) => role,
            None => proto::Role::Guest,
        }
    }

    fn info(&self, role: proto::Role) -> proto::PeerInfo {
        proto::PeerInfo {
            peer_id: self.peer_id.clone(),
            display_name: self.hello.params.display_name.clone(),
            role,
            awareness_client_id: self.hello.params.awareness_client_id,
        }
    }

    /// Mints a room or admits this connection to an existing one, then queues the
    /// handshake response. The response is queued while the registry lock is held, so
    /// it is the first frame on the connection's channel; the announcements to the
    /// peers already in the room wait until the lock is dropped, since a slow one of
    /// them is removed rather than written to.
    ///
    /// # Errors
    ///
    /// Returns the refusal to send back when the room is unknown, the token is wrong,
    /// or the host role is taken.
    pub async fn seat(self, shared: &Shared) -> Result<Session, Refusal> {
        let role = self.role();
        let info = self.info(role);
        let seating = Seating {
            applicant: &self,
            config: &shared.config,
            role,
            info: &info,
            room_id: self.join.room.as_deref(),
            peer: Peer::new(info.clone(), self.queue.clone()),
        };
        let mut guard = shared.registry.lock().await;
        let ((event_name, params), attached) = seating.place(&mut guard)?;
        guard.set_task(&self.peer_id, self.poison);

        let body = serde_json::to_value(&params).map_err(|e| bad_body(&e))?;
        let response = event_frame(event_name, body);
        if let Some(frame) = response {
            let _ = self.queue.try_queue(frame);
        }
        // A joining connection learns the room's grant straight after its `room.joined`, so
        // it needs no round trip and `room.joined` needs no fifth member. It is queued after
        // the reply, so the reply is still this connection's first frame.
        let granted = join_grant(guard.room(&params.room_id));
        if let Some(frame) = granted {
            let _ = self.queue.try_queue(frame);
        }
        // Late arrivals must be announced to the peers already in the room.
        let joined = if params.token.is_none() {
            event_frame(event::PEER_JOINED, serde_json::json!({ "peer": info }))
        } else {
            None
        };
        let room_id = params.room_id.clone();
        let peer_id = self.peer_id.clone();
        let queue = self.queue.clone();
        drop(guard);
        deliver(shared, &room_id, Some(&peer_id), attached).await;
        deliver(shared, &room_id, Some(&peer_id), joined).await;
        Ok(Session {
            peer_id: self.peer_id,
            room_id: params.room_id,
            queue,
            poisoned: AtomicBool::new(false),
        })
    }
}

fn bad_body(error: &serde_json::Error) -> Refusal {
    (code::BAD_MESSAGE, error.to_string())
}

impl Seating<'_> {
    /// Seats the newcomer: its reply, and the reclaim frame for the room if any.
    fn place(self, registry: &mut Registry) -> Result<Placement, Refusal> {
        match self.room_id {
            None => self.mint(registry),
            Some(room_id) => self.admit(registry, room_id),
        }
    }

    fn mint(self, registry: &mut Registry) -> Result<Placement, Refusal> {
        let room_id = mint_room_id();
        let token = mint_token();
        if !registry.create(
            NewRoom {
                id: room_id.clone(),
                token: token.clone(),
                keepalive: self.config.keepalive,
            },
            self.peer,
            self.config.max_rooms,
        ) {
            return Err((
                SERVER_FULL,
                format!(
                    "the server holds at most {} rooms",
                    self.config.max_rooms
                ),
            ));
        }
        Ok((
            (
                event::ROOM_CREATED,
                proto::SessionParams {
                    room_id,
                    token: Some(token),
                    self_peer: self.info.clone(),
                    peers: Vec::new(),
                    documents: Vec::new(),
                    capabilities: capabilities(),
                    keepalive: self.config.keepalive,
                },
            ),
            None,
        ))
    }

    fn admit(
        self,
        registry: &mut Registry,
        room_id: &str,
    ) -> Result<Placement, Refusal> {
        let host_was_present =
            registry.room(room_id).is_some_and(Room::host_present);
        registry
            .admit(
                Claim {
                    room_id,
                    token: self.applicant.join.token.as_deref(),
                    role: self.role,
                },
                self.peer,
                self.config.max_peers_per_room,
            )
            .map_err(|error| match error {
                SeatError::RoomFull => (
                    ROOM_FULL,
                    format!(
                        "the room seats at most {} peers",
                        self.config.max_peers_per_room
                    ),
                ),
                SeatError::Unknown
                | SeatError::TokenMismatch
                | SeatError::HostPresent => refusal_for(error, room_id),
            })?;
        // `host.attached` is a host reclaiming the room (§6, §9.1): a guest joining
        // while the room is between hosts is a `peer.joined` and nothing more.
        let attached = if !host_was_present && self.role == proto::Role::Host {
            event_frame(
                event::HOST_ATTACHED,
                serde_json::json!({ "peer": self.info }),
            )
        } else {
            None
        };
        let room = registry
            .room(room_id)
            .ok_or_else(|| (code::ROOM_GONE, "the room is gone".to_string()))?;
        Ok((
            (
                event::ROOM_JOINED,
                proto::SessionParams {
                    room_id: room.id.clone(),
                    token: None,
                    self_peer: self.info.clone(),
                    peers: room.peers_except(&self.applicant.peer_id),
                    documents: room.documents().to_vec(),
                    capabilities: capabilities(),
                    keepalive: room.keepalive,
                },
            ),
            attached,
        ))
    }
}

/// A seated connection: its identity and the queue its frames leave through.
pub struct Session {
    peer_id: String,
    room_id: String,
    queue: Queue,
    /// Set when a reply found the queue full: the peer stopped reading. `handle_text`
    /// ends the session on the same frame the reply was dropped on.
    poisoned: AtomicBool,
}

impl Session {
    /// Handles one inbound frame. Returns `false` when the session must stop.
    pub async fn handle_frame(
        &self,
        incoming: Option<Result<Message, WireError>>,
        shared: &Shared,
    ) -> bool {
        match incoming {
            Some(Ok(Message::Binary(frame))) => {
                self.relay(frame.to_vec(), shared).await;
            }
            Some(Ok(Message::Text(text))) => {
                self.handle_text(&text, shared).await;
            }
            Some(Ok(_)) => {}
            Some(Err(_)) | None => return false,
        }
        true
    }

    /// Relays a document or awareness payload to the rest of the room, untouched.
    async fn relay(&self, frame: Vec<u8>, shared: &Shared) {
        deliver(
            shared,
            &self.room_id,
            Some(&self.peer_id),
            Some(Outbound::Binary(frame)),
        )
        .await;
    }

    /// Handles one JSON envelope: a session method, or an error for anything else.
    /// A reply the queue would not take means the peer stopped reading: the session
    /// ends on this same frame rather than stacking more behind the unread frames,
    /// whichever path marked it. The room learns of it as `peer.left`, like any drop.
    async fn handle_text(&self, text: &str, shared: &Shared) {
        self.dispatch_text(text, shared).await;
        if self.poisoned.swap(false, Ordering::Relaxed) {
            remove_peer(shared, &self.room_id, &self.peer_id).await;
        }
    }

    /// Runs one JSON envelope, queueing its reply. Marks the session when the queue
    /// will not take the reply; the caller ends it on the same frame.
    async fn dispatch_text(&self, text: &str, shared: &Shared) {
        let msg = match serde_json::from_str::<proto::ClientMessage>(text) {
            Ok(msg) => msg,
            Err(e) => return self.alert(code::BAD_MESSAGE, e.to_string()),
        };
        let Some(id) = msg.id else {
            self.alert(code::BAD_MESSAGE, "a request needs an id");
            return;
        };
        if !proto::is_compatible(&msg.v) {
            self.expire(id, &msg.v);
            return;
        }
        let proto::ClientMessage { method, params, .. } = msg;
        let request = Request { id, params };
        match method.as_str() {
            method::DOC_OPEN => self.open_document(request, shared).await,
            method::DOC_CLOSE => self.close_document(request, shared).await,
            method::DOC_GRANT => self.grant(request, shared).await,
            method::SESSION_RENAME => self.rename(request, shared).await,
            method::SESSION_HELLO => self.reply(&proto::ServerMessage::error(
                id,
                code::ALREADY_SEATED,
                "this connection already completed the handshake",
            )),
            other => self.reply(&proto::ServerMessage::error(
                id,
                code::UNKNOWN_METHOD,
                format!("no such method: {other}"),
            )),
        }
    }

    /// `doc.open`: declares a path open for this peer and announces the room's set.
    async fn open_document(&self, request: Request, shared: &Shared) {
        let path = match document_path(request.params) {
            Ok(path) => path,
            Err((code, message)) => {
                return self.reply(&proto::ServerMessage::error(
                    request.id, code, message,
                ));
            }
        };
        let Some(documents) = self.hold(shared, &path).await else {
            return self.reply(&proto::ServerMessage::error(
                request.id,
                ROOM_FULL,
                format!(
                    "the room holds at most {} open documents",
                    shared.config.max_documents_per_room
                ),
            ));
        };
        self.reply(&proto::ServerMessage::response(
            request.id,
            doc_set(&documents),
        ));
        let announced = serde_json::json!({
            "peer_id": self.peer_id,
            "path": path,
            "documents": documents,
        });
        self.announce_documents(
            shared,
            event_frame(event::DOC_OPENED, announced),
        )
        .await;
    }

    /// `doc.close`: releases this peer's hold on a path and announces the room's set.
    async fn close_document(&self, request: Request, shared: &Shared) {
        let path = match document_path(request.params) {
            Ok(path) => path,
            Err((code, message)) => {
                return self.reply(&proto::ServerMessage::error(
                    request.id, code, message,
                ));
            }
        };
        let documents = self.release(shared, &path).await;
        self.reply(&proto::ServerMessage::response(
            request.id,
            doc_set(&documents),
        ));
        let announced = serde_json::json!({
            "peer_id": self.peer_id,
            "path": path,
            "documents": documents,
        });
        self.announce_documents(
            shared,
            event_frame(event::DOC_CLOSED, announced),
        )
        .await;
    }

    /// `session.rename` (`PROTOCOL.md` §5): changes this connection's own display name and
    /// tells the whole room, the one that asked included. A malformed, blank or over-long
    /// name is the error response `bad_params`, and the connection stays open — a seated
    /// request fault is not a close (§9.2).
    async fn rename(&self, request: Request, shared: &Shared) {
        let display_name = match rename_name(request.params) {
            Ok(name) => name,
            Err((code, message)) => {
                return self.reply(&proto::ServerMessage::error(
                    request.id, code, message,
                ));
            }
        };
        let renamed = self.set_display_name(shared, &display_name).await;
        // The response is queued before the event, as a `doc.open` result precedes its
        // `doc.opened`: both leave on this connection's channel, so the bytes keep order.
        self.reply(&proto::ServerMessage::response(
            request.id,
            serde_json::json!({}),
        ));
        let Some(peer) = renamed else {
            // The room or the peer is already gone. The reply still stands; a rename on a
            // peer that has just left is not a fault, and there is nobody to announce to.
            return;
        };
        let announced = serde_json::json!({
            "display_name": peer.display_name,
            "peer_id": peer.peer_id,
        });
        self.announce_documents(
            shared,
            event_frame(event::PEER_RENAMED, announced),
        )
        .await;
    }

    /// Records this peer's new name, returning the record as it now reads. `None` when the
    /// room or the peer is gone.
    async fn set_display_name(
        &self,
        shared: &Shared,
        display_name: &str,
    ) -> Option<proto::PeerInfo> {
        let mut guard = shared.registry.lock().await;
        let room = guard.room_mut(&self.room_id)?;
        room.rename_peer(&self.peer_id, display_name)
    }

    /// `doc.grant` (`PROTOCOL.md` §5): replaces the room's grant with the host's listing and
    /// tells the whole room, the host included. Only the connection the server holds as the
    /// room's host may publish one; §11 has no code for "not permitted", so anyone else is
    /// refused `bad_params`, which is the code a malformed request gets. A malformed or
    /// over-long listing is refused the same way and changes nothing.
    async fn grant(&self, request: Request, shared: &Shared) {
        let paths = match grant_paths(request.params) {
            Ok(paths) => paths,
            Err((code, message)) => {
                return self.reply(&proto::ServerMessage::error(
                    request.id, code, message,
                ));
            }
        };
        if !self.store_grant(shared, paths.clone()).await {
            return self.reply(&proto::ServerMessage::error(
                request.id,
                code::BAD_PARAMS,
                "only the room's host may publish the grant".to_string(),
            ));
        }
        // The response is queued before the event, as a `doc.open` result precedes its
        // `doc.opened`: both leave on this connection's channel, so the bytes keep order.
        self.reply(&proto::ServerMessage::response(
            request.id,
            serde_json::json!({}),
        ));
        let announced = serde_json::json!({ "paths": paths });
        self.announce_documents(
            shared,
            event_frame(event::DOC_GRANTED, announced),
        )
        .await;
    }

    /// Stores the room's new grant if this connection is its host, returning whether it was
    /// stored. `false` means the room is gone or this connection is not its host, and then
    /// nothing was written.
    async fn store_grant(&self, shared: &Shared, paths: Vec<String>) -> bool {
        let mut guard = shared.registry.lock().await;
        let Some(room) = guard.room_mut(&self.room_id) else {
            return false;
        };
        if !room.is_host(&self.peer_id) {
            return false;
        }
        room.set_grant(paths);
        true
    }

    /// Records this peer's hold on a path, returning the room's set afterwards — or
    /// `None` when the set is at its cap and the path is not in it.
    async fn hold(&self, shared: &Shared, path: &str) -> Option<Vec<String>> {
        let mut guard = shared.registry.lock().await;
        let Some(room) = guard.room_mut(&self.room_id) else {
            return Some(Vec::new());
        };
        room.open_document(
            &self.peer_id,
            path,
            shared.config.max_documents_per_room,
        )
        .map(|_| room.documents().to_vec())
    }

    /// Releases this peer's hold on a path, returning the room's set afterwards.
    async fn release(&self, shared: &Shared, path: &str) -> Vec<String> {
        let mut guard = shared.registry.lock().await;
        let Some(room) = guard.room_mut(&self.room_id) else {
            return Vec::new();
        };
        room.close_document(&self.peer_id, path);
        room.documents().to_vec()
    }

    /// Tells a connection its wire version is not this server's, then closes it. When
    /// even the close cannot be queued the session is already over: the poison the
    /// reply left ends it on the same frame at the end of `handle_text`.
    fn expire(&self, id: u64, version: &str) {
        self.reply(&proto::ServerMessage::error(
            id,
            code::UNSUPPORTED_VERSION,
            format!("unsupported wire version {version}"),
        ));
        let _ = self.queue.try_queue(Outbound::Close(
            close::UNSUPPORTED_VERSION,
            "version".to_string(),
        ));
    }

    /// Queues a response or an error for the connection. A full queue means the peer
    /// stopped reading: the frame is dropped and the session is marked. `handle_text`
    /// ends a marked session on this same frame, whichever path marked it. The
    /// room is told `peer.left`; nothing unsent is kept for the peer, and no reply is
    /// presented as delivered that was not queued.
    fn reply(&self, msg: &proto::ServerMessage) {
        let Some(frame) = super::frame_of(msg) else {
            return;
        };
        if !self.queue.try_queue(frame) {
            self.poisoned.store(true, Ordering::Relaxed);
        }
    }

    /// Sends a `session.error` event: a failure that cannot be attributed to a request.
    fn alert(&self, code: &str, message: impl Into<String>) {
        let msg = proto::ServerMessage::event(
            event::SESSION_ERROR,
            serde_json::json!({ "code": code, "message": message.into() }),
        );
        self.reply(&msg);
    }

    /// Sends a room-wide event to every peer, the one that made the change included: the
    /// open-document set is the room's, so everyone has to hold the same view of it, and a
    /// rename is announced to the mover as well as to the rest.
    async fn announce_documents(
        &self,
        shared: &Shared,
        frame: Option<Outbound>,
    ) {
        deliver(shared, &self.room_id, None, frame).await;
    }

    /// Detaches this connection, tells the room, and arms the room's grace period if
    /// the host just left.
    pub async fn leave(&self, shared: &Shared) {
        remove_peer(shared, &self.room_id, &self.peer_id).await;
    }
}

/// Waits out the host grace period, then destroys the room if the host stayed away.
fn reap_later(shared: Shared, room_id: String, generation: u64) {
    tokio::spawn(async move {
        sleep(shared.config.room_grace).await;
        let mut guard = shared.registry.lock().await;
        let peers = guard.reap_if_host_absent(&room_id, generation);
        for peer in &peers {
            drop(guard.take_task(&peer.info.peer_id));
        }
        drop(guard);
        tell_room_gone(&room_id, peers);
    });
}

/// Tells the peers left behind that the room is gone, then closes their connections.
/// A queue that will not take even the close is not kept for the peer: its task was
/// already ended, and the socket goes with it.
fn tell_room_gone(room_id: &str, peers: Vec<Peer>) {
    let frame = event_frame(
        event::ROOM_GONE,
        serde_json::json!({ "room_id": room_id, "reason": "host did not return" }),
    );
    for peer in peers {
        if let Some(out) = frame.clone() {
            let _ = peer.send(out);
        }
        let _ = peer
            .send(Outbound::Close(close::ROOM_GONE, "room gone".to_string()));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use selvage_protocol::{Keepalive, PeerInfo, Role};
    use tokio::sync::Mutex;

    use super::*;
    use crate::room::{MAX_QUEUE_FRAMES, peer_channel};

    /// Fills a fresh queue exactly: a full queue's worth of frames fits, so the
    /// session holding it is alive — and the next frame past it is refused.
    fn fill(queue: &Queue) {
        for _ in 0..MAX_QUEUE_FRAMES {
            assert!(queue.try_queue(Outbound::Text("x".to_string())));
        }
    }

    /// A reply the queue will not take ends the session on the same frame even when
    /// the poison came from an error path: with the queue exactly full, one malformed
    /// frame detaches the peer without any further inbound.
    #[tokio::test]
    async fn a_rejected_alert_ends_the_session_on_the_same_frame() {
        let mut registry = Registry::default();
        let (host, _rx) = peer_channel(PeerInfo {
            peer_id: "p-host".to_string(),
            display_name: "Ada".to_string(),
            role: Role::Host,
            awareness_client_id: None,
        });
        registry.create(
            NewRoom {
                id: "r-1".to_string(),
                token: "t".to_string(),
                keepalive: Keepalive::default(),
            },
            host,
            usize::MAX,
        );
        let (guest, _rx) = peer_channel(PeerInfo {
            peer_id: "p-slow".to_string(),
            display_name: "Bob".to_string(),
            role: Role::Guest,
            awareness_client_id: None,
        });
        let queue = guest.queue.clone();
        registry
            .admit(
                Claim {
                    room_id: "r-1",
                    token: Some("t"),
                    role: Role::Guest,
                },
                guest,
                usize::MAX,
            )
            .expect("the guest seats");
        // Exactly full, so the session is alive — and the alert for one malformed
        // frame is the one past it.
        fill(&queue);
        let live = Arc::new(Mutex::new(registry));
        let shared = Shared::new(
            ServerConfig {
                room_grace: Duration::from_millis(1),
                ..ServerConfig::default()
            },
            Arc::clone(&live),
        );
        let session = Session {
            peer_id: "p-slow".to_string(),
            room_id: "r-1".to_string(),
            queue,
            poisoned: AtomicBool::new(false),
        };
        session.handle_text("{not json", &shared).await;
        let guard = live.lock().await;
        let room = guard.room("r-1").expect("the room survives");
        assert!(
            !room.peers.contains_key("p-slow"),
            "the slow peer is detached"
        );
        assert!(room.peers.contains_key("p-host"), "the host is undisturbed");
    }
}
