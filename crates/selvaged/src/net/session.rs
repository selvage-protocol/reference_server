//! The session protocol: the handshake, seating a connection, and the session methods
//! a seated connection answers.

use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Error as WireError;
use tokio_tungstenite::tungstenite::Message;

use selvage_protocol as proto;
use selvage_protocol::{Keepalive, code, event, method};

use crate::ServerConfig;
use crate::room::{
    Claim, NewRoom, Outbound, Peer, Queue, Registry, SeatError, send_all,
};
use crate::{mint_room_id, mint_token};

use super::{
    RATE_LIMITED, SessionStream, Shared, budget_message, event_frame,
    payload_len,
};
use crate::budget::InboundBudget;

/// Why a connection was not seated: the error code and the message to send back.
type Refusal = (&'static str, String);

/// Capacity refusals are this server's policy, not the protocol's (`PROTOCOL.md` §2.1,
/// §11): an implementation that needs a code of its own names it in the `x.` namespace
/// rather than inventing a bare name a later version may want. Neither code is in the
/// clients' terminal sets, so a refused client retries with its bounded backoff and
/// then stops — the tolerable shape for a full server.
const SERVER_FULL: &str = "x.server_full";
const ROOM_FULL: &str = "x.room_full";

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
}

/// What seating a newcomer produces: the reply's event name and params, whether the
/// room was minted, and the room id. Plain data only: the reply is serialized by the
/// caller while it still holds the registry lock, because the order of the newcomer's
/// first frames is part of what seating decides (see `seat`).
struct Placement {
    event: &'static str,
    body: Value,
    room_id: String,
    minted: bool,
}

/// Everything that decides where a newcomer is seated.
struct Seating<'a> {
    applicant: &'a Applicant,
    config: &'a ServerConfig,
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
    config: &ServerConfig,
    budget: &mut InboundBudget,
) -> Result<Hello, Refusal> {
    let text =
        match timeout(config.hello_timeout, next_text(stream, config, budget))
            .await
        {
            Ok(Ok(text)) => text,
            Ok(Err(refusal)) => return Err(refusal),
            Err(_) => {
                return Err((
                    code::HELLO_REQUIRED,
                    "session.hello was not sent in time".to_string(),
                ));
            }
        };
    if text.len() > config.max_envelope_bytes {
        return Err(envelope_too_large(text.len(), config.max_envelope_bytes));
    }
    let msg: proto::ClientMessage = proto::ClientMessage::from_text(&text)
        .map_err(|e| {
            envelope_refusal("first message is not a session envelope", &e)
        })?;
    if msg.id.is_none() {
        return Err((code::BAD_MESSAGE, "a request needs an id".to_string()));
    }
    // The version before the method, as on a seated connection and as §11 orders the
    // checks: a first frame that is both a non-hello method and another version was
    // answered `hello_required`, which tells the client the wrong thing about why it was
    // refused. The envelope and the id are judged first, the method after the version.
    if !proto::is_compatible(&msg.v) {
        return Err((
            code::BAD_MESSAGE,
            format!("unsupported wire version {}", msg.v),
        ));
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
    let mut params: proto::HelloParams = serde_json::from_value(msg.params)
        .map_err(|e| envelope_refusal("bad session.hello params", &e))?;
    validate_name(&mut params.display_name)?;
    Ok(Hello { params })
}

/// The one place a display name is judged, for both the handshake and a rename
/// (`PROTOCOL.md` §5): non-blank, no control character, at most 32 UTF-16 code units,
/// judged on the received value before the trim.
fn validate_name(display_name: &mut String) -> Result<(), Refusal> {
    if proto::has_control_characters(display_name) {
        return Err((
            code::BAD_PARAMS,
            "display_name contains control characters".to_string(),
        ));
    }
    *display_name = display_name.trim().to_string();
    if display_name.is_empty() {
        return Err((
            code::BAD_PARAMS,
            "a display_name is required".to_string(),
        ));
    }
    if proto::display_name_over_limit(display_name) {
        return Err((
            code::BAD_PARAMS,
            format!(
                "display_name is longer than {} UTF-16 code units",
                proto::DISPLAY_NAME_MAX_UTF16
            ),
        ));
    }
    Ok(())
}

/// The next text frame on a connection that is still in the handshake. Every frame read
/// is charged to the connection's budget first: before seating is when a peer can send
/// frames nobody answers, which is the cheapest flood to mount, and §11's shape for a
/// fault there is a refusal and a close.
async fn next_text(
    stream: &mut SessionStream,
    config: &ServerConfig,
    budget: &mut InboundBudget,
) -> Result<String, Refusal> {
    loop {
        let frame = match stream.next().await {
            Some(Ok(frame)) => frame,
            Some(Err(e)) => return Err((code::BAD_MESSAGE, e.to_string())),
            None => {
                return Err((
                    code::BAD_MESSAGE,
                    "connection closed during handshake".to_string(),
                ));
            }
        };
        if !budget.try_take(payload_len(&frame)) {
            return Err((RATE_LIMITED, budget_message(config)));
        }
        match frame {
            Message::Text(text) => return Ok(text.to_string()),
            Message::Binary(_) => {
                return Err((
                    code::BAD_MESSAGE,
                    "a binary frame arrived before session.hello".to_string(),
                ));
            }
            Message::Close(_) => {
                return Err((
                    code::BAD_MESSAGE,
                    "connection closed during handshake".to_string(),
                ));
            }
            // A control frame is charged and otherwise ignored, as on a seated
            // connection: an unseated peer that floods them is held to the same budget.
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

fn envelope_refusal(what: &str, error: &serde_json::Error) -> Refusal {
    (code::BAD_MESSAGE, format!("{what}: {error}"))
}

/// The refusal for a text frame longer than this server parses, naming both the bound
/// and the frame so an operator can see which of the two is wrong.
///
/// The bound is judged on the frame's length *before* `serde_json` is handed it. What
/// that buys is the whole point of the bound: megabytes of attacker-chosen JSON are
/// never materialized as a `Value` and a `Vec<String>` to be refused afterwards, and a
/// frame that could not be a legal request is refused for the reason it actually has
/// rather than for whichever parse error it happens to produce first. It stays a
/// session fault rather than a transport one — the frame was received whole, so there is
/// a session to refuse on — and the connection stays open, since a peer that sends one
/// oversized envelope can send a smaller one next (`PROTOCOL.md` §2.1, §11).
fn envelope_too_large(found: usize, limit: usize) -> Refusal {
    (
        code::BAD_MESSAGE,
        format!(
            "a text envelope is at most {limit} bytes; this frame is {found} bytes and \
             was not parsed"
        ),
    )
}

/// Maps an admission failure to its refusal. `Unknown` and `TokenMismatch` stay
/// distinct on purpose: guests reuse the difference, and the oracle is accepted —
/// room ids carry 48 bits with 128-bit tokens behind them, so enumeration is
/// infeasible, and handshake rate limiting belongs to the §12 proxy.
fn refusal_for(error: SeatError, room_id: &str) -> Refusal {
    match error {
        SeatError::Unknown => {
            (code::ROOM_UNKNOWN, format!("no such room: {room_id}"))
        }
        SeatError::TokenMismatch => {
            (code::TOKEN_INVALID, "invalid room token".to_string())
        }
        SeatError::RoomFull => {
            (ROOM_FULL, "the room seats no more peers".to_string())
        }
    }
}

/// The `display_name` of a `session.rename` request, which carries the handshake's bound
/// (`PROTOCOL.md` §5). Params that do not parse and a name that is blank, control-bearing
/// or over-long are all `bad_params`; unlike the handshake the refusal is a response, not a
/// close.
fn rename_name(raw: Value) -> Result<String, Refusal> {
    let mut params = serde_json::from_value::<proto::RenameParams>(raw)
        .map_err(|e| (code::BAD_PARAMS, e.to_string()))?;
    validate_name(&mut params.display_name)?;
    Ok(params.display_name)
}

/// A peer taken out of the room under the lock: what its announcements need after it.
struct DetachedPeer {
    peer_id: String,
    generation: u64,
    /// The room holds nobody after this leave, which is what arms the room's grace.
    empty: bool,
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
        generation: detach.generation,
        empty: detach.empty,
        poison,
    })
}

/// Takes a placement back when its handshake frame could not be queued.
///
/// A mint's room is removed outright: nothing had been told about it and no grace period
/// is armed, so leaving it would hold a room slot for a session that does not exist. A
/// join is detached like any departing peer, which leaves the room's other peers where
/// they were. Either way the seat, its task registration and the sender that would have
/// ended the connection are dropped, and the caller refuses the connection.
#[expect(
    clippy::too_many_arguments,
    reason = "an unseat names the registry, whether the room was minted, and the two ids"
)]
fn unseat(
    registry: &mut Registry,
    minted: bool,
    room_id: &str,
    peer_id: &str,
) -> Option<oneshot::Sender<()>> {
    if minted {
        drop(registry.remove_room(room_id));
        return registry.take_task(peer_id);
    }
    detach_locked(registry, room_id, peer_id).and_then(|peer| peer.poison)
}

/// The `peer.left` a departure is announced with.
fn peer_left_frame(peer_id: &str) -> Option<Outbound> {
    event_frame(event::PEER_LEFT, serde_json::json!({ "peer_id": peer_id }))
}

/// The room grace period in whole milliseconds, as `host.detached` carries it and as
/// `GET /meta` advertises it before a session exists.
pub(super) fn grace_ms(config: &ServerConfig) -> u64 {
    u64::try_from(config.room_grace.as_millis()).unwrap_or(u64::MAX)
}

/// Sends a frame to a room's peers, minus one. A peer whose queue is full is not kept
/// and told nothing: it is removed, the room is told `peer.left`, and its task is
/// stopped. Frames the departures themselves need join the same loop, so a burst of
/// slow peers drains without recursing. The snapshot holds only queue handles: the
/// per-peer copies leave after the registry lock is dropped, so a large relay never
/// stalls the other rooms.
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
        let queues: Vec<(String, Queue)> = {
            let guard = shared.registry.lock().await;
            guard
                .room(room_id)
                .map(|room| room.queues(skip.as_deref()))
                .unwrap_or_default()
        };
        for peer_id in send_all(&queues, &out) {
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
    if removed.empty {
        reap_later_empty(
            shared.clone(),
            room_id.to_string(),
            removed.generation,
        );
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
    if removed.empty {
        reap_later_empty(
            shared.clone(),
            room_id.to_string(),
            removed.generation,
        );
    }
    drop(removed.poison);
}

/// What a handshake reply takes from the room, snapshotted under the registry lock.
struct RoomView {
    capabilities: Vec<String>,
    keepalive: Keepalive,
}

/// The params of `room.created`/`room.joined`, serialized. `PROTOCOL.md` §6.1's shape:
/// membership and nothing else — no document set, no listing, no role.
#[expect(
    clippy::too_many_arguments,
    reason = "a reply names its room, its token, its own seat, the room it is seated into and its clocks"
)]
fn reply_body(
    room_id: String,
    token: Option<String>,
    self_info: &proto::PeerInfo,
    peers: Vec<proto::PeerInfo>,
    view: RoomView,
) -> Result<Value, Refusal> {
    serde_json::to_value(proto::SessionParams {
        room_id,
        token,
        self_peer: self_info.clone(),
        peers,
        capabilities: view.capabilities,
        keepalive: view.keepalive,
    })
    .map_err(|error| bad_body(&error))
}

/// The `peer.joined` announcement for a seat.
fn peer_joined_frame(peer: &proto::PeerInfo) -> Option<Outbound> {
    event_frame(event::PEER_JOINED, serde_json::json!({ "peer": peer }))
}

impl Applicant {
    fn info(&self) -> proto::PeerInfo {
        proto::PeerInfo {
            peer_id: self.peer_id.clone(),
            display_name: self.hello.params.display_name.clone(),
            awareness_client_id: self.hello.params.awareness_client_id,
        }
    }

    /// Mints a room or admits this connection to an existing one, then queues the
    /// handshake response. The seating and the reply are one step under the registry
    /// lock: a publication that takes the lock after this seating takes it after the
    /// snapshot has been queued, which is what makes every publication after the
    /// snapshot follow it on the joining connection (`PROTOCOL.md` §6.3); one that took
    /// the lock before this seating is already in the snapshot. The announcements
    /// exclude the newcomer, so the reply is still this connection's first frame.
    ///
    /// # Errors
    ///
    /// Returns the refusal to send back when the room is unknown or the token is wrong.
    pub async fn seat(self, shared: &Shared) -> Result<Session, Refusal> {
        let info = self.info();
        let seating = Seating {
            applicant: &self,
            config: &shared.config,
            info: &info,
            room_id: self.join.room.as_deref(),
            peer: Peer::new(info.clone(), self.queue.clone()),
        };
        let mut guard = shared.registry.lock().await;
        let Placement {
            event: event_name,
            body,
            room_id,
            minted,
        } = seating.place(&mut guard)?;
        guard.set_task(&self.peer_id, self.poison);
        let joined_peer = (!minted).then(|| info.clone());
        let peer_id = self.peer_id.clone();
        let queue = self.queue.clone();

        // The reply is the frame that seats this connection, and it is one the
        // connection cannot be told about later: a queue that will not take it has
        // seated nothing. Handing back a live session anyway is a client waiting for a
        // handshake that is already lost, holding a seat and — for a mint — a room, so
        // the placement is taken back and the connection refused instead.
        // `--outbound-queue-bytes` cannot be set below the largest frame this server
        // generates (`ServerConfig::smallest_queue_bytes`), so a deployment does not
        // reach this by flag; it is the backstop for a configuration built past that.
        let seated = event_frame(event_name, body)
            .is_some_and(|frame| self.queue.try_queue(frame));
        if !seated {
            let poison = unseat(&mut guard, minted, &room_id, &peer_id);
            drop(guard);
            drop(poison);
            return Err((
                SERVER_FULL,
                format!(
                    "a handshake frame does not fit this server's outbound queue of \
                     {} bytes: the connection is refused rather than seated without its \
                     handshake",
                    shared.config.max_queue_bytes
                ),
            ));
        }
        drop(guard);

        // Late arrivals must be announced to the peers already in the room.
        let joined = joined_peer.and_then(|peer| peer_joined_frame(&peer));
        deliver(shared, &room_id, Some(&peer_id), joined).await;
        Ok(Session {
            peer_id,
            room_id,
            queue,
            poisoned: AtomicBool::new(false),
        })
    }
}

fn bad_body(error: &serde_json::Error) -> Refusal {
    (code::BAD_MESSAGE, error.to_string())
}

impl Seating<'_> {
    /// Seats the newcomer: its reply.
    fn mint(self, registry: &mut Registry) -> Result<Placement, Refusal> {
        let token = mint_token();
        let room_id = registry
            .create(
                NewRoom {
                    id: mint_room_id(),
                    token: token.clone(),
                    keepalive: self.config.keepalive,
                },
                self.peer,
                self.config.max_rooms,
            )
            .ok_or_else(|| {
                (
                    SERVER_FULL,
                    format!(
                        "the server holds at most {} rooms",
                        self.config.max_rooms
                    ),
                )
            })?;
        let body = reply_body(
            room_id.clone(),
            Some(token),
            self.info,
            Vec::new(),
            RoomView {
                capabilities: capabilities(),
                keepalive: self.config.keepalive,
            },
        )?;
        Ok(Placement {
            event: event::ROOM_CREATED,
            body,
            room_id,
            minted: true,
        })
    }

    fn admit(
        self,
        registry: &mut Registry,
        room_id: &str,
    ) -> Result<Placement, Refusal> {
        registry
            .admit(
                Claim {
                    room_id,
                    token: self.applicant.join.token.as_deref(),
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
                SeatError::Unknown | SeatError::TokenMismatch => {
                    refusal_for(error, room_id)
                }
            })?;
        let room = registry
            .room(room_id)
            .ok_or_else(|| (code::ROOM_GONE, "the room is gone".to_string()))?;
        let body = reply_body(
            room.id.clone(),
            None,
            self.info,
            room.peers_except(&self.applicant.peer_id),
            RoomView {
                capabilities: capabilities(),
                keepalive: room.keepalive,
            },
        )?;
        Ok(Placement {
            event: event::ROOM_JOINED,
            body,
            room_id: room.id.clone(),
            minted: false,
        })
    }

    fn place(self, registry: &mut Registry) -> Result<Placement, Refusal> {
        match self.room_id {
            None => self.mint(registry),
            Some(room_id) => self.admit(registry, room_id),
        }
    }
}

/// The capabilities a server of this version advertises (`PROTOCOL.md` §2).
fn capabilities() -> Vec<String> {
    proto::CAPABILITIES
        .iter()
        .map(ToString::to_string)
        .collect()
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
    /// will not take the reply; the caller ends it on the same frame. An envelope that
    /// repeats a member name anywhere is `bad_message` like one that does not parse:
    /// a `params` object with two of one member is last-wins to a JSON reader, and one
    /// frame must not mean two things (`PROTOCOL.md` §4).
    async fn dispatch_text(&self, text: &str, shared: &Shared) {
        // Judged on the frame, before the parser sees it (`PROTOCOL.md` §2.1): the
        // envelope bound is the one check that has to happen on the way in rather than
        // on what came out.
        if text.len() > shared.config.max_envelope_bytes {
            let (code, message) = envelope_too_large(
                text.len(),
                shared.config.max_envelope_bytes,
            );
            return self.alert(code, message);
        }
        let msg = match proto::ClientMessage::from_text(text) {
            Ok(msg) => msg,
            Err(e) => return self.alert(code::BAD_MESSAGE, e.to_string()),
        };
        let Some(id) = msg.id else {
            self.alert(code::BAD_MESSAGE, "a request needs an id");
            return;
        };
        // §10: the version is checked on every request. Another version is a frame this
        // server cannot read, which is `bad_message` like any other, and the connection
        // stays open: a peer that sent one frame can send a readable one next.
        if !proto::is_compatible(&msg.v) {
            self.alert(
                code::BAD_MESSAGE,
                format!("unsupported wire version {}", msg.v),
            );
            return;
        }
        let proto::ClientMessage { method, params, .. } = msg;
        let request = Request { id, params };
        // `PROTOCOL.md` §5: `session.hello` and `session.rename` are the whole method
        // surface. Anything else, `doc.*` included, is the answer any unknown method gets.
        match method.as_str() {
            method::SESSION_RENAME => self.rename(request, shared).await,
            method::SESSION_HELLO => {
                self.reply(&proto::ServerMessage::error(
                    id,
                    code::ALREADY_SEATED,
                    "this connection already completed the handshake",
                ));
            }
            other => self.reply(&proto::ServerMessage::error(
                id,
                code::UNKNOWN_METHOD,
                format!("no such method: {other}"),
            )),
        }
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
        // The response is queued before the event: both leave on this connection's
        // channel, so the bytes keep order.
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
        self.announce(shared, event_frame(event::PEER_RENAMED, announced))
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

    /// Sends a room-wide event to every peer, the one that made the change included: a
    /// rename is announced to the mover as well as to the rest.
    async fn announce(&self, shared: &Shared, frame: Option<Outbound>) {
        deliver(shared, &self.room_id, None, frame).await;
    }

    /// Detaches this connection, tells the room and arms the room's grace period.
    pub async fn leave(&self, shared: &Shared) {
        remove_peer(shared, &self.room_id, &self.peer_id).await;
    }
}

/// Waits out `room_grace_ms`, then destroys a room that still holds nobody.
///
/// The deadline can only be reached with no connection seated, so the destruction has no
/// recipient (`PROTOCOL.md` §6, §9): no `room.gone` is sent, the id is gone for good, and
/// the next connection that names it learns so as `room_unknown`.
fn reap_later_empty(shared: Shared, room_id: String, generation: u64) {
    tokio::spawn(async move {
        sleep(shared.config.room_grace).await;
        let mut guard = shared.registry.lock().await;
        // A room that emptied and was reoccupied since this timer was armed is at a later
        // generation, and `reap_if_empty` leaves it alone; an empty one holds no task to
        // end and nobody to tell.
        drop(guard.reap_if_empty(&room_id, generation));
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::sync::Mutex;

    use selvage_protocol::{Keepalive, PeerInfo};

    use super::*;
    use crate::room::{MAX_QUEUE_BYTES, MAX_QUEUE_FRAMES, peer_channel};

    /// A `session.hello` as the handshake would have produced it.
    fn hello(name: &str) -> Hello {
        Hello {
            params: proto::HelloParams {
                awareness_client_id: None,
                capabilities: Vec::new(),
                client: None,
                display_name: name.to_string(),
            },
        }
    }

    /// Seats `applicant`, returning the refusal: the tests below are all about the one
    /// case where seating must not happen.
    async fn refused(applicant: Applicant, shared: &Shared) -> Refusal {
        match applicant.seat(shared).await {
            Ok(_) => panic!("the handshake frame does not fit the queue"),
            Err(refusal) => refusal,
        }
    }

    /// A server holding one registry, for a test that drives seating directly.
    fn serving(config: ServerConfig) -> Shared {
        Shared::new(config, Arc::new(Mutex::new(Registry::default())))
    }

    /// A room whose membership is wide enough that a joining connection's `room.joined`
    /// does not fit a small queue. The peers are seated under the lock, so nothing is
    /// delivered while the room grows.
    async fn crowded_room(shared: &Shared) {
        let (host, _host_rx) = peer_channel(
            PeerInfo {
                peer_id: "p-ada".to_string(),
                display_name: "p".repeat(300),
                awareness_client_id: None,
            },
            MAX_QUEUE_BYTES,
        );
        let mut guard = shared.registry.lock().await;
        guard
            .create(
                NewRoom {
                    id: "r-1".to_string(),
                    token: "t".to_string(),
                    keepalive: Keepalive::default(),
                },
                host,
                1,
            )
            .expect("the room is minted");
    }

    /// A mint whose `room.created` the queue will not take is taken back: the room is
    /// removed rather than kept for a session that never heard about it, so its slot is
    /// free for the next mint.
    #[tokio::test]
    async fn a_mint_that_cannot_be_told_is_taken_back() {
        let shared = serving(ServerConfig {
            max_rooms: 1,
            max_queue_bytes: 64,
            ..ServerConfig::default()
        });
        let (queue, _leftovers) = Queue::channel(64);
        let (poison, _poisoned) = oneshot::channel();
        let applicant = Applicant {
            peer_id: "p-ada".to_string(),
            join: proto::parse_join_query("").expect("a mint's query parses"),
            hello: hello("Ada"),
            queue,
            poison,
        };
        let refused = refused(applicant, &shared).await;
        assert_eq!(refused.0, SERVER_FULL);

        // The one room slot this server has is free: the refused mint did not keep it.
        let (host, _host_rx) = peer_channel(
            PeerInfo {
                peer_id: "p-ada".to_string(),
                display_name: "Ada".to_string(),
                awareness_client_id: None,
            },
            MAX_QUEUE_BYTES,
        );
        let mut guard = shared.registry.lock().await;
        assert!(
            guard
                .create(
                    NewRoom {
                        id: "r-1".to_string(),
                        token: "t".to_string(),
                        keepalive: Keepalive::default(),
                    },
                    host,
                    1,
                )
                .is_some(),
            "the room the refused mint made was given back"
        );
    }

    /// A join whose `room.joined` the queue will not take takes no seat either: the room
    /// keeps the peers it had, and nothing of the newcomer's is left registered.
    #[tokio::test]
    async fn a_join_that_cannot_be_told_takes_no_seat() {
        let shared = serving(ServerConfig {
            max_queue_bytes: 4096,
            ..ServerConfig::default()
        });
        crowded_room(&shared).await;

        let (queue, _leftovers) = Queue::channel(64);
        let (poison, _poisoned) = oneshot::channel();
        let applicant = Applicant {
            peer_id: "p-bob".to_string(),
            join: proto::parse_join_query("room=r-1&token=t")
                .expect("the join query parses"),
            hello: hello("Bob"),
            queue,
            poison,
        };
        let refused = refused(applicant, &shared).await;
        assert_eq!(refused.0, SERVER_FULL);

        let mut guard = shared.registry.lock().await;
        let room = guard.room("r-1").expect("the room is still there");
        assert_eq!(room.peers.len(), 1, "the newcomer took no seat");
        assert!(
            guard.take_task("p-bob").is_none(),
            "no task is left registered for it"
        );
    }

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
        let (host, _rx) = peer_channel(
            PeerInfo {
                peer_id: "p-host".to_string(),
                display_name: "Ada".to_string(),
                awareness_client_id: None,
            },
            MAX_QUEUE_BYTES,
        );
        assert_eq!(
            registry
                .create(
                    NewRoom {
                        id: "r-1".to_string(),
                        token: "t".to_string(),
                        keepalive: Keepalive::default(),
                    },
                    host,
                    usize::MAX,
                )
                .as_deref(),
            Some("r-1")
        );
        let (guest, _rx) = peer_channel(
            PeerInfo {
                peer_id: "p-slow".to_string(),
                display_name: "Bob".to_string(),
                awareness_client_id: None,
            },
            MAX_QUEUE_BYTES,
        );
        let queue = guest.queue.clone();
        registry
            .admit(
                Claim {
                    room_id: "r-1",
                    token: Some("t"),
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
