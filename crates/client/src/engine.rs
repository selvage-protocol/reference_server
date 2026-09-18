//! The connection task: one task owns the `Y.Doc`, the y-protocols awareness state and
//! the WebSocket, and answers commands from [`crate::SyncEngine`].
//!
//! Everything after the session handshake that is not a session method goes through
//! `yrs::sync::protocol::DefaultProtocol`, i.e. the reference implementation of
//! y-protocols. Nothing in this file invents a document or awareness encoding.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Interval, MissedTickBehavior, interval, sleep, timeout};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Error as WireError;
use tokio_tungstenite::tungstenite::Message;

use selvage_protocol as proto;
use selvage_protocol::{code, event, method};
use yrs::Observable;
use yrs::Subscription;
use yrs::block::ClientID;
use yrs::encoding::read::Cursor;
use yrs::sync::protocol::{
    DefaultProtocol, MessageReader, Protocol as YProtocol,
};
use yrs::sync::{Awareness, Message as YMessage, SyncMessage};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{
    Assoc, BranchID, Doc, GetString, IndexedSequence, OffsetKind, Options,
    ReadTxn, StateVector, StickyIndex, Text as YText, TextRef, Transact,
    TransactionMut, Update,
};

use crate::editor::EngineEvent;
use crate::presence::{
    Anchor, AwarenessState, PeerInfo, Presence, Selection, SelectionOffsets,
};
use crate::session::ReconnectPolicy;
use crate::{ConnectOptions, Error, KeepaliveConfig, SessionInfo};

/// The origin this client tags its own document writes with. A remote frame is applied by
/// `yrs`'s protocol implementation with no origin at all, so an observer tells an adapter's
/// own edit from a peer's by this, and reports only the peer's.
const LOCAL_ORIGIN: &str = "selvage:local";

/// How long a handshake may take before the attempt is abandoned, first connect and
/// reconnect alike.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest inbound binary frame this client will apply, in bytes: the transport's own
/// bound (`PROTOCOL.md` §2.1, informative 16 MiB), and the same number the TypeScript
/// engine's `MAX_INBOUND_BINARY_BYTES` carries — the two are one behaviour and move
/// together.
///
/// A frame over it never arrives from a conforming transport — the reference server's own
/// bound is 8 MiB and a frame past it ends the connection — so refusing one here changes no
/// session that obeys the wire, and bounds what an unbounded y-protocols update can allocate
/// or do. Refusal is drop-and-continue, never fail: an over-bound frame is ignored and the
/// session goes on with the grant, documents and replica it holds, stale rather than ended,
/// because ending it would hand any sender a kill switch.
pub const MAX_INBOUND_BINARY_BYTES: usize = 16 * 1024 * 1024;

type Socket = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = SplitSink<Socket, Message>;
type Stream = SplitStream<Socket>;

/// A local edit to apply to a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditOp {
    Insert { index: u32, text: String },
    Delete { index: u32, len: u32 },
}

pub enum Command {
    Open {
        path: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Close {
        path: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Rename {
        display_name: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Grant {
        paths: Vec<String>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Text {
        path: String,
        reply: oneshot::Sender<String>,
    },
    Edit {
        path: String,
        op: EditOp,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    SetAwareness {
        path: Option<String>,
        selection: Option<SelectionOffsets>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Presence {
        reply: oneshot::Sender<Vec<Presence>>,
    },
    Peers {
        reply: oneshot::Sender<Vec<PeerInfo>>,
    },
    StateVector {
        reply: oneshot::Sender<Vec<(u64, u32)>>,
    },
    Documents {
        reply: oneshot::Sender<Vec<String>>,
    },
    GrantedPaths {
        reply: oneshot::Sender<Vec<String>>,
    },
    OpenDocuments {
        reply: oneshot::Sender<Vec<String>>,
    },
    SetOutboundPaused {
        paused: bool,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Shutdown {
        reply: Option<oneshot::Sender<()>>,
    },
}

/// The channels a caller reaches a running connection task through.
pub struct Channel {
    pub commands: mpsc::UnboundedSender<Command>,
    pub events: broadcast::Sender<EngineEvent>,
    /// The current session description, replaced on a reconnect.
    pub session: Arc<Mutex<SessionInfo>>,
}

/// A completed handshake: a socket, the replica it was seated with, and the server's
/// description of the session.
struct Replica {
    sink: Sink,
    stream: Stream,
    awareness: Awareness,
    session: SessionInfo,
}

/// Runs one connection attempt: socket, handshake, and the reply. Never retried here:
/// a first connect that fails is a failure the caller sees, and a reconnect decides for
/// itself whether another attempt is worth making.
///
/// # Errors
///
/// Returns [`Error`] when the socket cannot be opened, the hello cannot be sent, or the
/// server refuses the session.
pub async fn connect(
    options: ConnectOptions,
) -> Result<(Channel, SessionInfo), Error> {
    let local_state = serde_json::to_string(&options.initial_awareness)?;
    let replica = handshake(&options, None, &local_state).await?;
    let session = replica.session.clone();
    Ok((spawn(options, replica, local_state), session))
}

/// Opens the socket, seeds a fresh `Y.Doc` and sends `session.hello`.
///
/// Every attempt gets a fresh replica: a reconnecting client is a new peer, and `yrs`
/// keeps a tombstone for an awareness client id whose state was removed, so reusing one
/// drops the first republish. `previous` carries the outgoing replica's state into the
/// new one, so what the client already holds is not lost with the socket.
async fn handshake(
    options: &ConnectOptions,
    previous: Option<Vec<u8>>,
    local_state: &str,
) -> Result<Replica, Error> {
    // A text offset on this API is a UTF-16 code unit, the unit `yjs`, every editor's
    // `offsetAt` and every peer on the wire use (PROTOCOL.md §8.1). `yrs` defaults to
    // UTF-8 byte offsets, which would put a cursor after the first non-BMP character
    // somewhere else than every other implementation does.
    let doc = fresh_doc(previous)?;
    let awareness_client_id = doc.client_id().get();
    let mut awareness = Awareness::new(doc);
    awareness.set_local_state_raw(local_state.to_string());

    let url = proto::session_url(
        &options.base_url,
        options.room.as_deref(),
        options.token.as_deref(),
    );
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(Error::Wire)?;
    let (mut sink, mut stream) = ws.split();

    let hello = proto::ClientMessage::new(
        1,
        method::SESSION_HELLO,
        serde_json::json!(proto::HelloParams {
            display_name: options.display_name.clone(),
            role: options.role,
            awareness_client_id: Some(awareness_client_id),
            capabilities: options.capabilities.clone(),
            client: options.client.clone(),
        }),
    );
    sink.send(Message::text(hello.to_text()?))
        .await
        .map_err(Error::Wire)?;
    let session = await_session(&mut stream, &options.base_url).await?;
    Ok(Replica {
        sink,
        stream,
        awareness,
        session,
    })
}

/// A new replica carrying `previous`'s state, if any. The fresh client id is what a
/// reconnect needs; the state is what it must not lose.
fn fresh_doc(previous: Option<Vec<u8>>) -> Result<Doc, Error> {
    let doc = Doc::with_options(Options {
        offset_kind: OffsetKind::Utf16,
        ..Options::default()
    });
    if let Some(update) = previous
        && !update.is_empty()
    {
        let decoded = Update::decode_v1(&update)
            .map_err(|e| Error::Yjs(e.to_string()))?;
        doc.transact_mut()
            .apply_update(decoded)
            .map_err(|e| Error::Yjs(e.to_string()))?;
    }
    Ok(doc)
}

/// The state a fresh replica is seated with, or `None` when there is nothing to carry.
///
/// A replica that has integrated nothing encodes to the encoding of nothing, which
/// [`fresh_doc`] would decode and apply for no effect at all — and the encode is O(document),
/// once per attempt. The state vector is O(clients), which is why it is what decides.
fn seed_update(doc: &Doc) -> Option<Vec<u8>> {
    let txn = doc.transact();
    if txn.state_vector().is_empty() {
        return None;
    }
    Some(txn.encode_state_as_update_v1(&StateVector::default()))
}

/// Waits for `room.created`/`room.joined`, or the reason the server refused.
async fn await_session(
    stream: &mut Stream,
    base_url: &str,
) -> Result<SessionInfo, Error> {
    loop {
        let frame = stream.next().await;
        if let Some(session) = session_from(frame, base_url)? {
            return Ok(session);
        }
    }
}

/// Reads one frame of the handshake. `Ok(None)` means "keep waiting".
fn session_from(
    frame: Option<Result<Message, WireError>>,
    base_url: &str,
) -> Result<Option<SessionInfo>, Error> {
    match frame {
        Some(Ok(Message::Text(text))) => session_event(&text, base_url),
        Some(Ok(Message::Close(_))) | None => Err(Error::Closed),
        Some(Err(e)) => Err(Error::Wire(e)),
        Some(Ok(_)) => Ok(None),
    }
}

/// Interprets the JSON envelope that ends the handshake. `Ok(None)` means "not yet".
fn session_event(
    text: &str,
    base_url: &str,
) -> Result<Option<SessionInfo>, Error> {
    // A frame that repeats a member name anywhere is refused here as it is by the server:
    // one frame that reads two ways is one the receiver must not pick a meaning for
    // (`PROTOCOL.md` §4).
    let msg: proto::ServerMessage = proto::ServerMessage::from_text(text)?;
    match msg.event.as_deref() {
        Some(event::ROOM_CREATED | event::ROOM_JOINED) => {
            let body = msg.params.unwrap_or_else(|| serde_json::json!({}));
            let params: proto::SessionParams = serde_json::from_value(body)?;
            Ok(Some(session_info(params, base_url)))
        }
        Some(event::SESSION_ERROR) => {
            let body = msg.params.unwrap_or_else(|| serde_json::json!({}));
            Err(refusal(&body))
        }
        _ => Ok(None),
    }
}

fn refusal(params: &serde_json::Value) -> Error {
    Error::Protocol {
        code: text_param(params, "code").unwrap_or_else(|| "error".to_string()),
        message: text_param(params, "message")
            .unwrap_or_else(|| "the server refused the session".to_string()),
    }
}

fn text_param(params: &serde_json::Value, key: &str) -> Option<String> {
    params.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn session_info(params: proto::SessionParams, base_url: &str) -> SessionInfo {
    SessionInfo {
        room_id: params.room_id,
        token: params.token,
        role: params.self_peer.role,
        peer: params.self_peer,
        peers: params.peers,
        documents: params.documents,
        capabilities: params.capabilities,
        keepalive: params.keepalive,
        base_url: base_url.to_string(),
    }
}

fn spawn(
    options: ConnectOptions,
    replica: Replica,
    local_state: String,
) -> Channel {
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (events_tx, _) = broadcast::channel(64);
    let session_slot = Arc::new(Mutex::new(replica.session.clone()));
    // The server advertises the session's keepalive; a caller that did not override it
    // runs on the server's clock, so both ends measure awareness the same way.
    let keepalive = options
        .keepalive
        .unwrap_or_else(|| KeepaliveConfig::from(replica.session.keepalive));
    let room = options.room.clone();
    let token = options.token.clone();
    let task = EngineTask {
        policy: options.reconnect,
        options,
        sink: replica.sink,
        stream: replica.stream,
        awareness: replica.awareness,
        session: replica.session,
        session_slot: Arc::clone(&session_slot),
        room,
        token,
        terminal: false,
        attempts: 0,
        commands: commands_rx,
        events: events_tx.clone(),
        keepalive,
        documents: Vec::new(),
        granted_paths: Vec::new(),
        open_documents: Vec::new(),
        peers: HashMap::new(),
        request_id: 1,
        inflight: None,
        waiting: VecDeque::new(),
        watched: HashMap::new(),
        local_state: Some(local_state),
        queued: VecDeque::new(),
        paused: false,
    };
    tokio::spawn(task.run());
    Channel {
        commands: commands_tx,
        events: events_tx,
        session: session_slot,
    }
}

/// How a turn loop ended.
enum SessionEnd {
    /// The session ended deliberately: the client asked to stop.
    Stopped,
    /// The socket failed. Whether that is recoverable is the caller's call.
    Dropped,
}

/// The result of one turn of the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Turn {
    Continue,
    /// The client asked to end the session; nothing is recovered.
    Stopped,
    /// The socket failed. This may be recoverable.
    Dropped,
}

/// The outcome of a reconnect.
enum Reconnect {
    /// Re-helloed and seated as a fresh peer.
    Seated,
    /// Retries are exhausted or a refusal was terminal: report a disconnect.
    GivenUp,
    /// The client was shut down while waiting: emit nothing.
    Disposed,
}

/// The outcome of one reconnect attempt.
enum Attempt {
    Seated(Box<Replica>),
    Retry,
    Refused,
}

/// Refusals after which retrying the same URL cannot help (`PROTOCOL.md` §9.1, §11).
///
/// A code in the reserved `x.` namespace is one this document does not define, and §9.1
/// makes every one of them a stop: a handshake refused with one **MUST NOT** be re-helloed
/// automatically, whether or not this client knows the code. Recognising only the bare
/// five would re-hello a `x.server_full` or `x.room_full` refusal until the retries ran
/// out, which is the one behaviour §9.1 names as forbidden.
fn is_terminal_code(name: &str) -> bool {
    name.starts_with("x.")
        || matches!(
            name,
            code::ROOM_UNKNOWN
                | code::TOKEN_INVALID
                | code::HOST_PRESENT
                | code::UNSUPPORTED_VERSION
                | code::ROOM_GONE
        )
}

/// Runs the turn loop until the session ends.
async fn session(task: &mut EngineTask, renew: &mut Interval) -> SessionEnd {
    if task.flush_outbound().await.is_err() {
        return SessionEnd::Dropped;
    }
    loop {
        match task.turn(renew).await {
            Turn::Continue => {}
            Turn::Stopped => return SessionEnd::Stopped,
            Turn::Dropped => return SessionEnd::Dropped,
        }
        // One flush per turn: every path only queues frames, and a paused client keeps
        // queueing until it is resumed.
        if task.paused {
            continue;
        }
        if task.flush_outbound().await.is_err() {
            return SessionEnd::Dropped;
        }
    }
}

/// A request that has gone out and is waiting for its answer. The answer either fails the
/// request or is accepted, and acceptance is what moves local state.
enum Pending {
    Open {
        path: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Close {
        path: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Rename {
        display_name: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Grant {
        paths: Vec<String>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
}

impl Pending {
    /// The session method this request calls.
    const fn method(&self) -> &'static str {
        match self {
            Self::Open { .. } => method::DOC_OPEN,
            Self::Close { .. } => method::DOC_CLOSE,
            Self::Rename { .. } => method::SESSION_RENAME,
            Self::Grant { .. } => method::DOC_GRANT,
        }
    }

    /// The params this request carries. They come from the same fields acceptance reads,
    /// so the request and its meaning cannot drift apart.
    fn params(&self) -> serde_json::Value {
        match self {
            Self::Open { path, .. } | Self::Close { path, .. } => {
                serde_json::json!({ "path": path })
            }
            Self::Rename { display_name, .. } => {
                serde_json::json!({ "display_name": display_name })
            }
            Self::Grant { paths, .. } => serde_json::json!({ "paths": paths }),
        }
    }

    /// Answers the caller. The response is what carries the outcome, so this is called
    /// once the server has spoken — or once it never will.
    fn answer(self, outcome: Result<(), Error>) {
        let reply = match self {
            Self::Open { reply, .. }
            | Self::Close { reply, .. }
            | Self::Rename { reply, .. }
            | Self::Grant { reply, .. } => reply,
        };
        let _ = reply.send(outcome);
    }
}

/// The room's open-document set, from a `doc.open`/`doc.close` result.
fn documents_from(
    result: Option<&serde_json::Value>,
) -> Result<Vec<String>, Error> {
    let body = result.cloned().unwrap_or_else(|| serde_json::json!({}));
    let accepted: proto::DocSet = serde_json::from_value(body)?;
    Ok(accepted.documents)
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "the outbound pause and a terminal refusal are independent state"
)]
struct EngineTask {
    policy: ReconnectPolicy,
    options: ConnectOptions,
    sink: Sink,
    stream: Stream,
    awareness: Awareness,
    session: SessionInfo,
    /// The description `SyncEngine::session` reads, replaced on every reconnect.
    session_slot: Arc<Mutex<SessionInfo>>,
    /// The room to reconnect to, learned from `room.created` when this client minted it.
    room: Option<String>,
    /// The token a reconnect has to carry, learned from `room.created`.
    token: Option<String>,
    /// A refusal a retry cannot change was seen; the next drop must not be retried.
    terminal: bool,
    /// Reconnect attempts made since the last successful seat.
    attempts: u32,
    commands: mpsc::UnboundedReceiver<Command>,
    events: broadcast::Sender<EngineEvent>,
    keepalive: KeepaliveConfig,
    /// The room's open-document set, as owned by the server.
    documents: Vec<String>,
    /// The room's grant, as published by its host. It is not a member of `room.joined`: the
    /// server sends `doc.granted` right after the join reply, and only when the listing is
    /// non-empty.
    granted_paths: Vec<String>,
    /// The documents this client has open, in the order it opened them.
    open_documents: Vec<String>,
    peers: HashMap<String, PeerInfo>,
    request_id: u64,
    /// The request the server has not answered yet, with its id. At most one is in flight
    /// on a connection (`PROTOCOL.md` §5): a seated `session.error` carries no `id`, so a
    /// client that pipelined could not tell which request it sank.
    inflight: Option<(u64, Pending)>,
    /// Requests made while another was in flight, in the order they were made. Each goes
    /// out when the one before it is answered; none of them is on the wire before then.
    waiting: VecDeque<Pending>,
    /// The documents whose changes reach an adapter, by path: the handle whose observer
    /// reports them. A document that has not arrived has no text to watch — creating one
    /// would make an unreceived document look like an empty one, which §8.1 forbids a
    /// sender to anchor against — so it is watched when it arrives.
    watched: HashMap<String, Subscription>,
    /// The JSON published for the local client, replayed on every renewal.
    local_state: Option<String>,
    /// Frames produced while outbound is paused, flushed on resume. Text frames carry
    /// the JSON session envelope; binary frames are y-protocols payloads.
    queued: VecDeque<Message>,
    paused: bool,
}

impl EngineTask {
    async fn run(mut self) {
        let mut renew = interval(self.keepalive.awareness_renew);
        renew.set_missed_tick_behavior(MissedTickBehavior::Delay);
        renew.tick().await;
        self.seat();
        while self.cycle(&mut renew).await {}
        // Whatever is still outstanding can never be answered now.
        self.fail_pending();
    }

    /// Runs one connection's lifetime. Returns `false` when the task is done: the
    /// client asked to stop, retries are exhausted, or a terminal refusal arrived.
    async fn cycle(&mut self, renew: &mut Interval) -> bool {
        match session(self, renew).await {
            SessionEnd::Stopped => return false,
            SessionEnd::Dropped => {}
        }
        // The frames that were queued belonged to the socket that just died. What they
        // carried is either in the document — recovered by the sync handshake — or
        // re-issued by the caller that asked for it.
        self.fail_pending();
        self.queued.clear();
        match self.reconnect().await {
            Reconnect::Seated => {
                self.seat();
                true
            }
            Reconnect::GivenUp => {
                self.disconnect();
                false
            }
            Reconnect::Disposed => false,
        }
    }

    /// One turn of the session: waits on commands, the awareness clock and the socket,
    /// and reports what the turn did.
    async fn turn(&mut self, renew: &mut Interval) -> Turn {
        tokio::select! {
            command = self.commands.recv() => self.handle_command(command).await,
            _ = renew.tick() => {
                self.renew_awareness();
                Turn::Continue
            }
            incoming = self.stream.next() => self.handle_incoming(incoming),
        }
    }

    /// Applies a completed handshake: a fresh peer, the room's document set, the sync
    /// handshake, and this client's own documents re-opened. Runs for the first seat and
    /// for every reconnect alike.
    fn seat(&mut self) {
        // Every attempt seats a fresh replica, so the observers taken from the last one go
        // with it. The documents it already carries are watched again — silently, because
        // the adapter has their content and nothing about it changed — and one that only
        // arrives later is reported as it arrives.
        self.watched.clear();
        let _ = self.attach_arrivals();
        // §9.1: the documents this client still holds open are re-opened, which is what
        // puts them back in the room's set when nobody else had them. Content is not
        // replayed: the sync handshake brings it back from the peers.
        for path in self.open_documents.clone() {
            let (reply, _gone) = oneshot::channel();
            self.request(Pending::Open { path, reply });
        }
        // §7: immediately after seating, SyncStep1 with our state vector — every peer
        // replies with what we are missing. Then publish our awareness, so a newcomer's
        // presence is complete before anyone moves a cursor.
        let step1 = {
            let txn = self.doc().transact();
            encode_y_message(&YMessage::Sync(SyncMessage::SyncStep1(
                txn.state_vector(),
            )))
        };
        self.enqueue(Message::binary(step1));
        let _ = self.publish_local_awareness();
        self.remember();
        self.terminal = false;
        self.attempts = 0;
        let _ = self.events.send(EngineEvent::DocumentsChanged {
            documents: self.documents.clone(),
        });
        let _ = self.events.send(EngineEvent::GrantChanged {
            paths: self.granted_paths.clone(),
        });
        let _ = self.events.send(EngineEvent::PeersChanged {
            peers: self.peer_list(),
        });
    }

    /// Records what this seat says about the session: the room to reconnect to, the
    /// token a reconnect has to carry, the room's document set and its peers.
    fn remember(&mut self) {
        self.room = Some(self.session.room_id.clone());
        if self.session.token.is_some() {
            self.token = self.session.token.clone();
        }
        self.documents = self.session.documents.clone();
        // The grant is not a member of `room.joined`, and the server sends `doc.granted` only
        // for a non-empty listing, so a seat starts from nothing and learns what the room
        // holds from the event. A stale listing must not outlive its connection.
        self.granted_paths.clear();
        self.peers = self
            .session
            .peers
            .iter()
            .map(|peer| (peer.peer_id.clone(), peer.clone()))
            .collect();
        self.publish_session();
    }

    fn publish_session(&self) {
        let mut slot = match self.session_slot.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        slot.clone_from(&self.session);
    }

    // --- reconnecting --------------------------------------------------------

    /// Re-hellos the room after a recoverable drop, under the policy (`PROTOCOL.md`
    /// §9.1). Returns whether the session was reseated, given up on, or shut down while
    /// waiting.
    async fn reconnect(&mut self) -> Reconnect {
        // Nothing to retry, or nothing left to retry with: the drop is the end, and saying
        // `reconnecting` first would announce a retry that is not going to run.
        if self.terminal
            || !self.policy.enabled
            || self.attempts >= self.policy.max_attempts
        {
            return Reconnect::GivenUp;
        }
        // Said out loud, so an adapter can show the retry without inferring it from
        // silence (`PROTOCOL.md` §9.1): the re-seat or the give-up follows as its own
        // event.
        let _ = self.events.send(EngineEvent::Reconnecting);
        self.retry_until_seated().await
    }

    /// Attempts until one seats, a terminal refusal stops the retries, or the client is
    /// shut down. Returns the last decision.
    async fn retry_until_seated(&mut self) -> Reconnect {
        let mut outcome: Option<Reconnect> = None;
        while outcome.is_none() && self.attempts < self.policy.max_attempts {
            outcome = self.retry_once().await;
        }
        outcome.unwrap_or(Reconnect::GivenUp)
    }

    /// One wait-then-attempt step. `None` says another attempt is allowed.
    async fn retry_once(&mut self) -> Option<Reconnect> {
        if !self.pause_before_retry(self.backoff_delay()).await {
            return Some(Reconnect::Disposed);
        }
        self.attempts = self.attempts.saturating_add(1);
        match self.attempt().await {
            Attempt::Seated(replica) => Some(self.reseat(replica)),
            Attempt::Retry => None,
            Attempt::Refused => Some(Reconnect::GivenUp),
        }
    }

    /// Moves a freshly handshaken replica into this task.
    fn reseat(&mut self, replica: Box<Replica>) -> Reconnect {
        self.sink = replica.sink;
        self.stream = replica.stream;
        self.awareness = replica.awareness;
        self.session = replica.session;
        Reconnect::Seated
    }

    /// The delay before the next attempt: `initial_delay` doubling to `max_delay`.
    fn backoff_delay(&self) -> Duration {
        let factor = 1u32.checked_shl(self.attempts).unwrap_or(u32::MAX);
        self.policy
            .initial_delay
            .saturating_mul(factor)
            .min(self.policy.max_delay)
    }

    /// Waits out the backoff, serving commands as they arrive so a caller that shuts the
    /// engine down does not wait out the whole delay. Returns `false` on shutdown.
    async fn pause_before_retry(&mut self, delay: Duration) -> bool {
        let wait = sleep(delay);
        tokio::pin!(wait);
        loop {
            tokio::select! {
                () = &mut wait => return true,
                command = self.commands.recv() => {
                    if self.handle_command(command).await != Turn::Continue {
                        return false;
                    }
                }
            }
        }
    }

    /// One bounded reconnect attempt. The replica this client already holds is carried
    /// into the fresh one, so what it knows is not lost with the socket; a terminal
    /// refusal stops the retries, every other failure is retried.
    async fn attempt(&self) -> Attempt {
        let mut options = self.options.clone();
        options.room.clone_from(&self.room);
        options.token.clone_from(&self.token);
        let local_state =
            self.local_state.clone().unwrap_or_else(|| "{}".to_string());
        let previous = seed_update(self.awareness.doc());
        let outcome = timeout(
            HANDSHAKE_TIMEOUT,
            handshake(&options, previous, &local_state),
        )
        .await;
        match outcome {
            Ok(Ok(replica)) => Attempt::Seated(Box::new(replica)),
            Ok(Err(Error::Protocol { code, message }))
                if is_terminal_code(&code) =>
            {
                let _ = self
                    .events
                    .send(EngineEvent::SessionError { code, message });
                Attempt::Refused
            }
            _ => Attempt::Retry,
        }
    }

    /// Handles one inbound frame.
    fn handle_incoming(
        &mut self,
        incoming: Option<Result<Message, WireError>>,
    ) -> Turn {
        match incoming {
            Some(Ok(Message::Binary(frame))) => self.handle_binary(&frame),
            Some(Ok(Message::Text(text))) => self.handle_text(&text),
            Some(Ok(Message::Close(_)) | Err(_)) | None => {
                return Turn::Dropped;
            }
            Some(Ok(_)) => {}
        }
        Turn::Continue
    }

    fn disconnect(&self) {
        let _ = self.events.send(EngineEvent::Disconnected);
    }

    // --- commands ------------------------------------------------------------

    /// Runs one command.
    async fn handle_command(&mut self, command: Option<Command>) -> Turn {
        let Some(next) = command else {
            return Turn::Stopped;
        };
        match next {
            Command::Open { path, reply } => self.open(&path, reply),
            Command::Close { path, reply } => self.close(&path, reply),
            Command::Rename {
                display_name,
                reply,
            } => {
                self.rename(display_name, reply);
            }
            Command::Grant { paths, reply } => self.grant(paths, reply),
            Command::Text { path, reply } => {
                let _ = reply.send(self.read_text(&path));
            }
            Command::Edit { path, op, reply } => {
                self.apply_edit(&path, op);
                let _ = reply.send(Ok(()));
            }
            Command::SetAwareness {
                path,
                selection,
                reply,
            } => {
                let result = self.set_local_awareness(path, selection);
                let _ = reply.send(result);
            }
            Command::Presence { reply } => {
                let _ = reply.send(self.presence());
            }
            Command::Peers { reply } => {
                let _ = reply.send(self.peer_list());
            }
            Command::StateVector { reply } => {
                let _ = reply.send(self.state_vector());
            }
            Command::Documents { reply } => {
                let _ = reply.send(self.documents.clone());
            }
            Command::GrantedPaths { reply } => {
                let _ = reply.send(self.granted_paths.clone());
            }
            Command::OpenDocuments { reply } => {
                let _ = reply.send(self.open_documents.clone());
            }
            Command::SetOutboundPaused { paused, reply } => {
                let result = self.set_paused(paused).await;
                let _ = reply.send(result);
            }
            Command::Shutdown { reply } => return self.shutdown(reply).await,
        }
        Turn::Continue
    }

    /// Asks the server to open a document. Local state moves when it agrees.
    fn open(&mut self, path: &str, reply: oneshot::Sender<Result<(), Error>>) {
        self.request(Pending::Open {
            path: path.to_string(),
            reply,
        });
    }

    /// Asks the server to close a document. Local state moves when it agrees.
    fn close(&mut self, path: &str, reply: oneshot::Sender<Result<(), Error>>) {
        self.request(Pending::Close {
            path: path.to_string(),
            reply,
        });
    }

    /// Asks the server to change this connection's display name. Local state moves when the
    /// `peer.renamed` event arrives, which the mover receives like every other peer.
    fn rename(
        &mut self,
        display_name: String,
        reply: oneshot::Sender<Result<(), Error>>,
    ) {
        self.request(Pending::Rename {
            display_name,
            reply,
        });
    }

    /// Asks the server to publish the room's grant. Local state moves when the `doc.granted`
    /// event arrives, which the publisher receives like every other peer.
    fn grant(
        &mut self,
        paths: Vec<String>,
        reply: oneshot::Sender<Result<(), Error>>,
    ) {
        self.request(Pending::Grant { paths, reply });
    }

    /// Applies a local edit and sends exactly the delta it produced. Sending the delta
    /// rather than the whole state keeps the frame small and stays inside y-protocols.
    fn apply_edit(&mut self, path: &str, op: EditOp) {
        let before = {
            let txn = self.doc().transact();
            txn.state_vector()
        };
        self.mutate_text(path, op);
        // A document this client is the first to touch is watched from now on; its arrival
        // is its own edit, which the adapter already has.
        let _ = self.attach_arrivals();
        let update = {
            let txn = self.doc().transact();
            txn.encode_state_as_update_v1(&before)
        };
        if !update.is_empty() {
            self.enqueue(Message::binary(encode_y_message(&YMessage::Sync(
                SyncMessage::Update(update),
            ))));
        }
    }

    /// Applies `op` to a document's text, inside one write transaction.
    fn mutate_text(&mut self, path: &str, op: EditOp) {
        let text = self.doc().get_or_insert_text(path);
        // Tagged as this client's own: an observer tells an adapter's edit from a peer's by
        // the origin, and only the peer's is news to the adapter that made the other.
        let mut txn = self.doc().transact_mut_with(LOCAL_ORIGIN);
        match op {
            EditOp::Insert { index, text: chunk } => {
                text.insert(&mut txn, index, &chunk);
            }
            EditOp::Delete { index, len } => {
                text.remove_range(&mut txn, index, len);
            }
        }
    }

    async fn set_paused(&mut self, paused: bool) -> Result<(), Error> {
        self.paused = paused;
        if paused {
            return Ok(());
        }
        self.flush_outbound().await
    }

    async fn shutdown(&mut self, reply: Option<oneshot::Sender<()>>) -> Turn {
        let _ = self.flush_outbound().await;
        let _ = self.sink.close().await;
        if let Some(ack) = reply {
            let _ = ack.send(());
        }
        Turn::Stopped
    }

    // --- state ---------------------------------------------------------------

    fn doc(&mut self) -> &mut Doc {
        self.awareness.doc_mut()
    }

    /// Attaches an observer to every held document that has arrived and is not watched
    /// yet, returning the paths it attached: their arrival is a change an adapter has to
    /// hear about, and no transaction reports it — `yrs` gives a root type an update
    /// creates no kind of its own, so nothing fires for the very update that brings a
    /// document this replica did not have.
    fn attach_arrivals(&mut self) -> Vec<String> {
        let arrived: Vec<String> = self
            .open_documents
            .clone()
            .into_iter()
            .filter(|path| !self.watched.contains_key(path))
            .filter(|path| self.has_text(path))
            .collect();
        for path in &arrived {
            self.watch(path);
        }
        arrived
    }

    /// Watches `path`: the document is here, so its text is there to be observed. The
    /// observer reports the document a transaction changed, and only that one — a remote
    /// keystroke costs one event for the document it typed into, not one per document this
    /// client has open.
    fn watch(&mut self, path: &str) {
        let events = self.events.clone();
        let watched: Arc<str> = Arc::from(path);
        let text = self.doc().get_or_insert_text(path);
        let subscription = text
            .observe(move |txn, _event| report_change(&events, &watched, txn));
        self.watched.insert(path.to_string(), subscription);
    }

    /// Whether this replica holds the text for `path` at all. Reading never creates one.
    fn has_text(&self, path: &str) -> bool {
        let doc = self.awareness.doc();
        let txn = doc.transact();
        txn.get_text(path).is_some()
    }

    /// The text of a document this replica holds, empty for one it does not hold or that
    /// nobody has written to. Reading never creates the text.
    fn read_text(&mut self, path: &str) -> String {
        let doc = self.doc();
        let txn = doc.transact();
        txn.get_text(path)
            .map_or_else(String::new, |text| text.get_string(&txn))
    }

    fn state_vector(&mut self) -> Vec<(u64, u32)> {
        let txn = self.doc().transact();
        let mut entries: Vec<(u64, u32)> = txn
            .state_vector()
            .iter()
            .map(|(client, clock)| (client.get(), *clock))
            .collect();
        entries.sort_unstable();
        entries
    }

    /// Publishes this client's presence, turning the offsets the adapter speaks into the
    /// anchors the wire carries (`PROTOCOL.md` §8.1).
    ///
    /// A state equal to the one already published is not published again: `yrs` emits an
    /// awareness update for every `set_local_state_raw`, changed or not, and a caret that has
    /// not moved is not news. The renewal and a fresh seat are the two callers that publish
    /// regardless, because a newer clock and a new client id are each something a peer has to
    /// see (§8.2, §9.1).
    fn set_local_awareness(
        &mut self,
        path: Option<String>,
        selection: Option<SelectionOffsets>,
    ) -> Result<(), Error> {
        let state = self.anchored_state(path, selection);
        let json = serde_json::to_string(&state)?;
        if self.local_state.as_deref() == Some(json.as_str()) {
            return Ok(());
        }
        self.awareness.set_local_state_raw(json.clone());
        self.local_state = Some(json);
        self.publish_local_awareness()?;
        let _ = self.events.send(EngineEvent::PresenceChanged {
            presence: self.presence(),
        });
        Ok(())
    }

    /// The state to publish: a selection is anchored against this replica, and is dropped
    /// when there is no path to anchor it in.
    fn anchored_state(
        &mut self,
        path: Option<String>,
        selection: Option<SelectionOffsets>,
    ) -> AwarenessState {
        let anchored = match (path.as_deref(), selection) {
            (Some(target), Some(offsets)) => {
                self.anchor_selection(target, offsets)
            }
            _ => None,
        };
        AwarenessState {
            path,
            selection: anchored,
        }
    }

    /// Anchors both endpoints of a selection against this replica, or `None` when it cannot:
    /// §8.1 forbids a *sender* from manufacturing a position, and the scope-only fallback is
    /// indistinguishable on the wire from a genuine caret at the end of the text.
    fn anchor_selection(
        &mut self,
        path: &str,
        offsets: SelectionOffsets,
    ) -> Option<Selection> {
        let txn = self.doc().transact();
        // A document this replica has not received has no text to anchor against, and an
        // offset past the end of one is not a position in it either.
        let text = txn.get_text(path)?;
        let end = text.len(&txn);
        if offsets.anchor > end || offsets.head > end {
            return None;
        }
        Some(Selection {
            anchor: anchor_at(&txn, &text, offsets.anchor),
            head: anchor_at(&txn, &text, offsets.head),
        })
    }

    /// y-protocols awareness renews every 15s and expires at 30s: renewal means
    /// republishing the same state so peers see a newer clock. The awareness clock
    /// never ends the session.
    fn renew_awareness(&mut self) {
        if let Some(json) = self.local_state.clone() {
            self.awareness.set_local_state_raw(json);
            let _ = self.publish_local_awareness();
        }
        self.expire_awareness();
    }

    fn expire_awareness(&mut self) {
        let now = now_millis();
        let expire = millis(self.keepalive.awareness_expire);
        let stale: Vec<ClientID> = self
            .awareness
            .iter()
            .filter(|(client_id, state)| {
                *client_id != self.awareness.client_id()
                    && state
                        .last_updated
                        .checked_add(expire)
                        .is_some_and(|deadline| deadline <= now)
            })
            .map(|(client_id, _)| client_id)
            .collect();
        for client_id in stale {
            self.awareness.remove_state(client_id);
        }
    }

    fn presence(&self) -> Vec<Presence> {
        // The local client is not in `peers`, but it is a participant.
        let mut by_client: HashMap<u64, PeerInfo> = self
            .peers
            .values()
            .filter_map(|peer| {
                peer.awareness_client_id.map(|id| (id, peer.clone()))
            })
            .collect();
        by_client.insert(
            self.awareness.client_id().get(),
            self.session.peer.clone(),
        );
        // Resolution is deferred, not part of applying the update (§8.1): a state that
        // arrived before its document resolves on a later read, not never.
        let txn = self.awareness.doc().transact();
        let mut presence: Vec<Presence> = self
            .awareness
            .iter()
            // A removed client keeps its slot in the awareness map with no data.
            .filter(|(_, state)| state.data.is_some())
            .map(|(client_id, entry)| {
                let state: Option<AwarenessState> = entry
                    .data
                    .as_deref()
                    .and_then(|json| serde_json::from_str(json).ok());
                let resolved =
                    state.as_ref().and_then(|s| resolve_selection(&txn, s));
                Presence {
                    client_id: client_id.get(),
                    peer: by_client.get(&client_id.get()).cloned(),
                    state,
                    resolved,
                }
            })
            .collect();
        presence.sort_by_key(|p| p.client_id);
        presence
    }

    fn peer_list(&self) -> Vec<PeerInfo> {
        let mut peers: Vec<_> = self.peers.values().cloned().collect();
        peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        peers
    }

    // --- wire ----------------------------------------------------------------

    /// Sends a request and keeps its caller waiting: the response with this id answers
    /// it, and the session ending answers it with `Error::Closed`. A request made while
    /// another is outstanding waits for its turn rather than going on the wire behind it
    /// (`PROTOCOL.md` §5).
    fn request(&mut self, pending: Pending) {
        if self.inflight.is_some() {
            self.waiting.push_back(pending);
            return;
        }
        self.send_request(pending);
    }

    /// Puts one request on the wire as the only one in flight.
    fn send_request(&mut self, pending: Pending) {
        self.request_id = self.request_id.wrapping_add(1);
        let id = self.request_id;
        let msg =
            proto::ClientMessage::new(id, pending.method(), pending.params());
        match msg.to_text() {
            Ok(text) => {
                self.inflight = Some((id, pending));
                self.enqueue(Message::text(text));
            }
            Err(e) => {
                // Nothing went out, so there is nothing to wait for.
                pending.answer(Err(Error::Json(e)));
            }
        }
    }

    /// Sends the request next in line, if there is one: the slot the answered request just
    /// left is the next request's, and only one is ever on the wire.
    fn send_next(&mut self) {
        if let Some(next) = self.waiting.pop_front() {
            self.send_request(next);
        }
    }

    /// Answers the caller of a request the server has now answered. An id that is not the
    /// one in flight answers nothing: this client never puts two on the wire, so it can
    /// only be a server frame about a request this connection does not have.
    fn resolve(&mut self, id: u64, msg: &proto::ServerMessage) {
        let Some((inflight, answered)) = self.inflight.take() else {
            return;
        };
        if inflight != id {
            self.inflight = Some((inflight, answered));
            return;
        }
        let outcome = msg.error.as_ref().map_or_else(
            || self.accept(&answered, msg.result.as_ref()),
            |error| {
                Err(Error::Protocol {
                    code: error.code.clone(),
                    message: error.message.clone(),
                })
            },
        );
        answered.answer(outcome);
        self.send_next();
    }

    /// Moves local state to what the server accepted. The room's open-document set comes
    /// from the response, and this client's own holds follow from its own request. A rename
    /// returns `{}` and moves neither: its name arrives as `peer.renamed`.
    fn accept(
        &mut self,
        pending: &Pending,
        result: Option<&serde_json::Value>,
    ) -> Result<(), Error> {
        match pending {
            Pending::Open { path, .. } => {
                self.documents = documents_from(result)?;
                self.hold(path);
            }
            Pending::Close { path, .. } => {
                self.documents = documents_from(result)?;
                self.release(path);
            }
            Pending::Rename { .. } | Pending::Grant { .. } => return Ok(()),
        }
        let _ = self.events.send(EngineEvent::DocumentsChanged {
            documents: self.documents.clone(),
        });
        Ok(())
    }

    /// Adds a path to this client's own open set. The text itself arrives with the document:
    /// creating one here would make an unreceived document look like an empty one, and §8.1
    /// forbids a sender from anchoring a selection against the difference.
    fn hold(&mut self, path: &str) {
        if self.open_documents.iter().all(|p| p != path) {
            self.open_documents.push(path.to_string());
        }
        // A document this replica already holds is watched silently: the adapter is being
        // told the open set right now and can read the text from it. One that has not
        // arrived is watched when it does.
        let _ = self.attach_arrivals();
    }

    /// Removes a path from this client's own open set, and stops watching it: an adapter
    /// that no longer presents a document is not told about it.
    fn release(&mut self, path: &str) {
        self.open_documents.retain(|p| p != path);
        let _ = self.watched.remove(path);
    }

    /// Fails every request still waiting: the session ended before the server answered.
    /// The requests queued behind the in-flight one were never sent and are failed too —
    /// their callers are waiting, and no answer can arrive on a socket that is gone.
    fn fail_pending(&mut self) {
        if let Some((_, pending)) = self.inflight.take() {
            pending.answer(Err(Error::Closed));
        }
        for pending in self.waiting.drain(..) {
            pending.answer(Err(Error::Closed));
        }
    }

    fn publish_local_awareness(&mut self) -> Result<(), Error> {
        let client_id = self.awareness.client_id();
        let update = self
            .awareness
            .update_with_clients([client_id])
            .map_err(|e| Error::Yjs(e.to_string()))?;
        self.enqueue(Message::binary(encode_y_message(&YMessage::Awareness(
            update,
        ))));
        Ok(())
    }

    /// Queues a frame. The run loop decides when it is flushed: while outbound is
    /// paused the queue simply grows, which is what makes two edits concurrent.
    fn enqueue(&mut self, frame: Message) {
        self.queued.push_back(frame);
    }

    async fn flush_outbound(&mut self) -> Result<(), Error> {
        while let Some(frame) = self.queued.pop_front() {
            self.sink.send(frame).await.map_err(Error::Wire)?;
        }
        Ok(())
    }

    /// Handles one y-protocols frame. Decoding, applying and the replies are all done
    /// by `yrs`'s reference protocol implementation.
    fn handle_binary(&mut self, frame: &[u8]) {
        if frame.len() > MAX_INBOUND_BINARY_BYTES {
            // A payload this large never arrives from a conforming transport; applying it
            // would grow this replica without bound on a peer's word. Dropped, and the
            // session continues stale rather than ending.
            return;
        }
        let awareness = carries_awareness(frame);
        let Ok(replies) = DefaultProtocol.handle(&mut self.awareness, frame)
        else {
            // A payload no replica decodes is a peer bug or version skew, and the room
            // never receives it. Say so on the session-error channel rather than drop
            // it quietly: silent loss is the divergence nobody can see.
            let _ = self.events.send(EngineEvent::SessionError {
                code: code::BAD_MESSAGE.to_string(),
                message: "a binary frame could not be decoded".to_string(),
            });
            return;
        };
        for reply in replies {
            self.enqueue(Message::binary(encode_y_message(&reply)));
        }
        // A cursor move is not a text change: an adapter must not re-reconcile every
        // buffer because somebody else's caret moved.
        for path in self.attach_arrivals() {
            let _ = self.events.send(EngineEvent::DocumentChanged { path });
        }
        if awareness {
            let _ = self.events.send(EngineEvent::PresenceChanged {
                presence: self.presence(),
            });
        }
    }

    fn handle_text(&mut self, text: &str) {
        let Ok(msg) = proto::ServerMessage::from_text(text) else {
            // A server frame that does not parse is corruption or version skew, not an
            // event to ignore: it arrives on the same channel a server fault does.
            let _ = self.events.send(EngineEvent::SessionError {
                code: code::BAD_MESSAGE.to_string(),
                message: "a text frame could not be parsed".to_string(),
            });
            return;
        };
        if let Some(id) = msg.id {
            self.resolve(id, &msg);
            return;
        }
        match msg.event.as_deref() {
            Some(event::PEER_JOINED) => self.peer_joined(msg.params.as_ref()),
            Some(event::PEER_LEFT) => self.peer_left(msg.params.as_ref()),
            Some(event::PEER_RENAMED) => self.peer_renamed(msg.params.as_ref()),
            Some(event::DOC_OPENED | event::DOC_CLOSED) => {
                self.documents_changed(msg.params.as_ref());
            }
            Some(event::DOC_GRANTED) => self.grant_changed(msg.params.as_ref()),
            Some(event::HOST_DETACHED) => {
                self.host_detached(msg.params.as_ref());
            }
            Some(event::HOST_ATTACHED) => {
                self.host_attached(msg.params.as_ref());
            }
            Some(event::ROOM_GONE) => self.room_gone(msg.params.as_ref()),
            Some(event::SESSION_ERROR) => {
                self.session_error(msg.params.as_ref());
            }
            _ => {}
        }
    }

    /// A fault the server could not attach to a request id. There is nobody to return it
    /// to, so it goes to the adapter. A terminal code also ends the retries: the same
    /// refusal would greet the next connection. The request in flight is failed on the way
    /// (`PROTOCOL.md` §5): an id-less fault cannot be attributed, and a caller left holding
    /// a request that never completes cannot tell that from a slow server. The one behind
    /// it goes out, so a queue never stalls on a fault that did not end the session.
    fn session_error(&mut self, params: Option<&serde_json::Value>) {
        let code = params
            .and_then(|p| p.get("code"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("error")
            .to_string();
        let message = params
            .and_then(|p| p.get("message"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("the server reported a fault")
            .to_string();
        if is_terminal_code(&code) {
            self.terminal = true;
        }
        if let Some((_, outstanding)) = self.inflight.take() {
            outstanding.answer(Err(Error::Protocol {
                code: code.clone(),
                message: message.clone(),
            }));
            self.send_next();
        }
        let _ = self
            .events
            .send(EngineEvent::SessionError { code, message });
    }

    fn peer_joined(&mut self, params: Option<&serde_json::Value>) {
        let Some(peer) = peer_from(params) else {
            return;
        };
        self.peers.insert(peer.peer_id.clone(), peer);
        let _ = self.events.send(EngineEvent::PeersChanged {
            peers: self.peer_list(),
        });
        // A newcomer has no awareness of us yet; republish ours so their presence
        // list is complete before anyone moves a cursor.
        let _ = self.publish_local_awareness();
    }

    fn peer_left(&mut self, params: Option<&serde_json::Value>) {
        let departed = params
            .and_then(|p| p.get("peer_id"))
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let Some(peer_id) = departed else {
            return;
        };
        if let Some(peer) = self.peers.remove(&peer_id)
            && let Some(client_id) = peer.awareness_client_id
        {
            self.free_awareness(client_id);
        }
        let _ = self.events.send(EngineEvent::PeersChanged {
            peers: self.peer_list(),
        });
        let _ = self.events.send(EngineEvent::PresenceChanged {
            presence: self.presence(),
        });
    }

    /// §8.4: a departed peer's awareness state is dropped — but only while no other seated
    /// peer still claims the id. Nothing requires an id to be unique in a room, so a client
    /// that reuses one after reconnecting makes two peers speak for one replica, and
    /// removing the state on the first departure would erase a cursor the peer that is
    /// still present is holding.
    fn free_awareness(&mut self, client_id: u64) {
        let claimed = self
            .peers
            .values()
            .any(|peer| peer.awareness_client_id == Some(client_id));
        if claimed {
            return;
        }
        self.awareness.remove_state(ClientID::new(client_id));
    }

    /// `peer.renamed` (`PROTOCOL.md` §5): the peer keeps its role and its awareness client
    /// id, and only its name moves. The mover is not in `peers` — it is in `session.peer` —
    /// so a rename of itself is applied there and published to `SyncEngine::session`.
    fn peer_renamed(&mut self, params: Option<&serde_json::Value>) {
        let parsed = params.and_then(|p| {
            serde_json::from_value::<proto::PeerRenamedParams>(p.clone()).ok()
        });
        let Some(renamed) = parsed else {
            return;
        };
        if let Some(peer) = self.peers.get_mut(&renamed.peer_id) {
            peer.display_name.clone_from(&renamed.display_name);
        }
        if renamed.peer_id == self.session.peer.peer_id {
            self.session
                .peer
                .display_name
                .clone_from(&renamed.display_name);
            self.publish_session();
        }
        let _ = self.events.send(EngineEvent::PeersChanged {
            peers: self.peer_list(),
        });
    }

    /// Takes the room's open-document set from an event about it. The event carries the
    /// set itself, so a peer that has just closed a document another peer still holds is
    /// told the document is still open rather than guessing from its own action.
    fn documents_changed(&mut self, params: Option<&serde_json::Value>) {
        let parsed = params.and_then(|p| {
            serde_json::from_value::<proto::DocEvent>(p.clone()).ok()
        });
        if let Some(doc) = parsed {
            self.documents = doc.documents;
        }
        let _ = self.events.send(EngineEvent::DocumentsChanged {
            documents: self.documents.clone(),
        });
    }

    /// Takes the room's grant from a `doc.granted`. The event carries the listing, so a
    /// receiver replaces its view with `paths` and never merges the two: a shorter listing is
    /// a smaller grant, not a partial one.
    fn grant_changed(&mut self, params: Option<&serde_json::Value>) {
        let parsed = params.and_then(|p| {
            serde_json::from_value::<proto::GrantedParams>(p.clone()).ok()
        });
        if let Some(grant) = parsed {
            self.granted_paths = grant.paths;
        }
        let _ = self.events.send(EngineEvent::GrantChanged {
            paths: self.granted_paths.clone(),
        });
    }

    fn host_detached(&self, params: Option<&serde_json::Value>) {
        let grace_ms = params
            .and_then(|p| p.get("grace_ms"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        let _ = self.events.send(EngineEvent::HostDetached { grace_ms });
    }

    fn host_attached(&mut self, params: Option<&serde_json::Value>) {
        let fallback = params.and_then(peer_info);
        let Some(peer) = peer_from(params).or(fallback) else {
            return;
        };
        self.peers.insert(peer.peer_id.clone(), peer.clone());
        let _ = self.events.send(EngineEvent::HostAttached { peer });
        let _ = self.events.send(EngineEvent::PeersChanged {
            peers: self.peer_list(),
        });
    }

    fn room_gone(&mut self, params: Option<&serde_json::Value>) {
        let reason = params
            .and_then(|p| p.get("reason"))
            .and_then(|v| v.as_str())
            .unwrap_or("room gone")
            .to_string();
        // §9: after `room.gone` there is no room to rejoin, on any URL.
        self.terminal = true;
        let _ = self.events.send(EngineEvent::RoomGone { reason });
    }
}

/// A peer record straight out of an event's params.
fn peer_info(params: &serde_json::Value) -> Option<PeerInfo> {
    serde_json::from_value(params.clone()).ok()
}

fn peer_from(params: Option<&serde_json::Value>) -> Option<PeerInfo> {
    let given = params?;
    let value = given.get("peer").unwrap_or(given);
    serde_json::from_value(value.clone()).ok()
}

/// The anchor for an offset in `text`: the element sitting there, or the `tname` form when
/// there is no element to name — the end of the text, or an empty text (§8.1).
///
/// `sticky_index` returns `None` for a position past the end as well, so a caller must check
/// the offset against the text before asking (see `anchor_selection`); here a `None` means the
/// end and nothing else, and the fallback names the text itself rather than the path it was
/// looked up under, so the two cannot drift apart.
fn anchor_at<T: ReadTxn>(txn: &T, text: &TextRef, offset: u32) -> Anchor {
    let sticky = text
        .sticky_index(txn, offset, Assoc::After)
        .unwrap_or_else(|| StickyIndex::from_type(txn, text, Assoc::After));
    Anchor::from_sticky(&sticky)
}

/// Both endpoints of a published selection, resolved against this replica. An endpoint that
/// does not resolve fails the whole selection: §8.1 forbids clamping to a guess.
fn resolve_selection<T: ReadTxn>(
    txn: &T,
    state: &AwarenessState,
) -> Option<SelectionOffsets> {
    let path = state.path.as_deref()?;
    let selection = state.selection.as_ref()?;
    Some(SelectionOffsets {
        anchor: resolve_anchor(txn, path, &selection.anchor)?,
        head: resolve_anchor(txn, path, &selection.head)?,
    })
}

/// The offset an anchor denotes here, or `None` when it names an element this replica has
/// not seen or one living outside the text named by `path`.
///
/// Two checks, both required by §8.1 and neither implying the other: a `tname` must be the
/// document the state was published for, and whatever the anchor resolves to must land in
/// that document's text. The second is what catches an `item` from another type, which a
/// `tname`-less anchor carries no other evidence about.
fn resolve_anchor<T: ReadTxn>(
    txn: &T,
    path: &str,
    anchor: &Anchor,
) -> Option<u32> {
    if !anchor.names_document(path) {
        return None;
    }
    let offset = anchor.to_sticky()?.get_offset(txn)?;
    (offset.branch.id() == BranchID::Root(Arc::from(path)))
        .then_some(offset.index)
}

/// Reports a remote change to `path`. A transaction this client started itself is the
/// adapter's own edit and is not news to it; `yrs` tells the two apart by the origin a write
/// transaction carries.
fn report_change(
    events: &broadcast::Sender<EngineEvent>,
    path: &str,
    txn: &TransactionMut,
) {
    let mine = txn
        .origin()
        .is_some_and(|origin| origin.as_ref() == LOCAL_ORIGIN.as_bytes());
    if mine {
        return;
    }
    let _ = events.send(EngineEvent::DocumentChanged {
        path: path.to_string(),
    });
}

/// Whether a frame carries an awareness message: a state update, or a query. A frame may
/// hold several concatenated y-protocols messages, so this is asked of the whole frame.
///
/// A text message needs no flag of its own: the document it changes reports itself, through
/// the observer a held document carries (`EngineTask::watch`).
fn carries_awareness(frame: &[u8]) -> bool {
    let mut decoder = DecoderV1::new(Cursor::new(frame));
    MessageReader::new(&mut decoder).any(|message| {
        matches!(
            message,
            Ok(YMessage::Awareness(_) | YMessage::AwarenessQuery)
        )
    })
}

fn encode_y_message(message: &YMessage) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    message.encode(&mut encoder);
    encoder.to_vec()
}

/// Milliseconds in a duration, saturating only for absurdly long durations.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn now_millis() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    millis(elapsed)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::error::Error as StdError;
    use std::iter;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures_util::StreamExt;
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, mpsc, oneshot};
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;
    use yrs::ClientID;
    use yrs::sync::Awareness;
    use yrs::sync::AwarenessUpdate;
    use yrs::sync::Message as YMessage;
    use yrs::sync::awareness::AwarenessUpdateEntry;
    use yrs::sync::protocol::SyncMessage;
    use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact};

    use super::{
        EngineTask, MAX_INBOUND_BINARY_BYTES, Sink, Stream, encode_y_message,
        fresh_doc, is_terminal_code, seed_update,
    };
    use crate::editor::EngineEvent;
    use crate::engine::EditOp;
    use crate::presence::PeerInfo;
    use crate::session::ReconnectPolicy;
    use crate::{ConnectOptions, Error, KeepaliveConfig, SessionInfo};
    use selvage_protocol as proto;
    use selvage_protocol::code;

    const PATH: &str = "src/main.rs";
    const OTHER: &str = "src/other.rs";
    const THIRD: &str = "src/third.rs";

    /// The ids of the request envelopes the engine has put on the wire since the last call:
    /// the queue is drained the way the run loop's flush drains it. One id at most is the
    /// one-slot rule of §5.
    fn sent_requests(task: &mut EngineTask) -> Vec<u64> {
        task.queued
            .drain(..)
            .filter_map(|frame| match frame {
                Message::Text(text) => {
                    serde_json::from_str::<proto::ClientMessage>(&text)
                        .ok()
                        .and_then(|msg| msg.id)
                }
                Message::Binary(_)
                | Message::Ping(_)
                | Message::Pong(_)
                | Message::Close(_)
                | Message::Frame(_) => None,
            })
            .collect()
    }

    /// What a caller heard, as the session error it was: `None` when the request was
    /// accepted or failed some other way.
    fn refusal(outcome: &Result<(), Error>) -> Option<(String, String)> {
        match outcome {
            Err(Error::Protocol { code, message }) => {
                Some((code.clone(), message.clone()))
            }
            _ => None,
        }
    }

    /// The state vector of a replica, in the shape the engine reports it.
    fn vector(doc: &Doc) -> Vec<(u64, u32)> {
        let txn = doc.transact();
        let mut entries: Vec<(u64, u32)> = txn
            .state_vector()
            .iter()
            .map(|(client, clock)| (client.get(), *clock))
            .collect();
        entries.sort_unstable();
        entries
    }

    /// A replica that has integrated nothing has no seed to carry, and what it would encode
    /// is the encoding of nothing: applying it is what seating the fresh replica *without*
    /// it does, which is what makes skipping the encode safe.
    #[test]
    fn a_replica_that_holds_nothing_has_no_seed()
    -> Result<(), Box<dyn StdError>> {
        let empty = fresh_doc(None)?;
        assert!(
            seed_update(&empty).is_none(),
            "an empty replica has nothing to carry"
        );

        let encoded = {
            let txn = empty.transact();
            txn.encode_state_as_update_v1(&StateVector::default())
        };
        let nothing = fresh_doc(None)?;
        let carried = fresh_doc(Some(encoded))?;
        assert_eq!(
            vector(&carried),
            vector(&nothing),
            "an empty update applied leaves an empty replica"
        );
        Ok(())
    }

    /// A replica that holds text carries it: the seed is not skipped for a replica with
    /// content, and re-seating with it keeps what the client had.
    #[test]
    fn a_replica_that_holds_text_has_a_seed() -> Result<(), Box<dyn StdError>> {
        let doc = fresh_doc(None)?;
        let text = doc.get_or_insert_text(PATH);
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "fn main() {}");
        drop(txn);

        let seed = seed_update(&doc);
        assert!(seed.is_some(), "a replica holding text has a seed");
        let seated = fresh_doc(seed)?;
        let txn = seated.transact();
        let restored = txn.get_text(PATH).map(|text| text.get_string(&txn));
        assert_eq!(restored.as_deref(), Some("fn main() {}"));
        Ok(())
    }

    /// A refusal a retry cannot change stops the retries (`PROTOCOL.md` §9.1). Every `x.`
    /// code is one of them, known or not: the namespace is reserved for exactly this —
    /// capacity, in this slice — and §9.1 forbids re-helloing a handshake refused with one.
    #[test]
    fn a_refusal_is_terminal_for_every_stopping_code() {
        for name in [
            code::ROOM_UNKNOWN,
            code::TOKEN_INVALID,
            code::HOST_PRESENT,
            code::UNSUPPORTED_VERSION,
            code::ROOM_GONE,
            "x.server_full",
            "x.room_full",
            "x.something.invented.later",
            "x.",
        ] {
            assert!(is_terminal_code(name), "{name} stops the retries");
        }
        for name in [
            "",
            "bad_message",
            "bad_params",
            "unknown_method",
            "X.room_full",
        ] {
            assert!(!is_terminal_code(name), "{name} may be retried");
        }
    }

    /// A refused request is the one in flight and nothing else: the answer is the caller's,
    /// the request behind it goes out, and the connection stays usable.
    #[tokio::test]
    async fn a_response_answers_its_request_and_promotes_the_next()
    -> Result<(), Box<dyn StdError>> {
        let (mut task, _events) = receiving_task().await?;
        let (first, first_reply) = oneshot::channel();
        let (second, mut second_reply) = oneshot::channel();
        task.open(PATH, first);
        task.open(OTHER, second);
        assert_eq!(
            sent_requests(&mut task),
            vec![1],
            "only one request goes out"
        );

        // The server refuses the first: the caller hears it, and the second goes out.
        task.handle_text(
            r#"{"v":"selvage/1","id":1,"error":{"code":"bad_params","message":"no"}}"#,
        );
        let outcome = first_reply.await?;
        assert_eq!(
            refusal(&outcome).map(|(code, _)| code),
            Some(code::BAD_PARAMS.to_string()),
            "the refusal reaches the caller"
        );
        assert_eq!(
            sent_requests(&mut task),
            vec![2],
            "the request behind it goes out, and still only one"
        );
        assert!(
            second_reply.try_recv().is_err(),
            "the second request has not been answered"
        );

        // And its own answer reaches it.
        task.handle_text(
            r#"{"v":"selvage/1","id":2,"result":{"documents":["src/main.rs","src/other.rs"]}}"#,
        );
        second_reply.await??;
        assert_eq!(
            task.open_documents,
            vec![OTHER.to_string()],
            "the refused open left nothing behind; the answered one is held"
        );
        assert!(sent_requests(&mut task).is_empty());
        Ok(())
    }

    /// `PROTOCOL.md` §5: a seated `session.error` carries no `id`, so it cannot be
    /// attributed to a request. The one in flight is failed rather than left hanging, and
    /// the queue moves on — the fault did not end the session.
    #[tokio::test]
    async fn an_idless_session_error_fails_the_outstanding_request()
    -> Result<(), Box<dyn StdError>> {
        let (mut task, mut events) = receiving_task().await?;
        let (first, first_reply) = oneshot::channel();
        let (second, mut second_reply) = oneshot::channel();
        task.open(PATH, first);
        task.open(OTHER, second);
        assert_eq!(sent_requests(&mut task), vec![1]);
        task.handle_text(
            r#"{"v":"selvage/1","event":"session.error","params":{"code":"bad_message","message":"that frame was not an envelope"}}"#,
        );
        let outcome = first_reply.await?;
        let (code, message) =
            refusal(&outcome).expect("the in-flight request to fail");
        assert_eq!(code, code::BAD_MESSAGE);
        assert!(
            message.contains("envelope"),
            "the fault is the server's: {message}"
        );
        assert_eq!(next_session_error(&mut events).await?, code::BAD_MESSAGE);
        assert_eq!(
            sent_requests(&mut task),
            vec![2],
            "the request behind it goes out rather than stalling"
        );
        assert!(second_reply.try_recv().is_err());
        Ok(())
    }

    /// The paths the engine has reported as changed, as far as it has already said so.
    fn changed_documents(
        events: &mut broadcast::Receiver<EngineEvent>,
    ) -> Vec<String> {
        iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event {
                EngineEvent::DocumentChanged { path } => Some(path),
                EngineEvent::DocumentsChanged { .. }
                | EngineEvent::GrantChanged { .. }
                | EngineEvent::PeersChanged { .. }
                | EngineEvent::PresenceChanged { .. }
                | EngineEvent::HostDetached { .. }
                | EngineEvent::HostAttached { .. }
                | EngineEvent::RoomGone { .. }
                | EngineEvent::SessionError { .. }
                | EngineEvent::Reconnecting
                | EngineEvent::Disconnected => None,
            })
            .collect()
    }

    /// A held document that arrives, and one that changed, reach the adapter; and a
    /// document the replica already had does not arrive twice.
    #[tokio::test]
    async fn a_remote_change_reports_only_the_document_it_changed()
    -> Result<(), Box<dyn StdError>> {
        let (mut task, mut events) = receiving_task().await?;
        task.open_documents =
            vec![PATH.to_string(), OTHER.to_string(), THIRD.to_string()];
        // Two of the three are already here — as a re-seat finds the documents it carried —
        // and the third arrives with the frame below.
        let _ = task.doc().get_or_insert_text(PATH);
        let _ = task.doc().get_or_insert_text(OTHER);
        assert_eq!(
            task.attach_arrivals(),
            vec![PATH.to_string(), OTHER.to_string()],
            "what is already here is watched when the seat looks"
        );
        assert!(changed_documents(&mut events).is_empty(), "and is not news");

        // A peer's edit of PATH, as the wire carries it: one sync update.
        let peer = fresh_doc(None)?;
        let text = peer.get_or_insert_text(PATH);
        let mut txn = peer.transact_mut();
        text.insert(&mut txn, 0, "fn main() {}\n");
        drop(txn);
        task.handle_binary(&peer_frame(&peer));

        assert_eq!(
            changed_documents(&mut events),
            vec![PATH.to_string()],
            "one change, for the document that changed"
        );
        assert_eq!(
            task.read_text(PATH),
            "fn main() {}\n",
            "and the frame was applied"
        );
        Ok(())
    }

    /// A document that arrives is announced, and it keeps being announced after: `yrs`
    /// gives the root type an update creates no kind of its own, so nothing fires for the
    /// very update that brings a document a replica did not have, and the observer it needs
    /// is attached when it arrives.
    #[tokio::test]
    async fn a_document_that_arrives_is_announced_once_and_then_watched()
    -> Result<(), Box<dyn StdError>> {
        let (mut task, mut events) = receiving_task().await?;
        task.open_documents = vec![PATH.to_string()];
        assert!(
            task.attach_arrivals().is_empty(),
            "a document that has not arrived cannot be watched"
        );

        let peer = fresh_doc(None)?;
        let text = peer.get_or_insert_text(PATH);
        let mut txn = peer.transact_mut();
        text.insert(&mut txn, 0, "one\n");
        drop(txn);
        task.handle_binary(&peer_frame(&peer));
        assert_eq!(
            changed_documents(&mut events),
            vec![PATH.to_string()],
            "the arrival is announced"
        );

        // A later edit from the same peer is announced too — the case the replica cannot
        // report for a root type it never materialized itself.
        let mut later = peer.transact_mut();
        text.insert(&mut later, 0, "two\n");
        drop(later);
        task.handle_binary(&peer_frame(&peer));
        assert_eq!(
            changed_documents(&mut events),
            vec![PATH.to_string()],
            "and so is the edit after it"
        );
        assert_eq!(task.read_text(PATH), "two\none\n");
        Ok(())
    }

    /// A frame that changes nothing, and an edit this client made itself, reach nobody:
    /// the observer reports a peer's transaction and not the adapter's own.
    #[tokio::test]
    async fn a_local_edit_is_not_reported_back_to_the_adapter()
    -> Result<(), Box<dyn StdError>> {
        let (mut task, mut events) = receiving_task().await?;
        task.open_documents = vec![PATH.to_string()];

        // An empty update: a peer with nothing this replica is missing.
        let peer = fresh_doc(None)?;
        task.handle_binary(&peer_frame(&peer));
        assert!(changed_documents(&mut events).is_empty());
        assert!(
            task.attach_arrivals().is_empty(),
            "an empty update brings no document to watch"
        );

        // A local edit is applied, and reported to nobody: the adapter made it.
        task.apply_edit(
            PATH,
            EditOp::Insert {
                index: 0,
                text: "local\n".to_string(),
            },
        );
        assert!(changed_documents(&mut events).is_empty());
        assert_eq!(task.read_text(PATH), "local\n");
        Ok(())
    }

    /// `doc` as a peer would put it on the wire: one sync update carrying everything it has.
    fn peer_frame(doc: &Doc) -> Vec<u8> {
        let update = {
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&StateVector::default())
        };
        encode_y_message(&YMessage::Sync(SyncMessage::Update(update)))
    }

    /// An over-bound binary frame is dropped, not applied and not reported
    /// (`PROTOCOL.md` §2.1): a payload that large never arrives from a conforming
    /// transport, and applying it would grow the replica without bound on a peer's word.
    /// The session goes on stale rather than ending — ending it would hand any sender a
    /// kill switch — so the next frame still dispatches.
    #[tokio::test]
    async fn an_over_bound_binary_frame_is_dropped()
    -> Result<(), Box<dyn StdError>> {
        let (mut task, mut events) = receiving_task().await?;
        let over = vec![0xA5u8; MAX_INBOUND_BINARY_BYTES.saturating_add(1)];
        task.handle_binary(&over);
        assert!(
            events.try_recv().is_err(),
            "an over-bound frame is dropped in silence"
        );
        assert!(
            task.open_documents.is_empty(),
            "and nothing was held for it"
        );

        // Still dispatching: a frame under the bound is read as it always was, and an
        // undecodable one is still reported.
        task.handle_binary(UNDECODABLE);
        assert_eq!(next_session_error(&mut events).await?, code::BAD_MESSAGE);
        Ok(())
    }

    /// A remote awareness state under `client_id`, as a frame from that peer would leave
    /// it: what a receiver holds for an id before it learns who spoke for it.
    fn inject_state(task: &mut EngineTask, client_id: u64, state: &str) {
        let entry = AwarenessUpdateEntry {
            clock: 1,
            json: state.into(),
        };
        let clients = HashMap::from([(ClientID::new(client_id), entry)]);
        task.awareness
            .apply_update(AwarenessUpdate { clients })
            .expect("the state applies to the replica");
    }

    /// §8.4: an awareness id is freed only once no other seated peer still claims it. Two
    /// peers can speak for one id — a client that reuses one after reconnecting, a claim
    /// that is not validated at all in this slice — and removing the state on the first
    /// departure erases a cursor the peer that is still present is holding.
    #[tokio::test]
    async fn a_departure_keeps_an_awareness_id_another_peer_still_claims()
    -> Result<(), Box<dyn StdError>> {
        let (mut task, _events) = receiving_task().await?;
        let shared_id = 42;
        let claimants = two_claimants(&mut task, shared_id);
        inject_state(&mut task, shared_id, "{}");
        assert!(
            holds(&task, shared_id),
            "the state arrived before anyone left"
        );

        // One of the two leaves: the other still claims the id, so its state stays.
        task.peer_left(Some(&serde_json::json!({ "peer_id": claimants[0] })));
        assert!(
            holds(&task, shared_id),
            "the peer that is still here keeps the state it speaks for"
        );

        // The last claimant leaves: only now is the id free.
        task.peer_left(Some(&serde_json::json!({ "peer_id": claimants[1] })));
        assert!(
            !holds(&task, shared_id),
            "the id is freed once nobody claims it"
        );
        Ok(())
    }

    /// Seats two peers that both claim `client_id`, returning their ids: the room one id
    /// has two speakers in.
    fn two_claimants(
        task: &mut EngineTask,
        client_id: u64,
    ) -> [&'static str; 2] {
        let ids = ["p-one", "p-two"];
        for peer_id in ids {
            let peer = PeerInfo {
                peer_id: peer_id.to_string(),
                display_name: peer_id.to_string(),
                role: proto::Role::Guest,
                awareness_client_id: Some(client_id),
            };
            task.peers.insert(peer_id.to_string(), peer);
        }
        ids
    }

    /// Whether this replica still holds an awareness state for `client_id`.
    fn holds(task: &EngineTask, client_id: u64) -> bool {
        task.presence().iter().any(|p| p.client_id == client_id)
    }

    /// A loopback WebSocket pair: the engine end supplies a real sink and stream, and
    /// nothing connects anywhere.
    async fn loopback_pair() -> Result<(Sink, Stream), Box<dyn StdError>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let accept = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            tokio_tungstenite::accept_async(tcp).await
        });
        let url = format!("ws://{addr}/session");
        let (client, _) = tokio_tungstenite::connect_async(url).await?;
        accept.await??;
        Ok(client.split())
    }

    /// The smallest task that can receive frames: loopback transport, an empty room,
    /// and an event channel the test reads.
    async fn receiving_task()
    -> Result<(EngineTask, broadcast::Receiver<EngineEvent>), Box<dyn StdError>>
    {
        let (sink, stream) = loopback_pair().await?;
        let (events_tx, events_rx) = broadcast::channel(16);
        let (_commands_tx, commands_rx) = mpsc::unbounded_channel();
        let session = SessionInfo {
            room_id: "r-test".to_string(),
            token: None,
            role: proto::Role::Guest,
            peer: proto::PeerInfo {
                peer_id: "p-test".to_string(),
                display_name: "Test".to_string(),
                role: proto::Role::Guest,
                awareness_client_id: None,
            },
            peers: Vec::new(),
            documents: Vec::new(),
            capabilities: Vec::new(),
            keepalive: proto::Keepalive::default(),
            base_url: "ws://127.0.0.1:9".to_string(),
        };
        let task = EngineTask {
            policy: ReconnectPolicy::default(),
            options: ConnectOptions::host("ws://127.0.0.1:9", "Test"),
            sink,
            stream,
            awareness: Awareness::new(Doc::new()),
            session: session.clone(),
            session_slot: Arc::new(Mutex::new(session)),
            room: None,
            token: None,
            terminal: false,
            attempts: 0,
            commands: commands_rx,
            events: events_tx,
            keepalive: KeepaliveConfig::from(proto::Keepalive::default()),
            documents: Vec::new(),
            granted_paths: Vec::new(),
            open_documents: Vec::new(),
            peers: HashMap::new(),
            request_id: 0,
            inflight: None,
            waiting: VecDeque::new(),
            watched: HashMap::new(),
            local_state: None,
            queued: VecDeque::new(),
            paused: false,
        };
        Ok((task, events_rx))
    }

    /// The next session error, or what arrived instead.
    async fn next_session_error(
        events: &mut broadcast::Receiver<EngineEvent>,
    ) -> Result<String, Box<dyn StdError>> {
        let event = timeout(Duration::from_secs(1), events.recv()).await??;
        let EngineEvent::SessionError { code, .. } = event else {
            return Err(
                format!("a session error was expected, got {event:?}").into()
            );
        };
        Ok(code)
    }

    /// An auth denial whose reason is not UTF-8: present on the wire but undecodable.
    /// Truncation alone does not fail decoding — `yrs` reads it as end-of-messages —
    /// so the failing shape has to be semantic, not short.
    const UNDECODABLE: &[u8] = &[0x02, 0x00, 0x02, 0xFF, 0xFF];

    /// An undecodable frame is reported, not dropped: binary garbage and unparsable
    /// text each surface a session error, and the task still dispatches afterwards.
    #[tokio::test]
    async fn undecodable_frames_are_reported() -> Result<(), Box<dyn StdError>>
    {
        let (mut task, mut events) = receiving_task().await?;
        task.handle_binary(UNDECODABLE);
        task.handle_text("{not json");
        assert_eq!(next_session_error(&mut events).await?, code::BAD_MESSAGE);
        assert_eq!(next_session_error(&mut events).await?, code::BAD_MESSAGE);

        // Dispatch still works: a well-formed server fault arrives with its own code.
        task.handle_text(
            r#"{"v":"selvage/1","event":"session.error","params":{"code":"x.test","message":"m"}}"#,
        );
        assert_eq!(next_session_error(&mut events).await?, "x.test");
        Ok(())
    }
}
