//! The connection task: one task owns the `Y.Doc`, the y-protocols awareness state and
//! the WebSocket, and answers commands from [`crate::SyncEngine`].
//!
//! Everything after the session handshake that is not a session method goes through
//! `yrs::sync::protocol::DefaultProtocol`, i.e. the reference implementation of
//! y-protocols. Nothing in this file invents a document or awareness encoding.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Interval, MissedTickBehavior, interval};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Error as WireError;
use tokio_tungstenite::tungstenite::Message;

use selvage_protocol as proto;
use selvage_protocol::{event, method};
use yrs::block::ClientID;
use yrs::encoding::read::Cursor;
use yrs::sync::protocol::{
    DefaultProtocol, MessageReader, Protocol as YProtocol,
};
use yrs::sync::{Awareness, Message as YMessage, SyncMessage};
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{
    Assoc, BranchID, Doc, GetString, IndexedSequence, OffsetKind, Options,
    ReadTxn, StickyIndex, Text as YText, TextRef, Transact,
};

use crate::editor::EngineEvent;
use crate::presence::{
    Anchor, AwarenessState, PeerInfo, Presence, Selection, SelectionOffsets,
};
use crate::{ConnectOptions, Error, KeepaliveConfig, SessionInfo};

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
}

/// What the task needs before it can start the connection task.
struct EngineStart {
    sink: Sink,
    stream: Stream,
    awareness: Awareness,
    keepalive: KeepaliveConfig,
    local_state: Option<String>,
}

/// Runs the whole connection attempt: socket, handshake, then the task.
///
/// # Errors
///
/// Returns [`Error`] when the socket cannot be opened, the hello cannot be sent, or the
/// server refuses the session.
pub async fn connect(
    options: ConnectOptions,
) -> Result<(Channel, SessionInfo), Error> {
    let url = proto::session_url(
        &options.base_url,
        options.room.as_deref(),
        options.token.as_deref(),
    );
    let local_state = serde_json::to_string(&options.initial_awareness)?;
    let (sink, mut stream, awareness) = greet(&options, &url).await?;
    let session = await_session(&mut stream, &options.base_url).await?;
    // The server advertises the session's keepalive; a caller that did not override it
    // runs on the server's clock, so both ends measure awareness the same way.
    let keepalive = options
        .keepalive
        .unwrap_or_else(|| KeepaliveConfig::from(session.keepalive));
    let start = EngineStart {
        sink,
        stream,
        awareness,
        keepalive,
        local_state: Some(local_state),
    };
    Ok((spawn(start, &session), session))
}

/// Opens the socket, seeds the local `Y.Doc` and sends `session.hello`.
async fn greet(
    options: &ConnectOptions,
    url: &str,
) -> Result<(Sink, Stream, Awareness), Error> {
    // A text offset on this API is a UTF-16 code unit, the unit `yjs`, every editor's
    // `offsetAt` and every peer on the wire use (spec/PROTOCOL.md §8.1). `yrs` defaults to
    // UTF-8 byte offsets, which would put a cursor after the first non-BMP character
    // somewhere else than every other implementation does.
    let doc = Doc::with_options(Options {
        offset_kind: OffsetKind::Utf16,
        ..Options::default()
    });
    let awareness_client_id = doc.client_id().get();
    let mut awareness = Awareness::new(doc);
    awareness.set_local_state_raw(serde_json::to_string(
        &options.initial_awareness,
    )?);

    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(Error::Wire)?;
    let (mut sink, stream) = ws.split();

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
    Ok((sink, stream, awareness))
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
    let msg: proto::ServerMessage = serde_json::from_str(text)?;
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

fn spawn(start: EngineStart, session: &SessionInfo) -> Channel {
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (events_tx, _) = broadcast::channel(64);
    let task = EngineTask {
        sink: start.sink,
        stream: start.stream,
        awareness: start.awareness,
        session: session.clone(),
        commands: commands_rx,
        events: events_tx.clone(),
        keepalive: start.keepalive,
        documents: session.documents.clone(),
        open_documents: Vec::new(),
        peers: session
            .peers
            .iter()
            .map(|peer| (peer.peer_id.clone(), peer.clone()))
            .collect(),
        request_id: 1,
        pending: HashMap::new(),
        local_state: start.local_state,
        queued: VecDeque::new(),
        paused: false,
    };
    tokio::spawn(task.run());
    Channel {
        commands: commands_tx,
        events: events_tx,
    }
}

/// Runs the turn loop until the session ends. Returns `false` when the socket failed
/// rather than the session ending cleanly.
async fn session(task: &mut EngineTask, renew: &mut Interval) -> bool {
    if task.flush_outbound().await.is_err() {
        return false;
    }
    loop {
        if !task.turn(renew).await {
            return true;
        }
        // One flush per turn: every path only queues frames, and a paused client keeps
        // queueing until it is resumed.
        if task.paused {
            continue;
        }
        if task.flush_outbound().await.is_err() {
            return false;
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
}

impl Pending {
    /// The session method this request calls.
    const fn method(&self) -> &'static str {
        match self {
            Self::Open { .. } => method::DOC_OPEN,
            Self::Close { .. } => method::DOC_CLOSE,
        }
    }

    /// The params this request carries. They come from the same fields acceptance reads,
    /// so the request and its meaning cannot drift apart.
    fn params(&self) -> serde_json::Value {
        let path = match self {
            Self::Open { path, .. } | Self::Close { path, .. } => path,
        };
        serde_json::json!({ "path": path })
    }

    /// Answers the caller. The response is what carries the outcome, so this is called
    /// once the server has spoken — or once it never will.
    fn answer(self, outcome: Result<(), Error>) {
        let reply = match self {
            Self::Open { reply, .. } | Self::Close { reply, .. } => reply,
        };
        let _ = reply.send(outcome);
    }
}

struct EngineTask {
    sink: Sink,
    stream: Stream,
    awareness: Awareness,
    session: SessionInfo,
    commands: mpsc::UnboundedReceiver<Command>,
    events: broadcast::Sender<EngineEvent>,
    keepalive: KeepaliveConfig,
    /// The room's open-document set, as owned by the server.
    documents: Vec<String>,
    /// The documents this client has open, in the order it opened them.
    open_documents: Vec<String>,
    peers: HashMap<String, PeerInfo>,
    request_id: u64,
    /// Requests the server has not answered yet, by request id.
    pending: HashMap<u64, Pending>,
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

        // Open the document sync handshake: peers reply with the state we are missing.
        let step1 = {
            let txn = self.doc().transact();
            encode_y_message(&YMessage::Sync(SyncMessage::SyncStep1(
                txn.state_vector(),
            )))
        };
        self.enqueue(Message::binary(step1));
        let _ = self.publish_local_awareness();
        if !session(&mut self, &mut renew).await {
            self.disconnect();
        }
        // Whatever is still outstanding can never be answered now.
        self.fail_pending();
    }

    /// One turn of the session: waits on commands, the awareness clock and the socket,
    /// and reports whether the session should continue.
    async fn turn(&mut self, renew: &mut Interval) -> bool {
        tokio::select! {
            command = self.commands.recv() => self.handle_command(command).await,
            _ = renew.tick() => self.renew_awareness(),
            incoming = self.stream.next() => self.handle_incoming(incoming),
        }
    }

    /// Handles one inbound frame. Returns `false` when the session must stop.
    fn handle_incoming(
        &mut self,
        incoming: Option<Result<Message, WireError>>,
    ) -> bool {
        match incoming {
            Some(Ok(Message::Binary(frame))) => self.handle_binary(&frame),
            Some(Ok(Message::Text(text))) => self.handle_text(&text),
            Some(Ok(Message::Close(_)) | Err(_)) | None => return false,
            Some(Ok(_)) => {}
        }
        true
    }

    fn disconnect(&self) {
        let _ = self.events.send(EngineEvent::Disconnected);
    }

    // --- commands ------------------------------------------------------------

    /// Runs one command. Returns `false` when the task must stop.
    async fn handle_command(&mut self, command: Option<Command>) -> bool {
        let Some(next) = command else {
            return false;
        };
        match next {
            Command::Open { path, reply } => self.open(&path, reply),
            Command::Close { path, reply } => self.close(&path, reply),
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
            Command::OpenDocuments { reply } => {
                let _ = reply.send(self.open_documents.clone());
            }
            Command::SetOutboundPaused { paused, reply } => {
                let result = self.set_paused(paused).await;
                let _ = reply.send(result);
            }
            Command::Shutdown { reply } => return self.shutdown(reply).await,
        }
        true
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

    /// Applies a local edit and sends exactly the delta it produced. Sending the delta
    /// rather than the whole state keeps the frame small and stays inside y-protocols.
    fn apply_edit(&mut self, path: &str, op: EditOp) {
        let before = {
            let txn = self.doc().transact();
            txn.state_vector()
        };
        self.mutate_text(path, op);
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
        let mut txn = self.doc().transact_mut();
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

    async fn shutdown(&mut self, reply: Option<oneshot::Sender<()>>) -> bool {
        let _ = self.flush_outbound().await;
        let _ = self.sink.close().await;
        if let Some(ack) = reply {
            let _ = ack.send(());
        }
        false
    }

    // --- state ---------------------------------------------------------------

    fn doc(&mut self) -> &mut Doc {
        self.awareness.doc_mut()
    }

    fn read_text(&mut self, path: &str) -> String {
        let text = self.doc().get_or_insert_text(path);
        let txn = self.doc().transact();
        text.get_string(&txn)
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
    /// anchors the wire carries (`spec/PROTOCOL.md` §8.1).
    fn set_local_awareness(
        &mut self,
        path: Option<String>,
        selection: Option<SelectionOffsets>,
    ) -> Result<(), Error> {
        let state = self.anchored_state(path, selection);
        let json = serde_json::to_string(&state)?;
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
                Some(self.anchor_selection(target, offsets))
            }
            _ => None,
        };
        AwarenessState {
            path,
            selection: anchored,
        }
    }

    /// Anchors both endpoints of a selection against this replica.
    fn anchor_selection(
        &mut self,
        path: &str,
        offsets: SelectionOffsets,
    ) -> Selection {
        // `get_or_insert_text` takes a transaction of its own, so the handle has to be in
        // hand before one is held; taking it under a live transaction deadlocks the task.
        let text = self.doc().get_or_insert_text(path);
        let txn = self.doc().transact();
        Selection {
            anchor: anchor_at(&txn, &text, offsets.anchor),
            head: anchor_at(&txn, &text, offsets.head),
        }
    }

    /// y-protocols awareness renews every 15s and expires at 30s: renewal means
    /// republishing the same state so peers see a newer clock. The awareness clock
    /// never ends the session.
    fn renew_awareness(&mut self) -> bool {
        if let Some(json) = self.local_state.clone() {
            self.awareness.set_local_state_raw(json);
            let _ = self.publish_local_awareness();
        }
        self.expire_awareness();
        true
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
    /// it, and the session ending answers it with `Error::Closed`.
    fn request(&mut self, pending: Pending) {
        self.request_id = self.request_id.wrapping_add(1);
        let id = self.request_id;
        let msg =
            proto::ClientMessage::new(id, pending.method(), pending.params());
        match msg.to_text() {
            Ok(text) => {
                self.pending.insert(id, pending);
                self.enqueue(Message::text(text));
            }
            Err(e) => {
                // Nothing went out, so there is nothing to wait for.
                pending.answer(Err(Error::Json(e)));
            }
        }
    }

    /// Answers the caller of a request the server has now answered.
    fn resolve(&mut self, id: u64, msg: &proto::ServerMessage) {
        let Some(pending) = self.pending.remove(&id) else {
            return;
        };
        let outcome = msg.error.as_ref().map_or_else(
            || self.accept(&pending, msg.result.as_ref()),
            |error| {
                Err(Error::Protocol {
                    code: error.code.clone(),
                    message: error.message.clone(),
                })
            },
        );
        pending.answer(outcome);
    }

    /// Moves local state to what the server accepted. The room's open-document set comes
    /// from the response, and this client's own holds follow from its own request.
    fn accept(
        &mut self,
        pending: &Pending,
        result: Option<&serde_json::Value>,
    ) -> Result<(), Error> {
        let body = result.cloned().unwrap_or_else(|| serde_json::json!({}));
        let accepted: proto::DocSet = serde_json::from_value(body)?;
        self.documents = accepted.documents;
        match pending {
            Pending::Open { path, .. } => self.hold(path),
            Pending::Close { path, .. } => self.release(path),
        }
        let _ = self.events.send(EngineEvent::DocumentsChanged {
            documents: self.documents.clone(),
        });
        Ok(())
    }

    /// Adds a path to this client's own open set, and gives it the text to edit.
    fn hold(&mut self, path: &str) {
        if self.open_documents.iter().all(|p| p != path) {
            self.open_documents.push(path.to_string());
        }
        self.doc().get_or_insert_text(path);
    }

    /// Removes a path from this client's own open set.
    fn release(&mut self, path: &str) {
        self.open_documents.retain(|p| p != path);
    }

    /// Fails every request still waiting: the session ended before the server answered.
    fn fail_pending(&mut self) {
        for (_, pending) in self.pending.drain() {
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
        let kinds = kinds_in(frame);
        let Ok(replies) = DefaultProtocol.handle(&mut self.awareness, frame)
        else {
            // A payload we cannot decode is a peer bug; keep serving the session.
            return;
        };
        for reply in replies {
            self.enqueue(Message::binary(encode_y_message(&reply)));
        }
        // A cursor move is not a text change: an adapter must not re-reconcile every
        // buffer because somebody else's caret moved.
        if kinds.has(FrameKinds::TEXT) {
            self.notify_documents();
        }
        if kinds.has(FrameKinds::AWARENESS) {
            let _ = self.events.send(EngineEvent::PresenceChanged {
                presence: self.presence(),
            });
        }
    }

    /// Tells the adapter that the documents this client has open may have changed, because
    /// a frame carried text.
    fn notify_documents(&self) {
        for path in self.open_documents.clone() {
            let _ = self.events.send(EngineEvent::DocumentChanged { path });
        }
    }

    fn handle_text(&mut self, text: &str) {
        let Ok(msg) = serde_json::from_str::<proto::ServerMessage>(text) else {
            return;
        };
        if let Some(id) = msg.id {
            self.resolve(id, &msg);
            return;
        }
        match msg.event.as_deref() {
            Some(event::PEER_JOINED) => self.peer_joined(msg.params.as_ref()),
            Some(event::PEER_LEFT) => self.peer_left(msg.params.as_ref()),
            Some(event::DOC_OPENED | event::DOC_CLOSED) => {
                self.documents_changed(msg.params.as_ref());
            }
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
    /// to, so it goes to the adapter.
    fn session_error(&self, params: Option<&serde_json::Value>) {
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
            self.awareness.remove_state(ClientID::new(client_id));
        }
        let _ = self.events.send(EngineEvent::PeersChanged {
            peers: self.peer_list(),
        });
        let _ = self.events.send(EngineEvent::PresenceChanged {
            presence: self.presence(),
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

    fn room_gone(&self, params: Option<&serde_json::Value>) {
        let reason = params
            .and_then(|p| p.get("reason"))
            .and_then(|v| v.as_str())
            .unwrap_or("room gone")
            .to_string();
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
/// The fallback names the text itself rather than the path it was looked up under, so the two
/// cannot drift apart.
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

/// What a binary frame told this session about, one bit per kind: a frame may hold several
/// concatenated y-protocols messages, so it can be more than one thing at once.
#[derive(Debug, Default, Clone, Copy)]
struct FrameKinds(u8);

impl FrameKinds {
    /// A message that can change the document text: a sync update, or a sync step 2.
    const TEXT: u8 = 1;
    /// A message that can change awareness: a state update, or a query.
    const AWARENESS: u8 = 2;

    const fn has(self, kind: u8) -> bool {
        self.0 & kind != 0
    }

    const fn add(self, kind: u8) -> Self {
        Self(self.0 | kind)
    }
}

/// Reads the message types out of a frame. A frame that cannot be read reports neither
/// kind; the protocol handler rejects it too, so nothing is lost.
fn kinds_in(frame: &[u8]) -> FrameKinds {
    let mut kinds = FrameKinds::default();
    let mut decoder = DecoderV1::new(Cursor::new(frame));
    for message in MessageReader::new(&mut decoder) {
        kinds = match message {
            Ok(YMessage::Sync(
                SyncMessage::SyncStep2(_) | SyncMessage::Update(_),
            )) => kinds.add(FrameKinds::TEXT),
            Ok(YMessage::Awareness(_) | YMessage::AwarenessQuery) => {
                kinds.add(FrameKinds::AWARENESS)
            }
            Ok(
                YMessage::Sync(SyncMessage::SyncStep1(_))
                | YMessage::Auth(_)
                | YMessage::Custom(..),
            )
            | Err(_) => kinds,
        };
    }
    kinds
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
