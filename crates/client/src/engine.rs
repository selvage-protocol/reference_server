//! The connection task: one task owns the `Y.Doc`, the y-protocols awareness state and
//! the WebSocket, and answers commands from [`crate::SyncEngine`].
//!
//! Everything after the session handshake that is not a session method goes through
//! `yrs::sync::protocol::DefaultProtocol`, i.e. the reference implementation of
//! y-protocols. Nothing in this file invents a document or awareness encoding.

use std::collections::{HashMap, VecDeque};
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
use yrs::sync::protocol::{DefaultProtocol, Protocol as YProtocol};
use yrs::sync::{Awareness, Message as YMessage, SyncMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, ReadTxn, Text as YText, Transact};

use crate::editor::EngineEvent;
use crate::presence::{AwarenessState, PeerInfo, Presence};
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
        state: AwarenessState,
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
    let start = EngineStart {
        sink,
        stream,
        awareness,
        keepalive: options.keepalive,
        local_state: Some(local_state),
    };
    Ok((spawn(start, &session), session))
}

/// Opens the socket, seeds the local `Y.Doc` and sends `session.hello`.
async fn greet(
    options: &ConnectOptions,
    url: &str,
) -> Result<(Sink, Stream, Awareness), Error> {
    let doc = Doc::new();
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
            Command::SetAwareness { state, reply } => {
                let result = self.set_local_awareness(&state);
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

    fn open(&mut self, path: &str, reply: oneshot::Sender<Result<(), Error>>) {
        if self.open_documents.iter().all(|p| p != path) {
            self.open_documents.push(path.to_string());
        }
        if self.documents.iter().all(|p| p != path) {
            self.documents.push(path.to_string());
        }
        self.doc().get_or_insert_text(path);
        let result =
            self.request(method::DOC_OPEN, serde_json::json!({ "path": path }));
        let _ = reply.send(result);
        let _ = self.events.send(EngineEvent::DocumentsChanged {
            documents: self.documents.clone(),
        });
    }

    fn close(&mut self, path: &str, reply: oneshot::Sender<Result<(), Error>>) {
        self.open_documents.retain(|p| p != path);
        self.documents.retain(|p| p != path);
        let result = self
            .request(method::DOC_CLOSE, serde_json::json!({ "path": path }));
        let _ = reply.send(result);
        let _ = self.events.send(EngineEvent::DocumentsChanged {
            documents: self.documents.clone(),
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

    fn set_local_awareness(
        &mut self,
        state: &AwarenessState,
    ) -> Result<(), Error> {
        let json = serde_json::to_string(state)?;
        self.awareness.set_local_state_raw(json.clone());
        self.local_state = Some(json);
        self.publish_local_awareness()?;
        let _ = self.events.send(EngineEvent::PresenceChanged {
            presence: self.presence(),
        });
        Ok(())
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
        let mut presence: Vec<Presence> = self
            .awareness
            .iter()
            // A removed client keeps its slot in the awareness map with no data.
            .filter(|(_, state)| state.data.is_some())
            .map(|(client_id, state)| Presence {
                client_id: client_id.get(),
                peer: by_client.get(&client_id.get()).cloned(),
                state: state
                    .data
                    .as_deref()
                    .and_then(|json| serde_json::from_str(json).ok()),
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

    fn request(
        &mut self,
        method_name: &str,
        params: serde_json::Value,
    ) -> Result<(), Error> {
        self.request_id = self.request_id.wrapping_add(1);
        let msg =
            proto::ClientMessage::new(self.request_id, method_name, params);
        self.enqueue(Message::text(msg.to_text()?));
        Ok(())
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
        let Ok(replies) = DefaultProtocol.handle(&mut self.awareness, frame)
        else {
            // A payload we cannot decode is a peer bug; keep serving the session.
            return;
        };
        for reply in replies {
            self.enqueue(Message::binary(encode_y_message(&reply)));
        }
        let _ = self.events.send(EngineEvent::PresenceChanged {
            presence: self.presence(),
        });
        for path in self.open_documents.clone() {
            let _ = self.events.send(EngineEvent::DocumentChanged { path });
        }
    }

    fn handle_text(&mut self, text: &str) {
        let Ok(msg) = serde_json::from_str::<proto::ServerMessage>(text) else {
            return;
        };
        match msg.event.as_deref() {
            Some(event::PEER_JOINED) => self.peer_joined(msg.params.as_ref()),
            Some(event::PEER_LEFT) => self.peer_left(msg.params.as_ref()),
            Some(event::DOC_OPENED) => {
                self.document_changed(msg.params.as_ref(), true);
            }
            Some(event::DOC_CLOSED) => {
                self.document_changed(msg.params.as_ref(), false);
            }
            Some(event::HOST_DETACHED) => {
                self.host_detached(msg.params.as_ref());
            }
            Some(event::HOST_ATTACHED) => {
                self.host_attached(msg.params.as_ref());
            }
            Some(event::ROOM_GONE) => self.room_gone(msg.params.as_ref()),
            _ => {}
        }
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

    fn document_changed(
        &mut self,
        params: Option<&serde_json::Value>,
        opened: bool,
    ) {
        let parsed = params.and_then(|p| {
            serde_json::from_value::<proto::DocEvent>(p.clone()).ok()
        });
        if let Some(doc) = parsed {
            self.record_document(&doc.path, opened);
        }
        let _ = self.events.send(EngineEvent::DocumentsChanged {
            documents: self.documents.clone(),
        });
    }

    /// Adds or removes a path from the room's open-document set.
    fn record_document(&mut self, path: &str, opened: bool) {
        if !opened {
            self.documents.retain(|p| p != path);
            return;
        }
        if !self.documents.iter().any(|p| p == path) {
            self.documents.push(path.to_string());
        }
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
