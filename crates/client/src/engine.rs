//! The connection task: one task owns the `Y.Doc`, the y-protocols awareness state and
//! the WebSocket, and answers commands from [`crate::SyncEngine`].
//!
//! Everything after the session handshake that is not a session method goes through
//! `yrs::sync::protocol::DefaultProtocol`, i.e. the reference implementation of
//! y-protocols. Nothing in this file invents a document or awareness encoding.

use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::MaybeTlsStream;

use selvage_protocol as proto;
use yrs::sync::protocol::{DefaultProtocol, Protocol as YProtocol};
use yrs::sync::{Awareness, Message as YMessage, SyncMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::block::ClientID;
use yrs::{Doc, GetString, ReadTxn, Transact, Text as YText};

use crate::editor::EngineEvent;
use crate::presence::{AwarenessState, PeerInfo, Presence};
use crate::{Error, KeepaliveConfig, SessionInfo};

type Socket = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
pub(crate) type Sink = SplitSink<Socket, Message>;
pub(crate) type Stream = SplitStream<Socket>;

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

pub struct EngineTask {
    pub sink: Sink,
    pub stream: Stream,
    pub awareness: Awareness,
    #[allow(dead_code)]
    pub session: SessionInfo,
    pub commands: mpsc::UnboundedReceiver<Command>,
    pub events: broadcast::Sender<EngineEvent>,
    pub keepalive: KeepaliveConfig,
    /// The room's open-document set, as owned by the server.
    pub documents: Vec<String>,
    /// The documents this client has open, in the order it opened them.
    pub open_documents: Vec<String>,
    pub peers: HashMap<String, PeerInfo>,
    pub request_id: u64,
    /// The JSON published for the local client, replayed on every renewal.
    pub local_state: Option<String>,
    /// Frames produced while outbound is paused, flushed on resume. Text frames carry
    /// the JSON session envelope; binary frames are y-protocols payloads.
    pub queued: VecDeque<Message>,
    pub paused: bool,
}

impl EngineTask {
    pub async fn run(mut self) {
        let mut renew = tokio::time::interval(self.keepalive.awareness_renew);
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renew.tick().await;

        // Open the document sync handshake: peers reply with the state we are missing.
        let step1 = {
            let txn = self.doc().transact();
            encode_y_message(&YMessage::Sync(SyncMessage::SyncStep1(txn.state_vector())))
        };
        self.queued.push_back(Message::binary(step1));
        self.publish_local_awareness().ok();
        if self.flush_outbound().await.is_err() {
            let _ = self.events.send(EngineEvent::Disconnected);
            return;
        }

        loop {
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(Command::Shutdown { reply }) => {
                        let _ = self.flush_outbound().await;
                        let _ = self.sink.close().await;
                        if let Some(reply) = reply {
                            let _ = reply.send(());
                        }
                        break;
                    }
                    Some(command) => self.handle_command(command).await,
                    None => break,
                },
                _ = renew.tick() => self.renew_awareness().await,
                incoming = self.stream.next() => match incoming {
                    Some(Ok(Message::Binary(frame))) => self.handle_binary(&frame).await,
                    Some(Ok(Message::Text(text))) => self.handle_text(&text).await,
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        let _ = self.events.send(EngineEvent::Disconnected);
                        break;
                    }
                    Some(Ok(_)) => {}
                },
            }
            // One flush per turn: every path above only queues frames, and a paused
            // client keeps queueing until it is resumed.
            if !self.paused && self.flush_outbound().await.is_err() {
                let _ = self.events.send(EngineEvent::Disconnected);
                break;
            }
        }
    }

    // --- commands ------------------------------------------------------------

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Open { path, reply } => {
                if self.open_documents.iter().all(|p| p != &path) {
                    self.open_documents.push(path.clone());
                }
                if self.documents.iter().all(|p| p != &path) {
                    self.documents.push(path.clone());
                }
                self.doc().get_or_insert_text(path.as_str());
                let result = self
                    .request(proto::method::DOC_OPEN, serde_json::json!({ "path": path }))
                    .await;
                let _ = reply.send(result);
                let _ = self.events.send(EngineEvent::DocumentsChanged {
                    documents: self.documents.clone(),
                });
            }
            Command::Close { path, reply } => {
                self.open_documents.retain(|p| p != &path);
                self.documents.retain(|p| p != &path);
                let result = self
                    .request(proto::method::DOC_CLOSE, serde_json::json!({ "path": path }))
                    .await;
                let _ = reply.send(result);
                let _ = self.events.send(EngineEvent::DocumentsChanged {
                    documents: self.documents.clone(),
                });
            }
            Command::Text { path, reply } => {
                let _ = reply.send(self.read_text(&path));
            }
            Command::Edit { path, op, reply } => {
                let result = self.apply_edit(&path, op).await;
                let _ = reply.send(result);
            }
            Command::SetAwareness { state, reply } => {
                let result = self.set_local_awareness(state).await;
                let _ = reply.send(result);
            }
            Command::Presence { reply } => {
                let _ = reply.send(self.presence());
            }
            Command::Peers { reply } => {
                let _ = reply.send(self.peer_list());
            }
            Command::StateVector { reply } => {
                let txn = self.doc().transact();
                let mut entries: Vec<(u64, u32)> = txn
                    .state_vector()
                    .iter()
                    .map(|(client, clock)| (client.get(), *clock))
                    .collect();
                entries.sort_unstable();
                let _ = reply.send(entries);
            }
            Command::Documents { reply } => {
                let _ = reply.send(self.documents.clone());
            }
            Command::OpenDocuments { reply } => {
                let _ = reply.send(self.open_documents.clone());
            }
            Command::SetOutboundPaused { paused, reply } => {
                self.paused = paused;
                let result = if paused {
                    Ok(())
                } else {
                    self.flush_outbound().await
                };
                let _ = reply.send(result);
            }
            Command::Shutdown { .. } => unreachable!("shutdown is handled by the run loop"),
        }
    }

    fn doc(&mut self) -> &mut Doc {
        self.awareness.doc_mut()
    }

    fn read_text(&mut self, path: &str) -> String {
        let text = self.doc().get_or_insert_text(path);
        let txn = self.doc().transact();
        text.get_string(&txn)
    }

    /// Applies a local edit and sends exactly the delta it produced. Sending the delta
    /// rather than the whole state keeps the frame small and stays inside y-protocols.
    async fn apply_edit(&mut self, path: &str, op: EditOp) -> Result<(), Error> {
        let before = {
            let txn = self.doc().transact();
            txn.state_vector()
        };
        {
            let text = self.doc().get_or_insert_text(path);
            let mut txn = self.doc().transact_mut();
            match op {
                EditOp::Insert { index, text: chunk } => text.insert(&mut txn, index, &chunk),
                EditOp::Delete { index, len } => text.remove_range(&mut txn, index, len),
            }
        }
        let update = {
            let txn = self.doc().transact();
            txn.encode_state_as_update_v1(&before)
        };
        if !update.is_empty() {
            self.enqueue(Message::binary(encode_y_message(&YMessage::Sync(
                SyncMessage::Update(update),
            ))))?;
        }
        Ok(())
    }

    async fn set_local_awareness(&mut self, state: AwarenessState) -> Result<(), Error> {
        let json = serde_json::to_string(&state).map_err(Error::Json)?;
        self.awareness.set_local_state_raw(json.clone());
        self.local_state = Some(json);
        self.publish_local_awareness()?;
        let _ = self.events.send(EngineEvent::PresenceChanged {
            presence: self.presence(),
        });
        Ok(())
    }

    /// y-protocols awareness renews every 15s and expires at 30s: renewal means
    /// republishing the same state so peers see a newer clock.
    async fn renew_awareness(&mut self) {
        if let Some(json) = self.local_state.clone() {
            self.awareness.set_local_state_raw(json);
            let _ = self.publish_local_awareness();
        }
        self.expire_awareness();
    }

    fn expire_awareness(&mut self) {
        let now = now_millis();
        let expire = self.keepalive.awareness_expire.as_millis() as u64;
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
        let mut by_client: HashMap<u64, &PeerInfo> = self
            .peers
            .values()
            .filter_map(|peer| peer.awareness_client_id.map(|id| (id, peer)))
            .collect();
        by_client.insert(self.awareness.client_id().get(), &self.session.peer);
        let mut presence: Vec<Presence> = self
            .awareness
            .iter()
            // A removed client keeps its slot in the awareness map with no data.
            .filter(|(_, state)| state.data.is_some())
            .map(|(client_id, state)| Presence {
                client_id: client_id.get(),
                peer: by_client.get(&client_id.get()).map(|p| (*p).clone()),
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

    async fn request(&mut self, method: &str, params: serde_json::Value) -> Result<(), Error> {
        self.request_id += 1;
        let msg = proto::ClientMessage::new(self.request_id, method, params);
        self.enqueue(Message::text(msg.to_text()))
    }

    fn publish_local_awareness(&mut self) -> Result<(), Error> {
        let client_id = self.awareness.client_id();
        let update = self
            .awareness
            .update_with_clients([client_id])
            .map_err(|e| Error::Yjs(e.to_string()))?;
        self.enqueue(Message::binary(encode_y_message(&YMessage::Awareness(update))))
    }

    /// Queues a frame, honouring an explicit outbound pause. Pausing is what lets a
    /// caller make two genuinely concurrent edits; it is also what a reconnect buffer
    /// would need.
    fn enqueue(&mut self, frame: Message) -> Result<(), Error> {
        self.queued.push_back(frame);
        if self.paused {
            return Ok(());
        }
        // The flush happens on the next task poll; `select!` regains control immediately
        // after the current command completes.
        Ok(())
    }

    async fn flush_outbound(&mut self) -> Result<(), Error> {
        while let Some(frame) = self.queued.pop_front() {
            self.sink.send(frame).await.map_err(Error::Wire)?;
        }
        Ok(())
    }

    /// Handles one y-protocols frame. Decoding, applying and the replies are all done
    /// by `yrs`'s reference protocol implementation.
    async fn handle_binary(&mut self, frame: &[u8]) {
        let replies = match DefaultProtocol.handle(&mut self.awareness, frame) {
            Ok(replies) => replies,
            Err(_) => {
                // A payload we cannot decode is a peer bug; keep serving the session.
                return;
            }
        };
        for reply in replies {
            self.queued.push_back(Message::binary(encode_y_message(&reply)));
        }
        let _ = self.events.send(EngineEvent::PresenceChanged {
            presence: self.presence(),
        });
        for path in self.open_documents.clone() {
            let _ = self.events.send(EngineEvent::DocumentChanged { path });
        }
    }

    async fn handle_text(&mut self, text: &str) {
        let Ok(msg) = serde_json::from_str::<proto::ServerMessage>(text) else {
            return;
        };
        match msg.event.as_deref() {
            Some(proto::event::PEER_JOINED) => {
                if let Some(peer) = peer_from(msg.params.as_ref()) {
                    self.peers.insert(peer.peer_id.clone(), peer);
                    let _ = self.events.send(EngineEvent::PeersChanged {
                        peers: self.peer_list(),
                    });
                    // A newcomer has no awareness of us yet; republish ours so their
                    // presence list is complete before anyone moves a cursor.
                    let _ = self.publish_local_awareness();
                }
            }
            Some(proto::event::PEER_LEFT) => {
                let peer_id = msg
                    .params
                    .as_ref()
                    .and_then(|p| p.get("peer_id"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                if let Some(peer_id) = peer_id {
                    if let Some(peer) = self.peers.remove(&peer_id) {
                        if let Some(client_id) = peer.awareness_client_id {
                            self.awareness.remove_state(ClientID::new(client_id));
                        }
                    }
                    let _ = self.events.send(EngineEvent::PeersChanged {
                        peers: self.peer_list(),
                    });
                    let _ = self.events.send(EngineEvent::PresenceChanged {
                        presence: self.presence(),
                    });
                }
            }
            Some(proto::event::DOC_OPENED) | Some(proto::event::DOC_CLOSED) => {
                if let Some(params) = msg.params.as_ref() {
                    if let Ok(event) = serde_json::from_value::<proto::DocEvent>(params.clone()) {
                        let open = msg.event.as_deref() == Some(proto::event::DOC_OPENED);
                        if open && !self.documents.contains(&event.path) {
                            self.documents.push(event.path);
                        } else if !open {
                            self.documents.retain(|p| p != &event.path);
                        }
                    }
                }
                let _ = self.events.send(EngineEvent::DocumentsChanged {
                    documents: self.documents.clone(),
                });
            }
            Some(proto::event::HOST_DETACHED) => {
                let grace_ms = msg
                    .params
                    .as_ref()
                    .and_then(|p| p.get("grace_ms"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or_default();
                let _ = self.events.send(EngineEvent::HostDetached { grace_ms });
            }
            Some(proto::event::HOST_ATTACHED) => {
                let peer = peer_from(msg.params.as_ref()).or_else(|| {
                    msg.params
                        .as_ref()
                        .and_then(|p| serde_json::from_value::<PeerInfo>(p.clone()).ok())
                });
                if let Some(peer) = peer {
                    self.peers.insert(peer.peer_id.clone(), peer.clone());
                    let _ = self.events.send(EngineEvent::HostAttached { peer });
                    let _ = self.events.send(EngineEvent::PeersChanged {
                        peers: self.peer_list(),
                    });
                }
            }
            Some(proto::event::ROOM_GONE) => {
                let reason = msg
                    .params
                    .as_ref()
                    .and_then(|p| p.get("reason"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("room gone")
                    .to_string();
                let _ = self.events.send(EngineEvent::RoomGone { reason });
            }
            _ => {}
        }
    }
}

fn peer_from(params: Option<&serde_json::Value>) -> Option<PeerInfo> {
    let params = params?;
    let value = params.get("peer").unwrap_or(params);
    serde_json::from_value(value.clone()).ok()
}

fn encode_y_message(message: &YMessage) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    message.encode(&mut encoder);
    encoder.to_vec()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}
