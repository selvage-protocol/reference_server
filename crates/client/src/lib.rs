//! Selvage client: a *sync engine* (CRDT plus y-protocols, one `Y.Doc` per session and
//! one `Y.Text` per document) and the *editor adapter* seam it is driven through.
//!
//! ```no_run
//! # async fn example() -> Result<(), selvage_client::Error> {
//! use selvage_client::{ConnectOptions, SyncEngine};
//! let engine = SyncEngine::connect(ConnectOptions::host("ws://127.0.0.1:8080", "Ada")).await?;
//! engine.open("src/main.rs").await?;
//! engine.insert("src/main.rs", 0, "fn main() {}\n").await?;
//! # Ok(())
//! # }
//! ```

pub mod editor;
mod engine;
pub mod presence;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

pub use selvage_protocol::{Keepalive, PeerInfo, Role, WIRE_VERSION};

pub use crate::editor::{drive_editor, EditorAdapter, EngineEvent};
pub use crate::engine::{Command, EditOp};
pub use crate::presence::{AwarenessState, Presence, Selection};

/// y-protocols defaults: renew every 15s, expire at 30s.
#[derive(Debug, Clone, Copy)]
pub struct KeepaliveConfig {
    pub awareness_renew: Duration,
    pub awareness_expire: Duration,
}

impl Default for KeepaliveConfig {
    fn default() -> Self {
        Self {
            awareness_renew: Duration::from_millis(selvage_protocol::Keepalive::default().awareness_renew_ms),
            awareness_expire: Duration::from_millis(selvage_protocol::Keepalive::default().awareness_expire_ms),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Wire(tokio_tungstenite::tungstenite::Error),
    Json(serde_json::Error),
    /// The server refused the session; `code` is one of `selvage_protocol::code`.
    Protocol { code: String, message: String },
    /// The connection ended.
    Closed,
    /// The sync engine could not apply an operation.
    Yjs(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Wire(e) => write!(f, "websocket: {e}"),
            Error::Json(e) => write!(f, "json: {e}"),
            Error::Protocol { code, message } => write!(f, "{code}: {message}"),
            Error::Closed => write!(f, "the session is closed"),
            Error::Yjs(message) => write!(f, "sync: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

/// What the server said at the end of the handshake.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub room_id: String,
    /// Present only for the host that minted the room.
    pub token: Option<String>,
    pub role: Role,
    /// This connection's own peer record.
    pub peer: PeerInfo,
    /// Peers that were already in the room.
    pub peers: Vec<PeerInfo>,
    /// The room's open-document set at the moment of joining.
    pub documents: Vec<String>,
    pub capabilities: Vec<String>,
    pub keepalive: Keepalive,
    pub endpoint: String,
}

impl SessionInfo {
    /// The invite URL for this room, as a guest would use it.
    pub fn invite_url(&self) -> Option<String> {
        let token = self.token.as_ref()?;
        Some(selvage_protocol::session_url(
            &self.endpoint,
            Some(&self.room_id),
            Some(token),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// `ws://host:port`, without the `/session` path.
    pub base_url: String,
    pub display_name: String,
    /// Joining an existing room: its id.
    pub room: Option<String>,
    /// Joining an existing room: its invite token.
    pub token: Option<String>,
    /// Claimed role. `None` lets the server decide: host when minting, guest otherwise.
    pub role: Option<Role>,
    pub capabilities: Vec<String>,
    /// Free-form client identifier, e.g. `selvage-harness/0.1.0`.
    pub client: Option<String>,
    pub keepalive: KeepaliveConfig,
    pub initial_awareness: AwarenessState,
}

impl ConnectOptions {
    /// Mints a room; the caller becomes its host.
    pub fn host(base_url: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self::new(base_url, display_name).with_role(Role::Host)
    }

    /// Joins a room with its invite token.
    pub fn guest(
        base_url: impl Into<String>,
        display_name: impl Into<String>,
        room: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        let mut options = Self::new(base_url, display_name);
        options.room = Some(room.into());
        options.token = Some(token.into());
        options.role = Some(Role::Guest);
        options
    }

    fn new(base_url: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            display_name: display_name.into(),
            room: None,
            token: None,
            role: None,
            capabilities: Vec::new(),
            client: Some(format!("selvage-client/{}", env!("CARGO_PKG_VERSION"))),
            keepalive: KeepaliveConfig::default(),
            initial_awareness: AwarenessState::default(),
        }
    }

    pub fn with_role(mut self, role: Role) -> Self {
        self.role = Some(role);
        self
    }

    pub fn with_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.capabilities = capabilities.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_keepalive(mut self, renew: Duration, expire: Duration) -> Self {
        self.keepalive = KeepaliveConfig {
            awareness_renew: renew,
            awareness_expire: expire,
        };
        self
    }

    pub fn with_awareness(mut self, awareness: AwarenessState) -> Self {
        self.initial_awareness = awareness;
        self
    }
}

/// A connected sync engine.
///
/// Cloning shares the same session; the connection closes when the last clone is
/// dropped or [`SyncEngine::disconnect`] is called.
#[derive(Clone, Debug)]
pub struct SyncEngine {
    commands: mpsc::UnboundedSender<Command>,
    events: broadcast::Sender<EngineEvent>,
    session: std::sync::Arc<SessionInfo>,
}


impl SyncEngine {
    pub async fn connect(options: ConnectOptions) -> Result<Self, Error> {
        let established = tokio::time::timeout(Duration::from_secs(10), handshake(options)).await;
        match established {
            Ok(result) => result,
            Err(_) => Err(Error::Closed),
        }
    }

    pub fn session(&self) -> &SessionInfo {
        &self.session
    }

    /// Opens a document: it becomes part of this client's open set and of the room's.
    pub async fn open(&self, path: impl Into<String>) -> Result<(), Error> {
        let path = path.into();
        self.call(|reply| Command::Open { path, reply }).await?
    }

    pub async fn close(&self, path: impl Into<String>) -> Result<(), Error> {
        let path = path.into();
        self.call(|reply| Command::Close { path, reply }).await?
    }

    /// The current text of a document. Empty for a document nobody has written to.
    pub async fn text(&self, path: impl Into<String>) -> Result<String, Error> {
        let path = path.into();
        self.call(|reply| Command::Text { path, reply }).await
    }

    pub async fn insert(
        &self,
        path: impl Into<String>,
        index: u32,
        text: impl Into<String>,
    ) -> Result<(), Error> {
        let path = path.into();
        let op = EditOp::Insert {
            index,
            text: text.into(),
        };
        self.call(|reply| Command::Edit { path, op, reply }).await?
    }

    pub async fn delete(&self, path: impl Into<String>, index: u32, len: u32) -> Result<(), Error> {
        let path = path.into();
        let op = EditOp::Delete { index, len };
        self.call(|reply| Command::Edit { path, op, reply }).await?
    }

    /// Publishes this client's presence: document path plus selection.
    pub async fn set_awareness(&self, state: AwarenessState) -> Result<(), Error> {
        self.call(|reply| Command::SetAwareness { state, reply })
            .await?
    }

    pub async fn set_selection(
        &self,
        path: impl Into<String>,
        selection: Selection,
    ) -> Result<(), Error> {
        self.set_awareness(AwarenessState {
            path: Some(path.into()),
            selection: Some(selection),
        })
        .await
    }

    /// Every presence record this engine knows, including the local client's.
    pub async fn presence(&self) -> Result<Vec<Presence>, Error> {
        self.call(|reply| Command::Presence { reply }).await
    }

    /// Remote participants, excluding this client.
    pub async fn peers(&self) -> Result<Vec<PeerInfo>, Error> {
        self.call(|reply| Command::Peers { reply }).await
    }

    /// The CRDT state vector, as `(client id, clock)` pairs. Two fully synced replicas
    /// have identical vectors.
    pub async fn state_vector(&self) -> Result<Vec<(u64, u32)>, Error> {
        self.call(|reply| Command::StateVector { reply }).await
    }

    /// The room's open-document set.
    pub async fn documents(&self) -> Result<Vec<String>, Error> {
        self.call(|reply| Command::Documents { reply }).await
    }

    /// The documents this client has opened.
    pub async fn open_documents(&self) -> Result<Vec<String>, Error> {
        self.call(|reply| Command::OpenDocuments { reply }).await
    }

    /// Holds (or releases) outbound frames. While paused, local edits accumulate and
    /// are sent on resume — the deterministic way to make two edits concurrent.
    pub async fn set_outbound_paused(&self, paused: bool) -> Result<(), Error> {
        self.call(|reply| Command::SetOutboundPaused { paused, reply })
            .await?
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
    }

    pub async fn disconnect(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Shutdown { reply: Some(tx) })
            .map_err(|_| Error::Closed)?;
        let _ = rx.await;
        Ok(())
    }

    async fn call<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> Command) -> Result<T, Error> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(make(tx)).map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }
}

async fn handshake(options: ConnectOptions) -> Result<SyncEngine, Error> {
    let doc = yrs::Doc::new();
    let awareness_client_id = doc.client_id().get();
    let mut awareness = yrs::sync::Awareness::new(doc);
    awareness.set_local_state_raw(serde_json::to_string(&options.initial_awareness)?);

    let url = selvage_protocol::session_url(
        &options.base_url,
        options.room.as_deref(),
        options.token.as_deref(),
    );
    let (ws, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .map_err(Error::Wire)?;
    let (mut sink, mut stream) = ws.split();

    let hello = selvage_protocol::ClientMessage::new(
        1,
        selvage_protocol::method::SESSION_HELLO,
        json!(selvage_protocol::HelloParams {
            display_name: options.display_name.clone(),
            role: options.role,
            awareness_client_id: Some(awareness_client_id),
            capabilities: options.capabilities.clone(),
            client: options.client.clone(),
        }),
    );
    sink.send(Message::text(hello.to_text()))
        .await
        .map_err(Error::Wire)?;

    let session = loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                let msg: selvage_protocol::ServerMessage =
                    serde_json::from_str(&text).map_err(Error::Json)?;
                match msg.event.as_deref() {
                    Some(selvage_protocol::event::ROOM_CREATED)
                    | Some(selvage_protocol::event::ROOM_JOINED) => {
                        let params: selvage_protocol::SessionParams = serde_json::from_value(
                            msg.params.unwrap_or_else(|| json!({})),
                        )?;
                        break session_info(params, url.clone());
                    }
                    Some(selvage_protocol::event::SESSION_ERROR) => {
                        let params = msg.params.unwrap_or_else(|| json!({}));
                        return Err(Error::Protocol {
                            code: params
                                .get("code")
                                .and_then(|c| c.as_str())
                                .unwrap_or("error")
                                .to_string(),
                            message: params
                                .get("message")
                                .and_then(|m| m.as_str())
                                .unwrap_or("the server refused the session")
                                .to_string(),
                        });
                    }
                    _ => {}
                }
            }
            Some(Ok(Message::Close(_))) => {
                return Err(Error::Closed);
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(Error::Wire(e)),
            None => return Err(Error::Closed),
        }
    };

    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (events_tx, _) = broadcast::channel(64);
    let task = crate::engine::EngineTask {
        sink,
        stream,
        awareness,
        session: session.clone(),
        commands: commands_rx,
        events: events_tx.clone(),
        keepalive: options.keepalive,
        documents: session.documents.clone(),
        open_documents: Vec::new(),
        peers: session
            .peers
            .iter()
            .map(|peer| (peer.peer_id.clone(), peer.clone()))
            .collect(),
        request_id: 1,
        local_state: Some(serde_json::to_string(&options.initial_awareness)?),
        queued: Default::default(),
        paused: false,
    };
    tokio::spawn(task.run());

    Ok(SyncEngine {
        commands: commands_tx,
        events: events_tx,
        session: std::sync::Arc::new(session),
    })
}

fn session_info(params: selvage_protocol::SessionParams, endpoint: String) -> SessionInfo {
    SessionInfo {
        room_id: params.room_id,
        token: params.token,
        role: params.self_peer.role,
        peer: params.self_peer,
        peers: params.peers,
        documents: params.documents,
        capabilities: params.capabilities,
        keepalive: params.keepalive,
        endpoint,
    }
}
