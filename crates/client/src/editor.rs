//! The editor-adapter seam.
//!
//! DESIGN.md §6 names two client roles: a *sync engine* (CRDT, y-protocols, awareness)
//! and a *thin editor adapter* (buffers, paths, decorations). [`SyncEngine`] is the
//! first; [`EditorAdapter`] is the second, expressed as the smallest thing an adapter
//! has to implement to be driven by the engine. Actual editor integration is out of
//! scope for this slice — the trait is the seam.

use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

use crate::SyncEngine;
use crate::presence::{PeerInfo, Presence};

/// Everything an editor adapter is told about the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    /// The text of an open document changed (locally or remotely). The adapter should
    /// reconcile its buffer with [`SyncEngine::text`].
    DocumentChanged { path: String },
    /// The open-document set changed.
    DocumentsChanged { documents: Vec<String> },
    /// Membership changed.
    PeersChanged { peers: Vec<PeerInfo> },
    /// Awareness changed: remote cursors moved, joined or expired.
    PresenceChanged { presence: Vec<Presence> },
    /// The host disconnected; the room survives only until the grace period expires.
    HostDetached { grace_ms: u64 },
    /// The host came back before the grace period expired.
    HostAttached { peer: PeerInfo },
    /// The room is gone. No further traffic will arrive on this session.
    RoomGone { reason: String },
    /// The server reported a fault it could not attach to a request: `session.error`.
    SessionError { code: String, message: String },
    /// The connection ended for another reason.
    Disconnected,
}

/// What an editor adapter implements. Calls are made from the driver task; an adapter
/// that needs to touch editor state should do it on the editor's own thread.
pub trait EditorAdapter: Send + Sync + 'static {
    fn document_changed(&self, _path: &str, _text: &str) {}
    fn documents_changed(&self, _documents: &[String]) {}
    fn peers_changed(&self, _peers: &[PeerInfo]) {}
    fn presence_changed(&self, _presence: &[Presence]) {}
    fn host_detached(&self, _grace_ms: u64) {}
    fn host_attached(&self, _peer: &PeerInfo) {}
    fn room_gone(&self, _reason: &str) {}
    fn session_error(&self, _code: &str, _message: &str) {}
    fn disconnected(&self) {}
}

/// Pumps engine events into an adapter until the session ends.
///
/// The engine stays usable by the caller: this only reads the event stream, and the
/// adapter is expected to call back into the engine (through whatever handle it holds)
/// when the editor makes a local change.
#[must_use]
pub fn drive_editor(
    engine: &SyncEngine,
    adapter: Arc<dyn EditorAdapter>,
) -> JoinHandle<()> {
    let events = engine.subscribe();
    tokio::spawn(pump(engine.clone(), adapter, events))
}

/// Reads the event stream until the session ends. The subscription is taken before
/// the task starts, so nothing published after this call can slip past.
async fn pump(
    engine: SyncEngine,
    adapter: Arc<dyn EditorAdapter>,
    mut events: broadcast::Receiver<EngineEvent>,
) {
    loop {
        let Some(event) = next_event(&mut events).await else {
            return;
        };
        if !deliver(&engine, adapter.as_ref(), event).await {
            return;
        }
    }
}

/// Waits for the next engine event, skipping the ones a slow reader missed.
async fn next_event(
    events: &mut broadcast::Receiver<EngineEvent>,
) -> Option<EngineEvent> {
    loop {
        match events.recv().await {
            Ok(event) => return Some(event),
            Err(RecvError::Lagged(_)) => {}
            Err(RecvError::Closed) => return None,
        }
    }
}

/// Tells the adapter about one event. Returns `false` when the session is over.
async fn deliver(
    engine: &SyncEngine,
    adapter: &dyn EditorAdapter,
    event: EngineEvent,
) -> bool {
    match event {
        EngineEvent::DocumentChanged { path } => {
            if let Ok(text) = engine.text(&path).await {
                adapter.document_changed(&path, &text);
            }
        }
        EngineEvent::DocumentsChanged { documents } => {
            adapter.documents_changed(&documents);
        }
        EngineEvent::PeersChanged { peers } => adapter.peers_changed(&peers),
        EngineEvent::PresenceChanged { presence } => {
            adapter.presence_changed(&presence);
        }
        EngineEvent::HostDetached { grace_ms } => {
            adapter.host_detached(grace_ms);
        }
        EngineEvent::HostAttached { peer } => adapter.host_attached(&peer),
        EngineEvent::RoomGone { reason } => adapter.room_gone(&reason),
        EngineEvent::SessionError { code, message } => {
            adapter.session_error(&code, &message);
        }
        EngineEvent::Disconnected => {
            adapter.disconnected();
            return false;
        }
    }
    true
}
