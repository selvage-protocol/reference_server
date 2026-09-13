//! The editor-adapter seam.
//!
//! DESIGN.md §6 names two client roles: a *sync engine* (CRDT, y-protocols, awareness)
//! and a *thin editor adapter* (buffers, paths, decorations). [`SyncEngine`] is the
//! first; [`EditorAdapter`] is the second, expressed as the smallest thing an adapter
//! has to implement to be driven by the engine. Actual editor integration is out of
//! scope for this slice — the trait is the seam.

use std::sync::Arc;

use crate::presence::{PeerInfo, Presence};
use crate::SyncEngine;

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
    fn disconnected(&self) {}
}

/// Pumps engine events into an adapter until the session ends.
///
/// The engine stays usable by the caller: this only reads the event stream, and the
/// adapter is expected to call back into the engine (through whatever handle it holds)
/// when the editor makes a local change.
pub fn drive_editor(engine: &SyncEngine, adapter: Arc<dyn EditorAdapter>) -> tokio::task::JoinHandle<()> {
    let mut events = engine.subscribe();
    let engine = engine.clone();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(EngineEvent::DocumentChanged { path }) => {
                    if let Ok(text) = engine.text(&path).await {
                        adapter.document_changed(&path, &text);
                    }
                }
                Ok(EngineEvent::DocumentsChanged { documents }) => {
                    adapter.documents_changed(&documents);
                }
                Ok(EngineEvent::PeersChanged { peers }) => adapter.peers_changed(&peers),
                Ok(EngineEvent::PresenceChanged { presence }) => {
                    adapter.presence_changed(&presence)
                }
                Ok(EngineEvent::HostDetached { grace_ms }) => adapter.host_detached(grace_ms),
                Ok(EngineEvent::HostAttached { peer }) => adapter.host_attached(&peer),
                Ok(EngineEvent::RoomGone { reason }) => adapter.room_gone(&reason),
                Ok(EngineEvent::Disconnected) => {
                    adapter.disconnected();
                    return;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    })
}
