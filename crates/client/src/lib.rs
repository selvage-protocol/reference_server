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
pub mod error;
pub mod host;
pub mod peer;
pub mod presence;
pub mod relay;
pub mod sealed;
pub mod session;

use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::timeout;

pub use selvage_protocol::{Keepalive, PeerInfo, Role, WIRE_VERSION};

/// What a caller is told when a `selvage/2` invite is handed to the version-1 engine.
pub const SEALED_INVITE: &str = "this link is a `selvage/2` invite: its fragment carries the room key and the host key, which this engine cannot hold — join it with `RelaySession` instead";

pub use crate::editor::{EditorAdapter, EngineEvent, drive_editor};
pub use crate::engine::{Command, EditOp};
pub use crate::error::Error;
pub use crate::host::{
    HostOptions, HostProducer, HostPublication, HostReason, HostStore,
    ListingSource, PersistedHost,
};
pub use crate::peer::{PeerInvite, PeerOptions, PeerSession};
pub use crate::presence::{
    Anchor, AwarenessState, ItemId, Presence, Selection, SelectionOffsets,
};
pub use crate::relay::{
    RelayEnding, RelayEvent, RelayHostOptions, RelayJoinOptions, RelayPeer,
    RelaySession, RelaySessionInfo,
};
pub use crate::session::{
    ConnectOptions, Invite, KeepaliveConfig, ReconnectPolicy, SessionInfo,
};

/// A connected sync engine.
///
/// Cloning shares the same session; the connection closes when the last clone is
/// dropped or [`SyncEngine::disconnect`] is called.
#[derive(Clone, Debug)]
pub struct SyncEngine {
    commands: mpsc::UnboundedSender<Command>,
    events: broadcast::Sender<EngineEvent>,
    session: Arc<Mutex<SessionInfo>>,
}

impl SyncEngine {
    /// Connects, completes the session handshake and starts the engine task.
    ///
    /// A failure here is a failure: reconnection retries a session that dropped, never
    /// the first connection behind the caller's back (`PROTOCOL.md` §9.1). It reads
    /// `GET /meta` first, best-effort and bounded, because the grace that read carries is
    /// what sizes a later reconnect's budget and nothing else in the handshake has it.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the socket cannot be opened, the server refuses the
    /// session, or the handshake does not finish within ten seconds.
    pub async fn connect(options: ConnectOptions) -> Result<Self, Error> {
        // A `selvage/2` invite carries the room's key and the host's in its fragment, and this
        // engine is the version-1 one: it has no way to seal a frame and no place to put either
        // key, so connecting it to such a link would drop them and hand the room a peer that
        // cannot read a byte of it. The link is refused where it is read rather than at the
        // server, which would answer `unsupported_version` for a reason that is not this one.
        if options.sealed_invite.is_some() {
            return Err(Error::Version(SEALED_INVITE.to_string()));
        }
        // `PROTOCOL.md` §9.1: the room's grace is what a reconnect has to span, and the
        // handshake reply has no member to carry it. Read best-effort, bounded on its own
        // so the ten seconds below stay the handshake's, and only when a retry would use
        // it: a caller that named its own attempts is not sized by the grace at all.
        let grace = if options.reconnect.enabled
            && options.reconnect.max_attempts.is_none()
        {
            engine::advertised_grace(&options.base_url).await
        } else {
            None
        };
        let established =
            timeout(engine::HANDSHAKE_TIMEOUT, engine::connect(options, grace))
                .await;
        let (channel, _) = match established {
            Ok(result) => result?,
            Err(_) => return Err(Error::Closed),
        };
        Ok(Self {
            commands: channel.commands,
            events: channel.events,
            session: channel.session,
        })
    }

    /// What the server said at the end of the handshake. It changes on a reconnect: a
    /// reconnecting client is a new peer, and the room is the same.
    #[must_use]
    pub fn session(&self) -> SessionInfo {
        match self.session.lock() {
            Ok(session) => session.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Opens a document: it becomes part of this client's open set and of the room's.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped or the server refused the request.
    pub async fn open(&self, path: impl Into<String>) -> Result<(), Error> {
        let target = path.into();
        self.call(|reply| Command::Open {
            path: target,
            reply,
        })
        .await?
    }

    /// Closes a document: it leaves this client's open set and the room's.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped or the server refused the request.
    pub async fn close(&self, path: impl Into<String>) -> Result<(), Error> {
        let target = path.into();
        self.call(|reply| Command::Close {
            path: target,
            reply,
        })
        .await?
    }

    /// Changes this connection's display name for the rest of the session. The name in
    /// force reaches every peer, this client included, as the `peer.renamed` event; a
    /// refusal leaves the current name alone.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped or the server refused the name.
    pub async fn rename(
        &self,
        display_name: impl Into<String>,
    ) -> Result<(), Error> {
        let name = display_name.into();
        self.call(|reply| Command::Rename {
            display_name: name,
            reply,
        })
        .await?
    }

    /// Publishes the room's grant: the host's whole listing of its working tree, replacing
    /// whatever the room held. A listing is a snapshot and not a delta — a shorter one is a
    /// smaller grant, not a partial one — and its order is carried unchanged. The listing
    /// reaches every peer, this client included, as the `doc.granted` event; only the room's
    /// host may publish one, and a client that is not the host is refused.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped or the server refused the listing.
    pub async fn grant(&self, paths: Vec<String>) -> Result<(), Error> {
        self.call(|reply| Command::Grant { paths, reply }).await?
    }

    /// The current text of a document. Empty for a document nobody has written to.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    pub async fn text(&self, path: impl Into<String>) -> Result<String, Error> {
        let target = path.into();
        self.call(|reply| Command::Text {
            path: target,
            reply,
        })
        .await
    }

    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    #[expect(
        clippy::too_many_arguments,
        reason = "a path, an offset and the text are the three things an insert is"
    )]
    pub async fn insert(
        &self,
        path: impl Into<String>,
        index: u32,
        text: impl Into<String>,
    ) -> Result<(), Error> {
        let target = path.into();
        let op = EditOp::Insert {
            index,
            text: text.into(),
        };
        self.call(|reply| Command::Edit {
            path: target,
            op,
            reply,
        })
        .await?
    }

    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    #[expect(
        clippy::too_many_arguments,
        reason = "a path, an offset and a length are the three things a delete is"
    )]
    pub async fn delete(
        &self,
        path: impl Into<String>,
        index: u32,
        len: u32,
    ) -> Result<(), Error> {
        let target = path.into();
        let op = EditOp::Delete { index, len };
        self.call(|reply| Command::Edit {
            path: target,
            op,
            reply,
        })
        .await?
    }

    /// Publishes this client's presence: document path plus selection.
    ///
    /// The offsets are anchored by the engine against this replica — the wire carries CRDT
    /// anchors, never offsets (`PROTOCOL.md` §8.1) — and a selection this replica cannot
    /// anchor is withheld: the path is published with no `selection`. That is the case until
    /// the document named by `path` has arrived, and for any endpoint past the end of its
    /// text, so a caller that publishes before opening a document publishes no cursor rather
    /// than one at offset zero.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the state cannot be encoded, or the engine has stopped.
    pub async fn set_awareness(
        &self,
        path: Option<String>,
        selection: Option<SelectionOffsets>,
    ) -> Result<(), Error> {
        self.call(|reply| Command::SetAwareness {
            path,
            selection,
            reply,
        })
        .await?
    }

    /// # Errors
    ///
    /// Returns [`Error`] when the state cannot be encoded, or the engine has stopped.
    pub async fn set_selection(
        &self,
        path: impl Into<String>,
        selection: SelectionOffsets,
    ) -> Result<(), Error> {
        self.set_awareness(Some(path.into()), Some(selection)).await
    }

    /// Every presence record this engine knows, including the local client's.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    pub async fn presence(&self) -> Result<Vec<Presence>, Error> {
        self.call(|reply| Command::Presence { reply }).await
    }

    /// Remote participants, excluding this client.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    pub async fn peers(&self) -> Result<Vec<PeerInfo>, Error> {
        self.call(|reply| Command::Peers { reply }).await
    }

    /// The CRDT state vector, as `(client id, clock)` pairs. Two fully synced replicas
    /// have identical vectors.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    pub async fn state_vector(&self) -> Result<Vec<(u64, u32)>, Error> {
        self.call(|reply| Command::StateVector { reply }).await
    }

    /// The room's open-document set.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    pub async fn documents(&self) -> Result<Vec<String>, Error> {
        self.call(|reply| Command::Documents { reply }).await
    }

    /// The room's grant, as its host published it. Empty when the room grants nothing and
    /// until the server has sent the listing.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    pub async fn granted_paths(&self) -> Result<Vec<String>, Error> {
        self.call(|reply| Command::GrantedPaths { reply }).await
    }

    /// The documents this client has opened.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped.
    pub async fn open_documents(&self) -> Result<Vec<String>, Error> {
        self.call(|reply| Command::OpenDocuments { reply }).await
    }

    /// Holds (or releases) outbound frames. While paused, local edits accumulate and
    /// are sent on resume — the deterministic way to make two edits concurrent.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the engine has stopped or the held frames cannot be
    /// flushed on resume.
    pub async fn set_outbound_paused(&self, paused: bool) -> Result<(), Error> {
        self.call(|reply| Command::SetOutboundPaused { paused, reply })
            .await?
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
    }

    /// Ends the session and waits for the engine task to stop.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] when the engine task has already stopped.
    pub async fn disconnect(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Shutdown { reply: Some(tx) })
            .map_err(|_| Error::Closed)?;
        let _ = rx.await;
        Ok(())
    }

    async fn call<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<T, Error> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(make(tx)).map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }
}
