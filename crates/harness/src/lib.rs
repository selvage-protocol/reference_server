//! Headless harness: one server, two clients, driven programmatically.
//!
//! The integration tests and the `selvage-harness` binary both use this. Nothing here
//! sleeps and hopes: waiting is always bounded polling of a real predicate, and a
//! timeout reports the state it actually observed.

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use selvaged::{Server, ServerConfig};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use selvage_client::ConnectOptions;
pub use selvage_client::{
    AwarenessState, EditorAdapter, EngineEvent, Error, Invite, PeerInfo,
    Presence, Role, Selection, SyncEngine, drive_editor,
};

/// How long a test is willing to wait for a condition that should hold immediately.
pub const WAIT: Duration = Duration::from_secs(5);

/// A running server on an ephemeral port.
pub struct Harness {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

/// A room that a host client minted.
#[derive(Debug, Clone)]
pub struct Room {
    pub id: String,
    pub token: String,
    pub invite_url: String,
}

impl Room {
    /// What a guest needs to join.
    #[must_use]
    pub fn invite(&self) -> Invite {
        Invite::new(self.id.clone(), self.token.clone())
    }
}

impl Harness {
    /// Starts a server with a short room grace period, so lifecycle tests do not take
    /// thirty seconds.
    ///
    /// # Panics
    ///
    /// Panics when the loopback port cannot be bound.
    #[expect(
        clippy::expect_used,
        reason = "the harness owns this loopback port; not binding it must fail the test"
    )]
    pub async fn start(room_grace: Duration) -> Self {
        let config = ServerConfig {
            room_grace,
            ..ServerConfig::default()
        };
        let server =
            Server::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), config)
                .await
                .expect("the harness can bind an ephemeral port");
        let addr = server.local_addr();
        let task = tokio::spawn(server.run());
        Self { addr, task }
    }

    #[must_use]
    pub fn ws_base(&self) -> String {
        format!("ws://{}", self.addr)
    }

    #[must_use]
    pub fn http_base(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Connects a host, which mints a room.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the host cannot connect.
    ///
    /// # Panics
    ///
    /// Panics when the server does not invite the host that minted the room.
    #[expect(
        clippy::expect_used,
        reason = "the server always invites the host that minted the room; a session \
                  without a token means it did not, which must fail the test"
    )]
    pub async fn host(
        &self,
        display_name: &str,
    ) -> Result<(SyncEngine, Room), Error> {
        let engine = SyncEngine::connect(ConnectOptions::host(
            self.ws_base(),
            display_name,
        ))
        .await?;
        let session = engine.session();
        let room = Room {
            id: session.room_id.clone(),
            token: session.token.clone().expect("a host is told the token"),
            invite_url: session
                .invite_url()
                .expect("a host can build an invite URL"),
        };
        Ok((engine, room))
    }

    /// Connects a guest with the room's invite.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the guest cannot connect or the server refuses it.
    pub async fn join(
        &self,
        room: &Room,
        display_name: &str,
    ) -> Result<SyncEngine, Error> {
        let options =
            ConnectOptions::guest(self.ws_base(), display_name, room.invite());
        SyncEngine::connect(options).await
    }

    /// Joins with a published invite URL — the link itself, not the room id and token
    /// taken out of it. This is what a human does when they paste the link, so it fails
    /// when the link cannot be used as one.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invite`] when the URL is not a connection URL this client can
    /// use, and whatever [`SyncEngine::connect`] returns when the server refuses.
    pub async fn join_url(
        &self,
        invite_url: &str,
        display_name: &str,
    ) -> Result<SyncEngine, Error> {
        let options = ConnectOptions::from_invite_url(invite_url, display_name)
            .ok_or_else(|| Error::Invite(invite_url.to_string()))?;
        SyncEngine::connect(options).await
    }

    /// Reconnects a host that had disconnected: same room, hosts again.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the reconnect is refused, e.g. because a host is present.
    pub async fn reclaim(
        &self,
        room: &Room,
        display_name: &str,
    ) -> Result<SyncEngine, Error> {
        let options =
            ConnectOptions::guest(self.ws_base(), display_name, room.invite())
                .with_role(Role::Host);
        SyncEngine::connect(options).await
    }

    pub fn abort(&self) {
        self.task.abort();
    }
}

/// Waits until `check` says the state is right, polling with a deadline.
///
/// A timeout is a test failure that reports the last observed state rather than a
/// pass that depended on timing.
///
/// # Panics
///
/// Panics when `label` never becomes true within [`WAIT`].
pub async fn wait_for<F, Fut, T>(label: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            start.elapsed() < WAIT,
            "timed out after {WAIT:?} waiting for {label}"
        );
        sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until both engines hold identical text for `path`, then returns it.
///
/// # Panics
///
/// Panics when the replicas do not converge within [`WAIT`].
pub async fn wait_for_convergence(
    a: &SyncEngine,
    b: &SyncEngine,
    path: &str,
) -> String {
    wait_for(&format!("replicas to converge on {path}"), || async {
        let (left, right) =
            (a.text(path).await.ok()?, b.text(path).await.ok()?);
        (left == right).then_some(left)
    })
    .await
}

/// Waits until `engine` can see a remote peer with this display name.
///
/// # Panics
///
/// Panics when the peer does not appear within [`WAIT`].
pub async fn wait_for_peer(
    engine: &SyncEngine,
    display_name: &str,
) -> PeerInfo {
    wait_for(&format!("peer {display_name} to appear"), || async {
        engine
            .peers()
            .await
            .ok()?
            .into_iter()
            .find(|peer| peer.display_name == display_name)
    })
    .await
}

/// Waits until `engine` sees awareness from a peer with this display name.
///
/// # Panics
///
/// Panics when the presence does not appear within [`WAIT`].
pub async fn wait_for_presence(
    engine: &SyncEngine,
    display_name: &str,
) -> Presence {
    wait_for(&format!("presence from {display_name}"), || async {
        engine
            .presence()
            .await
            .ok()?
            .into_iter()
            .find(|presence| presence.display_name() == Some(display_name))
    })
    .await
}

/// Waits for a specific engine event, ignoring the others.
///
/// # Panics
///
/// Panics when the event does not arrive within [`WAIT`], or when the engine's event
/// stream closes first.
#[expect(
    clippy::panic,
    reason = "a wait that timed out is a test failure, not a value the caller can recover from"
)]
pub async fn wait_for_event(
    engine: &SyncEngine,
    label: &str,
    matches: impl Fn(&EngineEvent) -> bool,
) -> EngineEvent {
    let mut events = engine.subscribe();
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < WAIT,
            "timed out after {WAIT:?} waiting for {label}"
        );
        match timeout(Duration::from_millis(50), events.recv()).await {
            Ok(Ok(event)) if matches(&event) => return event,
            Ok(Err(RecvError::Closed)) => {
                panic!("the engine stream closed while waiting for {label}");
            }
            _ => {}
        }
    }
}
