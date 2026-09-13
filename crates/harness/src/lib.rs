//! Headless harness: one server, two clients, driven programmatically.
//!
//! The integration tests and the `selvage-harness` binary both use this. Nothing here
//! sleeps and hopes: waiting is always bounded polling of a real predicate, and a
//! timeout reports the state it actually observed.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use selvaged::{Server, ServerConfig};

use selvage_client::ConnectOptions;
pub use selvage_client::{
    drive_editor, AwarenessState, EditorAdapter, EngineEvent, Error, PeerInfo, Presence, Role,
    Selection, SyncEngine,
};

/// How long a test is willing to wait for a condition that should hold immediately.
pub const WAIT: Duration = Duration::from_secs(5);

/// A running server on an ephemeral port.
pub struct Harness {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

/// A room that a host client minted.
#[derive(Debug, Clone)]
pub struct Room {
    pub id: String,
    pub token: String,
    pub invite_url: String,
}

impl Harness {
    /// Starts a server with a short room grace period, so lifecycle tests do not take
    /// thirty seconds.
    pub async fn start(room_grace: Duration) -> Self {
        let config = ServerConfig {
            room_grace,
            ..ServerConfig::default()
        };
        let server = Server::bind("127.0.0.1:0".parse().expect("valid address"), config)
            .await
            .expect("the harness can bind an ephemeral port");
        let addr = server.local_addr().expect("bound listener has an address");
        let task = tokio::spawn(server.run());
        Self { addr, task }
    }

    pub fn ws_base(&self) -> String {
        format!("ws://{addr}", addr = self.addr)
    }

    pub fn http_base(&self) -> String {
        format!("http://{addr}", addr = self.addr)
    }

    /// Connects a host, which mints a room.
    pub async fn host(&self, display_name: &str) -> Result<(SyncEngine, Room), Error> {
        let engine = SyncEngine::connect(ConnectOptions::host(self.ws_base(), display_name)).await?;
        let room = Room {
            id: engine.session().room_id.clone(),
            token: engine.session().token.clone().expect("a host is told the token"),
            invite_url: engine
                .session()
                .invite_url()
                .expect("a host can build an invite URL"),
        };
        Ok((engine, room))
    }

    /// Connects a guest using the room's invite URL.
    pub async fn join(&self, room: &Room, display_name: &str) -> Result<SyncEngine, Error> {
        SyncEngine::connect(ConnectOptions::guest(
            self.ws_base(),
            display_name,
            room.id.clone(),
            room.token.clone(),
        ))
        .await
    }

    /// Reconnects a host that had disconnected: same room, hosts again.
    pub async fn reclaim(&self, room: &Room, display_name: &str) -> Result<SyncEngine, Error> {
        let options = ConnectOptions::guest(
            self.ws_base(),
            display_name,
            room.id.clone(),
            room.token.clone(),
        )
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
pub async fn wait_for<F, Fut, T>(label: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        if Instant::now() >= deadline {
            panic!("timed out after {WAIT:?} waiting for {label}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until both engines hold identical text for `path`, then returns it.
pub async fn wait_for_convergence(a: &SyncEngine, b: &SyncEngine, path: &str) -> String {
    wait_for(&format!("replicas to converge on {path}"), || async {
        let (left, right) = (a.text(path).await.ok()?, b.text(path).await.ok()?);
        (left == right).then_some(left)
    })
    .await
}

/// Waits until `engine` can see a remote peer with this display name.
pub async fn wait_for_peer(engine: &SyncEngine, display_name: &str) -> PeerInfo {
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
pub async fn wait_for_presence(engine: &SyncEngine, display_name: &str) -> Presence {
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
pub async fn wait_for_event(
    engine: &SyncEngine,
    label: &str,
    matches: impl Fn(&EngineEvent) -> bool,
) -> EngineEvent {
    use tokio::sync::broadcast::error::RecvError;
    let mut events = engine.subscribe();
    let deadline = Instant::now() + WAIT;
    loop {
        if Instant::now() >= deadline {
            panic!("timed out after {WAIT:?} waiting for {label}");
        }
        match tokio::time::timeout(Duration::from_millis(50), events.recv()).await {
            Ok(Ok(event)) if matches(&event) => return event,
            Ok(Ok(_)) => continue,
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => panic!("the engine stream closed while waiting for {label}"),
            Err(_) => continue,
        }
    }
}
