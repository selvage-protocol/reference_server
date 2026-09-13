//! Headless harness: one server, two clients, driven programmatically.
//!
//! The integration tests and the `selvage-harness` binary both use this. Nothing here
//! sleeps and hopes: waiting is always bounded polling of a real predicate, and a
//! timeout reports the state it actually observed.

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::panic::{PanicHookInfo, set_hook, take_hook};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use selvaged::Server;
pub use selvaged::ServerConfig;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use selvage_client::ConnectOptions;
pub use selvage_client::{
    Anchor, AwarenessState, EditorAdapter, EngineEvent, Error, Invite, ItemId,
    PeerInfo, Presence, Role, Selection, SelectionOffsets, SyncEngine,
    drive_editor,
};

/// How long a test is willing to wait for a condition that should hold immediately.
pub const WAIT: Duration = Duration::from_secs(5);

/// Every panic raised in this process, as the hook saw it.
///
/// A task that panics does not fail the test that owns it: the panic is caught by the
/// runtime and the task is dropped, which a green suite cannot tell from success. The
/// log plus the [`PanicWatch`] every [`Harness`] carries turns that into a failure.
fn panic_log() -> &'static Mutex<Vec<String>> {
    static LOG: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    LOG.get_or_init(|| Mutex::new(Vec::new()))
}

/// Set while a [`PanicWatch`] is reporting, so its own failure is not recorded.
fn reporting() -> &'static AtomicBool {
    static REPORTING: AtomicBool = AtomicBool::new(false);
    &REPORTING
}

/// Records one panic, unless a failure is already being reported.
fn note_panic(info: &PanicHookInfo<'_>) {
    if reporting().load(Ordering::Relaxed) {
        return;
    }
    if let Ok(mut log) = panic_log().lock() {
        log.push(info.to_string());
    }
}

/// Records every panic in this process, then defers to the hook that was installed.
///
/// A panic is recorded as it starts, before the stack unwinds, so a test that waits for
/// a task to finish has already seen the panic when that wait returns.
fn record_panics() {
    static HOOK: OnceLock<()> = OnceLock::new();
    HOOK.get_or_init(|| {
        let previous = take_hook();
        set_hook(Box::new(move |info| {
            note_panic(info);
            previous(info);
        }));
    });
}

/// The panics raised while a harness was alive, checked when that harness is dropped.
///
/// The check is part of the harness rather than something a test opts into: a test that
/// never looks for a panic still fails on one. What it reports it also removes, so one
/// crash fails the first test to notice it rather than every test in the binary. The log
/// is process-wide, so a panic raised after its own harness was dropped is reported by
/// the next harness dropped; only a panic raised after the last harness is never seen.
struct PanicWatch {
    seen: usize,
}

impl PanicWatch {
    fn start() -> Self {
        record_panics();
        let seen = panic_log().lock().map_or(0, |log| log.len());
        Self { seen }
    }

    /// Takes the panics raised since this watch started.
    fn take_raised(&self) -> Vec<String> {
        panic_log()
            .lock()
            .map(|mut log| {
                let at = self.seen.min(log.len());
                log.split_off(at)
            })
            .unwrap_or_default()
    }
}

impl Drop for PanicWatch {
    #[expect(
        clippy::panic,
        reason = "a panic in a task must fail the test that owns the task"
    )]
    fn drop(&mut self) {
        let raised = self.take_raised();
        if raised.is_empty() {
            return;
        }
        if thread::panicking() {
            // This test is already failing; panicking again would abort the run.
            eprintln!(
                "a task panicked while this test was running: {raised:#?}"
            );
            return;
        }
        // Reporting the failure must not itself be recorded as one.
        reporting().store(true, Ordering::Relaxed);
        panic!("a task panicked while this test was running: {raised:#?}");
    }
}

/// A running server on an ephemeral port.
pub struct Harness {
    addr: SocketAddr,
    task: JoinHandle<()>,
    /// Kept for its `Drop` impl: a test that never looks for a panic still fails on one.
    #[expect(
        dead_code,
        reason = "the field is a guard; nothing is meant to read it"
    )]
    panics: PanicWatch,
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
    pub async fn start(room_grace: Duration) -> Self {
        Self::start_with(ServerConfig {
            room_grace,
            ..ServerConfig::default()
        })
        .await
    }

    /// Starts a server with this configuration, for tests that need to move a clock or
    /// shorten a timeout.
    ///
    /// # Panics
    ///
    /// Panics when the loopback port cannot be bound.
    #[expect(
        clippy::expect_used,
        reason = "the harness owns this loopback port; not binding it must fail the test"
    )]
    pub async fn start_with(config: ServerConfig) -> Self {
        let server =
            Server::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), config)
                .await
                .expect("the harness can bind an ephemeral port");
        let addr = server.local_addr();
        let task = tokio::spawn(server.run());
        Self {
            addr,
            task,
            panics: PanicWatch::start(),
        }
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
