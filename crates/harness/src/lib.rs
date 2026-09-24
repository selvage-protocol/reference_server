//! Headless harness: one server, on an ephemeral port, driven by the tests that use it.
//!
//! The integration tests use this. Nothing here sleeps and hopes: waiting is always bounded
//! polling of a real predicate, and a timeout reports the state it actually observed.

use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::panic::{PanicHookInfo, set_hook, take_hook};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use selvaged::Server;
pub use selvaged::ServerConfig;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

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

    /// The server's `host:port`, for a relay to forward to.
    #[must_use]
    pub fn upstream(&self) -> String {
        self.addr.to_string()
    }

    pub fn abort(&self) {
        self.task.abort();
    }
}

/// A TCP relay in front of the server that a test can cut.
///
/// Dropping a real connection without stopping the server is what exercises
/// reconnection (`PROTOCOL.md` §9.1): aborting the server's accept loop would take the
/// room down with it, and there is no protocol method that closes one peer's socket.
/// The relay inspects nothing; it is the network failing under a session.
pub struct DropProxy {
    addr: SocketAddr,
    task: JoinHandle<()>,
    connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// How many connections the relay has accepted, one per TCP connection a client made
    /// through it: what makes a reconnect attempt countable rather than inferable.
    accepted: Arc<AtomicUsize>,
}

impl DropProxy {
    /// Starts a relay to `upstream`, a `host:port`, on an ephemeral loopback port.
    ///
    /// # Errors
    ///
    /// Returns the bind error when no loopback port can be taken.
    pub async fn start(upstream: &str) -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let target = upstream.to_string();
        let connections: Arc<Mutex<Vec<JoinHandle<()>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(AtomicUsize::new(0));
        let tracked = Arc::clone(&connections);
        let counted = Arc::clone(&accepted);
        let task =
            tokio::spawn(accept_loop(listener, target, tracked, counted));
        Ok(Self {
            addr,
            task,
            connections,
            accepted,
        })
    }

    #[must_use]
    pub fn ws_base(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// How many connections have been relayed since this proxy started.
    #[must_use]
    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// Cuts every relayed connection, dropping both of its sockets. The server sees its
    /// own side close, so what follows is an ordinary peer drop, not a shutdown.
    pub fn drop_all(&self) {
        let mut connections = match self.connections.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        for handle in connections.drain(..) {
            handle.abort();
        }
    }
}

impl Drop for DropProxy {
    fn drop(&mut self) {
        self.task.abort();
        self.drop_all();
    }
}

/// Forwards one relayed connection in both directions until either side closes.
async fn relay(mut client: TcpStream, target: String) {
    if let Ok(mut server) = TcpStream::connect(&target).await {
        let _ = copy_bidirectional(&mut client, &mut server).await;
    }
}

/// Records a relayed connection so [`DropProxy::drop_all`] can cut it.
fn track(connections: &Mutex<Vec<JoinHandle<()>>>, handle: JoinHandle<()>) {
    let mut handles = match connections.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    handles.push(handle);
}

/// Accepts relayed connections until the listener is dropped.
#[expect(
    clippy::too_many_arguments,
    reason = "a relay loop names its listener, target and the two counters a test reads"
)]
async fn accept_loop(
    listener: TcpListener,
    target: String,
    connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accepted: Arc<AtomicUsize>,
) {
    while let Ok((client, _)) = listener.accept().await {
        accepted.fetch_add(1, Ordering::SeqCst);
        track(&connections, tokio::spawn(relay(client, target.clone())));
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
pub async fn wait_for<F, Fut, T>(label: &str, check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    wait_for_described(label, || async { String::new() }, check).await
}

/// [`wait_for`], plus a closure that describes what was seen when the wait runs out.
///
/// The whole point of a bounded wait is that its failure says what the state was instead of
/// only what it was expected to be, and on this side the state is behind an `async` call.
///
/// # Panics
///
/// Panics when `label` never becomes true within [`WAIT`], printing what `describe` saw.
pub async fn wait_for_described<F, Fut, T, D, DFut>(
    label: &str,
    describe: D,
    check: F,
) -> T
where
    D: FnMut() -> DFut,
    DFut: Future<Output = String>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    wait_for_described_within(WAIT, label, describe, check).await
}

/// [`wait_for_described`], bounded by `deadline` instead of [`WAIT`].
///
/// A wait whose effect is known to be slower than [`WAIT`] needs a longer bound than a
/// local operation: the room ejecting a peer after relaying tens of mebibytes to
/// a socket that never drains is I/O under coverage, not a scheduling turn. The bound
/// bounds both callbacks as well as polling. Diagnostics are sampled after failed checks
/// while time remains, so a timeout reports the last completed observation without
/// awaiting another callback past the deadline.
///
/// # Panics
///
/// Panics when `label` never becomes true within `deadline`, printing the last observation.
#[expect(
    clippy::too_many_arguments,
    reason = "the deadline and the (label, describe, check) triple are the whole shape of a bounded wait; a builder for one helper is worse"
)]
pub async fn wait_for_described_within<F, Fut, T, D, DFut>(
    deadline: Duration,
    label: &str,
    mut describe: D,
    mut check: F,
) -> T
where
    D: FnMut() -> DFut,
    DFut: Future<Output = String>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    let mut observed = String::new();
    loop {
        assert!(
            start.elapsed() < deadline,
            "timed out after {deadline:?} waiting for {label}{}",
            as_suffix(&observed)
        );
        let mut remaining = deadline.saturating_sub(start.elapsed());
        if !remaining.is_zero()
            && let Ok(Some(value)) = timeout(remaining, check()).await
            && start.elapsed() < deadline
        {
            return value;
        }
        remaining = deadline.saturating_sub(start.elapsed());
        if !remaining.is_zero()
            && let Ok(description) = timeout(remaining, describe()).await
        {
            observed = description;
        }
        remaining = deadline.saturating_sub(start.elapsed());
        if !remaining.is_zero() {
            sleep(Duration::from_millis(5).min(remaining)).await;
        }
    }
}

/// What a [`wait_for_described`] failure appends, when the caller had anything to say.
fn as_suffix(observed: &str) -> String {
    if observed.is_empty() {
        return String::new();
    }
    format!("; observed {observed}")
}

#[cfg(test)]
mod wait_tests {
    use std::future::{pending, ready};
    use std::time::Duration;

    use tokio::time::timeout;

    use super::{WAIT, wait_for_described_within};

    #[tokio::test]
    #[should_panic(expected = "waiting for pending check")]
    async fn a_pending_check_cannot_outlive_the_deadline() {
        let result = timeout(
            WAIT,
            wait_for_described_within(
                Duration::from_millis(20),
                "pending check",
                pending::<String>,
                pending::<Option<()>>,
            ),
        )
        .await;
        assert!(result.is_ok(), "the helper exceeded its deadline");
    }

    #[tokio::test]
    #[should_panic(expected = "waiting for pending diagnostic")]
    async fn a_pending_diagnostic_cannot_outlive_the_deadline() {
        let result = timeout(
            WAIT,
            wait_for_described_within(
                Duration::from_millis(20),
                "pending diagnostic",
                pending::<String>,
                || ready(None::<()>),
            ),
        )
        .await;
        assert!(result.is_ok(), "the helper exceeded its deadline");
    }

    #[tokio::test]
    async fn a_successful_check_does_not_need_diagnostics() {
        let result = wait_for_described_within(
            WAIT,
            "ready state",
            pending::<String>,
            || ready(Some(42)),
        )
        .await;
        assert_eq!(result, 42);
    }
}
