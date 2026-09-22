//! A `selvage/2` client another toolchain can drive: the peer corpus's subject protocol.
//!
//! ```
//! cargo run -p selvage-harness --bin selvage-subject
//! ```
//!
//! `specification/runner/subject.py` is the protocol and
//! `specification/runner/run_peer.py --subject "…/selvage-subject"` is the driver. One JSON
//! object per line on stdin, one reply per line on stdout, and the caller does all the
//! waiting: this side never sleeps for a decision and never speaks first, so a vector cannot
//! read a state that arrived for a different reason. What it does run is the session's own
//! clocks, on a tick of its own, because `PROTOCOL.md` §13.7 renews a held set on one and
//! §13.8's windows are read on it — a subject that only moved when it was spoken to would
//! renew nothing.
//!
//! It opens no socket. The decision layer's frames are handed over by the caller
//! (`{"cmd": "deliver", "frame": "<hex>"}`), which is what makes a frame-by-frame decision an
//! observable one, so `join` must say `"offline": true` and this refuses a session that
//! expected a connection it does not make.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::str;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use selvage_client::peer::{Ending, PeerInvite, PeerOptions, PeerSession};
use serde_json::{Map, Value, json};

/// How often the session's clocks are run while the caller is not asking for anything.
const TICK: Duration = Duration::from_millis(10);

/// A running subject: one session and the zero of the clock it reads.
struct Running {
    peer: PeerSession,
    start: Instant,
}

impl Running {
    /// `PROTOCOL.md` §13.8's clock: this client's own monotone elapsed time from its seat.
    fn clock(&self) -> Duration {
        self.start.elapsed()
    }
}

/// The session, or nothing before a `join`. Shared with the ticker.
type Shared = Arc<Mutex<Option<Running>>>;

/// What a command asks for next: the body of a report, or the point of stopping.
///
/// Every answer but `quit` is one report — that is the decision channel — and it is wrapped
/// where it is written so that the envelope and the members are built in one place each.
enum Next {
    Report(Value),
    Stop,
}

fn main() -> ExitCode {
    let shared: Shared = Arc::new(Mutex::new(None));
    spawn_ticker(&shared);
    let stdin = io::stdin();
    for incoming in stdin.lock().lines() {
        let Ok(line) = incoming else { break };
        if line.trim().is_empty() {
            continue;
        }
        let reply = match serve(&shared, &line) {
            Ok(Next::Report(body)) => json!({"ok": true, "report": body}),
            Ok(Next::Stop) => {
                let _ = emit(&json!({"ok": true}));
                return ExitCode::SUCCESS;
            }
            // A command this side cannot take is answered and not fatal, so a caller sees
            // which one failed instead of a subject that vanished.
            Err(error) => json!({"ok": false, "error": error}),
        };
        if emit(&reply).is_err() {
            break;
        }
    }
    ExitCode::SUCCESS
}

/// Reads one command and answers it, or fails saying which member was missing.
fn serve(shared: &Shared, line: &str) -> Result<Next, String> {
    match serde_json::from_str::<Value>(line) {
        Ok(command) => dispatch(shared, &command),
        Err(error) => {
            Err(format!("a command is one JSON object per line: {error}"))
        }
    }
}

/// One reply per line, flushed as it is written: the caller is waiting on it.
fn emit(value: &Value) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{value}")?;
    stdout.flush()
}

/// Runs the session's clocks for as long as the process lives. The thread ends with the
/// process, which is what `quit` and a closed stdin both do.
fn spawn_ticker(shared: &Shared) {
    let ticking = Arc::clone(shared);
    let _ = thread::Builder::new()
        .name("selvage-subject-clock".to_string())
        .spawn(move || {
            loop {
                thread::sleep(TICK);
                tick(&ticking);
            }
        });
}

/// One tick of the session, on the clock this client runs from its seat. There is nothing to
/// tick before a `join`, which is the `None` arm.
fn tick(shared: &Shared) {
    let mut guard = lock(shared);
    if let Some(running) = guard.as_mut() {
        let clock = running.clock();
        running.peer.tick(clock);
    }
}

/// The lock, with a poisoned one treated as held: nothing here leaves shared state half
/// written, and a subject that refused every command after one panic would be worse.
fn lock(shared: &Shared) -> MutexGuard<'_, Option<Running>> {
    match shared.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Runs `f` against the session, or fails naming the command that has to come first.
fn with_session<T>(
    shared: &Shared,
    f: impl FnOnce(&mut Running) -> Result<T, String>,
) -> Result<T, String> {
    let mut guard = lock(shared);
    let running = guard.as_mut().ok_or("no session: `join` first")?;
    f(running)
}

fn dispatch(shared: &Shared, command: &Value) -> Result<Next, String> {
    let name = text(command, "cmd")?;
    match name {
        "join" => join(shared, command),
        "deliver" => deliver(shared, command),
        "insert" => insert(shared, command),
        "announce" => announce(shared, command),
        "report" => report(shared),
        "mutate" => mutate(shared, command),
        "quit" => Ok(Next::Stop),
        other => Err(format!("unknown command {other:?}")),
    }
}

/// Seats a session: the invite's two keys, the session's clock, and the seat and roster the
/// relay would have carried.
fn join(shared: &Shared, command: &Value) -> Result<Next, String> {
    if command.get("offline") != Some(&Value::Bool(true)) {
        return Err(
            "this subject opens no socket: `join` needs `\"offline\": true`"
                .to_string(),
        );
    }
    let invite = PeerInvite::parse(text(command, "invite")?)?;
    let keepalive = command
        .get("keepalive")
        .ok_or("`join` needs the session's `keepalive`")?;
    let options = PeerOptions {
        room_id: invite.room.clone(),
        room_key: invite.room_key,
        host_key: invite.host_key,
        renew: millis(keepalive, "awareness_renew_ms")?,
        expire: millis(keepalive, "awareness_expire_ms")?,
        seat: optional_text(command, "seat"),
        roster: seats(command.get("roster")),
        fixed_session_key: match command.get("session_key") {
            Some(Value::Null) | None => None,
            Some(value) => {
                Some(seed(value.as_str().ok_or("`session_key` is hex")?)?)
            }
        },
        declared_role: optional_text(command, "role"),
    };
    let mut peer =
        PeerSession::new(&options).map_err(|error| error.to_string())?;
    if let Some(path) = optional_text(command, "path") {
        peer.open(&path);
    }
    let start = Instant::now();
    let mut running = Running { peer, start };
    let clock = running.clock();
    running.peer.tick(clock);
    let answer = running_report(&running);
    let mut guard = lock(shared);
    if guard.is_some() {
        return Err("a session is already running".to_string());
    }
    *guard = Some(running);
    drop(guard);
    answer.map(Next::Report)
}

/// One sealed frame's bytes, read the way `CANONICAL.md` §6.1 says and decided about.
fn deliver(shared: &Shared, command: &Value) -> Result<Next, String> {
    let raw = hex_of(text(command, "frame")?)?;
    with_session(shared, |running| {
        let clock = running.clock();
        let _ = running.peer.deliver(clock, &raw);
        running.peer.tick(clock);
        running_report(running)
    })
    .map(Next::Report)
}

/// A local edit: the caller's cursor moves this replica, and the room hears it only if the
/// session may publish content (`PROTOCOL.md` §13.5, §13.9).
fn insert(shared: &Shared, command: &Value) -> Result<Next, String> {
    let path = text(command, "path")?.to_string();
    let index = u32_member(command, "index")?;
    let chunk = text(command, "text")?.to_string();
    with_session(shared, |running| {
        running
            .peer
            .insert(&path, index, &chunk)
            .map_err(|error| error.to_string())?;
        let clock = running.clock();
        running.peer.tick(clock);
        running_report(running)
    })
    .map(Next::Report)
}

/// A hold: the whole held set replaced by this one path and announced (§13.7).
fn announce(shared: &Shared, command: &Value) -> Result<Next, String> {
    let path = text(command, "path")?.to_string();
    with_session(shared, |running| {
        running.peer.release();
        running.peer.open(&path);
        let clock = running.clock();
        running.peer.tick(clock);
        running_report(running)
    })
    .map(Next::Report)
}

/// Removes one of §13.11's client guards, which is what the corpus's census is.
fn mutate(shared: &Shared, command: &Value) -> Result<Next, String> {
    let name = text(command, "name")?.to_string();
    with_session(shared, |running| match running.peer.mutate(&name) {
        Ok(()) => running_report(running),
        Err(error) => Err(error.to_string()),
    })
    .map(Next::Report)
}

/// The caller's liveness probe asks before it seats anything, and a client with no session
/// holds nothing: that is the empty report and not a refusal.
fn report(shared: &Shared) -> Result<Next, String> {
    let mut guard = lock(shared);
    let empty = || Ok(Next::Report(Report::empty().json()));
    let Some(running) = guard.as_mut() else {
        return empty();
    };
    running_report(running).map(Next::Report)
}

/// What the subject says about itself: `PROTOCOL.md` §13.11's observables and nothing else.
fn running_report(running: &Running) -> Result<Value, String> {
    // A frame this session could not produce would leave it looking like a client with
    // nothing to say, which is the one thing a subject must never do quietly.
    if let Some(fault) = running.peer.fault() {
        return Err(format!("this session could not publish a frame: {fault}"));
    }
    Ok(Report::of(&running.peer).json())
}

/// One report, built in one place so that the session's and the empty one are one shape.
struct Report {
    text: Map<String, Value>,
    documents: Vec<String>,
    applied: Vec<Value>,
    dropped: Vec<Value>,
    published: u64,
    handshake: u64,
    frames: u64,
    ended: bool,
    ending: Option<&'static str>,
    listing: Vec<String>,
    holds: BTreeMap<String, Vec<String>>,
    mutation: Option<String>,
}

impl Report {
    /// The report of a client that has joined nothing.
    fn empty() -> Self {
        Self {
            text: Map::new(),
            documents: Vec::new(),
            applied: Vec::new(),
            dropped: Vec::new(),
            published: 0,
            handshake: 0,
            frames: 0,
            ended: false,
            ending: None,
            listing: Vec::new(),
            holds: BTreeMap::new(),
            mutation: None,
        }
    }

    fn of(peer: &PeerSession) -> Self {
        let documents = peer.documents();
        let mut text: Map<String, Value> = Map::new();
        for path in &documents {
            let _ = text.insert(path.clone(), json!(peer.text(path)));
        }
        Self {
            text,
            documents,
            applied: peer
                .applied()
                .iter()
                .map(|applied| json!({"frame": applied.frame, "kind": applied.kind}))
                .collect(),
            dropped: peer
                .dropped()
                .iter()
                .map(|dropped| json!({"frame": dropped.frame, "reason": dropped.reason}))
                .collect(),
            published: peer.published(),
            handshake: peer.handshake(),
            frames: peer.frames(),
            ended: peer.ending().is_some(),
            // §13.10 requires a client to say why it ended; nothing asserts this member and a
            // person reading a failed run needs it.
            ending: peer.ending().map(Ending::as_str),
            listing: peer.listing().to_vec(),
            holds: peer.peer_holds(),
            mutation: peer.mutation().map(str::to_string),
        }
    }

    fn json(&self) -> Value {
        json!({
            "text": self.text,
            "documents": self.documents,
            "applied": self.applied,
            "dropped": self.dropped,
            "published": self.published,
            "handshake": self.handshake,
            "frames": self.frames,
            "ended": self.ended,
            "ending": self.ending,
            "listing": self.listing,
            "holds": self.holds,
            "peers": [],
            "mutation": self.mutation,
        })
    }
}

// --- reading the command's members ---------------------------------------------

fn text<'a>(value: &'a Value, member: &str) -> Result<&'a str, String> {
    value
        .get(member)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("a command needs a string `{member}`"))
}

fn optional_text(value: &Value, member: &str) -> Option<String> {
    value
        .get(member)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn u32_member(value: &Value, member: &str) -> Result<u32, String> {
    let number = value
        .get(member)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("a command needs a `{member}`"))?;
    u32::try_from(number).map_err(|_| format!("`{member}` is out of range"))
}

fn millis(value: &Value, member: &str) -> Result<Duration, String> {
    let number =
        value.get(member).and_then(Value::as_u64).ok_or_else(|| {
            format!("`keepalive.{member}` is a count of milliseconds")
        })?;
    Ok(Duration::from_millis(number))
}

/// The seats a roster names, which is what §13.8 reads a state's `host` entry against.
fn seats(value: Option<&Value>) -> BTreeSet<String> {
    value
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// A 32-byte seed from 64 hex characters, which is the shape the corpus's fixture uses.
fn seed(text: &str) -> Result<[u8; 32], String> {
    let raw = hex_of(text)?;
    <[u8; 32]>::try_from(raw.as_slice()).map_err(|_| {
        format!("a session key seed is 32 bytes, and {text:?} is not")
    })
}

/// Hex with or without the spaces a vector writes between bytes.
fn hex_of(text: &str) -> Result<Vec<u8>, String> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    let (pairs, rest) = compact.as_bytes().as_chunks::<2>();
    if !rest.is_empty() {
        return Err(format!("{text:?} is not an even number of hex digits"));
    }
    let mut out: Vec<u8> = Vec::with_capacity(pairs.len());
    for pair in pairs {
        let digits = str::from_utf8(pair).map_err(|error| error.to_string())?;
        out.push(
            u8::from_str_radix(digits, 16)
                .map_err(|error| format!("{text:?} is not hex: {error}"))?,
        );
    }
    Ok(out)
}
