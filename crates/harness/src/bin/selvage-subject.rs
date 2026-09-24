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
//!
//! **A link is decided about before any frame.** §5.1's fragment rule is a refusal a client
//! makes locally, with no socket and no frame to report it in, so `join` answers it itself —
//! the protocol's `{"ok": false, "error": …}` — and seats nothing. It is the client library's
//! rule and not a copy of it: §5.1's is [`PeerInvite::parse`], which `RelaySession::join`
//! also calls, because this layer opens no socket.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::str;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use selvage_client::peer::{Ending, PeerInvite, PeerOptions, PeerSession};
use selvage_client::sealed::encode_key;
use serde_json::{Map, Value, json};

/// How often the session's clocks are run while the caller is not asking for anything.
const TICK: Duration = Duration::from_millis(10);

/// The guards of `PROTOCOL.md` §13.11's table that sit on the **link** rather than in a
/// session, under the name `specification/runner/subject.py` gives it. A caller removes it
/// **before** the `join` that reads its link, because a client reads its link before any
/// session exists; every other name of the table is a session's guard and is handed to that
/// session.
const ACCEPT_PARTIAL_FRAGMENT: &str = "accept-partial-fragment";

/// A running subject: one seat and the zero of the clock it reads.
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

/// The subject: the session, or nothing before a `join`, and the guard this run removed from
/// the link — which is this subject's own state, because it is removed before there is a
/// session to hold it.
struct State {
    removed_on_the_link: Option<String>,
    running: Option<Running>,
}

/// The state, shared with the ticker.
type Shared = Arc<Mutex<State>>;

/// What a command asks for next: the body of a report, or the point of stopping.
///
/// Every answer but `quit` is one report — that is the decision channel — and it is wrapped
/// where it is written so that the envelope and the members are built in one place each.
enum Next {
    Report(Value),
    Stop,
}

fn main() -> ExitCode {
    let shared: Shared = Arc::new(Mutex::new(State {
        removed_on_the_link: None,
        running: None,
    }));
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
            // which one failed instead of a subject that vanished. A link this client
            // refuses arrives here too: §5.1's fragment rule is a decision, and this is the
            // channel it is reported in.
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
    let mut state = lock(shared);
    let Some(running) = state.running.as_mut() else {
        return;
    };
    let clock = running.clock();
    running.peer.tick(clock);
}

/// The lock, with a poisoned one treated as held: nothing here leaves shared state half
/// written, and a subject that refused every command after one panic would be worse.
fn lock(shared: &Shared) -> MutexGuard<'_, State> {
    match shared.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Runs `f` against the state with a session in it, or fails naming the command that has to
/// come first.
fn with_session<T>(
    shared: &Shared,
    f: impl FnOnce(&mut State) -> Result<T, String>,
) -> Result<T, String> {
    let mut state = lock(shared);
    if state.running.is_none() {
        return Err("no session: `join` first".to_string());
    }
    f(&mut state)
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

/// Seats a session from a link, or refuses the link in this client's own words.
///
/// The refusal is `PROTOCOL.md` §5.1's fragment, decided before a socket; `Err` is how it is
/// answered, and a subject left unseated by one is free to be handed another link, which is
/// what lets a vector carry a refusal and its control leg.
fn join(shared: &Shared, command: &Value) -> Result<Next, String> {
    if command.get("offline") != Some(&Value::Bool(true)) {
        return Err(
            "this subject opens no socket: `join` needs `\"offline\": true`"
                .to_string(),
        );
    }
    let keepalive = keepalive_of(command)?;
    let mut state = lock(shared);
    if state.running.is_some() {
        return Err("a session is already running".to_string());
    }

    let start = Instant::now();
    let invite = invite_of(command, state.removed_on_the_link.as_deref())?;
    let mut peer = sealed_peer(command, keepalive, &invite)?;
    if let Some(path) = optional_text(command, "path") {
        // §13.7's change is announced where it happens; before a state commits this key
        // nothing is published either way (§13.1's step 4).
        peer.open(start.elapsed(), &path);
    }
    let mut running = Running { peer, start };
    let clock = running.clock();
    running.peer.tick(clock);
    state.running = Some(running);
    state_report(&state).map(Next::Report)
}

/// The link this command joins with, or the client's own words for a link it refuses.
///
/// `removed` is the guard on the link this run removed, if any: a client that reads a
/// half-copied fragment as a join is the wrong implementation vector `157` is about.
fn invite_of(
    command: &Value,
    removed: Option<&str>,
) -> Result<PeerInvite, String> {
    let link = text(command, "invite")?;
    match PeerInvite::parse(link) {
        Ok(invite) => Ok(invite),
        Err(refusal) => {
            if removed == Some(ACCEPT_PARTIAL_FRAGMENT) && half_a_fragment(link)
            {
                // The guard was removed, so the wrong client seats: §5.1's two keys are
                // read from wherever they happen to be, and a link naming one of them
                // seats a session that will fail to verify every frame.
                PeerInvite::parse(&whole_fragment(link))
            } else {
                Err(refusal)
            }
        }
    }
}

/// The session a link seats: §5.1's two keys, the session's clock, and the seat and roster
/// the relay would have carried.
fn sealed_peer(
    command: &Value,
    keepalive: &Value,
    invite: &PeerInvite,
) -> Result<PeerSession, String> {
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
        // The corpus's decision layer drives a session and not a connection: there is no
        // handshake here to announce an awareness id, and a host's producer half needs a seat
        // this subject does not hold, so both are the session's own.
        awareness_client_id: None,
        host: None,
        frame_budget: None,
    };
    PeerSession::new(&options).map_err(|error| error.to_string())
}

/// Whether a link's fragment names one of §5.1's two keys and not the other.
///
/// Read off the link and not off a refusal message: what refuses the link is the client's own
/// rule ([`PeerInvite::parse`]), and this only decides whether the guard the run removed is the
/// one the link is about.
fn half_a_fragment(link: &str) -> bool {
    let Some((_, fragment)) = link.split_once('#') else {
        return false;
    };
    let names: BTreeSet<&str> = fragment
        .split('&')
        .map(|pair| pair.split_once('=').map_or(pair, |(name, _)| name))
        .collect();
    names.contains("k") != names.contains("h")
}

/// A link's fragment with both of §5.1's keys present, for the one guard a run removes.
///
/// A wrong client that ignores the half-copy rule seats anyway. What it seats with cannot be a
/// real second key — this layer holds no other — so the missing one is a placeholder: the
/// vector that removes this guard asserts that the leg seated at all, and a session opened
/// with a key that verifies nothing is exactly the shape the rule exists to prevent.
fn whole_fragment(link: &str) -> String {
    let Some((address, fragment)) = link.split_once('#') else {
        return link.to_string();
    };
    let mut parts: Vec<String> =
        fragment.split('&').map(str::to_string).collect();
    let names: BTreeSet<&str> = fragment
        .split('&')
        .map(|pair| pair.split_once('=').map_or(pair, |(name, _)| name))
        .collect();
    let placeholder = encode_key(&[0u8; 32]);
    for name in ["k", "h"] {
        if !names.contains(name) {
            parts.push(format!("{name}={placeholder}"));
        }
    }
    format!("{address}#{}", parts.join("&"))
}

/// The session's clock, which every `join` has to carry: it arrives on `room.created` in a real
/// session and there is no frame here to carry it.
fn keepalive_of(command: &Value) -> Result<&Value, String> {
    command
        .get("keepalive")
        .ok_or_else(|| "`join` needs the session's `keepalive`".to_string())
}

/// One sealed frame's bytes, read the way `CANONICAL.md` §6.1 says and decided about.
fn deliver(shared: &Shared, command: &Value) -> Result<Next, String> {
    let raw = hex_of(text(command, "frame")?)?;
    with_session(shared, |state| {
        let running = session(state)?;
        let clock = running.clock();
        let _ = running.peer.deliver(clock, &raw);
        running.peer.tick(clock);
        state_report(state)
    })
    .map(Next::Report)
}

/// A local edit: the caller's cursor moves this replica, and the room hears it only if the
/// session may publish content (`PROTOCOL.md` §13.5, §13.9).
fn insert(shared: &Shared, command: &Value) -> Result<Next, String> {
    let path = text(command, "path")?.to_string();
    let index = u32_member(command, "index")?;
    let chunk = text(command, "text")?.to_string();
    with_session(shared, |state| {
        let running = session(state)?;
        let clock = running.clock();
        running
            .peer
            .insert(&path, index, &chunk)
            .map_err(|error| error.to_string())?;
        running.peer.tick(clock);
        state_report(state)
    })
    .map(Next::Report)
}

/// A hold: the whole held set replaced by this one path and announced (§13.7).
fn announce(shared: &Shared, command: &Value) -> Result<Next, String> {
    let path = text(command, "path")?.to_string();
    with_session(shared, |state| {
        let running = session(state)?;
        let clock = running.clock();
        running.peer.hold_only(clock, &path);
        running.peer.tick(clock);
        state_report(state)
    })
    .map(Next::Report)
}

/// Removes one of §13.11's client guards, which is what the corpus's census is.
///
/// A guard on the link is removed **before** the `join` that reads its link, so it is recorded
/// as this subject's own state and answered with the report it holds; a guard that sits in a
/// session is handed to the session, and a caller that asks for one before there is a session is
/// told which command has to come first rather than having it silently dropped.
fn mutate(shared: &Shared, command: &Value) -> Result<Next, String> {
    let name = text(command, "name")?.to_string();
    let mut state = lock(shared);
    if name == ACCEPT_PARTIAL_FRAGMENT {
        state.removed_on_the_link = Some(name);
        return state_report(&state).map(Next::Report);
    }
    let running = state.running.as_mut().ok_or("no session: `join` first")?;
    running
        .peer
        .mutate(&name)
        .map_err(|error| error.to_string())?;
    state_report(&state).map(Next::Report)
}

/// The caller's liveness probe asks before it seats anything, and a client with no session
/// holds nothing: that is the empty report and not a refusal.
fn report(shared: &Shared) -> Result<Next, String> {
    let state = lock(shared);
    state_report(&state).map(Next::Report)
}

/// The running session, which every command but `join`, `report` and `mutate` needs.
fn session(state: &mut State) -> Result<&mut Running, String> {
    state
        .running
        .as_mut()
        .ok_or_else(|| "no session: `join` first".to_string())
}

/// What the subject says about itself: `PROTOCOL.md` §13.11's observables and nothing else.
///
/// The guard a link carried is this subject's own state and is reported whichever session is in
/// it, because a link guard is removed before any session exists.
fn state_report(state: &State) -> Result<Value, String> {
    let report = match state.running.as_ref() {
        None => Report::empty(),
        Some(running) => {
            // A frame this session could not produce would leave it looking like a client with
            // nothing to say, which is the one thing a subject must never do quietly.
            if let Some(fault) = running.peer.fault() {
                return Err(format!(
                    "this session could not publish a frame: {fault}"
                ));
            }
            Report::of(&running.peer)
        }
    };
    Ok(report
        .with_link_mutation(state.removed_on_the_link.as_deref())
        .json())
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

    /// This report with the guard a link carried, which no session holds.
    fn with_link_mutation(mut self, removed: Option<&str>) -> Self {
        if let Some(name) = removed {
            self.mutation = Some(name.to_string());
        }
        self
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
