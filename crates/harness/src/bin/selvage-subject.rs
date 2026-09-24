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
//! **A link is decided about before any frame, and the two rules are the client's own.** §5.1's
//! fragment and §2/§10's version rule are refusals a client makes locally, with no socket and no
//! frame to report them in, so `join` answers them itself — the protocol's `{"ok": false,
//! "error": …}` — and seats nothing. Both are the client library's rules and not copies of them:
//! §5.1's is [`PeerInvite::parse`] and §2/§10's is
//! [`selvage_client::relay::refuse_a_version_this_client_cannot_speak`], which the socket path
//! calls with the body it read and this calls with the body the vector handed it, because this
//! layer opens no socket.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::str;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use selvage_client::WIRE_VERSION;
use selvage_client::peer::{
    Ending, PeerInvite, PeerOptions, PeerSession, wire_address,
};
use selvage_client::relay::{
    WIRE_VERSION_V2, refuse_a_version_this_client_cannot_speak, session_base,
};
use selvage_client::session::Invite;
use selvage_protocol::parse_session_url;
use serde_json::{Map, Value, json};

/// How often the session's clocks are run while the caller is not asking for anything.
const TICK: Duration = Duration::from_millis(10);

/// The guards of `PROTOCOL.md` §13.11's table that sit on the **link** rather than in a session,
/// under the names `specification/runner/subject.py` gives them, and the two names each of the
/// corpus's link vectors declares in its `catches`. A caller removes one **before** the `join`
/// that reads its link, because a client reads its link before any session exists; every other
/// name of the table is a session's guard and is handed to that session.
const ACCEPT_PARTIAL_FRAGMENT: &str = "accept-partial-fragment";
const FALL_BACK_TO_VERSION_1: &str = "fall-back-to-version-1";

/// What a decision the sealed wire owns is told when this subject is seated on the clear one.
/// The room is named because the seat holds it and because a reader of a failed vector wants to
/// know which room the client was seated in when the decision it asked for turned out to belong
/// to the other wire.
fn clear_seat(room: &str) -> String {
    format!(
        "this seat is a `selvage/1` session of room {room:?}: the decision layer's frames are \
         §13's sealed ones, which this subject does not drive"
    )
}

/// What the subject is seated in.
///
/// A link that speaks the sealed wire seats [`Seat::Sealed`], which is the session the whole
/// decision layer is about. A link this client has pinned to `selvage/1` — §2's own member, the
/// deliberate choice to host a room the server can read — is a version-1 join: its invite carries
/// the room and the token and none of §5.1's keys, and [`Invite`] is that engine's join input.
/// Nothing here drives that wire, so the seat holds the join input and refuses every frame by
/// name rather than applying it to a session that could not read it.
///
/// The session is boxed because the two variants are a session and two strings: a subject holds
/// one seat for its whole life, so the difference is a size nothing moves around and not a reason
/// to put a session on the heap.
enum Seat {
    Sealed(Box<PeerSession>),
    Clear(Invite),
}

/// A running subject: one seat and the zero of the clock it reads.
struct Running {
    seat: Seat,
    start: Instant,
}

impl Running {
    /// `PROTOCOL.md` §13.8's clock: this client's own monotone elapsed time from its seat.
    fn clock(&self) -> Duration {
        self.start.elapsed()
    }

    /// The sealed session, or the refusal that names what this seat is instead.
    fn sealed(&mut self) -> Result<&mut PeerSession, String> {
        match &mut self.seat {
            Seat::Sealed(peer) => Ok(peer),
            Seat::Clear(invite) => Err(clear_seat(&invite.room)),
        }
    }
}

/// The subject: the session, or nothing before a `join`, and the guard this run removed from the
/// link — which is this subject's own state, because it is removed before there is a session to
/// hold it.
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
            // which one failed instead of a subject that vanished. A link this client refuses
            // arrives here too: `PROTOCOL.md` §5.1's fragment and §2/§10's version rule are
            // decisions, and this is the channel they are reported in.
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
/// tick before a `join`, which is the `None` arm, and nothing to tick on a clear seat: the
/// clocks §13.7 and §13.8 name are the sealed wire's.
fn tick(shared: &Shared) {
    let mut state = lock(shared);
    let Some(running) = state.running.as_mut() else {
        return;
    };
    let clock = running.clock();
    if let Seat::Sealed(peer) = &mut running.seat {
        peer.tick(clock);
    }
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
/// The refusal is `PROTOCOL.md` §5.1's fragment and §2/§10's version rule, both decided before a
/// socket; `Err` is how they are answered, and a subject left unseated by one is free to be
/// handed another link, which is what lets a vector carry a refusal and its control leg.
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
    let mut seat =
        seat_of(command, keepalive, state.removed_on_the_link.as_deref())?;
    if let Seat::Sealed(peer) = &mut seat
        && let Some(path) = optional_text(command, "path")
    {
        // §13.7's change is announced where it happens; before a state commits this key
        // nothing is published either way (§13.1's step 4).
        peer.open(start.elapsed(), &path);
    }
    let mut running = Running { seat, start };
    let clock = running.clock();
    if let Seat::Sealed(peer) = &mut running.seat {
        peer.tick(clock);
    }
    state.running = Some(running);
    state_report(&state).map(Next::Report)
}

/// The seat this link is joined with, or the client's own words for a link it refuses.
///
/// The order is the client's own ([`selvage_client::relay::RelaySession::join`]): the link is
/// read first — §5.1's fragment, which names both keys or is refused, naming the one that is
/// missing — and §2/§10's version rule is decided against the body `GET /meta` answered, which
/// this layer is handed as a member because it opens no socket.
///
/// `removed` is the guard on the link this run removed, if any, and each of the two is the wrong
/// implementation one of the corpus's vectors is about. Both are seated on the clear wire, which
/// is what those wrong clients do: the one that reads a half-copied fragment as a version-1 join
/// dials and is seated in the clear, and the one that falls back from a server whose `/meta` seats
/// no version at major 2 speaks the clear wire's version instead of refusing.
fn seat_of(
    command: &Value,
    keepalive: &Value,
    removed: Option<&str>,
) -> Result<Seat, String> {
    let link = text(command, "invite")?;
    let pin = optional_text(command, "pin");
    if let Some(pinned) = pin.as_deref()
        && pinned != WIRE_VERSION
        && pinned != WIRE_VERSION_V2
    {
        return Err(format!(
            "`pin` is one of the two wire versions, not {pinned:?}"
        ));
    }
    // §2's pin: the version this client has deliberately chosen to speak. Pinned to `selvage/1`
    // it is a version-1 join — §5.1 reads the fragment rule as the rule of "the client that would
    // speak `selvage/2` on this join, and not one that cannot speak it or is pinned to
    // `selvage/1`" — and §2/§10's no-fallback rule is not its rule either: §10 addresses it to
    // the client whose version is `selvage/2`.
    if pin.as_deref() == Some(WIRE_VERSION) {
        return Ok(Seat::Clear(clear_invite(link)?));
    }
    let sealed = match PeerInvite::parse(link) {
        Ok(invite) => Some(invite),
        Err(refusal) => {
            if removed == Some(ACCEPT_PARTIAL_FRAGMENT) && half_a_fragment(link)
            {
                None
            } else {
                return Err(refusal);
            }
        }
    };
    let Some(invite) = sealed else {
        return Ok(Seat::Clear(clear_invite(link)?));
    };
    if let Some(versions) = wire_versions(command)? {
        if removed == Some(FALL_BACK_TO_VERSION_1) {
            return Ok(Seat::Clear(clear_invite(link)?));
        }
        // The address the refusal names is the server base and not the link: an invite's
        // fragment is §5.1's and never part of a request, and the sentence the client says is
        // the one `RelaySession::join` says, because it is the same function. Its rendering is
        // the error's own — `unsupported_version`, then the words — which is the reason §2 says
        // a client refuses with.
        let base = session_base(&invite.socket_url)
            .unwrap_or_else(|| invite.socket_url.clone());
        refuse_a_version_this_client_cannot_speak(&base, &versions)
            .map_err(|error| error.to_string())?;
    }
    Ok(Seat::Sealed(Box::new(sealed_peer(
        command, keepalive, &invite,
    )?)))
}

/// The session a sealed link seats: §5.1's two keys, the session's clock, and the seat and roster
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
    };
    PeerSession::new(&options).map_err(|error| error.to_string())
}

/// The version-1 join a link names: the room and the token, and none of §5.1's fragment.
///
/// [`Invite`] is the clear engine's join input — "what a guest needs to join" — and a licence to
/// speak the sealed wire is not part of it. A link that names no room or no token is refused with
/// the same words `PeerInvite::parse` refuses those with, because it is the same link being read.
fn clear_invite(link: &str) -> Result<Invite, String> {
    let parsed = parse_session_url(&wire_address(link)).ok_or_else(|| {
        format!("{link:?} does not address the session endpoint")
    })?;
    let room = parsed
        .join
        .room
        .ok_or_else(|| "the invite names no room".to_string())?;
    let token = parsed
        .join
        .token
        .ok_or_else(|| "the invite carries no token".to_string())?;
    Ok(Invite::new(room, token))
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

/// The `wire_versions` of the body `GET /meta` answered, or `None` when the vector handed none.
///
/// `PROTOCOL.md` §2: a `/meta` that could not be read is not an answer about versions and is not
/// what a client refuses on, so an absent body decides nothing. Only this member of the body is
/// read, which is what §10 decides by.
fn wire_versions(command: &Value) -> Result<Option<Vec<String>>, String> {
    let Some(meta) = command.get("meta") else {
        return Ok(None);
    };
    if meta.is_null() {
        return Ok(None);
    }
    let listed = meta
        .get("wire_versions")
        .and_then(Value::as_array)
        .ok_or("`meta.wire_versions` is the list the server advertises")?;
    let mut versions = Vec::with_capacity(listed.len());
    for version in listed {
        versions.push(
            version
                .as_str()
                .ok_or("`meta.wire_versions` is a list of version names")?
                .to_string(),
        );
    }
    Ok(Some(versions))
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
        let peer = running.sealed()?;
        let _ = peer.deliver(clock, &raw);
        peer.tick(clock);
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
        let peer = running.sealed()?;
        peer.insert(&path, index, &chunk)
            .map_err(|error| error.to_string())?;
        peer.tick(clock);
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
        let peer = running.sealed()?;
        peer.hold_only(clock, &path);
        peer.tick(clock);
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
    if name == ACCEPT_PARTIAL_FRAGMENT || name == FALL_BACK_TO_VERSION_1 {
        state.removed_on_the_link = Some(name);
        return state_report(&state).map(Next::Report);
    }
    let running = state.running.as_mut().ok_or("no session: `join` first")?;
    running
        .sealed()?
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
/// A clear seat holds nothing and is handed nothing — this layer does not drive that wire — so
/// its report is the empty one. The guard a link carried is this subject's own state and is
/// reported whichever seat is in it, because a link guard is removed before any session exists.
fn state_report(state: &State) -> Result<Value, String> {
    let report = match state.running.as_ref().map(|running| &running.seat) {
        None | Some(Seat::Clear(_)) => Report::empty(),
        Some(Seat::Sealed(peer)) => {
            // A frame this session could not produce would leave it looking like a client with
            // nothing to say, which is the one thing a subject must never do quietly.
            if let Some(fault) = peer.fault() {
                return Err(format!(
                    "this session could not publish a frame: {fault}"
                ));
            }
            Report::of(peer)
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
