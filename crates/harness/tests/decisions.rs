//! The peer corpus's decision layer, replayed against this client's `selvage/2` session.
//!
//! `specification/vectors/peer/151…156.json` are the six vectors that are about what a client
//! *did* with a frame it received — what it applied, what it dropped and why, what it
//! published, whether it ended — and none of that is on a socket. The specification drives
//! them through `specification/runner/subject.py` and `runner/run_peer.py --subject`; this
//! drives the same files through the same subject binary, `selvage-subject`
//! (`crates/harness/src/bin/selvage-subject.rs`), so the decision half of the corpus is
//! evidence in this repository's own suite and not only wherever the specification's runner is
//! pointed by hand.
//!
//! The runner in Python seals each recipe and hands the bytes over; so does this, with
//! `sealed::seal`, and both check the bytes against the `hex` the vector carries for the
//! reason `runner/test_recipe.py` checks a frame vector: a recipe that drifted from the bytes
//! it explains is a red run rather than a quiet difference. The two implementations of the
//! *driver* are deliberate — the same argument `peer_vectors.rs` makes for replaying the frame
//! layer in both languages — and the layer under test is one client either way.
//!
//! **154's corpus defect is pinned, not passed.** See
//! `the_holds_vector_cannot_pass_and_the_corpus_is_what_is_wrong`.

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::str;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread;
use std::time::{Duration, Instant};

use selvage_client::sealed::{
    PublicKey, Recipe, RoomKey, SessionKey, encode_key, seal,
};
use serde_json::{Value, json};

/// How long the driver waits for one answer before it fails the vector. The subject is a child
/// process of this test: a bound here is what keeps a hung subject from hanging the suite.
const DEADLINE: Duration = Duration::from_secs(10);

/// How often a bounded poll asks again. The predicate is the report's, and the deadline is
/// what ends the wait.
const POLL: Duration = Duration::from_millis(20);

// --- the fixture, and the vector directory --------------------------------------

/// One fixture keypair, re-derived rather than trusted the way `peer_vectors.rs` does it.
struct FixtureKey {
    public: [u8; 32],
    private: [u8; 32],
}

struct Fixture {
    room_id: String,
    room_key: [u8; 32],
    keys: BTreeMap<String, FixtureKey>,
}

impl Fixture {
    fn load(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path).map_err(|error| {
            format!("{} is unreadable: {error}", path.display())
        })?;
        let document: Value = serde_json::from_str(&text).map_err(|error| {
            format!("{} is not JSON: {error}", path.display())
        })?;
        let room = document
            .get("room")
            .ok_or_else(|| "the fixture has no `room`".to_string())?;
        let room_id = member_text(room, "id")?.to_string();
        let room_key = to32(&hex_bytes(member_text(room, "key")?)?)?;
        let raw = document
            .get("keys")
            .and_then(Value::as_object)
            .ok_or_else(|| "the fixture has no `keys`".to_string())?;
        let mut keys = BTreeMap::new();
        for (name, entry) in raw {
            let _ = keys.insert(name.clone(), fixture_key(name, entry)?);
        }
        Ok(Self {
            room_id,
            room_key,
            keys,
        })
    }

    fn key(&self, name: &str) -> Result<&FixtureKey, String> {
        self.keys
            .get(name)
            .ok_or_else(|| format!("the fixture has no key {name:?}"))
    }

    /// The name a vector writes for a key the subject reported, whatever spelling it used:
    /// the fixture's name, the key's canonical spelling, or its id in hex.
    fn name_of(&self, reported: &str) -> String {
        self.keys
            .iter()
            .find(|(name, key)| {
                reported == name.as_str()
                    || reported == encode_key(&key.public)
                    || reported == PublicKey(key.public).id().hex()
            })
            .map_or_else(|| reported.to_string(), |(name, _)| name.clone())
    }
}

/// One fixture keypair, with the internal consistency rule re-derived rather than trusted: a
/// mismatched pair would make every derived frame wrong with nothing naming why.
fn fixture_key(name: &str, entry: &Value) -> Result<FixtureKey, String> {
    let public = to32(&hex_bytes(member_text(entry, "public")?)?)?;
    let private = to32(&hex_bytes(member_text(entry, "private")?)?)?;
    let derived = SessionKey::from_seed(private).public().0;
    if derived != public {
        return Err(format!("{name}: the private half is not its public half"));
    }
    Ok(FixtureKey { public, private })
}

fn member_text<'a>(value: &'a Value, member: &str) -> Result<&'a str, String> {
    value
        .get(member)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("an entry has no string {member:?}"))
}

fn to32(raw: &[u8]) -> Result<[u8; 32], String> {
    raw.try_into()
        .map_err(|_| "not a 32-byte value".to_string())
}

fn hex_bytes(text: &str) -> Result<Vec<u8>, String> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    let (pairs, rest) = compact.as_bytes().as_chunks::<2>();
    if !rest.is_empty() {
        return Err(format!("{text:?} is not an even number of hex digits"));
    }
    pairs
        .iter()
        .map(|pair| {
            let digits =
                str::from_utf8(pair).map_err(|error| error.to_string())?;
            u8::from_str_radix(digits, 16)
                .map_err(|error| format!("{text:?} is not hex: {error}"))
        })
        .collect()
}

fn hex_of(raw: &[u8]) -> String {
    use std::fmt::Write as _;
    raw.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// The vector directory: `SELVAGE_VECTORS` when the build supplies it, as the other harness
/// tests read it.
fn vectors_root() -> PathBuf {
    env::var_os("SELVAGE_VECTORS").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vectors"),
        PathBuf::from,
    )
}

fn load_decision_vectors() -> Result<Vec<Value>, String> {
    let dir = vectors_root().join("peer");
    let entries = fs::read_dir(&dir)
        .map_err(|error| format!("{} is unreadable: {error}", dir.display()))?;
    let mut vectors: Vec<Value> = entries
        .filter_map(|entry| entry.ok().map(|found| found.path()))
        .filter_map(|path| decision_vector(&path).transpose())
        .collect::<Result<Vec<Value>, String>>()?;
    vectors.sort_by_key(|vector| {
        vector
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    });
    if vectors.is_empty() {
        return Err(format!("{} holds no decision vector", dir.display()));
    }
    Ok(vectors)
}

/// One file, if it is a decision vector. `Ok(None)` is a file of another layer and not a
/// failure: the directory holds the frame vectors too.
fn decision_vector(path: &Path) -> Result<Option<Value>, String> {
    if path.extension().and_then(OsStr::to_str) != Some("json") {
        return Ok(None);
    }
    let text = fs::read_to_string(path).map_err(|error| {
        format!("{} is unreadable: {error}", path.display())
    })?;
    let vector: Value = serde_json::from_str(&text)
        .map_err(|error| format!("{} is not JSON: {error}", path.display()))?;
    Ok(
        (vector.get("kind").and_then(Value::as_str) == Some("decision"))
            .then_some(vector),
    )
}

// --- the subject, as a child process --------------------------------------------

/// The subject binary, driven over the line protocol `specification/runner/subject.py` fixes.
struct Subject {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
}

impl Subject {
    fn start() -> Result<Self, String> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_selvage-subject"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                format!("the subject could not be started: {error}")
            })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "the subject has no stdin".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "the subject has no stdout".to_string())?;
        let (sender, lines) = channel();
        let _ = thread::Builder::new()
            .name("selvage-subject-reader".to_string())
            .spawn(move || forward(stdout, &sender));
        Ok(Self {
            child,
            stdin,
            lines,
        })
    }

    /// One command and one reply, bounded: `PROTOCOL.md` §13's decisions are what the reply
    /// carries, and a subject that stops answering fails the vector rather than the run.
    fn request(&mut self, command: &Value) -> Result<Value, String> {
        let line = format!("{command}\n");
        self.stdin
            .write_all(line.as_bytes())
            .and_then(|()| self.stdin.flush())
            .map_err(|error| {
                format!("the subject is not reading its input: {error}")
            })?;
        let answer = match self.lines.recv_timeout(DEADLINE) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => return Err(silent(command)),
            Err(RecvTimeoutError::Disconnected) => return Err(self.closed()),
        };
        let reply: Value = serde_json::from_str(&answer).map_err(|error| {
            format!("the subject answered something that is not JSON: {error}")
        })?;
        if reply.get("ok") != Some(&Value::Bool(true)) {
            return Err(format!(
                "the subject refused `{}`: {}",
                command.get("cmd").and_then(Value::as_str).unwrap_or("?"),
                reply.get("error").unwrap_or(&Value::Null)
            ));
        }
        Ok(reply)
    }

    /// A subject whose stdout ended: its status, and whatever it said on stderr.
    fn closed(&mut self) -> String {
        let exited = self.child.try_wait().ok().flatten();
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        let said =
            (!stderr.trim().is_empty()).then(|| format!(": {}", stderr.trim()));
        format!(
            "the subject closed its stdout{}{}",
            exited.map_or_else(String::new, |code| format!(" with {code}")),
            said.unwrap_or_default()
        )
    }

    /// The report a reply carries, which is the decision channel.
    fn report(&mut self, command: &Value) -> Result<Value, String> {
        let reply = self.request(command)?;
        reply
            .get("report")
            .cloned()
            .ok_or_else(|| "a reply carries no report".to_string())
    }

    fn quit(&mut self) {
        let _ = self.request(&json!({"cmd": "quit"}));
    }
}

/// A subject that missed its deadline fails the vector with the command it did not answer.
fn silent(command: &Value) -> String {
    format!(
        "the subject did not answer `{}` within {}s",
        command.get("cmd").and_then(Value::as_str).unwrap_or("?"),
        DEADLINE.as_secs()
    )
}

/// Every line the subject writes, onto the channel. `any` short-circuits on the first line
/// that could not be sent, which is what a closed channel means; the count is not read.
fn forward(stdout: ChildStdout, sender: &Sender<String>) {
    let _ = BufReader::new(stdout)
        .lines()
        .map_while(Result::ok)
        .any(|line| sender.send(line).is_err());
}

impl Drop for Subject {
    /// A test that fails must not leave a child behind: `AGENTS.md` §5 bounds every spawn.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// --- the comparison, member by member -------------------------------------------

/// Every member of one `expectSubject` the report does not satisfy, with both values.
#[expect(
    clippy::excessive_nesting,
    reason = "a walk over one step's members; the shape of the expectation is the nesting"
)]
fn differences(
    expect: &Value,
    report: &Value,
    fixture: &Fixture,
) -> Vec<String> {
    let mut out = Vec::new();
    for member in [
        "applied",
        "dropped",
        "published",
        "handshake",
        "ended",
        "listing",
        "text",
    ] {
        let Some(want) = expect.get(member) else {
            continue;
        };
        let claimed = report.get(member).unwrap_or(&Value::Null);
        let actual = normalise(member, claimed, fixture);
        if &actual != want {
            out.push(format!(
                "`{member}` is {want} in the vector and {actual} in the report"
            ));
        }
    }
    if let Some(want) = expect.get("holds").and_then(Value::as_object) {
        let holds = normalise(
            "holds",
            report.get("holds").unwrap_or(&Value::Null),
            fixture,
        );
        for (key, paths) in want {
            let want = strings(paths);
            // A key the subject does not report at all holds nothing, so §13.7's expiry is
            // `[]` either way.
            let actual = holds.get(key).map_or_else(Vec::new, strings);
            if actual != want {
                out.push(format!(
                    "`holds[{key}]` is {want:?} in the vector and {actual:?} in the report"
                ));
            }
        }
    }
    if let Some(bounds) = expect.get("at_least").and_then(Value::as_object) {
        for (member, bound) in bounds {
            let actual = report.get(member).and_then(Value::as_u64);
            if actual.is_none_or(|value| Some(value) < bound.as_u64()) {
                out.push(format!(
                    "`{member}` must be at least {bound} and is {}",
                    report.get(member).unwrap_or(&Value::Null)
                ));
            }
        }
    }
    out
}

/// The strings of a JSON array, sorted: `CANONICAL.md` §2.7 gives a holds array no order.
fn strings(value: &Value) -> Vec<String> {
    let mut out: Vec<String> = value
        .as_array()
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// One report member as a vector writes it: `holds` keyed by fixture name, the paths sorted
/// (`CANONICAL.md` §2.7 gives a holds array no order), and the rest as it stands.
#[expect(
    clippy::excessive_nesting,
    reason = "one member's shape: a map, then a key's paths"
)]
fn normalise(member: &str, value: &Value, fixture: &Fixture) -> Value {
    match member {
        "holds" => {
            let mut out = serde_json::Map::new();
            if let Some(entries) = value.as_object() {
                for (key, paths) in entries {
                    let _ =
                        out.insert(fixture.name_of(key), json!(strings(paths)));
                }
            }
            Value::Object(out)
        }
        _ => value.clone(),
    }
}

/// Polls the report until every member the step asserts holds, or fails at the deadline with
/// the last report it saw.
#[expect(
    clippy::too_many_arguments,
    reason = "one step's expectation, the vector it came from and the deadline it polls to"
)]
fn settle(
    subject: &mut Subject,
    where_: &str,
    expect: &Value,
    fixture: &Fixture,
    deadline: Option<Instant>,
) -> Result<Value, String> {
    loop {
        let report = subject.report(&json!({"cmd": "report"}))?;
        let problems = differences(expect, &report, fixture);
        if problems.is_empty() {
            return Ok(report);
        }
        if deadline.is_none_or(|end| Instant::now() >= end) {
            return Err(format!("{where_}: {}", problems.join("; ")));
        }
        thread::sleep(POLL);
    }
}

/// The members `frozen` names must not move over the window `within_ms` gives.
#[expect(
    clippy::too_many_arguments,
    reason = "one step's expectation, the report it starts from and the window's end"
)]
fn freeze(
    subject: &mut Subject,
    where_: &str,
    expect: &Value,
    before: &Value,
    deadline: Option<Instant>,
) -> Result<(), String> {
    let Some(end) = deadline else {
        return Err(format!(
            "{where_}: `frozen` needs the `within_ms` it is frozen over"
        ));
    };
    while Instant::now() < end {
        thread::sleep(POLL);
    }
    let after = subject.report(&json!({"cmd": "report"}))?;
    let mut moved = Vec::new();
    for member in expect
        .get("frozen")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter().filter_map(Value::as_str).collect::<Vec<&str>>()
        })
        .unwrap_or_default()
    {
        if before.get(member) != after.get(member) {
            moved.push(format!(
                "`{member}` moved from {} to {}",
                before.get(member).unwrap_or(&Value::Null),
                after.get(member).unwrap_or(&Value::Null)
            ));
        }
    }
    if moved.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{where_}: the window is over and {}",
        moved.join("; ")
    ))
}

// --- the replay -----------------------------------------------------------------

/// The invite a `start` hands the subject, with `PROTOCOL.md` §5.1's two keys in it.
fn invite_of(fixture: &Fixture, step: &Value) -> Result<String, String> {
    let template = step
        .get("invite")
        .and_then(Value::as_str)
        .ok_or_else(|| "`start` needs an `invite`".to_string())?;
    let values = [
        ("$room_key", encode_key(&fixture.room_key)),
        (
            "$host_key",
            PublicKey(fixture.key("host-key")?.public).encode(),
        ),
        ("$token", "t-corpus-decision".to_string()),
        ("$room", fixture.room_id.clone()),
    ];
    let mut invite = template.to_string();
    // Longest name first: `$room` is a prefix of `$room_key`.
    for (name, value) in values {
        invite = invite.replace(name, &value);
    }
    if invite.contains('$') {
        return Err(format!(
            "`invite` names a value nothing substitutes: {invite}"
        ));
    }
    Ok(invite)
}

/// The seats the relay shows as present, taken from the seats the vector's own states label.
#[expect(
    clippy::excessive_nesting,
    reason = "a walk over a vector's steps for the seats its states label"
)]
fn roster_of(vector: &Value) -> Vec<String> {
    let mut seats: Vec<String> = Vec::new();
    for step in vector
        .get("steps")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let peers = step
            .get("recipe")
            .and_then(|recipe| recipe.get("payload"))
            .and_then(|payload| payload.get("peers"));
        let Some(entries) = peers.and_then(Value::as_object) else {
            continue;
        };
        for entry in entries.values() {
            if let Some(seat) = entry.get("peer_id").and_then(Value::as_str)
                && !seats.iter().any(|seen| seen == seat)
            {
                seats.push(seat.to_string());
            }
        }
    }
    seats
}

/// What one `join` carries: the invite, the session's clock, the roster, and the session
/// keypair the vector names. The fixture key is the one seam of this layer and it is a test
/// seam: `PROTOCOL.md` §13.1 mints a keypair per connection and nothing on a wire fixes one.
fn join_command(
    fixture: &Fixture,
    vector: &Value,
    step: &Value,
) -> Result<Value, String> {
    let key = step
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| "`start` names no `key`".to_string())?;
    let seed = hex_of(&fixture.key(key)?.private);
    let scenario = vector
        .get("scenario")
        .and_then(Value::as_object)
        .ok_or_else(|| "a decision vector carries a `scenario`".to_string())?;
    let keepalive = scenario.get("keepalive").ok_or_else(|| {
        "`scenario.keepalive` is the session's clock".to_string()
    })?;
    Ok(json!({
        "cmd": "join",
        "invite": invite_of(fixture, step)?,
        "offline": true,
        "keepalive": keepalive,
        "seat": "p-subject",
        "roster": roster_of(vector),
        "session_key": seed,
    }))
}

/// One decision vector, driven step by step, with a mutation removed if one is named.
#[expect(
    clippy::excessive_nesting,
    reason = "the step dispatch: one arm per op, each two levels into the walk"
)]
fn replay(
    vector: &Value,
    fixture: &Fixture,
    mutation: Option<&str>,
) -> Result<usize, String> {
    let name = vector
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_string();
    let steps = vector
        .get("steps")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| format!("{name} has no steps"))?;
    let mut subject: Option<Subject> = None;
    let mut assertions: usize = 0;
    for (index, step) in steps.iter().enumerate() {
        let op = step.get("op").and_then(Value::as_str).unwrap_or("?");
        let where_ = format!("{name} step {index} (`{op}`)");
        match op {
            "start" => {
                let mut started = Subject::start()?;
                started.request(&join_command(fixture, vector, step)?)?;
                if let Some(removed) = mutation {
                    started
                        .request(&json!({"cmd": "mutate", "name": removed}))?;
                }
                subject = Some(started);
            }
            "deliver" => {
                let raw = delivered(&where_, fixture, step)?;
                let running = subject.as_mut().ok_or_else(|| {
                    format!("{where_}: no subject is running")
                })?;
                running.request(&json!({
                    "cmd": "deliver",
                    "frame": hex_of(&raw),
                }))?;
            }
            "wait" => {
                let ms = step
                    .get("ms")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| format!("{where_}: `wait` needs `ms`"))?;
                thread::sleep(Duration::from_millis(ms));
            }
            "expectSubject" => {
                assertions = assertions.saturating_add(1);
                let running = subject.as_mut().ok_or_else(|| {
                    format!("{where_}: no subject is running")
                })?;
                let deadline = step
                    .get("within_ms")
                    .and_then(Value::as_u64)
                    .and_then(|ms| {
                        Instant::now().checked_add(Duration::from_millis(ms))
                    });
                let report = settle(running, &where_, step, fixture, deadline)?;
                if step.get("frozen").is_some() {
                    freeze(running, &where_, step, &report, deadline)?;
                }
            }
            "stop" => {
                if let Some(mut running) = subject.take() {
                    running.quit();
                }
            }
            other => {
                return Err(format!(
                    "{where_}: `{other}` is not a decision step"
                ));
            }
        }
    }
    Ok(assertions)
}

/// The bytes of one `deliver`: the recipe sealed, and the vector's own `hex` checked against
/// it, which is `runner/test_recipe.py`'s rule applied where the frame is handed over.
fn delivered(
    where_: &str,
    fixture: &Fixture,
    step: &Value,
) -> Result<Vec<u8>, String> {
    let recipe = step
        .get("recipe")
        .ok_or_else(|| format!("{where_}: `deliver` needs a `recipe`"))?;
    let signer =
        recipe.get("sign").and_then(Value::as_str).ok_or_else(|| {
            format!("{where_}: a recipe names the key that signs it")
        })?;
    let kind = recipe
        .get("kind")
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("{where_}: a recipe names its `kind`"))?;
    let counter = recipe
        .get("counter")
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("{where_}: a recipe names its `counter`"))?;
    let raw_nonce = hex_bytes(
        recipe
            .get("nonce")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{where_}: a recipe names its `nonce`"))?,
    )?;
    let nonce: [u8; 12] = raw_nonce
        .try_into()
        .map_err(|_| format!("{where_}: a nonce is 12 bytes"))?;
    let plaintext = recipe_plaintext(where_, recipe)?;
    let session = SessionKey::from_seed(fixture.key(signer)?.private);
    let room_key = RoomKey(fixture.room_key);
    let frame_key = room_key.frame_key(&fixture.room_id);
    let sealed = seal(
        &Recipe {
            room_id: &fixture.room_id,
            frame_key: &frame_key,
            kind,
            epoch: 0,
            counter,
            nonce,
            signer: &session,
        },
        &plaintext,
    )
    .map_err(|error| format!("{where_}: {error}"))?
    .bytes();
    let want = hex_bytes(step.get("hex").and_then(Value::as_str).ok_or_else(
        || format!("{where_}: `deliver` carries the frame's `hex`"),
    )?)?;
    if want != sealed {
        return Err(format!(
            "{where_}: the recipe produces other bytes than the vector carries:\n  vector: {}\n  sealed: {}",
            step.get("hex").and_then(Value::as_str).unwrap_or_default(),
            hex_of(&sealed)
        ));
    }
    Ok(sealed)
}

/// A recipe's plaintext: the JSON object as written, or the hex a `kind = 0` frame carries.
fn recipe_plaintext(where_: &str, recipe: &Value) -> Result<Vec<u8>, String> {
    if let Some(text) = recipe.get("plaintext").and_then(Value::as_str) {
        return hex_bytes(text);
    }
    let payload = recipe.get("payload").ok_or_else(|| {
        format!("{where_}: a recipe carries a `plaintext` or a `payload`")
    })?;
    serde_json::to_vec(payload).map_err(|error| format!("{where_}: {error}"))
}

// --- the vectors ----------------------------------------------------------------

/// The five decision vectors a conforming client passes.
///
/// 154 is not among them and cannot be: see the pin below.
const PASSABLE: [&str; 5] = ["151", "152", "153", "155", "156"];

fn fixture() -> Result<Fixture, String> {
    Fixture::load(&vectors_root().join("fixture").join("keys.json"))
}

#[test]
fn every_decision_vector_a_conforming_client_passes_holds() {
    let fixture = match fixture() {
        Ok(fixture) => fixture,
        Err(error) => panic!("{error}"),
    };
    let vectors =
        load_decision_vectors().unwrap_or_else(|error| panic!("{error}"));
    let mut ran = 0;
    for vector in &vectors {
        let id = vector.get("id").and_then(Value::as_str).unwrap_or("?");
        if !PASSABLE.contains(&id) {
            continue;
        }
        ran += 1;
        let assertions = replay(vector, &fixture, None)
            .unwrap_or_else(|error| panic!("vector {id}: {error}"));
        assert!(assertions > 0, "vector {id} asserted nothing");
    }
    // A sweep that read nothing reports a clean tree: the five are named, not counted.
    assert_eq!(ran, PASSABLE.len(), "every passable vector was replayed");
}

#[test]
fn the_decision_vectors_go_red_under_the_guard_they_declare() {
    let fixture = match fixture() {
        Ok(fixture) => fixture,
        Err(error) => panic!("{error}"),
    };
    let vectors =
        load_decision_vectors().unwrap_or_else(|error| panic!("{error}"));
    let mut ran = 0;
    for vector in &vectors {
        let id = vector.get("id").and_then(Value::as_str).unwrap_or("?");
        if !PASSABLE.contains(&id) {
            continue;
        }
        let catches = vector
            .get("catches")
            .and_then(Value::as_str)
            .unwrap_or_else(|| {
                panic!("vector {id} declares no mutation to catch")
            });
        ran += 1;
        // The guard removed, the vector must fail: a rule vector that stays green under its own
        // mutation is not testing the rule it names. And it must fail *at an expectation*: a
        // subject that refused to remove the guard, a recipe that drifted from its bytes or a
        // step the runner would not take are failures of the harness, and counting one of them
        // as the red run would let a vector with no guard at all pass the census.
        let red = replay(vector, &fixture, Some(catches))
            .err()
            .unwrap_or_else(|| {
                panic!("vector {id} is green under `{catches}`")
            });
        assert!(
            red.contains("(`expectSubject`)"),
            "vector {id} failed under `{catches}` before any expectation: {red}"
        );
    }
    assert_eq!(ran, PASSABLE.len());
}

/// **154 cannot pass, and the corpus is what is wrong.**
///
/// `PROTOCOL.md` §13.4 resolves a `kind = 3` frame against the keys the applied room state
/// commits, and `CANONICAL.md` §6.1's step 4 refuses one from any other key
/// `uncommitted_key`. `§13.11`'s own table says the same twice over: a holds message applies
/// "under a fixture state that commits it", and "a holds message signed by a key the fixture
/// state does not name" is the row for `uncommitted_key`.
///
/// Vector 154's state commits `host-session` and `guest-1` as its two peers and then delivers
/// a holds message signed by **`guest-2`**, which that state does not commit; its
/// `expectSubject` asserts that frame in `applied` and its paths in `holds`. A conforming
/// client refuses it, so the vector asserts something no client can do. Its own fixture line
/// asks for "a peer that announces once and is then silent", which is a *committed* peer — so
/// the state is missing the entry the vector's premise needs, and the fix is to re-seal the
/// state with `guest-2` in `peers` and update the `hex`.
///
/// This pin describes that defect rather than replaying the vector, because a red test with no
/// words is worse than none: when the corpus is fixed this fails, says so, and the next reader
/// deletes it and moves 154 into `PASSABLE`.
#[test]
fn the_holds_vector_cannot_pass_and_the_corpus_is_what_is_wrong() {
    let fixture = match fixture() {
        Ok(fixture) => fixture,
        Err(error) => panic!("{error}"),
    };
    let vectors =
        load_decision_vectors().unwrap_or_else(|error| panic!("{error}"));
    let vector = vectors
        .iter()
        .find(|vector| vector.get("id").and_then(Value::as_str) == Some("154"))
        .unwrap_or_else(|| panic!("the corpus no longer holds vector 154"));
    let steps = vector["steps"]
        .as_array()
        .unwrap_or_else(|| panic!("no steps"));

    // The key each delivered frame is signed with, and the keys the states commit.
    let mut signed_with = Vec::new();
    let mut committed = Vec::new();
    for step in steps {
        let recipe = step.get("recipe").cloned().unwrap_or(Value::Null);
        if let Some(signer) = recipe.get("sign").and_then(Value::as_str) {
            signed_with.push((
                recipe["kind"].as_u64().unwrap_or_default(),
                signer.to_string(),
            ));
        }
        if let Some(peers) = recipe
            .get("payload")
            .and_then(|payload| payload.get("peers"))
            .and_then(Value::as_object)
        {
            committed.extend(peers.keys().cloned());
        }
    }
    let Some((_, sends_holds)) =
        signed_with.iter().find(|(kind, _)| *kind == 3)
    else {
        panic!("154 delivers no holds message");
    };
    let spelling = encode_key(&fixture.key(sends_holds).unwrap().public);
    assert!(
        !committed.contains(&spelling),
        "vector 154's state now commits {sends_holds}: the corpus is fixed, so move 154 into \
         PASSABLE and delete this pin"
    );

    // And what this client does with it, which is what §13.4 says any conforming client does.
    let mut subject =
        Subject::start().unwrap_or_else(|error| panic!("{error}"));
    let start = steps
        .iter()
        .find(|step| step.get("op").and_then(Value::as_str) == Some("start"))
        .unwrap_or_else(|| panic!("154 has no `start`"));
    subject
        .request(&join_command(&fixture, vector, start).unwrap())
        .unwrap_or_else(|error| panic!("{error}"));
    for step in steps {
        if step.get("op").and_then(Value::as_str) != Some("deliver") {
            continue;
        }
        let raw = delivered("154", &fixture, step)
            .unwrap_or_else(|error| panic!("{error}"));
        subject
            .request(&json!({"cmd": "deliver", "frame": hex_of(&raw)}))
            .unwrap_or_else(|error| panic!("{error}"));
    }
    let report = subject
        .report(&json!({"cmd": "report"}))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        report["dropped"],
        json!([{"frame": 1, "reason": "uncommitted_key"}]),
        "a holds message from a key no state commits is refused and does not end the session"
    );
    assert_eq!(report["holds"], json!({}));
    assert_eq!(report["ended"], json!(false));
}
