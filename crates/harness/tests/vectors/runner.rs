//! Replays `spec/vectors/*.json` against a real server, and asserts the bytes.
//!
//! The vector set is the artifact; this module is what keeps it from rotting. Every step in
//! a transcript is executed against a server the harness starts, and every frame the vector
//! claims is compared twice: structurally, member by member, so a failure says which member
//! was wrong; and then as whole bytes, because the vector is written in the canonical form
//! of `spec/CANONICAL.md` and the reference server must produce exactly that.
//!
//! A vector may carry `$name` placeholders where the server mints a value — a room id, a
//! token, a peer id. A placeholder binds the first time it is seen and must match every later
//! time, which is what makes `room.created` and a later `room.joined` demonstrably the same
//! room. `$_` is the placeholder that matches anything and is never remembered: it is for the
//! members `PROTOCOL.md` calls unstable, such as `error.message`.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::env;
use std::error::Error as StdError;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use selvage_harness::{Harness, ServerConfig};
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;
use yrs::encoding::read::Cursor;
use yrs::sync::protocol::{Message as YMessage, MessageReader, SyncMessage};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::{ClientID, Doc, GetString, ReadTxn, Transact, Update};

/// Anything a transcript can fail with.
pub type Failure = Box<dyn StdError>;

/// The placeholder that matches anything and is never remembered.
const ANY: &str = "$_";

/// One vector file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Vector {
    pub id: String,
    pub title: String,
    pub spec: String,
    pub selvage: String,
    pub canonical: String,
    /// The vector's own explanation, for whoever reads the file rather than runs it.
    #[serde(default)]
    #[expect(
        dead_code,
        reason = "the loader must accept the member; the runner has no use for the prose"
    )]
    pub notes: String,
    #[serde(default)]
    pub harness: ServerSpec,
    pub steps: Vec<Step>,
}

/// The server a vector needs. `room_grace_ms` is the host-reconnect grace period.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSpec {
    #[serde(default = "default_grace")]
    pub room_grace_ms: u64,
}

const fn default_grace() -> u64 {
    // The reference default, when a vector does not care about the grace period.
    30_000
}

impl Default for ServerSpec {
    fn default() -> Self {
        Self {
            room_grace_ms: default_grace(),
        }
    }
}

/// One step of a transcript. A step may carry a member the runner does not read — `refused`
/// marks a frame the vector sends on purpose knowing it is malformed — because the schema
/// validator and this runner are not the same reader.
#[derive(Debug, Deserialize)]
pub struct Step {
    pub op: String,
    #[serde(default)]
    pub conn: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub hex: Option<String>,
    #[serde(default)]
    pub frame: Option<FrameSpec>,
    #[serde(default)]
    pub apply: bool,
    #[serde(default)]
    pub code: Option<u16>,
    #[serde(default)]
    pub status: Option<u16>,
    #[serde(default)]
    pub ms: Option<u64>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub conns: Vec<String>,
}

/// What a binary frame must be, described rather than spelled out: a payload depends on the
/// random client id of whoever wrote it, the framing does not.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameSpec {
    pub message_type: u64,
    #[serde(default)]
    pub sync_type: Option<u64>,
    #[serde(default)]
    pub awareness: Option<AwarenessSpec>,
}

/// An awareness update's content, as the reference decoder reads it back.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwarenessSpec {
    pub clients: Vec<u64>,
    pub clock: u64,
    pub state: Value,
}

type Raw = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The vector directory: `SELVAGE_VECTORS` when the build supplies it, otherwise the repository's
/// `spec/vectors` relative to this crate.
///
/// The Nix build copies only `impl/` into the sandbox, so `impl/flake.nix` hands the directory in
/// explicitly; from a checkout the relative path is correct.
#[must_use]
pub fn root() -> PathBuf {
    // `impl/crates/harness` -> the repository's `spec/vectors`.
    env::var_os("SELVAGE_VECTORS").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../spec/vectors"),
        PathBuf::from,
    )
}

/// Every vector, ordered by id.
///
/// # Errors
///
/// Returns an error when the directory cannot be read or a file is not a vector.
pub fn load() -> Result<Vec<Vector>, Failure> {
    let mut vectors = Vec::new();
    for entry in fs::read_dir(root())? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let bytes = fs::read_to_string(&path)?;
        let vector: Vector = serde_json::from_str(&bytes)
            .map_err(|e| format!("{} is not a vector: {e}", path.display()))?;
        vectors.push(vector);
    }
    vectors.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(vectors)
}

/// What the vector's placeholders have been bound to.
///
/// `named` holds the ones that mean one thing — a room id, a token, a peer id — and must be
/// the same every time they are seen. `matched` holds the value of *every* placeholder in the
/// order the frame's members appear, which is what the byte comparison needs: it rebuilds the
/// vector's frame with the wire's own values and compares.
#[derive(Clone, Default)]
struct Bindings {
    named: BTreeMap<String, String>,
    matched: Vec<String>,
}

impl Bindings {
    fn bind(&mut self, name: &str, value: &str) -> Result<(), String> {
        self.matched.push(value.to_string());
        if name == ANY {
            return Ok(());
        }
        match self.named.get(name) {
            Some(bound) if bound == value => Ok(()),
            Some(bound) => Err(format!(
                "`{name}` was {bound:?} earlier and is {value:?} here"
            )),
            None => {
                self.named.insert(name.to_string(), value.to_string());
                Ok(())
            }
        }
    }
}

/// A seated transcript: the peers, what has been bound, and the last HTTP reply.
struct Session {
    peers: BTreeMap<String, Peer>,
    bindings: Bindings,
    body: Option<String>,
    status: Option<u16>,
}

struct Peer {
    name: String,
    ws: Raw,
    doc: Doc,
    frames: usize,
}

/// A frame as it arrived.
enum Incoming {
    Text(String),
    Binary(Vec<u8>),
    Closed { code: u16, reason: String },
}

/// Reads a frame the runner cares about. A ping, a pong or a continuation is none of its
/// business: WebSocket keepalive is below the session layer.
fn classify(message: Message) -> Option<Incoming> {
    match message {
        Message::Text(text) => Some(Incoming::Text(text.to_string())),
        Message::Binary(bytes) => Some(Incoming::Binary(bytes.to_vec())),
        Message::Close(Some(frame)) => Some(Incoming::Closed {
            code: u16::from(frame.code),
            reason: frame.reason.to_string(),
        }),
        Message::Close(None) => Some(Incoming::Closed {
            code: 1005,
            reason: String::new(),
        }),
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => None,
    }
}

impl Peer {
    /// The next frame that is not a protocol-level ping, pong or continuation.
    #[expect(
        clippy::excessive_nesting,
        reason = "a read loop over frames; the two ways out are one level deeper than the gate allows"
    )]
    async fn incoming(&mut self) -> Result<Incoming, Failure> {
        loop {
            let Some(message) = self.ws.next().await.transpose()? else {
                return Ok(Incoming::Closed {
                    code: 1005,
                    reason: "the socket ended".to_string(),
                });
            };
            let Some(incoming) = classify(message) else {
                continue;
            };
            self.frames = self.frames.saturating_add(1);
            return Ok(incoming);
        }
    }

    /// The next frame, which this step claims is text.
    async fn text(&mut self, expect: &str) -> Result<String, Failure> {
        match self.incoming().await? {
            Incoming::Text(text) => Ok(text),
            Incoming::Binary(_) => {
                Err(self
                    .wrong("a binary frame where a text frame was expected"))
            }
            Incoming::Closed { code, reason } => Err(self.wrong(&format!(
                "closed with {code} ({reason}) before {expect}"
            ))),
        }
    }

    /// The next frame, which this step claims is binary.
    async fn binary(&mut self) -> Result<Vec<u8>, Failure> {
        match self.incoming().await? {
            Incoming::Binary(bytes) => Ok(bytes),
            Incoming::Text(text) => Err(self.wrong(&format!(
                "a text frame where binary was expected: {text}"
            ))),
            Incoming::Closed { code, reason } => Err(self.wrong(&format!(
                "closed with {code} ({reason}) before the binary frame"
            ))),
        }
    }

    /// Reads until the server closes, returning the close code.
    async fn closed(&mut self) -> Result<u16, Failure> {
        match self.incoming().await? {
            Incoming::Closed { code, .. } => Ok(code),
            Incoming::Text(text) => Err(self.wrong(&format!(
                "a text frame where a close was expected: {text}"
            ))),
            Incoming::Binary(_) => {
                Err(self.wrong("a binary frame where a close was expected"))
            }
        }
    }

    fn wrong(&self, what: &str) -> Failure {
        format!("on `{}`, after {} frames: {what}", self.name, self.frames)
            .into()
    }

    fn text_at(&self, path: &str) -> String {
        self.doc
            .get_or_insert_text(path)
            .get_string(&self.doc.transact())
    }

    fn state(&self) -> Vec<(u64, u32)> {
        let held = self.doc.transact().state_vector();
        let mut sorted = Vec::new();
        for (id, clock) in held.iter() {
            sorted.push((id.get(), *clock));
        }
        sorted.sort_unstable();
        sorted
    }

    /// Applies the sync messages in a frame, as a peer does (§7). Awareness is not text.
    #[expect(
        clippy::excessive_nesting,
        reason = "a frame is a stream of messages; skipping one is one level deeper than the gate allows"
    )]
    fn apply(&self, frame: &[u8]) -> Result<(), Failure> {
        let mut decoder = DecoderV1::new(Cursor::new(frame));
        for message in MessageReader::new(&mut decoder) {
            let YMessage::Sync(
                SyncMessage::SyncStep2(update) | SyncMessage::Update(update),
            ) = message?
            else {
                continue;
            };
            self.doc
                .transact_mut()
                .apply_update(Update::decode_v1(&update)?)?;
        }
        Ok(())
    }
}

impl Session {
    fn new() -> Self {
        Self {
            peers: BTreeMap::new(),
            bindings: Bindings::default(),
            body: None,
            status: None,
        }
    }

    fn peer(&mut self, step: &Step) -> Result<&mut Peer, Failure> {
        let name = step.conn.clone().ok_or("this step needs a conn")?;
        self.peers
            .get_mut(&name)
            .ok_or_else(|| format!("no connection named {name}").into())
    }

    /// Substitutes the named bindings into a URL, longest name first so that `$room` cannot
    /// swallow the front of a name that starts with it.
    #[expect(
        clippy::excessive_nesting,
        reason = "a substitution over a set of bound names, inside a loop"
    )]
    fn fill(&self, target: &str) -> String {
        let mut names: Vec<&String> = self.bindings.named.keys().collect();
        names.sort_by_key(|name| Reverse(name.len()));
        let mut out = target.to_string();
        for name in names {
            let Some(value) = self.bindings.named.get(name) else {
                continue;
            };
            out = out.replace(name.as_str(), value.as_str());
        }
        out
    }
}

// --- comparison ---------------------------------------------------------------

/// The vector's frame with every placeholder replaced by the value it matched.
///
/// A placeholder is a whole string member, so it appears in the canonical text as `"$name"`.
/// Members are written in ascending order and arrays in order, so the placeholders in the
/// text are the wildcards in the order the match saw them.
///
/// # Errors
///
/// Returns an error when the text holds a malformed placeholder or holds more of them than
/// were matched.
fn expected_bytes(text: &str, matched: &[String]) -> Result<String, Failure> {
    let mut out = String::new();
    let mut rest = text;
    let mut index = 0;
    while let Some(at) = rest.find("\"$") {
        let before = rest.get(..at).unwrap_or_default();
        out.push_str(before);
        let span = rest.get(at.saturating_add(1)..).unwrap_or_default();
        let end = span.find('"').ok_or("a placeholder is not closed")?;
        let value = matched
            .get(index)
            .ok_or("a placeholder has no matched value")?;
        out.push_str(&serde_json::to_string(value)?);
        rest = span.get(end.saturating_add(1)..).unwrap_or_default();
        index = index.saturating_add(1);
    }
    out.push_str(rest);
    Ok(out)
}

/// Matches one vector value against one on the wire, binding placeholders as it goes.
///
/// Objects must have the *same* member set in both directions: a version-locked vector is
/// checking that no member has been silently added or renamed (`spec/CANONICAL.md` §3).
#[expect(
    clippy::excessive_nesting,
    reason = "a recursive matcher over JSON; the nesting is the shape of the data"
)]
fn matches(
    expected: &Value,
    actual: &Value,
    bindings: &mut Bindings,
) -> Result<(), String> {
    if let Some(name) = expected.as_str().filter(|text| text.starts_with('$')) {
        let value = actual.as_str().ok_or_else(|| {
            format!("`{name}` is a placeholder but the wire has {actual}")
        })?;
        return bindings.bind(name, value);
    }

    match (expected, actual) {
        (Value::Object(want), Value::Object(have)) => {
            for (member, value) in want {
                let found = have.get(member).ok_or_else(|| {
                    format!("missing member `{member}` in {actual}")
                })?;
                if member == "peers" {
                    matches_peers(value, found, bindings)?;
                } else {
                    matches(value, found, bindings)?;
                }
            }
            for member in have.keys() {
                if !want.contains_key(member) {
                    return Err(format!(
                        "the wire has an unexpected member `{member}`"
                    ));
                }
            }
            Ok(())
        }
        (Value::Array(want), Value::Array(have)) => {
            if want.len() != have.len() {
                return Err(format!(
                    "expected {} members, the wire has {}",
                    want.len(),
                    have.len()
                ));
            }
            for (left, right) in want.iter().zip(have.iter()) {
                matches(left, right, bindings)?;
            }
            Ok(())
        }
        _ if expected == actual => Ok(()),
        _ => Err(format!("expected {expected}, the wire has {actual}")),
    }
}

/// Peers are compared as a set: the protocol promises no order for them.
#[expect(
    clippy::excessive_nesting,
    reason = "a set comparison that tries each candidate; the nesting is the search"
)]
fn matches_peers(
    want: &Value,
    have: &Value,
    bindings: &mut Bindings,
) -> Result<(), String> {
    let (Some(pairs), Some(held)) = (want.as_array(), have.as_array()) else {
        return Err(format!("expected a list of peers, the wire has {have}"));
    };
    if pairs.len() != held.len() {
        return Err(format!(
            "expected {} peers, the wire has {}",
            pairs.len(),
            held.len()
        ));
    }
    let mut used = vec![false; held.len()];
    for value in pairs {
        let mut found = None;
        for (index, candidate) in held.iter().enumerate() {
            if used.get(index) == Some(&true) {
                continue;
            }
            let mut trial = bindings.clone();
            if matches(value, candidate, &mut trial).is_ok() {
                found = Some((index, trial));
                break;
            }
        }
        let Some((index, trial)) = found else {
            return Err(format!("no peer in {have:?} matches {value}"));
        };
        if let Some(slot) = used.get_mut(index) {
            *slot = true;
        }
        *bindings = trial;
    }
    Ok(())
}

/// Checks one text frame, structurally and then byte for byte.
fn check_text(
    actual: &str,
    expected: &str,
    bindings: &mut Bindings,
) -> Result<(), Failure> {
    let want: Value = serde_json::from_str(expected)
        .map_err(|e| format!("the vector frame is not JSON: {e}"))?;
    let have: Value = serde_json::from_str(actual)
        .map_err(|e| format!("the wire frame is not JSON: {e}: {actual}"))?;
    // The named bindings carry across steps; the matched values are this frame's, in order.
    let mut trial = bindings.clone();
    trial.matched.clear();
    matches(&want, &have, &mut trial).map_err(|problem| {
        format!("frame does not match:\n  vector: {expected}\n  wire:   {actual}\n  {problem}")
    })?;
    let wanted = expected_bytes(expected, &trial.matched)?;
    *bindings = trial;
    if actual != wanted {
        return Err(format!(
            "the wire frame is not the canonical bytes the vector claims:\n  vector: {wanted}\n  wire:   {actual}"
        )
        .into());
    }
    Ok(())
}

fn hex_bytes(hex: &str) -> Result<Vec<u8>, Failure> {
    let mut out = Vec::new();
    for byte in hex.split_whitespace() {
        out.push(
            u8::from_str_radix(byte, 16)
                .map_err(|e| format!("`{byte}`: {e}"))?,
        );
    }
    Ok(out)
}

fn bytes_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Checks a binary frame against a description: the framing is fixed, the payload is not.
fn check_frame_spec(spec: &FrameSpec, frame: &[u8]) -> Result<(), Failure> {
    let mut decoder = DecoderV1::new(Cursor::new(frame));
    let message = YMessage::decode(&mut decoder).map_err(|e| {
        format!("the frame is not y-protocols: {e}: {}", bytes_hex(frame))
    })?;
    if spec.message_type == 0 {
        let YMessage::Sync(sync) = &message else {
            return Err(format!(
                "expected a sync frame, the frame is {message:?}"
            )
            .into());
        };
        let tag = match sync {
            SyncMessage::SyncStep1(_) => 0,
            SyncMessage::SyncStep2(_) => 1,
            SyncMessage::Update(_) => 2,
        };
        if let Some(wanted) = spec.sync_type
            && wanted != tag
        {
            return Err(format!(
                "expected sync_type {wanted}, the frame has {tag}"
            )
            .into());
        }
        return Ok(());
    }

    let YMessage::Awareness(update) = &message else {
        return Err(format!(
            "expected an awareness frame, the frame is {message:?}"
        )
        .into());
    };
    let wanted = spec
        .awareness
        .as_ref()
        .ok_or("an awareness frame needs an awareness spec")?;
    for client in &wanted.clients {
        let entry = update
            .clients
            .get(&ClientID::new(*client))
            .ok_or_else(|| format!("no awareness state for client {client}"))?;
        if u64::from(entry.clock) != wanted.clock {
            return Err(format!(
                "client {client} has clock {}, not {}",
                entry.clock, wanted.clock
            )
            .into());
        }
        let state: Value = serde_json::from_str(&entry.json)?;
        if state != wanted.state {
            return Err(format!(
                "client {client} published {state}, not {}",
                wanted.state
            )
            .into());
        }
    }
    Ok(())
}

// --- execution ----------------------------------------------------------------

async fn open(
    session: &mut Session,
    harness: &Harness,
    step: &Step,
) -> Result<(), Failure> {
    let name = step.conn.clone().ok_or("open needs a conn")?;
    let target = step.target.clone().ok_or("open needs a target")?;
    let url = format!("{}{}", harness.ws_base(), session.fill(&target));
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    session.peers.insert(
        name.clone(),
        Peer {
            name,
            ws,
            doc: Doc::new(),
            frames: 0,
        },
    );
    Ok(())
}

async fn expect_text(
    session: &mut Session,
    step: &Step,
) -> Result<(), Failure> {
    let expected = step.text.clone().ok_or("expect needs text")?;
    let actual = session.peer(step)?.text(&expected).await?;
    check_text(&actual, &expected, &mut session.bindings)
}

async fn send_binary(
    session: &mut Session,
    step: &Step,
) -> Result<(), Failure> {
    let bytes = hex_bytes(step.hex.as_deref().ok_or("sendBinary needs hex")?)?;
    let peer = session.peer(step)?;
    peer.ws.send(Message::binary(bytes.clone())).await?;
    if step.apply {
        peer.apply(&bytes)?;
    }
    Ok(())
}

async fn expect_binary(
    session: &mut Session,
    step: &Step,
) -> Result<(), Failure> {
    let peer = session.peer(step)?;
    let seen = peer.frames;
    let actual = peer.binary().await?;
    if let Some(hex) = &step.hex {
        let wanted = hex_bytes(hex)?;
        if actual != wanted {
            return Err(format!(
                "binary frame {seen} differs:\n  vector: {hex}\n  wire:   {}",
                bytes_hex(&actual)
            )
            .into());
        }
    } else if let Some(spec) = &step.frame {
        check_frame_spec(spec, &actual)?;
    } else {
        return Err("expectBinary needs hex or frame".into());
    }
    if step.apply {
        peer.apply(&actual)?;
    }
    Ok(())
}

async fn expect_close(
    session: &mut Session,
    step: &Step,
) -> Result<(), Failure> {
    let wanted = step.code.ok_or("expectClose needs code")?;
    let code = session.peer(step)?.closed().await?;
    if code != wanted {
        return Err(
            format!("closed with {code}, the vector claims {wanted}").into()
        );
    }
    Ok(())
}

/// Fetches a path over plain HTTP and remembers the status and the body.
async fn http(
    session: &mut Session,
    harness: &Harness,
    step: &Step,
) -> Result<(), Failure> {
    let target =
        session.fill(step.target.as_deref().ok_or("http needs target")?);
    let addr = harness
        .http_base()
        .trim_start_matches("http://")
        .to_string();
    let mut stream = TcpStream::connect(&addr).await?;
    let request = format!(
        "GET {target} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8(raw)?;
    let (head, body) =
        text.split_once("\r\n\r\n").ok_or("the reply has no body")?;
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .ok_or("the reply has no status")?
        .parse()?;
    session.status = Some(status);
    session.body = Some(body.to_string());
    Ok(())
}

fn expect_status(session: &Session, step: &Step) -> Result<(), Failure> {
    let wanted = step.status.ok_or("expectStatus needs status")?;
    let status = session.status.ok_or("no HTTP request has been made")?;
    if status != wanted {
        return Err(
            format!("the reply's status is {status}, not {wanted}").into()
        );
    }
    Ok(())
}

fn expect_body(session: &mut Session, step: &Step) -> Result<(), Failure> {
    let expected = step.text.clone().ok_or("expectBody needs text")?;
    let actual = session
        .body
        .clone()
        .ok_or("no HTTP request has been made")?;
    check_text(&actual, &expected, &mut session.bindings)
}

fn expect_doc(session: &Session, step: &Step) -> Result<(), Failure> {
    let path = step.path.clone().ok_or("expectDoc needs path")?;
    let wanted = step.text.clone().ok_or("expectDoc needs text")?;
    let name = step.conn.clone().ok_or("expectDoc needs conn")?;
    let peer = session
        .peers
        .get(&name)
        .ok_or_else(|| format!("no connection named {name}"))?;
    let actual = peer.text_at(&path);
    if actual != wanted {
        return Err(format!(
            "`{name}` holds {actual:?} in {path}, not {wanted:?}"
        )
        .into());
    }
    Ok(())
}

fn expect_same_state(session: &Session, step: &Step) -> Result<(), Failure> {
    let peers: Vec<&Peer> = step
        .conns
        .iter()
        .map(|name| {
            session
                .peers
                .get(name)
                .ok_or_else(|| format!("no connection named {name}"))
        })
        .collect::<Result<_, _>>()?;
    let Some(first) = peers.first() else {
        return Err("expectSameState needs conns".into());
    };
    let wanted = first.state();
    for peer in peers.iter().skip(1) {
        let held = peer.state();
        if held != wanted {
            return Err(format!(
                "`{}` and `{}` have different state vectors: {wanted:?} and {held:?}",
                first.name, peer.name
            )
            .into());
        }
    }
    Ok(())
}

/// Runs one step of a transcript.
async fn run_step(
    session: &mut Session,
    harness: &Harness,
    step: &Step,
) -> Result<(), Failure> {
    match step.op.as_str() {
        "open" => open(session, harness, step).await,
        "send" => {
            let text = step.text.clone().ok_or("send needs text")?;
            session.peer(step)?.ws.send(Message::text(text)).await?;
            Ok(())
        }
        "expect" => expect_text(session, step).await,
        "sendBinary" => send_binary(session, step).await,
        "expectBinary" => expect_binary(session, step).await,
        "expectClose" => expect_close(session, step).await,
        "close" => {
            session.peer(step)?.ws.close(None).await?;
            Ok(())
        }
        "wait" => {
            let ms = step.ms.ok_or("wait needs ms")?;
            sleep(Duration::from_millis(ms)).await;
            Ok(())
        }
        "http" => http(session, harness, step).await,
        "expectStatus" => expect_status(session, step),
        "expectBody" => expect_body(session, step),
        "expectDoc" => expect_doc(session, step),
        "expectSameState" => expect_same_state(session, step),
        other => {
            Err(format!("`{other}` is not a step this runner knows").into())
        }
    }
}

/// Replays one vector against a fresh server.
///
/// # Errors
///
/// Returns the first step that did not hold, with the vector and the step it was.
pub async fn replay(vector: &Vector) -> Result<(), Failure> {
    if vector.selvage != "selvage/1" || vector.canonical != "SJ-C/1" {
        return Err(format!(
            "{} is bound to {} / {}, but this implementation speaks selvage/1 / SJ-C/1",
            vector.id, vector.selvage, vector.canonical
        )
        .into());
    }
    let config = ServerConfig {
        room_grace: Duration::from_millis(vector.harness.room_grace_ms),
        ..ServerConfig::default()
    };
    let harness = Harness::start_with(config).await;
    let mut session = Session::new();
    for (index, step) in vector.steps.iter().enumerate() {
        run_step(&mut session, &harness, step)
            .await
            .map_err(|error| {
                format!(
                    "vector {} — {} ({}), step {index} `{}`: {error}",
                    vector.id, vector.title, vector.spec, step.op
                )
            })?;
    }
    Ok(())
}
