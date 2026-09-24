//! Wire types for the Selvage Session Protocol, wire version `selvage/2`.
//!
//! The protocol itself — the prose of `PROTOCOL.md`, the byte rule of `CANONICAL.md` and the
//! JSON Schema — lives in the specification repository
//! (`github.com/selvage-protocol/specification`), which is its canonical source; this crate
//! is one implementation of it.
//!
//! Every struct here declares its members in the canonical order of `CANONICAL.md`
//! §2.1 — ascending by member name — because `serde` writes them in declaration
//! order and `CANONICAL.md` fixes the bytes. Objects built with `serde_json::json!`
//! are sorted by `serde_json`'s map, so they need no such care.
//!
//! This crate is deliberately free of I/O: it holds the JSON session envelope, the
//! session-level vocabulary (methods, events, error codes, close codes) and the small
//! amount of URL plumbing needed to mint and join a room. Document and awareness
//! payloads are *not* described here — they are y-protocols binary frames
//! (`yrs::sync::protocol::Message`) and are opaque to the server.

use std::collections::HashSet;
use std::fmt;
use std::fmt::Write as _;
use std::str;

use std::error::Error as StdError;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Wire version carried in every session envelope.
pub const WIRE_VERSION: &str = "selvage/2";

/// WebSocket endpoint path.
pub const ENDPOINT_PATH: &str = "/session";

/// Negotiation endpoint path, served over plain HTTP on the same listener.
pub const META_PATH: &str = "/meta";

/// Capabilities this implementation advertises (`PROTOCOL.md` §2).
pub const CAPABILITIES: &[&str] = &["y-protocols/1", "awareness"];

/// The longest `display_name` a server seats, in UTF-16 code units (`PROTOCOL.md` §5).
///
/// Counted the way a JavaScript string is measured, so an astral character costs two.
/// `str::len` (bytes) and `chars().count()` (code points) both answer the wrong question.
pub const DISPLAY_NAME_MAX_UTF16: usize = 32;

/// Whether a `display_name` is longer than [`DISPLAY_NAME_MAX_UTF16`].
#[must_use]
pub fn display_name_over_limit(name: &str) -> bool {
    name.encode_utf16().count() > DISPLAY_NAME_MAX_UTF16
}

/// Whether `text` carries a control character: C0, DEL or C1, the Unicode `Cc` category.
///
/// A `display_name` is echoed into every peer's roster and a `path` into every peer's
/// document set, its file tree and any terminal a peer prints it to, so a control
/// character in one — an ANSI escape, a carriage return, a NUL — reaches a surface that
/// never asked for it. §5 refuses both kinds of value `bad_params` for exactly that.
#[must_use]
pub fn has_control_characters(text: &str) -> bool {
    text.chars().any(char::is_control)
}

/// Client -> server method names.
pub mod method {
    pub const SESSION_HELLO: &str = "session.hello";
    pub const SESSION_RENAME: &str = "session.rename";
}

/// Server -> client event names.
pub mod event {
    pub const ROOM_CREATED: &str = "room.created";
    pub const ROOM_JOINED: &str = "room.joined";
    pub const PEER_JOINED: &str = "peer.joined";
    pub const PEER_LEFT: &str = "peer.left";
    pub const PEER_RENAMED: &str = "peer.renamed";
    pub const ROOM_GONE: &str = "room.gone";
    pub const SESSION_ERROR: &str = "session.error";
}

/// Machine-readable error codes for `error.code`.
pub mod code {
    pub const UNKNOWN_METHOD: &str = "unknown_method";
    pub const BAD_MESSAGE: &str = "bad_message";
    pub const BAD_PARAMS: &str = "bad_params";
    pub const HELLO_REQUIRED: &str = "hello_required";
    pub const ROOM_UNKNOWN: &str = "room_unknown";
    pub const ROOM_GONE: &str = "room_gone";
    pub const TOKEN_INVALID: &str = "token_invalid";
    pub const ALREADY_SEATED: &str = "already_seated";
}

/// WebSocket close codes in the private use range, matching [`code`].
pub mod close {
    pub const PROTOCOL_ERROR: u16 = 4000;
    pub const ROOM_UNKNOWN: u16 = 4001;
    pub const TOKEN_INVALID: u16 = 4002;
    pub const ROOM_GONE: u16 = 4003;
}

/// Maps a fatal session error code to the close code used to end the connection.
#[must_use]
pub fn close_code_for(code: &str) -> u16 {
    match code {
        code::ROOM_UNKNOWN => close::ROOM_UNKNOWN,
        code::TOKEN_INVALID => close::TOKEN_INVALID,
        code::ROOM_GONE => close::ROOM_GONE,
        _ => close::PROTOCOL_ERROR,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Keepalive {
    pub awareness_expire_ms: u64,
    pub awareness_renew_ms: u64,
    pub ping_interval_ms: u64,
}

impl Default for Keepalive {
    fn default() -> Self {
        Self {
            awareness_expire_ms: 30_000,
            awareness_renew_ms: 15_000,
            ping_interval_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorObject {
    pub code: String,
    pub message: String,
}

/// A client -> server request. Unknown fields are ignored.
///
/// Members are declared in the canonical order of `CANONICAL.md` §2.1 — ascending by
/// name — because `serde` writes them in declaration order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub v: String,
}

impl ClientMessage {
    #[must_use]
    pub fn new(id: u64, method: &str, params: Value) -> Self {
        Self {
            id: Some(id),
            method: method.to_string(),
            params,
            v: WIRE_VERSION.to_string(),
        }
    }

    /// Serializes the envelope.
    ///
    /// # Errors
    ///
    /// Returns the serializer's error. These shapes are infallible in practice, so it
    /// is propagated rather than unwrapped purely so callers decide what a failed
    /// send means.
    pub fn to_text(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Parses a client frame, refusing one that repeats a member name anywhere inside it
    /// (§4) — the `to_text` half of the same rule, in the other direction. `serde` refuses
    /// a repeated member of the envelope itself, but a repeated member of `params`, or of
    /// anything nested in one, is last-wins to a `serde_json::Value` and is caught here.
    ///
    /// # Errors
    ///
    /// Returns the parse error of the frame, or the error naming the first repeated
    /// member, which is `bad_message` to a server.
    pub fn from_text(text: &str) -> Result<Self, serde_json::Error> {
        no_duplicate_members(text)?;
        serde_json::from_str(text)
    }
}

/// A server -> client message. A response carries `id` and exactly one of
/// `result`/`error`; an event carries `event`, `params` and no `id`.
///
/// Members are declared in the canonical order of `CANONICAL.md` §2.1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    pub v: String,
}

impl ServerMessage {
    #[must_use]
    pub fn response(id: u64, result: Value) -> Self {
        Self {
            error: None,
            event: None,
            id: Some(id),
            params: None,
            result: Some(result),
            v: WIRE_VERSION.to_string(),
        }
    }

    #[must_use]
    pub fn error(id: u64, code: &str, message: impl Into<String>) -> Self {
        Self {
            error: Some(ErrorObject {
                code: code.to_string(),
                message: message.into(),
            }),
            event: None,
            id: Some(id),
            params: None,
            result: None,
            v: WIRE_VERSION.to_string(),
        }
    }

    #[must_use]
    pub fn event(name: &str, params: Value) -> Self {
        Self {
            error: None,
            event: Some(name.to_string()),
            id: None,
            params: Some(params),
            result: None,
            v: WIRE_VERSION.to_string(),
        }
    }

    /// Serializes the envelope.
    ///
    /// # Errors
    ///
    /// Returns the serializer's error. These shapes are infallible in practice, so it
    /// is propagated rather than unwrapped purely so callers decide what a failed
    /// send means.
    pub fn to_text(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Parses a server frame, refusing one that repeats a member name anywhere inside it
    /// (§4), exactly as [`ClientMessage::from_text`] does in the other direction.
    ///
    /// # Errors
    ///
    /// Returns the parse error of the frame, or the error naming the first repeated
    /// member.
    pub fn from_text(text: &str) -> Result<Self, serde_json::Error> {
        no_duplicate_members(text)?;
        serde_json::from_str(text)
    }
}

/// `session.hello` params — the client's half of the handshake (`PROTOCOL.md` §5).
/// There is no `role` to claim: the server seats nobody as anything, and a `role` member
/// a caller sends is ignored as an unknown name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub awareness_client_id: Option<u64>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    pub display_name: String,
}

/// A peer as the server records it: `PROTOCOL.md` §6.1's `PeerInfo`. `role` is not a
/// member — it is the sealed room state's, not the server's (§1.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub awareness_client_id: Option<u64>,
    pub display_name: String,
    pub peer_id: String,
}

/// `room.created` / `room.joined` params: the members that carried the server's state
/// are gone and nothing took their place (`PROTOCOL.md` §6.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionParams {
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub keepalive: Keepalive,
    #[serde(default)]
    pub peers: Vec<PeerInfo>,
    pub room_id: String,
    #[serde(rename = "self")]
    pub self_peer: PeerInfo,
    /// Present only for the connection that minted the room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// `session.rename` params: the name this connection wants from now on. The bound is the
/// handshake's (`PROTOCOL.md` §5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameParams {
    pub display_name: String,
}

/// `peer.joined` / `peer.left` params.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEvent {
    pub peer: PeerInfo,
}

/// `peer.renamed` params: the one field a rename changes, about the peer it changed for.
/// It carries no `role` and no `awareness_client_id`; a rename touches neither, and a
/// receiver with no record for `peer_id` has to ignore the event rather than invent them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRenamedParams {
    pub display_name: String,
    pub peer_id: String,
}

/// What this implementation calls itself in `GET /meta` (`PROTOCOL.md` §2). Free-form and
/// not stable: a peer **MUST NOT** depend on it.
pub const SERVER_NAME: &str = concat!("selvaged/", env!("CARGO_PKG_VERSION"));

/// `GET /meta`'s `keepalive`: the session's clocks plus the room's grace period, which the
/// handshake reply has no place for. A host learns the grace only from `host.detached`,
/// which reaches the peers it *left behind* — the one connection that has to size its retry
/// budget to the grace is the one that is gone, so it has to be advertised before the
/// session exists. Members are in the canonical order of `CANONICAL.md` §2.1.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct MetaKeepalive {
    pub awareness_expire_ms: u64,
    pub awareness_renew_ms: u64,
    pub ping_interval_ms: u64,
    /// How long a room survives its last connection ending (`PROTOCOL.md` §2, §9).
    pub room_grace_ms: u64,
}

impl From<(Keepalive, u64)> for MetaKeepalive {
    fn from((keepalive, room_grace_ms): (Keepalive, u64)) -> Self {
        Self {
            awareness_expire_ms: keepalive.awareness_expire_ms,
            awareness_renew_ms: keepalive.awareness_renew_ms,
            ping_interval_ms: keepalive.ping_interval_ms,
            room_grace_ms,
        }
    }
}

/// `GET /meta` response body. Members are in the canonical order of
/// `CANONICAL.md` §2.1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub keepalive: MetaKeepalive,
    pub server: String,
    pub wire_versions: Vec<String>,
}

impl Meta {
    /// What this reference server advertises, with `keepalive` and the room grace it is
    /// actually configured with.
    #[must_use]
    pub fn reference(keepalive: Keepalive, room_grace_ms: u64) -> Self {
        Self {
            capabilities: CAPABILITIES
                .iter()
                .map(ToString::to_string)
                .collect(),
            keepalive: MetaKeepalive::from((keepalive, room_grace_ms)),
            server: SERVER_NAME.to_string(),
            wire_versions: vec![WIRE_VERSION.to_string()],
        }
    }
}

/// Whether `text` repeats a member name inside any object it holds.
///
/// `serde` refuses a repeated member of a *struct* — two `v` members never make it past
/// [`ClientMessage`] — but a member of a `params` object, or of anything nested in one,
/// is last-wins: the object is a `serde_json::Value`, and JSON's own reading of a
/// repeated name is that the last one is the value. One frame must not mean two things,
/// so §4 refuses a repetition anywhere in a frame, and this is the reading that finds it.
///
/// # Errors
///
/// Returns the parse error of the frame, or the error naming the first repeated member.
fn no_duplicate_members(text: &str) -> Result<(), serde_json::Error> {
    let mut de = serde_json::Deserializer::from_str(text);
    UniqueMembers::deserialize(&mut de)?;
    de.end()
}

/// A JSON document read only to find a repeated member name, anywhere inside it.
struct UniqueMembers;

impl<'de> Deserialize<'de> for UniqueMembers {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UniqueVisitor)
    }
}

/// Remembers `member`, reporting a name this object has already carried.
fn note_member<E: de::Error>(
    seen: &mut HashSet<String>,
    member: &str,
) -> Result<(), E> {
    if seen.insert(member.to_string()) {
        return Ok(());
    }
    Err(E::custom(format!("duplicate member: {member}")))
}

struct UniqueVisitor;

impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = UniqueMembers;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_map<A: MapAccess<'de>>(
        self,
        mut map: A,
    ) -> Result<Self::Value, A::Error> {
        let mut seen: HashSet<String> = HashSet::new();
        while let Some(member) = map.next_key::<String>()? {
            note_member(&mut seen, &member)?;
            map.next_value::<UniqueMembers>()?;
        }
        Ok(UniqueMembers)
    }

    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> Result<Self::Value, A::Error> {
        while seq.next_element::<UniqueMembers>()?.is_some() {}
        Ok(UniqueMembers)
    }

    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(UniqueMembers)
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(UniqueMembers)
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
        Ok(UniqueMembers)
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(UniqueMembers)
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
        Ok(UniqueMembers)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueMembers)
    }
}

/// A join query that names `room` or `token` more than once (`PROTOCOL.md` §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinQueryError {
    /// The parameter, named twice.
    Duplicate(&'static str),
}

impl fmt::Display for JoinQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate(name) => {
                write!(f, "the join query names {name} twice")
            }
        }
    }
}

impl StdError for JoinQueryError {}

/// The value a join query carries for `name`, or the refusal of a second one: a query that
/// names a parameter twice names two rooms, and a connection has to be one of them.
fn join_value(
    name: &'static str,
    already: bool,
    value: &str,
) -> Result<String, JoinQueryError> {
    if already {
        return Err(JoinQueryError::Duplicate(name));
    }
    Ok(percent_decode(value))
}

/// The room/token part of a join URL. `room` absent means "mint a new room".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JoinQuery {
    pub room: Option<String>,
    pub token: Option<String>,
}

/// Parses a URL query string into a room/token pair. Unknown parameters are ignored, and
/// one named more than once is refused rather than silently overwritten: which of two
/// `room` values a connection joins must not depend on their order.
///
/// # Errors
///
/// Returns [`JoinQueryError::Duplicate`] when `room` or `token` appears twice.
pub fn parse_join_query(query: &str) -> Result<JoinQuery, JoinQueryError> {
    let mut out = JoinQuery::default();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        match k {
            "room" => {
                out.room = Some(join_value("room", out.room.is_some(), v)?);
            }
            "token" => {
                out.token = Some(join_value("token", out.token.is_some(), v)?);
            }
            _ => {}
        }
    }
    Ok(out)
}

/// A connection URL split into the server base and the room/token it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionUrl {
    /// Scheme, authority and any path prefix — but not the endpoint path.
    pub base: String,
    pub join: JoinQuery,
}

/// Takes a full connection URL — in particular the invite URL a host publishes — apart
/// into the server base and the join query, which is what a client needs to connect.
///
/// Returns `None` when the URL does not address the session endpoint.
#[must_use]
pub fn parse_session_url(url: &str) -> Option<SessionUrl> {
    let (endpoint, query) = match url.split_once('?') {
        Some((endpoint, query)) => (endpoint, query),
        None => (url, ""),
    };
    // A URL that names `room` or `token` twice is not one this client can resolve to a
    // connection: the server refuses the query, and a client that guessed which of the two
    // values was meant would connect to a room the link did not name.
    let join = parse_join_query(query).ok()?;
    let base = endpoint.strip_suffix(ENDPOINT_PATH)?;
    Some(SessionUrl {
        base: base.to_string(),
        join,
    })
}

/// Builds the WebSocket URL for a connection. Host connections omit room and token.
///
/// `base` is the server base URL — scheme and authority, without the endpoint path. Pair
/// with [`parse_session_url`] when the base has to come out of a URL again.
#[must_use]
pub fn session_url(
    base: &str,
    room: Option<&str>,
    token: Option<&str>,
) -> String {
    let mut url = format!("{}{ENDPOINT_PATH}", base.trim_end_matches('/'));
    let mut sep = '?';
    for (key, part) in [("room", room), ("token", token)] {
        let Some(text) = part else {
            continue;
        };
        url.push(sep);
        sep = '&';
        url.push_str(key);
        url.push('=');
        url.push_str(&percent_encode(text));
    }
    url
}

/// True for the characters RFC 3986 leaves unescaped.
const fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

#[must_use]
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if is_unreserved(byte) {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Consumes `%XX` at the head of `rest`, returning the byte and what follows it.
fn take_escaped(rest: &[u8]) -> Option<(u8, &[u8])> {
    let pair = rest.get(..2)?;
    let hex = str::from_utf8(pair).ok()?;
    let byte = u8::from_str_radix(hex, 16).ok()?;
    Some((byte, rest.get(2..)?))
}

/// Decodes the percent escapes in a URL component.
///
/// A query is RFC 3986, not `application/x-www-form-urlencoded`: `+` is a literal plus in
/// it, and the reference's own [`percent_encode`] writes one as `%2B` for exactly that
/// reason. Treating it as a space would read a second implementation's literal `+` as a
/// different value — a different room, a different token — while both meant the same
/// thing.
#[must_use]
pub fn percent_decode(s: &str) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(s.len());
    let mut rest = s.as_bytes();
    while let Some((first, tail)) = rest.split_first() {
        let escaped = if *first == b'%' {
            take_escaped(tail)
        } else {
            None
        };
        if let Some((byte, after)) = escaped {
            out.push(byte);
            rest = after;
            continue;
        }
        out.push(*first);
        rest = tail;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// True when the wire version in an envelope is the one this protocol has.
///
/// `PROTOCOL.md` §10 with `CANONICAL.md` §2.5: the member has one value and there is no
/// grammar to write a second spelling in, so `selvage/2.0`, `selvage/2.9`, `selvage/1`,
/// `selvage/03` and `selvage` are all a version this receiver does not read, and the frame
/// carrying one is `bad_message` like any frame it cannot read. Nothing is negotiated and
/// nothing is compared by major.
#[must_use]
pub fn speaks(version: &str) -> bool {
    version == WIRE_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_shapes() {
        let req = ClientMessage::new(
            7,
            method::SESSION_RENAME,
            serde_json::json!({"display_name": "a.rs"}),
        );
        let text = req.to_text().unwrap();
        assert_eq!(
            text,
            r#"{"id":7,"method":"session.rename","params":{"display_name":"a.rs"},"v":"selvage/2"}"#
        );

        let ok = ServerMessage::response(7, serde_json::json!({}))
            .to_text()
            .unwrap();
        assert_eq!(ok, r#"{"id":7,"result":{},"v":"selvage/2"}"#);

        let err =
            ServerMessage::error(7, code::UNKNOWN_METHOD, "no such method")
                .to_text()
                .unwrap();
        assert_eq!(
            err,
            r#"{"error":{"code":"unknown_method","message":"no such method"},"id":7,"v":"selvage/2"}"#
        );

        let ev =
            ServerMessage::event(event::PEER_LEFT, serde_json::json!({"x": 1}))
                .to_text()
                .unwrap();
        assert_eq!(
            ev,
            r#"{"event":"peer.left","params":{"x":1},"v":"selvage/2"}"#
        );
    }

    #[test]
    fn rename_frames() {
        let request = ClientMessage::new(
            2,
            method::SESSION_RENAME,
            serde_json::json!({ "display_name": "Ada Lovelace" }),
        );
        assert_eq!(
            request.to_text().unwrap(),
            r#"{"id":2,"method":"session.rename","params":{"display_name":"Ada Lovelace"},"v":"selvage/2"}"#
        );

        let event = ServerMessage::event(
            event::PEER_RENAMED,
            serde_json::json!(PeerRenamedParams {
                display_name: "Ada Lovelace".to_string(),
                peer_id: "p-1".to_string(),
            }),
        );
        assert_eq!(
            event.to_text().unwrap(),
            r#"{"event":"peer.renamed","params":{"display_name":"Ada Lovelace","peer_id":"p-1"},"v":"selvage/2"}"#
        );

        // The wire is the contract in both directions: what this crate builds is what it
        // reads back.
        let params: PeerRenamedParams =
            serde_json::from_value(event.params.unwrap_or_default()).unwrap();
        assert_eq!(params.display_name, "Ada Lovelace");
        assert_eq!(params.peer_id, "p-1");

        let parsed: RenameParams =
            serde_json::from_value(request.params).unwrap();
        assert_eq!(parsed.display_name, "Ada Lovelace");
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let raw = r#"{"v":"selvage/2","id":1,"method":"session.hello",
                      "params":{"display_name":"Ada","nonsense":true},"future_field":9}"#;
        let msg: ClientMessage = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.id, Some(1));
        let params: HelloParams = serde_json::from_value(msg.params).unwrap();
        assert_eq!(params.display_name, "Ada");
    }

    #[test]
    fn a_display_name_is_measured_in_utf16_code_units() {
        // One astral character, so 32 code units from 31 code points.
        let at_limit = format!("{}𝄞", "a".repeat(30));
        assert_eq!(at_limit.encode_utf16().count(), 32);
        assert!(!display_name_over_limit(&at_limit));

        // One unit over, and still only 32 code points: a code-point bound admits it.
        let over_limit = format!("{}𝄞", "a".repeat(31));
        assert_eq!(over_limit.encode_utf16().count(), 33);
        assert_eq!(over_limit.chars().count(), 32);
        assert!(display_name_over_limit(&over_limit));

        // Bytes answer the other way: the at-limit name is already 34 of them.
        assert_eq!(at_limit.len(), 34);
    }

    /// The control characters §5 refuses are the Unicode `Cc` category: C0 (`\u{0}`,
    /// `\u{1b}`), DEL, and C1 (`\u{9b}`) — not the printable characters that sit beside
    /// them, and not the non-breaking space a name may be padded with.
    #[test]
    fn control_characters_are_the_cc_category() {
        for text in [
            "a\u{0}b",
            "\u{1b}[31m",
            "a\u{7f}b",
            "a\u{9b}b",
            "a\tb",
            "a\nb",
        ] {
            assert!(has_control_characters(text), "{text:?} is refused");
        }
        for text in ["Ada", " Ada ", "𝄞", "a\u{a0}b", "ｆ"] {
            assert!(!has_control_characters(text), "{text:?} is carried");
        }
    }

    #[test]
    fn a_plus_in_a_query_is_a_literal_plus() {
        // RFC 3986: `+` is a sub-delimiter, legal in a query and not a space. The two
        // spellings of one value must read the same, or a second implementation writing
        // the literal one is talking about a different room.
        let q = parse_join_query("room=r+1&token=a+b").expect("a query");
        assert_eq!(q.room.as_deref(), Some("r+1"));
        assert_eq!(q.token.as_deref(), Some("a+b"));
        assert_eq!(percent_decode("%2B"), "+");
        assert_eq!(percent_encode("a+b"), "a%2Bb");
        assert_eq!(percent_decode(&percent_encode("a+b")), "a+b");

        // A space still arrives as one, percent-encoded, which is the only way it can.
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(
            parse_join_query("room=a%20b")
                .expect("a query")
                .room
                .as_deref(),
            Some("a b")
        );
    }

    /// §4: a repeated member name anywhere in a frame is one frame that means two things,
    /// and a receiver refuses it rather than picking one. `serde` already refuses a repeat
    /// in the envelope's own members; the case that needed a rule here is a repeat inside
    /// `params`, or nested below it, where a `serde_json::Value` is last-wins.
    #[test]
    fn a_repeated_member_anywhere_in_a_frame_is_refused() {
        let repeated = [
            // The envelope itself, which `serde` catches as well.
            r#"{"v":"selvage/2","id":1,"method":"session.rename","params":{"display_name":"a"},"v":"selvage/2"}"#,
            // A member of `params`: the shape a duplicate could silently win before.
            r#"{"v":"selvage/2","id":1,"method":"session.rename","params":{"display_name":"a","display_name":"b"}}"#,
            // A member of an object nested below `params`.
            r#"{"v":"selvage/2","id":1,"method":"session.rename","params":{"display_name":"a","x":{"y":1,"y":2}}}"#,
            // And inside an array of objects.
            r#"{"v":"selvage/2","id":1,"method":"session.rename","params":{"display_name":"a","extra":[{"z":1,"z":2}]}}"#,
        ];
        for text in repeated {
            let error = ClientMessage::from_text(text)
                .expect_err("a repeated member is refused");
            assert!(
                error.to_string().contains("duplicate"),
                "the refusal names the repetition: {error}"
            );
        }

        // The same rule reads a server frame in the other direction.
        let error = ServerMessage::from_text(
            r#"{"v":"selvage/2","id":1,"result":{},"result":{}}"#,
        )
        .expect_err("a repeated member is refused");
        assert!(error.to_string().contains("duplicate"), "{error}");

        // And an ordinary frame is not refused: the same member twice in *different*
        // objects is not a repetition, and numbers, strings and arrays carry none.
        let msg = ClientMessage::from_text(
            r#"{"v":"selvage/2","id":1,"method":"session.rename","params":{"display_name":"a","same":1,"other":{"same":2},"list":[1,"x",null,true,1.5]}}"#,
        )
        .expect("a frame with no repetition parses");
        assert_eq!(msg.id, Some(1));
        assert_eq!(msg.params["display_name"], "a");
    }

    /// §5.1: a join query names `room` and `token` once. Which of two values a connection
    /// joined must not depend on the order they were written in, so a repetition is refused
    /// outright. An unknown parameter may repeat: it is ignored, and nothing reads it.
    #[test]
    fn a_repeated_join_parameter_is_refused() {
        assert_eq!(
            parse_join_query("room=a&room=b"),
            Err(JoinQueryError::Duplicate("room"))
        );
        assert_eq!(
            parse_join_query("token=a&token=b"),
            Err(JoinQueryError::Duplicate("token"))
        );
        assert_eq!(
            parse_join_query("room=a&token=b&token=c"),
            Err(JoinQueryError::Duplicate("token"))
        );
        assert_eq!(
            parse_join_query("room=a&room=a"),
            Err(JoinQueryError::Duplicate("room")),
            "even two equal values are a repetition"
        );
        let repeated_unknown =
            parse_join_query("x=1&x=2&room=r").expect("a query");
        assert_eq!(repeated_unknown.room.as_deref(), Some("r"));
        assert_eq!(
            JoinQueryError::Duplicate("room").to_string(),
            "the join query names room twice"
        );

        // A URL that names a room twice is not a connection URL at all: a client that
        // guessed would join a room the link did not name.
        assert_eq!(
            parse_session_url("ws://h/session?room=a&room=b&token=t"),
            None
        );
    }

    #[test]
    fn join_query_round_trip() {
        let url =
            session_url("ws://127.0.0.1:8080", Some("room 1"), Some("t/k"));
        assert_eq!(
            url,
            "ws://127.0.0.1:8080/session?room=room%201&token=t%2Fk"
        );
        let q =
            parse_join_query(url.split_once('?').unwrap().1).expect("a query");
        assert_eq!(q.room.as_deref(), Some("room 1"));
        assert_eq!(q.token.as_deref(), Some("t/k"));

        let host = session_url("ws://127.0.0.1:8080/", None, None);
        assert_eq!(host, "ws://127.0.0.1:8080/session");
        assert_eq!(
            parse_join_query("").expect("a query"),
            JoinQuery::default()
        );
        assert_eq!(
            parse_join_query("extra=1&room=r").expect("a query"),
            JoinQuery {
                room: Some("r".into()),
                token: None
            }
        );
    }

    #[test]
    fn a_session_url_is_taken_apart_into_its_base() {
        let url = "ws://127.0.0.1:8080/session?room=room%201&token=t%2Fk";
        let parsed = parse_session_url(url).unwrap();
        assert_eq!(parsed.base, "ws://127.0.0.1:8080");
        assert_eq!(parsed.join.room.as_deref(), Some("room 1"));
        assert_eq!(parsed.join.token.as_deref(), Some("t/k"));

        // The base a URL was built from round-trips, wherever the endpoint sits.
        for base in ["ws://127.0.0.1:8080", "wss://example.test/prefix"] {
            let url = session_url(base, Some("r"), Some("t"));
            assert_eq!(parse_session_url(&url).unwrap().base, base);
        }

        // A host connection carries no room, and anything off the endpoint is not a
        // session URL at all.
        assert_eq!(
            parse_session_url("ws://h/session").unwrap().join,
            JoinQuery::default()
        );
        assert_eq!(parse_session_url("ws://h/meta"), None);
    }

    /// The one value the member has, and every near miss: `CANONICAL.md` §2.5 makes
    /// `selvage/2` the protocol's only version, so a spelling that is not exactly it is a
    /// version this receiver does not read. The minor is the case that matters — it is
    /// what a same-major comparison would have let through.
    #[test]
    fn the_one_wire_version_is_the_one_that_is_spoken() {
        assert!(speaks("selvage/2"));
        for version in [
            "selvage/2.0",
            "selvage/2.9",
            "selvage/2.0.0",
            "selvage/1",
            "selvage/3",
            "selvage/03",
            "selvage",
            "selvage/",
            "selvage/x",
            "other/2",
            "selvage/2 ",
            " selvage/2",
            "SELVAGE/2",
            "selvage/2\n",
        ] {
            assert!(
                !speaks(version),
                "{version} is not this protocol's version"
            );
        }
    }

    /// The strings a version *could* be spelled as, and each one is refused for the same
    /// reason: the member has one value. This is the list a grammar would have admitted,
    /// which is why it is here — every entry is a near miss a parser would have seated.
    #[test]
    fn a_version_that_is_not_the_exact_value_is_refused() {
        for version in [
            "selvage/2.2.3",
            "selvage/01",
            "selvage/2.09",
            "selvage/00",
            "selvage/2.",
            "selvage/.1",
            "selvage/2..3",
            "selvage/+2",
            "selvage/-2",
            "selvage/ 2",
            "selvage/2.x",
            "selvage/99999999999999999999999",
        ] {
            assert!(!speaks(version), "{version} must be refused");
        }
    }
}
