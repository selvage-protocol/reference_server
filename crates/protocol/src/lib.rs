//! Wire types for the Selvage Session Protocol, wire version `selvage/1`.
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

use std::fmt::Write as _;
use std::str;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Wire version carried in every session envelope.
pub const WIRE_VERSION: &str = "selvage/1";

/// WebSocket endpoint path.
pub const ENDPOINT_PATH: &str = "/session";

/// Negotiation endpoint path, served over plain HTTP on the same listener.
pub const META_PATH: &str = "/meta";

/// Capabilities this implementation advertises.
pub const CAPABILITIES: &[&str] = &[
    "y-protocols/1",
    "awareness",
    "open-document-set",
    "host-reclaim",
];

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

/// Client -> server method names.
pub mod method {
    pub const SESSION_HELLO: &str = "session.hello";
    pub const SESSION_RENAME: &str = "session.rename";
    pub const DOC_OPEN: &str = "doc.open";
    pub const DOC_CLOSE: &str = "doc.close";
    pub const DOC_GRANT: &str = "doc.grant";
}

/// Server -> client event names.
pub mod event {
    pub const ROOM_CREATED: &str = "room.created";
    pub const ROOM_JOINED: &str = "room.joined";
    pub const PEER_JOINED: &str = "peer.joined";
    pub const PEER_LEFT: &str = "peer.left";
    pub const PEER_RENAMED: &str = "peer.renamed";
    pub const DOC_OPENED: &str = "doc.opened";
    pub const DOC_CLOSED: &str = "doc.closed";
    pub const DOC_GRANTED: &str = "doc.granted";
    pub const HOST_DETACHED: &str = "host.detached";
    pub const HOST_ATTACHED: &str = "host.attached";
    pub const ROOM_GONE: &str = "room.gone";
    pub const SESSION_ERROR: &str = "session.error";
}

/// Machine-readable error codes for `error.code`.
pub mod code {
    pub const UNKNOWN_METHOD: &str = "unknown_method";
    pub const BAD_MESSAGE: &str = "bad_message";
    pub const BAD_PARAMS: &str = "bad_params";
    pub const UNSUPPORTED_VERSION: &str = "unsupported_version";
    pub const HELLO_REQUIRED: &str = "hello_required";
    pub const ROOM_UNKNOWN: &str = "room_unknown";
    pub const ROOM_GONE: &str = "room_gone";
    pub const TOKEN_INVALID: &str = "token_invalid";
    pub const HOST_PRESENT: &str = "host_present";
    /// Reserved and never produced by this slice (`PROTOCOL.md` §11,
    /// `schema/errors.json`): named here so receivers and senders spell it the same
    /// way, not because any frame carries it. Closing a path nobody holds succeeds.
    pub const DOC_NOT_OPEN: &str = "doc_not_open";
    pub const ALREADY_SEATED: &str = "already_seated";
}

/// WebSocket close codes in the private use range, matching [`code`].
pub mod close {
    pub const PROTOCOL_ERROR: u16 = 4000;
    pub const ROOM_UNKNOWN: u16 = 4001;
    pub const TOKEN_INVALID: u16 = 4002;
    pub const ROOM_GONE: u16 = 4003;
    pub const HOST_PRESENT: u16 = 4004;
    pub const UNSUPPORTED_VERSION: u16 = 4005;
}

/// Maps a fatal session error code to the close code used to end the connection.
#[must_use]
pub fn close_code_for(code: &str) -> u16 {
    match code {
        code::ROOM_UNKNOWN => close::ROOM_UNKNOWN,
        code::TOKEN_INVALID => close::TOKEN_INVALID,
        code::ROOM_GONE => close::ROOM_GONE,
        code::HOST_PRESENT => close::HOST_PRESENT,
        code::UNSUPPORTED_VERSION => close::UNSUPPORTED_VERSION,
        _ => close::PROTOCOL_ERROR,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Host,
    Guest,
}

impl Role {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Guest => "guest",
        }
    }
}

/// A participant as seen by the session layer. Identity is the display name only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Which y-protocols awareness client id this peer speaks with. Lets an editor
    /// adapter attribute a remote cursor without putting identity into awareness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub awareness_client_id: Option<u64>,
    pub display_name: String,
    pub peer_id: String,
    pub role: Role,
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
}

/// `session.hello` params — the client's half of the handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub awareness_client_id: Option<u64>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    pub display_name: String,
    /// Defaults to host when the connection carried no room, guest otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,
}

/// Result of `session.hello`: the server's half of the handshake. Sent as
/// `room.created` (host, includes the token) or `room.joined` (guest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionParams {
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub documents: Vec<String>,
    #[serde(default)]
    pub keepalive: Keepalive,
    #[serde(default)]
    pub peers: Vec<PeerInfo>,
    pub room_id: String,
    #[serde(rename = "self")]
    pub self_peer: PeerInfo,
    /// Present only for the host that minted the room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// `doc.open` params.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocOpenParams {
    pub path: String,
}

/// `doc.close` params.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocCloseParams {
    pub path: String,
}

/// `doc.grant` params: the host's whole listing of the working tree, replacing the room's
/// grant wholesale. The order is part of what the frame says (`CANONICAL.md` §2.7) and a
/// server carries it unchanged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantParams {
    pub paths: Vec<String>,
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

/// `doc.opened` / `doc.closed` params. `documents` is the room's open-document set
/// after the change, so every peer holds the same view of it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocEvent {
    #[serde(default)]
    pub documents: Vec<String>,
    pub path: String,
    pub peer_id: String,
}

/// `doc.granted` params: the room's grant as it now stands, in the order its host wrote it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantedParams {
    pub paths: Vec<String>,
}

/// The result of `doc.open` / `doc.close`: the room's open-document set after the
/// change, which is what makes a request's effect visible to its own caller.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DocSet {
    #[serde(default)]
    pub documents: Vec<String>,
}

/// `room.gone` params.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomGoneParams {
    pub reason: String,
    pub room_id: String,
}

/// `GET /meta` response body. Members are in the canonical order of
/// `CANONICAL.md` §2.1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub keepalive: Keepalive,
    #[serde(default)]
    pub roles: Vec<String>,
    pub server: String,
    pub wire_versions: Vec<String>,
}

impl Meta {
    #[must_use]
    pub fn reference() -> Self {
        Self {
            capabilities: CAPABILITIES
                .iter()
                .map(ToString::to_string)
                .collect(),
            keepalive: Keepalive::default(),
            roles: vec!["host".to_string(), "guest".to_string()],
            server: concat!("selvaged/", env!("CARGO_PKG_VERSION")).to_string(),
            wire_versions: vec![WIRE_VERSION.to_string()],
        }
    }
}

/// The room/token part of a join URL. `room` absent means "mint a new room".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JoinQuery {
    pub room: Option<String>,
    pub token: Option<String>,
}

/// Parses a URL query string into a room/token pair. Unknown parameters are ignored.
#[must_use]
pub fn parse_join_query(query: &str) -> JoinQuery {
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
            "room" => out.room = Some(percent_decode(v)),
            "token" => out.token = Some(percent_decode(v)),
            _ => {}
        }
    }
    out
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
    let base = endpoint.strip_suffix(ENDPOINT_PATH)?;
    Some(SessionUrl {
        base: base.to_string(),
        join: parse_join_query(query),
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
        out.push(if *first == b'+' { b' ' } else { *first });
        rest = tail;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// True when the wire version in an envelope is compatible with this implementation.
/// Compatible means same major; while at 0.x, same minor.
///
/// A version outside the grammar of `CANONICAL.md` §2.5 is not compatible either: the
/// schema's `wireVersion` refuses it, so a receiver that seats one is a receiver a
/// conforming peer cannot predict.
#[must_use]
pub fn is_compatible(version: &str) -> bool {
    match (
        parse_wire_version(version),
        parse_wire_version(WIRE_VERSION),
    ) {
        (Some(a), Some(b)) => {
            if a.0 != b.0 {
                return false;
            }
            b.0 != 0 || a.1 == b.1
        }
        _ => false,
    }
}

/// `selvage/` major, optionally `.` minor, both plain decimal with no leading zero.
///
/// A second `.` leaves the minor unparsable, so it is refused with everything else the
/// grammar does not admit.
fn parse_wire_version(version: &str) -> Option<(u64, u64)> {
    let rest = version.strip_prefix("selvage/")?;
    let parts = rest.split_once('.');
    let major = parse_version_number(parts.map_or(rest, |(head, _)| head))?;
    let minor = match parts {
        Some((_, tail)) => parse_version_number(tail)?,
        None => 0,
    };
    Some((major, minor))
}

/// A number as `CANONICAL.md` §2.4 writes one: ASCII digits, and no leading zero.
///
/// `u64::from_str` is laxer than the grammar in two ways that matter on the wire — it
/// accepts `+1` and it accepts `01` — so the shape is checked before the parse.
fn parse_version_number(text: &str) -> Option<u64> {
    let decimal =
        !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    if !decimal || (text.len() > 1 && text.starts_with('0')) {
        return None;
    }
    text.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_shapes() {
        let req = ClientMessage::new(
            7,
            method::DOC_OPEN,
            serde_json::json!({"path": "a.rs"}),
        );
        let text = req.to_text().unwrap();
        assert_eq!(
            text,
            r#"{"id":7,"method":"doc.open","params":{"path":"a.rs"},"v":"selvage/1"}"#
        );

        let ok = ServerMessage::response(7, serde_json::json!({}))
            .to_text()
            .unwrap();
        assert_eq!(ok, r#"{"id":7,"result":{},"v":"selvage/1"}"#);

        let err =
            ServerMessage::error(7, code::UNKNOWN_METHOD, "no such method")
                .to_text()
                .unwrap();
        assert_eq!(
            err,
            r#"{"error":{"code":"unknown_method","message":"no such method"},"id":7,"v":"selvage/1"}"#
        );

        let ev =
            ServerMessage::event(event::PEER_LEFT, serde_json::json!({"x": 1}))
                .to_text()
                .unwrap();
        assert_eq!(
            ev,
            r#"{"event":"peer.left","params":{"x":1},"v":"selvage/1"}"#
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
            r#"{"id":2,"method":"session.rename","params":{"display_name":"Ada Lovelace"},"v":"selvage/1"}"#
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
            r#"{"event":"peer.renamed","params":{"display_name":"Ada Lovelace","peer_id":"p-1"},"v":"selvage/1"}"#
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
    fn grant_frames() {
        // The listing is an ordered array and the order survives both directions: this crate
        // writes what it was given and reads it back unchanged.
        let paths = vec!["README.md".to_string(), "ｆ.txt".to_string()];
        let request = ClientMessage::new(
            3,
            method::DOC_GRANT,
            serde_json::json!(GrantParams {
                paths: paths.clone()
            }),
        );
        assert_eq!(
            request.to_text().unwrap(),
            r#"{"id":3,"method":"doc.grant","params":{"paths":["README.md","ｆ.txt"]},"v":"selvage/1"}"#
        );

        let event = ServerMessage::event(
            event::DOC_GRANTED,
            serde_json::json!(GrantedParams { paths }),
        );
        assert_eq!(
            event.to_text().unwrap(),
            r#"{"event":"doc.granted","params":{"paths":["README.md","ｆ.txt"]},"v":"selvage/1"}"#
        );

        let parsed: GrantParams =
            serde_json::from_value(request.params).unwrap();
        assert_eq!(parsed.paths, vec!["README.md", "ｆ.txt"]);
        let params: GrantedParams =
            serde_json::from_value(event.params.unwrap_or_default()).unwrap();
        assert_eq!(params.paths, vec!["README.md", "ｆ.txt"]);
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let raw = r#"{"v":"selvage/1","id":1,"method":"session.hello",
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

    #[test]
    fn join_query_round_trip() {
        let url =
            session_url("ws://127.0.0.1:8080", Some("room 1"), Some("t/k"));
        assert_eq!(
            url,
            "ws://127.0.0.1:8080/session?room=room%201&token=t%2Fk"
        );
        let q = parse_join_query(url.split_once('?').unwrap().1);
        assert_eq!(q.room.as_deref(), Some("room 1"));
        assert_eq!(q.token.as_deref(), Some("t/k"));

        let host = session_url("ws://127.0.0.1:8080/", None, None);
        assert_eq!(host, "ws://127.0.0.1:8080/session");
        assert_eq!(parse_join_query(""), JoinQuery::default());
        assert_eq!(
            parse_join_query("extra=1&room=r"),
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

    #[test]
    fn version_compatibility() {
        assert!(is_compatible("selvage/1"));
        // At major 1 the rule is same-major, so the minor is not decisive.
        assert!(is_compatible("selvage/1.9"));
        assert!(is_compatible("selvage/1.0"));
        assert!(!is_compatible("selvage/2"));
        assert!(!is_compatible("selvage"));
        assert!(!is_compatible("selvage/x"));
        assert!(!is_compatible("other/1"));
    }

    /// The grammar of `CANONICAL.md` §2.5, which `schema/negotiation.json` encodes as
    /// `wireVersion`. None of these is a `selvage/<number>[.<number>]` string, so none
    /// is a version a receiver may seat.
    #[test]
    fn versions_outside_the_grammar_are_not_compatible() {
        for version in [
            "selvage/1.2.3",
            "selvage/1.0.0",
            "selvage/01",
            "selvage/1.09",
            "selvage/00",
            "selvage/1.",
            "selvage/.1",
            "selvage/1..2",
            "selvage/",
            "selvage/+1",
            "selvage/-1",
            "selvage/ 1",
            "selvage/1 ",
            "selvage/1.x",
            "selvage/99999999999999999999999",
        ] {
            assert!(!is_compatible(version), "{version} must be refused");
        }
    }
}
