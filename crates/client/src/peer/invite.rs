//! `PROTOCOL.md` §5.1's invite: the link, its two keys and the connection address
//! it names. Everything here reads a link and refuses a bad one locally, before a socket
//! is opened.

use std::fmt;

use crate::sealed::PublicKey;
use crate::sealed::RoomKey;
use selvage_protocol as proto;

/// An invite as `PROTOCOL.md` §5.1 writes one and §13.1's first step reads it.
///
/// The fragment is the only part of a link a user agent never sends, which is what makes it
/// the one place a key can travel from a host to a guest; this reads the room and the token
/// from the query and the two keys from the fragment, and the socket URL it hands back has
/// the fragment stripped. Both keys are required, and their absence is a local refusal before
/// a socket is opened: without both, a client can neither read a frame nor verify one, so
/// there is no fallback and no plaintext mode.
#[derive(Clone, PartialEq, Eq)]
pub struct PeerInvite {
    /// The connection URL, without the fragment. Nothing here puts the fragment back.
    pub socket_url: String,
    pub room: String,
    pub token: String,
    pub room_key: RoomKey,
    pub host_key: PublicKey,
}

/// §5.1's two keys are not printed. A derived `Debug` would put the room key and the host key
/// in whatever log this invite is written to,
/// which is the one thing the fragment exists to prevent. Their ids are in every frame's clear
/// prefix already (`CANONICAL.md` §6.1), so those stand in for them.
impl fmt::Debug for PeerInvite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerInvite")
            .field("socket_url", &self.socket_url)
            .field("room", &self.room)
            .field("token", &self.token)
            .field("room_key", &"<redacted>")
            .field("host_key", &self.host_key.id().hex())
            .finish()
    }
}

impl PeerInvite {
    /// Reads an invite link, or says which value is missing or malformed.
    ///
    /// Both of `PROTOCOL.md` §5.1's forms are links this joins: the connection URL and the page
    /// link, which names the same room, token and fragment over the scheme a browser speaks and
    /// is resolved to its connection address first ([`wire_address`]).
    ///
    /// §5.1's repeat rule is a local refusal in either form. A page link has no server in it, so
    /// a `room` or `token` written twice is refused here, and the refusal names the parameter —
    /// the shape [`fragment_keys`] already takes for `k` and `h`.
    ///
    /// # Errors
    ///
    /// Returns the client's own words for a link it cannot join with, which `PROTOCOL.md`
    /// §5.1 fixes as a local refusal: it is not a `session.error`, it is paired with no close
    /// code, and §11's vocabulary is not involved.
    pub fn parse(invite: &str) -> Result<Self, String> {
        let (address, fragment) = invite
            .split_once('#')
            .ok_or_else(|| MISSING_FRAGMENT.to_string())?;
        let socket_url = wire_address(address);
        let parsed = proto::parse_session_url(&socket_url)
            .ok_or_else(|| unreadable_address(address, &socket_url))?;
        let room = parsed
            .join
            .room
            .ok_or_else(|| "the invite names no room".to_string())?;
        let token = parsed
            .join
            .token
            .ok_or_else(|| "the invite carries no token".to_string())?;
        let (room_key, host_key) = fragment_keys(fragment)?;
        Ok(Self {
            socket_url,
            room,
            token,
            room_key,
            host_key,
        })
    }
}

/// The connection address a `PROTOCOL.md` §5.1 invite names, with any fragment left off.
///
/// The page form names the same room, token and fragment over the scheme a browser speaks
/// (`https://h/page/?room=…&token=…`), and the connection address is derived from it by
/// reading the scheme back — `http://` as `ws://`, `https://` as `wss://` — and appending the
/// session endpoint. A link that is already a connection URL, or one this cannot read as
/// either form, is handed back as its address: what refuses it is [`PeerInvite::parse`], which
/// knows which part is missing.
///
/// §5.1's two keys live in the fragment and a user agent never puts one in a request, so
/// nothing here keeps it — the returned address is one a socket may be dialled on, and a caller
/// that put the fragment back would put the room key in that request. A reader that needs the
/// keys splits the fragment off the link itself, which is what [`PeerInvite::parse`] does; this
/// function is the address half of that reading and nothing more.
#[must_use]
pub fn wire_address(link: &str) -> String {
    let address = link
        .split_once('#')
        .map_or(link, |(address, _fragment)| address);
    page_to_endpoint(address).unwrap_or_else(|| address.to_string())
}

/// Why a connection address is not one a `PROTOCOL.md` §5.1 invite reads from: the join
/// parameter it repeats, by name, or the plain fact that it addresses no session endpoint.
///
/// §5.1 has `room` and `token` appear at most once, in either form, and a receiver **MUST NOT**
/// take a value by the order it appears. A page link has no server to refuse a repeat, so the
/// receiver refuses it, and the reason names the parameter — the shape [`fragment_keys`] already
/// takes for `k` and `h`.
fn unreadable_address(address: &str, socket_url: &str) -> String {
    repeated_join_name(socket_url).map_or_else(
        || format!("{address:?} does not address the session endpoint"),
        |name| format!("the invite names `{name}` twice"),
    )
}

/// The join parameter a connection address repeats, if it repeats one.
///
/// §5.1's query rule, read through the parser the server uses: [`proto::parse_session_url`]
/// folds a repeat into `None`, and this asks [`proto::parse_join_query`] which name it refused,
/// so that a page link's refusal can say it instead of blaming the address. A genuine duplicate
/// in the `wss://` form is refused by the server, and this is only what the client says about
/// the link it was handed.
fn repeated_join_name(address: &str) -> Option<&'static str> {
    let query = address.split_once('?').map_or("", |(_, query)| query);
    match proto::parse_join_query(query) {
        Err(proto::JoinQueryError::Duplicate(name)) => Some(name),
        Ok(_) => None,
    }
}

/// The connection URL a page link names, or `None` when the address is not a page link.
fn page_to_endpoint(address: &str) -> Option<String> {
    let scheme = match address
        .strip_prefix("http://")
        .or_else(|| address.strip_prefix("https://"))
    {
        Some(rest) if address.starts_with("http://") => format!("ws://{rest}"),
        Some(rest) => format!("wss://{rest}"),
        None => return None,
    };
    let (before_query, query) = match scheme.split_once('?') {
        Some((before, query)) => (before, Some(query)),
        None => (scheme.as_str(), None),
    };
    let (authority, path) = match before_query.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (before_query, String::new()),
    };
    if authority.is_empty() {
        return None;
    }
    let base = format!("{authority}{}", path.trim_end_matches('/'));
    Some(query.map_or_else(
        || format!("{base}{}", proto::ENDPOINT_PATH),
        |rest| format!("{base}{}?{rest}", proto::ENDPOINT_PATH),
    ))
}

/// What a link without a fragment is told, which is `PROTOCOL.md` §5.1's own sentence: the
/// honest refusal asks for the whole link.
const MISSING_FRAGMENT: &str = "the invite carries no fragment, so neither its room key nor its host key is here: ask for the whole link, `#` and all";

/// The two keys a fragment carries: `k` and `h`, each at most once, and an unknown parameter
/// ignored as an unknown query parameter is.
fn fragment_keys(fragment: &str) -> Result<(RoomKey, PublicKey), String> {
    let mut room_key = None;
    let mut host_key = None;
    for pair in fragment.split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let decoded = proto::percent_decode(value);
        match name {
            "k" => set_once(&mut room_key, RoomKey::parse(&decoded), "k")?,
            "h" => set_once(&mut host_key, PublicKey::parse(&decoded), "h")?,
            _ => {}
        }
    }
    match (room_key, host_key) {
        (Some(k), Some(h)) => Ok((k, h)),
        (None, _) => Err("the invite carries no room key (`k`)".to_string()),
        (_, None) => Err("the invite carries no host key (`h`)".to_string()),
    }
}

/// One fragment value, refused when it is the second of its name or spells no key.
fn set_once<T>(
    slot: &mut Option<T>,
    value: Option<T>,
    name: &str,
) -> Result<(), String> {
    if slot.is_some() {
        return Err(format!("the invite names `{name}` twice"));
    }
    let key = value.ok_or_else(|| {
        format!("`{name}` is not a 32-byte key in the fragment's encoding")
    })?;
    *slot = Some(key);
    Ok(())
}
