//! `selvage/2`'s peer side: what a client does with the frames it receives.
//!
//! [`crate::sealed`] is `CANONICAL.md` §6.1's bytes — the envelope, the key schedule, the read
//! order and its verdicts. This module is `PROTOCOL.md` §13 on top of it: the session key the
//! connection announces and the order of operations at a join (§13.1), what a client may
//! publish before and after a state commits its key, the marks and the mark's owner (§13.2,
//! §13.3), attribution by the key that verified and the role the state gives it (§13.4,
//! §13.5), the holds and their lease (§13.7), the two presence windows (§13.3, §13.8), and
//! the four ways a session ends (§13.10).
//!
//! **It holds no socket.** A frame goes in and the decisions come out, and every clock is a
//! value the caller passes in: `PROTOCOL.md` §13.8 says a client's timers are its own
//! monotone elapsed time from an event it observed, so the session records the clock value at
//! each event it sees and asks no platform for the time. That is what lets the corpus's
//! decision layer drive it — `specification/runner/run_peer.py --subject` — and what lets its
//! own tests be about the rule rather than about how long a machine took.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem;
use std::time::Duration;

use serde_json::{Value, json};
use yrs::block::ClientID;
use yrs::sync::protocol::{DefaultProtocol, Protocol as YProtocol};
use yrs::sync::{Awareness, Message as YMessage, SyncMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, OffsetKind, Options, ReadTxn, Text, Transact};

use selvage_protocol as proto;

use crate::host::{HostOptions, HostProducer, HostReason};
use crate::sealed::{
    Announcement, FrameKey, Guard, KeyId, Payload, PublicKey, Reader, Recipe,
    RoomKey, SealedError, SessionKey, Verdict, fresh_nonce, seal,
};

/// An invite as `PROTOCOL.md` §5.1 writes one and §13.1's first step reads it.
///
/// The fragment is the only part of a link a user agent never sends, which is what makes it
/// the one place a key can travel from a host to a guest; this reads the room and the token
/// from the query and the two keys from the fragment, and the socket URL it hands back has
/// the fragment stripped. Both keys are required, and their absence is a local refusal before
/// a socket is opened: without both, a client can neither read a frame nor verify one, so
/// there is no fallback and no plaintext mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInvite {
    /// The connection URL, without the fragment. Nothing here puts the fragment back.
    pub socket_url: String,
    pub room: String,
    pub token: String,
    pub room_key: RoomKey,
    pub host_key: PublicKey,
}

impl PeerInvite {
    /// Reads an invite link, or says which value is missing or malformed.
    ///
    /// Both of `PROTOCOL.md` §5.1's forms are links this joins: the connection URL and the page
    /// link, which names the same room, token and fragment over the scheme a browser speaks and
    /// is resolved to its connection URL first ([`wire_invite`]).
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
        let parsed = proto::parse_session_url(&wire_invite(address))
            .ok_or_else(|| {
                format!("{address:?} does not address the session endpoint")
            })?;
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
            socket_url: wire_invite(address),
            room,
            token,
            room_key,
            host_key,
        })
    }
}

/// A link read back as the wire form of `PROTOCOL.md` §5.1, with any fragment left off.
///
/// The page form names the same room, token and fragment over the scheme a browser speaks
/// (`https://host/page/?room=…&token=…`), and the connection URL is derived from it by reading
/// the scheme back — `http://` as `ws://`, `https://` as `wss://` — and appending the session
/// endpoint. A link that is already a connection URL, or one this cannot read as either form,
/// is handed back unchanged: what refuses it is [`PeerInvite::parse`], which knows which part
/// is missing.
#[must_use]
pub fn wire_invite(link: &str) -> String {
    let (address, fragment) = match link.split_once('#') {
        Some((address, fragment)) => (address, format!("#{fragment}")),
        None => (link, String::new()),
    };
    let Some(mapped) = page_to_endpoint(address) else {
        return link.to_string();
    };
    format!("{mapped}{fragment}")
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

/// What a session built to be a host is told when the handshake gave it no seat: §7.1's state
/// labels its own connection's seat, and there is nothing to write without one.
const HOST_NEEDS_SEAT: &str =
    "a host session publishes its own seat, and the handshake gave it none";

/// What a host is told when §7.1's closing could not be sealed: a host cannot refuse to close
/// without somewhere to say so, and a driver that never looks is a driver closing nothing.
const CLOSING_UNSEALED: &str = "the closing could not be sealed";

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

/// What a session is opened with: the invite's two keys, the session's clock, and the seats
/// the server has shown.
#[derive(Debug, Clone)]
pub struct PeerOptions {
    pub room_id: String,
    pub room_key: RoomKey,
    /// The host key the invite's `h` carries: the room's root of trust, and the only key that
    /// verifies a `kind = 1` state or a `kind = 2` closing.
    pub host_key: PublicKey,
    /// `awareness_renew_ms` (`PROTOCOL.md` §8.2): the clock an uncommitted announcement is
    /// re-sent on and a held set is renewed on, and the tick a lease is checked on (§13.7).
    pub renew: Duration,
    /// `awareness_expire_ms`: the host-away window (§13.8) and, read from the seat, the
    /// no-state window (§13.3).
    pub expire: Duration,
    /// The seat this connection is shown under, from `room.created`/`room.joined`. It decides
    /// no attribution (§13.4) and is carried only because a person is shown it.
    pub seat: Option<String>,
    /// The seats the roster has, which is what §13.8 reads a state's `host` entry against.
    pub roster: BTreeSet<String>,
    /// A fixed session keypair, as its 32-byte Ed25519 seed.
    ///
    /// **A test seam, and not production surface.** §13.1 mints the session keypair in memory
    /// for the connection and never persists it, and no frame carries a private key to a
    /// client, so nothing in the protocol lets a caller choose one. It exists because the
    /// corpus's decision layer drives a *fixture* keypair: the state a decision vector
    /// delivers commits that fixture key's public half, so a client that minted its own key
    /// could not be the peer the vector is about. `None` mints one, which is what a session
    /// does.
    pub fixed_session_key: Option<[u8; 32]>,
    /// The role this client believes it has been given — `guest` or `viewer`, never `host` —
    /// declared in its announcement. A host **SHOULD** honour it (§7.1).
    pub declared_role: Option<String>,
    /// The awareness client id the handshake announced, so that the states this connection
    /// publishes and the seat the room attributes them to agree (§8.4). `None` mints one from
    /// the replica, which is what a session with no handshake does.
    pub awareness_client_id: Option<u64>,
    /// The host's producer half (§7.1), which makes this session the room's authority rather
    /// than one of its peers. The host key's private half lives here, so a session given one
    /// can sign a state and a session without one cannot; the room's host publishes the state
    /// that every peer's key and role come from, and a session with no state produces none.
    pub host: Option<HostOptions>,
}

/// What a session did with one frame: the decision `PROTOCOL.md` §13.11 observes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// It verified and what it carried was applied.
    Applied { kind: u64 },
    /// It was refused, at the first of `CANONICAL.md` §6.1's ten steps that refused it.
    Dropped { reason: String },
    /// It verified and applied nothing: §13.10's closing handed to a receiver that holds no
    /// verified state. Not a refusal — nothing was refused — and in neither list a vector
    /// asserts, which is what vector 155 pins.
    Ignored { kind: u64 },
}

/// What a session is about to publish: the kind of envelope it is, and whether `PROTOCOL.md`
/// §13.11's report counts it as a **publication** or as a §7 sync-handshake frame.
///
/// The two counts are apart because a vector asserts one of them: §13.1's step 6 obliges a
/// client that applies a state committing its own key to send a `SyncStep1`, so a single
/// number could not tell a republished request from a publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    /// The session-key announcement, `kind = 4`.
    Announcement,
    /// A holds message, `kind = 3`.
    Holds,
    /// Document content, `kind = 0`.
    Content,
    /// A `SyncStep1` or a `SyncStep2`, `kind = 0`.
    Sync,
}

impl Published {
    const fn kind(self) -> u64 {
        match self {
            Self::Holds => 3,
            Self::Announcement => 4,
            Self::Content | Self::Sync => 0,
        }
    }

    const fn counted(self) -> Counted {
        match self {
            Self::Content | Self::Holds | Self::Announcement => {
                Counted::Publication
            }
            Self::Sync => Counted::Handshake,
        }
    }
}

/// Which of the two counts a published frame moves (§13.11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Counted {
    Publication,
    Handshake,
}

/// One frame a session applied, with the index of the frame it was handed as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    pub frame: u64,
    pub kind: u64,
}

/// One frame a session refused, and the reason `CANONICAL.md` §6.1's vocabulary gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dropped {
    pub frame: u64,
    pub reason: String,
}

/// The three endings a session reaches on its own. What §13.10 calls "the room is destroyed"
/// is the fourth and has no frame: a client learns it as `room_unknown` or close 4001 when it
/// names the id, which is the socket's business and not this module's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ending {
    /// A `kind = 2` closing that verified and was above the mark, with a verified state
    /// below it (§13.10).
    Closing,
    /// The host-away clock passed its window (§13.8).
    HostAway,
    /// The no-state window passed with nothing applied (§13.3).
    NoState,
}

/// One connection's `selvage/2` session.
pub struct PeerSession {
    room_id: String,
    frame_key: FrameKey,
    session: SessionKey,
    declared_role: Option<String>,
    /// The host's producer half, when this connection holds the host key (§7.1).
    host: Option<HostProducer>,
    /// The bytes of the last room state this connection **applied** (§7.1).
    ///
    /// §7.1 has a peer that holds a verified state re-send it, unchanged, when it sees a
    /// `peer.joined`, so a joiner's state arrives while the host is away. The bytes are the
    /// ones it received: only the host key signs a state, and a peer that re-sealed or
    /// re-signed one would hand the room a frame every other peer refuses.
    held_state_frame: Option<Vec<u8>>,
    counter: u64,
    renew: Duration,
    expire: Duration,
    seat: Option<String>,
    roster: BTreeSet<String>,
    reader: Reader,
    awareness: Awareness,
    outbound: VecDeque<Vec<u8>>,
    /// The paths this client has open: the whole set every holds message carries (§13.7).
    held: BTreeSet<String>,
    /// The set the last holds message carried, so that a changed set is published at once and
    /// an unchanged one waits for the renewal clock.
    holds_sent: Vec<String>,
    holds_announced_at: Option<Duration>,
    /// When the last holds message from each key was accepted, which is its lease.
    leases: BTreeMap<KeyId, Duration>,
    /// The seats that have left, from `peer.left`: what §13.7 drops holds on and what tells
    /// a seat the roster never knew from one that departed.
    departed: BTreeSet<String>,
    announced_at: Option<Duration>,
    /// The local edits this client made while §13.1's step 4 held its content back, in the
    /// order it made them: the deltas themselves, so that flushing them cannot carry a peer's
    /// changes out under this connection's key.
    unsent: Vec<Vec<u8>>,
    /// The clock of the first content frame this client refused since its last `SyncStep1`,
    /// which is §13.6's interval. `None` means there is nothing to re-sync about.
    resync_from: Option<Duration>,
    /// The clock of the last `SyncStep1` this client sent, which is what the re-sync is
    /// bounded by: one is sent per renewal interval however many frames were refused.
    handshaken_at: Option<Duration>,
    /// The edition of the state this client holds, or `None` before one has been applied
    /// (§13.3's waiting rules and §13.10's closing both turn on it).
    state_issued: Option<u64>,
    host_away_since: Option<Duration>,
    published: u64,
    handshake: u64,
    frames: u64,
    applied: Vec<Applied>,
    dropped: Vec<Dropped>,
    ignored: Vec<u64>,
    ending: Option<Ending>,
    mutation: Option<String>,
    /// The first thing that went wrong on the way *out* — a CSPRNG that would not read, an
    /// AEAD that refused its own inputs. A session cannot refuse to publish without somewhere
    /// to say so, and a driver that never looks is a driver publishing nothing.
    fault: Option<String>,
}

impl PeerSession {
    /// A connection's session: §13.1's steps 1 and 2, and nothing sent yet.
    ///
    /// The clock this session reads starts at zero and every argument named `clock` is that
    /// many units past the seat, so a caller passes `start.elapsed()` and a test passes a
    /// number. Nothing here reads a platform clock.
    ///
    /// # Errors
    ///
    /// Returns an error when the platform's CSPRNG cannot be read, which is the one thing
    /// minting a session keypair needs, and when a host session's first state cannot be sealed:
    /// §7.1's mint state is what brings the room's listing into existence and commits this
    /// connection's key, so a host that could not publish one is a host no peer can see.
    pub fn new(options: &PeerOptions) -> Result<Self, SealedError> {
        let session = match options.fixed_session_key {
            Some(seed) => SessionKey::from_seed(seed),
            None => SessionKey::generate()?,
        };
        let reader =
            Reader::new(&options.room_id, options.room_key, options.host_key);
        let frame_key = reader.frame_key;
        let own_key = session.public();
        let host = match &options.host {
            None => None,
            Some(host_options) => {
                Some(host_producer(options, frame_key, host_options, own_key)?)
            }
        };
        let mut roster = options.roster.clone();
        if let (Some(seat), true) = (options.seat.as_ref(), host.is_some()) {
            // §9: a host is seated in the room it minted, whether or not the roster it was
            // handed names it. Without this its own state's `host` entry labels a seat the
            // session believes is absent, and §13.8's clock then ends its own session.
            let _ = roster.insert(seat.clone());
        }
        let mut peer = Self {
            room_id: options.room_id.clone(),
            frame_key,
            session,
            declared_role: options.declared_role.clone(),
            host,
            held_state_frame: None,
            counter: 0,
            renew: options.renew,
            expire: options.expire,
            seat: options.seat.clone(),
            roster,
            reader,
            awareness: Awareness::new(peer_doc(options.awareness_client_id)),
            outbound: VecDeque::new(),
            held: BTreeSet::new(),
            holds_sent: Vec::new(),
            holds_announced_at: None,
            leases: BTreeMap::new(),
            departed: BTreeSet::new(),
            announced_at: None,
            unsent: Vec::new(),
            resync_from: None,
            handshaken_at: None,
            state_issued: None,
            host_away_since: None,
            published: 0,
            handshake: 0,
            frames: 0,
            applied: Vec::new(),
            dropped: Vec::new(),
            ignored: Vec::new(),
            ending: None,
            mutation: None,
            fault: None,
        };
        if peer.host.is_some() {
            // §13.1: a host's order is a peer's with one difference — it publishes a state at
            // mint, so its own state may precede any it verifies. That state is also what
            // commits its own connection's key, which is why a host has nothing to announce.
            peer.publish_state(Duration::ZERO, HostReason::Mint);
        }
        let failed = peer.fault.take();
        if let Some(fault) = failed {
            return Err(SealedError::new(fault));
        }
        Ok(peer)
    }

    // --- what a caller reads --------------------------------------------------

    /// This connection's session public key, which the announcement names.
    #[must_use]
    pub fn session_key(&self) -> PublicKey {
        self.session.public()
    }

    /// This connection's own seat, as a caller that needs to show it reads it.
    #[must_use]
    pub fn seat(&self) -> Option<&str> {
        self.seat.as_deref()
    }

    /// Whether a verified room state has been applied, which §13.3's waiting rules turn on.
    #[must_use]
    pub const fn state_held(&self) -> bool {
        self.state_issued.is_some()
    }

    /// Whether this connection holds the host key, which is the whole of what being the host is
    /// in this version (§7.1).
    #[must_use]
    pub const fn is_host(&self) -> bool {
        self.host.is_some()
    }

    /// The awareness client id this connection announced, which is the one its replica speaks
    /// under (§8.4): the id the room records for this seat, so a peer's caret is attributed to
    /// the seat that published it.
    #[must_use]
    pub fn awareness_client_id(&self) -> u64 {
        self.awareness.client_id().get()
    }

    /// The role the applied state gives this connection's own key (§13.4), or `None` while no
    /// state commits it — the state is the only source of a role in this version.
    #[must_use]
    pub fn own_role(&self) -> Option<&str> {
        self.role()
    }

    /// The seat the applied state's `host` entry labels, or `None` while no state names one
    /// (§13.4: a client with none **MUST NOT** guess one).
    #[must_use]
    pub fn named_host_seat(&self) -> Option<&str> {
        self.host_seat()
    }

    /// The roles the applied state assigns, by the seat each committed key is labelled.
    #[must_use]
    pub fn roles_by_seat(&self) -> BTreeMap<String, String> {
        self.reader
            .committed
            .values()
            .map(|peer| (peer.peer_id.clone(), peer.role.clone()))
            .collect()
    }

    /// The edition of the last state this connection published, or `0` before it has published
    /// one. This is §7.1's `issued` series, which a host that keeps hosting continues.
    #[must_use]
    pub fn published_issued(&self) -> u64 {
        self.host.as_ref().map_or(0, HostProducer::published_issued)
    }

    /// The edition of the state this client holds, or `None` before one is applied.
    #[must_use]
    pub const fn state_issued(&self) -> Option<u64> {
        self.state_issued
    }

    /// The listing of the last state applied, with §5's refused paths dropped.
    #[must_use]
    pub fn listing(&self) -> &[String] {
        &self.reader.listing
    }

    /// How many frames this client **published** under §13's rules (§13.11).
    #[must_use]
    pub const fn published(&self) -> u64 {
        self.published
    }

    /// How many §7 handshake frames it sent, counted apart from its publications.
    #[must_use]
    pub const fn handshake(&self) -> u64 {
        self.handshake
    }

    #[must_use]
    pub const fn frames(&self) -> u64 {
        self.frames
    }

    #[must_use]
    pub fn applied(&self) -> &[Applied] {
        &self.applied
    }

    #[must_use]
    pub fn dropped(&self) -> &[Dropped] {
        &self.dropped
    }

    /// The frames that verified and applied nothing, by the index they were handed as.
    #[must_use]
    pub fn ignored(&self) -> &[u64] {
        &self.ignored
    }

    /// Why the session ended, if it has.
    #[must_use]
    pub const fn ending(&self) -> Option<Ending> {
        self.ending
    }

    #[must_use]
    pub fn mutation(&self) -> Option<&str> {
        self.mutation.as_deref()
    }

    /// The first frame this session could not produce, if any. A driver reads it and fails
    /// loudly: publishing fails silently otherwise, and a subject that publishes nothing looks
    /// like a client with nothing to say.
    #[must_use]
    pub fn fault(&self) -> Option<&str> {
        self.fault.as_deref()
    }

    /// The paths this client has open: what its own holds message carries.
    #[must_use]
    pub const fn held(&self) -> &BTreeSet<String> {
        &self.held
    }

    /// What each peer is held to, by the key's canonical spelling where the applied state
    /// names it and by its id where nothing does (§13.4). A key whose lease has lapsed is not
    /// here at all: §13.7 has the receiver forget the whole set.
    #[must_use]
    pub fn peer_holds(&self) -> BTreeMap<String, Vec<String>> {
        self.reader
            .holds
            .iter()
            .map(|(id, paths)| (self.spelling(*id), paths.clone()))
            .collect()
    }

    /// The length of a document's text in UTF-16 code units, the unit yjs, every editor's
    /// `offsetAt` and every peer on the wire count an offset in (`PROTOCOL.md` §8.1).
    #[must_use]
    pub fn length(&self, path: &str) -> u32 {
        let doc = self.awareness.doc();
        let text = doc.get_or_insert_text(path);
        text.len(&doc.transact())
    }

    /// The text this replica holds at a path, which is what a decision vector reads.
    #[must_use]
    pub fn text(&self, path: &str) -> String {
        let doc = self.awareness.doc();
        let text = doc.get_or_insert_text(path);
        text.get_string(&doc.transact())
    }

    /// The CRDT state vector, as `(client id, clock)` pairs: what a sync handshake exchanges,
    /// and what two fully synced replicas agree on.
    #[must_use]
    pub fn state_vector(&self) -> Vec<(u64, u32)> {
        let doc = self.awareness.doc();
        let txn = doc.transact();
        let mut entries: Vec<(u64, u32)> = txn
            .state_vector()
            .iter()
            .map(|(client, clock)| (client.get(), *clock))
            .collect();
        entries.sort_unstable();
        entries
    }

    /// The paths this replica holds any text for: the documents that have arrived.
    #[must_use]
    pub fn documents(&self) -> Vec<String> {
        let doc = self.awareness.doc();
        let txn = doc.transact();
        let mut paths: Vec<String> = txn
            .root_refs()
            .filter(|(name, _)| txn.get_text(*name).is_some())
            .map(|(name, _)| name.to_string())
            .collect();
        paths.sort();
        paths
    }

    // --- what the caller hands in ---------------------------------------------

    /// One sealed frame, as the relay delivered it.
    pub fn deliver(&mut self, clock: Duration, frame: &[u8]) -> Outcome {
        self.frames = self.frames.saturating_add(1);
        let index = self.frames.saturating_sub(1);
        // A closing folds into the receiver the moment it verifies, and §13.10 ignores one
        // handed to a client holding no state. The two values it moved are put back, because
        // `issued` is an order between two states and this receiver has none to compare one
        // with: vector 155 delivers a closing at edition 2 and then a state at 1, and the
        // state must still be the first edition this client holds.
        let issued = self.reader.issued;
        let ended = self.reader.ended;
        let verdict = self.reader.read(frame);
        if !verdict.ok {
            self.note_refusal(clock, &verdict);
            return self.refuse(index, verdict.reason.as_deref());
        }
        let kind = verdict.kind.unwrap_or_default();
        if matches!(verdict.payload, Some(Payload::RoomState(_))) {
            // §7.1: what a peer re-sends when it sees a `peer.joined` is the bytes it received,
            // and those are the ones that verified.
            self.held_state_frame = Some(frame.to_vec());
        }
        if self.ignores(&verdict) {
            self.reader.issued = issued;
            self.reader.ended = ended;
            self.ignored.push(index);
            return Outcome::Ignored { kind };
        }
        self.fold(clock, &verdict);
        self.applied.push(Applied { frame: index, kind });
        Outcome::Applied { kind }
    }

    /// The clocks, on the caller's tick: §13.7's renewal and expiry, §13.8's host-away window
    /// and §13.3's no-state window.
    ///
    /// The announcement of §13.1's step 4 goes out here too, at the first tick and whenever a
    /// state has not committed this key since — so a driver has one path to publish it and
    /// nothing has to remember to call it at the join.
    pub fn tick(&mut self, clock: Duration) {
        self.expire_leases(clock);
        self.refresh_host_away(clock, false);
        if self.ending.is_some() {
            return;
        }
        if self.window_passed(clock) {
            return;
        }
        self.publish_state(clock, HostReason::Announcement);
        self.reannounce(clock);
        self.resync(clock);
        self.announce_holds(clock);
    }

    /// Takes what this session has published, in the order it published it.
    pub fn take_outbound(&mut self) -> Vec<Vec<u8>> {
        self.outbound.drain(..).collect()
    }

    // --- the guard a census removes -------------------------------------------

    /// Removes one of `PROTOCOL.md` §13.11's client guards, for the corpus's mutation census.
    ///
    /// # Errors
    ///
    /// Returns an error naming the guard when nothing here removes it: a subject that cannot
    /// take a mutation must say so rather than pass the census silently.
    pub fn mutate(&mut self, name: &str) -> Result<(), SealedError> {
        match name {
            "ignore-roles" => self.reader.mutations.remove(Guard::Roles),
            "ignore-issued" => self.reader.mutations.remove(Guard::Issued),
            "announce-once" | "no-lease" | "any-closing" | "wait-for-ever" => {}
            other => {
                return Err(SealedError::new(format!(
                    "no mutation is named {other:?}"
                )));
            }
        }
        self.mutation = Some(name.to_string());
        Ok(())
    }

    fn mutating(&self, name: &str) -> bool {
        self.mutation.as_deref() == Some(name)
    }
}

/// The host's producer half for a session handed the host key (§7.1).
///
/// §7.1's own entry labels this connection's seat, and a state whose `host` entry labels no seat
/// of the roster leaves every peer with no identified host connection at all (§13.4), so a host
/// session needs the seat the handshake gave it.
#[expect(
    clippy::too_many_arguments,
    reason = "the session's options, its frame key, the host's own and the key it signs with"
)]
fn host_producer(
    options: &PeerOptions,
    frame_key: FrameKey,
    host_options: &HostOptions,
    own_key: PublicKey,
) -> Result<HostProducer, SealedError> {
    let Some(seat) = options.seat.as_deref() else {
        return Err(SealedError::new(HOST_NEEDS_SEAT));
    };
    HostProducer::new(
        &options.room_id,
        frame_key,
        options.renew,
        host_options,
        seat,
        own_key,
    )
}

/// The session document §7 fixes: one `Y.Doc`, one `Y.Text` per document.
///
/// A text offset is a UTF-16 code unit, which is what `yjs`, every editor's `offsetAt` and
/// every peer on the wire use (`PROTOCOL.md` §8.1); `yrs` defaults to UTF-8 byte offsets.
///
/// `awareness_client_id` is the id the handshake announced (§8.4): y-protocols seeds one of its
/// own, and a session that published under a second id would have every peer draw this client's
/// caret under a stranger's. `ClientID` is a 53-bit value, which is the width every id on this
/// wire is read in.
fn peer_doc(awareness_client_id: Option<u64>) -> Doc {
    const YJS_ID_BITS: u64 = 53;
    let client_id = awareness_client_id.map_or_else(ClientID::random, |id| {
        ClientID::new(id & ((1u64 << YJS_ID_BITS) - 1))
    });
    Doc::with_options(Options {
        offset_kind: OffsetKind::Utf16,
        client_id,
        ..Options::default()
    })
}

/// One y-protocols message as a `kind = 0` plaintext.
fn encode_y_message(message: &YMessage) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    message.encode(&mut encoder);
    encoder.to_vec()
}

/// A JSON value's canonical bytes. `CANONICAL.md` §2 writes an object with its members
/// ascending, which `serde_json`'s own map does (it is a `BTreeMap`).
///
/// The values this is handed are objects of strings and arrays of strings, and encoding one
/// cannot fail; an empty plaintext would be a frame no peer reads, which is `bad_payload` at
/// every receiver and a sender's bug here.
fn canonical(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}

impl Ending {
    /// The words a client says when it ends a session (§13.10 requires it to say why).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Closing => "the room closed",
            Self::HostAway => "the host has been away past its window",
            Self::NoState => "no state arrived within the no-state window",
        }
    }
}

// --- the rules, in the order §13 states them ------------------------------------

impl PeerSession {
    /// §13.6: a refused *content* frame opens an interval this client re-syncs in. The
    /// envelope's `kind` is readable before any signature is verified, so that is a claim about
    /// the bytes and not about the reason the frame was refused.
    fn note_refusal(&mut self, clock: Duration, verdict: &Verdict) {
        if verdict.kind == Some(0) {
            let _ = self.resync_from.get_or_insert(clock);
        }
    }

    /// One refused frame: §13.2's local report, which is never sent anywhere.
    fn refuse(&mut self, frame: u64, reason: Option<&str>) -> Outcome {
        // Every refusal carries the step that refused it; one without a reason would be a bug
        // in the byte layer and not a decision to report.
        let named = reason.unwrap_or("bad_envelope").to_string();
        self.dropped.push(Dropped {
            frame,
            reason: named.clone(),
        });
        Outcome::Dropped { reason: named }
    }

    /// Whether §13.10 has this frame ignored rather than applied.
    fn ignores(&self, verdict: &Verdict) -> bool {
        matches!(verdict.payload, Some(Payload::Closing(_)))
            && !self.state_held()
            && !self.mutating("any-closing")
    }

    /// Folds an applied frame into the session: §13.3's state, §13.7's lease, §13.10's
    /// closing, the announcement a host answers, and the content a `kind = 0` frame carries.
    fn fold(&mut self, clock: Duration, verdict: &Verdict) {
        match verdict.payload.as_ref() {
            Some(Payload::RoomState(state)) => {
                self.after_state(clock, state.issued);
            }
            Some(Payload::Closing(_)) => self.ending = Some(Ending::Closing),
            Some(Payload::Holds(_)) => self.renew_lease(clock, verdict.sender),
            Some(Payload::Content) => self.apply_content(&verdict.plaintext),
            Some(Payload::Announcement(announcement)) => {
                self.hear_announcement(clock, announcement);
            }
            None => {}
        }
    }

    /// §7.1: a session-key announcement the receiver accepted.
    ///
    /// A peer that is not the host owes it nothing at all — the state is the only source of
    /// the keys a receiver keeps, and an announcement is read for the host's sake (§13.3) — so
    /// the host's answer is the whole of what this decision is.
    fn hear_announcement(
        &mut self,
        clock: Duration,
        announcement: &Announcement,
    ) {
        if self.host.is_none() {
            return;
        }
        let Some(key) = PublicKey::parse(&announcement.key) else {
            return;
        };
        let declared = announcement.role.as_deref();
        if let Some(host) = self.host.as_mut() {
            host.announcement(key, declared);
        }
        self.publish_state(clock, HostReason::Announcement);
    }

    /// The state §7.1 has this host publish now, if it is due: sealed by the host key, counted
    /// as a publication, and folded into this session's own receiver.
    ///
    /// A host never receives the state it writes — the relay sends a frame to the room's
    /// *other* connections — so the receiver is folded from the value rather than from the
    /// bytes, and before anything that follows the frame (`after_state`) so that a `SyncStep1`
    /// goes out only once a state commits this connection's key (§13.1's steps 4 and 6).
    fn publish_state(&mut self, clock: Duration, reason: HostReason) {
        let Some(host) = self.host.as_mut() else {
            return;
        };
        let owed = host.publish(clock, reason);
        if let Some(fault) = host.failure() {
            let _ = self.fault.get_or_insert_with(|| fault.to_string());
        }
        let Some(publication) = owed else {
            return;
        };
        self.outbound.push_back(publication.frame);
        self.published = self.published.saturating_add(1);
        let fresh = publication.state.as_ref().filter(|_| publication.fresh);
        let Some(state) = fresh else {
            return;
        };
        self.reader.apply_own(state);
        self.after_state(clock, publication.issued);
    }

    /// One frame this connection did not author, put on the wire as the bytes it arrived as:
    /// §7.1's re-send of a state the room holds, when a peer is seated. A frame at the edition
    /// every peer already holds is refused `stale_issued` and changes nothing; the one this
    /// re-send is for is the peer that holds none.
    fn republish(&mut self, frame: &[u8]) {
        if self.ending.is_some() {
            return;
        }
        self.outbound.push_back(frame.to_vec());
        self.published = self.published.saturating_add(1);
    }

    /// What §13.3, §13.1's step 6 and §13.7 owe an applied room state.
    fn after_state(&mut self, clock: Duration, issued: u64) {
        self.state_issued = Some(issued);
        if let Some(host) = self.host.as_mut() {
            // §7.1: a host that has verified a state keeps its own series above it.
            host.verified_state(issued);
        }
        self.refresh_host_away(clock, true);
        self.drop_departed_holds();
        // §13.1's steps 6 and 4: a state that commits this key is where the handshake belongs,
        // and one that does not is what a re-announcement is for — the renewal clock alone would
        // wait a whole window for it.
        if self.commits_ours() {
            self.handshake_once(clock);
            self.flush_held_back();
        } else {
            self.announce(clock);
        }
    }

    /// §13.2 and §13.3: a `kind = 0` plaintext, applied to the session document and answered.
    fn apply_content(&mut self, plaintext: &[u8]) {
        if !self.state_held() {
            return;
        }
        let Ok(replies) =
            DefaultProtocol.handle(&mut self.awareness, plaintext)
        else {
            // A stream no replica decodes is a sender's bug: dropped, and the session goes on.
            return;
        };
        if self.role() == Some("viewer") {
            // §13.9: a `viewer` publishes its SyncStep1, its awareness and its holds, and
            // nothing else — a SyncStep2 is document content (§13.5) and is not its to send.
            return;
        }
        if !self.commits_ours() {
            // §13.1's step 4: nothing but the announcement until a state commits this key.
            return;
        }
        for reply in replies {
            let bytes = encode_y_message(&reply);
            self.publish(Published::Sync, &bytes);
        }
    }

    /// §13.7: only an accepted holds message renews a lease, and nothing else from the same
    /// peer does.
    fn renew_lease(&mut self, clock: Duration, sender: Option<KeyId>) {
        if let Some(id) = sender {
            let _ = self.leases.insert(id, clock);
        }
    }

    /// Whether an applied state commits this connection's own session key.
    fn commits_ours(&self) -> bool {
        self.reader
            .committed
            .values()
            .any(|peer| peer.key == self.session.public())
    }

    /// The role the applied state gives this connection's own key (§13.4).
    fn role(&self) -> Option<&str> {
        self.reader.role_of_key(self.session.public())
    }

    /// Whether §13.1's step 4 lets this client publish anything but its announcement.
    fn may_publish(&self) -> bool {
        self.state_held() && self.commits_ours()
    }

    /// §13.1's step 4: the session-key announcement, `kind = 4`, signed by the key it names.
    fn announce(&mut self, clock: Duration) {
        let mut members = serde_json::Map::new();
        let _ = members
            .insert("key".to_string(), json!(self.session.public().encode()));
        if let Some(role) = &self.declared_role {
            let _ = members.insert("role".to_string(), json!(role));
        }
        let plaintext = canonical(&Value::Object(members));
        self.publish(Published::Announcement, &plaintext);
        self.announced_at = Some(clock);
    }

    /// §13.1's step 4: re-announce on the session's renewal clock for as long as no applied
    /// state commits this key, which is how an announcement the relay dropped is recovered.
    fn reannounce(&mut self, clock: Duration) {
        if self.commits_ours() || self.mutating("announce-once") {
            return;
        }
        let due = match self.announced_at {
            None => true,
            Some(at) => clock.saturating_sub(at) >= self.renew,
        };
        if due {
            self.announce(clock);
        }
    }

    /// §13.7: the whole held set, renewed on the client's own clock and published at once
    /// when it changes. Rising from the tick and from nothing a peer sends is what lets a
    /// seated, idle peer keep its holds.
    fn announce_holds(&mut self, clock: Duration) {
        if !self.may_publish() {
            // A `viewer` keeps its edit in its own replica and never publishes it (§13.9);
            // everyone else's is held back by §13.1's step 4 and sent once a state commits this
            // key.
            return;
        }
        // A holder with nothing open and nothing yet said has no frame to send; one that has
        // released everything says so, because the empty set is what §13.7 asks for.
        if self.held.is_empty()
            && self.holds_sent.is_empty()
            && self.holds_announced_at.is_none()
        {
            return;
        }
        let paths: Vec<String> = self.held.iter().cloned().collect();
        let due = paths != self.holds_sent
            || match self.holds_announced_at {
                None => true,
                Some(at) => clock.saturating_sub(at) >= self.renew,
            };
        if !due {
            return;
        }
        let plaintext = canonical(&json!({"holds": &paths}));
        self.publish(Published::Holds, &plaintext);
        self.holds_sent = paths;
        self.holds_announced_at = Some(clock);
    }
}

// --- the clocks, the seats and what leaves the session --------------------------

impl PeerSession {
    /// §13.7's expiry: a peer's whole held set is forgotten once its lease has lapsed.
    ///
    /// The text has this checked on the renewal tick, and a client whose tick *is* its renewal
    /// tick checks there; a tick that comes more often forgets sooner, which is inside the
    /// `awareness_expire_ms + awareness_renew_ms` the text bounds the delay by and never
    /// inside the `MUST forget ... after awareness_expire_ms` it states. An expiry is not a
    /// refusal — no frame is dropped, the peer is not gone and nothing is reported anywhere —
    /// which is why the only observable is the set becoming empty.
    fn expire_leases(&mut self, clock: Duration) {
        if self.mutating("no-lease") {
            return;
        }
        let lapsed: Vec<KeyId> = self
            .leases
            .iter()
            .filter(|(_, at)| clock.saturating_sub(**at) >= self.expire)
            .map(|(id, _)| *id)
            .collect();
        for id in lapsed {
            let _ = self.leases.remove(&id);
            let _ = self.reader.holds.remove(&id);
        }
    }

    /// §13.8's host-away clock: armed while the applied state's `host` entry labels a seat
    /// the roster does not have, disarmed when it labels one the roster has. `restart` is
    /// what a new state does to a clock that is already running.
    fn refresh_host_away(&mut self, clock: Duration, restart: bool) {
        // §13.4: a state that names no `host` entry leaves this client with no identified
        // host connection and it MUST NOT guess one, so there is nothing to arm a clock with.
        let absent = self
            .host_seat()
            .is_some_and(|seat| !self.roster.contains(seat));
        if !absent {
            self.host_away_since = None;
        } else if restart || self.host_away_since.is_none() {
            self.host_away_since = Some(clock);
        }
    }

    /// The seat the applied state's `host` entry labels, in the order §6.1 reads two `host`
    /// keys: the one whose key comes first in UTF-16 code-unit order.
    fn host_seat(&self) -> Option<&str> {
        self.reader
            .committed
            .values()
            .find(|peer| peer.role == "host")
            .map(|peer| peer.peer_id.as_str())
    }

    /// §13.7: the roster is the authority on who is present, so a key whose entry labels a
    /// seat that has left loses its holds at once rather than with its lease. A seat the
    /// roster never knew is not this rule: it is left to its lease.
    fn drop_departed_holds(&mut self) {
        let gone: Vec<KeyId> = self
            .reader
            .committed
            .values()
            .filter(|peer| self.departed.contains(&peer.peer_id))
            .map(|peer| peer.key.id())
            .collect();
        for id in gone {
            let _ = self.leases.remove(&id);
            let _ = self.reader.holds.remove(&id);
        }
    }

    /// §13.3's no-state window and §13.8's host-away window, which run in sequence.
    fn window_passed(&mut self, clock: Duration) -> bool {
        if let Some(since) = self.host_away_since
            && clock.saturating_sub(since) >= self.expire
        {
            self.ending = Some(Ending::HostAway);
            return true;
        }
        if !self.state_held()
            && !self.mutating("wait-for-ever")
            && clock >= self.expire
        {
            self.ending = Some(Ending::NoState);
            return true;
        }
        false
    }

    /// §13.1's step 6's handshake, run when this connection's own key *becomes* committed and
    /// not for every state that commits it: a host republishes its state on every `peer.joined`
    /// and on every announcement it accepts, so a handshake per state would be a frame per
    /// republish.
    fn handshake_once(&mut self, clock: Duration) {
        if self.handshaken_at.is_none() {
            self.sync_step1(clock);
        }
    }

    /// §13.1's step 6: a `SyncStep1` with this replica's state vector, so the room can tell a
    /// client that has been refusing frames from one that has been applying them.
    fn sync_step1(&mut self, clock: Duration) {
        let vector = {
            let txn = self.awareness.doc().transact();
            txn.state_vector()
        };
        let message =
            encode_y_message(&YMessage::Sync(SyncMessage::SyncStep1(vector)));
        self.publish(Published::Sync, &message);
        self.handshaken_at = Some(clock);
    }

    /// §13.6: a client that refused a content frame re-syncs, and no more than once per renewal
    /// interval however many it refused — the lower bound is what keeps a peer that floods
    /// refused frames from being answered frame for frame.
    fn resync(&mut self, clock: Duration) {
        if self.resync_from.is_none() || !self.may_publish() {
            return;
        }
        let due = match self.handshaken_at {
            None => true,
            Some(at) => clock.saturating_sub(at) >= self.renew,
        };
        if due {
            self.resync_from = None;
            self.sync_step1(clock);
        }
    }

    /// One frame sealed under the frame key and signed by this connection's session key.
    fn publish(&mut self, what: Published, plaintext: &[u8]) {
        let Ok(bytes) = self.sealed_frame(what.kind(), plaintext) else {
            // A CSPRNG that will not read, or an AEAD that refuses its own inputs. Neither
            // happens in practice, and a session that kept quiet about one would look like a
            // client with nothing to say.
            self.fault = Some(format!("a {what:?} frame could not be sealed"));
            return;
        };
        self.outbound.push_back(bytes);
        let tally = match what.counted() {
            Counted::Publication => &mut self.published,
            Counted::Handshake => &mut self.handshake,
        };
        *tally = tally.saturating_add(1);
    }

    fn sealed_frame(
        &mut self,
        kind: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, SealedError> {
        self.counter = self.counter.saturating_add(1);
        let nonce = fresh_nonce()?;
        let recipe = Recipe {
            room_id: &self.room_id,
            frame_key: &self.frame_key,
            kind,
            epoch: 0,
            counter: self.counter,
            nonce,
            signer: &self.session,
        };
        Ok(seal(&recipe, plaintext)?.bytes())
    }

    /// The canonical spelling of a key id where an applied state names one, and its id where
    /// nothing does (§13.4).
    fn spelling(&self, id: KeyId) -> String {
        self.reader
            .committed
            .values()
            .find(|peer| peer.key.id() == id)
            .map_or_else(|| id.hex(), |peer| peer.key.encode())
    }
}

// --- what a driver hands in -----------------------------------------------------

impl PeerSession {
    /// A seat has joined, from `peer.joined`.
    ///
    /// §7.1 obliges a host to publish a state on it — which is how the joiner learns the
    /// listing and the roles without asking — and asks any peer that holds a verified state to
    /// re-send that state unchanged, so a joiner's state arrives while the host is away. §13.7
    /// has a holder re-announce its holds on the same event.
    pub fn seat_joined(&mut self, clock: Duration, seat: &str) {
        if let Some(host) = self.host.as_mut() {
            host.seat_joined(seat);
        }
        let _ = self.departed.remove(seat);
        let _ = self.roster.insert(seat.to_string());
        // §13.7: a hold is re-announced when a peer is seated, so a joiner learns the room's
        // held set without asking for it.
        self.holds_announced_at = None;
        if self.host.is_some() {
            self.publish_state(clock, HostReason::Roster);
            return;
        }
        if let Some(frame) = self.held_state_frame.clone() {
            self.republish(&frame);
        }
    }

    /// A seat has left, from `peer.left`: §13.8's clock can arm on it and §13.7's holds go.
    pub fn seat_left(&mut self, clock: Duration, seat: &str) {
        if let Some(host) = self.host.as_mut() {
            host.seat_left(seat);
        }
        let _ = self.roster.remove(seat);
        let _ = self.departed.insert(seat.to_string());
        self.drop_departed_holds();
        self.refresh_host_away(clock, false);
        if self.host.is_some() {
            self.publish_state(clock, HostReason::Roster);
        }
    }

    /// The host's listing changed, from whatever watches its working tree (§7.1).
    ///
    /// A listing is replaced wholesale by every state, so this is the one thing a host's
    /// adapter has to say about it: the state that follows names the whole tree as it now is,
    /// and a shorter listing is a smaller working tree rather than a partial update (§13.3).
    pub fn listing_changed(&mut self, clock: Duration) {
        self.publish_state(clock, HostReason::Listing);
    }

    /// §7.1's closing: the host's statement that the room is over, above every state it
    /// published.
    ///
    /// A host that publishes one stops publishing; every peer that already holds a verified
    /// state below its `issued` applies it and ends, and §9's room dies when its last
    /// connection ends. The session that published it ends with them, which is what §13.10
    /// gives a receiver that applies one and what keeps a host from publishing content into a
    /// room it has just declared over.
    pub fn close_room(&mut self) -> bool {
        let Some(host) = self.host.as_mut() else {
            return false;
        };
        let closing = host.closing();
        let failure = host.failure().map(ToString::to_string);
        let failed = failure.unwrap_or_else(|| CLOSING_UNSEALED.to_string());
        let Some(publication) = closing else {
            self.fault.get_or_insert(failed);
            return false;
        };
        self.outbound.push_back(publication.frame);
        self.published = self.published.saturating_add(1);
        self.ending = Some(Ending::Closing);
        true
    }

    /// Opens a path: this client offers it, and its whole held set changes (§13.7).
    pub fn open(&mut self, path: &str) {
        let _ = self.held.insert(path.to_string());
    }

    /// Releases every path. §13.7 asks for the empty set rather than for silence, so the room
    /// learns in one hop instead of waiting out a lease.
    pub fn release(&mut self) {
        self.held.clear();
    }

    /// A local edit, published as the delta it produced and never as the whole document.
    ///
    /// Returns whether anything went out: §13.5 and §13.9 have a `viewer` keep its edit and
    /// not send it, and §13.1's step 4 has any peer keep it until a state commits its key.
    ///
    /// # Errors
    ///
    /// Returns an error when `index` is past the end of the text, and when the platform's
    /// CSPRNG cannot be read.
    #[expect(
        clippy::too_many_arguments,
        reason = "the path, the offset and the text are the three members of one edit"
    )]
    pub fn insert(
        &mut self,
        path: &str,
        index: u32,
        text: &str,
    ) -> Result<bool, SealedError> {
        let before = {
            let txn = self.awareness.doc().transact();
            txn.state_vector()
        };
        // An index past the end of the text is the caller's bug, and `yrs` answers one by
        // panicking — the type is not there. A client any caller can kill with an offset is not
        // one a decision vector can drive, so it is refused as an error like any other.
        if index > self.length(path) {
            return Err(SealedError::new(format!(
                "there is no offset {index} in {path:?}"
            )));
        }
        {
            let doc = self.awareness.doc_mut();
            let handle = doc.get_or_insert_text(path);
            let mut txn = doc.transact_mut();
            handle.insert(&mut txn, index, text);
        }
        let update = {
            let txn = self.awareness.doc().transact();
            txn.encode_state_as_update_v1(&before)
        };
        if update.is_empty() {
            return Ok(false);
        }
        if !self.may_publish() {
            // §13.1's step 4 holds content back until a state commits this key, and a client
            // that dropped what it held back would leave the room without that edit for good:
            // §13.1's step 6 is a `SyncStep1`, which asks the room for what this replica lacks,
            // and nothing asks the room for what it lacks. What is kept is this edit's own
            // delta and not a diff over the document: another peer's content can have arrived
            // in the meantime, and a client that re-sent it would publish under its own key
            // changes it did not make.
            self.hold_back(update);
            return Ok(false);
        }
        if self.role() == Some("viewer") {
            // §13.5 and §13.9: a `viewer`'s edit is its own and never the room's, so there is
            // nothing to send later either.
            return Ok(false);
        }
        self.send_update(update);
        Ok(true)
    }

    /// Keeps one edit this connection may not publish yet (§13.1's step 4). §13.9's `viewer`
    /// keeps its edit in its own replica and never publishes it, so nothing is kept for one.
    fn hold_back(&mut self, update: Vec<u8>) {
        if self.role() != Some("viewer") {
            self.unsent.push(update);
        }
    }

    fn send_update(&mut self, update: Vec<u8>) {
        let plaintext =
            encode_y_message(&YMessage::Sync(SyncMessage::Update(update)));
        self.publish(Published::Content, &plaintext);
    }

    /// Publishes the edits this connection made before a state committed its key (§13.1's step
    /// 4), in the order they were made.
    fn flush_held_back(&mut self) {
        let held: Vec<Vec<u8>> = mem::take(&mut self.unsent);
        if held.is_empty() || self.role() == Some("viewer") {
            return;
        }
        for update in held {
            self.send_update(update);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sealed::{Envelope, RoomKey, encode_key, opens, read_varuint};
    use serde_json::json;
    use yrs::{Doc, Text, Transact};

    /// A room, a host keypair and a session keypair, all derived from constants: these are
    /// unit tests of §13's rules and they need no fixture to be one.
    const ROOM: &str = "R7f3a2c19";
    const MILLIS: u64 = 1;

    fn room_key() -> RoomKey {
        RoomKey([7; 32])
    }

    fn host() -> SessionKey {
        SessionKey::from_seed([3; 32])
    }

    /// This connection's session keypair, fixed so that a state can commit it.
    fn ours() -> SessionKey {
        SessionKey::from_seed([5; 32])
    }

    /// A second peer's, committed as `guest` or `viewer` beside ours.
    fn peer() -> SessionKey {
        SessionKey::from_seed([11; 32])
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "what a test seals with: the signer, the kind, the counter and the plaintext"
    )]
    fn frame(
        signer: &SessionKey,
        kind: u64,
        counter: u64,
        plaintext: &[u8],
    ) -> Vec<u8> {
        let key = room_key().frame_key(ROOM);
        let recipe = Recipe {
            room_id: ROOM,
            frame_key: &key,
            kind,
            epoch: 0,
            counter,
            nonce: [9; 12],
            signer,
        };
        seal(&recipe, plaintext).unwrap().bytes()
    }

    fn state(
        signer: &SessionKey,
        issued: u64,
        peers: &[(&SessionKey, &str, &str)],
    ) -> Vec<u8> {
        let members: serde_json::Map<String, Value> = peers
            .iter()
            .map(|(key, role, seat)| {
                (
                    key.public().encode(),
                    json!({"peer_id": seat, "role": role}),
                )
            })
            .collect();
        let payload = json!({"issued": issued, "listing": ["README.md"], "peers": members});
        frame(signer, 1, 1, &serde_json::to_vec(&payload).unwrap())
    }

    fn holds(signer: &SessionKey, counter: u64, paths: &[&str]) -> Vec<u8> {
        let payload = json!({"holds": paths});
        frame(signer, 3, counter, &serde_json::to_vec(&payload).unwrap())
    }

    fn closing(signer: &SessionKey, issued: u64) -> Vec<u8> {
        let payload = json!({"closing": true, "issued": issued});
        frame(signer, 2, 1, &serde_json::to_vec(&payload).unwrap())
    }

    /// A `kind = 0` frame carrying a real Update, which is document content.
    fn content(signer: &SessionKey, counter: u64, text: &str) -> Vec<u8> {
        let doc = Doc::new();
        let handle = doc.get_or_insert_text("README.md");
        {
            let mut txn = doc.transact_mut();
            handle.insert(&mut txn, 0, text);
        }
        let update = {
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };
        let message =
            encode_y_message(&YMessage::Sync(SyncMessage::Update(update)));
        frame(signer, 0, counter, &message)
    }

    fn session(roster: &[&str]) -> PeerSession {
        let options = PeerOptions {
            room_id: ROOM.to_string(),
            room_key: room_key(),
            host_key: host().public(),
            renew: Duration::from_millis(300),
            expire: Duration::from_millis(900),
            seat: Some("p-self".to_string()),
            roster: roster.iter().map(|seat| (*seat).to_string()).collect(),
            fixed_session_key: Some([5; 32]),
            declared_role: None,
            awareness_client_id: None,
            host: None,
        };
        PeerSession::new(&options).unwrap()
    }

    fn millis(count: u64) -> Duration {
        Duration::from_millis(count)
    }

    #[test]
    fn the_announcement_goes_out_first_and_nothing_else_before_a_state() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let published = session.take_outbound();
        assert_eq!(
            published.len(),
            1,
            "§13.1's step 4 is the first binary frame"
        );
        assert_eq!(session.published(), 1);
        assert_eq!(session.handshake(), 0);

        let envelope = Envelope::parse(&published[0]).unwrap();
        assert_eq!(envelope.kind, 4);
        assert_eq!(envelope.counter, 1);
        let plaintext =
            opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
        let announcement: Value = serde_json::from_slice(&plaintext).unwrap();
        assert_eq!(announcement["key"], json!(ours().public().encode()));
    }

    #[test]
    fn an_uncommitted_key_is_re_announced_on_the_renewal_clock_and_stops_once_committed()
     {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let _ = session.take_outbound();

        // A state that commits another peer and not this connection: §13.1's step 4 announces
        // again at once, because the renewal clock alone would wait a whole window.
        let other = state(&host(), 1, &[(&peer(), "guest", "p-other")]);
        assert_eq!(
            session.deliver(millis(MILLIS), &other),
            Outcome::Applied { kind: 1 }
        );
        let _ = session.take_outbound();
        assert_eq!(session.published(), 2);

        session.tick(millis(MILLIS + 299));
        assert!(
            session.take_outbound().is_empty(),
            "the clock has not passed"
        );
        session.tick(millis(MILLIS + 300));
        assert_eq!(session.take_outbound().len(), 1, "§13.1's renewal clock");
        assert_eq!(session.published(), 3);

        // A state that commits this connection's key ends the recovery.
        let committing = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
        assert_eq!(
            session.deliver(millis(MILLIS + 301), &committing),
            Outcome::Applied { kind: 1 }
        );
        let _ = session.take_outbound();
        session.tick(millis(MILLIS + 900));
        session.tick(millis(MILLIS + 1200));
        assert!(session.take_outbound().is_empty());
        assert_eq!(session.published(), 3);
    }

    #[test]
    fn a_committing_state_answers_with_the_handshake_and_moves_no_publication()
    {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let _ = session.take_outbound();

        let committing = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
        assert_eq!(
            session.deliver(millis(MILLIS), &committing),
            Outcome::Applied { kind: 1 }
        );
        let after = session.take_outbound();
        assert_eq!(after.len(), 1, "§13.1's step 6, once");
        assert_eq!(session.handshake(), 1);
        assert_eq!(
            session.published(),
            1,
            "a handshake frame is not a publication"
        );

        let envelope = Envelope::parse(&after[0]).unwrap();
        assert_eq!(envelope.kind, 0);
        let plaintext =
            opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
        assert_eq!(plaintext.first(), Some(&0), "message type 0: sync");
        assert_eq!(read_varuint(&plaintext, 1).unwrap().0, 0, "SyncStep1");
        assert_eq!(session.listing(), ["README.md"]);
        assert!(session.state_held());
    }

    #[test]
    fn a_second_state_that_commits_our_key_sends_no_second_handshake() {
        // §13.1's step 6 is a handshake *once*, and a host republishes its state on every
        // `peer.joined` and on every announcement it accepts: a handshake per state would be a
        // frame per republish.
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let first = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
        assert_eq!(
            session.deliver(millis(1), &first),
            Outcome::Applied { kind: 1 }
        );
        let _ = session.take_outbound();
        assert_eq!(session.handshake(), 1);

        let republished = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
        assert_eq!(
            session.deliver(millis(2), &republished),
            Outcome::Applied { kind: 1 }
        );
        session.tick(millis(2 + 300));
        assert!(session.take_outbound().is_empty());
        assert_eq!(session.handshake(), 1, "one handshake per connection");
    }

    #[test]
    fn an_edit_past_the_end_of_the_text_is_refused_and_not_a_panic() {
        // `yrs` answers an offset that is not there by panicking, and a client a caller can
        // kill with an index is not one a decision vector can drive.
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
        let _ = session.deliver(millis(1), &state);
        assert!(session.insert("README.md", 0, "hello").unwrap());
        assert!(session.insert("README.md", 5, "!").unwrap());
        assert_eq!(session.length("README.md"), 6);
        let refused = session.insert("README.md", 7, "!");
        assert!(refused.is_err(), "past the end is refused");
        assert_eq!(session.length("README.md"), 6, "and nothing was applied");
    }

    #[test]
    fn two_states_at_one_edition_leave_the_first_applied() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let first = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
        let second = state(&host(), 2, &[(&peer(), "guest", "p-other")]);
        assert_eq!(
            session.deliver(millis(1), &first),
            Outcome::Applied { kind: 1 }
        );
        assert_eq!(
            session.deliver(millis(2), &second),
            Outcome::Dropped {
                reason: "stale_issued".to_string()
            }
        );
        assert_eq!(session.applied().len(), 1);
        assert_eq!(session.dropped().len(), 1);
    }

    #[test]
    fn the_same_state_is_still_refused_under_the_ignore_issued_mutation() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let first = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
        let second = state(&host(), 2, &[(&peer(), "guest", "p-other")]);
        let _ = session.deliver(millis(1), &first);
        session.mutate("ignore-issued").unwrap();
        assert_eq!(
            session.deliver(millis(2), &second),
            Outcome::Applied { kind: 1 }
        );
    }

    #[test]
    fn a_committed_viewers_content_is_refused_and_its_own_edit_is_not_published()
     {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        // Both keys: ours is committed, so §13.1's step 4 has nothing to re-announce, and
        // the peer's is the `viewer` whose frame is refused.
        let state = state(
            &host(),
            1,
            &[
                (&ours(), "guest", "p-self"),
                (&peer(), "viewer", "p-viewer"),
            ],
        );
        assert_eq!(
            session.deliver(millis(1), &state),
            Outcome::Applied { kind: 1 }
        );
        let _ = session.take_outbound();
        let edit = content(&peer(), 1, "hello");
        assert_eq!(
            session.deliver(millis(2), &edit),
            Outcome::Dropped {
                reason: "unauthorised_content".to_string()
            }
        );
        assert_eq!(session.published(), 1, "nothing was published in answer");
        assert_eq!(session.text("README.md"), "", "and nothing was applied");

        // §13.6: the refusal opens an interval, and the client re-syncs in it — once per
        // renewal interval, however many content frames it refused.
        assert!(session.take_outbound().is_empty());
        session.tick(millis(2 + 300));
        let after = session.take_outbound();
        assert_eq!(after.len(), 1, "§13.6's re-sync");
        assert_eq!(session.handshake(), 2, "§13.1's step 6, then §13.6's");
        assert_eq!(
            session.published(),
            1,
            "a handshake frame is not a publication"
        );
        assert_eq!(
            Envelope::parse(&after[0]).unwrap().kind,
            0,
            "a `kind = 0` frame carrying the SyncStep1"
        );
        session.tick(millis(2 + 301));
        session.tick(millis(2 + 302));
        assert!(session.take_outbound().is_empty(), "once per interval");
    }

    #[test]
    fn an_edit_made_before_a_committing_state_goes_out_when_it_arrives() {
        // §13.1's step 4 holds content back until a state commits this key, and a client that
        // dropped what it held back would leave the room without that edit: nothing asks the
        // room for what this replica lacks, only for what it has.
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let _ = session.take_outbound();
        assert!(
            !session.insert("README.md", 0, "held back").unwrap(),
            "nothing may be published before a state commits this key"
        );
        assert_eq!(session.published(), 1, "the announcement, and no content");

        let committing = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
        assert_eq!(
            session.deliver(millis(1), &committing),
            Outcome::Applied { kind: 1 }
        );
        let out = session.take_outbound();
        assert_eq!(out.len(), 2, "§13.1's step 6, then the held-back edit");
        assert_eq!(session.handshake(), 1);
        assert_eq!(session.published(), 2, "the edit is a publication");
        assert_eq!(Envelope::parse(&out[1]).unwrap().kind, 0);
    }

    #[test]
    fn a_viewer_does_not_publish_its_own_edit() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&ours(), "viewer", "p-self")]);
        let _ = session.deliver(millis(1), &state);
        let _ = session.take_outbound();
        assert!(!session.insert("README.md", 0, "hello").unwrap());
        assert_eq!(session.published(), 1);
        assert_eq!(
            session.text("README.md"),
            "hello",
            "its own replica holds it"
        );
    }

    #[test]
    fn a_guest_publishes_its_edit_as_a_delta() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
        let _ = session.deliver(millis(1), &state);
        let _ = session.take_outbound();
        assert!(session.insert("README.md", 0, "hello").unwrap());
        let out = session.take_outbound();
        assert_eq!(out.len(), 1);
        assert_eq!(session.published(), 2);
        let envelope = Envelope::parse(&out[0]).unwrap();
        assert_eq!(envelope.kind, 0);
    }

    #[test]
    fn a_closing_handed_to_a_state_less_client_is_ignored_and_the_state_below_it_still_applies()
     {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let _ = session.take_outbound();
        assert_eq!(
            session.deliver(millis(1), &closing(&host(), 2)),
            Outcome::Ignored { kind: 2 }
        );
        assert_eq!(session.applied().len(), 0);
        assert_eq!(session.dropped().len(), 0);
        assert_eq!(session.ignored(), [0]);
        assert_eq!(session.ending(), None);

        let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
        assert_eq!(
            session.deliver(millis(2), &state),
            Outcome::Applied { kind: 1 }
        );
        let again = closing(&host(), 2);
        assert_eq!(
            session.deliver(millis(3), &again),
            Outcome::Applied { kind: 2 }
        );
        assert_eq!(session.ending(), Some(Ending::Closing));
    }

    #[test]
    fn the_same_closing_ends_a_client_that_holds_no_state_under_the_any_closing_mutation()
     {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        session.mutate("any-closing").unwrap();
        assert_eq!(
            session.deliver(millis(1), &closing(&host(), 2)),
            Outcome::Applied { kind: 2 }
        );
        assert_eq!(session.ending(), Some(Ending::Closing));
    }

    #[test]
    fn a_lease_lapses_a_window_after_the_last_message_and_drops_no_frame() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&peer(), "guest", "p-other")]);
        let _ = session.deliver(millis(1), &state);
        let set = holds(&peer(), 1, &["README.md"]);
        assert_eq!(
            session.deliver(millis(2), &set),
            Outcome::Applied { kind: 3 }
        );
        assert_eq!(
            session.peer_holds()[&peer().public().encode()],
            ["README.md"]
        );

        session.tick(millis(2 + 899));
        assert_eq!(session.peer_holds().len(), 1, "the lease has not lapsed");
        session.tick(millis(2 + 900));
        assert!(session.peer_holds().is_empty());
        assert_eq!(session.dropped().len(), 0, "an expiry is not a refusal");
        assert_eq!(session.ending(), None, "and it is not a departure");
    }

    #[test]
    fn a_lease_never_lapses_under_the_no_lease_mutation() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&peer(), "guest", "p-other")]);
        let _ = session.deliver(millis(1), &state);
        let _ = session.deliver(millis(2), &holds(&peer(), 1, &["README.md"]));
        session.mutate("no-lease").unwrap();
        session.tick(millis(10_000));
        assert_eq!(session.peer_holds().len(), 1);
    }

    #[test]
    fn a_seated_client_with_no_state_ends_at_the_no_state_window() {
        let mut session = session(&[]);
        session.tick(millis(899));
        assert_eq!(session.ending(), None, "not before the window");
        session.tick(millis(900));
        assert_eq!(session.ending(), Some(Ending::NoState));
    }

    #[test]
    fn the_same_client_stays_for_ever_under_the_wait_for_ever_mutation() {
        let mut session = session(&[]);
        session.mutate("wait-for-ever").unwrap();
        session.tick(millis(60_000));
        assert_eq!(session.ending(), None);
    }

    #[test]
    fn a_state_whose_host_entry_is_unseated_ends_the_session_a_window_later() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        // The roster names no seat, and the state's `host` entry labels one: §13.8's clock.
        let state = state(&host(), 1, &[(&peer(), "host", "p-absent")]);
        assert_eq!(
            session.deliver(millis(1), &state),
            Outcome::Applied { kind: 1 }
        );
        session.tick(millis(900));
        assert_eq!(
            session.ending(),
            None,
            "one millisecond short of the window"
        );
        session.tick(millis(901));
        assert_eq!(session.ending(), Some(Ending::HostAway));
    }

    #[test]
    fn a_peer_left_for_the_seat_the_host_entry_labels_arms_the_clock() {
        // §13.11's fixture for the host-away clock, which no decision vector carries: the
        // state labels a seat the roster has, and the roster then loses it.
        let mut session = session(&["p-host"]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&peer(), "host", "p-host")]);
        let _ = session.deliver(millis(1), &state);
        session.tick(millis(500));
        assert_eq!(session.ending(), None, "the host is seated");

        session.seat_left(millis(600), "p-host");
        session.tick(millis(600 + 899));
        assert_eq!(session.ending(), None);
        session.tick(millis(600 + 900));
        assert_eq!(session.ending(), Some(Ending::HostAway));
    }

    #[test]
    fn a_state_that_does_not_name_a_host_arms_nothing() {
        // §13.4: a client MUST NOT guess which connection is the host's.
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&peer(), "guest", "p-other")]);
        let _ = session.deliver(millis(1), &state);
        session.tick(millis(5_000));
        assert_eq!(session.ending(), None);
    }

    #[test]
    fn a_held_set_is_announced_wholesale_and_an_empty_one_is_a_release() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
        let _ = session.deliver(millis(1), &state);
        let _ = session.take_outbound();

        session.open("README.md");
        session.open("src/main.rs");
        session.tick(millis(2));
        let out = session.take_outbound();
        assert_eq!(out.len(), 1, "a changed set goes out at once");
        let envelope = Envelope::parse(&out[0]).unwrap();
        assert_eq!(envelope.kind, 3);
        let plaintext =
            opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
        let payload: Value = serde_json::from_slice(&plaintext).unwrap();
        assert_eq!(payload["holds"], json!(["README.md", "src/main.rs"]));

        session.release();
        session.tick(millis(3));
        let out = session.take_outbound();
        assert_eq!(out.len(), 1);
        assert_eq!(session.held().len(), 0);
        let envelope = Envelope::parse(&out[0]).unwrap();
        let plaintext =
            opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
        let payload: Value = serde_json::from_slice(&plaintext).unwrap();
        assert_eq!(payload["holds"], json!([] as [&str; 0]));
    }

    #[test]
    fn a_hold_message_from_a_key_no_state_commits_is_refused() {
        // `PROTOCOL.md` §13.4 and `CANONICAL.md` §6.1's step 4: a `kind = 3` frame resolves
        // against the keys the applied state commits, and this one commits only ours.
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
        let _ = session.deliver(millis(1), &state);
        let _ = session.take_outbound();
        assert_eq!(
            session.deliver(millis(2), &holds(&peer(), 1, &["README.md"])),
            Outcome::Dropped {
                reason: "uncommitted_key".to_string()
            }
        );
        assert!(session.peer_holds().is_empty());
    }

    #[test]
    fn a_frame_is_refused_without_ending_the_session() {
        let mut session = session(&[]);
        session.tick(Duration::ZERO);
        assert_eq!(
            session.deliver(millis(1), &frame(&peer(), 3, 1, b"{}")),
            Outcome::Dropped {
                reason: "uncommitted_key".to_string()
            }
        );
        assert_eq!(session.ending(), None);
    }

    fn invite(room: RoomKey, fragment: &str) -> String {
        format!("/session?room={ROOM}&token=t-1#{fragment}")
            .replace("$k", &encode_key(&[7; 32]))
            .replace("$h", &host().public().encode())
            .replace("$room", &encode_key(&room.0))
    }

    #[test]
    fn the_invite_carries_the_room_and_both_keys_and_the_fragment_is_stripped()
    {
        let link = invite(room_key(), "k=$k&h=$h");
        let parsed = PeerInvite::parse(&link).unwrap();
        assert_eq!(parsed.room, ROOM);
        assert_eq!(parsed.token, "t-1");
        assert_eq!(parsed.room_key, RoomKey([7; 32]));
        assert_eq!(parsed.host_key, host().public());
        assert!(!parsed.socket_url.contains('#'));
    }

    #[test]
    fn an_invite_with_no_fragment_or_with_a_value_that_is_not_a_key_is_refused_locally()
     {
        let missing = format!("/session?room={ROOM}&token=t-1");
        assert!(
            PeerInvite::parse(&missing)
                .unwrap_err()
                .contains("fragment")
        );

        let short = invite(room_key(), "k=AAAA&h=$h");
        assert!(PeerInvite::parse(&short).unwrap_err().contains("`k`"));

        let twice = invite(room_key(), "k=$k&k=$k&h=$h");
        assert!(PeerInvite::parse(&twice).unwrap_err().contains("twice"));

        // §6.1: the final character of a 32-byte value carries two zero bits, so a spelling
        // whose last character is any other spells no key.
        let uncanonical = invite(room_key(), "k=$kB&h=$h");
        assert!(PeerInvite::parse(&uncanonical).unwrap_err().contains("`k`"));

        let no_host = invite(room_key(), "k=$k");
        assert!(PeerInvite::parse(&no_host).unwrap_err().contains("`h`"));
    }

    #[test]
    fn the_page_link_is_the_same_room_token_and_fragment_over_the_browsers_scheme()
     {
        // `PROTOCOL.md` §5.1's second form: the page the room's server serves, over the scheme
        // a browser speaks, with the same host, port and path prefix and nothing else in it.
        let room = encode_key(&room_key().0);
        let host_key = host().public().encode();
        let page = format!(
            "http://h:8080/?room={ROOM}&token=t-1#k={room}&h={host_key}"
        );
        let wire = format!("ws://h:8080/session?room={ROOM}&token=t-1");
        let wire_with_fragment = format!("{wire}#k={room}&h={host_key}");
        assert_eq!(
            wire_invite(&page),
            wire_with_fragment,
            "the page reads back as the same wire invite, fragment and all"
        );
        let parsed = PeerInvite::parse(&page).unwrap();
        assert_eq!(
            parsed.socket_url, wire,
            "and the address a socket is opened on carries no fragment"
        );
        assert_eq!(parsed.room, ROOM);
        assert_eq!(parsed.token, "t-1");
        assert_eq!(parsed.room_key, room_key());
        assert_eq!(parsed.host_key, host().public());

        // A path prefix survives the reading: the page is served under it and the endpoint is
        // under it too.
        let mounted = format!(
            "https://h/page/?room={ROOM}&token=t-1#k={room}&h={host_key}"
        );
        assert_eq!(
            PeerInvite::parse(&mounted).unwrap().socket_url,
            format!("wss://h/page/session?room={ROOM}&token=t-1")
        );

        // A link that is already a connection URL is handed back exactly as it stands.
        assert_eq!(wire_invite(&wire), wire);
        assert!(PeerInvite::parse(&wire).unwrap_err().contains("fragment"));
        // And one this cannot read as either form is not silently rewritten.
        assert_eq!(wire_invite("wss://h/other?room=r"), "wss://h/other?room=r");
    }
}
