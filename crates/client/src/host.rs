//! `selvage/2`'s host: the producer half of `PROTOCOL.md` §7.1.
//!
//! A host is a peer with one extra key and one statement to publish. The key is the host key
//! the invite's fragment carries, whose private half only the host holds; the statement is the
//! **room state** (`kind = 1`) — the room's listing, the roles the host assigns and the state's
//! own `issued` edition — sealed under the frame key and signed by the host key. Nothing else
//! in this version carries a listing or a role, so a joiner that has no state holds no key and
//! may publish nothing at all (§13.1).
//!
//! This module decides **when** a state goes out and **what** it carries: §7.1's obligations
//! (at mint, on a change to the listing or to `peers`, on every `peer.joined` and `peer.left`,
//! on every announcement accepted), the publish-rate bound that keeps a peer minting keys
//! without bound from obliging a state a frame, and the two values a host that means to keep
//! hosting after a reload must keep (the host key, and its `issued` beside it).
//!
//! It holds no socket and no editor: every clock is the caller's monotone elapsed time since
//! the seat (§13.8), as it is for [`crate::peer::PeerSession`], and the listing comes from a
//! caller that knows the host's working tree.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value, json};

use crate::peer::FRAME_BUDGET;

use crate::sealed::{
    FrameKey, PeerEntry, PublicKey, Recipe, RoomState, SealedError, SessionKey,
    fresh_nonce, seal, usable_path,
};

/// The room's working tree as this host enumerates it: names, and no content (§7.1).
pub type ListingSource = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// §13.3's second bound, at this client's own numbers: how many paths a host will enumerate.
pub const MAX_LISTING_PATHS: usize = 100_000;

/// §13.3's third bound: the path bytes one listing may carry.
pub const MAX_LISTING_BYTES: usize = 4 * 1024 * 1024;

/// `CANONICAL.md` §6.1's absence charge: what every return of the host costs its count, a fixed
/// ceiling on the frames one absence can hide. 1024 returns spend the frame budget on charges
/// alone. This client has no in-process reconnect, so its one return is a reload from its store.
pub const ABSENCE_CHARGE: u64 = 1 << 21;

/// The count a reload continues from (`CANONICAL.md` §6.1): the saved one with the absence charge
/// on it, since a reload is a return, or the spent budget for a record written before the count
/// existed.
fn resumed_frames(saved: Option<u64>) -> u64 {
    saved.map_or(FRAME_BUDGET, |frames| frames.saturating_add(ABSENCE_CHARGE))
}

/// What §7.1 has a host keep together: the host key, its `issued` beside it, and the room's
/// frame count (`CANONICAL.md` §6.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedHost {
    /// The host key's 32-byte seed — the private half of the `h` the fragment carries.
    pub host_seed: [u8; 32],
    /// The highest `issued` this host has published.
    pub issued: u64,
    /// The room's frame count as this host has kept it since the mint (`CANONICAL.md` §6.1's
    /// frame budget). `None` is a record written before the count existed, which cannot say what
    /// the room has sealed and so reads as a spent budget.
    pub frames: Option<u64>,
}

/// Where a host keeps what makes it the host after a reload (§7.1, §9.1).
///
/// The client has no filesystem and this is the seam: an adapter hands one in, and a session
/// without one is a host that cannot outlive its process, which §7.1 permits and §9.1 prices —
/// a host whose key is gone can be seated in its room and can never publish a state a peer
/// accepts again.
///
/// `load` is called once, when the session is built; `save` once per state the host publishes.
/// Each half is checked where it is read rather than trusted: a seed that is not the one this
/// host signs with starts the series at `1` instead of continuing another host's.
pub trait HostStore: Send + Sync {
    fn load(&self) -> Option<PersistedHost>;
    fn save(&self, persisted: PersistedHost);
}

/// What a host is given: the key it signs states with, and where its listing comes from.
#[derive(Clone)]
pub struct HostOptions {
    /// The host key's 32-byte seed, minted where the invite is minted (§5.1).
    pub host_seed: [u8; 32],
    /// The room's working tree as this host enumerates it: names, and no content (§7.1).
    pub listing: ListingSource,
    /// Where the host key and its `issued` are kept; omitted is an in-memory host.
    pub store: Option<Arc<dyn HostStore>>,
}

impl fmt::Debug for HostOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostOptions")
            .field("host_seed", &"..")
            .field("listing", &"..")
            .field("store", &self.store.is_some())
            .finish()
    }
}

/// What asked for a state. Only `announcement` is measured by §7.1's publish-rate bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostReason {
    /// The room was minted: the first state, and the one that commits this connection's key.
    Mint,
    /// The roster changed: a `peer.joined` or a `peer.left`.
    Roster,
    /// The host's listing changed.
    Listing,
    /// A session-key announcement this host accepted.
    Announcement,
}

/// One state (or closing) this host published.
#[derive(Debug, Clone)]
pub struct HostPublication {
    /// The frame's bytes, exactly as the relay carries them.
    pub frame: Vec<u8>,
    /// The edition the frame carries.
    pub issued: u64,
    /// Whether the frame carries a new edition or re-sends the state this host holds.
    ///
    /// §7.1: an announcement whose key the state already commits publishes nothing new, so what
    /// answers it is the state its sender has not applied, re-sent unchanged.
    pub fresh: bool,
    /// The state's own value, for a `kind = 1` publication; absent for a closing.
    pub state: Option<RoomState>,
}

/// One key this host has committed, and the seat label and role it committed it under.
#[derive(Debug, Clone)]
struct SeatEntry {
    spelling: String,
    seat: String,
    role: String,
    /// Where this commitment sits in the host's order, so a replacement has a rule (§7.1).
    order: u64,
}

/// One host's producer: the room's seats, the room's listing, and the `issued` series.
///
/// A session drives it from four places — its own seat, the roster's changes, its listing's
/// changes and the announcements its receiver accepts — and takes the frame it owes from
/// [`HostProducer::publish`] and [`HostProducer::closing`].
pub struct HostProducer {
    /// The public half of the host key, which the invite's `h` carries.
    pub host_public: PublicKey,

    room_id: String,
    frame_key: FrameKey,
    host: SessionKey,
    /// The seed the host key was derived from, kept so the store can be told which host it is.
    host_seed: [u8; 32],
    listing: ListingSource,
    store: Option<Arc<dyn HostStore>>,
    renew: Duration,

    /// The seats the roster has, in the order the relay showed them.
    roster: Vec<String>,
    /// The keys this host has committed, by the key's canonical spelling.
    seats: BTreeMap<String, SeatEntry>,

    own_seat: String,
    own_key: PublicKey,

    /// The highest `issued` this host has published.
    issued: u64,
    /// `CANONICAL.md` §6.1: the room's frame count, which the host's session keeps from the mint.
    frames: u64,
    /// The count the store last holds, and the clock it was written at.
    saved_frames: u64,
    saved_at: Option<Duration>,
    /// The highest `issued` a state this host verified carried (§7.1).
    verified: u64,
    host_counter: u64,
    order: u64,
    /// The clock of the last state this host published: §7.1's publish-rate window.
    window_from: Option<Duration>,
    /// An announcement this host accepted is owed a state (§7.1).
    owed: bool,
    /// The last state published and its frame, so §7.1's answer can re-send it unchanged.
    last_state: Option<RoomState>,
    last_frame: Option<Vec<u8>>,
    /// Whether this host is still a host this room can publish into.
    standing: Standing,
    /// The first state this host could not seal, which its session reports as a fault.
    faulted: Option<String>,
}

/// What a host is to its room, as §7.1 reads it: it publishes nothing once it has left, and
/// nothing after the closing it published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    Live,
    /// §13.8's clock read as §7.1 writes it: a host that has left publishes nothing.
    Gone,
    /// §7.1's closing has gone out: the room is over.
    Closed,
}

impl HostProducer {
    /// The host key's keypair, from the seed the invite was minted with, and the `issued`
    /// series a previous session of this host left behind (§7.1).
    ///
    /// # Errors
    ///
    /// Returns an error when the platform's CSPRNG cannot be read, which is the one thing
    /// deriving the host key needs.
    #[expect(
        clippy::too_many_arguments,
        reason = "the room, the frame key, the clock, the host's own seat and key are what a producer is built from"
    )]
    pub fn new(
        room_id: &str,
        frame_key: FrameKey,
        renew: Duration,
        options: &HostOptions,
        own_seat: &str,
        own_key: PublicKey,
    ) -> Result<Self, SealedError> {
        let host = SessionKey::from_seed(options.host_seed);
        let mut producer = Self {
            host_public: host.public(),
            room_id: room_id.to_string(),
            frame_key,
            host,
            host_seed: options.host_seed,
            listing: Arc::clone(&options.listing),
            store: options.store.clone(),
            renew,
            roster: Vec::new(),
            seats: BTreeMap::new(),
            own_seat: own_seat.to_string(),
            own_key,
            issued: 0,
            frames: 0,
            saved_frames: 0,
            saved_at: None,
            verified: 0,
            host_counter: 0,
            order: 0,
            window_from: None,
            owed: false,
            last_state: None,
            last_frame: None,
            standing: Standing::Live,
            faulted: None,
        };
        // §7.1's own entry labels this connection's seat, and a state's `peers` carries only the
        // keys of the seats the roster has: a host that never put its own seat there would drop
        // every commitment it labelled with it — which is the label an announcement takes when
        // no seat of the roster is free, its own being the last resort. The commitment is what
        // §7.1 obliges and a peer cannot publish without (§13.1's step 4), so the seat goes in
        // with the key rather than the label giving way later.
        producer.seat(own_seat);
        if let Some(persisted) =
            producer.store.as_ref().and_then(|store| store.load())
            && persisted.host_seed == options.host_seed
        {
            producer.issued = persisted.issued;
            // `CANONICAL.md` §6.1: a reload is a return, so it costs the absence charge; a record
            // with no count cannot say what the room has sealed, so it reads as a spent budget and
            // the room closes at the first tick.
            producer.frames = resumed_frames(persisted.frames);
            producer.saved_frames = persisted.frames.unwrap_or(u64::MAX);
        }
        Ok(producer)
    }

    /// Whether this host has a seat and a connection key to publish an entry for.
    #[must_use]
    pub const fn ready(&self) -> bool {
        matches!(self.standing, Standing::Live)
    }

    /// The first state this host could not seal, if any.
    #[must_use]
    pub fn failure(&self) -> Option<&str> {
        self.faulted.as_deref()
    }

    /// The highest `issued` this host has published.
    #[must_use]
    pub const fn published_issued(&self) -> u64 {
        self.issued
    }

    /// The room's frame count this host continues from: `0` at a mint, the stored one on a
    /// reload (`CANONICAL.md` §6.1).
    #[must_use]
    pub const fn room_frames(&self) -> u64 {
        self.frames
    }

    /// The session's count as it moves, kept here so every save writes it beside `issued`.
    pub const fn count_frames(&mut self, count: u64) {
        self.frames = count;
    }

    /// Writes the count now, whatever the window: a session that is ending has no later tick to
    /// leave it to, and a reload must continue from the frame that ended it.
    pub fn save_frames(&mut self) {
        if self.frames != self.saved_frames {
            self.save(self.saved_at);
        }
    }

    /// `CANONICAL.md` §6.1: the count is written at least once every `awareness_renew_ms` while
    /// it moves, so a host that dies loses at most one renewal interval of it.
    pub fn flush_frames(&mut self, clock: Duration) {
        if self.frames == self.saved_frames {
            return;
        }
        if self
            .saved_at
            .is_some_and(|at| clock.saturating_sub(at) < self.renew)
        {
            return;
        }
        self.save(Some(clock));
    }

    /// The seats the roster has, which §7.1 keeps "a statement about the seats the roster has".
    #[must_use]
    pub fn roster(&self) -> &[String] {
        &self.roster
    }

    /// §7.1's own entry: exactly one key has role `host` and it is this connection's.
    pub fn seat(&mut self, seat: &str) {
        if seat.is_empty() {
            return;
        }
        self.own_seat = seat.to_string();
        self.add_seat(seat);
    }

    fn add_seat(&mut self, seat: &str) {
        if !self.roster.iter().any(|held| held == seat) {
            self.roster.push(seat.to_string());
        }
    }

    /// A `peer.joined`: the roster gains a seat, and §7.1 obliges a state.
    pub fn seat_joined(&mut self, seat: &str) {
        if seat == self.own_seat {
            self.standing = Standing::Live;
        }
        self.add_seat(seat);
    }

    /// A `peer.left`: the roster loses a seat and every key its label named goes with it (§7.1).
    ///
    /// A host that has left publishes nothing, so losing its own seat stops it rather than
    /// emptying its own entry out of one more state.
    pub fn seat_left(&mut self, seat: &str) {
        if seat == self.own_seat {
            self.standing = Standing::Gone;
        }
        self.roster.retain(|held| held != seat);
        self.seats.retain(|_, entry| entry.seat != seat);
    }

    /// §7.1: commit every announcement accepted, with the seat label this host believes.
    ///
    /// A declaration of `guest` or `viewer` is honoured when the key is first committed — it is
    /// the one statement about a peer's role that comes from that peer's own key — and a
    /// declaration changes nothing for a key the state already commits.
    pub fn announcement(&mut self, key: PublicKey, declared: Option<&str>) {
        let spelling = key.encode();
        if self.seats.contains_key(&spelling) {
            self.owed = true;
            return;
        }
        self.commit(&spelling, declared);
        self.owed = true;
    }

    /// A state this host verified, whose edition its own series must stay above (§7.1).
    pub const fn verified_state(&mut self, issued: u64) {
        if issued > self.verified {
            self.verified = issued;
        }
    }

    /// The state frame this host owes at `clock`, or `None` when it owes none.
    ///
    /// `Mint`, `Roster` and `Listing` are the obligations §7.1's rate bound does not touch: the
    /// roster is the server's word and bounded by it, and the listing is the host's own.
    /// `Announcement` is the one the bound is about, and it takes the promise §7.1 leaves a
    /// host: a later announcement is answered **at once** where no state has gone out for one
    /// in the current window, and folded into the end of that window where one has — so a peer
    /// that mints keys without bound obliges at most one state a window, and a joiner is not
    /// left unable to publish for a window it has no reason to wait for.
    pub fn publish(
        &mut self,
        clock: Duration,
        reason: HostReason,
    ) -> Option<HostPublication> {
        if !self.ready() {
            return None;
        }
        if reason == HostReason::Announcement
            && (!self.owed || !self.window_open(clock))
        {
            return None;
        }
        self.owed = false;
        let listing = self.room_listing();
        let peers = self.peer_entries();
        let resent = match (&self.last_state, &self.last_frame) {
            // §7.1: a state this host verified above its own is one its peers hold, so
            // re-sending the edition it published itself would be a frame they all refuse
            // `stale_issued` — and the fresh state it would otherwise publish above that
            // edition is what a joiner needs.
            (Some(held), Some(frame))
                if held.issued >= self.verified
                    && same_paths(&held.listing, &listing)
                    && same_peers(&held.peers, &peers) =>
            {
                Some((frame.clone(), held.clone()))
            }
            _ => None,
        };
        if let Some((frame, held)) = resent {
            self.window(clock, reason);
            return Some(HostPublication {
                frame,
                issued: held.issued,
                fresh: false,
                state: Some(held),
            });
        }
        let issued = self.next_issued();
        let frame = self.seal_state(issued, &listing, &peers)?;
        self.commit_series(issued, Some(clock));
        let state = RoomState {
            issued,
            listing,
            peers,
        };
        self.last_state = Some(state.clone());
        self.last_frame = Some(frame.clone());
        self.window(clock, reason);
        Some(HostPublication {
            frame,
            issued,
            fresh: true,
            state: Some(state),
        })
    }

    /// §7.1's rate bound is the announcements' own: only an answer opens a window.
    fn window(&mut self, clock: Duration, reason: HostReason) {
        if reason == HostReason::Announcement {
            self.window_from = Some(clock);
        }
    }

    /// §7.1's closing: the host's statement that the room is over, at an `issued` above every
    /// state it has published.
    ///
    /// Only the host key signs one, and a receiver applies one only when it already holds a
    /// verified state below it (§13.10).
    pub fn closing(&mut self) -> Option<HostPublication> {
        if !self.ready() {
            return None;
        }
        let issued = self.next_issued();
        let plaintext = canonical(&json!({"closing": true, "issued": issued}));
        let frame = self.seal(2, &plaintext)?;
        self.commit_series(issued, None);
        self.standing = Standing::Closed;
        Some(HostPublication {
            frame,
            issued,
            fresh: true,
            state: None,
        })
    }

    /// §7.1: `issued` above every state published, and above one it has verified.
    fn next_issued(&self) -> u64 {
        self.issued.max(self.verified).saturating_add(1)
    }

    /// `clock` is the publication's own, and a save records it: the renewal-window batching in
    /// [`Self::flush_frames`] measures from the last write of any kind. A closing has no clock of
    /// its own and keeps the last one, which is harmless because nothing is published after it.
    fn commit_series(&mut self, issued: u64, clock: Option<Duration>) {
        self.issued = issued;
        self.save(clock.or(self.saved_at));
    }

    fn save(&mut self, clock: Option<Duration>) {
        let Some(store) = &self.store else {
            return;
        };
        store.save(PersistedHost {
            host_seed: self.host_seed,
            issued: self.issued,
            frames: Some(self.frames),
        });
        self.saved_frames = self.frames;
        self.saved_at = clock;
    }

    /// §7.1's rate bound: the window the last published state opened.
    fn window_open(&self, clock: Duration) -> bool {
        self.window_from
            .is_none_or(|from| clock.saturating_sub(from) >= self.renew)
    }

    fn commit(&mut self, spelling: &str, declared: Option<&str>) {
        let seat = self.label();
        // §7.1's *at most one key per seat* is about the seats the roster has. The host's own
        // seat is the last resort of the label above, not one a second key can take over:
        // evicting the key already there would drop a commitment §7.1 obliges, in exchange for
        // nothing.
        if seat != self.own_seat {
            self.seats.retain(|_, entry| entry.seat != seat);
        }
        let entry = SeatEntry {
            spelling: spelling.to_string(),
            seat,
            role: declared.unwrap_or("guest").to_string(),
            order: self.order,
        };
        let _ = self.seats.insert(spelling.to_string(), entry);
        self.order = self.order.saturating_add(1);
    }

    /// The seat §7.1 has this host label a newly committed key with: the announcer's own seat
    /// where it can tell which seat that is, and otherwise any seat the roster names that
    /// carries no key yet.
    ///
    /// Nothing on the wire ties a key to a seat — an announcement names no peer and the relay
    /// says nothing about which connection sent one (§13.4) — so a host can tell in no case
    /// this client can see, and the label is the belief §7.1 calls it rather than a fact.
    ///
    /// A roster whose every seat already carries a key is §7.1's *commit every announcement*
    /// and its *at most one key per seat* meeting, and that section does not say which seat
    /// gives way. This host replaces the key it has held longest, which is the one most likely
    /// to belong to a connection the roster no longer has. A key that arrives before its seat
    /// does leaves no other seat to replace, and the label is then the host's own, which §7.1
    /// permits by name (*its own included*) and which it obliges over withholding the
    /// commitment. That seat is the roster's from the start, so the commitment is carried
    /// rather than dropped; the price is the one §7.1's *at most one key per seat* names, two
    /// keys under this host's seat until the announcer's own seat is known.
    fn label(&self) -> String {
        let taken: BTreeSet<String> = self
            .seats
            .values()
            .filter(|entry| self.roster.contains(&entry.seat))
            .map(|entry| entry.seat.clone())
            .chain([self.own_seat.clone()])
            .collect();
        let free = self.roster.iter().find(|seat| !taken.contains(*seat));
        free.map_or_else(|| self.fallback_seat(), Clone::clone)
    }

    /// The seat a host labels a key with when every seat the roster names already carries one:
    /// the key it has held longest, because that is the one most likely to belong to a
    /// connection the roster no longer has — and its own seat when there is no other.
    fn fallback_seat(&self) -> String {
        let oldest = self
            .seats
            .values()
            .filter(|entry| {
                entry.seat != self.own_seat && self.roster.contains(&entry.seat)
            })
            .min_by_key(|entry| entry.order);
        oldest.map_or_else(|| self.own_seat.clone(), |entry| entry.seat.clone())
    }

    /// `peers` as §7.1 writes it: the keys of the seats the roster has, ascending by the key's
    /// canonical spelling, and the host's own connection's key as the one `host` entry.
    fn peer_entries(&self) -> BTreeMap<String, PeerEntry> {
        let mut entries: Vec<&SeatEntry> = self
            .seats
            .values()
            .filter(|entry| self.roster.contains(&entry.seat))
            .collect();
        entries.sort_by(|left, right| left.spelling.cmp(&right.spelling));
        let mut peers: BTreeMap<String, PeerEntry> = BTreeMap::new();
        for entry in entries {
            let _ = peers.insert(
                entry.spelling.clone(),
                PeerEntry {
                    peer_id: entry.seat.clone(),
                    role: entry.role.clone(),
                },
            );
        }
        let _ = peers.insert(
            self.own_key.encode(),
            PeerEntry {
                peer_id: self.own_seat.clone(),
                role: "host".to_string(),
            },
        );
        peers
    }

    /// The room's listing: §5's path rule applied, ascending by UTF-16 code unit (§2.7), and
    /// bounded, because a listing is one sealed frame and one that does not fit never arrives
    /// at all (§13.3).
    fn room_listing(&self) -> Vec<String> {
        let mut bounded = Listing::default();
        // `position` stops at the first path the listing has no room for, which is the bound
        // §13.3 asks for rather than an error.
        let _ = (self.listing)()
            .into_iter()
            .position(|path| !bounded.add(path));
        bounded
            .paths
            .sort_by(|left, right| utf16_order(left, right));
        bounded.paths
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "a state is its issued edition, its listing and its peers, and the seal needs the receiver"
    )]
    fn seal_state(
        &mut self,
        issued: u64,
        listing: &[String],
        peers: &BTreeMap<String, PeerEntry>,
    ) -> Option<Vec<u8>> {
        let peer_values: Map<String, Value> = peers
            .iter()
            .map(|(key, entry)| {
                (
                    key.clone(),
                    json!({"peer_id": entry.peer_id, "role": entry.role}),
                )
            })
            .collect();
        let members: Map<String, Value> = [
            ("issued".to_string(), json!(issued)),
            ("listing".to_string(), json!(listing)),
            ("peers".to_string(), Value::Object(peer_values)),
        ]
        .into_iter()
        .collect();
        self.seal(1, &canonical(&Value::Object(members)))
    }

    /// One frame, signed by the host key, at this host's own counter under it.
    ///
    /// `None` here is a fault and not a decision — a CSPRNG that will not read, or an AEAD that
    /// refuses its own inputs — so it is kept where the session can report it: a host that said
    /// nothing would look like one with nothing to publish.
    fn seal(&mut self, kind: u64, plaintext: &[u8]) -> Option<Vec<u8>> {
        let Ok(nonce) = fresh_nonce() else {
            self.note_fault(kind);
            return None;
        };
        let counter = self.host_counter.saturating_add(1);
        let sealed = {
            let recipe = Recipe {
                room_id: &self.room_id,
                frame_key: &self.frame_key,
                kind,
                epoch: 0,
                counter,
                nonce,
                signer: &self.host,
            };
            seal(&recipe, plaintext).map(|envelope| envelope.bytes())
        };
        let Ok(bytes) = sealed else {
            self.note_fault(kind);
            return None;
        };
        self.host_counter = counter;
        Some(bytes)
    }

    fn note_fault(&mut self, kind: u64) {
        let what = if kind == 2 { "closing" } else { "state" };
        let _ = self.faulted.get_or_insert_with(|| {
            format!("a {what} frame could not be sealed")
        });
    }
}

/// A JSON value's canonical bytes. `CANONICAL.md` §2 writes an object with its members
/// ascending, which `serde_json`'s own map does (it is a `BTreeMap`).
fn canonical(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}

/// Whether two listings are the same sequence, which decides §7.1's re-send.
fn same_paths(left: &[String], right: &[String]) -> bool {
    left == right
}

/// Whether two `peers` maps say the same thing, key for key and member for member.
fn same_peers(
    left: &BTreeMap<String, PeerEntry>,
    right: &BTreeMap<String, PeerEntry>,
) -> bool {
    left == right
}

/// Ascending order of UTF-16 code units, the unit `PROTOCOL.md` §2.7 orders a listing by.
fn utf16_order(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

/// The paths one state will carry: §5's rule applied, deduplicated, and bounded, because a
/// listing is one sealed frame and one that does not fit never arrives at all (§13.3).
#[derive(Default)]
struct Listing {
    paths: Vec<String>,
    seen: BTreeSet<String>,
    bytes: usize,
}

impl Listing {
    /// Takes one path, and says whether the listing has room for any more.
    fn add(&mut self, path: String) -> bool {
        if !usable_path(&path) || self.seen.contains(&path) {
            return true;
        }
        let size = path.len();
        if self.paths.len() >= MAX_LISTING_PATHS
            || self.bytes.saturating_add(size) > MAX_LISTING_BYTES
        {
            return false;
        }
        let _ = self.seen.insert(path.clone());
        self.bytes = self.bytes.saturating_add(size);
        self.paths.push(path);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::sealed::{Envelope, RoomKey, opens};

    const ROOM: &str = "R7f3a2c19";

    fn room_key() -> RoomKey {
        RoomKey([7; 32])
    }

    fn our_key() -> SessionKey {
        SessionKey::from_seed([5; 32])
    }

    fn peer_key() -> SessionKey {
        SessionKey::from_seed([11; 32])
    }

    /// A second peer's, for a state that follows the first one's announcement.
    fn other_key() -> SessionKey {
        SessionKey::from_seed([13; 32])
    }

    /// The host key of the invite's fragment, derived from the seed a host is given.
    fn host_seed() -> [u8; 32] {
        [3; 32]
    }

    /// A store that keeps the last thing it was handed, which is what a host's persistence does.
    #[derive(Default)]
    struct Memory {
        saved: Mutex<Option<PersistedHost>>,
    }

    impl HostStore for Memory {
        fn load(&self) -> Option<PersistedHost> {
            *self.saved.lock().unwrap()
        }

        fn save(&self, persisted: PersistedHost) {
            *self.saved.lock().unwrap() = Some(persisted);
        }
    }

    fn listing() -> ListingSource {
        Arc::new(|| vec!["README.md".to_string(), "src/main.rs".to_string()])
    }

    fn producer(store: Option<Arc<dyn HostStore>>) -> HostProducer {
        let options = HostOptions {
            host_seed: host_seed(),
            listing: listing(),
            store,
        };
        HostProducer::new(
            ROOM,
            room_key().frame_key(ROOM),
            Duration::from_millis(300),
            &options,
            "p-self",
            our_key().public(),
        )
        .unwrap()
    }

    fn state_of(publication: &HostPublication) -> RoomState {
        let envelope = Envelope::parse(&publication.frame).unwrap();
        let plaintext =
            opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
        serde_json::from_slice(&plaintext).unwrap()
    }

    fn millis(count: u64) -> Duration {
        Duration::from_millis(count)
    }

    #[test]
    fn the_mint_state_commits_this_connections_key_as_the_only_host_entry() {
        let mut host = producer(None);
        let mint = host.publish(Duration::ZERO, HostReason::Mint).unwrap();
        assert!(mint.fresh);
        assert_eq!(mint.issued, 1, "§7.1's first state carries `1`");
        let state = state_of(&mint);
        assert_eq!(state.listing, ["README.md", "src/main.rs"]);
        assert_eq!(
            state.peers[&our_key().public().encode()].role,
            "host",
            "the host's own entry is the one `host` entry"
        );
        assert_eq!(state.peers[&our_key().public().encode()].peer_id, "p-self");
    }

    #[test]
    fn an_announcement_is_committed_under_a_free_seat_and_answered_once_a_window()
     {
        let mut host = producer(None);
        host.seat_joined("p-guest");
        let _ = host.publish(Duration::ZERO, HostReason::Roster);
        assert!(
            host.publish(millis(1), HostReason::Announcement).is_none(),
            "nothing is owed before an announcement is accepted"
        );

        host.announcement(peer_key().public(), Some("viewer"));
        let answered =
            host.publish(millis(2), HostReason::Announcement).unwrap();
        let state = state_of(&answered);
        assert_eq!(
            state.peers[&peer_key().public().encode()].role,
            "viewer",
            "the declaration the peer's own key made is honoured"
        );
        assert_eq!(
            state.peers[&peer_key().public().encode()].peer_id,
            "p-guest",
            "the label is the seat the roster names and carries no key yet"
        );

        // §7.1's rate bound: a second announcement inside the window waits it out rather than
        // obliging a state a frame.
        host.announcement(peer_key().public(), None);
        assert!(host.publish(millis(3), HostReason::Announcement).is_none());
        assert!(
            host.publish(millis(2 + 300), HostReason::Announcement)
                .is_some()
        );
    }

    #[test]
    fn an_announcement_whose_key_the_state_commits_re_sends_it_unchanged() {
        let mut host = producer(None);
        let mint = host.publish(Duration::ZERO, HostReason::Mint).unwrap();
        host.announcement(our_key().public(), None);
        let again =
            host.publish(millis(500), HostReason::Announcement).unwrap();
        assert!(!again.fresh, "§7.1 re-sends the state it already holds");
        assert_eq!(again.issued, mint.issued);
        assert_eq!(again.frame, mint.frame, "the bytes it received, unchanged");
    }

    #[test]
    fn the_store_carries_the_issued_series_and_a_returning_host_continues_above_it()
     {
        let store: Arc<Memory> = Arc::new(Memory::default());
        let kept: Arc<dyn HostStore> = Arc::clone(&store) as Arc<dyn HostStore>;
        let mut host = producer(Some(kept));
        let mint = host.publish(Duration::ZERO, HostReason::Mint).unwrap();
        let saved = store.load().unwrap();
        assert_eq!(saved.issued, mint.issued);
        assert_eq!(saved.host_seed, host_seed());

        let closing = host.closing().unwrap();
        assert!(closing.issued > mint.issued);
        assert_eq!(
            store.load().unwrap().issued,
            closing.issued,
            "the closing moves the series too"
        );

        // A host that comes back with the key and the series continues it: a state at or below
        // the room's edition is refused by every peer and nothing carries that refusal back.
        let returning =
            producer(Some(Arc::clone(&store) as Arc<dyn HostStore>));
        assert_eq!(returning.published_issued(), closing.issued);
        assert!(returning.published_issued() > 0);
    }

    #[test]
    fn a_store_holding_another_hosts_key_leaves_the_series_at_its_own_start() {
        let store: Arc<Memory> = Arc::new(Memory::default());
        store.save(PersistedHost {
            host_seed: [9; 32],
            issued: 42,
            frames: Some(7),
        });
        let host = producer(Some(store));
        assert_eq!(
            host.published_issued(),
            0,
            "a seed that is not this host's starts the series at §7.1's `1`"
        );
    }

    /// §7.1 obliges the commitment of every accepted announcement, and the state that carries
    /// it is the only thing that lets that peer publish at all (§13.1's step 4): a commitment
    /// the state leaves out is a peer that never joins. The label is the belief §7.1 calls it
    /// and the commitment is what it obliges, so a key the host has accepted is in the state
    /// whatever seat — its own included — the host had to label it with.
    #[test]
    fn an_announcement_accepted_before_its_seat_joins_is_still_committed() {
        let mut host = producer(None);
        let mint = host.publish(Duration::ZERO, HostReason::Mint).unwrap();

        // The announcement arrives before the `peer.joined` that names its seat, so no seat of
        // the roster is free and the label is the host's own.
        host.announcement(peer_key().public(), None);
        let answered =
            host.publish(millis(500), HostReason::Announcement).unwrap();
        assert!(answered.issued > mint.issued);
        let state = state_of(&answered);
        assert!(
            state.peers.contains_key(&peer_key().public().encode()),
            "the commitment is in the state that answers the announcement: {:?}",
            state.peers.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            state.peers[&peer_key().public().encode()].role,
            "guest",
            "and carries the declaration the announcer's own key made"
        );
        assert_eq!(
            state.peers[&peer_key().public().encode()].peer_id,
            "p-self",
            "the label is the host's own, the last seat §7.1 lets it fall back to"
        );
        assert_eq!(
            host.roster().to_vec(),
            ["p-self"],
            "which is a seat the state keeps: the producer's roster holds its own"
        );

        // The seat joins afterwards, and the key already committed is in the state that
        // follows too — a key the state had dropped would not come back with its seat.
        host.seat_joined("p-guest");
        host.announcement(other_key().public(), None);
        let after = host
            .publish(millis(1000), HostReason::Announcement)
            .unwrap();
        let state = state_of(&after);
        assert!(
            state.peers.contains_key(&peer_key().public().encode()),
            "the key accepted before its seat joined is still committed: {:?}",
            state.peers.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            state.peers[&other_key().public().encode()].peer_id,
            "p-guest",
            "the seat the roster gained is the one a later key is labelled with"
        );
    }

    #[test]
    fn the_closing_is_above_every_state_and_the_host_publishes_nothing_after_it()
     {
        let mut host = producer(None);
        let mint = host.publish(Duration::ZERO, HostReason::Mint).unwrap();
        let closing = host.closing().unwrap();
        assert!(closing.issued > mint.issued);
        assert_eq!(closing.issued, 2, "the first `issued` a closing can carry");
        assert!(host.publish(millis(1), HostReason::Listing).is_none());
        assert!(host.closing().is_none());
    }
}
