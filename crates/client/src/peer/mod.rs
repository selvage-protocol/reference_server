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
use std::time::Duration;

use yrs::Doc;
use yrs::block::ClientID;
use yrs::sync::Awareness;

use crate::host::HostProducer;
use crate::sealed::{FrameKey, KeyId, PublicKey, Reader, SessionKey};

mod clocks;
mod deliver;
mod events;
mod holds;
mod invite;
mod options;
mod plaintext;
mod publish;
mod read;
mod report;
mod roles;
mod speculation;

pub use invite::{PeerInvite, wire_address};
pub use options::{FRAME_BUDGET, PeerOptions};
pub use report::{Applied, Dropped, Ending, Outcome, Published};

use speculation::Unsent;

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
    /// §8.2's receiver bookkeeping: the local monotone clock, the one every argument named
    /// `clock` carries, at which each **remote** awareness client id's state was last
    /// actually applied. A tick forgets an id whose stamp is `awareness_expire_ms` old, and an
    /// entry that was ignored because its clock was not above the one held does not stamp.
    ///
    /// This connection's own id is never here: §8.2 has a client never expire itself.
    awareness_seen: BTreeMap<ClientID, Duration>,
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
    /// changes out under this connection's key. `speculative` marks an edit made while no
    /// applied state gave this key a role yet, which a `viewer` resolution discards.
    unsent: Vec<Unsent>,
    /// A clean replica, present only while UNKNOWN-role local edits need a rollback (§13.9).
    ///
    /// A genuine second document built from an encoded update, never `Doc::clone` (which is a
    /// handle to the same store): this is what a `viewer` resolution adopts in place of the
    /// replica that holds the speculative edits.
    baseline: Option<Doc>,
    /// A typed effect of the owned replica, for the relay's §13.5 text surface: bumped whenever
    /// this session replaces the document a consumer was reading text from (§13.9's `viewer`
    /// resolution adopting the clean baseline). A state-driven rollback can then be reported
    /// even when the path set and listing are unchanged, without re-reading or comparing text.
    replica_epoch: u64,
    /// The committed writer keys whose first accepted `SyncStep1` this connection has served
    /// with its own request (§13.6), by the full key that verified.
    reciprocal_served: BTreeSet<PublicKey>,
    /// The committed writer keys whose reciprocal request is owed and not yet sealed and queued.
    reciprocal_pending: BTreeSet<PublicKey>,
    /// Readiness owed for this local key's renewed continuous writer eligibility (§13.6).
    local_readiness_pending: bool,
    /// The role the last applied state gave this connection's own key, for continuous
    /// eligibility: an already-handshaken key that re-enters writer eligibility owes one request.
    role_seen: Option<String>,
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
    /// `CANONICAL.md` §6.1's count: every binary frame this session was delivered and every
    /// frame it sealed, across reconnects, because the frame key it is counted against outlives
    /// a socket.
    room_frames: u64,
    frame_budget: u64,
    applied: Vec<Applied>,
    dropped: Vec<Dropped>,
    ignored: Vec<u64>,
    /// The frames refused because they were a second state at an edition this client already
    /// holds (§13.3). The reason is `stale_issued` like any other stale state; this is the
    /// local annotation that says which of the two it was.
    conflicts: Vec<u64>,
    ending: Option<Ending>,
    mutation: Option<String>,
    /// The first thing that went wrong on the way *out* — a CSPRNG that would not read, an
    /// AEAD that refused its own inputs. A session cannot refuse to publish without somewhere
    /// to say so, and a driver that never looks is a driver publishing nothing.
    fault: Option<String>,
}

#[cfg(test)]
mod tests;
