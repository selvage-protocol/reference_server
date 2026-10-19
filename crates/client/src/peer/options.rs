//! What a session is opened with, and the host's producer half built from it (§7.1).

use crate::host::HostOptions;
use crate::host::HostProducer;
use crate::sealed::FrameKey;
use crate::sealed::PublicKey;
use crate::sealed::RoomKey;
use crate::sealed::SealedError;
use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

/// What a session built to be a host is told when the handshake gave it no seat: §7.1's state
/// labels its own connection's seat, and there is nothing to write without one.
const HOST_NEEDS_SEAT: &str =
    "a host session publishes its own seat, and the handshake gave it none";

/// What a session is opened with: the invite's two keys, the session's clock, and the seats
/// the server has shown.
#[derive(Clone)]
pub struct PeerOptions {
    pub room_id: String,
    pub room_key: RoomKey,
    /// The host key the invite's `h` carries: the room's root of trust, and the only key that
    /// verifies a `kind = 1` state or a `kind = 2` closing.
    pub host_key: PublicKey,
    /// `awareness_renew_ms` (`PROTOCOL.md` §8.2): the clock an uncommitted announcement is
    /// re-sent on and a held set is renewed on, and the tick a lease (§13.7) and an expired
    /// remote awareness state (§8.2) are checked on.
    pub renew: Duration,
    /// `awareness_expire_ms`: the window a remote awareness state is forgotten after
    /// (§8.2), the host-away window (§13.8) and, read from the seat, the no-state window
    /// (§13.3).
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
    /// The room's frame budget (`CANONICAL.md` §6.1), [`FRAME_BUDGET`] when `None`. **A test
    /// seam**: a live session has no reason to set it, and a smaller one only ends a room sooner.
    pub frame_budget: Option<u64>,
}

/// `CANONICAL.md` §6.1's frame budget: half of SP 800-38D's 2³² bound on one key with random
/// nonces, which is the room's and not one sender's, so that a client that missed frames the
/// relay dropped still stops well short of it.
pub const FRAME_BUDGET: u64 = 1 << 31;

/// The same rule as [`PeerInvite`]'s: the room key, the host key and the two private seeds are
/// not printed. A session's options are built from an invite, and §5.1 forbids what they carry
/// from reaching a log as much as it forbids it reaching a request.
impl fmt::Debug for PeerOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerOptions")
            .field("room_id", &self.room_id)
            .field("room_key", &"<redacted>")
            .field("host_key", &self.host_key.id().hex())
            .field("renew", &self.renew)
            .field("expire", &self.expire)
            .field("seat", &self.seat)
            .field("roster", &self.roster)
            .field(
                "fixed_session_key",
                &self.fixed_session_key.map(|_| "<redacted>"),
            )
            .field("declared_role", &self.declared_role)
            .field("awareness_client_id", &self.awareness_client_id)
            .field("host", &self.host.as_ref().map(|_| "<redacted>"))
            .field("frame_budget", &self.frame_budget)
            .finish()
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
pub(super) fn host_producer(
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
