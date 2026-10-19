//! §13.4's attribution read back: the role an applied state gives a key, and the
//! eligibility §13.6's reciprocal and readiness accounting turns on.

use super::PeerSession;
use crate::sealed::KeyId;
use crate::sealed::PublicKey;
use std::collections::BTreeSet;

impl PeerSession {
    /// Whether an applied state commits this connection's own session key.
    pub(super) fn commits_ours(&self) -> bool {
        self.reader
            .committed
            .values()
            .any(|peer| peer.key == self.session.public())
    }

    /// The role the applied state gives this connection's own key (§13.4).
    pub(super) fn role(&self) -> Option<&str> {
        self.reader.role_of_key(self.session.public())
    }

    /// Whether §13.1's step 4 lets this client publish anything but its announcement.
    ///
    /// §13.10: a session that has ended publishes nothing, whichever ending reached it, so
    /// the ending is checked here rather than at each caller.
    pub(super) fn may_publish(&self) -> bool {
        self.ending.is_none() && self.state_held() && self.commits_ours()
    }

    /// §13.6: whether the applied state begins a new continuous writer eligibility for this
    /// connection's own key, which owes one coalesced readiness request.
    ///
    /// An already-handshaken key that re-enters writer eligibility — a `viewer` promotion or an
    /// absent interval — owes one. The first commitment owes §13.1's immediate initial request
    /// instead, which `handshake_once` sends, and an uninterrupted eligibility owes nothing.
    /// Eligibility ending before the request is queued retires it, leaving unrelated remote
    /// reciprocal and refusal obligations alone.
    pub(super) fn note_local_eligibility(&mut self, role: Option<&str>) {
        if !matches!(role, Some("host" | "guest")) {
            self.local_readiness_pending = false;
            return;
        }
        if self.handshaken_at.is_some()
            && !matches!(self.role_seen.as_deref(), Some("host" | "guest"))
        {
            self.local_readiness_pending = true;
        }
    }

    /// Whether the applied state gives this connection's own key a publishing role (§13.4).
    pub(super) fn is_writer(&self) -> bool {
        matches!(self.role(), Some("host" | "guest"))
    }

    /// Whether the applied state commits `key` as a writer (§13.6's eligibility).
    fn is_writer_key(&self, key: PublicKey) -> bool {
        matches!(self.reader.role_of_key(key), Some("host" | "guest"))
    }

    /// §13.6: arms one reciprocal request for the first accepted `SyncStep1` from a continuously
    /// committed writer key.
    ///
    /// The identity is the full key whose signature verified, never its id (§6.1). A request
    /// from a `viewer` is not one — a viewer sends no content, so reciprocating would solicit
    /// nothing — and a key already served keeps its mark until its eligibility ends, which is
    /// what stops the request from echoing for ever between two peers.
    ///
    /// Activation is the **remote** writer's eligibility and nothing local: a connection that is
    /// itself uncommitted, or committed only as a `viewer`, still records the obligation, and the
    /// publication gate withholds the request until it may publish. Gating activation on local
    /// commitment would drop the obligation outright, which is not a delay.
    pub(super) fn arm_reciprocal(&mut self, verified: PublicKey) {
        if !self.is_writer_key(verified) {
            return;
        }
        if self.reciprocal_served.insert(verified) {
            let _ = self.reciprocal_pending.insert(verified);
        }
    }

    /// §13.6: retires the reciprocal accounting for keys the applied state no longer commits as
    /// writers, so a later promotion of one may arm once more while a continuously committed key
    /// keeps its served mark.
    pub(super) fn prune_reciprocal(&mut self) {
        let writers: BTreeSet<PublicKey> = self
            .reader
            .committed
            .values()
            .filter(|peer| peer.role == "host" || peer.role == "guest")
            .map(|peer| peer.key)
            .collect();
        self.reciprocal_served.retain(|key| writers.contains(key));
        self.reciprocal_pending.retain(|key| writers.contains(key));
    }

    /// The seat the applied state's `host` entry labels, in the order §6.1 reads two `host`
    /// keys: the one whose key comes first in UTF-16 code-unit order.
    pub(super) fn host_seat(&self) -> Option<&str> {
        self.reader
            .committed
            .values()
            .find(|peer| peer.role == "host")
            .map(|peer| peer.peer_id.as_str())
    }

    /// The canonical spelling of a key id where an applied state names one, and its id where
    /// nothing does (§13.4).
    pub(super) fn spelling(&self, id: KeyId) -> String {
        self.reader
            .committed
            .values()
            .find(|peer| peer.key.id() == id)
            .map_or_else(|| id.hex(), |peer| peer.key.encode())
    }
}
