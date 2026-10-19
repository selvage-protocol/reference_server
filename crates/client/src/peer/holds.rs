//! §13.7: the paths this client offers, the lease a peer's holds live under, and the
//! expiry that forgets them.

use super::PeerSession;
use super::plaintext::canonical;
use super::report::Published;
use crate::sealed::KeyId;
use serde_json::json;
use std::time::Duration;

impl PeerSession {
    /// §13.7: only an accepted holds message renews a lease, and nothing else from the same
    /// peer does.
    pub(super) fn renew_lease(
        &mut self,
        clock: Duration,
        sender: Option<KeyId>,
    ) {
        if let Some(id) = sender {
            let _ = self.leases.insert(id, clock);
        }
    }

    /// §13.7: the whole held set, renewed on the client's own clock and published at once
    /// when it changes. Rising from the tick and from nothing a peer sends is what lets a
    /// seated, idle peer keep its holds.
    pub(super) fn announce_holds(&mut self, clock: Duration) {
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

    /// §13.7's expiry: a peer's whole held set is forgotten once its lease has lapsed.
    ///
    /// The text has this checked on the renewal tick, and a client whose tick *is* its renewal
    /// tick checks there; a tick that comes more often forgets sooner, which is inside the
    /// `awareness_expire_ms + awareness_renew_ms` the text bounds the delay by and never
    /// inside the `MUST forget ... after awareness_expire_ms` it states. An expiry is not a
    /// refusal — no frame is dropped, the peer is not gone and nothing is reported anywhere —
    /// which is why the only observable is the set becoming empty.
    pub(super) fn expire_leases(&mut self, clock: Duration) {
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

    /// §13.7: the roster is the authority on who is present, so a key whose entry labels a
    /// seat that has left loses its holds at once rather than with its lease. A seat the
    /// roster never knew is not this rule: it is left to its lease.
    pub(super) fn drop_departed_holds(&mut self) {
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

    /// Opens a path: this client offers it, and its whole held set changes (§13.7).
    ///
    /// The change is announced here rather than left to the caller's next tick: §13.7 asks a
    /// holder to re-announce at once when its set changes, and the tick's period is the room's
    /// own `awareness_renew_ms` — fifteen seconds at the reference defaults, which is not
    /// "at once". Nothing goes out while §13.1's step 4 withholds everything but the
    /// announcement, and nothing goes out for a set that did not change.
    pub fn open(&mut self, clock: Duration, path: &str) {
        let _ = self.held.insert(path.to_string());
        self.announce_holds(clock);
    }

    /// Releases every path. §13.7 asks for the empty set rather than for silence, so the room
    /// learns in one hop instead of waiting out a lease — and it learns it here, for the reason
    /// [`Self::open`] gives.
    pub fn release(&mut self, clock: Duration) {
        self.held.clear();
        self.announce_holds(clock);
    }

    /// Holds this one path and nothing else, announced once (§13.7).
    ///
    /// A holds message carries the whole set and no delta, so a caller replacing the set with
    /// one path cannot say so with [`Self::release`] and [`Self::open`]: each announces the
    /// change it makes, and the room would see an empty set between them.
    pub fn hold_only(&mut self, clock: Duration, path: &str) {
        self.held.clear();
        let _ = self.held.insert(path.to_string());
        self.announce_holds(clock);
    }
}
