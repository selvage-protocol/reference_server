//! The caller's tick: §13.7's lease, §8.2's awareness expiry, §13.8's host-away
//! window, §13.3's no-state window, and §13.11's mutation census.

use super::PeerSession;
use super::report::Ending;
use crate::host::HostReason;
use crate::sealed::Guard;
use crate::sealed::SealedError;
use std::time::Duration;
use yrs::block::ClientID;

impl PeerSession {
    /// The clocks, on the caller's tick: §13.7's renewal and expiry, §8.2's awareness expiry,
    /// §13.8's host-away window and §13.3's no-state window.
    ///
    /// The announcement of §13.1's step 4 goes out here too, at the first tick and whenever a
    /// state has not committed this key since — so a driver has one path to publish it and
    /// nothing has to remember to call it at the join.
    pub fn tick(&mut self, clock: Duration) {
        if let Some(host) = self.host.as_mut() {
            host.flush_frames(clock);
        }
        self.expire_leases(clock);
        self.expire_awareness(clock);
        self.refresh_host_away(clock, false);
        if self.ending.is_some() {
            return;
        }
        if self.budget_spent() {
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

    pub(super) fn mutating(&self, name: &str) -> bool {
        self.mutation.as_deref() == Some(name)
    }

    /// §8.2's expiry, beside §13.7's: a remote awareness state whose last **applied** entry is
    /// `awareness_expire_ms` old is forgotten. The renewal tick is where this is checked, so a
    /// state is forgotten at the first tick after `last seen + awareness_expire_ms` and never
    /// sooner, which is the `awareness_expire_ms + awareness_renew_ms` the text bounds it by.
    ///
    /// The removal is `yrs`'s own, which leaves the id a tombstone: a renewal at the clock the
    /// dropped state carried and one at the clock above it are both ignored (`NOTES.md` §A.11),
    /// and a renewal above both makes the peer visible again. An expiry is not a refusal and not
    /// a departure — no frame is dropped, the peer is not gone — so the only observable is the
    /// state going.
    fn expire_awareness(&mut self, clock: Duration) {
        let lapsed: Vec<ClientID> = self
            .awareness_seen
            .iter()
            .filter(|(_, at)| clock.saturating_sub(**at) >= self.expire)
            .map(|(id, _)| *id)
            .collect();
        for id in lapsed {
            let _ = self.awareness_seen.remove(&id);
            self.awareness.remove_state(id);
        }
    }

    /// §13.8's host-away clock: armed while the applied state's `host` entry labels a seat
    /// the roster does not have, disarmed when it labels one the roster has. `restart` is
    /// what a new state does to a clock that is already running.
    pub(super) fn refresh_host_away(&mut self, clock: Duration, restart: bool) {
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

    /// Drops the awareness state published under `client_id`, which §8.4 asks for when the
    /// peer that last claimed the id has left and no seated peer still claims it. The caller
    /// holds the claims (the roster's `PeerInfo`), so it is the caller that decides which id.
    ///
    /// The state is removed the way y-protocols removes one, with its clock moved past the
    /// last one seen, so a stale copy still in flight does not bring it back. This
    /// connection's own id is never dropped, and an id with no state is left as it is.
    pub fn forget_awareness(&mut self, client_id: u64) {
        let id = ClientID::new(client_id);
        // §8.4's drop and §8.2's expiry are the same bookkeeping: with the state gone there is
        // nothing left for the expiry tick to notice, and stepping the tombstone twice would
        // swallow one more renewal from the same id.
        let _ = self.awareness_seen.remove(&id);
        let held = self
            .awareness
            .iter()
            .any(|(client, state)| client == id && state.data.is_some());
        if held && id != self.awareness.client_id() {
            self.awareness.remove_state(id);
        }
    }
}
