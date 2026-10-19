//! What a driver hands in that is not a frame: `peer.joined`, `peer.left`, and a
//! host's listing changing under it.

use super::PeerSession;
use crate::host::HostReason;
use std::time::Duration;

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
        // held set without asking for it — on the event itself rather than at the next renewal,
        // which is a whole window the joiner would spend holding nothing from this peer. The
        // state goes first: a holds message resolves against the keys the applied state commits
        // (§13.4), so a joiner that has applied no state yet refuses it `uncommitted_key`.
        self.holds_announced_at = None;
        if self.host.is_some() {
            self.publish_state(clock, HostReason::Roster);
            self.announce_holds(clock);
            return;
        }
        // §7.1: the re-send is for a room whose host is away — a joiner the host cannot answer,
        // or a returning host that lost its `issued`. While the seat the `host` entry labels is
        // seated, the host's own fresh state answers this join, and a copy from every peer would
        // put the whole listing on every connection once per seated peer.
        let host_away = self
            .host_seat()
            .is_none_or(|host_seat| !self.roster.contains(host_seat));
        if host_away && let Some(frame) = self.held_state_frame.clone() {
            self.republish(&frame);
        }
        self.announce_holds(clock);
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
}
