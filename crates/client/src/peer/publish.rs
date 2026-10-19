//! Everything that leaves a session: the seal, §13.6's publication gates, the
//! announcement and its renewal, the handshake, and the two ways a host's publishing ends.

use super::PeerSession;
use super::plaintext::canonical;
use super::plaintext::encode_y_message;
use super::plaintext::is_content;
use super::report::Counted;
use super::report::Ending;
use super::report::Published;
use crate::host::HostProducer;
use crate::host::HostReason;
use crate::sealed::Recipe;
use crate::sealed::SealedError;
use crate::sealed::fresh_nonce;
use crate::sealed::seal;
use serde_json::Value;
use serde_json::json;
use std::time::Duration;
use yrs::ReadTxn;
use yrs::Transact;
use yrs::sync::Message as YMessage;
use yrs::sync::SyncMessage;

/// What a host is told when §7.1's closing could not be sealed: a host cannot refuse to close
/// without somewhere to say so, and a driver that never looks is a driver closing nothing.
const CLOSING_UNSEALED: &str = "the closing could not be sealed";

impl PeerSession {
    /// The state §7.1 has this host publish now, if it is due: sealed by the host key, counted
    /// as a publication, and folded into this session's own receiver.
    ///
    /// A host never receives the state it writes — the relay sends a frame to the room's
    /// *other* connections — so the receiver is folded from the value rather than from the
    /// bytes, and before anything that follows the frame (`after_state`) so that a `SyncStep1`
    /// goes out only once a state commits this connection's key (§13.1's steps 4 and 6).
    pub(super) fn publish_state(
        &mut self,
        clock: Duration,
        reason: HostReason,
    ) {
        if self.room_frames >= self.frame_budget {
            return;
        }
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
        if publication.fresh {
            self.count_frame();
        }
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
    pub(super) fn republish(&mut self, frame: &[u8]) {
        if self.ending.is_some() {
            return;
        }
        self.outbound.push_back(frame.to_vec());
        self.published = self.published.saturating_add(1);
    }

    /// §13.1's step 4: the session-key announcement, `kind = 4`, signed by the key it names.
    pub(super) fn announce(&mut self, clock: Duration) {
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
    pub(super) fn reannounce(&mut self, clock: Duration) {
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

    /// §13.1's step 6's handshake, run when this connection's own key *becomes* committed and
    /// not for every state that commits it: a host republishes its state on every `peer.joined`
    /// and on every announcement it accepts, so a handshake per state would be a frame per
    /// republish.
    pub(super) fn handshake_once(&mut self, clock: Duration) {
        if self.handshaken_at.is_none() {
            // §13.6: only an actual seal-and-queue discharges a request. A failed attempt is not
            // a publication, so the clock is not moved and the next due tick retries it.
            let _ = self.sync_step1(clock);
        }
    }

    /// §13.1's step 6: a `SyncStep1` with this replica's state vector, so the room can tell a
    /// client that has been refusing frames from one that has been applying them.
    ///
    /// The vector is the publication replica's: §13.9 keeps a `viewer`'s own history out of it,
    /// so the room is asked for what the room-safe history lacks. The shared clock and the
    /// coalesced obligations move only when the frame was actually sealed and queued.
    fn sync_step1(&mut self, clock: Duration) -> bool {
        let vector = {
            let txn = self.publication_doc().transact();
            txn.state_vector()
        };
        let message =
            encode_y_message(&YMessage::Sync(SyncMessage::SyncStep1(vector)));
        if !self.publish(Published::Sync, &message) {
            return false;
        }
        self.handshaken_at = Some(clock);
        self.resync_from = None;
        self.reciprocal_pending.clear();
        self.local_readiness_pending = false;
        true
    }

    /// §13.6: refusal, reciprocal and local readiness obligations share one clock — at most one
    /// locally generated `SyncStep1` per renewal interval, measured from the last request
    /// actually sealed and queued, whichever obligation asked for it. The initial request is
    /// always due; an obligation too early to send is retained and served at the first due tick.
    pub(super) fn resync(&mut self, clock: Duration) {
        if !self.may_publish() {
            return;
        }
        let Some(at) = self.handshaken_at else {
            let _ = self.sync_step1(clock);
            return;
        };
        if self.resync_from.is_none()
            && self.reciprocal_pending.is_empty()
            && !self.local_readiness_pending
        {
            return;
        }
        if clock.saturating_sub(at) >= self.renew {
            let _ = self.sync_step1(clock);
        }
    }

    /// One frame sealed under the frame key and signed by this connection's session key.
    ///
    /// Returns whether the frame was sealed and put on this session's own outbound queue: the
    /// §13.6 clock moves on that and never on an attempt. The socket's own queue is the relay's
    /// ([`crate::relay`]), so a successful seal is not a claim of delivery.
    pub(super) fn publish(
        &mut self,
        what: Published,
        plaintext: &[u8],
    ) -> bool {
        if !self.may_queue(what, plaintext)
            || self.room_frames >= self.frame_budget
        {
            return false;
        }
        let Ok(bytes) = self.sealed_frame(what.kind(), plaintext) else {
            // A CSPRNG that will not read, or an AEAD that refuses its own inputs. Neither
            // happens in practice, and a session that kept quiet about one would look like a
            // client with nothing to say.
            self.fault = Some(format!("a {what:?} frame could not be sealed"));
            return false;
        };
        self.count_frame();
        self.outbound.push_back(bytes);
        let tally = match what.counted() {
            Counted::Publication => &mut self.published,
            Counted::Handshake => &mut self.handshake,
        };
        *tally = tally.saturating_add(1);
        true
    }

    /// §13.6's publication gates, checked at the moment of queueing: an ended session publishes
    /// nothing, a key no state commits publishes only its announcement, and document content is
    /// a writer's alone (§13.5). The announcement keeps its deliberate precommit exception.
    fn may_queue(&self, what: Published, plaintext: &[u8]) -> bool {
        if self.ending.is_some() {
            return false;
        }
        what == Published::Announcement
            || (self.may_publish()
                && (!is_content(plaintext) || self.is_writer()))
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
        self.count_frame();
        if let Some(host) = self.host.as_mut() {
            host.save_frames();
        }
        self.outbound.push_back(publication.frame);
        self.published = self.published.saturating_add(1);
        self.ending = Some(Ending::Closing);
        true
    }

    /// `CANONICAL.md` §6.1's frame budget: once the room's count reaches it, nothing more is
    /// sealed under the frame key. A host publishes its one closing first — the frame that ends
    /// the room for every peer holding its state — and every session, the host's included, ends
    /// and says why.
    /// One more frame against the room's count, handed to the host's producer to persist.
    pub(super) const fn count_frame(&mut self) {
        self.room_frames = self.room_frames.saturating_add(1);
        if let Some(host) = self.host.as_mut() {
            host.count_frames(self.room_frames);
        }
    }

    pub(super) fn budget_spent(&mut self) -> bool {
        if self.room_frames < self.frame_budget {
            return false;
        }
        if let Some(publication) =
            self.host.as_mut().and_then(HostProducer::closing)
        {
            self.count_frame();
            self.outbound.push_back(publication.frame);
            self.published = self.published.saturating_add(1);
        }
        // The session ends here, so the count is written now rather than on a tick that will not
        // come.
        if let Some(host) = self.host.as_mut() {
            host.save_frames();
        }
        self.ending = Some(Ending::FrameBudget);
        true
    }
}
