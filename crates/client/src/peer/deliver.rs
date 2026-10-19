//! One frame in: `CANONICAL.md` §6.1's verdict, §13's decision, and the stream of
//! y-protocols messages a content frame carries.

use super::PeerSession;
use super::plaintext::encode_y_message;
use super::report::Applied;
use super::report::Dropped;
use super::report::Ending;
use super::report::Outcome;
use super::report::Published;
use crate::host::HostReason;
use crate::nesting::decode_update;
use crate::sealed::Announcement;
use crate::sealed::Payload;
use crate::sealed::PublicKey;
use crate::sealed::Verdict;
use std::time::Duration;
use yrs::Doc;
use yrs::ReadTxn;
use yrs::StateVector;
use yrs::Transact;
use yrs::sync::AwarenessUpdate;
use yrs::sync::Message as YMessage;
use yrs::sync::MessageReader;
use yrs::sync::SyncMessage;
use yrs::sync::awareness::AwarenessUpdateSummary;
use yrs::updates::decoder::DecoderV1;

/// How many `SyncStep1` messages one `kind = 0` frame is answered for.
///
/// A frame is a stream of messages with no count (`PROTOCOL.md` §7), and the answer to a
/// `SyncStep1` is a diff of the whole document, so a frame of them answered one by one would
/// cost the room a document per four bytes. A conforming peer asks once per frame, so past the
/// first the state vector is read and dropped and its diff is never computed.
const MAX_REPLIES_PER_FRAME: usize = 1;

/// The replies and the readiness one frame drew, accumulated as its messages are read.
#[derive(Debug, Default)]
struct ReadState {
    replies: Vec<YMessage>,
    requested: bool,
}

impl PeerSession {
    /// One sealed frame, as the relay delivered it.
    pub fn deliver(&mut self, clock: Duration, frame: &[u8]) -> Outcome {
        self.frames = self.frames.saturating_add(1);
        self.count_frame();
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
            return self.refuse(index, &verdict);
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

    /// §13.6: a refused *content* frame opens an interval this client re-syncs in. The
    /// envelope's `kind` is readable before any signature is verified, so that is a claim about
    /// the bytes and not about the reason the frame was refused.
    fn note_refusal(&mut self, clock: Duration, verdict: &Verdict) {
        if verdict.kind == Some(0) {
            let _ = self.resync_from.get_or_insert(clock);
        }
    }

    /// One refused frame: §13.2's local report, which is never sent anywhere.
    fn refuse(&mut self, frame: u64, verdict: &Verdict) -> Outcome {
        // §13.3: the reason stays `stale_issued` for a second state at one edition and for an
        // older one alike, and this index is the local annotation beside it that says which of
        // the two the frame was.
        if verdict.conflict {
            self.conflicts.push(frame);
        }
        // Every refusal carries the step that refused it; one without a reason would be a bug
        // in the byte layer and not a decision to report.
        let named = verdict
            .reason
            .clone()
            .unwrap_or_else(|| "bad_envelope".to_string());
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
            Some(Payload::Content) => {
                self.apply_content(clock, &verdict.plaintext, verdict.verified);
            }
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

    /// What §13.3, §13.1's step 6 and §13.7 owe an applied room state.
    pub(super) fn after_state(&mut self, clock: Duration, issued: u64) {
        self.state_issued = Some(issued);
        if let Some(host) = self.host.as_mut() {
            // §7.1: a host that has verified a state keeps its own series above it.
            host.verified_state(issued);
        }
        self.refresh_host_away(clock, true);
        self.drop_departed_holds();
        // §13.6: the applied state is the room's writer set, so a revoked or demoted key's
        // reciprocal accounting goes with its eligibility and a later promotion may arm again.
        self.prune_reciprocal();
        let role = self.role().map(String::from);
        self.note_local_eligibility(role.as_deref());
        self.role_seen = role;
        // §13.1's steps 6 and 4: a state that commits this key is where the handshake belongs,
        // and one that does not is what a re-announcement is for — the renewal clock alone would
        // wait a whole window for it.
        if self.commits_ours() {
            self.resolve_speculation();
            self.handshake_once(clock);
            self.flush_held_back();
        } else {
            self.announce(clock);
        }
        // §13.7: a holder's *first* holds message waits for the state that commits its key, so
        // one that was open when that state landed announces here rather than a renewal late.
        // Nothing goes out for a session that holds nothing and has said nothing, and nothing
        // goes out until the key is committed — that test is `announce_holds`' own.
        self.announce_holds(clock);
    }

    /// §13.2 and §13.3: a `kind = 0` plaintext, applied to the session document and answered.
    /// An awareness message inside it is stamped on `clock`, the session's own, for §8.2.
    #[expect(
        clippy::too_many_arguments,
        reason = "the frame, the clock it is read on, the verified key and the replica are the whole of a content frame"
    )]
    fn apply_content(
        &mut self,
        clock: Duration,
        plaintext: &[u8],
        verified: Option<PublicKey>,
    ) {
        if !self.state_held() {
            return;
        }
        // §13.9: a `viewer` publishes its SyncStep1, its awareness and its holds, and nothing
        // else — a SyncStep2 is document content (§13.5) and is not its to send. An uncommitted
        // key is the same: nothing it would answer with is publishable yet. The request is still
        // recognised and the rest of the frame still applies.
        let answers = self.may_publish() && self.is_writer();
        // §13.9: while a speculative edit is held, the readiness and replies a peer draws must
        // be computed from the clean replica and not the one carrying the speculation.
        let publication = self.publication_doc().clone();
        let (replies, requested) =
            self.read_stream(clock, plaintext, &publication, answers);
        if self.baseline.is_some() {
            // Keep the ordinary replica's local view current without computing a second reply.
            let ordinary = self.awareness.doc().clone();
            let _ = self.read_stream(clock, plaintext, &ordinary, false);
        }
        if requested && let Some(key) = verified {
            self.arm_reciprocal(key);
        }
        if !answers {
            return;
        }
        for reply in replies {
            let bytes = encode_y_message(&reply);
            self.publish(Published::Sync, &bytes);
        }
    }

    /// §7's stream, read message by message and applied as it is read, with the replies it
    /// owes. A message that cannot be read or applied ends the reading, and the messages
    /// before it stand: a stream no replica decodes is a sender's bug, and the session goes on.
    #[expect(
        clippy::too_many_arguments,
        reason = "the frame, the replica it is applied to and whether it may be answered are the whole of a read"
    )]
    fn read_stream(
        &mut self,
        clock: Duration,
        plaintext: &[u8],
        doc: &Doc,
        answer: bool,
    ) -> (Vec<YMessage>, bool) {
        let mut decoder = DecoderV1::from(plaintext);
        let mut state = ReadState::default();
        let _ = MessageReader::new(&mut decoder).map_while(Result::ok).all(
            |message| {
                self.read_message(clock, message, doc, answer, &mut state)
            },
        );
        (state.replies, state.requested)
    }

    /// One message of the stream. Returns whether the reading goes on.
    ///
    /// §8.3: an auth message is read and ignored, a denial included, and an awareness query
    /// is dropped rather than answered. A `SyncStep1` is answered once per frame
    /// ([`MAX_REPLIES_PER_FRAME`]), and a message type §7 does not define ends the reading.
    #[expect(
        clippy::too_many_arguments,
        reason = "one message, its target replica, whether it may be answered and the read it accumulates into"
    )]
    fn read_message(
        &mut self,
        clock: Duration,
        message: YMessage,
        doc: &Doc,
        answer: bool,
        state: &mut ReadState,
    ) -> bool {
        match message {
            YMessage::Sync(SyncMessage::SyncStep1(vector)) => {
                // §13.6: the request is read and past whatever the reply cap or the role does
                // with it, so a caller still sees the obligation a `viewer` or an uncommitted
                // key may not answer. The inner state vector was decoded by the reader already,
                // so a malformed one never reaches here and neither arms nor consumes anything.
                state.requested = true;
                Self::answer(doc, answer, &vector, &mut state.replies);
                true
            }
            YMessage::Sync(
                SyncMessage::SyncStep2(bytes) | SyncMessage::Update(bytes),
            ) => Self::apply_update(doc, &bytes),
            YMessage::Awareness(update) => self.apply_awareness(clock, update),
            YMessage::Auth(_) | YMessage::AwarenessQuery => true,
            YMessage::Custom(..) => false,
        }
    }

    /// §7: a `SyncStep1` is answered with the `SyncStep2` its state vector asks for, while the
    /// frame has answers left and the caller permits one. `answer` is the §13.9 gate: a
    /// `viewer` or an uncommitted key reads and drops the request instead of computing a diff.
    #[expect(
        clippy::too_many_arguments,
        reason = "the replica, whether a reply is permitted, the vector and the replies are one answer"
    )]
    fn answer(
        doc: &Doc,
        permitted: bool,
        vector: &StateVector,
        replies: &mut Vec<YMessage>,
    ) {
        if !permitted || replies.len() >= MAX_REPLIES_PER_FRAME {
            return;
        }
        let txn = doc.transact();
        let diff = txn.encode_state_as_update_v1(vector);
        replies.push(YMessage::Sync(SyncMessage::SyncStep2(diff)));
    }

    /// One update applied to the chosen replica. Returns whether it was read and applied.
    ///
    /// The reader has already refused a frame holding an update nested past the bound, and
    /// this decode is bounded too, so no path hands `yrs` one to recurse into.
    pub(super) fn apply_update(doc: &Doc, bytes: &[u8]) -> bool {
        let Ok(update) = decode_update(bytes) else {
            return false;
        };
        let mut txn = doc.transact_mut();
        txn.apply_update(update).is_ok()
    }

    /// §8.2: one awareness message, applied by its own clocks and stamped on this session's
    /// clock for each id it actually applied. Returns whether it was read, which is the same
    /// answer [`Awareness::apply_update`] gives.
    ///
    /// The stamp is the applied ids and not the message's: an entry whose clock is not above
    /// the one this replica already holds for that id is ignored by y-protocols and renews
    /// nothing, so a peer that stops publishing is forgotten while a stale copy of its last
    /// state is still arriving.
    ///
    /// An entry whose clock is [`u32::MAX`] is refused rather than applied, and the rest of the
    /// message is applied as usual. `yrs` steps a clock by one without checking in two places
    /// this path reaches — applying §8.2's removal for the id this connection announced
    /// (`Awareness::apply_update_internal`), and dropping a state the room is done with
    /// (`Awareness::remove_state`, reached by §8.2's expiry and §8.4's departure) — and a clock
    /// is the sender's to choose, so the one value that cannot be stepped never enters: no state
    /// at the maximum is held, and no removal can step one that is.
    fn apply_awareness(
        &mut self,
        clock: Duration,
        mut update: AwarenessUpdate,
    ) -> bool {
        update.clients.retain(|_, entry| entry.clock != u32::MAX);
        let Ok(summary) = self.awareness.apply_update_summary(update) else {
            return false;
        };
        if let Some(applied) = summary {
            self.stamp_awareness(&applied, clock);
        }
        true
    }

    /// §8.2: the local clock an awareness update's applied ids were last seen at. A removed id
    /// is dropped from the bookkeeping with its state, so the expiry tick never steps a
    /// tombstone a second time.
    fn stamp_awareness(
        &mut self,
        summary: &AwarenessUpdateSummary,
        clock: Duration,
    ) {
        let own = self.awareness.client_id();
        let applied = summary
            .added
            .iter()
            .chain(summary.updated.iter())
            .copied()
            .filter(|id| *id != own);
        for id in applied {
            let _ = self.awareness_seen.insert(id, clock);
        }
        for id in &summary.removed {
            let _ = self.awareness_seen.remove(id);
        }
    }
}
