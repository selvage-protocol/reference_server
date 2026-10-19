//! §13.9: the local edits a session may not publish yet, the clean baseline they are
//! held apart from, and what a resolved role does with them.

use super::PeerSession;
use super::plaintext::encode_y_message;
use super::plaintext::peer_doc;
use super::report::Published;
use crate::nesting::decode_update;
use crate::sealed::SealedError;
use std::mem;
use yrs::Doc;
use yrs::ReadTxn;
use yrs::StateVector;
use yrs::Text;
use yrs::Transact;
use yrs::block::ClientID;
use yrs::sync::Awareness;
use yrs::sync::Message as YMessage;
use yrs::sync::SyncMessage;

/// One locally authored edit §13.1's step 4 is holding back, and whether it was made while no
/// applied state had given this connection's key a role yet.
///
/// The deltas themselves and not a diff over the replica: a state that does not commit this key
/// still lets its peers' content be applied (§13.2), so a vector taken at the first held-back
/// edit would carry their changes out again under this connection's key.
#[derive(Debug, Clone)]
pub(super) struct Unsent {
    update: Vec<u8>,
    speculative: bool,
}

impl PeerSession {
    /// §13.9: resolves UNKNOWN-role history before any request or reply is encoded, so a `viewer`
    /// resolution never exports the speculation and a writer never answers from it. A writer
    /// adopts it as room history; a `viewer` discards it outright.
    pub(super) fn resolve_speculation(&mut self) {
        if self.role() == Some("viewer") {
            self.discard_speculative();
        } else if self.is_writer() {
            self.accept_speculation();
        }
    }

    /// The replica publication is built from: the clean baseline while a speculative edit is
    /// held, and this connection's own document otherwise (§13.9).
    pub(super) fn publication_doc(&self) -> &Doc {
        self.baseline
            .as_ref()
            .unwrap_or_else(|| self.awareness.doc())
    }

    /// §13.9: captures a clean replica before the first UNKNOWN-role local mutation, so a later
    /// `viewer` resolution can adopt history without the speculation.
    ///
    /// A genuinely separate `Doc` built from an encoded update, never `Doc::clone` (which is a
    /// handle to the same store). The whole-doc update carries every pending struct and the
    /// delete set, so the baseline is faithful.
    fn begin_speculation(&mut self) {
        if self.baseline.is_some() {
            return;
        }
        let baseline = peer_doc(Some(self.awareness.client_id().get()));
        let whole = {
            let txn = self.awareness.doc().transact();
            txn.encode_state_as_update_v1(&StateVector::default())
        };
        let Ok(update) = decode_update(&whole) else {
            return;
        };
        let mut txn = baseline.transact_mut();
        let _ = txn.apply_update(update);
        drop(txn);
        self.baseline = Some(baseline);
    }

    /// §13.9: the speculative edits are room history after all — the state resolved the key as a
    /// writer — so the clean baseline is dropped and the deltas are publishable.
    fn accept_speculation(&mut self) {
        self.baseline = None;
        for entry in &mut self.unsent {
            entry.speculative = false;
        }
    }

    /// §13.9: adopts the clean baseline in place of the replica that holds the speculative edits.
    ///
    /// The replica is replaced rather than inverse-deleted, so no tombstone or dependency from
    /// the disallowed history survives. The remote awareness states and their clocks are carried
    /// onto the new owner, and this connection's announced awareness id is unchanged.
    fn discard_speculative(&mut self) {
        self.unsent.retain(|entry| !entry.speculative);
        let Some(next) = self.baseline.take() else {
            return;
        };
        // §8.2/§8.4: the removal clocks are as much a part of the inventory as the live states.
        // `update()` would drop every tombstone, letting a stale entry resurrect a peer the room
        // has forgotten, so the whole tracked set is carried by id — `update_with_clients` writes
        // a removed id's clock with a null state — and reapplied, which preserves both.
        let tracked: Vec<ClientID> =
            self.awareness.iter().map(|(id, _)| id).collect();
        let inventory = self.awareness.update_with_clients(tracked);
        let mut adopted = Awareness::new(next);
        if let Ok(carried) = inventory {
            let _ = adopted.apply_update(carried);
        }
        self.awareness = adopted;
        self.replica_epoch = self.replica_epoch.saturating_add(1);
    }

    /// A local edit, published as the delta it produced and never as the whole document.
    ///
    /// Returns whether the delta was queued. A known `viewer` refuses the mutation outright and
    /// an UNKNOWN-role edit is held until a state resolves its eligibility (§13.1's step 4,
    /// §13.9).
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
        // §13.5 and §13.9: a known `viewer` never creates the CRDT history a later promotion
        // could export, so the mutation is refused before it touches the replica.
        if self.role() == Some("viewer") {
            return Ok(false);
        }
        // §13.9: before any state gives this key a role, an edit is speculative — it goes into
        // the replica for the local view, and a clean baseline is captured so a later `viewer`
        // resolution can drop it without leaving a tombstone or a dependency behind.
        let speculative = !self.is_writer();
        if speculative {
            self.begin_speculation();
        }
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
        if !self.may_publish() || !self.is_writer() {
            // §13.1's step 4 holds content back until a state commits this key, and a client
            // that dropped what it held back would leave the room without that edit for good.
            // What is kept is this edit's own delta and not a diff over the document: another
            // peer's content can have arrived in the meantime, and a client that re-sent it
            // would publish under its own key changes it did not make.
            self.hold_back(update, speculative);
            return Ok(false);
        }
        // The state resolved this key as a writer: the edit is room history, not speculation.
        self.accept_speculation();
        if self.send_update(&update) {
            Ok(true)
        } else {
            // A failed seal is not a publication: keep the delta for the next flush.
            self.hold_back(update, false);
            Ok(false)
        }
    }

    /// Keeps one edit this connection may not publish yet (§13.1's step 4). §13.9's `viewer`
    /// keeps no such edit — its mutation is refused — and §13.10's ended session will publish
    /// nothing later either: keeping it would grow a buffer nothing drains for as long as the
    /// session lives.
    fn hold_back(&mut self, update: Vec<u8>, speculative: bool) {
        if self.role() != Some("viewer") && self.ending.is_none() {
            self.unsent.push(Unsent {
                update,
                speculative,
            });
        }
    }

    fn send_update(&mut self, update: &[u8]) -> bool {
        let plaintext = encode_y_message(&YMessage::Sync(SyncMessage::Update(
            update.to_vec(),
        )));
        self.publish(Published::Content, &plaintext)
    }

    /// Publishes the edits this connection made before a state committed its key (§13.1's step
    /// 4), in the order they were made.
    ///
    /// §13.10 reaches this path too: a state that arrives after the session ended would
    /// otherwise flush the whole held set into a room this connection has left, so an ended
    /// session drops the buffer rather than growing it.
    pub(super) fn flush_held_back(&mut self) {
        if self.unsent.is_empty() {
            return;
        }
        if self.ending.is_some() {
            self.unsent.clear();
            return;
        }
        if !self.is_writer() {
            return;
        }
        self.accept_speculation();
        let held: Vec<Unsent> = mem::take(&mut self.unsent);
        if !self.may_publish() {
            self.unsent = held;
            return;
        }
        // A failed seal is not a publication: keep that delta for the next flush.
        let retained: Vec<Unsent> = held
            .into_iter()
            .filter(|entry| !self.send_update(&entry.update))
            .collect();
        self.unsent = retained;
    }
}
