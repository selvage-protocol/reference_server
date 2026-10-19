//! What a caller reads off a session: its keys, its roles, its counts and its replica.

use super::PeerSession;
use super::options::FRAME_BUDGET;
use super::options::PeerOptions;
use super::options::host_producer;
use super::plaintext::peer_doc;
use super::report::Applied;
use super::report::Dropped;
use super::report::Ending;
use crate::host::HostProducer;
use crate::host::HostReason;
use crate::sealed::PublicKey;
use crate::sealed::Reader;
use crate::sealed::SealedError;
use crate::sealed::SessionKey;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::time::Duration;
use yrs::GetString;
use yrs::ReadTxn;
use yrs::Text;
use yrs::Transact;
use yrs::sync::Awareness;

impl PeerSession {
    /// A connection's session: §13.1's steps 1 and 2, and nothing sent yet.
    ///
    /// The clock this session reads starts at zero and every argument named `clock` is that
    /// many units past the seat, so a caller passes `start.elapsed()` and a test passes a
    /// number. Nothing here reads a platform clock.
    ///
    /// # Errors
    ///
    /// Returns an error when the platform's CSPRNG cannot be read, which is the one thing
    /// minting a session keypair needs, and when a host session's first state cannot be sealed:
    /// §7.1's mint state is what brings the room's listing into existence and commits this
    /// connection's key, so a host that could not publish one is a host no peer can see.
    pub fn new(options: &PeerOptions) -> Result<Self, SealedError> {
        let session = match options.fixed_session_key {
            Some(seed) => SessionKey::from_seed(seed),
            None => SessionKey::generate()?,
        };
        let reader =
            Reader::new(&options.room_id, options.room_key, options.host_key);
        let frame_key = reader.frame_key;
        let own_key = session.public();
        let host = match &options.host {
            None => None,
            Some(host_options) => {
                Some(host_producer(options, frame_key, host_options, own_key)?)
            }
        };
        // `CANONICAL.md` §6.1: the host's count is the room's, so a host continues the one it
        // saved.
        let room_frames = host.as_ref().map_or(0, HostProducer::room_frames);
        let mut roster = options.roster.clone();
        if let (Some(seat), true) = (options.seat.as_ref(), host.is_some()) {
            // §9: a host is seated in the room it minted, whether or not the roster it was
            // handed names it. Without this its own state's `host` entry labels a seat the
            // session believes is absent, and §13.8's clock then ends its own session.
            let _ = roster.insert(seat.clone());
        }
        let mut peer = Self {
            room_id: options.room_id.clone(),
            frame_key,
            session,
            declared_role: options.declared_role.clone(),
            host,
            held_state_frame: None,
            counter: 0,
            renew: options.renew,
            expire: options.expire,
            seat: options.seat.clone(),
            roster,
            reader,
            awareness: Awareness::new(peer_doc(options.awareness_client_id)),
            awareness_seen: BTreeMap::new(),
            outbound: VecDeque::new(),
            held: BTreeSet::new(),
            holds_sent: Vec::new(),
            holds_announced_at: None,
            leases: BTreeMap::new(),
            departed: BTreeSet::new(),
            announced_at: None,
            unsent: Vec::new(),
            baseline: None,
            replica_epoch: 0,
            reciprocal_served: BTreeSet::new(),
            reciprocal_pending: BTreeSet::new(),
            local_readiness_pending: false,
            role_seen: None,
            resync_from: None,
            handshaken_at: None,
            state_issued: None,
            host_away_since: None,
            published: 0,
            handshake: 0,
            frames: 0,
            room_frames,
            frame_budget: options.frame_budget.unwrap_or(FRAME_BUDGET),
            applied: Vec::new(),
            dropped: Vec::new(),
            ignored: Vec::new(),
            conflicts: Vec::new(),
            ending: None,
            mutation: None,
            fault: None,
        };
        if peer.host.is_some() {
            // §13.1: a host's order is a peer's with one difference — it publishes a state at
            // mint, so its own state may precede any it verifies. That state is also what
            // commits its own connection's key, which is why a host has nothing to announce.
            peer.publish_state(Duration::ZERO, HostReason::Mint);
        }
        let failed = peer.fault.take();
        if let Some(fault) = failed {
            return Err(SealedError::new(fault));
        }
        Ok(peer)
    }

    /// This connection's session public key, which the announcement names.
    #[must_use]
    pub fn session_key(&self) -> PublicKey {
        self.session.public()
    }

    /// The generation of the document this connection currently owns text in, for a caller that
    /// watches the replica rather than its contents: it moves when §13.9's `viewer` resolution
    /// replaces the replica, which is the one state-driven way text can change without a content
    /// frame. Comparing it is how the relay reports that change without reading any text.
    #[must_use]
    pub const fn replica_epoch(&self) -> u64 {
        self.replica_epoch
    }

    /// This connection's own seat, as a caller that needs to show it reads it.
    #[must_use]
    pub fn seat(&self) -> Option<&str> {
        self.seat.as_deref()
    }

    /// Whether a verified room state has been applied, which §13.3's waiting rules turn on.
    #[must_use]
    pub const fn state_held(&self) -> bool {
        self.state_issued.is_some()
    }

    /// Whether this connection holds the host key, which is the whole of what being the host is
    /// in this version (§7.1).
    #[must_use]
    pub const fn is_host(&self) -> bool {
        self.host.is_some()
    }

    /// The awareness client id this connection announced, which is the one its replica speaks
    /// under (§8.4): the id the room records for this seat, so a peer's caret is attributed to
    /// the seat that published it.
    #[must_use]
    pub fn awareness_client_id(&self) -> u64 {
        self.awareness.client_id().get()
    }

    /// The awareness client ids this replica holds a state for, ascending: the ones a peer
    /// published and nothing has removed since.
    #[must_use]
    pub fn awareness_clients(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self
            .awareness
            .iter()
            .filter(|(_, state)| state.data.is_some())
            .map(|(id, _)| id.get())
            .collect();
        ids.sort_unstable();
        ids
    }

    /// The role the applied state gives this connection's own key (§13.4), or `None` while no
    /// state commits it — the state is the only source of a role in this version.
    #[must_use]
    pub fn own_role(&self) -> Option<&str> {
        self.role()
    }

    /// The seat the applied state's `host` entry labels, or `None` while no state names one
    /// (§13.4: a client with none **MUST NOT** guess one).
    #[must_use]
    pub fn named_host_seat(&self) -> Option<&str> {
        self.host_seat()
    }

    /// The roles the applied state assigns, by the seat each committed key is labelled.
    #[must_use]
    pub fn roles_by_seat(&self) -> BTreeMap<String, String> {
        self.reader
            .committed
            .values()
            .map(|peer| (peer.peer_id.clone(), peer.role.clone()))
            .collect()
    }

    /// The edition of the last state this connection published, or `0` before it has published
    /// one. This is §7.1's `issued` series, which a host that keeps hosting continues.
    #[must_use]
    pub fn published_issued(&self) -> u64 {
        self.host.as_ref().map_or(0, HostProducer::published_issued)
    }

    /// The edition of the state this client holds, or `None` before one is applied.
    #[must_use]
    pub const fn state_issued(&self) -> Option<u64> {
        self.state_issued
    }

    /// The listing of the last state applied, with §5's refused paths dropped.
    #[must_use]
    pub fn listing(&self) -> &[String] {
        &self.reader.listing
    }

    /// How many frames this client **published** under §13's rules (§13.11).
    #[must_use]
    pub const fn published(&self) -> u64 {
        self.published
    }

    /// How many §7 handshake frames it sent, counted apart from its publications.
    #[must_use]
    pub const fn handshake(&self) -> u64 {
        self.handshake
    }

    #[must_use]
    pub const fn frames(&self) -> u64 {
        self.frames
    }

    #[must_use]
    pub fn applied(&self) -> &[Applied] {
        &self.applied
    }

    #[must_use]
    pub fn dropped(&self) -> &[Dropped] {
        &self.dropped
    }

    /// The frames that verified and applied nothing, by the index they were handed as.
    #[must_use]
    pub fn ignored(&self) -> &[u64] {
        &self.ignored
    }

    /// The frames refused as a second publication at an edition this client already holds
    /// (§13.3), by the index they were handed as. They are in `dropped` too, with the reason
    /// `stale_issued`: §13.3 keeps the reason and asks for the divergence to be visible beside
    /// it, because two receivers can hold different listings at one edition and nothing on the
    /// wire says which of the two is the host's latest.
    #[must_use]
    pub fn conflicts(&self) -> &[u64] {
        &self.conflicts
    }

    /// Why the session ended, if it has.
    #[must_use]
    pub const fn ending(&self) -> Option<Ending> {
        self.ending
    }

    #[must_use]
    pub fn mutation(&self) -> Option<&str> {
        self.mutation.as_deref()
    }

    /// The first frame this session could not produce, if any. A driver reads it and fails
    /// loudly: publishing fails silently otherwise, and a subject that publishes nothing looks
    /// like a client with nothing to say.
    #[must_use]
    pub fn fault(&self) -> Option<&str> {
        self.fault.as_deref()
    }

    /// The paths this client has open: what its own holds message carries.
    #[must_use]
    pub const fn held(&self) -> &BTreeSet<String> {
        &self.held
    }

    /// What each peer is held to, by the key's canonical spelling where the applied state
    /// names it and by its id where nothing does (§13.4). A key whose lease has lapsed is not
    /// here at all: §13.7 has the receiver forget the whole set.
    #[must_use]
    pub fn peer_holds(&self) -> BTreeMap<String, Vec<String>> {
        self.reader
            .holds
            .iter()
            .map(|(id, paths)| (self.spelling(*id), paths.clone()))
            .collect()
    }

    /// The length of a document's text in UTF-16 code units, the unit yjs, every editor's
    /// `offsetAt` and every peer on the wire count an offset in (`PROTOCOL.md` §8.1).
    #[must_use]
    pub fn length(&self, path: &str) -> u32 {
        let doc = self.awareness.doc();
        let text = doc.get_or_insert_text(path);
        text.len(&doc.transact())
    }

    /// The text this replica holds at a path, which is what a decision vector reads.
    #[must_use]
    pub fn text(&self, path: &str) -> String {
        let doc = self.awareness.doc();
        let text = doc.get_or_insert_text(path);
        text.get_string(&doc.transact())
    }

    /// The CRDT state vector, as `(client id, clock)` pairs: what a sync handshake exchanges,
    /// and what two fully synced replicas agree on.
    #[must_use]
    pub fn state_vector(&self) -> Vec<(u64, u32)> {
        let doc = self.publication_doc();
        let txn = doc.transact();
        let mut entries: Vec<(u64, u32)> = txn
            .state_vector()
            .iter()
            .map(|(client, clock)| (client.get(), *clock))
            .collect();
        entries.sort_unstable();
        entries
    }

    /// The paths this replica holds any text for: the documents that have arrived.
    #[must_use]
    pub fn documents(&self) -> Vec<String> {
        let doc = self.awareness.doc();
        let txn = doc.transact();
        let mut paths: Vec<String> = txn
            .root_refs()
            .filter(|(name, _)| txn.get_text(*name).is_some())
            .map(|(name, _)| name.to_string())
            .collect();
        paths.sort();
        paths
    }

    /// Takes what this session has published, in the order it published it.
    pub fn take_outbound(&mut self) -> Vec<Vec<u8>> {
        self.outbound.drain(..).collect()
    }
}
