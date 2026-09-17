//! Memory-only session state: rooms, membership, the open-document set, and the
//! host-reconnect grace period.
//!
//! Nothing here looks at document or awareness payloads. A room knows only which
//! peers are connected and which documents they have declared open.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use selvage_protocol::{Keepalive, PeerInfo, Role};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::oneshot;

/// How many frames one connection may have queued but unwritten. Past it the peer is
/// slow: its frames are not dropped silently, the peer is disconnected and the room is
/// told `peer.left`. Frames alone cannot bound memory — one full-set echo already
/// wires to ~4.2 MiB (`crates/harness/tests/bounds.rs`) — so `MAX_QUEUE_BYTES` bounds
/// the bytes beside it and this stays as the backstop for a flood of small frames.
pub const MAX_QUEUE_FRAMES: usize = 32;

/// How many payload bytes one connection may have queued but unwritten: 32 MiB, four
/// times the largest frame a legitimate session sends (an 8 MiB update, measured in
/// `crates/harness/tests/session.rs`), so a full-state sync plus concurrent traffic
/// still fits. Past it the peer is slow, like past the frame cap. One slow peer holds
/// at most this many counted bytes — the count includes the frame being written,
/// released only after its send completes — plus the kernel's own buffers; the 33rd
/// frame, or the byte past the cap, disconnects it instead.
pub const MAX_QUEUE_BYTES: usize = 32 * 1024 * 1024;

/// A frame the connection task should write out.
#[derive(Debug, Clone)]
pub enum Outbound {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Close(u16, String),
}

impl Outbound {
    /// The payload bytes this frame holds queued: what the byte cap accounts. The
    /// envelope around them (the enum, the channel slot) is tens of bytes per frame
    /// and is not counted; 32 frames of it vanish beside a megabyte payload.
    #[must_use]
    pub const fn payload_len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Binary(bytes) | Self::Ping(bytes) => bytes.len(),
            Self::Close(_, reason) => reason.len(),
        }
    }
}

/// One connection's outbound queue: the channel its frames leave through and the
/// count of payload bytes queued but unwritten. Bytes are reserved before queueing
/// and released after the writer's send of the frame completes, written or not, so
/// a frame being written stays counted and the count tracks what the server still
/// holds for the peer rather than what it has ever sent.
#[derive(Debug, Clone)]
pub struct Queue {
    tx: Sender<Outbound>,
    queued: Arc<AtomicUsize>,
}

impl Queue {
    /// A fresh queue and its receiving end. The writer drains the receiver and
    /// releases each frame's bytes after its send completes.
    #[must_use]
    pub fn channel() -> (Self, Receiver<Outbound>) {
        let (tx, rx) = channel(MAX_QUEUE_FRAMES);
        (
            Self {
                tx,
                queued: Arc::new(AtomicUsize::new(0)),
            },
            rx,
        )
    }

    /// Shares the byte count with the writer draining this queue.
    #[must_use]
    pub fn queued_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.queued)
    }

    /// Payload bytes currently held for the peer: queued, being written, or reserved
    /// just before the channel refused the frame. Never above the cap.
    #[must_use]
    pub fn queued_bytes(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    /// Queues one frame, reporting whether it fit. Past either bound the peer is
    /// slow and the caller disconnects it rather than queue without bound. The byte
    /// reservation is atomic — reserved bytes never exceed the cap — and a
    /// reservation whose frame the channel refuses is released outright. A closed
    /// receiver means the connection task is already gone.
    #[must_use]
    pub fn try_queue(&self, out: Outbound) -> bool {
        let len = out.payload_len();
        if self
            .queued
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(len)
                    .filter(|reserved| *reserved <= MAX_QUEUE_BYTES)
            })
            .is_err()
        {
            return false;
        }
        if self.tx.try_send(out).is_ok() {
            return true;
        }
        self.queued.fetch_sub(len, Ordering::Relaxed);
        false
    }
}

#[derive(Clone)]
pub struct Peer {
    pub info: PeerInfo,
    pub queue: Queue,
    /// The room's arrival counter when this peer was seated. `peers` carries no order in the
    /// protocol (`PROTOCOL.md` §6.2), but the frame's bytes must not be a hash artifact either,
    /// so the list is written in join order and this is what says what that is.
    joined: u64,
}

impl Peer {
    #[must_use]
    pub const fn new(info: PeerInfo, queue: Queue) -> Self {
        Self {
            info,
            queue,
            joined: 0,
        }
    }

    /// Queues one frame, reporting whether it fit. A full queue means the peer stopped
    /// reading; the caller disconnects it rather than queue without bound. A closed
    /// receiver means the connection task is already gone.
    #[must_use]
    pub fn send(&self, out: Outbound) -> bool {
        self.queue.try_queue(out)
    }
}

pub struct Room {
    pub id: String,
    pub token: String,
    pub keepalive: Keepalive,
    pub peers: HashMap<String, Peer>,
    /// The paths peers have declared open, in first-opened order. The set belongs to the
    /// room and outlives the peers that opened a path; only `doc.close` removes one.
    documents: Vec<String>,
    /// The host's listing of its working tree, in the order the host wrote it. Like the
    /// open-document set it belongs to the room and outlives the peers in it; only a
    /// `doc.grant` changes it, and only the host may send one.
    grant: Vec<String>,
    /// Which paths each connected peer currently holds open.
    open: HashMap<String, BTreeSet<String>>,
    host: Option<String>,
    /// Bumped whenever the host attaches or detaches, so a stale grace timer cannot
    /// destroy a room that has been reclaimed.
    generation: u64,
    /// The arrival counter a newly seated peer takes its place from.
    arrivals: u64,
}

impl Room {
    #[must_use]
    pub fn host_present(&self) -> bool {
        self.host
            .as_ref()
            .is_some_and(|id| self.peers.contains_key(id))
    }

    /// Whether `peer_id` is the connection the server holds as this room's host.
    #[must_use]
    pub fn is_host(&self, peer_id: &str) -> bool {
        self.host.as_deref() == Some(peer_id)
    }

    #[must_use]
    pub fn peers_except(&self, peer_id: &str) -> Vec<PeerInfo> {
        // In join order, so that two runs of the same transcript write the same bytes. The
        // prose promises no order and a receiver must not depend on one (`PROTOCOL.md` §6.2);
        // this is what the reference server happens to write.
        let mut peers: Vec<&Peer> = self
            .peers
            .values()
            .filter(|p| p.info.peer_id != peer_id)
            .collect();
        peers.sort_by_key(|p| p.joined);
        peers.into_iter().map(|p| p.info.clone()).collect()
    }

    /// Seats a peer, remembering when it arrived.
    fn seat(&mut self, mut peer: Peer) {
        peer.joined = self.arrivals;
        self.arrivals = self.arrivals.saturating_add(1);
        self.peers.insert(peer.info.peer_id.clone(), peer);
    }

    /// Snapshots the queues of every peer but one: cheap `Sender` plus `Arc` clones
    /// taken under the lock, sent on after it is dropped. Queueing a large frame
    /// copies its bytes once per peer; that copy must not sit inside the critical
    /// section, or one relay stalls every room. A peer removed after the snapshot
    /// keeps its own queue, so sending through it stays safe.
    #[must_use]
    pub fn queues(&self, except: Option<&str>) -> Vec<(String, Queue)> {
        self.peers
            .values()
            .filter(|peer| Some(peer.info.peer_id.as_str()) != except)
            .map(|peer| (peer.info.peer_id.clone(), peer.queue.clone()))
            .collect()
    }

    pub fn attach_host(&mut self, peer_id: &str) {
        self.host = Some(peer_id.to_string());
        self.generation = self.generation.saturating_add(1);
    }

    /// Hands the host role back. Returns the new generation, which is what stops a grace
    /// timer armed before this point from destroying a room the host has since reclaimed.
    pub fn detach_host(&mut self) -> u64 {
        self.host = None;
        self.generation = self.generation.saturating_add(1);
        self.generation
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Records a peer's hold on a path, returning whether the path is newly in the
    /// room's set — or `None` when the set is at its cap and the path is not in it. A
    /// path the room already holds is always fine: re-opening one grows nothing.
    #[expect(
        clippy::too_many_arguments,
        reason = "an open names its peer, path and the cap it is checked against"
    )]
    pub fn open_document(
        &mut self,
        peer_id: &str,
        path: &str,
        max_documents: usize,
    ) -> Option<bool> {
        if self.documents.iter().any(|p| p == path) {
            self.claims_mut(peer_id).insert(path.to_string());
            return Some(false);
        }
        if self.documents.len() >= max_documents {
            return None;
        }
        self.claims_mut(peer_id).insert(path.to_string());
        self.documents.push(path.to_string());
        Some(true)
    }

    /// Releases one peer's hold on a path. The path leaves the room only when no peer
    /// still holds it open.
    pub fn close_document(&mut self, peer_id: &str, path: &str) -> bool {
        if let Some(mine) = self.open.get_mut(peer_id) {
            mine.remove(path);
        }
        if self.open.values().any(|paths| paths.contains(path)) {
            return false;
        }
        let before = self.documents.len();
        self.documents.retain(|p| p != path);
        self.documents.len() != before
    }

    /// Renames a seated peer in place, returning the record as it now reads. The arrival
    /// counter is deliberately untouched: a rename changes a name and nothing else, so the
    /// room's peer list keeps its join order (`PROTOCOL.md` §5, `session.rename`).
    #[must_use]
    pub fn rename_peer(
        &mut self,
        peer_id: &str,
        display_name: &str,
    ) -> Option<PeerInfo> {
        let peer = self.peers.get_mut(peer_id)?;
        peer.info.display_name = display_name.to_string();
        Some(peer.info.clone())
    }

    /// The room's open-document set.
    #[must_use]
    pub fn documents(&self) -> &[String] {
        &self.documents
    }

    /// Replaces the room's grant wholesale: a listing is a snapshot, not a delta, and its
    /// order is the host's, carried unchanged (`CANONICAL.md` §2.7).
    pub fn set_grant(&mut self, paths: Vec<String>) {
        self.grant = paths;
    }

    /// The room's grant.
    #[must_use]
    pub fn grant(&self) -> &[String] {
        &self.grant
    }

    /// Forgets what a peer held open. The paths stay in the room's set: it outlives the
    /// peers that opened them, so a host that reconnects is told what was in play.
    fn forget_claims(&mut self, peer_id: &str) {
        self.open.remove(peer_id);
    }

    fn claims_mut(&mut self, peer_id: &str) -> &mut BTreeSet<String> {
        self.open.entry(peer_id.to_string()).or_default()
    }
}

/// A room about to be minted: its id, its invite token and the keepalive it advertises.
pub struct NewRoom {
    pub id: String,
    pub token: String,
    pub keepalive: Keepalive,
}

/// A connection's claim on a room: which room, with which token, in which role.
#[derive(Debug, Clone, Copy)]
pub struct Claim<'a> {
    pub room_id: &'a str,
    pub token: Option<&'a str>,
    pub role: Role,
}

/// Queues one frame on every snapshotted queue, returning the ids whose queue
/// was full. Runs after the registry lock is dropped: the per-peer copies leave
/// outside the critical section, and the room holds no unsent bytes for the slow
/// ones — the caller removes them instead.
#[must_use]
pub fn send_all(queues: &[(String, Queue)], out: &Outbound) -> Vec<String> {
    queues
        .iter()
        .filter(|(_, queue)| !queue.try_queue(out.clone()))
        .map(|(peer_id, _)| peer_id.clone())
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatError {
    Unknown,
    TokenMismatch,
    HostPresent,
    RoomFull,
}

#[derive(Default)]
pub struct Registry {
    rooms: HashMap<String, Room>,
    /// How to end each seated connection's task, by peer id. Dropping the sender
    /// completes the task's poison channel, so taking it here ends the task: a slow
    /// peer's task is ended when it is removed, and a clean leave takes its sender
    /// only so the map does not keep one for a task that is already ending.
    tasks: HashMap<String, oneshot::Sender<()>>,
}

impl Registry {
    /// Mints a room and seats its host in it.
    /// Mints a room and seats its host in it, reporting whether there was room for
    /// one more. `false` means the server is at its cap and nothing was minted.
    #[expect(
        clippy::too_many_arguments,
        reason = "a mint names its room, host and the cap it is checked against"
    )]
    pub fn create(
        &mut self,
        new: NewRoom,
        host: Peer,
        max_rooms: usize,
    ) -> bool {
        if self.rooms.len() >= max_rooms {
            return false;
        }
        let mut room = Room {
            id: new.id.clone(),
            token: new.token,
            keepalive: new.keepalive,
            peers: HashMap::new(),
            documents: Vec::new(),
            grant: Vec::new(),
            open: HashMap::new(),
            host: None,
            generation: 0,
            arrivals: 0,
        };
        room.attach_host(&host.info.peer_id);
        room.seat(host);
        self.rooms.insert(new.id, room);
        true
    }

    /// Seats a connection in an existing room. The claimed role is honoured only while
    /// the room is between host connections.
    ///
    /// # Errors
    ///
    /// Returns [`SeatError::Unknown`] for a room that does not exist,
    /// [`SeatError::TokenMismatch`] for a wrong token, [`SeatError::HostPresent`]
    /// when the host role is taken, and [`SeatError::RoomFull`] when the room seats no
    /// more peers. A host reclaiming a host-less room always seats: the room's owner
    /// must be able to come back to a full room.
    #[expect(
        clippy::too_many_arguments,
        reason = "an admission names its claim, peer and the cap it is checked against"
    )]
    pub fn admit(
        &mut self,
        claim: Claim<'_>,
        peer: Peer,
        max_peers: usize,
    ) -> Result<(), SeatError> {
        let room = self
            .rooms
            .get_mut(claim.room_id)
            .ok_or(SeatError::Unknown)?;
        if Some(room.token.as_str()) != claim.token {
            return Err(SeatError::TokenMismatch);
        }
        if claim.role == Role::Host && room.host_present() {
            return Err(SeatError::HostPresent);
        }
        let reclaiming = claim.role == Role::Host && !room.host_present();
        if room.peers.len() >= max_peers && !reclaiming {
            return Err(SeatError::RoomFull);
        }
        if claim.role == Role::Host {
            room.attach_host(&peer.info.peer_id);
        }
        room.seat(peer);
        Ok(())
    }

    /// Remembers how to end a seated connection's task.
    pub fn set_task(&mut self, peer_id: &str, poison: oneshot::Sender<()>) {
        self.tasks.insert(peer_id.to_string(), poison);
    }

    /// Forgets a connection's task, returning how to end it. Dropping the sender ends
    /// the task; `None` when the peer was never seated or its task was already taken.
    pub fn take_task(&mut self, peer_id: &str) -> Option<oneshot::Sender<()>> {
        self.tasks.remove(peer_id)
    }

    /// Detaches a peer. Returns whether it was the host, so the caller can announce
    /// `host.detached`. `None` when the room is gone or the peer was never in it:
    /// detaching twice announces once.
    #[must_use]
    pub fn detach(&mut self, room_id: &str, peer_id: &str) -> Option<Detach> {
        let room = self.rooms.get_mut(room_id)?;
        let was_host = room.host.as_deref() == Some(peer_id);
        room.peers.remove(peer_id)?;
        room.forget_claims(peer_id);
        let generation = if was_host {
            room.detach_host()
        } else {
            room.generation()
        };
        Some(Detach {
            was_host,
            generation,
        })
    }

    #[must_use]
    pub fn room(&self, room_id: &str) -> Option<&Room> {
        self.rooms.get(room_id)
    }

    pub fn room_mut(&mut self, room_id: &str) -> Option<&mut Room> {
        self.rooms.get_mut(room_id)
    }

    /// Tears a room down if it is still host-less at `generation`, returning the
    /// peers that were in it so the caller can tell them.
    #[must_use]
    pub fn reap_if_host_absent(
        &mut self,
        room_id: &str,
        generation: u64,
    ) -> Vec<Peer> {
        let Some(room) = self.rooms.get(room_id) else {
            return Vec::new();
        };
        if room.generation() != generation || room.host_present() {
            return Vec::new();
        }
        self.rooms
            .remove(room_id)
            .map(|room| room.peers.into_values().collect())
            .unwrap_or_default()
    }
}

pub struct Detach {
    pub was_host: bool,
    pub generation: u64,
}

#[must_use]
pub fn peer_channel(info: PeerInfo) -> (Peer, Receiver<Outbound>) {
    let (queue, rx) = Queue::channel();
    (Peer::new(info, queue), rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The queue bound is exact: a full queue's worth of frames fits, and the next
    /// send past it reports the peer slow instead of queueing without bound.
    #[test]
    fn a_full_queue_reports_the_peer_slow() {
        let mut registry = Registry::default();
        let info = PeerInfo {
            peer_id: "p-slow".to_string(),
            display_name: "Slow".to_string(),
            role: Role::Guest,
            awareness_client_id: None,
        };
        let (peer, _leftovers) = peer_channel(info);
        registry.create(
            NewRoom {
                id: "r-1".to_string(),
                token: "t".to_string(),
                keepalive: Keepalive::default(),
            },
            peer,
            usize::MAX,
        );
        let room = registry.room("r-1").expect("the room is minted");
        let out = Outbound::Text("{}".to_string());
        for _ in 0..MAX_QUEUE_FRAMES {
            assert!(send_all(&room.queues(None), &out).is_empty());
        }
        assert_eq!(
            send_all(&room.queues(None), &out),
            vec!["p-slow".to_string()]
        );
    }

    /// The byte bound is exact beside the frame bound: payload bytes up to the cap
    /// fit, and the next byte reports the peer slow — long before the 32nd frame.
    #[test]
    fn queued_bytes_past_the_cap_report_the_peer_slow() {
        let mut registry = Registry::default();
        let info = PeerInfo {
            peer_id: "p-slow".to_string(),
            display_name: "Slow".to_string(),
            role: Role::Guest,
            awareness_client_id: None,
        };
        let (peer, _leftovers) = peer_channel(info);
        registry.create(
            NewRoom {
                id: "r-1".to_string(),
                token: "t".to_string(),
                keepalive: Keepalive::default(),
            },
            peer,
            usize::MAX,
        );
        let room = registry.room("r-1").expect("the room is minted");
        let chunk = Outbound::Binary(vec![0xA5u8; 8 * 1024 * 1024]);
        for _ in 0..4 {
            assert!(send_all(&room.queues(None), &chunk).is_empty());
        }
        assert_eq!(
            room.peers
                .get("p-slow")
                .map(|peer| peer.queue.queued_bytes()),
            Some(MAX_QUEUE_BYTES)
        );
        let over = Outbound::Binary(vec![0xA5u8; 1]);
        assert_eq!(
            send_all(&room.queues(None), &over),
            vec!["p-slow".to_string()]
        );
    }

    /// A snapshot outlives the seat it was taken from: a peer removed after the
    /// snapshot keeps its own queue, so sending through the snapshot still lands.
    /// That is what makes eject-after-send safe — the slow peer's frames go
    /// nowhere the room still owns.
    #[test]
    fn a_snapshot_outlives_the_seat_it_was_taken_from() {
        let mut registry = Registry::default();
        let info = PeerInfo {
            peer_id: "p-ada".to_string(),
            display_name: "Ada".to_string(),
            role: Role::Host,
            awareness_client_id: None,
        };
        let (peer, mut leftovers) = peer_channel(info);
        registry.create(
            NewRoom {
                id: "r-1".to_string(),
                token: "t".to_string(),
                keepalive: Keepalive::default(),
            },
            peer,
            usize::MAX,
        );
        let queues = registry
            .room("r-1")
            .expect("the room is minted")
            .queues(None);
        assert!(registry.detach("r-1", "p-ada").is_some());
        let out = Outbound::Text("{}".to_string());
        for (peer_id, queue) in &queues {
            assert_eq!(peer_id, "p-ada");
            assert!(queue.try_queue(out.clone()));
        }
        leftovers
            .try_recv()
            .expect("the removed peer's queue holds the frame");
    }

    /// Detaching a peer the room never seated announces nothing: the first detach of
    /// a seated peer reports it, and detaching again — or one never there — is `None`.
    #[test]
    fn detaching_an_absent_peer_is_silent() {
        let mut registry = Registry::default();
        let info = PeerInfo {
            peer_id: "p-ada".to_string(),
            display_name: "Ada".to_string(),
            role: Role::Host,
            awareness_client_id: None,
        };
        let (peer, _leftovers) = peer_channel(info);
        registry.create(
            NewRoom {
                id: "r-1".to_string(),
                token: "t".to_string(),
                keepalive: Keepalive::default(),
            },
            peer,
            usize::MAX,
        );
        assert!(registry.detach("r-1", "p-ada").is_some());
        assert!(registry.detach("r-1", "p-ada").is_none());
        assert!(registry.detach("r-1", "p-never-there").is_none());
    }
}
