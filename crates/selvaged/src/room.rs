//! Memory-only session state: rooms, membership and the room's grace period.
//!
//! Nothing here looks at document or awareness payloads. A room knows only which
//! peers are connected to it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use selvage_protocol::Keepalive;
use selvage_protocol::PeerInfo;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::oneshot;

/// How many frames one connection may have queued but unwritten. Past it the peer is
/// slow: its frames are not dropped silently, the peer is disconnected and the room is
/// told `peer.left`. Frames alone cannot bound memory — one relayed frame already
/// wires to ~8 MiB — so `MAX_QUEUE_BYTES` bounds the bytes beside it and this stays as
/// the backstop for a flood of small frames.
pub const MAX_QUEUE_FRAMES: usize = 32;

/// How many payload bytes one connection may have queued but unwritten: the default for
/// [`ServerConfig::max_queue_bytes`](crate::ServerConfig::max_queue_bytes), and 32 MiB —
/// four times the largest frame a legitimate session sends (an 8 MiB update), so a
/// full-state sync plus concurrent traffic still fits. Past it the peer is slow, like
/// past the frame cap. One slow peer holds at most the configured cap in counted bytes
/// — the count includes the frame being written, released only after its send completes
/// — plus the kernel's own buffers; the 33rd frame, or the byte past the cap,
/// disconnects it instead.
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
    /// The byte cap this queue was built with, from the server's configuration.
    max_bytes: usize,
}

impl Queue {
    /// A fresh queue and its receiving end, holding at most `max_bytes` payload bytes.
    /// The writer drains the receiver and releases each frame's bytes after its send
    /// completes.
    #[must_use]
    pub fn channel(max_bytes: usize) -> (Self, Receiver<Outbound>) {
        let (tx, rx) = channel(MAX_QUEUE_FRAMES);
        (
            Self {
                tx,
                queued: Arc::new(AtomicUsize::new(0)),
                max_bytes,
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
                    .filter(|reserved| *reserved <= self.max_bytes)
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
    /// Bumped whenever a peer leaves, so a stale grace timer cannot destroy a room
    /// that has been reoccupied since the timer was armed.
    generation: u64,
    /// The arrival counter a newly seated peer takes its place from.
    arrivals: u64,
}

impl Room {
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

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Advances the generation without changing anything else, which invalidates a timer
    /// armed before it.
    pub const fn bump_generation(&mut self) -> u64 {
        self.generation = self.generation.saturating_add(1);
        self.generation
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
}

/// A room about to be minted: its id, its invite token and the keepalive it advertises.
pub struct NewRoom {
    pub id: String,
    pub token: String,
    pub keepalive: Keepalive,
}

/// A connection's claim on a room: which room and with which token.
#[derive(Debug, Clone, Copy)]
pub struct Claim<'a> {
    pub room_id: &'a str,
    pub token: Option<&'a str>,
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
    /// Mints a room and seats its host in it, returning the room's id — or `None`
    /// when the server is at its cap and nothing was minted. A colliding id is
    /// regenerated, never overwritten: at 48 bits a collision needs on the order
    /// of 2^24 rooms to become likely, but silently replacing a live room would
    /// evict its peers while their tasks still point at the id.
    #[expect(
        clippy::too_many_arguments,
        reason = "a mint names its room, host and the cap it is checked against"
    )]
    pub fn create(
        &mut self,
        mut new: NewRoom,
        host: Peer,
        max_rooms: usize,
    ) -> Option<String> {
        if self.rooms.len() >= max_rooms {
            return None;
        }
        while self.rooms.contains_key(&new.id) {
            new.id = crate::mint_room_id();
        }
        let id = new.id.clone();
        let mut room = Room {
            id: new.id.clone(),
            token: new.token,
            keepalive: new.keepalive,
            peers: HashMap::new(),
            generation: 0,
            arrivals: 0,
        };
        room.seat(host);
        self.rooms.insert(new.id, room);
        Some(id)
    }

    /// Seats a connection in an existing room.
    ///
    /// # Errors
    ///
    /// Returns [`SeatError::Unknown`] for a room that does not exist,
    /// [`SeatError::TokenMismatch`] for a wrong token, and [`SeatError::RoomFull`]
    /// when the room seats no more peers.
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
        if room.peers.len() >= max_peers {
            return Err(SeatError::RoomFull);
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

    /// Detaches a peer. `None` when the room is gone or the peer was never in it:
    /// detaching twice announces once.
    #[must_use]
    pub fn detach(&mut self, room_id: &str, peer_id: &str) -> Option<Detach> {
        let room = self.rooms.get_mut(room_id)?;
        room.peers.remove(peer_id)?;
        // Every leave advances the generation, so the timer armed by a leave that
        // emptied the room cannot outlive a later leave that armed its own: the stale
        // timer finds a generation that is no longer the room's and reaps nothing.
        let generation = room.bump_generation();
        Some(Detach {
            generation,
            // A room survives its last connection for `room_grace_ms`, so the caller arms
            // the timer on exactly this: the leave that emptied the room.
            empty: room.peers.is_empty(),
        })
    }

    /// Takes a room out of the registry outright, returning it.
    ///
    /// A mint whose handshake never reached its host has no peers to announce and no
    /// grace period to arm, and leaving it would hold a room slot for a session that does
    /// not exist. Removing it twice is [`None`].
    #[must_use]
    pub fn remove_room(&mut self, room_id: &str) -> Option<Room> {
        self.rooms.remove(room_id)
    }

    #[must_use]
    pub fn room(&self, room_id: &str) -> Option<&Room> {
        self.rooms.get(room_id)
    }

    pub fn room_mut(&mut self, room_id: &str) -> Option<&mut Room> {
        self.rooms.get_mut(room_id)
    }

    /// Tears a room down if it holds nobody and is still at `generation`, returning the
    /// peers that were (`PROTOCOL.md` §9). The timer arms when the room's **last**
    /// connection ends, so the predicate is "the room holds nobody", and the generation
    /// keeps a timer armed by an earlier empty window from reaping a room whose grace has
    /// only just started. A connection seated in the window makes this return nothing,
    /// and a later last-leave arms a fresh timer.
    #[must_use]
    pub fn reap_if_empty(
        &mut self,
        room_id: &str,
        generation: u64,
    ) -> Vec<Peer> {
        let stale = self.rooms.get(room_id).is_none_or(|room| {
            room.generation() != generation || !room.peers.is_empty()
        });
        if stale {
            return Vec::new();
        }
        self.rooms
            .remove(room_id)
            .map(|room| room.peers.into_values().collect())
            .unwrap_or_default()
    }
}

pub struct Detach {
    pub generation: u64,
    pub empty: bool,
}

#[must_use]
pub fn peer_channel(
    info: PeerInfo,
    max_bytes: usize,
) -> (Peer, Receiver<Outbound>) {
    let (queue, rx) = Queue::channel(max_bytes);
    (Peer::new(info, queue), rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(peer_id: &str, display_name: &str) -> PeerInfo {
        PeerInfo {
            peer_id: peer_id.to_string(),
            display_name: display_name.to_string(),
            awareness_client_id: None,
        }
    }

    /// The queue bound is exact: a full queue's worth of frames fits, and the next
    /// send past it reports the peer slow instead of queueing without bound.
    #[test]
    fn a_full_queue_reports_the_peer_slow() {
        let mut registry = Registry::default();
        let (peer, _leftovers) =
            peer_channel(peer("p-slow", "Slow"), MAX_QUEUE_BYTES);
        assert_eq!(
            registry
                .create(
                    NewRoom {
                        id: "r-1".to_string(),
                        token: "t".to_string(),
                        keepalive: Keepalive::default(),
                    },
                    peer,
                    usize::MAX,
                )
                .as_deref(),
            Some("r-1")
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
        let (peer, _leftovers) =
            peer_channel(peer("p-slow", "Slow"), MAX_QUEUE_BYTES);
        assert_eq!(
            registry
                .create(
                    NewRoom {
                        id: "r-1".to_string(),
                        token: "t".to_string(),
                        keepalive: Keepalive::default(),
                    },
                    peer,
                    usize::MAX,
                )
                .as_deref(),
            Some("r-1")
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
        let (peer, mut leftovers) =
            peer_channel(peer("p-ada", "Ada"), MAX_QUEUE_BYTES);
        assert_eq!(
            registry
                .create(
                    NewRoom {
                        id: "r-1".to_string(),
                        token: "t".to_string(),
                        keepalive: Keepalive::default(),
                    },
                    peer,
                    usize::MAX,
                )
                .as_deref(),
            Some("r-1")
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

    /// A colliding room id is regenerated, never overwritten: minting into an
    /// occupied id keeps both rooms, and the second mint reports the id it took.
    #[test]
    fn a_colliding_room_id_is_regenerated() {
        let mut registry = Registry::default();
        let (host, _leftovers) =
            peer_channel(peer("p-ada", "Ada"), MAX_QUEUE_BYTES);
        assert_eq!(
            registry
                .create(
                    NewRoom {
                        id: "r-1".to_string(),
                        token: "t".to_string(),
                        keepalive: Keepalive::default(),
                    },
                    host,
                    usize::MAX,
                )
                .as_deref(),
            Some("r-1")
        );
        let (guest, _leftovers) =
            peer_channel(peer("p-bob", "Bob"), MAX_QUEUE_BYTES);
        let minted = registry
            .create(
                NewRoom {
                    id: "r-1".to_string(),
                    token: "t".to_string(),
                    keepalive: Keepalive::default(),
                },
                guest,
                usize::MAX,
            )
            .expect("a colliding mint still seats");
        assert_ne!(minted, "r-1");
        assert!(registry.room("r-1").is_some(), "the first room survives");
        assert!(registry.room(&minted).is_some(), "the second room seats");
    }

    /// Detaching a peer the room never seated announces nothing: the first detach of
    /// a seated peer reports it, and detaching again — or one never there — is `None`.
    #[test]
    fn detaching_an_absent_peer_is_silent() {
        let mut registry = Registry::default();
        let (peer, _leftovers) =
            peer_channel(peer("p-ada", "Ada"), MAX_QUEUE_BYTES);
        assert_eq!(
            registry
                .create(
                    NewRoom {
                        id: "r-1".to_string(),
                        token: "t".to_string(),
                        keepalive: Keepalive::default(),
                    },
                    peer,
                    usize::MAX,
                )
                .as_deref(),
            Some("r-1")
        );
        assert!(registry.detach("r-1", "p-ada").is_some());
        assert!(registry.detach("r-1", "p-ada").is_none());
        assert!(registry.detach("r-1", "p-never-there").is_none());
    }
}
