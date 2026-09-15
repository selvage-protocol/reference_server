//! Memory-only session state: rooms, membership, the open-document set, and the
//! host-reconnect grace period.
//!
//! Nothing here looks at document or awareness payloads. A room knows only which
//! peers are connected and which documents they have declared open.

use std::collections::{BTreeSet, HashMap};

use selvage_protocol::{Keepalive, PeerInfo, Role};
use tokio::sync::mpsc::{
    UnboundedReceiver, UnboundedSender, unbounded_channel,
};

/// A frame the connection task should write out.
#[derive(Debug, Clone)]
pub enum Outbound {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Close(u16, String),
}

#[derive(Clone)]
pub struct Peer {
    pub info: PeerInfo,
    pub tx: UnboundedSender<Outbound>,
    /// The room's arrival counter when this peer was seated. `peers` carries no order in the
    /// protocol (`PROTOCOL.md` §6.2), but the frame's bytes must not be a hash artifact either,
    /// so the list is written in join order and this is what says what that is.
    joined: u64,
}

impl Peer {
    #[must_use]
    pub const fn new(info: PeerInfo, tx: UnboundedSender<Outbound>) -> Self {
        Self {
            info,
            tx,
            joined: 0,
        }
    }

    pub fn send(&self, out: Outbound) {
        // The receiver lives in the connection task; a send only fails once it is gone.
        let _ = self.tx.send(out);
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

    pub fn broadcast(&self, except: Option<&str>, out: &Outbound) {
        let others = self
            .peers
            .values()
            .filter(|peer| Some(peer.info.peer_id.as_str()) != except);
        for peer in others {
            peer.send(out.clone());
        }
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

    pub fn open_document(&mut self, peer_id: &str, path: &str) -> bool {
        self.claims_mut(peer_id).insert(path.to_string());
        if self.documents.iter().any(|p| p == path) {
            return false;
        }
        self.documents.push(path.to_string());
        true
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatError {
    Unknown,
    TokenMismatch,
    HostPresent,
}

#[derive(Default)]
pub struct Registry {
    rooms: HashMap<String, Room>,
}

impl Registry {
    /// Mints a room and seats its host in it.
    pub fn create(&mut self, new: NewRoom, host: Peer) {
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
    }

    /// Seats a connection in an existing room. The claimed role is honoured only while
    /// the room is between host connections.
    ///
    /// # Errors
    ///
    /// Returns [`SeatError::Unknown`] for a room that does not exist,
    /// [`SeatError::TokenMismatch`] for a wrong token, and [`SeatError::HostPresent`]
    /// when the host role is taken.
    pub fn admit(
        &mut self,
        claim: Claim<'_>,
        peer: Peer,
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
        if claim.role == Role::Host {
            room.attach_host(&peer.info.peer_id);
        }
        room.seat(peer);
        Ok(())
    }

    #[must_use]
    pub fn peer(&self, room_id: &str, peer_id: &str) -> Option<&Peer> {
        self.rooms.get(room_id)?.peers.get(peer_id)
    }

    /// Detaches a peer. Returns whether it was the host, so the caller can announce
    /// `host.detached`, and whether the room became empty.
    #[must_use]
    pub fn detach(&mut self, room_id: &str, peer_id: &str) -> Option<Detach> {
        let room = self.rooms.get_mut(room_id)?;
        let was_host = room.host.as_deref() == Some(peer_id);
        room.peers.remove(peer_id);
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

    #[must_use]
    pub fn remove(&mut self, room_id: &str) -> Option<Room> {
        self.rooms.remove(room_id)
    }
}

pub struct Detach {
    pub was_host: bool,
    pub generation: u64,
}

#[must_use]
pub fn peer_channel(info: PeerInfo) -> (Peer, UnboundedReceiver<Outbound>) {
    let (tx, rx) = unbounded_channel();
    (Peer::new(info, tx), rx)
}
