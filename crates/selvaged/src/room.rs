//! Memory-only session state: rooms, membership, the open-document set, and the
//! host-reconnect grace period.
//!
//! Nothing here looks at document or awareness payloads. A room knows only which
//! peers are connected and which documents they have declared open.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use selvage_protocol::{Keepalive, PeerInfo, Role};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

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
}

impl Peer {
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
    /// Order in which documents were first opened; the set survives peers leaving.
    pub documents: Vec<String>,
    host: Option<String>,
    pub host_deadline: Option<Instant>,
    /// Bumped whenever the host attaches or detaches, so a stale grace timer cannot
    /// destroy a room that has been reclaimed.
    generation: u64,
}

impl Room {
    pub fn host_present(&self) -> bool {
        self.host
            .as_ref()
            .is_some_and(|id| self.peers.contains_key(id))
    }

    pub fn peers_except(&self, peer_id: &str) -> Vec<PeerInfo> {
        self.peers
            .values()
            .filter(|p| p.info.peer_id != peer_id)
            .map(|p| p.info.clone())
            .collect()
    }

    pub fn broadcast(&self, except: Option<&str>, out: Outbound) {
        for peer in self.peers.values() {
            if Some(peer.info.peer_id.as_str()) == except {
                continue;
            }
            peer.send(out.clone());
        }
    }

    pub fn attach_host(&mut self, peer_id: &str) {
        self.host = Some(peer_id.to_string());
        self.host_deadline = None;
        self.generation += 1;
    }

    pub fn detach_host(&mut self, grace: Duration) -> u64 {
        self.host = None;
        self.host_deadline = Some(Instant::now() + grace);
        self.generation += 1;
        self.generation
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn open_document(&mut self, path: &str) -> bool {
        if self.documents.iter().any(|p| p == path) {
            false
        } else {
            self.documents.push(path.to_string());
            true
        }
    }

    pub fn close_document(&mut self, path: &str) -> bool {
        let before = self.documents.len();
        self.documents.retain(|p| p != path);
        self.documents.len() != before
    }
}

/// Outcome of seating a connection in a room.
pub enum Seat {
    /// The connection minted the room; only the minting host is told the token.
    Created { room_id: String, token: String },
    Joined,
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(
        &mut self,
        room_id: String,
        token: String,
        keepalive: Keepalive,
        host: Peer,
    ) -> Seat {
        let mut room = Room {
            id: room_id.clone(),
            token: token.clone(),
            keepalive,
            peers: HashMap::new(),
            documents: Vec::new(),
            host: None,
            host_deadline: None,
            generation: 0,
        };
        room.peers.insert(host.info.peer_id.clone(), host.clone());
        room.attach_host(&host.info.peer_id);
        self.rooms.insert(room_id.clone(), room);
        Seat::Created { room_id, token }
    }

    /// Seats a connection in an existing room. `role` is what the client claimed;
    /// the host role can only be taken while the room is between host connections.
    pub fn admit(
        &mut self,
        room_id: &str,
        token: Option<&str>,
        role: Role,
        peer: Peer,
    ) -> Result<Seat, SeatError> {
        let room = self.rooms.get_mut(room_id).ok_or(SeatError::Unknown)?;
        if Some(room.token.as_str()) != token {
            return Err(SeatError::TokenMismatch);
        }
        if role == Role::Host {
            if room.host_present() {
                return Err(SeatError::HostPresent);
            }
            room.attach_host(&peer.info.peer_id);
        }
        room.peers.insert(peer.info.peer_id.clone(), peer);
        Ok(Seat::Joined)
    }

    pub fn peer(&self, room_id: &str, peer_id: &str) -> Option<&Peer> {
        self.rooms.get(room_id)?.peers.get(peer_id)
    }

    /// Detaches a peer. Returns whether it was the host, so the caller can arm the
    /// grace timer, and whether the room became empty.
    pub fn detach(&mut self, room_id: &str, peer_id: &str, grace: Duration) -> Option<Detach> {
        let room = self.rooms.get_mut(room_id)?;
        let was_host = room.host.as_deref() == Some(peer_id);
        room.peers.remove(peer_id);
        let generation = if was_host {
            room.detach_host(grace)
        } else {
            room.generation()
        };
        Some(Detach {
            was_host,
            generation,
            room_empty: room.peers.is_empty(),
        })
    }

    pub fn room(&self, room_id: &str) -> Option<&Room> {
        self.rooms.get(room_id)
    }

    pub fn room_mut(&mut self, room_id: &str) -> Option<&mut Room> {
        self.rooms.get_mut(room_id)
    }

    /// Tears a room down if it is still host-less at `generation`, returning the
    /// peers that were in it so the caller can tell them.
    pub fn reap_if_host_absent(&mut self, room_id: &str, generation: u64) -> Vec<Peer> {
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

    pub fn remove(&mut self, room_id: &str) -> Option<Room> {
        self.rooms.remove(room_id)
    }
}

pub struct Detach {
    pub was_host: bool,
    pub generation: u64,
    pub room_empty: bool,
}

pub fn peer_channel(info: PeerInfo) -> (Peer, tokio::sync::mpsc::UnboundedReceiver<Outbound>) {
    let (tx, rx) = unbounded_channel();
    (Peer { info, tx }, rx)
}
