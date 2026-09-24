//! Selvage client: the sealed `selvage/2` session — a relay over a socket, the peer session
//! that decides what a frame means, and the frame layer that seals and verifies it.
//!
//! ```no_run
//! use std::sync::Arc;
//! use selvage_client::{RelayHostOptions, RelaySession};
//!
//! # async fn example() -> Result<(), selvage_client::Error> {
//! let session = RelaySession::host(RelayHostOptions {
//!     base_url: "ws://127.0.0.1:8080".to_string(),
//!     display_name: "Ada".to_string(),
//!     listing: Arc::new(Vec::new),
//!     room_key: None,
//!     host_seed: None,
//!     store: None,
//!     client: None,
//!     keepalive: None,
//! })
//! .await?;
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod host;
pub mod peer;
pub mod relay;
pub mod sealed;
pub mod session;

pub use selvage_protocol::{Keepalive, PeerInfo, WIRE_VERSION};

pub use crate::error::Error;
pub use crate::host::{
    HostOptions, HostProducer, HostPublication, HostReason, HostStore,
    ListingSource, PersistedHost,
};
pub use crate::peer::{PeerInvite, PeerOptions, PeerSession};
pub use crate::relay::{
    RelayEnding, RelayEvent, RelayHostOptions, RelayJoinOptions, RelayPeer,
    RelaySession, RelaySessionInfo,
};
pub use crate::session::KeepaliveConfig;
