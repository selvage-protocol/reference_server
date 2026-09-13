//! Connect options and the session description the server sends back.

use std::time::Duration;

use selvage_protocol as proto;

use crate::Role;
use crate::presence::AwarenessState;

/// y-protocols defaults: renew every 15s, expire at 30s.
#[derive(Debug, Clone, Copy)]
pub struct KeepaliveConfig {
    pub awareness_renew: Duration,
    pub awareness_expire: Duration,
}

impl Default for KeepaliveConfig {
    fn default() -> Self {
        let base = proto::Keepalive::default();
        Self {
            awareness_renew: Duration::from_millis(base.awareness_renew_ms),
            awareness_expire: Duration::from_millis(base.awareness_expire_ms),
        }
    }
}

/// What the server said at the end of the handshake.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub room_id: String,
    /// Present only for the host that minted the room.
    pub token: Option<String>,
    pub role: Role,
    /// This connection's own peer record.
    pub peer: proto::PeerInfo,
    /// Peers that were already in the room.
    pub peers: Vec<proto::PeerInfo>,
    /// The room's open-document set at the moment of joining.
    pub documents: Vec<String>,
    pub capabilities: Vec<String>,
    pub keepalive: proto::Keepalive,
    /// The server base URL this connection was opened against, without the endpoint
    /// path. The invite URL is built from this and nothing else, so it cannot pick up
    /// the endpoint path twice.
    pub base_url: String,
}

impl SessionInfo {
    /// The invite URL for this room, as a guest would use it: the server this session
    /// is on, the room, and the token.
    ///
    /// This is the shared link the whole workflow is built on, and it is a URL a client
    /// can connect with directly — see [`ConnectOptions::from_invite_url`].
    #[must_use]
    pub fn invite_url(&self) -> Option<String> {
        let token = self.token.as_ref()?;
        Some(proto::session_url(
            &self.base_url,
            Some(&self.room_id),
            Some(token),
        ))
    }
}

/// What a guest needs to join: the room id and its invite token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invite {
    pub room: String,
    pub token: String,
}

impl Invite {
    #[must_use]
    pub fn new(room: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            room: room.into(),
            token: token.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// `ws://host:port`, without the `/session` path.
    pub base_url: String,
    pub display_name: String,
    /// Joining an existing room: its id.
    pub room: Option<String>,
    /// Joining an existing room: its invite token.
    pub token: Option<String>,
    /// Claimed role. `None` lets the server decide: host when minting, guest otherwise.
    pub role: Option<Role>,
    pub capabilities: Vec<String>,
    /// Free-form client identifier, e.g. `selvage-harness/0.1.0`.
    pub client: Option<String>,
    pub keepalive: KeepaliveConfig,
    pub initial_awareness: AwarenessState,
}

impl ConnectOptions {
    /// Mints a room; the caller becomes its host.
    #[must_use]
    pub fn host(
        base_url: impl Into<String>,
        display_name: impl Into<String>,
    ) -> Self {
        Self::new(base_url, display_name).with_role(Role::Host)
    }

    /// Joins a room with its invite token.
    #[must_use]
    pub fn guest(
        base_url: impl Into<String>,
        display_name: impl Into<String>,
        invite: Invite,
    ) -> Self {
        let mut options = Self::new(base_url, display_name);
        options.room = Some(invite.room);
        options.token = Some(invite.token);
        options.role = Some(Role::Guest);
        options
    }

    /// Connects using an invite URL exactly as `invite_url` produced it: the link
    /// carries the server, the room and the token, so pasting it is enough.
    ///
    /// Returns `None` when the URL does not address the session endpoint or does not
    /// carry both a room and a token.
    #[must_use]
    pub fn from_invite_url(
        url: &str,
        display_name: impl Into<String>,
    ) -> Option<Self> {
        let parsed = proto::parse_session_url(url)?;
        let mut options = Self::new(parsed.base, display_name);
        options.room = parsed.join.room;
        options.token = parsed.join.token;
        options.role = Some(Role::Guest);
        (options.room.is_some() && options.token.is_some()).then_some(options)
    }

    fn new(
        base_url: impl Into<String>,
        display_name: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            display_name: display_name.into(),
            room: None,
            token: None,
            role: None,
            capabilities: Vec::new(),
            client: Some(format!(
                "selvage-client/{}",
                env!("CARGO_PKG_VERSION")
            )),
            keepalive: KeepaliveConfig::default(),
            initial_awareness: AwarenessState::default(),
        }
    }

    #[must_use]
    pub const fn with_role(mut self, role: Role) -> Self {
        self.role = Some(role);
        self
    }

    #[must_use]
    pub fn with_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.capabilities = capabilities.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub const fn with_keepalive(
        mut self,
        renew: Duration,
        expire: Duration,
    ) -> Self {
        self.keepalive = KeepaliveConfig {
            awareness_renew: renew,
            awareness_expire: expire,
        };
        self
    }

    #[must_use]
    pub fn with_awareness(mut self, awareness: AwarenessState) -> Self {
        self.initial_awareness = awareness;
        self
    }
}
