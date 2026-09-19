//! Connect options and the session description the server sends back.

use std::time::Duration;

use selvage_protocol as proto;

use crate::Role;
use crate::presence::AwarenessState;

/// The awareness clock this client runs: renew every `awareness_renew`, forget a remote
/// state after `awareness_expire`.
///
/// The defaults are the y-protocols ones (15 s and 30 s). A client uses the values the
/// server advertises in its reply to `session.hello` unless the caller overrides them with
/// [`crate::ConnectOptions::with_keepalive`].
#[derive(Debug, Clone, Copy)]
pub struct KeepaliveConfig {
    pub awareness_renew: Duration,
    pub awareness_expire: Duration,
}

impl From<proto::Keepalive> for KeepaliveConfig {
    fn from(advertised: proto::Keepalive) -> Self {
        Self {
            awareness_renew: Duration::from_millis(
                advertised.awareness_renew_ms,
            ),
            awareness_expire: Duration::from_millis(
                advertised.awareness_expire_ms,
            ),
        }
    }
}

impl Default for KeepaliveConfig {
    fn default() -> Self {
        Self::from(proto::Keepalive::default())
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

/// How a dropped connection is retried (`PROTOCOL.md` §9.1).
///
/// The protocol only asks that a retry be bounded; these are the reference client's
/// numbers. A refusal that a retry cannot change is terminal whatever this says.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub enabled: bool,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    /// The caller's own cap on attempts. `Some` is the caller's number and is never
    /// raised by anything a room advertises; `None` — the default — starts at
    /// [`DEFAULT_MAX_ATTEMPTS`] and is raised to cover the grace the room reports, so a
    /// client does not abandon a room that is still joinable (§9.1).
    pub max_attempts: Option<u32>,
}

/// The attempts a reconnect makes before giving up when the caller named no number of
/// its own, and the floor a grace-sized budget never goes below.
const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// The most attempts a grace-derived budget asks for: an hour of the default backoff.
/// A retry loop has to end, and a room advertising a grace past this one is advertising
/// a window the client does not keep retrying through (§9.1). It is the same cap the
/// shared engine applies.
const MAX_GRACE_ATTEMPTS: u32 = 360;

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(10),
            max_attempts: None,
        }
    }
}

impl ReconnectPolicy {
    /// The attempts a reconnect makes for a room that reported `grace` — or reported none,
    /// when it is `None`.
    ///
    /// A caller that named its own number gets exactly that. Otherwise the budget is the
    /// count of this policy's own delays whose sum first reaches the grace, floored at
    /// [`DEFAULT_MAX_ATTEMPTS`] and capped, so neither a tiny delay nor a huge advertised
    /// grace turns the retry loop into an unbounded one (§9.1).
    #[must_use]
    pub fn budget_for_grace(&self, grace: Option<Duration>) -> u32 {
        let Some(named) = self.max_attempts else {
            let covered =
                grace.map_or(0, |window| attempts_for_grace(window, self));
            return DEFAULT_MAX_ATTEMPTS.max(covered);
        };
        named
    }
}

/// How many delays of this policy's backoff fit in `grace`. The count is capped, so a
/// policy with a tiny delay cannot turn a large advertised grace into a long loop.
fn attempts_for_grace(grace: Duration, policy: &ReconnectPolicy) -> u32 {
    // A zero delay would make the sum never advance; one millisecond is the floor the
    // cap is sized against.
    let initial = policy.initial_delay.max(Duration::from_millis(1));
    let ceiling = policy.max_delay.max(Duration::from_millis(1));
    let mut waited = Duration::ZERO;
    let mut attempts: u32 = 0;
    while waited < grace && attempts < MAX_GRACE_ATTEMPTS {
        let factor = 1u32.checked_shl(attempts).unwrap_or(u32::MAX);
        waited =
            waited.saturating_add(initial.saturating_mul(factor).min(ceiling));
        attempts = attempts.saturating_add(1);
    }
    attempts
}

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Scheme and authority, without the `/session` path.
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
    /// Overrides the awareness clock the server advertises. `None` — the default — uses the
    /// server's values, so both sides of the session measure awareness the same way.
    pub keepalive: Option<KeepaliveConfig>,
    /// How a dropped connection is retried. `enabled` turns reconnection off; the
    /// fields override the defaults. An unset `max_attempts` is not a default of five:
    /// it is sized from the grace the room reports, so the retry spans the window the
    /// room survives its host's absence (`PROTOCOL.md` §9.1).
    pub reconnect: ReconnectPolicy,
    /// The awareness state to publish as soon as the session is seated, published **verbatim**:
    /// its anchors are not checked against this replica and not converted from offsets, which
    /// is what a caller that already holds anchored state — a reconnect, say — needs. The
    /// caller is therefore responsible for the replica holding the documents it names; until it
    /// does, peers resolve nothing. [`ConnectOptions::with_awareness`] sets this.
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
            keepalive: None,
            reconnect: ReconnectPolicy::default(),
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

    /// Overrides the server's advertised awareness clock. The server is the authority for
    /// a session's keepalive; this exists for callers that have to run the clock faster,
    /// such as tests.
    #[must_use]
    pub const fn with_keepalive(
        mut self,
        renew: Duration,
        expire: Duration,
    ) -> Self {
        self.keepalive = Some(KeepaliveConfig {
            awareness_renew: renew,
            awareness_expire: expire,
        });
        self
    }

    /// Overrides how a dropped connection is retried. A policy with `enabled: false`
    /// leaves reconnection to the caller.
    #[must_use]
    pub const fn with_reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// Publishes `awareness` verbatim when the session is seated, anchors and all —
    /// [`SyncEngine::set_awareness`](crate::SyncEngine::set_awareness) is the path that anchors
    /// offsets against this replica and withholds what it cannot anchor. Use this one to resume
    /// previously published anchors; the replica has to hold the documents they name for a peer
    /// to resolve them (`PROTOCOL.md` §8.1).
    #[must_use]
    pub fn with_awareness(mut self, awareness: AwarenessState) -> Self {
        self.initial_awareness = awareness;
        self
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ReconnectPolicy;

    fn fast() -> ReconnectPolicy {
        ReconnectPolicy {
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(50),
            ..ReconnectPolicy::default()
        }
    }

    /// The reference default: five attempts when the room has advertised nothing to
    /// size against.
    #[test]
    fn no_grace_leaves_the_policy_its_own_attempts() {
        let policy = ReconnectPolicy::default();
        assert_eq!(policy.budget_for_grace(None), 5);
        assert_eq!(policy.budget_for_grace(Some(Duration::ZERO)), 5);
    }

    /// The reference grace, 30 s, takes seven attempts at the default backoff: the sum
    /// of the delays in front of them passes 30 s on the seventh.
    #[test]
    fn the_reference_grace_needs_seven_attempts() {
        assert_eq!(
            ReconnectPolicy::default()
                .budget_for_grace(Some(Duration::from_secs(30))),
            7
        );
    }

    /// A grace longer than the policy's own five attempts raises them; a grace too
    /// short to cover even one is never a reason to lower them.
    #[test]
    fn a_grace_raises_the_budget_and_never_lowers_it() {
        let policy = fast();
        assert_eq!(
            policy.budget_for_grace(Some(Duration::from_millis(500))),
            10
        );
        assert_eq!(
            policy.budget_for_grace(Some(Duration::from_millis(100))),
            5
        );
    }

    /// A caller that named its own number keeps it, whatever the room advertises —
    /// the escape hatch for a client that wants more or fewer tries than the grace
    /// implies.
    #[test]
    fn a_callers_own_attempts_win() {
        let policy = ReconnectPolicy {
            max_attempts: Some(6),
            ..fast()
        };
        assert_eq!(
            policy.budget_for_grace(Some(Duration::from_millis(500))),
            6
        );
        assert_eq!(policy.budget_for_grace(None), 6);
    }

    /// A server advertising a grace nobody could retry through gets the capped budget
    /// rather than an unbounded loop — the cap is the hour the default backoff's
    /// delays would span.
    #[test]
    fn a_grace_past_the_cap_is_capped() {
        let policy = ReconnectPolicy::default();
        // 10^15 ms, the value the shared engine's test uses for a grace nobody could
        // retry through.
        let far = Duration::from_secs(1_000_000_000_000);
        assert_eq!(policy.budget_for_grace(Some(far)), 360);
        // And a policy with no delay at all still terminates.
        let instant = ReconnectPolicy {
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            ..ReconnectPolicy::default()
        };
        assert_eq!(instant.budget_for_grace(Some(far)), 360);
    }
}
