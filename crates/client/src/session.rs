//! Connect options and the session description the server sends back.

use std::time::Duration;

use selvage_protocol as proto;

use crate::Error;
use crate::Role;
use crate::peer::{PeerInvite, wire_address};
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
    /// Initial backoff, floored at one millisecond wherever it is used: a caller that
    /// named no delay retries rather than spins.
    pub initial_delay: Duration,
    /// Backoff ceiling, floored at one millisecond wherever it is used.
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
    /// The wait before attempt `attempts` (zero-based): the initial delay doubling to the
    /// ceiling, both floored at a millisecond.
    ///
    /// The retry budget is sized against this same function, so the window a retry was
    /// measured against is the window it spans.
    pub(crate) fn backoff_delay(&self, attempts: u32) -> Duration {
        let initial = self.initial_delay.max(Duration::from_millis(1));
        let ceiling = self.max_delay.max(Duration::from_millis(1));
        let factor = 1u32.checked_shl(attempts).unwrap_or(u32::MAX);
        initial.saturating_mul(factor).min(ceiling)
    }

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
    let mut waited = Duration::ZERO;
    let mut attempts: u32 = 0;
    while waited < grace && attempts < MAX_GRACE_ATTEMPTS {
        waited = waited.saturating_add(policy.backoff_delay(attempts));
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
    /// The invite's fragment, read as `PROTOCOL.md` §5.1's two keys and the address it names.
    ///
    /// It is `Some` exactly when the link carried a fragment with both values, which is what
    /// makes the link a `selvage/2` one: this engine cannot seal a frame, so
    /// [`SyncEngine::connect`](crate::SyncEngine::connect) refuses options that carry these and
    /// [`RelaySession`](crate::relay::RelaySession) is what joins one.
    pub sealed_invite: Option<PeerInvite>,
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
    /// Both of `PROTOCOL.md` §5.1's forms are read — the connection URL and the page link —
    /// and a fragment is read rather than dropped: a link that carries §5.1's two keys sets
    /// [`ConnectOptions::sealed_invite`], which is how a caller learns it is holding a
    /// `selvage/2` invite. The version itself is the fragment's presence and nothing else, so
    /// a link without one stays a `selvage/1` connection.
    ///
    /// Returns `None` when the URL does not address a session endpoint, does not carry both a
    /// room and a token, or carries a fragment that is not §5.1's two keys. The last of those
    /// is §5.1's local refusal and not a link dialled without its keys;
    /// [`ConnectOptions::read_invite_url`] is this reading with the reason it refused.
    #[must_use]
    pub fn from_invite_url(
        url: &str,
        display_name: impl Into<String>,
    ) -> Option<Self> {
        Self::read_invite_url(url, display_name).ok()
    }

    /// Reads an invite link, or says in the client's own words what the link is missing.
    ///
    /// This is the same reading [`ConnectOptions::from_invite_url`] performs, with the
    /// sentence a refusal carries rather than a bare `None`. The fragment is stripped before
    /// anything reads the query, so neither the room nor the token these options carry is
    /// any part of it, and it is not put back into the socket URL either.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invite`] when the link does not address a session endpoint or does
    /// not carry both a room and a token, and [`Error::InvalidInvite`] when the link carries
    /// a fragment that is not §5.1's room key and host key.
    pub fn read_invite_url(
        url: &str,
        display_name: impl Into<String>,
    ) -> Result<Self, Error> {
        // §5.1: the fragment is the one part of a link a user agent never puts in a request,
        // so it is split off here, before anything reads the query. A reader that handed the
        // whole link on would find the fragment's first `&` and glue `k` onto `token`, and
        // the room key would leave in the socket URL the server is dialled on.
        let address = url
            .split_once('#')
            .map_or(url, |(address, _fragment)| address);
        let parsed = proto::parse_session_url(&wire_address(address))
            .ok_or_else(|| Error::Invite(address.to_string()))?;
        let (Some(room), Some(token)) = (parsed.join.room, parsed.join.token)
        else {
            return Err(Error::Invite(address.to_string()));
        };
        let mut options = Self::new(parsed.base, display_name);
        options.room = Some(room);
        options.token = Some(token);
        options.role = Some(Role::Guest);
        if url.contains('#') {
            // §5.1: the presence of a fragment is the version, and a fragment that is present
            // but is not both keys is refused here rather than dialled as `selvage/1` — which
            // would drop the keys and put the room key in the token above on the wire.
            options.sealed_invite =
                Some(PeerInvite::parse(url).map_err(Error::InvalidInvite)?);
        }
        Ok(options)
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
            sealed_invite: None,
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

    use super::{ConnectOptions, ReconnectPolicy};
    use crate::Error;
    use crate::Role;
    use crate::sealed::{RoomKey, SessionKey, encode_key};

    /// The keys and the address a `selvage/2` invite carries, as a host mints them.
    fn sealed_link() -> String {
        format!(
            "ws://h:8080/session?room=r-1&token=t-1#k={}&h={}",
            encode_key(&RoomKey([7; 32]).0),
            SessionKey::from_seed([3; 32]).public().encode()
        )
    }

    /// §5.1: the fragment's two keys are what make a link a `selvage/2` one, so a link without
    /// one stays the version-1 connection the engine speaks. The fragment is read as the
    /// fragment on every path, so neither `room` nor `token` — the two values the socket URL
    /// is built from — carries any part of it.
    #[test]
    fn the_fragment_decides_the_version_and_a_link_without_one_stays_version_one()
     {
        let plain = ConnectOptions::from_invite_url(
            "ws://h:8080/session?room=r-1&token=t-1",
            "Ada",
        )
        .unwrap();
        assert!(plain.sealed_invite.is_none());
        assert_eq!(plain.base_url, "ws://h:8080");
        assert_eq!(plain.room.as_deref(), Some("r-1"));
        assert_eq!(plain.token.as_deref(), Some("t-1"));
        assert_eq!(plain.role, Some(Role::Guest));

        let sealed =
            ConnectOptions::from_invite_url(&sealed_link(), "Ada").unwrap();
        let invite = sealed
            .sealed_invite
            .clone()
            .expect("the fragment names both keys");
        assert_eq!(sealed.room.as_deref(), Some("r-1"));
        assert_eq!(sealed.token.as_deref(), Some("t-1"));
        assert_eq!(invite.room, "r-1");
        assert_eq!(invite.token, "t-1");
        assert_eq!(invite.room_key, RoomKey([7; 32]));
        assert_eq!(invite.host_key, SessionKey::from_seed([3; 32]).public());
        assert!(!invite.socket_url.contains('#'));

        // The page form is the same link over the scheme a browser speaks, and it resolves to
        // the same room, token and two keys.
        let page =
            sealed_link().replace("ws://h:8080/session?", "http://h:8080/?");
        let from_page = ConnectOptions::from_invite_url(&page, "Ada").unwrap();
        assert_eq!(from_page.base_url, "ws://h:8080");
        assert_eq!(from_page.room.as_deref(), Some("r-1"));
        assert_eq!(from_page.token.as_deref(), Some("t-1"));
        assert_eq!(from_page.sealed_invite, Some(invite));
    }

    /// §5.1: a client "MUST refuse an invite whose fragment is absent, whose `k` or `h` is
    /// missing, or whose `k` or `h` is not a 32-byte value — locally, and before it opens a
    /// socket". A link whose fragment is present but is not the two keys is refused with the
    /// value named, and never read as `selvage/1` — whose token would put the fragment in the
    /// socket URL the server is dialled on.
    #[test]
    fn a_fragment_that_is_not_two_keys_is_refused_by_name() {
        let link = sealed_link();
        let (address, fragment) =
            link.split_once('#').expect("the link carries a fragment");
        // A chat client loses the last character of `h` and leaves `k` whole, which is the
        // shape that used to leave the room key in `token`.
        let short = fragment
            .get(..fragment.len().saturating_sub(1))
            .unwrap_or(fragment);
        let truncated = format!("{address}#{short}");

        assert!(
            ConnectOptions::from_invite_url(&truncated, "Ada").is_none(),
            "a present-but-malformed fragment is refused, not dialled as version 1"
        );
        let refused =
            ConnectOptions::read_invite_url(&truncated, "Ada").unwrap_err();
        assert!(
            matches!(&refused, Error::InvalidInvite(reason) if reason.contains("`h`")),
            "the refusal names the value that is wrong: {refused}"
        );

        // A fragment that names no room key is refused by that name too.
        let no_room_key = format!(
            "{address}#h={}",
            SessionKey::from_seed([3; 32]).public().encode()
        );
        let refused =
            ConnectOptions::read_invite_url(&no_room_key, "Ada").unwrap_err();
        assert!(
            matches!(&refused, Error::InvalidInvite(reason) if reason.contains("`k`")),
            "the refusal names the value that is missing: {refused}"
        );
        assert!(ConnectOptions::from_invite_url(&no_room_key, "Ada").is_none());

        // The whole link is still read, keys and all.
        assert!(ConnectOptions::from_invite_url(&link, "Ada").is_some());
    }

    /// §5.1: a client "MUST NOT log" the invite's fragment, and `read_invite_url` is the
    /// path a pasted link takes. A link it refuses is named by its address — the fragment-free
    /// part — so neither key and no `#` reaches the sentence a caller prints.
    #[test]
    fn a_refused_link_is_named_by_its_address_and_never_by_its_fragment() {
        let link = sealed_link();
        let (address, fragment) =
            link.split_once('#').expect("the link has a fragment");
        let keys = (
            encode_key(&RoomKey([7; 32]).0),
            SessionKey::from_seed([3; 32]).public().encode(),
        );
        let names_no_key = |text: &str| {
            assert!(!text.contains('#'), "the fragment survived into: {text}");
            assert!(
                !text.contains(&keys.0),
                "the room key survived into: {text}"
            );
            assert!(
                !text.contains(&keys.1),
                "the host key survived into: {text}"
            );
        };

        // An ordinary truncated paste: the query lost its token, and the fragment is whole.
        let no_token = format!("ws://h:8080/session?room=r-1#{fragment}");
        let refused =
            ConnectOptions::read_invite_url(&no_token, "Ada").unwrap_err();
        assert!(matches!(&refused, Error::Invite(_)));
        names_no_key(&refused.to_string());

        // The token twice: the query is not one this reads, and the fragment is still whole.
        let twice = format!("{address}&token=t-2#{fragment}");
        let refused =
            ConnectOptions::read_invite_url(&twice, "Ada").unwrap_err();
        assert!(matches!(&refused, Error::Invite(_)));
        names_no_key(&refused.to_string());

        // And an address that is not a session endpoint at all.
        let elsewhere =
            format!("ws://h:8080/other?room=r-1&token=t-1#{fragment}");
        let refused =
            ConnectOptions::read_invite_url(&elsewhere, "Ada").unwrap_err();
        assert!(matches!(&refused, Error::Invite(_)));
        names_no_key(&refused.to_string());

        // §5.1 reaches the options a caller holds too: `ConnectOptions`' derived `Debug`
        // delegates to the invite's, so neither key is printed there either.
        let options = ConnectOptions::from_invite_url(&link, "Ada")
            .expect("the whole link is read");
        let printed = format!("{options:?}");
        assert!(
            !printed.contains(&format!("{:?}", RoomKey([7; 32])))
                && !printed.contains(&format!(
                    "{:?}",
                    SessionKey::from_seed([3; 32]).public()
                )),
            "a key the options hold was printed: {printed}"
        );
    }

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

    /// The pair the budget is sized against is the pair a retry waits: a policy with a
    /// zero delay is a burst, and a budget of 360 attempts sized against it would be 360
    /// connections with no wait at all between them.
    #[test]
    fn a_zero_delay_policy_waits_a_millisecond_per_attempt() {
        let instant = ReconnectPolicy {
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            ..ReconnectPolicy::default()
        };
        assert_eq!(instant.backoff_delay(0), Duration::from_millis(1));
        assert_eq!(
            instant.backoff_delay(400),
            Duration::from_millis(1),
            "the floor is not doubled"
        );
        // A policy with a real backoff is untouched by the floor, and stops doubling at
        // its ceiling.
        let policy = ReconnectPolicy::default();
        assert_eq!(policy.backoff_delay(0), Duration::from_millis(500));
        assert_eq!(policy.backoff_delay(3), Duration::from_secs(4));
        assert_eq!(policy.backoff_delay(9), Duration::from_secs(10));
    }
}
