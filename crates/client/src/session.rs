//! The keepalive clocks a session runs on.

use std::time::Duration;

use selvage_protocol as proto;

/// The awareness clock a session runs: renew every `awareness_renew`, forget a remote
/// state after `awareness_expire`.
///
/// The defaults are the y-protocols ones (15 s and 30 s). A connection uses the values
/// the server advertises in its reply to `session.hello` unless the caller overrides them
/// with [`crate::RelayJoinOptions::keepalive`].
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
