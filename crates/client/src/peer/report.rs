//! What a session says it did: §13.11's decisions and counts, and the four endings.

/// What a session did with one frame: the decision `PROTOCOL.md` §13.11 observes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// It verified and what it carried was applied.
    Applied { kind: u64 },
    /// It was refused, at the first of `CANONICAL.md` §6.1's ten steps that refused it.
    Dropped { reason: String },
    /// It verified and applied nothing: §13.10's closing handed to a receiver that holds no
    /// verified state. Not a refusal — nothing was refused — and in neither list a vector
    /// asserts, which is what vector 155 pins.
    Ignored { kind: u64 },
}

/// What a session is about to publish: the kind of envelope it is, and whether `PROTOCOL.md`
/// §13.11's report counts it as a **publication** or as a §7 sync-handshake frame.
///
/// The two counts are apart because a vector asserts one of them: §13.1's step 6 obliges a
/// client that applies a state committing its own key to send a `SyncStep1`, so a single
/// number could not tell a republished request from a publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    /// The session-key announcement, `kind = 4`.
    Announcement,
    /// A holds message, `kind = 3`.
    Holds,
    /// Document content, `kind = 0`.
    Content,
    /// A `SyncStep1` or a `SyncStep2`, `kind = 0`.
    Sync,
}

impl Published {
    pub(super) const fn kind(self) -> u64 {
        match self {
            Self::Holds => 3,
            Self::Announcement => 4,
            Self::Content | Self::Sync => 0,
        }
    }

    pub(super) const fn counted(self) -> Counted {
        match self {
            Self::Content | Self::Holds | Self::Announcement => {
                Counted::Publication
            }
            Self::Sync => Counted::Handshake,
        }
    }
}

/// Which of the two counts a published frame moves (§13.11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Counted {
    Publication,
    Handshake,
}

/// One frame a session applied, with the index of the frame it was handed as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    pub frame: u64,
    pub kind: u64,
}

/// One frame a session refused, and the reason `CANONICAL.md` §6.1's vocabulary gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dropped {
    pub frame: u64,
    pub reason: String,
}

/// The four endings a session reaches on its own. What §13.10 calls "the room is destroyed"
/// is the fifth and has no frame: a client learns it as `room_unknown` or close 4001 when it
/// names the id, which is the socket's business and not this module's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ending {
    /// A `kind = 2` closing that verified and was above the mark, with a verified state
    /// below it (§13.10).
    Closing,
    /// The host-away clock passed its window (§13.8).
    HostAway,
    /// The no-state window passed with nothing applied (§13.3).
    NoState,
    /// The room's frame count reached `CANONICAL.md` §6.1's frame budget.
    FrameBudget,
}

impl Ending {
    /// The words a client says when it ends a session (§13.10 requires it to say why).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Closing => "the room closed",
            Self::HostAway => "the host has been away past its window",
            Self::NoState => "no state arrived within the no-state window",
            Self::FrameBudget => {
                "the room has sealed as many frames as its key allows; start a new room"
            }
        }
    }
}
