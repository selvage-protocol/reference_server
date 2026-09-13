//! Presence as defined by the session layer plus the awareness state this
//! implementation carries inside y-protocols awareness frames.

use serde::{Deserialize, Serialize};

pub use selvage_protocol::PeerInfo;

/// The awareness state an editor adapter publishes for its local user.
///
/// y-protocols treats the awareness payload as opaque, so this shape is ours: a
/// document path plus a selection within it. Identity (the display name) deliberately
/// does *not* travel here — it lives in the session layer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwarenessState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<Selection>,
}

/// A selection rendered as character offsets into the document text.
///
/// DESIGN.md §4.3 asks for selections anchored to CRDT-relative positions. Offsets are
/// what this slice implements; see `spec/PROTOCOL.md` for the open question.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    pub anchor: u32,
    pub head: u32,
}

impl Selection {
    #[must_use]
    pub const fn caret(at: u32) -> Self {
        Self {
            anchor: at,
            head: at,
        }
    }
}

/// A remote participant's awareness, attributed to a session peer where possible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Presence {
    /// y-protocols awareness client id.
    pub client_id: u64,
    /// The session peer speaking with that awareness client id, if it is known.
    pub peer: Option<PeerInfo>,
    pub state: Option<AwarenessState>,
}

impl Presence {
    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        self.peer.as_ref().map(|p| p.display_name.as_str())
    }

    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.state.as_ref()?.path.as_deref()
    }

    #[must_use]
    pub fn selection(&self) -> Option<Selection> {
        self.state.as_ref()?.selection
    }
}
