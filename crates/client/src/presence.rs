//! Presence as defined by the session layer plus the awareness state this
//! implementation carries inside y-protocols awareness frames.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use yrs::block::ClientID;
use yrs::{Assoc, ID, IndexScope, StickyIndex};

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

/// A CRDT element, named by the client that wrote it and that client's clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemId {
    pub client: u64,
    pub clock: u32,
}

impl From<ItemId> for ID {
    fn from(id: ItemId) -> Self {
        Self::new(ClientID::new(id.client), id.clock)
    }
}

impl From<&ID> for ItemId {
    fn from(id: &ID) -> Self {
        Self {
            client: id.client.get(),
            clock: id.clock,
        }
    }
}

/// One endpoint of a selection: a CRDT anchor, never an offset.
///
/// This is the JSON shape of a yjs `RelativePosition` (`spec/PROTOCOL.md` §8.1): a scope
/// (`tname` or `type`), an optional element within it, and the side of that position. It is
/// deliberately *not* deserialised through `yrs::StickyIndex`'s own `Deserialize`, which
/// rejects the unknown keys §8.1 requires a receiver to ignore.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anchor {
    /// The element this position names. Authoritative when present; when it is absent the
    /// anchor denotes an end of the scope, chosen by `assoc`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<ItemId>,
    /// The scope: a root type name, which for Selvage is the document path.
    ///
    /// yjs sends this *alongside* `item` for every position inside a root type; `yrs` sends
    /// `item` alone. Both are conforming, so neither may be treated as malformed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tname: Option<String>,
    /// A nested type. Selvage documents are root texts, so this is never produced.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<ItemId>,
    /// `0` for the element after the position, `-1` for the one before.
    #[serde(default)]
    pub assoc: i64,
}

impl Anchor {
    /// The anchor for a resolved `yrs` position.
    #[must_use]
    pub fn from_sticky(sticky: &StickyIndex) -> Self {
        let assoc = match sticky.assoc {
            Assoc::After => 0,
            Assoc::Before => -1,
        };
        let mut anchor = Self {
            assoc,
            ..Self::default()
        };
        match sticky.scope() {
            IndexScope::Relative(id) => anchor.item = Some(id.into()),
            IndexScope::Root(name) => anchor.tname = Some(name.to_string()),
            IndexScope::Nested(id) => anchor.nested = Some(id.into()),
        }
        anchor
    }

    /// The `yrs` position this anchor names, or `None` when it is malformed: carrying both
    /// scopes at once, or nothing to position by at all. Whether it *resolves* is a separate
    /// question, and needs a replica to ask.
    ///
    /// `item` wins when it is there. `tname`/`type` are the *scope*, not a third alternative
    /// to it, so `tname` beside an `item` is the ordinary yjs shape rather than a conflict.
    #[must_use]
    pub fn to_sticky(&self) -> Option<StickyIndex> {
        if self.tname.is_some() && self.nested.is_some() {
            return None;
        }
        let scope = if let Some(id) = self.item {
            IndexScope::Relative(id.into())
        } else if let Some(name) = self.tname.as_deref() {
            IndexScope::Root(Arc::from(name))
        } else {
            let id = self.nested?;
            IndexScope::Nested(id.into())
        };
        Some(StickyIndex::new(scope, self.side()))
    }

    /// Whether this anchor's scope, if it carries one, is the document it arrived for.
    ///
    /// A `tname` next to an `item` is an extra check on that element, not a contradiction of
    /// it; an anchor with no `tname` has nothing to check here and is left to the branch test.
    #[must_use]
    pub fn names_document(&self, path: &str) -> bool {
        self.tname.as_deref().is_none_or(|name| name == path)
    }

    /// Which side of the scope this position sits on. §8.1 normalises anything other than
    /// `0` and `-1` rather than rejecting it.
    #[must_use]
    pub const fn side(&self) -> Assoc {
        if self.assoc < 0 {
            Assoc::Before
        } else {
            Assoc::After
        }
    }
}

/// A selection as the wire carries it: two CRDT anchors, never offsets.
///
/// Direction is implicit — a `head` left of the `anchor` is a selection made backwards, and a
/// caret is the two resolving to the same index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    pub anchor: Anchor,
    pub head: Anchor,
}

/// A selection as an editor speaks it: two offsets into the document text.
///
/// This is the `EditorAdapter` seam's unit, not the wire's — no offset ever reaches the wire
/// (`spec/PROTOCOL.md` §8.1). It is UTF-16 code units, because that is the unit the anchor is
/// computed from and the unit `yjs` and VS Code both count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectionOffsets {
    pub anchor: u32,
    pub head: u32,
}

impl SelectionOffsets {
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
    /// The published anchors resolved against this replica when the record was read.
    ///
    /// `None` when the peer published no selection *and* when the anchors did not resolve:
    /// §8.1 forbids manufacturing a position, so the two are one outcome for a renderer.
    /// [`Presence::anchors`] distinguishes them for a caller that needs to.
    pub resolved: Option<SelectionOffsets>,
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

    /// Where this peer's cursor is, in the offset unit of the adapter seam.
    #[must_use]
    pub const fn selection(&self) -> Option<SelectionOffsets> {
        self.resolved
    }

    /// The anchors this peer published, whether or not they resolve here.
    #[must_use]
    pub fn anchors(&self) -> Option<&Selection> {
        self.state.as_ref()?.selection.as_ref()
    }
}
