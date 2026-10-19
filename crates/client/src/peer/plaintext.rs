//! The y-protocols side of a `kind = 0` frame: the session document, one message's
//! bytes, and whether a plaintext is document content or a handshake.

use crate::sealed::read_varuint;
use serde_json::Value;
use yrs::Doc;
use yrs::OffsetKind;
use yrs::Options;
use yrs::block::ClientID;
use yrs::sync::Message as YMessage;
use yrs::updates::encoder::Encode;
use yrs::updates::encoder::Encoder;
use yrs::updates::encoder::EncoderV1;

/// The session document §7 fixes: one `Y.Doc`, one `Y.Text` per document.
///
/// A text offset is a UTF-16 code unit, which is what `yjs`, every editor's `offsetAt` and
/// every peer on the wire use (`PROTOCOL.md` §8.1); `yrs` defaults to UTF-8 byte offsets.
///
/// `awareness_client_id` is the id the handshake announced (§8.4): y-protocols seeds one of its
/// own, and a session that published under a second id would have every peer draw this client's
/// caret under a stranger's. `ClientID` is a 53-bit value, which is the width every id on this
/// wire is read in.
pub(super) fn peer_doc(awareness_client_id: Option<u64>) -> Doc {
    const YJS_ID_BITS: u64 = 53;
    let client_id = awareness_client_id.map_or_else(ClientID::random, |id| {
        ClientID::new(id & ((1u64 << YJS_ID_BITS) - 1))
    });
    Doc::with_options(Options {
        offset_kind: OffsetKind::Utf16,
        client_id,
        ..Options::default()
    })
}

/// One y-protocols message as a `kind = 0` plaintext.
pub(super) fn encode_y_message(message: &YMessage) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    message.encode(&mut encoder);
    encoder.to_vec()
}

/// Whether a `kind = 0` plaintext carries document content: a sync `Update` or `SyncStep2`, as
/// opposed to the `SyncStep1` of §13.1's step 6, which shares the envelope's kind.
///
/// §13.5: content is a writer's to publish, and a `viewer` answers a request with nothing.
pub(super) fn is_content(plaintext: &[u8]) -> bool {
    plaintext.first() == Some(&0)
        && read_varuint(plaintext, 1).is_ok_and(|(tag, _)| tag == 1 || tag == 2)
}

/// A JSON value's canonical bytes. `CANONICAL.md` §2 writes an object with its members
/// ascending, which `serde_json`'s own map does (it is a `BTreeMap`).
///
/// The values this is handed are objects of strings and arrays of strings, and encoding one
/// cannot fail; an empty plaintext would be a frame no peer reads, which is `bad_payload` at
/// every receiver and a sender's bug here.
pub(super) fn canonical(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}
