//! The unit of a text offset, which the wire never says but every peer must agree on.
//!
//! `PROTOCOL.md` §8.1 puts *no* offset on the wire — a selection travels as CRDT anchors —
//! so the protocol fixes no unit, and an implementation fixes one at its editor-adapter seam
//! instead. This client's seam, which `insert`, `delete` and `SelectionOffsets` all speak, is
//! UTF-16 code units: that is what `yjs` counts, what every editor's `offsetAt` counts, and what
//! `yrs` calls `OffsetKind::Utf16`. Being local does not make the unit free: an anchor is
//! computed *from* an offset, so a client counting bytes or code points anchors a cursor to the
//! wrong element, and its peers then render that cursor somewhere plausible-looking and wrong
//! with nothing on the wire to reveal the disagreement.

use std::time::Duration;

use selvage_harness::{Harness, WAIT};

const PATH: &str = "src/main.rs";

/// `😀` is one code point, two UTF-16 code units and four UTF-8 bytes, so where an offset
/// lands after it differs between the three units.
#[tokio::test]
async fn a_text_offset_is_a_utf16_code_unit() {
    let harness = Harness::start(WAIT).await;
    let (host, _) = harness.host("Ada").await.expect("a host connects");
    host.open(PATH).await.expect("the document opens");
    host.insert(PATH, 0, "😀abc")
        .await
        .expect("the whole document goes in at 0");

    // UTF-16: the emoji is 0 and 1, so index 4 is between `b` and `c`.
    // Bytes:  the emoji is 0..3, so index 4 would be between `c`... and the emoji, at `a`.
    // Codepoints: index 4 would be past the end of a four-character text.
    host.insert(PATH, 4, "X")
        .await
        .expect("the insert applies at index 4");

    assert_eq!(
        host.text(PATH).await.expect("the text"),
        "😀abXc",
        "index 4 is a UTF-16 code unit; a byte offset would have produced `😀Xabc`"
    );
    assert_eq!(host.text(PATH).await.expect("the text").chars().count(), 5);
    drop(host.disconnect().await);
}

/// The same document, read back after a peer has applied every update: the unit is a
/// property of the API, not of the wire, so two replicas that exchange the same bytes agree
/// whatever either of them counts locally.
#[tokio::test]
async fn two_replicas_agree_on_the_text_whatever_they_count() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("a host connects");
    let guest = harness.join(&room, "Bob").await.expect("the guest joins");
    host.open(PATH).await.expect("the document opens");
    guest.open(PATH).await.expect("the document opens");

    host.insert(PATH, 0, "😀abc")
        .await
        .expect("the insert applies");
    let text = selvage_harness::wait_for_convergence(&host, &guest, PATH).await;
    assert_eq!(text, "😀abc");
    drop(host.disconnect().await);
    drop(guest.disconnect().await);
}
