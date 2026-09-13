//! The unit of a text offset, which the wire never says but every peer must agree on.
//!
//! `spec/PROTOCOL.md` §8.1 puts a selection in the awareness state as `{ anchor, head }` and
//! §12.4 records that the unit was never written down. It is UTF-16 code units: that is what
//! `yjs` counts, what every editor's `offsetAt` counts, and what `yrs` calls
//! `OffsetKind::Utf16`. An implementation that counts bytes or code points puts every cursor
//! after the first non-BMP character in a different place from its peers, and the two peers
//! show each other no cursor at all rather than the wrong one only in the happy case where
//! the document is ASCII.

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
