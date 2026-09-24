//! The shapes behind the size bounds: the wire size of a document sync, measured with the
//! workspace's own `yrs`. Totals are reported for the record and pinned with slack, so a
//! workload or encoding change that moves them shows up where the bounds are decided
//! (`MAX_FRAME_BYTES` in `selvaged`).

use yrs::sync::{Message as YMessage, SyncMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact};

/// Wires an update the way a client sends one: a single `Sync::Update` y-protocols message
/// in one binary frame — the payload a relayed sealed frame carries.
fn wired(update: Vec<u8>) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    YMessage::Sync(SyncMessage::Update(update)).encode(&mut encoder);
    encoder.to_vec()
}

/// A large-file sync: a single 4 MiB insert wires to about its text bytes, clearing the
/// 8 MiB frame bound.
#[test]
fn a_large_single_insert_wires_to_about_its_text() {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("large.dat");
    let mut txn = doc.transact_mut();
    text.insert(&mut txn, 0, &"x".repeat(4 * 1024 * 1024));
    drop(txn);
    let update = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };
    let wire_len = wired(update).len();
    eprintln!("a 4 MiB single insert wires to {wire_len} bytes");
    assert!(
        wire_len >= 4 * 1024 * 1024,
        "the wire carries the text, not a summary: {wire_len}"
    );
    assert!(
        wire_len < 5 * 1024 * 1024,
        "a bare insert carries ~30 B of overhead, not megabytes: {wire_len}"
    );
}

/// A long-edited document: 2000 inserts with 90% deleted leave 9 KB live while the
/// update still carries the history — the amplification the 8 MiB bound budgets headroom
/// for.
#[test]
fn a_long_edited_document_wires_to_multiples_of_its_text() {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("scratch.rs");
    let mut txn = doc.transact_mut();
    for round in 0..2000u32 {
        let chunk =
            format!("// scratch line {round:04} filling the document\n");
        text.insert(&mut txn, 0, &chunk);
    }
    text.remove_range(&mut txn, 0, 75_000);
    drop(txn);
    let live_len = {
        let txn = doc.transact();
        txn.get_text("scratch.rs")
            .map_or(String::new(), |t| t.get_string(&txn))
            .len()
    };
    let update = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };
    let wire_len = wired(update).len();
    eprintln!(
        "{live_len} live bytes after 2000 inserts with 90% deleted wire to {wire_len} bytes"
    );
    assert_eq!(live_len, 9000);
    assert!(
        wire_len < 8 * live_len,
        "history amplification stays single-digit: {wire_len} for {live_len} live"
    );
}
