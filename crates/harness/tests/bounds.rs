//! The shapes behind the size bounds: a multi-thousand-path listing and a large-file
//! sync, measured with the workspace's own `yrs` and JSON. Totals are reported for the
//! record and pinned with slack, so a workload or encoding change that moves them shows
//! up where the bounds are decided (`MAX_GRANT_BYTES` and `MAX_FRAME_BYTES` in
//! `selvaged`); the clearance itself — legitimate shapes passing, over-bound shapes
//! refused — is pinned in `session.rs`.

use selvage_protocol as proto;
use yrs::sync::{Message as YMessage, SyncMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact};

/// Wires an update the way the client engine sends one: a single `Sync::Update`
/// y-protocols message in one binary frame (`engine.rs` `apply_edit`, and the
/// full-state reply to a newcomer's `SyncStep1`).
fn wired(update: Vec<u8>) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    YMessage::Sync(SyncMessage::Update(update)).encode(&mut encoder);
    encoder.to_vec()
}

/// A large-file sync: a single 4 MiB insert wires to about its text bytes, clearing the
/// 8 MiB frame bound the way the 4 MiB convergence test in `session.rs` shows end to
/// end.
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

/// A multi-thousand-path listing: the 25,000-path shape a large checkout shares, and
/// 100,000 typical paths — the most the count cap admits — both inside the 4 MiB grant
/// budget the way the publishing test in `session.rs` shows end to end.
#[test]
fn working_tree_listings_measure_under_the_grant_budget() {
    let typical = [
        "src/main.rs",
        "crates/selvaged/src/net/session.rs",
        "packages/foo/src/components/Thing.tsx",
        "docs/studies/client-command-parity.md",
    ];
    let total = |count: usize| {
        (0..count)
            .map(|n| format!("{}{n:06}", typical[n % typical.len()]).len())
            .sum::<usize>()
    };
    let large = total(25_000);
    eprintln!("a 25,000-path listing carries {large} path bytes");
    assert_eq!(large, 893_750);
    let widest = total(100_000);
    eprintln!("a 100,000-path listing carries {widest} path bytes");
    assert_eq!(widest, 3_575_000);
    assert!(
        widest < 4 * 1024 * 1024,
        "the widest listing the count cap admits fits the byte budget"
    );
}

/// The largest server-generated echoes, which is why frames alone cannot bound memory:
/// a full open-document set (1024 paths of 4096 bytes) in a `doc.opened` event, a
/// `room.joined` for 128 max-named peers beside that same set, and a `doc.granted`
/// carrying the widest listing the budget admits.
#[test]
fn full_set_echoes_wire_to_about_4_mib() {
    let documents = vec!["d".repeat(4096); 1024];
    let opened = proto::ServerMessage::event(
        "doc.opened",
        serde_json::json!({
            "peer_id": "p-0123456789abcdef",
            "path": "d".repeat(100),
            "documents": documents,
        }),
    )
    .to_text()
    .expect("serializes")
    .len();
    eprintln!("a doc.opened event with a full set wires to {opened} bytes");
    assert!(
        (4 * 1024 * 1024..5 * 1024 * 1024).contains(&opened),
        "the full-set echo dominates the queue: {opened}"
    );

    let peers: Vec<proto::PeerInfo> = (0..128)
        .map(|n| proto::PeerInfo {
            awareness_client_id: Some(n),
            display_name: "n".repeat(32),
            peer_id: format!("p-{n:016x}"),
            role: proto::Role::Guest,
        })
        .collect();
    let joined = proto::SessionParams {
        capabilities: vec!["sync/1".to_string()],
        documents,
        keepalive: proto::Keepalive::default(),
        peers,
        room_id: "r-abcdef012345".to_string(),
        self_peer: proto::PeerInfo {
            awareness_client_id: Some(999),
            display_name: "n".repeat(32),
            peer_id: "p-ffffffffffffffff".to_string(),
            role: proto::Role::Host,
        },
        token: None,
    };
    let joined_len = proto::ServerMessage::event(
        "room.joined",
        serde_json::to_value(&joined).expect("serializes"),
    )
    .to_text()
    .expect("serializes")
    .len();
    eprintln!(
        "a room.joined event with 128 peers and a full set wires to {joined_len} bytes"
    );
    assert!(
        (4 * 1024 * 1024..5 * 1024 * 1024).contains(&joined_len),
        "the full-room join dominates the queue: {joined_len}"
    );

    let typical = [
        "src/main.rs",
        "crates/selvaged/src/net/session.rs",
        "packages/foo/src/components/Thing.tsx",
        "docs/studies/client-command-parity.md",
    ];
    let widest: Vec<String> = (0..100_000)
        .map(|n| format!("{}{n:06}", typical[n % typical.len()]))
        .collect();
    let granted = proto::ServerMessage::event(
        "doc.granted",
        serde_json::json!({ "paths": widest }),
    )
    .to_text()
    .expect("serializes")
    .len();
    eprintln!(
        "a doc.granted event with the widest listing wires to {granted} bytes"
    );
    assert!(
        granted < 5 * 1024 * 1024,
        "the widest grant echo stays near the budget: {granted}"
    );
}
