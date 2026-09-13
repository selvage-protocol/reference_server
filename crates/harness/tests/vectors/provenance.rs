//! Where the literal bytes in the binary vectors came from.
//!
//! Vectors 009 and 010 carry frames that cannot be written as JSON: a CRDT update and an
//! awareness update, both of which encode the *client id* of whoever wrote them. They are
//! worth keeping as exact bytes only because they are reproducible — a replica with a fixed
//! client id, a fixed edit and a fixed encoding produces them every time.
//!
//! This module rebuilds them and asserts they are what the vectors claim, so a change in the
//! y-protocols encoding fails a test that names the frame, rather than leaving a vector that
//! has quietly stopped meaning what it says. It decodes them with the reference decoder too,
//! which is the check that the state the vectors describe is the state on the wire.

use yrs::encoding::read::Cursor;
use yrs::sync::protocol::{Message as YMessage, SyncMessage};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::sync::Awareness;
use yrs::{
    ClientID, Doc, GetString, OffsetKind, Options, ReadTxn, StateVector, Text,
    Transact,
};

use super::runner::{self, Failure, Step, Vector};

const PATH: &str = "src/main.rs";
const VECTOR_SYNC: &str = "009";
const VECTOR_AWARENESS: &str = "010";

/// A replica that speaks UTF-16 code units and has a client id we chose, so what it encodes
/// is a constant.
fn fixed(client_id: u64) -> Doc {
    Doc::with_options(Options {
        client_id: ClientID::new(client_id),
        offset_kind: OffsetKind::Utf16,
        ..Options::default()
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn frame(message: &YMessage) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    message.encode(&mut encoder);
    encoder.to_vec()
}

/// The hex of every `sendBinary` step in a vector, in order.
fn sent_frames(vector: &Vector) -> Vec<String> {
    vector
        .steps
        .iter()
        .filter(|step: &&Step| step.op == "sendBinary")
        .filter_map(|step| step.hex.clone())
        .collect()
}

fn vector(id: &str) -> Result<Vector, Failure> {
    runner::load()?
        .into_iter()
        .find(|vector| vector.id == id)
        .ok_or_else(|| format!("no vector {id}").into())
}

/// One awareness frame, as this implementation's encoder writes it: publish `state` on
/// `awareness` and take the update that produces. The harness builds its frames the same way
/// (`tests/awareness.rs`), which is what makes the vector's bytes reproducible.
fn encoded(
    awareness: &mut Awareness,
    state: &str,
) -> Result<Vec<u8>, Failure> {
    awareness.set_local_state_raw(state);
    let update = awareness.update()?;
    Ok(frame(&YMessage::Awareness(update)))
}

#[test]
fn the_sync_frames_are_the_ones_a_fixed_replica_produces() {
    let held = vector(VECTOR_SYNC).expect("vector 009 loads");
    let sent = sent_frames(&held);
    assert_eq!(sent.len(), 4, "vector 009 sends four binary frames");

    // 1. A guest with an empty replica asks for the room's history: three varints, no payload.
    let step1 = frame(&YMessage::Sync(SyncMessage::SyncStep1(
        StateVector::default(),
    )));
    assert_eq!(hex(&step1), sent[0], "the SyncStep1 of an empty replica");

    // 2. The host's replica writes `a😀bc`; its whole state is the SyncStep2 the guest receives.
    let doc = fixed(1);
    let text = doc.get_or_insert_text(PATH);
    {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "a😀bc");
    }
    assert_eq!(text.get_string(&doc.transact()), "a😀bc");
    let catch_up = doc
        .transact_mut()
        .encode_state_as_update_v1(&StateVector::default());
    assert_eq!(
        hex(&frame(&YMessage::Sync(SyncMessage::SyncStep2(
            catch_up.clone()
        )))),
        sent[1],
        "the SyncStep2 that carries the whole document"
    );

    // 3. A local edit is broadcast as the delta it produced. Index 4 is a UTF-16 code unit,
    //    so it lands after the `b` — a byte offset would put it after the emoji.
    let edit_x = {
        let before = doc.transact().state_vector();
        {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 4, "X");
        }
        doc.transact_mut().encode_state_as_update_v1(&before)
    };
    assert_eq!(text.get_string(&doc.transact()), "a😀bXc");
    assert_eq!(
        hex(&frame(&YMessage::Sync(SyncMessage::Update(edit_x.clone())))),
        sent[2],
        "the host's delta"
    );

    // 4. The guest catches up, edits the merged text, and ships its own delta.
    let guest = fixed(2);
    let guest_text = guest.get_or_insert_text(PATH);
    for update in [&catch_up, &edit_x] {
        guest
            .transact_mut()
            .apply_update(yrs::Update::decode_v1(update).expect("an update"))
            .expect("the update applies");
    }
    assert_eq!(guest_text.get_string(&guest.transact()), "a😀bXc");
    let edit_bang = {
        let before = guest.transact().state_vector();
        {
            let mut txn = guest.transact_mut();
            guest_text.insert(&mut txn, 0, "!");
        }
        guest.transact_mut().encode_state_as_update_v1(&before)
    };
    assert_eq!(guest_text.get_string(&guest.transact()), "!a😀bXc");
    assert_eq!(
        hex(&frame(&YMessage::Sync(SyncMessage::Update(
            edit_bang.clone()
        )))),
        sent[3],
        "the guest's delta"
    );

    // 5. The host applies the same three frames and converges on the same text and state.
    let host = fixed(3);
    let host_text = host.get_or_insert_text(PATH);
    for held in [&catch_up, &edit_x, &edit_bang] {
        host.transact_mut()
            .apply_update(yrs::Update::decode_v1(held).expect("an update"))
            .expect("the update applies");
    }
    assert_eq!(host_text.get_string(&host.transact()), "!a😀bXc");
    assert_eq!(
        host.transact().state_vector(),
        guest.transact().state_vector(),
        "converged replicas have equal state vectors"
    );
}

#[test]
fn the_awareness_frames_are_the_ones_this_implementation_encodes() {
    let held = vector(VECTOR_AWARENESS).expect("vector 010 loads");
    let sent = sent_frames(&held);
    assert_eq!(sent.len(), 3, "vector 010 sends three binary frames");

    // A selection endpoint is a CRDT anchor, never an offset (§8.1). Three shapes appear on
    // purpose, because a receiver has to accept all three: the yjs shape, which names the
    // scope beside the element; the element alone, which is what `yrs` publishes for a
    // position inside a root type; and the scope alone, the only encoding that exists for a
    // position with no element to name.
    let yjs = format!(
        r#"{{"path":"{PATH}","selection":{{"anchor":{{"tname":"{PATH}","item":{{"client":9,"clock":4}},"assoc":0}},"head":{{"tname":"{PATH}","item":{{"client":9,"clock":4}},"assoc":0}}}}}}"#
    );
    let scope_only = format!(
        r#"{{"path":"{PATH}","selection":{{"anchor":{{"tname":"{PATH}","assoc":0}},"head":{{"tname":"{PATH}","assoc":0}}}}}}"#
    );
    let element_only = format!(
        r#"{{"path":"{PATH}","selection":{{"anchor":{{"item":{{"client":9,"clock":4}},"assoc":0}},"head":{{"item":{{"client":9,"clock":4}},"assoc":0}}}}}}"#
    );

    // Two clients, each on the awareness clock `yrs` gives it: the host publishes twice, so
    // its second frame has the newer clock that a later state must carry.
    let mut host = Awareness::new(fixed(5));
    let mut guest = Awareness::new(fixed(9));
    let frames = [
        (
            5,
            1,
            encoded(&mut host, &yjs).expect("the host's first frame"),
            &yjs,
        ),
        (
            5,
            2,
            encoded(&mut host, &scope_only).expect("the host's second frame"),
            &scope_only,
        ),
        (
            9,
            1,
            encoded(&mut guest, &element_only).expect("the guest's frame"),
            &element_only,
        ),
    ];

    for (index, (client, clock, bytes, state)) in frames.iter().enumerate() {
        assert_eq!(
            hex(bytes),
            sent[index],
            "the awareness frame of client {client} at clock {clock}"
        );

        // And it decodes back to exactly that state, which is what the vector asserts.
        let mut decoder = DecoderV1::new(Cursor::new(bytes));
        let Ok(YMessage::Awareness(read_back)) = YMessage::decode(&mut decoder)
        else {
            panic!("the frame is not an awareness update");
        };
        let entry = read_back
            .clients
            .get(&ClientID::new(*client))
            .expect("the client is in the update");
        assert_eq!(u64::from(entry.clock), *clock);
        assert_eq!(entry.json.as_ref(), *state);
    }
}
