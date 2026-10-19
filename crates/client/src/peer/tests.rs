use super::plaintext::encode_y_message;
use super::*;
use crate::host::{
    ABSENCE_CHARGE, HostOptions, HostStore, ListingSource, PersistedHost,
};
use crate::nesting::{MAX_ANY_DEPTH, decode_update};
use crate::sealed::{
    Envelope, Recipe, RoomKey, encode_key, opens, read_varuint, seal,
};
use selvage_protocol as proto;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use yrs::sync::awareness::{AwarenessUpdate, AwarenessUpdateEntry};
use yrs::sync::{Message as YMessage, MessageReader, SyncMessage};
use yrs::updates::decoder::DecoderV1;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact};

/// A room, a host keypair and a session keypair, all derived from constants: these are
/// unit tests of §13's rules and they need no fixture to be one.
const ROOM: &str = "R7f3a2c19";
const MILLIS: u64 = 1;

fn room_key() -> RoomKey {
    RoomKey([7; 32])
}

fn host() -> SessionKey {
    SessionKey::from_seed([3; 32])
}

/// This connection's session keypair, fixed so that a state can commit it.
fn ours() -> SessionKey {
    SessionKey::from_seed([5; 32])
}

/// A second peer's, committed as `guest` or `viewer` beside ours.
fn peer() -> SessionKey {
    SessionKey::from_seed([11; 32])
}

/// A third peer's, so all three real sessions can be driven at once.
fn third() -> SessionKey {
    SessionKey::from_seed([17; 32])
}

#[expect(
    clippy::too_many_arguments,
    reason = "what a test seals with: the signer, the kind, the counter and the plaintext"
)]
fn frame(
    signer: &SessionKey,
    kind: u64,
    counter: u64,
    plaintext: &[u8],
) -> Vec<u8> {
    let key = room_key().frame_key(ROOM);
    let recipe = Recipe {
        room_id: ROOM,
        frame_key: &key,
        kind,
        epoch: 0,
        counter,
        nonce: [9; 12],
        signer,
    };
    seal(&recipe, plaintext).unwrap().bytes()
}

fn state(
    signer: &SessionKey,
    issued: u64,
    peers: &[(&SessionKey, &str, &str)],
) -> Vec<u8> {
    let members: serde_json::Map<String, Value> = peers
        .iter()
        .map(|(key, role, seat)| {
            (
                key.public().encode(),
                json!({"peer_id": seat, "role": role}),
            )
        })
        .collect();
    let payload =
        json!({"issued": issued, "listing": ["README.md"], "peers": members});
    frame(signer, 1, 1, &serde_json::to_vec(&payload).unwrap())
}

/// A state committing this connection's own key as a `viewer` beside a committed writer.
fn viewer_beside_writer(issued: u64) -> Vec<u8> {
    state(
        &host(),
        issued,
        &[(&ours(), "viewer", "p-self"), (&peer(), "guest", "p-other")],
    )
}

fn holds(signer: &SessionKey, counter: u64, paths: &[&str]) -> Vec<u8> {
    let payload = json!({"holds": paths});
    frame(signer, 3, counter, &serde_json::to_vec(&payload).unwrap())
}

fn closing(signer: &SessionKey, issued: u64) -> Vec<u8> {
    let payload = json!({"closing": true, "issued": issued});
    frame(signer, 2, 1, &serde_json::to_vec(&payload).unwrap())
}

/// A `kind = 0` frame carrying a real Update, which is document content.
fn content(signer: &SessionKey, counter: u64, text: &str) -> Vec<u8> {
    frame(signer, 0, counter, &update_message(text))
}

/// One sync Update message inserting `text` at the start of `README.md`.
fn update_message(text: &str) -> Vec<u8> {
    let doc = Doc::new();
    let handle = doc.get_or_insert_text("README.md");
    {
        let mut txn = doc.transact_mut();
        handle.insert(&mut txn, 0, text);
    }
    let update = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    };
    encode_y_message(&YMessage::Sync(SyncMessage::Update(update)))
}

/// A guest session whose state commits its own key and `peer()`'s, both as `guest`, with
/// the handshake that state draws already taken off its queue.
fn beside_a_guest() -> PeerSession {
    let mut session = session(&["p-peer"]);
    session.tick(Duration::ZERO);
    let committing = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-peer")],
    );
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    session
}

/// Whether a published frame carries document content: a `kind = 0` plaintext whose
/// y-protocols message is a sync `Update` rather than the `SyncStep1` of §13.1's step 6,
/// which shares the envelope's kind (`CANONICAL.md` §6.1).
fn carries_an_update(frame: &[u8]) -> bool {
    let Ok(envelope) = Envelope::parse(frame) else {
        return false;
    };
    if envelope.kind != 0 {
        return false;
    }
    let Ok(plaintext) = opens(&room_key().frame_key(ROOM), ROOM, &envelope)
    else {
        return false;
    };
    plaintext.first() == Some(&0)
        && read_varuint(&plaintext, 1).is_ok_and(|(tag, _)| tag == 2)
}

/// Whether a published frame carries any y-protocols awareness message at all: §8.2's
/// renewal and §8.3's step 2 are the two ways this session would send one.
fn carries_awareness(frame: &[u8]) -> bool {
    let Ok(envelope) = Envelope::parse(frame) else {
        return false;
    };
    if envelope.kind != 0 {
        return false;
    }
    let Ok(plaintext) = opens(&room_key().frame_key(ROOM), ROOM, &envelope)
    else {
        return false;
    };
    let mut decoder = DecoderV1::from(plaintext.as_slice());
    MessageReader::new(&mut decoder)
        .map_while(Result::ok)
        .any(|message| matches!(message, YMessage::Awareness(_)))
}

fn session(roster: &[&str]) -> PeerSession {
    let options = PeerOptions {
        room_id: ROOM.to_string(),
        room_key: room_key(),
        host_key: host().public(),
        renew: Duration::from_millis(300),
        expire: Duration::from_millis(900),
        seat: Some("p-self".to_string()),
        roster: roster.iter().map(|seat| (*seat).to_string()).collect(),
        fixed_session_key: Some([5; 32]),
        declared_role: None,
        awareness_client_id: None,
        host: None,
        frame_budget: None,
    };
    PeerSession::new(&options).unwrap()
}

fn millis(count: u64) -> Duration {
    Duration::from_millis(count)
}

/// A second connection's session, fixed so a state can commit it beside ours.
fn second_session(roster: &[&str]) -> PeerSession {
    let options = PeerOptions {
        room_id: ROOM.to_string(),
        room_key: room_key(),
        host_key: host().public(),
        renew: Duration::from_millis(300),
        expire: Duration::from_millis(900),
        seat: Some("p-other".to_string()),
        roster: roster.iter().map(|seat| (*seat).to_string()).collect(),
        fixed_session_key: Some([11; 32]),
        declared_role: None,
        awareness_client_id: None,
        host: None,
        frame_budget: None,
    };
    PeerSession::new(&options).unwrap()
}

/// A third connection's session, fixed so three real peers can be driven at once.
fn third_session(roster: &[&str]) -> PeerSession {
    let options = PeerOptions {
        room_id: ROOM.to_string(),
        room_key: room_key(),
        host_key: host().public(),
        renew: Duration::from_millis(300),
        expire: Duration::from_millis(900),
        seat: Some("p-third".to_string()),
        roster: roster.iter().map(|seat| (*seat).to_string()).collect(),
        fixed_session_key: Some([17; 32]),
        declared_role: None,
        awareness_client_id: None,
        host: None,
        frame_budget: None,
    };
    PeerSession::new(&options).unwrap()
}

/// A room state with a chosen listing, which a listing-only edition needs.
#[expect(
    clippy::too_many_arguments,
    reason = "a signer, an edition, the peers and the listing are the whole of a state"
)]
fn state_listing(
    signer: &SessionKey,
    issued: u64,
    peers: &[(&SessionKey, &str, &str)],
    listing: &[&str],
) -> Vec<u8> {
    let members: serde_json::Map<String, Value> = peers
        .iter()
        .map(|(key, role, seat)| {
            (
                key.public().encode(),
                json!({"peer_id": seat, "role": role}),
            )
        })
        .collect();
    let payload =
        json!({"issued": issued, "listing": listing, "peers": members});
    frame(signer, 1, 1, &serde_json::to_vec(&payload).unwrap())
}

/// One sync `SyncStep1` message with an empty state vector: what a peer asks with.
fn step1() -> Vec<u8> {
    encode_y_message(&YMessage::Sync(SyncMessage::SyncStep1(
        StateVector::default(),
    )))
}

/// The sync subtype of a published `kind = 0` frame: 0 is a `SyncStep1`, 1 a `SyncStep2`.
fn sync_subtype(frame: &[u8]) -> Option<u64> {
    let envelope = Envelope::parse(frame).ok()?;
    if envelope.kind != 0 {
        return None;
    }
    let plaintext = opens(&room_key().frame_key(ROOM), ROOM, &envelope).ok()?;
    if plaintext.first() != Some(&0) {
        return None;
    }
    read_varuint(&plaintext, 1).ok().map(|(tag, _)| tag)
}

/// The `README.md` text a published `SyncStep2` carries, applied to a fresh replica.
fn step2_text(frame: &[u8]) -> String {
    let envelope = Envelope::parse(frame).unwrap();
    let plaintext =
        opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
    let mut decoder = DecoderV1::from(plaintext.as_slice());
    let Some(Ok(YMessage::Sync(SyncMessage::SyncStep2(update)))) =
        MessageReader::new(&mut decoder).next()
    else {
        return String::new();
    };
    let doc = Doc::new();
    let handle = doc.get_or_insert_text("README.md");
    let mut txn = doc.transact_mut();
    txn.apply_update(decode_update(&update).unwrap()).unwrap();
    handle.get_string(&txn)
}

#[test]
fn the_announcement_goes_out_first_and_nothing_else_before_a_state() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let published = session.take_outbound();
    assert_eq!(
        published.len(),
        1,
        "§13.1's step 4 is the first binary frame"
    );
    assert_eq!(session.published(), 1);
    assert_eq!(session.handshake(), 0);

    let envelope = Envelope::parse(&published[0]).unwrap();
    assert_eq!(envelope.kind, 4);
    assert_eq!(envelope.counter, 1);
    let plaintext =
        opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
    let announcement: Value = serde_json::from_slice(&plaintext).unwrap();
    assert_eq!(announcement["key"], json!(ours().public().encode()));
}

#[test]
fn an_uncommitted_key_is_re_announced_on_the_renewal_clock_and_stops_once_committed()
 {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();

    // A state that commits another peer and not this connection: §13.1's step 4 announces
    // again at once, because the renewal clock alone would wait a whole window.
    let other = state(&host(), 1, &[(&peer(), "guest", "p-other")]);
    assert_eq!(
        session.deliver(millis(MILLIS), &other),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(session.published(), 2);

    session.tick(millis(MILLIS + 299));
    assert!(
        session.take_outbound().is_empty(),
        "the clock has not passed"
    );
    session.tick(millis(MILLIS + 300));
    assert_eq!(session.take_outbound().len(), 1, "§13.1's renewal clock");
    assert_eq!(session.published(), 3);

    // A state that commits this connection's key ends the recovery.
    let committing = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(MILLIS + 301), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    session.tick(millis(MILLIS + 900));
    session.tick(millis(MILLIS + 1200));
    assert!(session.take_outbound().is_empty());
    assert_eq!(session.published(), 3);
}

#[test]
fn a_committing_state_answers_with_the_handshake_and_moves_no_publication() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();

    let committing = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(MILLIS), &committing),
        Outcome::Applied { kind: 1 }
    );
    let after = session.take_outbound();
    assert_eq!(after.len(), 1, "§13.1's step 6, once");
    assert_eq!(session.handshake(), 1);
    assert_eq!(
        session.published(),
        1,
        "a handshake frame is not a publication"
    );

    let envelope = Envelope::parse(&after[0]).unwrap();
    assert_eq!(envelope.kind, 0);
    let plaintext =
        opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
    assert_eq!(plaintext.first(), Some(&0), "message type 0: sync");
    assert_eq!(read_varuint(&plaintext, 1).unwrap().0, 0, "SyncStep1");
    assert_eq!(session.listing(), ["README.md"]);
    assert!(session.state_held());
}

#[test]
fn a_second_state_that_commits_our_key_sends_no_second_handshake() {
    // §13.1's step 6 is a handshake *once*, and a host republishes its state on every
    // `peer.joined` and on every announcement it accepts: a handshake per state would be a
    // frame per republish.
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let first = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(1), &first),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(session.handshake(), 1);

    let republished = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(2), &republished),
        Outcome::Applied { kind: 1 }
    );
    session.tick(millis(2 + 300));
    assert!(session.take_outbound().is_empty());
    assert_eq!(session.handshake(), 1, "one handshake per connection");
}

#[test]
fn an_edit_past_the_end_of_the_text_is_refused_and_not_a_panic() {
    // `yrs` answers an offset that is not there by panicking, and a client a caller can
    // kill with an index is not one a decision vector can drive.
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    let _ = session.deliver(millis(1), &state);
    assert!(session.insert("README.md", 0, "hello").unwrap());
    assert!(session.insert("README.md", 5, "!").unwrap());
    assert_eq!(session.length("README.md"), 6);
    let refused = session.insert("README.md", 7, "!");
    assert!(refused.is_err(), "past the end is refused");
    assert_eq!(session.length("README.md"), 6, "and nothing was applied");
}

#[test]
fn two_states_at_one_edition_leave_the_first_applied() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let first = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
    let second = state(&host(), 2, &[(&peer(), "guest", "p-other")]);
    assert_eq!(
        session.deliver(millis(1), &first),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        session.deliver(millis(2), &second),
        Outcome::Dropped {
            reason: "stale_issued".to_string()
        }
    );
    assert_eq!(session.applied().len(), 1);
    assert_eq!(session.dropped().len(), 1);
    // §13.3's `SHOULD` — the second state at one edition is the divergence, and a
    // state *below* the mark is an ordinary stale state. The reason is the same for both.
    assert_eq!(session.conflicts(), [1]);
}

#[test]
fn a_state_below_the_mark_is_stale_and_not_a_conflict() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let first = state(&host(), 5, &[(&ours(), "guest", "p-self")]);
    let old = state(&host(), 4, &[(&peer(), "guest", "p-other")]);
    let _ = session.deliver(millis(1), &first);
    assert_eq!(
        session.deliver(millis(2), &old),
        Outcome::Dropped {
            reason: "stale_issued".to_string()
        }
    );
    assert_eq!(session.dropped().len(), 1, "still refused, still named");
    assert!(
        session.conflicts().is_empty(),
        "an edition below the mark is not two publications at one edition"
    );
}

#[test]
fn the_same_state_is_still_refused_under_the_ignore_issued_mutation() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let first = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
    let second = state(&host(), 2, &[(&peer(), "guest", "p-other")]);
    let _ = session.deliver(millis(1), &first);
    session.mutate("ignore-issued").unwrap();
    assert_eq!(
        session.deliver(millis(2), &second),
        Outcome::Applied { kind: 1 }
    );
}

#[test]
fn a_committed_viewers_content_is_refused_and_its_own_edit_is_not_published() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    // Both keys: ours is committed, so §13.1's step 4 has nothing to re-announce, and
    // the peer's is the `viewer` whose frame is refused.
    let state = state(
        &host(),
        1,
        &[
            (&ours(), "guest", "p-self"),
            (&peer(), "viewer", "p-viewer"),
        ],
    );
    assert_eq!(
        session.deliver(millis(1), &state),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    let edit = content(&peer(), 1, "hello");
    assert_eq!(
        session.deliver(millis(2), &edit),
        Outcome::Dropped {
            reason: "unauthorised_content".to_string()
        }
    );
    assert_eq!(session.published(), 1, "nothing was published in answer");
    assert_eq!(session.text("README.md"), "", "and nothing was applied");

    // §13.6: the refusal opens an interval, and the client re-syncs in it — once per
    // renewal interval, however many content frames it refused.
    assert!(session.take_outbound().is_empty());
    session.tick(millis(2 + 300));
    let after = session.take_outbound();
    assert_eq!(after.len(), 1, "§13.6's re-sync");
    assert_eq!(session.handshake(), 2, "§13.1's step 6, then §13.6's");
    assert_eq!(
        session.published(),
        1,
        "a handshake frame is not a publication"
    );
    assert_eq!(
        Envelope::parse(&after[0]).unwrap().kind,
        0,
        "a `kind = 0` frame carrying the SyncStep1"
    );
    session.tick(millis(2 + 301));
    session.tick(millis(2 + 302));
    assert!(session.take_outbound().is_empty(), "once per interval");
}

#[test]
fn an_edit_made_before_a_committing_state_goes_out_when_it_arrives() {
    // §13.1's step 4 holds content back until a state commits this key, and a client that
    // dropped what it held back would leave the room without that edit: nothing asks the
    // room for what this replica lacks, only for what it has.
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    assert!(
        !session.insert("README.md", 0, "held back").unwrap(),
        "nothing may be published before a state commits this key"
    );
    assert_eq!(session.published(), 1, "the announcement, and no content");

    let committing = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let out = session.take_outbound();
    assert_eq!(out.len(), 2, "§13.1's step 6, then the held-back edit");
    assert_eq!(session.handshake(), 1);
    assert_eq!(session.published(), 2, "the edit is a publication");
    assert_eq!(Envelope::parse(&out[1]).unwrap().kind, 0);
}

#[test]
fn a_viewer_discards_its_own_edit_and_publishes_nothing() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&ours(), "viewer", "p-self")]);
    let _ = session.deliver(millis(1), &state);
    let _ = session.take_outbound();
    assert!(!session.insert("README.md", 0, "hello").unwrap());
    assert_eq!(session.published(), 1);
    assert_eq!(
        session.text("README.md"),
        "",
        "the disallowed edit is not even locally visible"
    );
}

#[test]
fn a_guest_publishes_its_edit_as_a_delta() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    let _ = session.deliver(millis(1), &state);
    let _ = session.take_outbound();
    assert!(session.insert("README.md", 0, "hello").unwrap());
    let out = session.take_outbound();
    assert_eq!(out.len(), 1);
    assert_eq!(session.published(), 2);
    let envelope = Envelope::parse(&out[0]).unwrap();
    assert_eq!(envelope.kind, 0);
}

/// §13.10: a session that has ended publishes nothing, whichever ending reached it. A
/// guest whose ending is the host's closing keeps a later edit in its own replica and
/// sends none of it.
#[test]
fn an_ended_session_publishes_no_edit() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(1), &state),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        session.deliver(millis(2), &closing(&host(), 2)),
        Outcome::Applied { kind: 2 }
    );
    assert_eq!(session.ending(), Some(Ending::Closing));
    let _ = session.take_outbound();
    let published = session.published();
    assert!(
        !session.insert("README.md", 0, "after the closing").unwrap(),
        "an ended session publishes nothing"
    );
    assert!(
        session.unsent.is_empty(),
        "and keeps nothing back: §13.10 publishes nothing later either, so a held set \
         would only grow for as long as the session lives"
    );
    assert!(session.take_outbound().is_empty());
    assert_eq!(session.published(), published);
    assert_eq!(
        session.text("README.md"),
        "after the closing",
        "and its own replica still holds the edit"
    );

    // The buffer stays empty however many edits follow, which is the whole of what an
    // ended session does with one.
    for _ in 0..64 {
        assert!(!session.insert("README.md", 0, "x").unwrap());
    }
    assert!(session.unsent.is_empty());
    assert!(session.take_outbound().is_empty());
    assert_eq!(session.published(), published);
}

/// §13.10: an ended session publishes nothing, and the path that could still have done it
/// is the flush §13.1's step 4 owes a state that commits this connection's key. An edit held
/// back before the ending must not be published by a state arriving after it.
#[test]
fn a_state_after_the_ending_publishes_none_of_what_was_held_back() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();

    // No state commits this key yet, so §13.1's step 4 holds the edit back.
    assert!(!session.insert("README.md", 0, "held").unwrap());
    assert_eq!(session.unsent.len(), 1, "the edit is held, not published");

    // §13.10's no-state window ends the session while the edit is still held.
    session.tick(millis(900));
    assert_eq!(session.ending(), Some(Ending::NoState));

    let committing = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(901), &committing),
        Outcome::Applied { kind: 1 }
    );
    assert!(
        !session
            .take_outbound()
            .iter()
            .any(|frame| carries_an_update(frame)),
        "the state that commits this key is the flush's own trigger, and the flush of an \
         ended session is nothing"
    );
    assert!(session.unsent.is_empty());
}

#[test]
fn a_closing_handed_to_a_state_less_client_is_ignored_and_the_state_below_it_still_applies()
 {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(1), &closing(&host(), 2)),
        Outcome::Ignored { kind: 2 }
    );
    assert_eq!(session.applied().len(), 0);
    assert_eq!(session.dropped().len(), 0);
    assert_eq!(session.ignored(), [0]);
    assert_eq!(session.ending(), None);

    let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(2), &state),
        Outcome::Applied { kind: 1 }
    );
    let again = closing(&host(), 2);
    assert_eq!(
        session.deliver(millis(3), &again),
        Outcome::Applied { kind: 2 }
    );
    assert_eq!(session.ending(), Some(Ending::Closing));
}

#[test]
fn the_same_closing_ends_a_client_that_holds_no_state_under_the_any_closing_mutation()
 {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    session.mutate("any-closing").unwrap();
    assert_eq!(
        session.deliver(millis(1), &closing(&host(), 2)),
        Outcome::Applied { kind: 2 }
    );
    assert_eq!(session.ending(), Some(Ending::Closing));
}

#[test]
fn a_lease_lapses_a_window_after_the_last_message_and_drops_no_frame() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&peer(), "guest", "p-other")]);
    let _ = session.deliver(millis(1), &state);
    let set = holds(&peer(), 1, &["README.md"]);
    assert_eq!(
        session.deliver(millis(2), &set),
        Outcome::Applied { kind: 3 }
    );
    assert_eq!(
        session.peer_holds()[&peer().public().encode()],
        ["README.md"]
    );

    session.tick(millis(2 + 899));
    assert_eq!(session.peer_holds().len(), 1, "the lease has not lapsed");
    session.tick(millis(2 + 900));
    assert!(session.peer_holds().is_empty());
    assert_eq!(session.dropped().len(), 0, "an expiry is not a refusal");
    assert_eq!(session.ending(), None, "and it is not a departure");
}

#[test]
fn a_lease_never_lapses_under_the_no_lease_mutation() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&peer(), "guest", "p-other")]);
    let _ = session.deliver(millis(1), &state);
    let _ = session.deliver(millis(2), &holds(&peer(), 1, &["README.md"]));
    session.mutate("no-lease").unwrap();
    session.tick(millis(10_000));
    assert_eq!(session.peer_holds().len(), 1);
}

#[test]
fn a_silent_peers_awareness_state_lapses_a_window_after_it_was_applied() {
    let mut session = beside_a_guest();
    let states = awareness_message(&[41], 1, "{\"cursor\":null}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &states));
    assert_eq!(session.awareness_clients(), [41]);

    session.tick(millis(2 + 899));
    assert_eq!(session.awareness_clients(), [41], "not before the window");
    session.tick(millis(2 + 900));
    assert!(
        session.awareness_clients().is_empty(),
        "§8.2: a remote state not renewed inside `awareness_expire_ms` is forgotten"
    );
    assert_eq!(session.dropped().len(), 0, "an expiry is not a refusal");
    assert_eq!(session.ending(), None, "and it is not a departure");
}

#[test]
fn a_renewed_awareness_state_is_not_forgotten() {
    let mut session = beside_a_guest();
    let first = awareness_message(&[41], 1, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &first));
    session.tick(millis(2 + 899));

    let renewed = awareness_message(&[41], 2, "{}");
    let _ = session.deliver(millis(2 + 899), &frame(&peer(), 0, 2, &renewed));
    session.tick(millis(2 + 899 + 899));
    assert_eq!(
        session.awareness_clients(),
        [41],
        "the renewal moved the window it is measured from"
    );
    session.tick(millis(2 + 899 + 900));
    assert!(
        session.awareness_clients().is_empty(),
        "and it lapses from there"
    );
}

#[test]
fn an_awareness_entry_below_the_clock_held_does_not_renew_it() {
    let mut session = beside_a_guest();
    let current = awareness_message(&[41], 7, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &current));

    // §8.2: an entry whose clock is not above the one held is ignored, and does not renew
    // the state. A stale copy of a live peer's state must not hold its cursor open.
    let replay = awareness_message(&[41], 4, "{}");
    let _ = session.deliver(millis(2 + 899), &frame(&peer(), 0, 2, &replay));
    session.tick(millis(2 + 900));
    assert!(
        session.awareness_clients().is_empty(),
        "the replay renewed nothing"
    );
}

#[test]
fn a_peer_that_expired_is_visible_again_above_its_tombstone() {
    let mut session = beside_a_guest();
    let states = awareness_message(&[41], 5, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &states));
    session.tick(millis(2 + 900));
    assert!(session.awareness_clients().is_empty(), "forgotten");

    // `yrs`'s removal leaves the id a tombstone one above the state it dropped
    // (`NOTES.md` §A.11), so the publication at the dropped clock and the one above it are
    // both ignored...
    let dropped = awareness_message(&[41], 5, "{}");
    let _ = session.deliver(millis(2 + 900), &frame(&peer(), 0, 2, &dropped));
    assert!(
        session.awareness_clients().is_empty(),
        "the dropped clock is a tombstone"
    );
    let next = awareness_message(&[41], 6, "{}");
    let _ = session.deliver(millis(2 + 900), &frame(&peer(), 0, 3, &next));
    assert!(
        session.awareness_clients().is_empty(),
        "and so is the one above it"
    );

    // ...and the renewal above both makes the peer visible again, with its window moved.
    let above = awareness_message(&[41], 7, "{}");
    let _ = session.deliver(millis(2 + 901), &frame(&peer(), 0, 4, &above));
    assert_eq!(session.awareness_clients(), [41], "visible again");
    session.tick(millis(2 + 901 + 900));
    assert!(
        session.awareness_clients().is_empty(),
        "and it lapses from the renewal"
    );
}

#[test]
fn a_departed_peers_awareness_state_goes_at_once_and_stays_gone() {
    let mut session = beside_a_guest();
    let states = awareness_message(&[41], 1, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &states));
    // 42's state arrives a window later, so the two stamps do not coincide: the tick below
    // is past 41's window and short of 42's. Stamped together they would both lapse there,
    // and the tick would then test the window rather than the departure's own bookkeeping.
    let neighbour = awareness_message(&[42], 1, "{}");
    let _ = session.deliver(millis(2 + 899), &frame(&peer(), 0, 2, &neighbour));
    assert_eq!(session.awareness_clients(), [41, 42]);

    session.forget_awareness(41);
    assert_eq!(
        session.awareness_clients(),
        [42],
        "§8.4: a departed seat's state goes at once, not at the window"
    );

    // The first tick at or after 41's window. `forget_awareness` cleared its stamp, so
    // there is nothing for the expiry to act on; without that clear this is the tick that
    // steps 41's tombstone a second time.
    session.tick(millis(2 + 900));
    assert_eq!(
        session.awareness_clients(),
        [42],
        "the expiry tick left the tombstone alone"
    );

    // If the expiry tick had stepped 41's tombstone a second time, this renewal would be
    // swallowed and the id would stay gone.
    let again = awareness_message(&[41], 3, "{}");
    let _ = session.deliver(millis(2 + 900), &frame(&peer(), 0, 3, &again));
    assert_eq!(
        session.awareness_clients(),
        [41, 42],
        "the id is welcome back"
    );
}
/// A clean replica adopted by §13.9's `viewer` resolution must carry the removal clocks as
/// well as the live states: `Awareness::update()` alone would drop every tombstone and let a
/// stale entry resurrect a peer the room has forgotten.
#[test]
fn an_adopted_baseline_keeps_a_forgotten_ids_tombstone() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    // A state committing only the peer leaves this connection's own key UNKNOWN, so the
    // local edit is speculative and captures the clean baseline a later resolution adopts.
    assert_eq!(
        session.deliver(
            millis(1),
            &state(&host(), 1, &[(&peer(), "guest", "p-other")])
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert!(!session.insert("README.md", 0, "spec").unwrap());
    assert_eq!(session.text("README.md"), "spec");

    // An awareness state, then §8.4's departure: the removal leaves a tombstone one above
    // the state it dropped.
    let states = awareness_message(&[41], 5, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &states));
    assert_eq!(session.awareness_clients(), [41]);
    session.forget_awareness(41);
    assert!(session.awareness_clients().is_empty());

    // The viewer state replaces the replica; the tombstone must move onto the new owner.
    assert_eq!(
        session.deliver(
            millis(3),
            &state(
                &host(),
                2,
                &[(&ours(), "viewer", "p-self"), (&peer(), "guest", "p-other")]
            )
        ),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        session.text("README.md"),
        "",
        "the speculative edit is discarded"
    );

    // The dropped clock and the one above it stay absent; a higher one renews.
    let dropped = awareness_message(&[41], 5, "{}");
    let _ = session.deliver(millis(4), &frame(&peer(), 0, 2, &dropped));
    assert!(
        session.awareness_clients().is_empty(),
        "the dropped clock stays a tombstone across adoption"
    );
    let equal = awareness_message(&[41], 6, "{}");
    let _ = session.deliver(millis(5), &frame(&peer(), 0, 3, &equal));
    assert!(
        session.awareness_clients().is_empty(),
        "and so does the clock above it"
    );
    let above = awareness_message(&[41], 7, "{}");
    let _ = session.deliver(millis(6), &frame(&peer(), 0, 4, &above));
    assert_eq!(
        session.awareness_clients(),
        [41],
        "a renewal above the tombstone makes the peer visible again"
    );
}

/// Adoption replaces the replica but must not disturb an unrelated live state's expiry: the
/// local stamp is this session's and survives the swap untouched.
#[test]
fn an_adopted_baseline_leaves_an_unrelated_state_on_its_own_schedule() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(1),
            &state(&host(), 1, &[(&peer(), "guest", "p-other")])
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert!(!session.insert("README.md", 0, "spec").unwrap());

    // Two peers' states, stamped a window apart: 41 is forgotten, 42 stays live.
    let fortyone = awareness_message(&[41], 5, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &fortyone));
    let fortytwo = awareness_message(&[42], 1, "{}");
    let _ = session.deliver(millis(3), &frame(&peer(), 0, 2, &fortytwo));
    assert_eq!(session.awareness_clients(), [41, 42]);
    session.forget_awareness(41);
    assert_eq!(session.awareness_clients(), [42]);

    assert_eq!(
        session.deliver(
            millis(4),
            &state(
                &host(),
                2,
                &[(&ours(), "viewer", "p-self"), (&peer(), "guest", "p-other")]
            )
        ),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        session.awareness_clients(),
        [42],
        "an unrelated live state survives adoption"
    );
    session.tick(millis(3 + 899));
    assert_eq!(
        session.awareness_clients(),
        [42],
        "not before its own window"
    );
    session.tick(millis(3 + 900));
    assert!(
        session.awareness_clients().is_empty(),
        "and it lapses on the schedule it was stamped on"
    );
}

/// The tombstone routes that are not `forget_awareness` — §8.2's expiry and an incoming
/// removal — must survive adoption too, or a stale copy resurrects a peer through the swap.
#[test]
fn an_adopted_baseline_keeps_tombstones_from_expiry_and_an_incoming_removal() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(1),
            &state(&host(), 1, &[(&peer(), "guest", "p-other")])
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert!(!session.insert("README.md", 0, "spec").unwrap());

    // 41 lives, then §8.2's expiry tick leaves it a tombstone one above its state.
    let fortyone = awareness_message(&[41], 5, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &fortyone));
    session.tick(millis(902));
    assert!(session.awareness_clients().is_empty(), "41 expired");

    // 42 is removed by an incoming null state, which leaves its own tombstone.
    let fortytwo = awareness_message(&[42], 1, "{}");
    let _ = session.deliver(millis(3), &frame(&peer(), 0, 2, &fortytwo));
    assert_eq!(session.awareness_clients(), [42]);
    let removal = awareness_message(&[42], 2, "null");
    let _ = session.deliver(millis(4), &frame(&peer(), 0, 3, &removal));
    assert!(session.awareness_clients().is_empty(), "42 removed");

    // Adoption replaces the replica; both tombstones must move onto the new owner.
    assert_eq!(
        session.deliver(millis(5), &viewer_beside_writer(2)),
        Outcome::Applied { kind: 1 }
    );
    let stale = awareness_message(&[41], 5, "{}");
    let _ = session.deliver(millis(6), &frame(&peer(), 0, 4, &stale));
    assert!(
        session.awareness_clients().is_empty(),
        "41's expiry clock stays gone"
    );
    let equal = awareness_message(&[41], 6, "{}");
    let _ = session.deliver(millis(7), &frame(&peer(), 0, 5, &equal));
    assert!(
        session.awareness_clients().is_empty(),
        "41's clock above the tombstone stays gone"
    );
    let above = awareness_message(&[41], 7, "{}");
    let _ = session.deliver(millis(8), &frame(&peer(), 0, 6, &above));
    assert_eq!(
        session.awareness_clients(),
        [41],
        "41 renews above its tombstone"
    );

    let stale42 = awareness_message(&[42], 1, "{}");
    let _ = session.deliver(millis(9), &frame(&peer(), 0, 7, &stale42));
    assert!(
        !session.awareness_clients().contains(&42),
        "42's removal clock stays gone"
    );
    let above42 = awareness_message(&[42], 3, "{}");
    let _ = session.deliver(millis(10), &frame(&peer(), 0, 8, &above42));
    assert!(
        session.awareness_clients().contains(&42),
        "42 renews above its removal"
    );
}

#[test]
fn this_client_publishes_no_awareness_state_of_its_own() {
    let mut session = beside_a_guest();
    let states = awareness_message(&[41], 1, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &states));
    let _ = session.take_outbound();
    let published = session.published();
    assert!(
        !session
            .awareness_clients()
            .contains(&session.awareness_client_id()),
        "§8.2: a client that publishes no state holds none of its own"
    );

    for clock in [302, 902, 1802, 1803] {
        session.tick(millis(clock));
        let sent = session.take_outbound();
        assert!(
            sent.iter().all(|frame| !carries_awareness(frame)),
            "no state, renewal or query goes out at {clock}"
        );
    }
    assert_eq!(session.published(), published, "no tick published anything");
}

#[test]
fn a_seated_client_with_no_state_ends_at_the_no_state_window() {
    let mut session = session(&[]);
    session.tick(millis(899));
    assert_eq!(session.ending(), None, "not before the window");
    session.tick(millis(900));
    assert_eq!(session.ending(), Some(Ending::NoState));
}

#[test]
fn the_same_client_stays_for_ever_under_the_wait_for_ever_mutation() {
    let mut session = session(&[]);
    session.mutate("wait-for-ever").unwrap();
    session.tick(millis(60_000));
    assert_eq!(session.ending(), None);
}

#[test]
fn a_state_whose_host_entry_is_unseated_ends_the_session_a_window_later() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    // The roster names no seat, and the state's `host` entry labels one: §13.8's clock.
    let state = state(&host(), 1, &[(&peer(), "host", "p-absent")]);
    assert_eq!(
        session.deliver(millis(1), &state),
        Outcome::Applied { kind: 1 }
    );
    session.tick(millis(900));
    assert_eq!(
        session.ending(),
        None,
        "one millisecond short of the window"
    );
    session.tick(millis(901));
    assert_eq!(session.ending(), Some(Ending::HostAway));
}

#[test]
fn a_peer_left_for_the_seat_the_host_entry_labels_arms_the_clock() {
    // §13.11's fixture for the host-away clock, which no decision vector carries: the
    // state labels a seat the roster has, and the roster then loses it.
    let mut session = session(&["p-host"]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&peer(), "host", "p-host")]);
    let _ = session.deliver(millis(1), &state);
    session.tick(millis(500));
    assert_eq!(session.ending(), None, "the host is seated");

    session.seat_left(millis(600), "p-host");
    session.tick(millis(600 + 899));
    assert_eq!(session.ending(), None);
    session.tick(millis(600 + 900));
    assert_eq!(session.ending(), Some(Ending::HostAway));
}

#[test]
fn a_state_that_does_not_name_a_host_arms_nothing() {
    // §13.4: a client MUST NOT guess which connection is the host's.
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&peer(), "guest", "p-other")]);
    let _ = session.deliver(millis(1), &state);
    session.tick(millis(5_000));
    assert_eq!(session.ending(), None);
}

#[test]
fn a_held_set_is_announced_wholesale_and_an_empty_one_is_a_release() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    let _ = session.deliver(millis(1), &state);
    let _ = session.take_outbound();

    // §13.7: a set that changed goes out with the change, and not at the next renewal —
    // no `tick` between the two, so nothing else can have published it.
    session.open(millis(2), "README.md");
    let out = session.take_outbound();
    assert_eq!(out.len(), 1, "a changed set goes out at once");
    let envelope = Envelope::parse(&out[0]).unwrap();
    assert_eq!(envelope.kind, 3);
    let plaintext =
        opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
    let payload: Value = serde_json::from_slice(&plaintext).unwrap();
    assert_eq!(payload["holds"], json!(["README.md"]));

    // The whole set and no delta, so the second open carries both paths.
    session.open(millis(3), "src/main.rs");
    let out = session.take_outbound();
    assert_eq!(out.len(), 1);
    let envelope = Envelope::parse(&out[0]).unwrap();
    let plaintext =
        opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
    let payload: Value = serde_json::from_slice(&plaintext).unwrap();
    assert_eq!(payload["holds"], json!(["README.md", "src/main.rs"]));

    // Opening a path that is already held changes nothing, so nothing is published.
    session.open(millis(4), "README.md");
    assert!(session.take_outbound().is_empty());

    session.release(millis(5));
    let out = session.take_outbound();
    assert_eq!(out.len(), 1, "the empty set is a change like any other");
    assert_eq!(session.held().len(), 0);
    let envelope = Envelope::parse(&out[0]).unwrap();
    let plaintext =
        opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
    let payload: Value = serde_json::from_slice(&plaintext).unwrap();
    assert_eq!(payload["holds"], json!([] as [&str; 0]));

    // The whole set replaced by one path is one message: `release` and `open` would put an
    // empty set on the wire between them, which is a release no caller asked for.
    session.open(millis(6), "README.md");
    let _ = session.take_outbound();
    session.hold_only(millis(7), "src/main.rs");
    let out = session.take_outbound();
    assert_eq!(out.len(), 1, "a set replaced in one step is one message");
    let envelope = Envelope::parse(&out[0]).unwrap();
    let plaintext =
        opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
    let payload: Value = serde_json::from_slice(&plaintext).unwrap();
    assert_eq!(payload["holds"], json!(["src/main.rs"]));
}

/// §13.7's two "at once" moments, which the tick alone does not honour: the state that
/// commits this key is where a holder's *first* holds message belongs, and a `peer.joined`
/// is where a joiner learns the room's holds.
#[test]
fn a_first_holds_message_and_a_joiner_are_answered_on_the_event() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    session.open(millis(1), "README.md");
    assert!(
        session.take_outbound().is_empty(),
        "§13.1's step 4 withholds it until a state commits this key"
    );

    let committing = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    let _ = session.deliver(millis(2), &committing);
    let out = session.take_outbound();
    let kinds: Vec<u64> = out
        .iter()
        .map(|frame| Envelope::parse(frame).unwrap().kind)
        .collect();
    assert_eq!(
        kinds,
        [0, 3],
        "§13.1's step 6 handshake, then the first holds message: no tick between them"
    );
    assert_eq!(session.handshake(), 1);
    assert_eq!(
        session.published(),
        2,
        "the announcement and the holds message; the handshake is counted apart"
    );

    // A seat is seated: the state that will commit the joiner's key goes out with the event,
    // and the room's holds behind it — a holds message resolves against the keys the applied
    // state commits (§13.4), so one that arrives before the state is refused.
    session.seat_joined(millis(3), "p-other");
    let out = session.take_outbound();
    let kinds: Vec<u64> = out
        .iter()
        .map(|frame| Envelope::parse(frame).unwrap().kind)
        .collect();
    assert_eq!(kinds, [1, 3], "the state the room holds, then the holds");
}

#[test]
fn a_state_is_re_sent_on_a_join_only_while_the_host_is_away() {
    // `PROTOCOL.md` §7.1: while the seat the `host` entry labels is seated, the host's own
    // fresh state answers a join, so this peer sends none; once that seat has gone, a new
    // seat — a returning host among them — is sent the held state, unchanged.
    let mut session = session(&["p-host"]);
    session.tick(Duration::ZERO);
    let held = state(
        &host(),
        1,
        &[(&peer(), "host", "p-host"), (&ours(), "guest", "p-self")],
    );
    let _ = session.deliver(millis(1), &held);
    let _ = session.take_outbound();

    session.seat_joined(millis(2), "p-new");
    let kinds: Vec<u64> = session
        .take_outbound()
        .iter()
        .map(|frame| Envelope::parse(frame).unwrap().kind)
        .collect();
    assert!(
        !kinds.contains(&1),
        "no state goes back while the host is seated: {kinds:?}"
    );

    session.seat_left(millis(3), "p-host");
    let _ = session.take_outbound();
    session.seat_joined(millis(4), "p-host-again");
    let out = session.take_outbound();
    assert_eq!(
        out.first().map(Vec::as_slice),
        Some(held.as_slice()),
        "the held state goes back first, unchanged"
    );
}

fn budgeted(budget: u64, host: Option<HostOptions>) -> PeerSession {
    let options = PeerOptions {
        room_id: ROOM.to_string(),
        room_key: room_key(),
        host_key: self::host().public(),
        renew: Duration::from_millis(300),
        expire: Duration::from_millis(900),
        seat: Some("p-self".to_string()),
        roster: BTreeSet::new(),
        fixed_session_key: Some([5; 32]),
        declared_role: None,
        awareness_client_id: None,
        host,
        frame_budget: Some(budget),
    };
    PeerSession::new(&options).unwrap()
}

#[test]
fn a_session_that_reaches_the_frame_budget_seals_nothing_more_and_ends() {
    // `CANONICAL.md` §6.1: the announcement is the first frame the room counts and the
    // state the second, so the handshake the state would be answered with is not sealed,
    // and the next tick ends the session and says why.
    let mut session = budgeted(2, None);
    session.tick(Duration::ZERO);
    assert_eq!(session.take_outbound().len(), 1, "the announcement");
    let _ = session.deliver(
        millis(1),
        &state(&host(), 1, &[(&ours(), "guest", "p-self")]),
    );
    assert!(
        session.take_outbound().is_empty(),
        "nothing is sealed past the budget"
    );
    session.tick(millis(2));
    assert_eq!(session.ending(), Some(Ending::FrameBudget));
    assert!(Ending::FrameBudget.as_str().contains("new room"));
    session.open(millis(3), "README.md");
    session.tick(millis(400));
    assert!(session.take_outbound().is_empty(), "and nothing after it");
    assert_eq!(FRAME_BUDGET, 1 << 31);
}

#[derive(Default)]
struct Saved(Mutex<Option<PersistedHost>>);

impl HostStore for Saved {
    fn load(&self) -> Option<PersistedHost> {
        *self.0.lock().unwrap()
    }

    fn save(&self, persisted: PersistedHost) {
        *self.0.lock().unwrap() = Some(persisted);
    }
}

fn stored_host(store: &Arc<Saved>) -> HostOptions {
    let listing: ListingSource = Arc::new(|| vec!["README.md".to_string()]);
    HostOptions {
        host_seed: [3; 32],
        listing,
        store: Some(Arc::clone(store) as Arc<dyn HostStore>),
    }
}

#[test]
fn a_host_continues_the_frame_count_it_saved() {
    // `CANONICAL.md` §6.1: the host's count is the room's, so a reload continues it rather
    // than starting at 0, and the budget is reached from there.
    let store = Arc::new(Saved::default());
    store.save(PersistedHost {
        host_seed: [3; 32],
        issued: 5,
        frames: Some(10),
    });
    // A reload is a return, so the saved count is continued with the absence charge on it.
    let mut session =
        budgeted(ABSENCE_CHARGE.saturating_add(11), Some(stored_host(&store)));
    session.tick(Duration::ZERO);
    session.tick(millis(1));
    assert_eq!(
        session.ending(),
        Some(Ending::FrameBudget),
        "from the saved count"
    );
}

#[test]
fn a_host_writes_its_moving_frame_count_once_a_renewal_interval() {
    let store = Arc::new(Saved::default());
    let mut session = budgeted(FRAME_BUDGET, Some(stored_host(&store)));
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    // The mint state's own write opened the window at 0, so the count it could not yet carry
    // is written on the first tick a window later.
    session.tick(millis(300));
    let first = store
        .load()
        .and_then(|saved| saved.frames)
        .unwrap_or_default();
    assert!(
        first > 0,
        "the frames the host sealed are written on the tick"
    );
    let _ = session.deliver(millis(301), &holds(&peer(), 1, &["README.md"]));
    session.tick(millis(600));
    let saved = store.load().unwrap();
    assert!(
        saved.frames.unwrap_or_default() > first,
        "a delivered frame is in the next write"
    );
    assert!(saved.issued > 0, "written beside `issued`");
}

#[test]
fn a_host_record_without_a_count_reads_as_a_spent_budget() {
    // `CANONICAL.md` §6.1: a record written before the count existed cannot say what the
    // room has sealed, so the host closes the room at its first tick rather than continue it.
    let store = Arc::new(Saved::default());
    store.save(PersistedHost {
        host_seed: [3; 32],
        issued: 3,
        frames: None,
    });
    let mut session = budgeted(FRAME_BUDGET, Some(stored_host(&store)));
    session.tick(Duration::ZERO);
    let kinds: Vec<u64> = session
        .take_outbound()
        .iter()
        .map(|frame| Envelope::parse(frame).unwrap().kind)
        .collect();
    assert_eq!(kinds, [2], "the closing, and nothing sealed before it");
    assert_eq!(session.ending(), Some(Ending::FrameBudget));
    assert_eq!(store.load().map(|saved| saved.issued), Some(4));
}

#[test]
fn a_host_that_closes_its_room_saves_the_count_with_the_closing_in_it() {
    let store = Arc::new(Saved::default());
    let mut session = budgeted(FRAME_BUDGET, Some(stored_host(&store)));
    session.tick(Duration::ZERO);
    let sealed = u64::try_from(session.take_outbound().len()).unwrap();
    assert!(session.close_room());
    assert_eq!(
        store.load().and_then(|saved| saved.frames),
        Some(sealed.saturating_add(1)),
        "a reload continues from the frame that ended the room"
    );
}

#[derive(Default)]
struct Counted {
    saved: Saved,
    saves: Mutex<u32>,
}

impl HostStore for Counted {
    fn load(&self) -> Option<PersistedHost> {
        self.saved.load()
    }

    fn save(&self, persisted: PersistedHost) {
        let mut saves = self.saves.lock().unwrap();
        *saves = saves.saturating_add(1);
        self.saved.save(persisted);
    }
}

#[test]
fn a_state_published_after_a_flush_counts_as_a_write_for_the_renewal_window() {
    let store = Arc::new(Counted::default());
    let listing: ListingSource = Arc::new(|| vec!["README.md".to_string()]);
    let options = HostOptions {
        host_seed: [3; 32],
        listing,
        store: Some(Arc::clone(&store) as Arc<dyn HostStore>),
    };
    let mut session = budgeted(FRAME_BUDGET, Some(options));
    session.tick(Duration::ZERO);
    session.tick(millis(300));
    // An announcement commits a key, so the host publishes a fresh state, and that write
    // carries the count at its own clock: a frame that moves the count right after it waits
    // for the next window rather than being written on the next tick.
    session.seat_joined(millis(600), "p-alice");
    let named = json!({ "key": peer().public().encode() }).to_string();
    let _ =
        session.deliver(millis(600), &frame(&peer(), 4, 1, named.as_bytes()));
    let after_state = *store.saves.lock().unwrap();
    let _ = session.deliver(millis(601), &[1, 2, 3]);
    session.tick(millis(602));
    assert_eq!(
        *store.saves.lock().unwrap(),
        after_state,
        "no write inside the window"
    );
    session.tick(millis(900));
    assert_eq!(
        *store.saves.lock().unwrap(),
        after_state.saturating_add(1),
        "one once it has passed"
    );
}

#[test]
fn a_host_that_reaches_the_frame_budget_publishes_its_closing_and_ends() {
    let listing: ListingSource = Arc::new(|| vec!["README.md".to_string()]);
    let mut session = budgeted(
        1,
        Some(HostOptions {
            host_seed: [3; 32],
            listing,
            store: None,
        }),
    );
    session.tick(Duration::ZERO);
    let out = session.take_outbound();
    let kinds: Vec<u64> = out
        .iter()
        .map(|frame| Envelope::parse(frame).unwrap().kind)
        .collect();
    session.tick(millis(1));
    let closing: Vec<u64> = session
        .take_outbound()
        .iter()
        .map(|frame| Envelope::parse(frame).unwrap().kind)
        .collect();
    assert_eq!(
        [kinds, closing].concat().last(),
        Some(&2),
        "the last frame this host seals is its closing"
    );
    assert_eq!(session.ending(), Some(Ending::FrameBudget));
}

#[test]
fn a_hold_message_from_a_key_no_state_commits_is_refused() {
    // `PROTOCOL.md` §13.4 and `CANONICAL.md` §6.1's step 4: a `kind = 3` frame resolves
    // against the keys the applied state commits, and this one commits only ours.
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let state = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    let _ = session.deliver(millis(1), &state);
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(2), &holds(&peer(), 1, &["README.md"])),
        Outcome::Dropped {
            reason: "uncommitted_key".to_string()
        }
    );
    assert!(session.peer_holds().is_empty());
}

#[test]
fn a_frame_is_refused_without_ending_the_session() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    assert_eq!(
        session.deliver(millis(1), &frame(&peer(), 3, 1, b"{}")),
        Outcome::Dropped {
            reason: "uncommitted_key".to_string()
        }
    );
    assert_eq!(session.ending(), None);
}

#[test]
fn a_denied_auth_message_costs_the_frame_its_other_messages_nothing() {
    // §8.3: an auth message is read and ignored, a denial included, and the Update behind
    // it in the same frame is applied.
    let mut session = beside_a_guest();
    let mut plaintext =
        encode_y_message(&YMessage::Auth(Some("denied".to_string())));
    plaintext.extend(update_message("hello"));
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &plaintext)),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(session.text("README.md"), "hello");
}

#[test]
fn a_frame_of_awareness_queries_draws_no_answer() {
    // §8.3: a query message is one byte, so a frame of them would otherwise draw one
    // reply each. This client answers none.
    let mut session = beside_a_guest();
    let published = session.published();
    let plaintext = vec![3u8; 4096];
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &plaintext)),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(session.take_outbound().len(), 0, "no answer at all");
    assert_eq!(session.published(), published);
}

#[test]
fn a_frame_of_sync_step1_messages_draws_one_answer() {
    // The answer to a SyncStep1 is a whole diff of the document, so one frame is answered
    // once however many it carries.
    let mut session = beside_a_guest();
    let ask = encode_y_message(&YMessage::Sync(SyncMessage::SyncStep1(
        yrs::StateVector::default(),
    )));
    let plaintext = ask.repeat(64);
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &plaintext)),
        Outcome::Applied { kind: 0 }
    );
    let answers = session.take_outbound();
    assert_eq!(answers.len(), 1, "one SyncStep2 for the whole frame");
    let envelope = Envelope::parse(&answers[0]).unwrap();
    let answer = opens(&room_key().frame_key(ROOM), ROOM, &envelope).unwrap();
    assert_eq!(answer.first(), Some(&0), "message type 0: sync");
    assert_eq!(read_varuint(&answer, 1).unwrap().0, 1, "SyncStep2");
}

/// One awareness message carrying `json` for each client id, at `clock`.
fn awareness_message(ids: &[u64], clock: u32, json: &str) -> Vec<u8> {
    let entries: Vec<(u64, u32, &str)> =
        ids.iter().map(|id| (*id, clock, json)).collect();
    awareness_entries(&entries)
}

/// One awareness message carrying each `(client id, clock, json)` entry as it stands.
fn awareness_entries(entries: &[(u64, u32, &str)]) -> Vec<u8> {
    let clients = entries
        .iter()
        .map(|(id, clock, json)| {
            let entry = AwarenessUpdateEntry {
                clock: *clock,
                json: Arc::from(*json),
            };
            (ClientID::new(*id), entry)
        })
        .collect();
    encode_y_message(&YMessage::Awareness(AwarenessUpdate { clients }))
}

#[test]
fn an_awareness_entry_at_the_maximum_clock_is_refused() {
    let mut session = beside_a_guest();
    let states = awareness_entries(&[(41, u32::MAX, "{}"), (42, 1, "{}")]);
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &states)),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        session.awareness_clients(),
        [42],
        "the entry at the maximum is refused and the rest of the message applies"
    );

    // Nothing is held for 41, so its next publication is a first state whatever clock it
    // carries, and the expiry has no clock at the maximum to step.
    let first = awareness_entries(&[(41, 1, "{}")]);
    let _ = session.deliver(millis(3), &frame(&peer(), 0, 2, &first));
    assert_eq!(session.awareness_clients(), [41, 42]);
    session.tick(millis(3 + 900));
    assert!(
        session.awareness_clients().is_empty(),
        "the refused entry left no state and no stamp behind"
    );
}

#[test]
fn the_messages_after_a_maximum_clock_entry_still_apply() {
    let mut session = beside_a_guest();
    let mut plaintext = awareness_entries(&[(41, u32::MAX, "{}")]);
    plaintext.extend(update_message("hello"));
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &plaintext)),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(session.text("README.md"), "hello", "the stream went on");
    assert!(
        session.awareness_clients().is_empty(),
        "41 was not entered at the maximum"
    );
}

#[test]
fn a_state_at_the_maximum_clock_never_reaches_the_expiry_tick() {
    let mut session = beside_a_guest();
    let hostile = awareness_entries(&[(41, u32::MAX, "{}")]);
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &hostile));

    // `Awareness::remove_state` steps the clock of the state it drops, unchecked, so a state
    // entered at the maximum would step past it here. Refused, there is nothing to step.
    session.tick(millis(2 + 900));
    assert!(session.awareness_clients().is_empty(), "not held");
    assert_eq!(session.ending(), None, "the session goes on");
}

#[test]
fn a_removal_of_this_connections_own_id_at_the_maximum_clock_is_refused() {
    let mut session = beside_a_guest();
    let own = session.awareness_client_id();
    // A peer publishes a state under the id this connection announced (§8.4).
    let claimed = awareness_entries(&[(own, 1, "{}")]);
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &claimed));
    assert_eq!(session.awareness_clients(), [own]);

    // §8.2: a removal for the receiver's own id is not applied, and the receiver takes the
    // removal's clock plus one. At the maximum that step is the unchecked one, so the entry
    // is refused instead and the state it named stands.
    let hostile = awareness_entries(&[(own, u32::MAX, "null")]);
    assert_eq!(
        session.deliver(millis(3), &frame(&peer(), 0, 2, &hostile)),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(session.awareness_clients(), [own], "the state stands");

    // The session goes on: a renewal above the clock the state actually holds is applied.
    let renewed = awareness_entries(&[(own, 2, "{}")]);
    let _ = session.deliver(millis(4), &frame(&peer(), 0, 3, &renewed));
    assert_eq!(session.awareness_clients(), [own]);
}

#[test]
fn a_forgotten_awareness_state_stays_gone_and_its_neighbours_stay() {
    let mut session = beside_a_guest();
    let states = awareness_message(&[41, 42], 1, "{\"cursor\":null}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &states));
    assert_eq!(session.awareness_clients(), [41, 42]);

    session.forget_awareness(41);
    assert_eq!(session.awareness_clients(), [42]);

    // A copy of the old state still in flight is older than the removal.
    let stale = awareness_message(&[41], 1, "{\"cursor\":null}");
    let _ = session.deliver(millis(3), &frame(&peer(), 0, 2, &stale));
    assert_eq!(session.awareness_clients(), [42], "no resurrection");
}

#[test]
fn forgetting_an_id_with_no_state_leaves_its_first_state_welcome() {
    let mut session = beside_a_guest();
    session.forget_awareness(43);
    let first = awareness_message(&[43], 1, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &first));
    assert_eq!(session.awareness_clients(), [43]);
}

#[test]
fn this_connections_own_awareness_id_is_never_forgotten() {
    let mut session = beside_a_guest();
    let own = session.awareness_client_id();
    let states = awareness_message(&[own], 1, "{}");
    let _ = session.deliver(millis(2), &frame(&peer(), 0, 1, &states));
    assert_eq!(session.awareness_clients(), [own]);
    session.forget_awareness(own);
    assert_eq!(session.awareness_clients(), [own]);
}

/// A sync Update whose one item is an `Any` opening `depth` one-element arrays inside one
/// another around a `null`. It is built by hand because no document holds such a value:
/// one client's group of one struct, client 1 at clock 0, an `Any` item (info 8) under the
/// root named `x` carrying one value, then an empty delete set.
fn nested_any_update(depth: usize) -> Vec<u8> {
    let mut update = vec![1, 1, 1, 0, 8, 1, 1, b'x', 1];
    for _ in 0..depth {
        update.extend([117, 1]);
    }
    update.extend([126, 0]);
    encode_y_message(&YMessage::Sync(SyncMessage::Update(update)))
}

#[test]
fn an_update_nested_past_any_stack_is_refused_and_the_session_goes_on() {
    // Two bytes a level: a million levels is two megabytes, and no thread's stack holds a
    // recursive reader that deep.
    let mut session = beside_a_guest();
    let hostile = frame(&peer(), 0, 1, &nested_any_update(1 << 20));
    assert_eq!(
        session.deliver(millis(2), &hostile),
        Outcome::Dropped {
            reason: "bad_payload".to_string()
        }
    );
    assert_eq!(
        session.deliver(millis(3), &content(&peer(), 2, "after")),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(session.text("README.md"), "after");
}

#[test]
fn a_frame_carrying_a_nested_update_is_refused_whole() {
    // The refusal is the frame's, before anything in it is applied: an edit ahead of the
    // hostile update in the same stream is not applied either.
    let mut session = beside_a_guest();
    let mut plaintext = update_message("hello");
    plaintext.extend(nested_any_update(MAX_ANY_DEPTH + 1));
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &plaintext)),
        Outcome::Dropped {
            reason: "bad_payload".to_string()
        }
    );
    assert_eq!(session.text("README.md"), "");
}

#[test]
fn an_update_nested_to_the_bound_is_applied() {
    let mut session = beside_a_guest();
    let mut plaintext = nested_any_update(MAX_ANY_DEPTH);
    plaintext.extend(update_message("hello"));
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &plaintext)),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(session.text("README.md"), "hello");
}

fn invite(room: RoomKey, fragment: &str) -> String {
    format!("/session?room={ROOM}&token=t-1#{fragment}")
        .replace("$k", &encode_key(&[7; 32]))
        .replace("$h", &host().public().encode())
        .replace("$room", &encode_key(&room.0))
}

#[test]
fn the_invite_carries_the_room_and_both_keys_and_the_fragment_is_stripped() {
    let link = invite(room_key(), "k=$k&h=$h");
    let parsed = PeerInvite::parse(&link).unwrap();
    assert_eq!(parsed.room, ROOM);
    assert_eq!(parsed.token, "t-1");
    assert_eq!(parsed.room_key, RoomKey([7; 32]));
    assert_eq!(parsed.host_key, host().public());
    assert!(!parsed.socket_url.contains('#'));
}

#[test]
fn an_invite_with_no_fragment_or_with_a_value_that_is_not_a_key_is_refused_locally()
 {
    let missing = format!("/session?room={ROOM}&token=t-1");
    assert!(
        PeerInvite::parse(&missing)
            .unwrap_err()
            .contains("fragment")
    );

    let short = invite(room_key(), "k=AAAA&h=$h");
    assert!(PeerInvite::parse(&short).unwrap_err().contains("`k`"));

    let twice = invite(room_key(), "k=$k&k=$k&h=$h");
    assert!(PeerInvite::parse(&twice).unwrap_err().contains("twice"));

    // §6.1: the final character of a 32-byte value carries two zero bits, so a spelling
    // whose last character is any other spells no key.
    let uncanonical = invite(room_key(), "k=$kB&h=$h");
    assert!(PeerInvite::parse(&uncanonical).unwrap_err().contains("`k`"));

    let no_host = invite(room_key(), "k=$k");
    assert!(PeerInvite::parse(&no_host).unwrap_err().contains("`h`"));
}

/// §5.1: the fragment's two keys are read to be used, never repeated. A `Debug` that
/// printed them would put both in whatever log an invite — or the options a session is
/// built from — is written to, which is the one thing the fragment exists to prevent.
#[test]
fn the_invites_keys_are_not_printed_by_debug() {
    let room_key = room_key();
    let host_key = host().public();
    let parsed = PeerInvite::parse(&invite(room_key, "k=$k&h=$h")).unwrap();
    let printed = format!("{parsed:?}");
    assert!(
        !printed.contains(&format!("{room_key:?}")),
        "the room key was printed: {printed}"
    );
    assert!(
        !printed.contains(&format!("{host_key:?}")),
        "the host key was printed: {printed}"
    );
    // A `Debug` that printed a key in the encoding §5.1's fragment carries it in is a key
    // printed, and neither `Debug` spelling above is that string: the value itself is the
    // needle that reaches it.
    assert!(
        !printed.contains(&encode_key(&room_key.0))
            && !printed.contains(&host_key.encode()),
        "the encoded form of a key the invite holds was printed: {printed}"
    );

    let options = PeerOptions {
        room_id: ROOM.to_string(),
        room_key,
        host_key,
        renew: Duration::from_millis(300),
        expire: Duration::from_millis(900),
        seat: Some("p-self".to_string()),
        roster: BTreeSet::new(),
        fixed_session_key: Some([5; 32]),
        declared_role: None,
        awareness_client_id: None,
        host: None,
        frame_budget: None,
    };
    let printed = format!("{options:?}");
    assert!(
        !printed.contains(&format!("{room_key:?}"))
            && !printed.contains(&format!("{host_key:?}")),
        "a key the options hold was printed: {printed}"
    );
    assert!(
        !printed.contains(&encode_key(&room_key.0))
            && !printed.contains(&host_key.encode()),
        "the encoded form of a key the options hold was printed: {printed}"
    );
}

#[test]
fn the_page_link_is_the_same_room_token_and_fragment_over_the_browsers_scheme()
{
    // `PROTOCOL.md` §5.1's second form: the page the room's server serves, over the scheme
    // a browser speaks, with the same host, port and path prefix and nothing else in it.
    let room = encode_key(&room_key().0);
    let host_key = host().public().encode();
    let page =
        format!("http://h:8080/?room={ROOM}&token=t-1#k={room}&h={host_key}");
    let wire = format!("ws://h:8080/session?room={ROOM}&token=t-1");
    assert_eq!(
        wire_address(&page),
        wire,
        "the page reads back as the same connection address, fragment left off"
    );
    // §5.1: the fragment is the one part a request never carries, so the address is the
    // whole of what `wire_address` hands back however it is called. Re-appending the
    // fragment and handing the result to `parse_session_url` would glue `k` onto `token`
    // and put the room key in the URL a socket is dialled on.
    let door = wire_address(&page);
    assert!(
        !door.contains('#')
            && !door.contains(&room)
            && !door.contains(&host_key),
        "no key byte and no fragment survives the address reading: {door}"
    );
    assert_eq!(
        proto::parse_session_url(&door)
            .unwrap()
            .join
            .token
            .as_deref(),
        Some("t-1"),
        "the token stays the query's own, nothing glued onto it"
    );
    let parsed = PeerInvite::parse(&page).unwrap();
    assert_eq!(
        parsed.socket_url, wire,
        "and the address a socket is opened on carries no fragment"
    );
    assert_eq!(parsed.room, ROOM);
    assert_eq!(parsed.token, "t-1");
    assert_eq!(parsed.room_key, room_key());
    assert_eq!(parsed.host_key, host().public());

    // A path prefix survives the reading: the page is served under it and the endpoint is
    // under it too.
    let mounted =
        format!("https://h/page/?room={ROOM}&token=t-1#k={room}&h={host_key}");
    assert_eq!(
        PeerInvite::parse(&mounted).unwrap().socket_url,
        format!("wss://h/page/session?room={ROOM}&token=t-1")
    );

    // A link that is already a connection URL is handed back exactly as it stands.
    assert_eq!(wire_address(&wire), wire);
    assert!(
        PeerInvite::parse(&wire).unwrap_err().contains("fragment"),
        "a connection URL still carries no fragment"
    );
    // And one this cannot read as either form is not silently rewritten.
    assert_eq!(wire_address("wss://h/other?room=r"), "wss://h/other?room=r");
}

/// §5.1: `room` and `token` each appear at most once, in either form. A page link has no
/// server in it, so the receiver refuses a repeat itself and names the parameter — the shape
/// the fragment's rule already takes for `k` and `h` — rather than reporting that the address
/// names no endpoint, which is what the reader of the old message was told.
#[test]
fn a_page_link_that_repeats_a_join_key_is_refused_by_name() {
    let room = encode_key(&room_key().0);
    let host_key = host().public().encode();
    let twice_room = format!(
        "https://h/?room=r-1&room=r-other&token=t-1#k={room}&h={host_key}"
    );
    assert_eq!(
        PeerInvite::parse(&twice_room).unwrap_err(),
        "the invite names `room` twice"
    );
    let twice_token = format!(
        "https://h/?room=r-1&token=t-1&token=t-other#k={room}&h={host_key}"
    );
    assert_eq!(
        PeerInvite::parse(&twice_token).unwrap_err(),
        "the invite names `token` twice"
    );
    // The `wss://` form is read through the same query rule and names the repeat too.
    let wire_repeat = format!(
        "wss://h/session?room=r-1&room=r-other&token=t-1#k={room}&h={host_key}"
    );
    assert_eq!(
        PeerInvite::parse(&wire_repeat).unwrap_err(),
        "the invite names `room` twice"
    );
    // A link that genuinely names no endpoint keeps the generic refusal: a repeat is not
    // what is wrong with it.
    let elsewhere =
        format!("wss://h/other?room=r-1&token=t-1#k={room}&h={host_key}");
    assert_eq!(
        PeerInvite::parse(&elsewhere).unwrap_err(),
        "\"wss://h/other?room=r-1&token=t-1\" does not address the session endpoint"
    );
}

// --- §13.6: the bounded reciprocal request, the shared clock and local readiness -----

#[test]
fn a_committed_writers_first_request_arms_one_coalesced_reciprocal_request() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let committing = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    // The immediate initial request: the clock every later request is measured from.
    let initial = session.take_outbound();
    assert_eq!(initial.len(), 1);
    assert_eq!(sync_subtype(&initial[0]), Some(0));

    // The first `SyncStep1` from a committed writer is answered and remembered.
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let answer = session.take_outbound();
    assert_eq!(answer.len(), 1, "one Step2 for the request");
    assert_eq!(sync_subtype(&answer[0]), Some(1));

    // The request clock is 1, so the window opens at 301.
    session.tick(millis(300));
    assert!(session.take_outbound().is_empty(), "not before the clock");
    session.tick(millis(301));
    let reciprocal = session.take_outbound();
    assert_eq!(reciprocal.len(), 1, "one coalesced reciprocal request");
    assert_eq!(sync_subtype(&reciprocal[0]), Some(0));

    // A later request from the same continuously eligible key draws its Step2 only.
    assert_eq!(
        session.deliver(millis(302), &frame(&peer(), 0, 2, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let again = session.take_outbound();
    assert_eq!(again.len(), 1, "one Step2 per inbound frame");
    assert_eq!(sync_subtype(&again[0]), Some(1));
    session.tick(millis(601));
    session.tick(millis(901));
    assert!(
        session.take_outbound().is_empty(),
        "the served census stops the echo"
    );
}

#[test]
fn a_viewers_request_does_not_arm_a_reciprocal_request() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let committing = state(
        &host(),
        1,
        &[
            (&ours(), "guest", "p-self"),
            (&peer(), "viewer", "p-viewer"),
        ],
    );
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let answer = session.take_outbound();
    assert_eq!(answer.len(), 1);
    assert_eq!(sync_subtype(&answer[0]), Some(1), "the request is answered");
    session.tick(millis(302));
    session.tick(millis(602));
    assert!(
        session.take_outbound().is_empty(),
        "a viewer sends no content, so there is nothing to ask for"
    );
}

#[test]
fn a_step2_or_update_never_arms_a_reciprocal_request() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let committing = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(2), &content(&peer(), 1, "theirs")),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        session.deliver(millis(3), &content(&peer(), 2, "more")),
        Outcome::Applied { kind: 0 }
    );
    session.tick(millis(303));
    session.tick(millis(603));
    assert!(
        session.take_outbound().is_empty(),
        "content never asks for a request"
    );
}

#[test]
fn several_new_writer_keys_in_one_window_draw_one_broadcast_request() {
    let mut session = session(&[]);
    let third = SessionKey::from_seed([17; 32]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let committing = state(
        &host(),
        1,
        &[
            (&ours(), "guest", "p-self"),
            (&peer(), "guest", "p-other"),
            (&third, "guest", "p-third"),
        ],
    );
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        session.deliver(millis(3), &frame(&third, 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let answers = session.take_outbound();
    assert_eq!(answers.len(), 2, "each request is answered");
    session.tick(millis(303));
    let requests = session.take_outbound();
    assert_eq!(requests.len(), 1, "the two new keys coalesce");
    assert_eq!(sync_subtype(&requests[0]), Some(0));
}

#[test]
fn a_valid_request_prefix_survives_a_malformed_suffix() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let committing = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    let mut plaintext = step1();
    plaintext.extend_from_slice(&[0, 0, 1, 128]);
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &plaintext)),
        Outcome::Applied { kind: 0 }
    );
    let answer = session.take_outbound();
    assert_eq!(answer.len(), 1, "the valid prefix is answered");
    assert_eq!(sync_subtype(&answer[0]), Some(1));
    session.tick(millis(301));
    let request = session.take_outbound();
    assert_eq!(request.len(), 1, "the valid prefix still owes readiness");
    assert_eq!(sync_subtype(&request[0]), Some(0));
}

#[test]
fn a_malformed_first_message_arms_nothing_and_a_later_valid_request_does() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let committing = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &[0, 0, 1, 128])),
        Outcome::Applied { kind: 0 }
    );
    assert!(
        session.take_outbound().is_empty(),
        "an invalid first message is not answered"
    );
    session.tick(millis(300));
    assert!(
        session.take_outbound().is_empty(),
        "a malformed request must not arm readiness"
    );
    assert_eq!(
        session.deliver(millis(301), &frame(&peer(), 0, 2, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let answer = session.take_outbound();
    assert_eq!(answer.len(), 1);
    assert_eq!(sync_subtype(&answer[0]), Some(1));
    session.tick(millis(601));
    let request = session.take_outbound();
    assert_eq!(request.len(), 1, "the valid request armed the obligation");
    assert_eq!(sync_subtype(&request[0]), Some(0));
}

#[test]
fn a_local_reentry_after_absence_owes_one_coalesced_readiness_request() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let first = state(&host(), 1, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        session.deliver(millis(1), &first),
        Outcome::Applied { kind: 1 }
    );
    let initial = session.take_outbound();
    assert_eq!(
        initial.len(),
        1,
        "the first commitment owes the initial request"
    );
    assert_eq!(sync_subtype(&initial[0]), Some(0));
    session.tick(millis(2));
    assert!(
        session.take_outbound().is_empty(),
        "the first commitment adds no readiness"
    );

    // The key is dropped, then recommitted as a writer: a new continuous eligibility.
    assert_eq!(
        session.deliver(millis(3), &state(&host(), 2, &[])),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(4),
            &state(&host(), 3, &[(&ours(), "guest", "p-self")])
        ),
        Outcome::Applied { kind: 1 }
    );
    assert!(
        session.take_outbound().is_empty(),
        "a state cannot send renewed readiness immediately"
    );
    session.tick(millis(300));
    assert!(session.take_outbound().is_empty(), "not before the clock");
    session.tick(millis(301));
    let readiness = session.take_outbound();
    assert_eq!(readiness.len(), 1);
    assert_eq!(sync_subtype(&readiness[0]), Some(0));
    session.tick(millis(601));
    session.tick(millis(901));
    assert!(
        session.take_outbound().is_empty(),
        "readiness is served once per eligibility"
    );
}

#[test]
fn an_uninterrupted_writer_eligibility_and_listing_editions_never_rearm() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let first = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &first),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    let changed = state(
        &host(),
        2,
        &[(&ours(), "host", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(2), &changed),
        Outcome::Applied { kind: 1 }
    );
    let listing = state_listing(
        &host(),
        3,
        &[(&ours(), "host", "p-self"), (&peer(), "guest", "p-other")],
        &["README.md", "other.md"],
    );
    assert_eq!(
        session.deliver(millis(3), &listing),
        Outcome::Applied { kind: 1 }
    );
    assert!(
        session.take_outbound().is_empty(),
        "a host/guest change and a listing edition owe nothing"
    );
    session.tick(millis(301));
    assert!(
        session.take_outbound().is_empty(),
        "uninterrupted eligibility never rearms"
    );
}

#[test]
fn local_readiness_retires_when_writer_eligibility_ends_before_service() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(1),
            &state(&host(), 1, &[(&ours(), "guest", "p-self")])
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    // Re-entry arms readiness, then eligibility ends before the window opens.
    assert_eq!(
        session.deliver(millis(2), &state(&host(), 2, &[])),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(3),
            &state(&host(), 3, &[(&ours(), "guest", "p-self")])
        ),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        session.deliver(
            millis(4),
            &state(&host(), 4, &[(&ours(), "viewer", "p-self")])
        ),
        Outcome::Applied { kind: 1 }
    );
    assert!(session.take_outbound().is_empty());
    session.tick(millis(301));
    assert!(
        session.take_outbound().is_empty(),
        "retired readiness must not be served"
    );
}

#[test]
fn a_remote_demotion_and_re_promotion_rearms_exactly_one_request() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let first = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &first),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let _ = session.take_outbound();
    session.tick(millis(301));
    let armed = session.take_outbound();
    assert_eq!(armed.len(), 1, "the first eligibility armed once");
    assert_eq!(sync_subtype(&armed[0]), Some(0));

    // The peer is demoted, so its census entry goes; re-promotion arms once more.
    let demoted = state(
        &host(),
        2,
        &[(&ours(), "guest", "p-self"), (&peer(), "viewer", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(302), &demoted),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    let promoted = state(
        &host(),
        3,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(303), &promoted),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(304), &frame(&peer(), 0, 2, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let _ = session.take_outbound();
    session.tick(millis(601));
    let rearmed = session.take_outbound();
    assert_eq!(rearmed.len(), 1, "the new eligibility armed once");
    assert_eq!(sync_subtype(&rearmed[0]), Some(0));
}
/// §13.6: activation is the remote writer's eligibility, so a request is recorded even while
/// this connection is uncommitted and may not publish; the shared clock then serves it once
/// the local key can publish again. Gating activation on local commitment would drop it.
#[test]
fn a_remote_request_arms_while_uncommitted_and_is_served_after_recommitment() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    // L is committed as a viewer and takes the initial request: the clock later ones use.
    assert_eq!(
        session.deliver(millis(1), &viewer_beside_writer(1)),
        Outcome::Applied { kind: 1 }
    );
    let initial = session.take_outbound();
    assert_eq!(
        initial.len(),
        1,
        "a committed viewer still takes the initial request"
    );
    assert_eq!(sync_subtype(&initial[0]), Some(0));

    // A newer state drops L while W stays a committed writer, so L cannot publish at all.
    assert_eq!(
        session.deliver(
            millis(2),
            &state(&host(), 2, &[(&peer(), "guest", "p-other")])
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();

    // W's first valid request in this eligibility is recognised though L is uncommitted.
    assert_eq!(
        session.deliver(millis(3), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    assert!(
        session.take_outbound().is_empty(),
        "an uncommitted receiver answers nothing"
    );

    // L is recommitted as a viewer with W retained: the remote cause survives the local gate.
    assert_eq!(
        session.deliver(millis(4), &viewer_beside_writer(3)),
        Outcome::Applied { kind: 1 }
    );
    assert!(
        session.take_outbound().is_empty(),
        "a state cannot send the request immediately"
    );
    session.tick(millis(300));
    assert!(session.take_outbound().is_empty(), "not before the clock");
    session.tick(millis(301));
    let served = session.take_outbound();
    assert_eq!(
        served.len(),
        1,
        "the remote request is served once at the due tick"
    );
    assert_eq!(sync_subtype(&served[0]), Some(0));

    // Served once: a continuously committed key does not rearm over later windows.
    session.tick(millis(601));
    session.tick(millis(901));
    assert!(
        session.take_outbound().is_empty(),
        "a served remote eligibility does not rearm"
    );
}

/// The companion: a request whose writer is removed or demoted before the local key returns
/// is retired with that writer's eligibility, so no request is owed.
#[test]
fn a_remote_request_for_a_key_that_leaves_before_the_return_owes_nothing() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(1),
            &state(
                &host(),
                1,
                &[(&ours(), "viewer", "p-self"), (&peer(), "guest", "p-other")]
            )
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(2),
            &state(&host(), 2, &[(&peer(), "guest", "p-other")])
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(3), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let _ = session.take_outbound();

    // W is demoted before L comes back: the obligation retires with its eligibility.
    assert_eq!(
        session.deliver(
            millis(4),
            &state(
                &host(),
                3,
                &[
                    (&ours(), "viewer", "p-self"),
                    (&peer(), "viewer", "p-other")
                ]
            )
        ),
        Outcome::Applied { kind: 1 }
    );
    assert!(session.take_outbound().is_empty());
    session.tick(millis(301));
    session.tick(millis(601));
    assert!(
        session.take_outbound().is_empty(),
        "a retired request owes nothing"
    );
}

/// Retiring a remote cause must not erase the refusal interval or local readiness sharing the
/// same clock: one request serves every remaining obligation.
#[test]
fn a_retired_remote_request_leaves_the_refusal_and_readiness_causes() {
    let mut session = session(&[]);
    let unmoved = SessionKey::from_seed([23; 32]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(1),
            &state(
                &host(),
                1,
                &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")]
            )
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(
            millis(2),
            &state(&host(), 2, &[(&peer(), "guest", "p-other")])
        ),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    // W's request arms the remote cause.
    assert_eq!(
        session.deliver(millis(3), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let _ = session.take_outbound();
    // A content frame from a key no state commits opens §13.6's refusal interval.
    assert_eq!(
        session.deliver(millis(4), &content(&unmoved, 1, "theirs")),
        Outcome::Dropped {
            reason: "uncommitted_key".to_string()
        }
    );
    // L returns as a writer and W is demoted: the remote cause retires, a new eligibility
    // begins for L, and the refusal interval is still open.
    assert_eq!(
        session.deliver(
            millis(5),
            &state(
                &host(),
                3,
                &[(&ours(), "guest", "p-self"), (&peer(), "viewer", "p-other")]
            )
        ),
        Outcome::Applied { kind: 1 }
    );
    assert!(session.take_outbound().is_empty());
    session.tick(millis(300));
    assert!(session.take_outbound().is_empty(), "not before the clock");
    session.tick(millis(301));
    let served = session.take_outbound();
    assert_eq!(
        served.len(),
        1,
        "refusal and readiness survive the retired remote cause"
    );
    assert_eq!(sync_subtype(&served[0]), Some(0));
}

#[test]
fn a_viewers_local_edits_never_enter_room_history() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let as_viewer = state(
        &host(),
        1,
        &[(&ours(), "viewer", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &as_viewer),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert!(!session.insert("README.md", 0, "secret").unwrap());
    assert_eq!(
        session.text("README.md"),
        "",
        "the disallowed edit is not even locally visible"
    );
    assert!(session.take_outbound().is_empty());

    // Promotion with the same key: the request is answered from room-safe history alone.
    let as_writer = state(
        &host(),
        2,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(2), &as_writer),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(3), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let answer = session.take_outbound();
    assert_eq!(answer.len(), 1);
    assert_eq!(sync_subtype(&answer[0]), Some(1));
    assert_eq!(
        step2_text(&answer[0]),
        "",
        "no viewer history is exported on promotion"
    );
}

#[test]
fn an_unknown_role_edit_is_discarded_when_a_viewer_state_arrives() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    assert!(
        !session.insert("README.md", 0, "spec").unwrap(),
        "held while the role is unknown"
    );
    assert_eq!(
        session.text("README.md"),
        "spec",
        "held locally while unknown"
    );
    let as_viewer = state(
        &host(),
        1,
        &[(&ours(), "viewer", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &as_viewer),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        session.text("README.md"),
        "",
        "the viewer state discards the speculative edit"
    );
    let _ = session.take_outbound();

    // Re-promotion: the discarded edit never returns.
    let as_writer = state(
        &host(),
        2,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(2), &as_writer),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(3), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let answer = session.take_outbound();
    assert_eq!(answer.len(), 1);
    assert_eq!(
        step2_text(&answer[0]),
        "",
        "the discarded edit never returns"
    );
}

#[test]
fn a_demoted_writer_keeps_its_history_and_exports_no_viewer_edit() {
    let mut session = session(&[]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let as_writer = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(1), &as_writer),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert!(session.insert("README.md", 0, "legit").unwrap());
    let _ = session.take_outbound();

    let as_viewer = state(
        &host(),
        2,
        &[(&ours(), "viewer", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(2), &as_viewer),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert!(!session.insert("README.md", 5, "bad").unwrap());
    assert_eq!(
        session.text("README.md"),
        "legit",
        "the viewer edit is discarded"
    );

    // Promotion: the writer history survives and is exported.
    let promoted = state(
        &host(),
        3,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        session.deliver(millis(3), &promoted),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(4), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let answer = session.take_outbound();
    assert_eq!(answer.len(), 1);
    assert_eq!(
        step2_text(&answer[0]),
        "legit",
        "the writer history is preserved"
    );
}

#[test]
fn three_writer_requests_coalesce_and_the_room_quiesces() {
    let mut session = session(&[]);
    let third = SessionKey::from_seed([17; 32]);
    session.tick(Duration::ZERO);
    let _ = session.take_outbound();
    let committing = state(
        &host(),
        1,
        &[
            (&ours(), "guest", "p-self"),
            (&peer(), "guest", "p-other"),
            (&third, "guest", "p-third"),
        ],
    );
    assert_eq!(
        session.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = session.take_outbound();
    assert_eq!(
        session.deliver(millis(2), &frame(&peer(), 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        session.deliver(millis(3), &frame(&third, 0, 1, &step1())),
        Outcome::Applied { kind: 0 }
    );
    let answers = session.take_outbound();
    assert_eq!(answers.len(), 2, "each request draws its own Step2");
    session.tick(millis(303));
    let request = session.take_outbound();
    assert_eq!(request.len(), 1, "one coalesced request for both keys");
    assert_eq!(sync_subtype(&request[0]), Some(0));
    // No obligation left, so later windows are silent.
    session.tick(millis(603));
    session.tick(millis(903));
    session.tick(millis(1203));
    assert!(
        session.take_outbound().is_empty(),
        "no periodic full sync without an obligation"
    );
}
/// One round of a three-peer room: each session hears the other two's frames, answers each
/// with exactly one Step2, and arms nothing new.
/// The two frames one session hears in a three-peer round.
type Heard<'a> = [&'a [u8]; 2];

fn three_way_round(
    trio: [&mut PeerSession; 3],
    clock: Duration,
    heard: [Heard<'_>; 3],
) {
    for (peer, [first, second]) in trio.into_iter().zip(heard) {
        assert_eq!(peer.deliver(clock, first), Outcome::Applied { kind: 0 });
        assert_eq!(peer.deliver(clock, second), Outcome::Applied { kind: 0 });
        let answers = peer.take_outbound();
        assert_eq!(answers.len(), 2, "each request draws one Step2");
        assert!(answers.iter().all(|frame| sync_subtype(frame) == Some(1)));
    }
}

/// Three real sessions, each with its own replica and key: every pair's reciprocal exchange
/// is bounded, one coalesced request per session per window, and the room falls quiet. The
/// older three-writer test drives one receiver and two signers, which is a census bound and
/// not an all-room one.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the three-peer exchange, its bounded counts and the quiet windows are one sequence"
)]
fn three_real_sessions_exchange_reciprocally_and_then_quiesce() {
    let mut alpha = session(&["p-other", "p-third"]);
    let mut beta = second_session(&["p-self", "p-third"]);
    let mut gamma = third_session(&["p-self", "p-other"]);
    for peer in [&mut alpha, &mut beta, &mut gamma] {
        peer.tick(Duration::ZERO);
        let _ = peer.take_outbound();
    }
    let committing = state(
        &host(),
        1,
        &[
            (&ours(), "guest", "p-self"),
            (&peer(), "guest", "p-other"),
            (&third(), "guest", "p-third"),
        ],
    );
    let mut initial = Vec::new();
    for peer in [&mut alpha, &mut beta, &mut gamma] {
        assert_eq!(
            peer.deliver(millis(1), &committing),
            Outcome::Applied { kind: 1 }
        );
        let frames = peer.take_outbound();
        assert_eq!(
            frames.len(),
            1,
            "the first commitment owes one initial request"
        );
        assert_eq!(sync_subtype(&frames[0]), Some(0));
        initial.push(frames[0].clone());
    }
    three_way_round(
        [&mut alpha, &mut beta, &mut gamma],
        millis(2),
        [
            [initial[1].as_slice(), initial[2].as_slice()],
            [initial[0].as_slice(), initial[2].as_slice()],
            [initial[0].as_slice(), initial[1].as_slice()],
        ],
    );
    // One coalesced request per session at the shared clock, not one per writer key.
    let mut requests = Vec::new();
    for peer in [&mut alpha, &mut beta, &mut gamma] {
        peer.tick(millis(300));
        assert!(peer.take_outbound().is_empty(), "not before the clock");
        peer.tick(millis(301));
        let frames = peer.take_outbound();
        assert_eq!(
            frames.len(),
            1,
            "the two obligations coalesce into one request"
        );
        assert_eq!(sync_subtype(&frames[0]), Some(0));
        requests.push(frames[0].clone());
    }
    three_way_round(
        [&mut alpha, &mut beta, &mut gamma],
        millis(302),
        [
            [requests[1].as_slice(), requests[2].as_slice()],
            [requests[0].as_slice(), requests[2].as_slice()],
            [requests[0].as_slice(), requests[1].as_slice()],
        ],
    );
    // One session's edit reaches the other two; all three replicas agree and nothing replies.
    assert!(alpha.insert("README.md", 0, "x").unwrap());
    let edit = alpha.take_outbound();
    assert_eq!(edit.len(), 1, "the edit publishes");
    assert_eq!(
        beta.deliver(millis(303), &edit[0]),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        gamma.deliver(millis(303), &edit[0]),
        Outcome::Applied { kind: 0 }
    );
    assert!(
        beta.take_outbound().is_empty() && gamma.take_outbound().is_empty(),
        "content draws no reply"
    );
    assert_eq!(alpha.state_vector(), beta.state_vector());
    assert_eq!(beta.state_vector(), gamma.state_vector());

    for peer in [&mut alpha, &mut beta, &mut gamma] {
        peer.tick(millis(601));
        peer.tick(millis(901));
        peer.tick(millis(1201));
        assert!(
            peer.take_outbound().is_empty(),
            "no periodic full sync without an obligation"
        );
    }
}

/// A retained same-key session keeps its replica through an omission and recommitment, owes
/// one renewed readiness request, and keeps editing on that same replica.
#[test]
fn a_recommitted_same_key_retains_its_replica_and_keeps_editing() {
    let mut writer = session(&["p-other"]);
    let mut receiver = second_session(&["p-self"]);
    writer.tick(Duration::ZERO);
    receiver.tick(Duration::ZERO);
    let _ = writer.take_outbound();
    let _ = receiver.take_outbound();
    let committing = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        writer.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        receiver.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let _ = writer.take_outbound();
    let _ = receiver.take_outbound();

    // The receiver edits as a committed writer; the writer applies that history.
    assert!(receiver.insert("README.md", 0, "abc").unwrap());
    let edit = receiver.take_outbound();
    assert_eq!(edit.len(), 1);
    assert_eq!(
        writer.deliver(millis(2), &edit[0]),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(receiver.state_vector(), writer.state_vector());
    let vector = receiver.state_vector();

    // The receiver's key is dropped, then recommitted under the same key.
    let omitted = state(&host(), 2, &[(&ours(), "guest", "p-self")]);
    assert_eq!(
        receiver.deliver(millis(3), &omitted),
        Outcome::Applied { kind: 1 }
    );
    let _ = receiver.take_outbound();
    let recommitted = state(
        &host(),
        3,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        receiver.deliver(millis(4), &recommitted),
        Outcome::Applied { kind: 1 }
    );
    assert!(
        receiver.take_outbound().is_empty(),
        "a state cannot send the renewed readiness immediately"
    );
    // The retained replica is untouched by the omission and recommitment.
    assert_eq!(receiver.text("README.md"), "abc");
    assert_eq!(receiver.state_vector(), vector);

    receiver.tick(millis(300));
    assert!(receiver.take_outbound().is_empty(), "not before the clock");
    receiver.tick(millis(301));
    let readiness = receiver.take_outbound();
    assert_eq!(
        readiness.len(),
        1,
        "recommitment owes one readiness request"
    );
    assert_eq!(sync_subtype(&readiness[0]), Some(0));
    assert!(receiver.insert("README.md", 3, "d").unwrap());
    assert_eq!(
        receiver.take_outbound().len(),
        1,
        "the continued edit publishes"
    );
    assert_eq!(receiver.text("README.md"), "abcd");
}

/// Two real sessions, one lost `Update`: the reciprocal request carries the missing struct
/// back and the replicas converge with no further edit, no new key and no refusal.
#[test]
fn a_lost_update_is_recovered_by_the_reciprocal_request() {
    let mut writer = session(&["p-other"]);
    let mut receiver = second_session(&["p-self"]);
    writer.tick(Duration::ZERO);
    receiver.tick(Duration::ZERO);
    let _ = writer.take_outbound();
    let _ = receiver.take_outbound();
    let committing = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        writer.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        receiver.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let writer_initial = writer.take_outbound();
    let _ = receiver.take_outbound();
    assert_eq!(writer_initial.len(), 1);

    // The receiver sees the writer's request and arms one reciprocal obligation.
    assert_eq!(
        receiver.deliver(millis(2), &writer_initial[0]),
        Outcome::Applied { kind: 0 }
    );
    let _ = receiver.take_outbound();

    // The writer inserts, and the frame carrying the first insertion is lost on the way.
    assert!(writer.insert("README.md", 0, "off ").unwrap());
    let lost = writer.take_outbound();
    assert_eq!(lost.len(), 1);
    // A second, dependent insertion is delivered: the receiver holds it pending.
    assert!(writer.insert("README.md", 0, "on ").unwrap());
    let dependent = writer.take_outbound();
    assert_eq!(dependent.len(), 1);
    assert_eq!(
        receiver.deliver(millis(3), &dependent[0]),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        receiver.text("README.md"),
        "",
        "the dependent insertion waits for its dependency"
    );

    // The reciprocal request recovers the missing history without a new edit or key.
    receiver.tick(millis(302));
    let request = receiver.take_outbound();
    assert_eq!(request.len(), 1, "one coalesced reciprocal request");
    assert_eq!(sync_subtype(&request[0]), Some(0));
    assert_eq!(
        writer.deliver(millis(303), &request[0]),
        Outcome::Applied { kind: 0 }
    );
    let repair = writer.take_outbound();
    let step2 = repair
        .iter()
        .find(|frame| sync_subtype(frame.as_slice()) == Some(1))
        .expect("the writer answers the request with a Step2");
    assert_eq!(
        receiver.deliver(millis(304), step2),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        receiver.text("README.md"),
        "on off ",
        "the lost insertion returns"
    );
    assert_eq!(writer.text("README.md"), "on off ");
    assert_eq!(
        writer.state_vector(),
        receiver.state_vector(),
        "the replicas converge"
    );
}

/// The same two sessions, one lost deletion: a deletion is not a struct, so the state
/// vectors already agree while the texts differ; the reciprocal request carries the whole
/// delete set back and the texts converge.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the whole loss and repair is one sequence and is clearer unsplit"
)]
fn a_lost_deletion_is_recovered_by_the_reciprocal_request() {
    let mut writer = session(&[]);
    let mut receiver = second_session(&[]);
    let third = SessionKey::from_seed([19; 32]);
    writer.tick(Duration::ZERO);
    receiver.tick(Duration::ZERO);
    let _ = writer.take_outbound();
    let _ = receiver.take_outbound();
    let committing = state(
        &host(),
        1,
        &[
            (&ours(), "guest", "p-self"),
            (&peer(), "guest", "p-other"),
            (&third, "guest", "p-third"),
        ],
    );
    assert_eq!(
        writer.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        receiver.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let writer_initial = writer.take_outbound();
    let _ = receiver.take_outbound();
    // Arm the receiver's reciprocal obligation.
    assert_eq!(
        receiver.deliver(millis(2), &writer_initial[0]),
        Outcome::Applied { kind: 0 }
    );
    let _ = receiver.take_outbound();

    // A third key inserts "abc"; both replicas apply it.
    let source = Doc::new();
    let handle = source.get_or_insert_text("README.md");
    {
        let mut txn = source.transact_mut();
        handle.insert(&mut txn, 0, "abc");
    }
    let inserted = {
        let txn = source.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };
    let insert_frame = frame(
        &third,
        0,
        1,
        &encode_y_message(&YMessage::Sync(SyncMessage::Update(inserted))),
    );
    assert_eq!(
        writer.deliver(millis(3), &insert_frame),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        receiver.deliver(millis(3), &insert_frame),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(writer.text("README.md"), "abc");
    assert_eq!(receiver.text("README.md"), "abc");

    // The third key deletes `b`; only the writer receives that update.
    let before = {
        let txn = source.transact();
        txn.state_vector()
    };
    {
        let mut txn = source.transact_mut();
        handle.remove_range(&mut txn, 1, 1);
    }
    let deletion = {
        let txn = source.transact();
        txn.encode_state_as_update_v1(&before)
    };
    let delete_frame = frame(
        &third,
        0,
        2,
        &encode_y_message(&YMessage::Sync(SyncMessage::Update(deletion))),
    );
    assert_eq!(
        writer.deliver(millis(4), &delete_frame),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(writer.text("README.md"), "ac");
    assert_eq!(receiver.text("README.md"), "abc", "the deletion was lost");
    // A deletion is not a struct, so the vectors agree before the repair.
    assert_eq!(writer.state_vector(), receiver.state_vector());

    // The reciprocal request carries the whole delete set back.
    receiver.tick(millis(302));
    let request = receiver.take_outbound();
    assert_eq!(request.len(), 1, "one coalesced reciprocal request");
    assert_eq!(sync_subtype(&request[0]), Some(0));
    assert_eq!(
        writer.deliver(millis(303), &request[0]),
        Outcome::Applied { kind: 0 }
    );
    let repair = writer.take_outbound();
    let step2 = repair
        .iter()
        .find(|frame| sync_subtype(frame.as_slice()) == Some(1))
        .expect("the writer answers the request with a Step2");
    assert_eq!(
        receiver.deliver(millis(304), step2),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        receiver.text("README.md"),
        "ac",
        "the lost deletion returns"
    );
    assert_eq!(
        writer.state_vector(),
        receiver.state_vector(),
        "the replicas converge"
    );
}

/// After a lost update is repaired and the room has gone quiet, a fresh writer eligibility
/// lets a second lost update be repaired with the same two retained replicas and no new API:
/// the demotion and re-promotion are signed states, and the renewed request is what a fresh
/// continuous eligibility owes.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the two loss-and-repair rounds and the fresh eligibility between them are one sequence"
)]
fn a_second_lost_update_is_repaired_after_a_fresh_writer_eligibility() {
    let mut writer = session(&["p-other"]);
    let mut receiver = second_session(&["p-self"]);
    writer.tick(Duration::ZERO);
    receiver.tick(Duration::ZERO);
    let _ = writer.take_outbound();
    let _ = receiver.take_outbound();
    let committing = state(
        &host(),
        1,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        writer.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        receiver.deliver(millis(1), &committing),
        Outcome::Applied { kind: 1 }
    );
    let writer_initial = writer.take_outbound();
    let _ = receiver.take_outbound();
    assert_eq!(
        receiver.deliver(millis(2), &writer_initial[0]),
        Outcome::Applied { kind: 0 }
    );
    let _ = receiver.take_outbound();

    // First loss: the independent insertion is dropped, the dependent one arrives pending.
    assert!(writer.insert("README.md", 0, "off ").unwrap());
    let _ = writer.take_outbound();
    assert!(writer.insert("README.md", 0, "on ").unwrap());
    let dependent = writer.take_outbound();
    assert_eq!(
        receiver.deliver(millis(3), &dependent[0]),
        Outcome::Applied { kind: 0 }
    );
    receiver.tick(millis(302));
    let request = receiver.take_outbound();
    assert_eq!(request.len(), 1, "one reciprocal request");
    assert_eq!(
        writer.deliver(millis(303), &request[0]),
        Outcome::Applied { kind: 0 }
    );
    let repair = writer.take_outbound();
    let step2 = repair
        .iter()
        .find(|frame| sync_subtype(frame.as_slice()) == Some(1))
        .expect("the writer answers with a Step2");
    assert_eq!(
        receiver.deliver(millis(304), step2),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(receiver.text("README.md"), "on off ");
    assert_eq!(writer.state_vector(), receiver.state_vector());

    // A quiet interval: the receiver owes nothing.
    receiver.tick(millis(601));
    receiver.tick(millis(901));
    assert!(
        receiver.take_outbound().is_empty(),
        "the repaired window is silent"
    );

    // The writer is demoted and re-promoted under the same key: a fresh eligibility.
    let demoted = state(
        &host(),
        2,
        &[(&ours(), "viewer", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        writer.deliver(millis(902), &demoted),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        receiver.deliver(millis(902), &demoted),
        Outcome::Applied { kind: 1 }
    );
    let promoted = state(
        &host(),
        3,
        &[(&ours(), "guest", "p-self"), (&peer(), "guest", "p-other")],
    );
    assert_eq!(
        writer.deliver(millis(903), &promoted),
        Outcome::Applied { kind: 1 }
    );
    assert_eq!(
        receiver.deliver(millis(903), &promoted),
        Outcome::Applied { kind: 1 }
    );

    // The renewed writer eligibility sends one request; the receiver arms from it.
    writer.tick(millis(1201));
    let renewed = writer.take_outbound();
    assert_eq!(renewed.len(), 1, "the fresh eligibility owes one request");
    assert_eq!(sync_subtype(&renewed[0]), Some(0));
    assert_eq!(
        receiver.deliver(millis(1202), &renewed[0]),
        Outcome::Applied { kind: 0 }
    );
    let _ = receiver.take_outbound();

    // Second loss: the same shape, repaired by the receiver's renewed reciprocal request.
    assert!(writer.insert("README.md", 0, "z").unwrap());
    let _ = writer.take_outbound();
    assert!(writer.insert("README.md", 0, "y").unwrap());
    let dependent = writer.take_outbound();
    assert_eq!(
        receiver.deliver(millis(1203), &dependent[0]),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(
        receiver.text("README.md"),
        "on off ",
        "the second dependency waits"
    );
    receiver.tick(millis(1502));
    let request = receiver.take_outbound();
    assert_eq!(request.len(), 1, "the fresh eligibility re-armed once");
    assert_eq!(sync_subtype(&request[0]), Some(0));
    assert_eq!(
        writer.deliver(millis(1503), &request[0]),
        Outcome::Applied { kind: 0 }
    );
    let repair = writer.take_outbound();
    let step2 = repair
        .iter()
        .find(|frame| sync_subtype(frame.as_slice()) == Some(1))
        .expect("the writer answers the second request with a Step2");
    assert_eq!(
        receiver.deliver(millis(1504), step2),
        Outcome::Applied { kind: 0 }
    );
    assert_eq!(receiver.text("README.md"), "yzon off ");
    assert_eq!(writer.text("README.md"), "yzon off ");
    assert_eq!(writer.state_vector(), receiver.state_vector());
}
