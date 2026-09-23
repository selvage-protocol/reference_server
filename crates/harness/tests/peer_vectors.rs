//! The peer corpus's frame layer, replayed against this implementation.
//!
//! `specification/vectors/peer/*.json` is the corpus of `CANONICAL.md` §6.1's bytes: a
//! *frame* vector hands over recipes and frames, and it seals, signs, verifies and
//! refuses them the way that section says. The specification's own runner replays them in
//! Python (`runner/run_peer.py`); this replays the same files against the Rust sealed
//! layer the client and this harness use, so an implementation that disagrees with the
//! frozen bytes is a red run here rather than a surprise on a wire.
//!
//! The layer's `expectSubject` — the decision half, which needs a client and a relay — is
//! replayed in `decisions.rs`. The mutation census, too, is the Python runner's: this
//! reads each vector once, as it stands, where it must pass.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::str;

use selvage_client::sealed::{
    Envelope, FrameKey, KeyId, PublicKey, Reader, Recipe, RoomKey, SessionKey,
    associated_data, encode_key, opens, read_varuint, seal, signing_input,
};
use serde_json::Value;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, Transact};

/// One fixture keypair.
struct FixtureKey {
    public: [u8; 32],
    private: [u8; 32],
}

/// `vectors/fixture/keys.json`: public test values whose authority is reproducibility.
struct Fixture {
    room_id: String,
    room_key: [u8; 32],
    host_name: String,
    keys: BTreeMap<String, FixtureKey>,
}

impl Fixture {
    fn load(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path).map_err(|error| {
            format!("{} is unreadable: {error}", path.display())
        })?;
        let document: Value = serde_json::from_str(&text).map_err(|error| {
            format!("{} is not JSON: {error}", path.display())
        })?;
        let room = document
            .get("room")
            .ok_or_else(|| "the fixture has no `room`".to_string())?;
        let room_id = text_of(room, "id")?.to_string();
        let room_key = to32(&hex_bytes(text_of(room, "key")?)?)?;
        let host_name = text_of(room, "host")?.to_string();
        let raw = document
            .get("keys")
            .and_then(Value::as_object)
            .ok_or_else(|| "the fixture has no `keys`".to_string())?;
        let mut keys = BTreeMap::new();
        for (name, entry) in raw {
            keys.insert(name.clone(), fixture_key(name, entry)?);
        }
        Ok(Self {
            room_id,
            room_key,
            host_name,
            keys,
        })
    }

    fn frame_key(&self) -> FrameKey {
        RoomKey(self.room_key).frame_key(&self.room_id)
    }

    fn host(&self) -> Result<PublicKey, String> {
        self.keys
            .get(&self.host_name)
            .map(|key| PublicKey(key.public))
            .ok_or_else(|| {
                format!("the fixture has no host key {:?}", self.host_name)
            })
    }

    fn key_id(&self, name: &str) -> Result<KeyId, String> {
        self.keys
            .get(name)
            .map(|key| PublicKey(key.public).id())
            .ok_or_else(|| format!("the fixture has no key {name:?}"))
    }

    fn signer(&self, name: &str) -> Result<SessionKey, String> {
        self.keys
            .get(name)
            .map(|key| SessionKey::from_seed(key.private))
            .ok_or_else(|| format!("the fixture has no key {name:?}"))
    }
}

/// One fixture keypair, with the fixture's own two internal checks re-derived rather
/// than trusted: a mismatched pair would make every derived frame wrong with nothing
/// naming why.
fn fixture_key(name: &str, entry: &Value) -> Result<FixtureKey, String> {
    let public = to32(&hex_bytes(text_of(entry, "public")?)?)?;
    let private = to32(&hex_bytes(text_of(entry, "private")?)?)?;
    if SessionKey::from_seed(private).public().0 != public {
        return Err(format!("{name}: the private half is not its public half"));
    }
    Ok(FixtureKey { public, private })
}

fn text_of<'a>(value: &'a Value, member: &str) -> Result<&'a str, String> {
    value
        .get(member)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("an entry has no string {member:?}"))
}

fn to32(raw: &[u8]) -> Result<[u8; 32], String> {
    raw.try_into()
        .map_err(|_| "not a 32-byte value".to_string())
}

/// Hex with or without the spaces a vector writes between bytes.
fn hex_bytes(text: &str) -> Result<Vec<u8>, String> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    compact
        .as_bytes()
        .chunks(2)
        .map(|pair| decode_pair(pair, text))
        .collect()
}

fn decode_pair(pair: &[u8], text: &str) -> Result<u8, String> {
    let digits = str::from_utf8(pair).map_err(|error| error.to_string())?;
    u8::from_str_radix(digits, 16)
        .map_err(|error| format!("{text:?} is not hex: {error}"))
}

fn hex_of(raw: &[u8]) -> String {
    use std::fmt::Write as _;
    raw.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// The vector directory: `SELVAGE_VECTORS` when the build supplies it, as the other
/// harness tests read it.
fn vectors_root() -> PathBuf {
    env::var_os("SELVAGE_VECTORS").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vectors"),
        PathBuf::from,
    )
}

fn id_of(vector: &Value) -> String {
    vector
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_string()
}

fn is_frame_vector(vector: &Value) -> bool {
    vector.get("kind").and_then(Value::as_str) == Some("frame")
}

fn load_frame_vectors() -> Result<Vec<Value>, String> {
    let dir = vectors_root().join("peer");
    let entries = fs::read_dir(&dir)
        .map_err(|error| format!("{} is unreadable: {error}", dir.display()))?;
    let mut vectors = Vec::new();
    for entry in entries {
        let path = entry.map_err(|error| error.to_string())?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = fs::read_to_string(&path).map_err(|error| {
            format!("{} is unreadable: {error}", path.display())
        })?;
        let vector: Value = serde_json::from_str(&text).map_err(|error| {
            format!("{} is not JSON: {error}", path.display())
        })?;
        if is_frame_vector(&vector) {
            vectors.push(vector);
        }
    }
    vectors.sort_by_key(id_of);
    Ok(vectors)
}

/// One frame a vector produced, and what a receiver would read in it.
#[derive(Clone)]
struct Frame {
    bytes: Vec<u8>,
    envelope: Option<Envelope>,
}

/// The plaintext a recipe seals: hex bytes, or a payload written as canonical JSON.
fn plaintext_of(spec: &Value) -> Result<Vec<u8>, String> {
    if let Some(hex) = spec.get("plaintext").and_then(Value::as_str) {
        return hex_bytes(hex);
    }
    let payload = spec
        .get("payload")
        .ok_or_else(|| "a recipe needs `plaintext` or `payload`".to_string())?;
    serde_json::to_vec(payload).map_err(|error| error.to_string())
}

/// The frame a `seal` step describes, and the vector's own cache of its bytes.
fn sealed_frame(fixture: &Fixture, step: &Value) -> Result<Frame, String> {
    let spec = step
        .get("recipe")
        .ok_or_else(|| "a `seal` needs a recipe".to_string())?;
    let signer = fixture.signer(text_of(spec, "sign")?)?;
    let frame_key = fixture.frame_key();
    let envelope = seal(
        &Recipe {
            room_id: &fixture.room_id,
            frame_key: &frame_key,
            kind: spec
                .get("kind")
                .and_then(Value::as_u64)
                .ok_or_else(|| "a recipe needs a kind".to_string())?,
            epoch: spec.get("epoch").and_then(Value::as_u64).unwrap_or(0),
            counter: spec
                .get("counter")
                .and_then(Value::as_u64)
                .ok_or_else(|| "a recipe needs a counter".to_string())?,
            nonce: nonce_of(spec)?,
            signer: &signer,
        },
        &plaintext_of(spec)?,
    )
    .map_err(|error| error.to_string())?;
    let bytes = envelope.bytes();
    same_bytes(step, &bytes)?;
    Ok(Frame {
        bytes,
        envelope: Some(envelope),
    })
}

fn nonce_of(spec: &Value) -> Result<[u8; 12], String> {
    let raw = hex_bytes(text_of(spec, "nonce")?)?;
    raw.get(..12)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| "a nonce is 12 bytes".to_string())
}

/// The deliberate corruption a `corrupt` step applies: the one place a vector's bytes are
/// not derivable, and the place the corruption itself is re-derived.
fn corrupted_frame(source: &[u8], step: &Value) -> Result<Vec<u8>, String> {
    if step.get("xor").is_some() {
        return xor_frame(source, step);
    }
    if let Some(dropped) = step.get("truncate").and_then(Value::as_u64) {
        let dropped_usize = usize::try_from(dropped)
            .map_err(|_| "`truncate` is out of range".to_string())?;
        let keep = source.len().saturating_sub(dropped_usize);
        return source.get(..keep).map(<[u8]>::to_vec).ok_or_else(|| {
            "`truncate` must leave some of the frame".to_string()
        });
    }
    if let Some(appended) = step.get("append").and_then(Value::as_str) {
        let mut out = source.to_vec();
        out.extend_from_slice(&hex_bytes(appended)?);
        return Ok(out);
    }
    Err("`corrupt` needs `xor`, `truncate` or `append`".to_string())
}

fn xor_frame(source: &[u8], step: &Value) -> Result<Vec<u8>, String> {
    let named = step
        .get("at")
        .and_then(Value::as_u64)
        .ok_or_else(|| "`xor` needs the `at` byte it flips".to_string())?;
    let offset = usize::try_from(named)
        .map_err(|_| "`at` is out of range".to_string())?;
    let mask = step
        .get("xor")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .ok_or_else(|| "`xor` is a byte".to_string())?;
    let mut out = source.to_vec();
    let byte = out.get_mut(offset).ok_or_else(|| {
        format!("`at` must name a byte of the {}-byte frame", source.len())
    })?;
    *byte ^= mask;
    Ok(out)
}

/// The vector's `hex` is a checked cache of the bytes it claims.
fn same_bytes(step: &Value, raw: &[u8]) -> Result<(), String> {
    let want = text_of(step, "hex")?;
    if hex_bytes(want)? != raw {
        return Err(format!(
            "the recipe produces other bytes than the vector carries:\n  vector: {want}\n  sealed: {}",
            hex_of(raw)
        ));
    }
    Ok(())
}

fn frame_by_name<'a>(
    frames: &'a BTreeMap<String, Frame>,
    step: &Value,
) -> Result<&'a Frame, String> {
    let name = text_of(step, "frame")?;
    frames
        .get(name)
        .ok_or_else(|| format!("no frame named {name:?}"))
}

/// Applies a `kind = 0` plaintext to the replica, the way a receiver does: message type 0
/// (sync), a sync sub-type, and a length-prefixed payload.
fn apply_stream(doc: &Doc, plaintext: &[u8]) -> Result<(), String> {
    let mut at = 0usize;
    while at < plaintext.len() {
        let next = read_message(doc, plaintext, at)?;
        at = next;
    }
    Ok(())
}

/// One y-protocols message: its type, its sync sub-type and its payload.
fn read_message(
    doc: &Doc,
    plaintext: &[u8],
    at: usize,
) -> Result<usize, String> {
    let (message_type, after_type) =
        read_varuint(plaintext, at).map_err(|e| e.to_string())?;
    if message_type != 0 {
        return Err(format!(
            "a kind 0 stream carries message type {message_type}"
        ));
    }
    let (sync_type, after_sync) =
        read_varuint(plaintext, after_type).map_err(|e| e.to_string())?;
    let (length, after_length) =
        read_varuint(plaintext, after_sync).map_err(|e| e.to_string())?;
    let payload_length =
        usize::try_from(length).map_err(|_| "a payload length".to_string())?;
    let end = after_length
        .checked_add(payload_length)
        .ok_or_else(|| "a payload length overflows".to_string())?;
    let payload = plaintext
        .get(after_length..end)
        .ok_or_else(|| "the stream runs out inside a payload".to_string())?;
    if matches!(sync_type, 1 | 2) {
        let update = yrs::Update::decode_v1(payload)
            .map_err(|e| format!("not a v1 update: {e}"))?;
        doc.transact_mut()
            .apply_update(update)
            .map_err(|e| format!("the update does not apply: {e}"))?;
    }
    Ok(end)
}

/// One vector's receiver: one reader, one replica, and the frames it has produced.
struct Replay {
    reader: Reader,
    replica: Doc,
    frames: BTreeMap<String, Frame>,
}

impl Replay {
    fn new(fixture: &Fixture) -> Result<Self, String> {
        Ok(Self {
            reader: Reader::new(
                &fixture.room_id,
                RoomKey(fixture.room_key),
                fixture.host()?,
            ),
            replica: Doc::new(),
            frames: BTreeMap::new(),
        })
    }

    fn step(&mut self, fixture: &Fixture, step: &Value) -> Result<(), String> {
        match step.get("op").and_then(Value::as_str) {
            Some("seal") => self.seal(fixture, step),
            Some("corrupt") => self.corrupt(step),
            Some("expectVerify") => self.expect_verify(step),
            Some("expectReject") => self.expect_reject(step),
            Some("expectPlaintext") => self.expect_plaintext(fixture, step),
            Some("expectListing") => self.expect_listing(step),
            Some("expectHolds") => self.expect_holds(fixture, step),
            Some("expectDoc") => self.expect_doc(step),
            other => Err(format!("`{other:?}` is not a frame-layer step")),
        }
    }

    fn seal(&mut self, fixture: &Fixture, step: &Value) -> Result<(), String> {
        let produced = sealed_frame(fixture, step)?;
        self.frames
            .insert(text_of(step, "frame")?.to_string(), produced);
        Ok(())
    }

    fn corrupt(&mut self, step: &Value) -> Result<(), String> {
        let source = frame_by_name(&self.frames, step)?.bytes.clone();
        let raw = corrupted_frame(&source, step)?;
        same_bytes(step, &raw)?;
        let envelope = Envelope::parse(&raw).ok();
        self.frames.insert(
            text_of(step, "as")?.to_string(),
            Frame {
                bytes: raw,
                envelope,
            },
        );
        Ok(())
    }

    fn expect_verify(&mut self, step: &Value) -> Result<(), String> {
        let bytes = frame_by_name(&self.frames, step)?.bytes.clone();
        let verdict = self.reader.read(&bytes);
        if !verdict.ok {
            return Err(format!(
                "the receiver refused the frame with `{}`",
                verdict.reason.unwrap_or_default()
            ));
        }
        if verdict.kind == Some(0) {
            apply_stream(&self.replica, &verdict.plaintext)?;
        }
        Ok(())
    }

    fn expect_reject(&mut self, step: &Value) -> Result<(), String> {
        let bytes = frame_by_name(&self.frames, step)?.bytes.clone();
        let verdict = self.reader.read(&bytes);
        let want = text_of(step, "reason")?;
        if verdict.ok {
            return Err(format!(
                "the receiver applied the frame, and {want} is claimed"
            ));
        }
        if verdict.reason.as_deref() != Some(want) {
            return Err(format!(
                "the receiver refused with `{}`, and {want} is claimed",
                verdict.reason.unwrap_or_default()
            ));
        }
        Ok(())
    }

    fn expect_plaintext(
        &self,
        fixture: &Fixture,
        step: &Value,
    ) -> Result<(), String> {
        let frame = frame_by_name(&self.frames, step)?;
        let envelope = frame
            .envelope
            .as_ref()
            .ok_or_else(|| "the frame is not an envelope".to_string())?;
        let frame_key = fixture.frame_key();
        let plaintext = opens(&frame_key, &fixture.room_id, envelope)
            .map_err(|error| format!("the frame does not open: {error}"))?;
        verify_signed_by(fixture, step, envelope)?;
        compare_plaintext(step, &plaintext)
    }

    fn expect_listing(&self, step: &Value) -> Result<(), String> {
        let claimed = string_list(step, "listing")?;
        if self.reader.listing != claimed {
            return Err(format!(
                "the receiver's listing is {:?}, and the vector claims {claimed:?}",
                self.reader.listing
            ));
        }
        Ok(())
    }

    fn expect_holds(
        &self,
        fixture: &Fixture,
        step: &Value,
    ) -> Result<(), String> {
        let key_id = fixture.key_id(text_of(step, "sign")?)?;
        let held = self.reader.holds.get(&key_id).cloned().unwrap_or_default();
        let claimed = string_list(step, "holds")?;
        if held != claimed {
            return Err(format!(
                "the receiver holds {held:?} for that key, and the vector claims {claimed:?}"
            ));
        }
        Ok(())
    }

    fn expect_doc(&self, step: &Value) -> Result<(), String> {
        let path = text_of(step, "path")?;
        let claimed = text_of(step, "text")?;
        let text = self.replica.get_or_insert_text(path);
        let txn = self.replica.transact();
        let actual = text.get_string(&txn);
        if actual != claimed {
            return Err(format!(
                "the replica holds {actual:?} in {path}, and the vector claims {claimed:?}"
            ));
        }
        Ok(())
    }
}

/// The signature claim a `expectPlaintext` step makes, against the fixture key it names.
fn verify_signed_by(
    fixture: &Fixture,
    step: &Value,
    envelope: &Envelope,
) -> Result<(), String> {
    let Some(signed_by) = step.get("signed_by").and_then(Value::as_str) else {
        return Ok(());
    };
    let key = fixture.signer(signed_by)?.public();
    let aad = associated_data(
        &fixture.room_id,
        envelope.kind,
        envelope.epoch,
        envelope.key_id,
    );
    if key.verifies(&signing_input(&aad, envelope), &envelope.signature) {
        return Ok(());
    }
    Err(format!("the signature does not verify against {signed_by}"))
}

/// What the frame's plaintext is claimed to be: the bytes, or the JSON value they encode.
fn compare_plaintext(step: &Value, plaintext: &[u8]) -> Result<(), String> {
    if let Some(want) = step.get("plaintext").and_then(Value::as_str)
        && hex_bytes(want)? != plaintext
    {
        return Err(
            "the frame opens to other bytes than the vector claims".to_string()
        );
    }
    let Some(payload) = step.get("payload") else {
        return Ok(());
    };
    let read: Value = serde_json::from_slice(plaintext)
        .map_err(|error| format!("the frame does not open to JSON: {error}"))?;
    if &read == payload {
        return Ok(());
    }
    Err(format!(
        "the frame opens to {read}, and the vector claims {payload}"
    ))
}

fn string_list(step: &Value, member: &str) -> Result<Vec<String>, String> {
    let items = step
        .get(member)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("`{member}` is a list of paths"))?;
    let mut out = Vec::new();
    for item in items {
        let text = item
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("`{member}` is a list of paths"))?;
        out.push(text);
    }
    Ok(out)
}

/// Replays one frame vector, returning how many steps asserted something.
fn replay(fixture: &Fixture, vector: &Value) -> Result<usize, String> {
    let steps = vector
        .get("steps")
        .and_then(Value::as_array)
        .ok_or_else(|| "a vector needs steps".to_string())?;
    let mut subject = Replay::new(fixture)?;
    let mut assertions = 0usize;
    for (index, step) in steps.iter().enumerate() {
        subject.step(fixture, step).map_err(|error| {
            format!("step {index} (`{}`): {error}", op_of(step))
        })?;
        if op_of(step).starts_with("expect") {
            assertions = assertions.saturating_add(1);
        }
    }
    Ok(assertions)
}

fn op_of(step: &Value) -> &str {
    step.get("op").and_then(Value::as_str).unwrap_or("?")
}

#[test]
fn every_frame_vector_holds() {
    let fixture_path = vectors_root().join("fixture").join("keys.json");
    let fixture =
        Fixture::load(&fixture_path).expect("the fixture is readable");
    let vectors =
        load_frame_vectors().expect("the peer vector directory is readable");
    assert!(
        !vectors.is_empty(),
        "no frame vectors in {}: this replay has nothing to run",
        vectors_root().join("peer").display()
    );
    let mut failures = Vec::new();
    let mut assertions = 0usize;
    for vector in &vectors {
        match replay(&fixture, vector) {
            Ok(count) => assertions = assertions.saturating_add(count),
            Err(error) => failures.push(format!("{}: {error}", id_of(vector))),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} frame vectors failed:\n{}",
        failures.len(),
        vectors.len(),
        failures.join("\n")
    );
    println!(
        "{} frame vectors replayed, {assertions} assertion steps, from {}",
        vectors.len(),
        vectors_root().join("peer").display()
    );
}

/// The corpus pins the collection: a deleted vector replays green on less, so the count
/// is asserted here as it is in `schema/validate.py`'s `EXPECTED_PEER_VECTORS`.
#[test]
fn the_frame_layer_holds_the_corpus_it_was_written_for() {
    let vectors =
        load_frame_vectors().expect("the peer vector directory is readable");
    assert_eq!(
        vectors.len(),
        19,
        "the peer layer has 19 frame vectors; this run found {}",
        vectors.len()
    );
    assert_eq!(encode_key(&[0u8; 32]).len(), 43);
}
