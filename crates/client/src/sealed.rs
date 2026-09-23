//! `selvage/2`'s sealed frame: the bytes, and the verdict a receiver reaches on them.
//!
//! [`CANONICAL.md`](https://github.com/selvage-protocol/specification/blob/main/CANONICAL.md)
//! §6.1 fixes the envelope, the key schedule, the associated data, the signature input,
//! the key id, the counter and the order a receiver reads the bytes in. This module is
//! that section in Rust: it seals and signs a frame, opens it again, and reads a frame
//! the way §6.1 says, reporting the first step that refuses it.
//!
//! The five kinds the version defines are `0` (a y-protocols stream), `1` (the room
//! state), `2` (the closing), `3` (the sender's holds) and `4` (the session-key
//! announcement). `PROTOCOL.md` §7.1 says what each carries; this module is the bytes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error as StdError;
use std::fmt;

use aes_gcm::aead::{Aead, KeyInit, Payload as AeadPayload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};

use serde::Deserialize;
use serde_json::Value;
use yrs::sync::{
    Message as YMessage, MessageReader, SyncMessage as YSyncMessage,
};
use yrs::updates::decoder::DecoderV1;

/// The five kinds this version defines, in the order `CANONICAL.md` §6.1 gives them.
pub const KINDS: [u64; 5] = [0, 1, 2, 3, 4];

/// The ten reasons a receiver reports, in the order §6.1's table reads them. The order is
/// normative because the report is observable.
pub const REASONS: [&str; 10] = [
    "bad_envelope",
    "unknown_kind",
    "unknown_epoch",
    "uncommitted_key",
    "replayed_counter",
    "bad_signature",
    "bad_aead",
    "bad_payload",
    "stale_issued",
    "unauthorised_content",
];

/// Bytes that are not an envelope, or a value that is not a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedError(String);

impl SealedError {
    /// What the crate's own layers report a frame, a key or a payload with.
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for SealedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for SealedError {}

/// A 32-byte public key in the fragment's own encoding: base64url, unpadded.
///
/// The encoding is canonical (§6.1): a 43-character value whose last character carries
/// non-zero pad bits spells no 32-byte key, and [`PublicKey::parse`] refuses it rather
/// than resolving it to something a strict decoder would not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicKey(pub [u8; 32]);

impl PublicKey {
    /// The key a canonical base64url value spells, or `None`.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        decode_key(text).map(Self)
    }

    /// This key's canonical base64url spelling.
    #[must_use]
    pub fn encode(self) -> String {
        encode_key(&self.0)
    }

    /// `SHA-256` of the key's 32 bytes, first 8 — §6.1's index, not an identity.
    #[must_use]
    pub fn id(self) -> KeyId {
        KeyId(key_id(&self.0))
    }

    /// Whether a signature over `message` verifies against this key.
    ///
    /// A public key the curve refuses is a verification that fails, not a panic: a state
    /// may name any 32 bytes, and a receiver decides about them.
    #[must_use]
    pub fn verifies(self, message: &[u8], signature: &[u8; 64]) -> bool {
        let Ok(key) = VerifyingKey::from_bytes(&self.0) else {
            return false;
        };
        key.verify_strict(message, &Signature::from_bytes(signature))
            .is_ok()
    }
}

/// The first 8 bytes of a public key's `SHA-256`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyId(pub [u8; 8]);

impl KeyId {
    /// The id as lowercase hex, which is how a report names it.
    #[must_use]
    pub fn hex(self) -> String {
        use std::fmt::Write as _;
        self.0.iter().fold(String::new(), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
    }
}

/// `SHA-256(public)[0..8]` (§6.1).
#[must_use]
pub fn key_id(public: &[u8; 32]) -> [u8; 8] {
    let digest = Sha256::digest(public);
    let mut id = [0u8; 8];
    if let (Some(head), Some(target)) = (digest.get(..8), id.get_mut(..8)) {
        target.copy_from_slice(head);
    }
    id
}

/// The room key every frame of a room is sealed under, 32 bytes from the invite's `k`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomKey(pub [u8; 32]);

impl RoomKey {
    /// The key a canonical base64url value spells, or `None`.
    ///
    /// The fragment's `k` is written the way its `h` is (`PROTOCOL.md` §5.1): 32 bytes,
    /// base64url, unpadded, and as canonical as a public key's
    /// ([`crate::sealed::PublicKey::parse`]).
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        decode_key(text).map(Self)
    }

    /// The frame key: `HKDF-SHA256(ikm = room key, salt = room id, info = "selvage/2
    /// frame", L = 32)`.
    #[must_use]
    pub fn frame_key(self, room_id: &str) -> FrameKey {
        let hkdf = Hkdf::<Sha256>::new(Some(room_id.as_bytes()), &self.0);
        let mut out = [0u8; 32];
        let _ = hkdf.expand(b"selvage/2 frame", &mut out);
        FrameKey(out)
    }
}

/// The key a sealed frame is sealed under. It carries no identity and is never on the
/// wire; every member of a room derives the same one.
#[derive(Clone, Copy)]
pub struct FrameKey([u8; 32]);

impl FrameKey {
    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.0))
    }
}

/// A connection's session keypair: Ed25519, minted per connection and never persisted.
pub struct SessionKey {
    signing: SigningKey,
}

impl SessionKey {
    /// A fresh keypair from the platform's CSPRNG.
    ///
    /// # Errors
    ///
    /// Returns the platform's error when the CSPRNG cannot be read.
    pub fn generate() -> Result<Self, SealedError> {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed)
            .map_err(|e| SealedError::new(format!("no randomness: {e}")))?;
        Ok(Self::from_seed(seed))
    }

    /// The keypair a 32-byte seed names, which is what makes a fixture key reproducible.
    #[must_use]
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self {
            signing: SigningKey::from_bytes(&seed),
        }
    }

    /// This connection's public half, which the room state commits.
    #[must_use]
    pub fn public(&self) -> PublicKey {
        PublicKey(self.signing.verifying_key().to_bytes())
    }

    /// Over `message`, which for a frame is §6.1's signature input.
    #[must_use]
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.signing.sign(message).to_bytes()
    }
}

impl fmt::Debug for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SessionKey(..)")
    }
}

/// 12 fresh bytes for one frame's nonce, never derived from a counter or a key.
///
/// # Errors
///
/// Returns the platform's error when the CSPRNG cannot be read.
pub fn fresh_nonce() -> Result<[u8; 12], SealedError> {
    let mut nonce = [0u8; 12];
    getrandom::fill(&mut nonce)
        .map_err(|e| SealedError::new(format!("no randomness: {e}")))?;
    Ok(nonce)
}

// --- varints, and the two byte strings §6.1 builds out of them ------------------

/// `varUint` is LEB128 (`PROTOCOL.md` §7).
#[must_use]
pub fn varuint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = u8::try_from(value & 0x7f).unwrap_or(0);
        let shifted = value.checked_shr(7).unwrap_or(0);
        if shifted == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
        value = shifted;
    }
}

/// A length in front of the bytes, as `PROTOCOL.md` §7 writes one.
#[must_use]
pub fn varuint8array(raw: &[u8]) -> Vec<u8> {
    let mut out = varuint(u64::try_from(raw.len()).unwrap_or(u64::MAX));
    out.extend_from_slice(raw);
    out
}

/// Reads one `varUint`, returning it and the offset after it.
///
/// Public because a `kind = 0` plaintext is a stream of these and the session that applies
/// it is elsewhere in this crate.
///
/// # Errors
///
/// Returns an error when the bytes run out inside the value or it does not fit 64 bits.
pub fn read_varuint(
    data: &[u8],
    at: usize,
) -> Result<(u64, usize), SealedError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut cursor = at;
    loop {
        let byte = *data.get(cursor).ok_or_else(|| {
            SealedError::new("the bytes run out inside a varUint")
        })?;
        cursor = cursor.saturating_add(1);
        let low = byte & 0x7f;
        // A byte read at shift 63 may set only bit 0. Anything above it is a value past
        // 64 bits, which a silent `checked_shl` would truncate into a different counter.
        if shift == 63 && low & 0x7e != 0 {
            return Err(SealedError::new("a varUint longer than 64 bits"));
        }
        let part = u64::from(low).checked_shl(shift).unwrap_or(0);
        value |= part;
        if byte & 0x80 == 0 {
            return Ok((value, cursor));
        }
        shift = shift.saturating_add(7);
        if shift > 63 {
            return Err(SealedError::new("a varUint longer than 64 bits"));
        }
    }
}

// --- the envelope ---------------------------------------------------------------

/// One `selvage/2` binary frame, as §6.1's table writes its fields.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub key_id: KeyId,
    pub kind: u64,
    pub epoch: u64,
    pub counter: u64,
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
    pub signature: [u8; 64],
}

impl Envelope {
    /// The frame's bytes: the three fixed-width fields at their widths, the three
    /// `varUint` fields, and the ciphertext length-prefixed.
    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.key_id.0);
        out.extend_from_slice(&varuint(self.kind));
        out.extend_from_slice(&varuint(self.epoch));
        out.extend_from_slice(&varuint(self.counter));
        out.extend_from_slice(&self.nonce);
        out.extend_from_slice(&varuint8array(&self.ciphertext));
        out.extend_from_slice(&self.signature);
        out
    }

    /// Read the layout of §6.1's step 1.
    ///
    /// # Errors
    ///
    /// Returns an error when a field is missing, a fixed-width field runs out, or any
    /// byte is left over after the signature.
    pub fn parse(raw: &[u8]) -> Result<Self, SealedError> {
        let id = fixed::<8>(raw, 0, "key id")?;
        let mut at = 8usize;
        let (kind, after) = read_varuint(raw, at)?;
        at = after;
        let (epoch, after) = read_varuint(raw, at)?;
        at = after;
        let (counter, after) = read_varuint(raw, at)?;
        at = after;
        let nonce = fixed::<12>(raw, at, "nonce")?;
        at = at.saturating_add(12);
        let (size_raw, after) = read_varuint(raw, at)?;
        at = after;
        let size = usize::try_from(size_raw).map_err(|_| {
            SealedError::new("the ciphertext length is not a length")
        })?;
        let end = at.checked_add(size).ok_or_else(|| {
            SealedError::new("the ciphertext length overflows")
        })?;
        let ciphertext = raw.get(at..end).ok_or_else(|| {
            SealedError::new("the bytes run out inside the ciphertext")
        })?;
        let signature = fixed::<64>(raw, end, "signature")?;
        let used = end.saturating_add(64);
        if used != raw.len() {
            return Err(SealedError::new(format!(
                "{} bytes left over after the signature",
                raw.len().saturating_sub(used)
            )));
        }
        Ok(Self {
            key_id: KeyId(id),
            kind,
            epoch,
            counter,
            nonce,
            ciphertext: ciphertext.to_vec(),
            signature,
        })
    }
}

/// `N` bytes at `at`, or an error naming the field that ran out.
fn fixed<const N: usize>(
    raw: &[u8],
    at: usize,
    what: &str,
) -> Result<[u8; N], SealedError> {
    let slice = raw.get(at..at.saturating_add(N)).ok_or_else(|| {
        SealedError::new(format!("the bytes run out inside the {what}"))
    })?;
    let mut out = [0u8; N];
    out.copy_from_slice(slice);
    Ok(out)
}

/// `aad = varUint8Array("selvage/2") ‖ varUint8Array(room) ‖ varUint(kind) ‖
/// varUint(epoch) ‖ varUint8Array(key_id)`.
#[must_use]
#[expect(
    clippy::too_many_arguments,
    reason = "the four members of §6.1's associated data are the four arguments"
)]
pub fn associated_data(
    room_id: &str,
    kind: u64,
    epoch: u64,
    key_id: KeyId,
) -> Vec<u8> {
    let mut out = varuint8array(b"selvage/2");
    out.extend_from_slice(&varuint8array(room_id.as_bytes()));
    out.extend_from_slice(&varuint(kind));
    out.extend_from_slice(&varuint(epoch));
    out.extend_from_slice(&varuint8array(&key_id.0));
    out
}

/// `signed = aad ‖ varUint(counter) ‖ varUint8Array(nonce) ‖ varUint8Array(ciphertext)`.
#[must_use]
pub fn signing_input(aad: &[u8], envelope: &Envelope) -> Vec<u8> {
    let mut out = aad.to_vec();
    out.extend_from_slice(&varuint(envelope.counter));
    out.extend_from_slice(&varuint8array(&envelope.nonce));
    out.extend_from_slice(&varuint8array(&envelope.ciphertext));
    out
}

/// What a producer seals: the frame's kind, counter and nonce, the room it is for, and
/// the key that signs it.
pub struct Recipe<'a> {
    pub room_id: &'a str,
    pub frame_key: &'a FrameKey,
    pub kind: u64,
    pub epoch: u64,
    pub counter: u64,
    pub nonce: [u8; 12],
    pub signer: &'a SessionKey,
}

/// Seal and sign one frame: the bytes a `selvage/2` client hands the relay.
///
/// # Errors
///
/// Returns an error when the AEAD refuses the inputs.
pub fn seal(
    recipe: &Recipe<'_>,
    plaintext: &[u8],
) -> Result<Envelope, SealedError> {
    let key_id = recipe.signer.public().id();
    let aad =
        associated_data(recipe.room_id, recipe.kind, recipe.epoch, key_id);
    let ciphertext = recipe
        .frame_key
        .cipher()
        .encrypt(
            Nonce::from_slice(&recipe.nonce),
            AeadPayload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| SealedError::new("the AEAD refused the plaintext"))?;
    let mut envelope = Envelope {
        key_id,
        kind: recipe.kind,
        epoch: recipe.epoch,
        counter: recipe.counter,
        nonce: recipe.nonce,
        ciphertext,
        signature: [0u8; 64],
    };
    envelope.signature = recipe.signer.sign(&signing_input(&aad, &envelope));
    Ok(envelope)
}

/// The AEAD's output, with the frame's own associated data — §6.1's step 7.
///
/// # Errors
///
/// Returns an error when the AEAD does not open the ciphertext.
pub fn opens(
    frame_key: &FrameKey,
    room_id: &str,
    envelope: &Envelope,
) -> Result<Vec<u8>, SealedError> {
    let aad = associated_data(
        room_id,
        envelope.kind,
        envelope.epoch,
        envelope.key_id,
    );
    frame_key
        .cipher()
        .decrypt(
            Nonce::from_slice(&envelope.nonce),
            AeadPayload {
                msg: &envelope.ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| SealedError::new("the AEAD does not open"))
}

// --- the four sealed payloads ---------------------------------------------------

/// One entry of a room state's `peers`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PeerEntry {
    pub peer_id: String,
    pub role: String,
}

/// `kind = 1`: the room's listing, the roles the host assigns, and the state's edition.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RoomState {
    pub issued: u64,
    pub listing: Vec<String>,
    /// Keyed by the public key's canonical base64url spelling.
    pub peers: BTreeMap<String, PeerEntry>,
}

/// `kind = 2`: the host's statement that the room is over.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Closing {
    pub closing: bool,
    pub issued: u64,
}

/// `kind = 3`: the sender's whole held set.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Holds {
    pub holds: Vec<String>,
}

/// `kind = 4`: the announcing connection's session key, and the role it believes it has.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Announcement {
    pub key: String,
    #[serde(default)]
    pub role: Option<String>,
}

/// The payload a frame's plaintext carries, once step 8 has read it.
#[derive(Debug, Clone)]
pub enum Payload {
    RoomState(RoomState),
    Closing(Closing),
    Holds(Holds),
    Announcement(Announcement),
    /// A `kind = 0` plaintext is the y-protocols stream of `PROTOCOL.md` §7, whole.
    Content,
}

/// What a receiver did with one frame: accepted it, or refused it and why.
///
/// The two flags answer two questions the reason does not: whether the frame was accepted, and
/// whether the refusal was the equal-`issued` one §13.3 asks a client to tell apart.
#[expect(
    clippy::struct_excessive_bools,
    reason = "accepted, and refused at the mark: two questions the reason alone does not answer"
)]
#[derive(Debug, Clone)]
pub struct Verdict {
    pub ok: bool,
    pub reason: Option<String>,
    /// The key id whose signature verified, or failed to.
    pub sender: Option<KeyId>,
    pub kind: Option<u64>,
    pub counter: Option<u64>,
    pub plaintext: Vec<u8>,
    pub payload: Option<Payload>,
    /// §13.3's equal-`issued` refusal, which the reason does not distinguish: the frame was a
    /// room state at exactly the edition this receiver holds, so two states were published at
    /// one `issued` and this is the second. The reason stays `stale_issued` and this is the
    /// local annotation beside it, which is what the `SHOULD` asks a client to be able to say.
    pub conflict: bool,
}

impl Verdict {
    fn refused(reason: &str, envelope: Option<&Envelope>) -> Self {
        Self {
            ok: false,
            reason: Some(reason.to_string()),
            sender: envelope.map(|e| e.key_id),
            kind: envelope.map(|e| e.kind),
            counter: envelope.map(|e| e.counter),
            plaintext: Vec::new(),
            payload: None,
            conflict: false,
        }
    }
}

/// One committed entry: the key that verified, its role, and the seat it is labelled.
#[derive(Debug, Clone)]
pub struct Committed {
    pub key: PublicKey,
    pub role: String,
    pub peer_id: String,
}

/// The highest counter a receiver has not refused from one key, and when it last moved
/// (`CANONICAL.md` §6.1).
#[derive(Debug, Clone, Copy)]
struct Mark {
    counter: u64,
    /// The order the marks were last moved in, which is what [`Reader::cap_marks`] evicts by.
    seen: u64,
}

/// How many marks a receiver keeps for keys its applied state does not commit
/// (`PROTOCOL.md` §13.3's `MAY`). A room's roster is tens of seats on a busy day and each
/// committed key has a mark of its own, so this bounds the announcement-driven growth without
/// touching an honest room. It is not a bound on the keys themselves: nothing here holds one.
const UNCOMMITTED_MARKS: usize = 64;

/// One of `CANONICAL.md` §6.1's ten read steps that is also a client rule, so that a client's
/// mutation census has to remove it from the byte layer to see what the rule was doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Guard {
    /// Step 9: a state or a closing at or below the mark is applied anyway.
    Issued,
    /// Step 10: a committed `viewer`'s document content is applied.
    Roles,
}

/// The guards a receiver has had removed, for `PROTOCOL.md` §13.11's mutation census.
///
/// The frame layer's own census is `specification/runner/run_peer.py --mutation-census`,
/// which removes `runner/sealed.py`'s thirteen. The subset here is the two that are also
/// client rules; every other guard a subject mutation removes is the session's own and lives
/// in [`crate::peer`].
#[derive(Debug, Clone, Default)]
pub struct Mutations {
    removed: BTreeSet<Guard>,
}

impl Mutations {
    /// Removes a guard, which is what a census does to make a vector go red.
    pub fn remove(&mut self, guard: Guard) {
        let _ = self.removed.insert(guard);
    }

    /// Whether a guard has been removed.
    #[must_use]
    pub fn contains(&self, guard: Guard) -> bool {
        self.removed.contains(&guard)
    }
}

/// A conforming receiver's byte layer: §6.1's marks, and §13.3's committed keys and roles.
///
/// It holds the frame key and the host key the invite's fragment carries, and the marks
/// §6.1 says a receiver keeps for as long as it holds the room's keys. Every [`read`] is
/// one frame and one verdict; an accepted frame folds into the receiver exactly as
/// `PROTOCOL.md` §13.2, §13.3, §13.7 and §13.10 describe, so a sequence of frames is a
/// sequence of decisions and not a list of independent ones.
///
/// [`read`]: Reader::read
pub struct Reader {
    pub room_id: String,
    pub frame_key: FrameKey,
    pub host_key: PublicKey,
    /// The keys an applied state commits, under the canonical spelling of each key.
    pub committed: BTreeMap<String, Committed>,
    /// The guards a *client's* mutation census removes from the ten-step read
    /// (`runner/sealed.py`'s table, as much of it as a subject's mutation needs).
    pub mutations: Mutations,
    marks: HashMap<KeyId, Mark>,
    /// How many marks have been moved, which orders them for the cap in [`Reader::cap_marks`].
    marks_moved: u64,
    /// The listing of the last applied state, with §5's refused paths dropped.
    pub listing: Vec<String>,
    /// Each key's held paths, by key id.
    pub holds: HashMap<KeyId, Vec<String>>,
    pub issued: u64,
    pub ended: bool,
}

impl Reader {
    /// A reader that knows only the invite's two keys, which is a joiner before a state.
    #[must_use]
    pub fn new(room_id: &str, room_key: RoomKey, host_key: PublicKey) -> Self {
        Self {
            room_id: room_id.to_string(),
            frame_key: room_key.frame_key(room_id),
            host_key,
            committed: BTreeMap::new(),
            mutations: Mutations::default(),
            marks: HashMap::new(),
            marks_moved: 0,
            listing: Vec::new(),
            holds: HashMap::new(),
            issued: 0,
            ended: false,
        }
    }

    /// The committed entries an 8-byte id names, in UTF-16 code-unit order of the key.
    ///
    /// §6.1 makes the `key_id` an **index** and not an identity: a receiver resolves it
    /// by verifying against every key that id names, and a collision costs a verification
    /// that fails rather than a misattribution.
    #[must_use]
    pub fn by_key_id(&self, id: KeyId) -> Vec<&Committed> {
        // `BTreeMap` iterates in key order, which over ASCII is UTF-16 code-unit order.
        self.committed
            .values()
            .filter(|peer| peer.key.id() == id)
            .collect()
    }

    /// The role the applied state gives a key id, or `None` when nothing commits it.
    ///
    /// Two keys given `host`: the one whose key comes first is the host's connection.
    #[must_use]
    pub fn role_of(&self, id: KeyId) -> Option<&str> {
        let entries = self.by_key_id(id);
        entries
            .iter()
            .find(|peer| peer.role == "host")
            .or_else(|| entries.first())
            .map(|peer| peer.role.as_str())
    }

    /// The committed entry whose key a frame verified against, or `None`.
    #[must_use]
    pub fn entry_by_key_id(&self, id: KeyId) -> Option<&Committed> {
        self.by_key_id(id).into_iter().next()
    }

    /// The role the applied state gives **this** key, which is the role a frame that
    /// verified against it is read with (§13.4). A key id resolves to an entry by lookup;
    /// a key resolves to its own entry exactly.
    #[must_use]
    pub fn role_of_key(&self, key: PublicKey) -> Option<&str> {
        self.committed
            .values()
            .find(|peer| peer.key == key)
            .map(|peer| peer.role.as_str())
    }

    /// §6.1's table, in its order, with the first step that refuses the frame reported.
    #[must_use]
    pub fn read(&mut self, frame: &[u8]) -> Verdict {
        let Ok(envelope) = Envelope::parse(frame) else {
            return Verdict::refused("bad_envelope", None);
        };
        if !KINDS.contains(&envelope.kind) {
            return Verdict::refused("unknown_kind", Some(&envelope));
        }
        if envelope.epoch != 0 {
            return Verdict::refused("unknown_epoch", Some(&envelope));
        }
        if envelope.kind == 4 {
            return self.read_announcement(&envelope);
        }
        self.read_ordinary(&envelope)
    }

    /// `kind = 4`'s own order: the AEAD (7), the payload (8), the key (4), the signature
    /// (6), and last the mark (5).
    ///
    /// Its signer is inside its plaintext, so a receiver cannot verify anything before it
    /// opens the AEAD — the one place the table's order is not the read's.
    fn read_announcement(&mut self, envelope: &Envelope) -> Verdict {
        let Ok(plaintext) = opens(&self.frame_key, &self.room_id, envelope)
        else {
            return Verdict::refused("bad_aead", Some(envelope));
        };
        let Some(announcement) = parse_announcement(&plaintext) else {
            return Verdict::refused("bad_payload", Some(envelope));
        };
        let Some(key) = PublicKey::parse(&announcement.key) else {
            return Verdict::refused("bad_payload", Some(envelope));
        };
        if key.id() != envelope.key_id {
            return Verdict::refused("uncommitted_key", Some(envelope));
        }
        let aad = associated_data(
            &self.room_id,
            envelope.kind,
            envelope.epoch,
            envelope.key_id,
        );
        if !key.verifies(&signing_input(&aad, envelope), &envelope.signature) {
            return Verdict::refused("bad_signature", Some(envelope));
        }
        if self.replayed(key.id(), envelope.counter) {
            return Verdict::refused("replayed_counter", Some(envelope));
        }
        self.mark(key.id(), envelope.counter);
        Verdict {
            ok: true,
            reason: None,
            sender: Some(key.id()),
            kind: Some(4),
            counter: Some(envelope.counter),
            plaintext,
            payload: Some(Payload::Announcement(announcement)),
            conflict: false,
        }
    }

    fn read_ordinary(&mut self, envelope: &Envelope) -> Verdict {
        // A `kind = 1` or `2` frame verifies against the host key the fragment names; a
        // `kind = 0` or `3` one against the committed keys its `key_id` indexes. §6.1
        // makes the id an index and not an identity, so a collision is resolved by
        // verifying: every key the id names is tried and the frame belongs to the one
        // that verified. A collision therefore costs a verification that fails, never a
        // misattribution.
        let candidates: Vec<PublicKey> = if matches!(envelope.kind, 1 | 2) {
            // Only the host key may sign these two, and only if the frame's id names it:
            // a state signed by a committed session key is `uncommitted_key`, not a
            // signature failure.
            (self.host_key.id() == envelope.key_id)
                .then_some(self.host_key)
                .into_iter()
                .collect()
        } else {
            self.by_key_id(envelope.key_id)
                .into_iter()
                .map(|peer| peer.key)
                .collect()
        };
        if candidates.is_empty() {
            return Verdict::refused("uncommitted_key", Some(envelope));
        }
        if matches!(envelope.kind, 0 | 3)
            && self.replayed(envelope.key_id, envelope.counter)
        {
            return Verdict::refused("replayed_counter", Some(envelope));
        }
        let aad = associated_data(
            &self.room_id,
            envelope.kind,
            envelope.epoch,
            envelope.key_id,
        );
        let signed = signing_input(&aad, envelope);
        let Some(key) = candidates
            .into_iter()
            .find(|key| key.verifies(&signed, &envelope.signature))
        else {
            return Verdict::refused("bad_signature", Some(envelope));
        };
        let Ok(plaintext) = opens(&self.frame_key, &self.room_id, envelope)
        else {
            return Verdict::refused("bad_aead", Some(envelope));
        };
        let payload = match read_payload(envelope.kind, &plaintext) {
            Ok(payload) => payload,
            Err(reason) => return Verdict::refused(reason, Some(envelope)),
        };
        // Step 9: a state or a closing is ordered by its own `issued`.
        if let Some(issued) = payload_issued(payload.as_ref())
            && issued <= self.issued
            && !self.mutations.contains(Guard::Issued)
        {
            let mut verdict = Verdict::refused("stale_issued", Some(envelope));
            // §13.3: two connections of one host can publish states at one edition, and a
            // second state at the mark is that divergence rather than an old state. A closing
            // at the mark is not: it is one frame of one series, refused like any other.
            verdict.conflict = issued == self.issued
                && matches!(payload, Some(Payload::RoomState(_)));
            return verdict;
        }
        // Step 10: a committed `viewer` may not send document content.
        if envelope.kind == 0
            && is_content(&plaintext)
            && !self.mutations.contains(Guard::Roles)
            && self.role_of_key(key) == Some("viewer")
        {
            return Verdict::refused("unauthorised_content", Some(envelope));
        }
        self.accept(envelope, key.id(), plaintext, payload)
    }

    fn replayed(&self, id: KeyId, counter: u64) -> bool {
        counter <= self.marks.get(&id).map_or(0, |mark| mark.counter)
    }

    /// Moves a key's mark to `counter` (`CANONICAL.md` §6.1). Only an accepted frame does: a
    /// frame a receiver refuses never moves a mark, whatever step refused it.
    fn mark(&mut self, id: KeyId, counter: u64) {
        self.marks_moved = self.marks_moved.saturating_add(1);
        let seen = self.marks_moved;
        let mark = self.marks.entry(id).or_insert(Mark { counter: 0, seen });
        mark.counter = mark.counter.max(counter);
        mark.seen = seen;
        self.cap_marks();
    }

    /// `PROTOCOL.md` §13.3's `MAY`: a peer that holds the room key can announce keys without
    /// bound, and a receiver that kept a mark for every one carries that state for the life of
    /// the room. The marks of the keys the applied state commits are kept — they are what the
    /// room has told this receiver (`CANONICAL.md` §6.1) — and the rest are kept as far as
    /// [`UNCOMMITTED_MARKS`], the most recently moved first.
    ///
    /// What the cap gives up is the mark of a key no state commits, and the cost is bounded to
    /// one thing: such a key's *announcement* is read against its mark (step 5 is the only mark
    /// read before the key is resolved), so a replayed announcement from an evicted key is
    /// accepted again. Every other frame of that key is refused `uncommitted_key` whatever mark
    /// stands against it. That is the trade §13.3 states rather than a defect.
    fn cap_marks(&mut self) {
        let committed: BTreeSet<KeyId> =
            self.committed.values().map(|peer| peer.key.id()).collect();
        let uncommitted = self
            .marks
            .keys()
            .filter(|id| !committed.contains(*id))
            .count();
        if uncommitted <= UNCOMMITTED_MARKS {
            return;
        }
        let mut evictable: Vec<(u64, KeyId)> = self
            .marks
            .iter()
            .filter(|(id, _)| !committed.contains(*id))
            .map(|(id, mark)| (mark.seen, *id))
            .collect();
        evictable.sort_unstable();
        for (_, id) in evictable
            .into_iter()
            .take(uncommitted.saturating_sub(UNCOMMITTED_MARKS))
        {
            let _ = self.marks.remove(&id);
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "a frame, the key that verified, its plaintext and its payload are the whole of an acceptance"
    )]
    fn accept(
        &mut self,
        envelope: &Envelope,
        sender: KeyId,
        plaintext: Vec<u8>,
        payload: Option<Payload>,
    ) -> Verdict {
        if matches!(envelope.kind, 0 | 3) {
            self.mark(sender, envelope.counter);
        }
        match payload.as_ref() {
            Some(Payload::RoomState(state)) => self.apply_state(state),
            Some(Payload::Closing(closing)) => {
                self.issued = closing.issued;
                self.ended = true;
            }
            Some(Payload::Holds(holds)) => {
                let kept: Vec<String> = holds
                    .holds
                    .iter()
                    .filter(|path| usable_path(path))
                    .cloned()
                    .collect();
                self.holds.insert(sender, kept);
            }
            _ => {}
        }
        Verdict {
            ok: true,
            reason: None,
            sender: Some(sender),
            kind: Some(envelope.kind),
            counter: Some(envelope.counter),
            plaintext,
            payload,
            conflict: false,
        }
    }

    /// Folds a state this connection **published** into its own receiver (§7.1).
    ///
    /// The relay never hands a sender its own frame back, so a host that only read its peers'
    /// frames would hold no listing, no committed key and no `issued` — it would offer nothing
    /// to a peer and refuse the content of everything it committed itself.
    pub fn apply_own(&mut self, state: &RoomState) {
        self.apply_state(state);
    }

    fn apply_state(&mut self, state: &RoomState) {
        // `PROTOCOL.md` §13.3: a state replaces the receiver's keys and roles for the
        // whole room, and a key it does not name is uncommitted from that moment.
        self.committed = state
            .peers
            .iter()
            .filter_map(|(name, entry)| {
                let key = PublicKey::parse(name)?;
                Some((
                    name.clone(),
                    Committed {
                        key,
                        role: entry.role.clone(),
                        peer_id: entry.peer_id.clone(),
                    },
                ))
            })
            .collect();
        self.listing = state
            .listing
            .iter()
            .filter(|path| usable_path(path))
            .cloned()
            .collect();
        self.issued = state.issued;
    }
}

/// Step 8 for a frame whose plaintext is one of the four JSON payloads.
///
/// A `kind = 0` plaintext is the y-protocols stream of `PROTOCOL.md` §7 and not JSON: the
/// receiver that applies it is the one that decodes it, so this layer carries it whole.
fn read_payload(
    kind: u64,
    plaintext: &[u8],
) -> Result<Option<Payload>, &'static str> {
    if kind == 0 {
        return Ok(Some(Payload::Content));
    }
    let value: Value =
        serde_json::from_slice(plaintext).map_err(|_| "bad_payload")?;
    match kind {
        1 => {
            let state: RoomState =
                serde_json::from_value(value).map_err(|_| "bad_payload")?;
            if !state_is_well_formed(&state) {
                return Err("bad_payload");
            }
            Ok(Some(Payload::RoomState(state)))
        }
        2 => {
            let closing: Closing =
                serde_json::from_value(value).map_err(|_| "bad_payload")?;
            if !closing.closing {
                return Err("bad_payload");
            }
            Ok(Some(Payload::Closing(closing)))
        }
        3 => {
            let holds: Holds =
                serde_json::from_value(value).map_err(|_| "bad_payload")?;
            Ok(Some(Payload::Holds(holds)))
        }
        _ => Ok(Some(Payload::Content)),
    }
}

/// The value rules step 8 reads that `serde` cannot express: a key that is not the
/// canonical encoding and a role this version does not define are `bad_payload`.
fn state_is_well_formed(state: &RoomState) -> bool {
    state.peers.iter().all(|(name, entry)| {
        PublicKey::parse(name).is_some() && is_seated_role(&entry.role)
    })
}

/// The three roles a state may assign (`CANONICAL.md` §6.1).
fn is_seated_role(role: &str) -> bool {
    matches!(role, "host" | "guest" | "viewer")
}

/// A session-key announcement, or `None` when it is not step 8's object.
fn parse_announcement(plaintext: &[u8]) -> Option<Announcement> {
    let value: Value = serde_json::from_slice(plaintext).ok()?;
    let announcement: Announcement = serde_json::from_value(value).ok()?;
    // `role` is a declaration of `guest` or `viewer`, and never `host`.
    match announcement.role.as_deref() {
        None | Some("guest" | "viewer") => Some(announcement),
        Some(_) => None,
    }
}

/// The `issued` a state or a closing is ordered by, or `None` for any other payload.
const fn payload_issued(payload: Option<&Payload>) -> Option<u64> {
    match payload {
        Some(Payload::RoomState(state)) => Some(state.issued),
        Some(Payload::Closing(closing)) => Some(closing.issued),
        _ => None,
    }
}

/// Document content is a `kind = 0` plaintext carrying a `SyncStep2` or an `Update`
/// (`PROTOCOL.md` §13.5): a sync message of sub-type 1 or 2, **anywhere** in the stream.
///
/// The whole stream, and not its first message: this check is what refuses a `viewer`'s
/// edits, and the receiver that applies a frame reads every message it holds, so a frame
/// whose first message is a `SyncStep1` and whose second is an `Update` would otherwise pass
/// and have its content applied. The walk is that receiver's own decoder — `yrs`'s message
/// reader, which is what the session applies a `kind = 0` plaintext with — so the two cannot
/// disagree about where one message ends and the next begins. A hand-rolled walk does not
/// keep that agreement: `yrs` reads `Auth` as a status varint and a reason only when the
/// status is `PERMISSION_DENIED`, where a length-prefixed-buffer read loses alignment and can
/// hide an `Update` behind an auth message.
fn is_content(plaintext: &[u8]) -> bool {
    let mut decoder = DecoderV1::from(plaintext);
    let mut content = false;
    for message in MessageReader::new(&mut decoder) {
        match message {
            Ok(YMessage::Sync(
                YSyncMessage::SyncStep2(_) | YSyncMessage::Update(_),
            )) => {
                content = true;
            }
            Ok(_) => {}
            // The applier stops at a stream this decoder cannot read, and so does the
            // walk; what it has already seen is what the frame carries.
            Err(_) => break,
        }
    }
    content
}

/// The key a `peers` member or an announcement's `key` must be, or `None`.
///
/// RFC 4648 §3.5: over 32 bytes the encoding is 43 characters, the last of which carries
/// four bits of the value and two that are zero. A spelling that is not that one spells no
/// 32-byte value, so it is refused rather than resolved.
fn decode_key(text: &str) -> Option<[u8; 32]> {
    if text.len() != 43 {
        return None;
    }
    let last = text.as_bytes().last().copied()?;
    if !matches!(last, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_') {
        return None;
    }
    if !b"AEIMQUYcgkosw048".contains(&last) {
        return None;
    }
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    let mut out = [0u8; 32];
    let mut written = 0usize;
    for byte in text.bytes() {
        let value = base64_value(byte)?;
        acc = acc.checked_shl(6).unwrap_or(0) | u32::from(value);
        bits = bits.saturating_add(6);
        if bits >= 8 {
            bits = bits.saturating_sub(8);
            written = write_byte(&mut out, written, acc, bits);
        }
    }
    (written == 32).then_some(out)
}

/// Writes the byte the six-bit accumulator now holds, returning the next offset.
#[expect(
    clippy::too_many_arguments,
    reason = "the four arguments are the accumulator, its bit count, the output and the offset"
)]
fn write_byte(
    out: &mut [u8; 32],
    written: usize,
    acc: u32,
    bits: u32,
) -> usize {
    let decoded =
        u8::try_from(acc.checked_shr(bits).unwrap_or(0) & 0xff).unwrap_or(0);
    if let Some(slot) = out.get_mut(written) {
        *slot = decoded;
    }
    written.saturating_add(1)
}

/// A key's canonical base64url spelling, unpadded.
#[must_use]
pub fn encode_key(raw: &[u8; 32]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in raw.chunks(3) {
        out.push_str(&encode_chunk(chunk, ALPHABET));
    }
    out.truncate(raw.len().saturating_mul(8).div_ceil(6));
    out
}

/// The four base64url characters one three-byte group spells.
fn encode_chunk(chunk: &[u8], alphabet: &[u8; 64]) -> String {
    let b0 = u32::from(chunk.first().copied().unwrap_or(0));
    let b1 = u32::from(chunk.get(1).copied().unwrap_or(0));
    let b2 = u32::from(chunk.get(2).copied().unwrap_or(0));
    let triple = (b0 << 16) | (b1 << 8) | b2;
    [18u32, 12, 6, 0]
        .iter()
        .filter_map(|shift| {
            let index = usize::try_from((triple >> shift) & 0x3f).unwrap_or(0);
            alphabet.get(index).map(|byte| char::from(*byte))
        })
        .collect()
}

const fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte.saturating_sub(b'A')),
        b'a'..=b'z' => Some(byte.saturating_sub(b'a').saturating_add(26)),
        b'0'..=b'9' => Some(byte.saturating_sub(b'0').saturating_add(52)),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

/// §13.3's and §13.7's bound on one path, in the bytes a listing carries it as.
pub const MAX_PATH_BYTES: usize = 4096;

/// A path `PROTOCOL.md` §5 refuses, or one over §13.3's 4096-byte bound, is dropped at the
/// receiver, never refused: §13.3 and §13.7 leave it out of a listing and a hold set and
/// apply the rest.
#[must_use]
pub fn usable_path(path: &str) -> bool {
    !path.is_empty()
        && !path.chars().any(char::is_control)
        && path.len() <= MAX_PATH_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room_key() -> RoomKey {
        let mut raw = [0u8; 32];
        for (index, slot) in raw.iter_mut().enumerate() {
            *slot = u8::try_from(index).unwrap();
        }
        RoomKey(raw)
    }

    #[test]
    fn a_key_round_trips_through_its_canonical_encoding() {
        let raw = [
            0x9b, 0x04, 0xa4, 0xc5, 0xe4, 0x72, 0xb4, 0x55, 0x2c, 0xd1, 0x77,
            0x1a, 0x06, 0xe8, 0xe4, 0x03, 0x1b, 0x9a, 0x98, 0x59, 0x9f, 0x8d,
            0xbf, 0x60, 0x00, 0x6d, 0x55, 0xd5, 0xa6, 0x14, 0x90, 0x4c,
        ];
        assert_eq!(
            encode_key(&raw),
            "mwSkxeRytFUs0XcaBujkAxuamFmfjb9gAG1V1aYUkEw"
        );
        assert_eq!(PublicKey::parse(&encode_key(&raw)), Some(PublicKey(raw)));
    }

    #[test]
    fn a_key_with_non_zero_pad_bits_is_refused() {
        // 43 characters, the last carrying non-zero pad bits: no 32-byte value.
        let raw = [0u8; 32];
        let mut altered = encode_key(&raw);
        assert_eq!(altered.len(), 43);
        altered.pop();
        altered.push('B');
        assert_eq!(PublicKey::parse(&altered), None);
        assert_eq!(PublicKey::parse("short"), None);
    }

    #[test]
    fn the_frame_key_is_the_schedules() {
        // HKDF-SHA256(ikm = room key, salt = room id, info = "selvage/2 frame", L = 32):
        // the derivation is a function of both halves, so two room ids differ.
        let key = room_key();
        let a = key.frame_key("R7f3a2c19");
        let b = key.frame_key("R7f3a2c20");
        assert_ne!(a.0, b.0);
    }

    #[test]
    fn a_sealed_frame_opens_and_verifies() {
        let key = room_key();
        let frame_key = key.frame_key("R7f3a2c19");
        let signer = SessionKey::from_seed([7u8; 32]);
        let envelope = seal(
            &Recipe {
                room_id: "R7f3a2c19",
                frame_key: &frame_key,
                kind: 3,
                epoch: 0,
                counter: 1,
                nonce: [3u8; 12],
                signer: &signer,
            },
            br#"{"holds":["src/main.rs"]}"#,
        )
        .unwrap();
        let raw = envelope.bytes();
        let parsed = Envelope::parse(&raw).unwrap();
        assert_eq!(parsed.kind, 3);
        assert_eq!(parsed.counter, 1);
        let plaintext = opens(&frame_key, "R7f3a2c19", &parsed).unwrap();
        assert_eq!(plaintext, br#"{"holds":["src/main.rs"]}"#);
        let aad = associated_data("R7f3a2c19", 3, 0, parsed.key_id);
        assert!(
            signer
                .public()
                .verifies(&signing_input(&aad, &parsed), &parsed.signature)
        );
    }

    #[test]
    fn a_corrupted_tag_is_a_signature_failure() {
        let key = room_key();
        let frame_key = key.frame_key("R7f3a2c19");
        let signer = SessionKey::from_seed([7u8; 32]);
        let mut envelope = seal(
            &Recipe {
                room_id: "R7f3a2c19",
                frame_key: &frame_key,
                kind: 3,
                epoch: 0,
                counter: 1,
                nonce: [3u8; 12],
                signer: &signer,
            },
            br#"{"holds":["a"]}"#,
        )
        .unwrap();
        // One bit inside the GCM tag, which is the ciphertext's last byte.
        let last = envelope.ciphertext.len().saturating_sub(1);
        if let Some(byte) = envelope.ciphertext.get_mut(last) {
            *byte ^= 0x01;
        }
        let aad = associated_data("R7f3a2c19", 3, 0, envelope.key_id);
        assert!(
            !signer
                .public()
                .verifies(&signing_input(&aad, &envelope), &envelope.signature)
        );
    }

    #[test]
    fn left_over_bytes_are_not_an_envelope() {
        let raw = vec![0u8; 8 + 1 + 1 + 1 + 12 + 1 + 16 + 64 + 1];
        assert!(Envelope::parse(&raw).is_err());
        assert!(Envelope::parse(&raw[..10]).is_err());
    }

    #[test]
    fn a_varuint_is_leb128() {
        assert_eq!(varuint(0), vec![0]);
        assert_eq!(varuint(127), vec![127]);
        assert_eq!(varuint(128), vec![0x80, 0x01]);
        assert_eq!(varuint(300), vec![0xac, 0x02]);
        let (value, at) = read_varuint(&[0xac, 0x02, 0xff], 0).unwrap();
        assert_eq!(value, 300);
        assert_eq!(at, 2);
    }

    /// n2: `PROTOCOL.md` §13.3's `MAY`. A peer that holds the room key can announce keys
    /// without bound, and a mark per key is state a receiver carries for the life of the room.
    #[test]
    fn the_marks_kept_for_keys_no_state_commits_are_capped() {
        let key = room_key();
        let frame_key = key.frame_key("R7f3a2c19");
        let mut reader = Reader::new("R7f3a2c19", key, PublicKey([1u8; 32]));
        let mut announced = Vec::new();
        for index in 0..(UNCOMMITTED_MARKS + 8) {
            let seed = u8::try_from(index).unwrap().saturating_add(1);
            let signer = SessionKey::from_seed([seed; 32]);
            let public = signer.public();
            let frame = seal(
                &Recipe {
                    room_id: "R7f3a2c19",
                    frame_key: &frame_key,
                    kind: 4,
                    epoch: 0,
                    counter: 1,
                    nonce: [seed; 12],
                    signer: &signer,
                },
                format!("{{\"key\":\"{}\"}}", public.encode()).as_bytes(),
            )
            .unwrap()
            .bytes();
            let verdict = reader.read(&frame);
            assert!(verdict.ok, "an announcement verifies on its own");
            announced.push((public.id(), frame));
        }
        assert_eq!(
            reader.marks.len(),
            UNCOMMITTED_MARKS,
            "the cap holds where no state commits anything"
        );

        // The oldest is what the cap gave up, and the mark it gave up is the one its
        // announcement is read against: the same bytes are accepted again.
        let (oldest_id, oldest_frame) =
            announced.first().expect("an announcement was made");
        assert!(!reader.marks.contains_key(oldest_id));
        assert_eq!(
            reader.read(oldest_frame).reason,
            None,
            "the evicted mark is the cost §13.3 states"
        );

        // The newest keep theirs, and a replay of one is still refused.
        let (newest_id, newest_frame) =
            announced.last().expect("an announcement was made");
        assert!(reader.marks.contains_key(newest_id));
        assert_eq!(
            reader.read(newest_frame).reason.as_deref(),
            Some("replayed_counter")
        );
    }

    #[test]
    fn a_holds_message_applies_and_a_replay_is_refused() {
        let key = room_key();
        let frame_key = key.frame_key("R7f3a2c19");
        let signer = SessionKey::from_seed([9u8; 32]);
        let public = signer.public();
        let mut reader = Reader::new("R7f3a2c19", key, PublicKey([1u8; 32]));
        reader.committed.insert(
            public.encode(),
            Committed {
                key: public,
                role: "guest".to_string(),
                peer_id: "p-1".to_string(),
            },
        );
        let recipe = Recipe {
            room_id: "R7f3a2c19",
            frame_key: &frame_key,
            kind: 3,
            epoch: 0,
            counter: 1,
            nonce: [4u8; 12],
            signer: &signer,
        };
        let frame = seal(&recipe, br#"{"holds":["src/main.rs"]}"#)
            .unwrap()
            .bytes();
        let verdict = reader.read(&frame);
        assert!(verdict.ok, "{:?}", verdict.reason);
        assert_eq!(reader.holds[&public.id()], vec!["src/main.rs".to_string()]);

        // The same bytes again: the counter is at the mark, so it is a replay.
        let replay = reader.read(&frame);
        assert_eq!(replay.reason.as_deref(), Some("replayed_counter"));
    }

    #[test]
    fn a_viewers_content_is_refused() {
        let key = room_key();
        let frame_key = key.frame_key("R7f3a2c19");
        let viewer = SessionKey::from_seed([11u8; 32]);
        let public = viewer.public();
        let mut reader = Reader::new("R7f3a2c19", key, PublicKey([1u8; 32]));
        reader.committed.insert(
            public.encode(),
            Committed {
                key: public,
                role: "viewer".to_string(),
                peer_id: "p-1".to_string(),
            },
        );
        // message type 0 (sync), sub-type 1 (SyncStep2), then a zero length payload.
        let plaintext = [0u8, 1, 0];
        let frame = seal(
            &Recipe {
                room_id: "R7f3a2c19",
                frame_key: &frame_key,
                kind: 0,
                epoch: 0,
                counter: 1,
                nonce: [6u8; 12],
                signer: &viewer,
            },
            &plaintext,
        )
        .unwrap()
        .bytes();
        let verdict = reader.read(&frame);
        assert_eq!(verdict.reason.as_deref(), Some("unauthorised_content"));
    }

    #[test]
    fn a_viewers_content_behind_a_leading_request_is_refused() {
        let key = room_key();
        let frame_key = key.frame_key("R7f3a2c19");
        let viewer = SessionKey::from_seed([11u8; 32]);
        let public = viewer.public();
        let mut reader = Reader::new("R7f3a2c19", key, PublicKey([1u8; 32]));
        reader.committed.insert(
            public.encode(),
            Committed {
                key: public,
                role: "viewer".to_string(),
                peer_id: "p-1".to_string(),
            },
        );
        // A SyncStep1 first (type 0, sub-type 0, a one-byte empty state vector) and an Update
        // second (type 0, sub-type 2, an empty update): the content is not the frame's first
        // message, and §6.1's step 10 reads the whole stream.
        let plaintext = [0u8, 0, 1, 0, 0, 2, 0];
        let frame = seal(
            &Recipe {
                room_id: "R7f3a2c19",
                frame_key: &frame_key,
                kind: 0,
                epoch: 0,
                counter: 1,
                nonce: [6u8; 12],
                signer: &viewer,
            },
            &plaintext,
        )
        .unwrap()
        .bytes();
        let verdict = reader.read(&frame);
        assert_eq!(verdict.reason.as_deref(), Some("unauthorised_content"));
    }

    #[test]
    fn a_viewers_content_behind_an_auth_message_is_refused() {
        let key = room_key();
        let frame_key = key.frame_key("R7f3a2c19");
        let viewer = SessionKey::from_seed([11u8; 32]);
        let public = viewer.public();
        let mut reader = Reader::new("R7f3a2c19", key, PublicKey([1u8; 32]));
        reader.committed.insert(
            public.encode(),
            Committed {
                key: public,
                role: "viewer".to_string(),
                peer_id: "p-1".to_string(),
            },
        );
        // An Auth message first (`yrs` reads it as a status varint, `PERMISSION_GRANTED`), then
        // a sync Update: a walk that reads Auth as a length-prefixed buffer loses alignment
        // and can miss the Update behind it.
        let plaintext = [2u8, 1, 0, 2, 0];
        let frame = seal(
            &Recipe {
                room_id: "R7f3a2c19",
                frame_key: &frame_key,
                kind: 0,
                epoch: 0,
                counter: 1,
                nonce: [6u8; 12],
                signer: &viewer,
            },
            &plaintext,
        )
        .unwrap()
        .bytes();
        let verdict = reader.read(&frame);
        assert_eq!(verdict.reason.as_deref(), Some("unauthorised_content"));
    }

    #[test]
    fn a_path_over_the_bound_is_dropped_from_a_listing_and_a_hold_set() {
        let key = room_key();
        let frame_key = key.frame_key("R7f3a2c19");
        let host = SessionKey::from_seed([3u8; 32]);
        let guest = SessionKey::from_seed([9u8; 32]);
        let public = guest.public();
        let mut reader = Reader::new("R7f3a2c19", key, host.public());
        // 4096 bytes is kept and 4097 is dropped: the bound is over it, not at it.
        let at_bound = "b".repeat(MAX_PATH_BYTES);
        let over = "a".repeat(MAX_PATH_BYTES + 1);
        assert!(usable_path(&at_bound));
        assert!(!usable_path(&over));
        let peers: serde_json::Map<String, Value> = [(
            public.encode(),
            serde_json::json!({"peer_id": "p-1", "role": "guest"}),
        )]
        .into_iter()
        .collect();
        let listing = serde_json::json!({
            "issued": 1,
            "listing": [at_bound.as_str(), over.as_str()],
            "peers": peers,
        })
        .to_string();
        let frame = seal(
            &Recipe {
                room_id: "R7f3a2c19",
                frame_key: &frame_key,
                kind: 1,
                epoch: 0,
                counter: 1,
                nonce: [5u8; 12],
                signer: &host,
            },
            listing.as_bytes(),
        )
        .unwrap()
        .bytes();
        let verdict = reader.read(&frame);
        assert!(verdict.ok, "{:?}", verdict.reason);
        assert_eq!(reader.listing, vec![at_bound.clone()]);

        let holds =
            serde_json::json!({"holds": [at_bound.as_str(), over.as_str()]})
                .to_string();
        let frame = seal(
            &Recipe {
                room_id: "R7f3a2c19",
                frame_key: &frame_key,
                kind: 3,
                epoch: 0,
                counter: 1,
                nonce: [6u8; 12],
                signer: &guest,
            },
            holds.as_bytes(),
        )
        .unwrap()
        .bytes();
        let verdict = reader.read(&frame);
        assert!(verdict.ok, "{:?}", verdict.reason);
        assert_eq!(reader.holds[&public.id()], vec![at_bound]);
    }
}
