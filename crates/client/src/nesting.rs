//! A yjs update decoded with a bound on how deeply an `Any` value nests.
//!
//! `yrs` reads an `Any` array or map by recursing into it, with no bound of its own, and an
//! array costs two bytes a level: one frame can nest deeper than any thread's stack holds,
//! and decoding it ends the process. Every update this client applies is decoded here, and
//! the reader refuses a `kind = 0` frame carrying one that nests past [`MAX_ANY_DEPTH`]
//! before anything is applied (`crate::sealed`'s step 8).

use std::sync::Arc;

use yrs::block::ClientID;
use yrs::encoding::read::{Error, Read};
use yrs::sync::{Message as YMessage, MessageReader, SyncMessage};
use yrs::updates::decoder::{Decode, Decoder, DecoderV1};
use yrs::{Any, ID, Update};

/// The most `Any` arrays and maps one value may open inside one another in an update this
/// client decodes.
///
/// This is the implementation's limit, not the protocol's: `PROTOCOL.md` §7 sets none
/// (`NOTES.md` §B.49 in the specification). A Selvage document is a `Y.Text`, whose updates
/// carry no `Any` value at all, so nothing a conforming peer writes comes near it, and it is
/// far below the depth at which `yrs`'s recursive reader exhausts a thread's stack.
pub const MAX_ANY_DEPTH: usize = 256;

/// Why an update was not decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unread {
    /// An `Any` in it nests past [`MAX_ANY_DEPTH`].
    TooDeep,
    /// It is not an update `yrs` reads.
    Malformed,
}

/// One update in yjs's V1 format, decoded by `yrs` with every `Any` it holds checked against
/// [`MAX_ANY_DEPTH`] before `yrs` reads it.
///
/// # Errors
///
/// [`Unread::TooDeep`] when an `Any` nests past the bound, and [`Unread::Malformed`] when
/// `yrs` refuses the bytes for any other reason.
pub fn decode_update(bytes: &[u8]) -> Result<Update, Unread> {
    let mut decoder = Bounded {
        inner: DecoderV1::from(bytes),
        too_deep: false,
    };
    Update::decode(&mut decoder).map_err(|_| {
        if decoder.too_deep {
            Unread::TooDeep
        } else {
            Unread::Malformed
        }
    })
}

/// Whether a `kind = 0` plaintext carries an update that nests an `Any` past the bound.
///
/// The walk is `yrs`'s message reader, the one the session applies the stream with, so the
/// two agree on where each message ends; it stops where that reader can read no further.
#[must_use]
pub fn nests_too_deep(plaintext: &[u8]) -> bool {
    let mut decoder = DecoderV1::from(plaintext);
    for message in MessageReader::new(&mut decoder) {
        let update = match message {
            Ok(YMessage::Sync(
                SyncMessage::SyncStep2(update) | SyncMessage::Update(update),
            )) => update,
            Ok(YMessage::Custom(..)) | Err(_) => return false,
            Ok(_) => continue,
        };
        if decode_update(&update) == Err(Unread::TooDeep) {
            return true;
        }
    }
    false
}

/// `yrs`'s own V1 decoder, with a depth check in front of every `Any` it reads.
///
/// Everything else is the inner decoder's, so the update's structure is read by `yrs` alone
/// and the check sees exactly the bytes `yrs` would hand its recursive reader next.
struct Bounded<'a> {
    inner: DecoderV1<'a>,
    too_deep: bool,
}

impl Read for Bounded<'_> {
    fn read_exact(&mut self, len: usize) -> Result<&[u8], Error> {
        self.inner.read_exact(len)
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        self.inner.read_u8()
    }
}

impl Decoder for Bounded<'_> {
    fn reset_ds_cur_val(&mut self) {
        self.inner.reset_ds_cur_val();
    }

    fn read_ds_clock(&mut self) -> Result<u32, Error> {
        self.inner.read_ds_clock()
    }

    fn read_ds_len(&mut self) -> Result<u32, Error> {
        self.inner.read_ds_len()
    }

    fn read_left_id(&mut self) -> Result<ID, Error> {
        self.inner.read_left_id()
    }

    fn read_right_id(&mut self) -> Result<ID, Error> {
        self.inner.read_right_id()
    }

    fn read_client(&mut self) -> Result<ClientID, Error> {
        self.inner.read_client()
    }

    fn read_info(&mut self) -> Result<u8, Error> {
        self.inner.read_info()
    }

    fn read_parent_info(&mut self) -> Result<bool, Error> {
        self.inner.read_parent_info()
    }

    fn read_type_ref(&mut self) -> Result<u8, Error> {
        self.inner.read_type_ref()
    }

    fn read_len(&mut self) -> Result<u32, Error> {
        self.inner.read_len()
    }

    /// The check, then `yrs`'s read. The V1 decoder's `read_to_end` hands back the bytes
    /// after its cursor without moving it, so the check reads a copy of what follows.
    fn read_any(&mut self) -> Result<Any, Error> {
        if deeper_than_bound(self.inner.read_to_end()?) {
            self.too_deep = true;
            return Err(Error::UnexpectedValue);
        }
        self.inner.read_any()
    }

    fn read_json(&mut self) -> Result<Any, Error> {
        self.inner.read_json()
    }

    fn read_key(&mut self) -> Result<Arc<str>, Error> {
        self.inner.read_key()
    }

    fn read_to_end(&mut self) -> Result<&[u8], Error> {
        self.inner.read_to_end()
    }
}

/// Whether the `Any` at the front of `bytes` opens more than [`MAX_ANY_DEPTH`] arrays and
/// maps inside one another.
///
/// It reads the value the way `Any::decode` does, with the same primitive reads, but keeps the
/// open containers in a list rather than on the stack. Bytes it cannot read are not "too
/// deep": `yrs` refuses them at the same place on its own.
fn deeper_than_bound(bytes: &[u8]) -> bool {
    let mut decoder = DecoderV1::from(bytes);
    // The containers still open, innermost last: how many values each has left, and whether
    // it is a map, whose every value follows its key.
    let mut open: Vec<(usize, bool)> = Vec::new();
    loop {
        match read_one(&mut decoder, &mut open) {
            Ok(Step::Deeper) => return true,
            Ok(Step::Read) => {}
            Err(_) => return false,
        }
        match next_value(&mut decoder, &mut open) {
            Ok(true) => {}
            Ok(false) | Err(_) => return false,
        }
    }
}

/// What reading one value did.
enum Step {
    /// A scalar was read, or a container opened within the bound.
    Read,
    /// A container would open past the bound.
    Deeper,
}

/// Reads one value's tag and, for a scalar, the value; a container is opened instead.
fn read_one(
    decoder: &mut DecoderV1<'_>,
    open: &mut Vec<(usize, bool)>,
) -> Result<Step, Error> {
    match decoder.read_u8()? {
        120 | 121 | 126 | 127 => {}
        125 => {
            let _: i64 = decoder.read_var()?;
        }
        124 => {
            let _ = decoder.read_f32()?;
        }
        123 => {
            let _ = decoder.read_f64()?;
        }
        122 => {
            let _ = decoder.read_i64()?;
        }
        119 => {
            let _ = decoder.read_string()?;
        }
        116 => {
            let _ = decoder.read_buf()?;
        }
        tag @ (117 | 118) => {
            if open.len() >= MAX_ANY_DEPTH {
                return Ok(Step::Deeper);
            }
            let len: usize = decoder.read_var()?;
            open.push((len, tag == 118));
        }
        _ => return Err(Error::UnexpectedValue),
    }
    Ok(Step::Read)
}

/// Closes every container that has no value left and positions the decoder on the next value,
/// past a map's key. Returns `false` once the outermost value is complete.
fn next_value(
    decoder: &mut DecoderV1<'_>,
    open: &mut Vec<(usize, bool)>,
) -> Result<bool, Error> {
    while let Some((left, map)) = open.last_mut() {
        if *left == 0 {
            let _ = open.pop();
            continue;
        }
        *left = left.saturating_sub(1);
        if *map {
            let _ = decoder.read_string()?;
        }
        return Ok(true);
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use yrs::{Array, ArrayRef, Doc, Out, ReadTxn, Transact};

    /// An `Any` that opens `depth` one-element arrays inside one another around a `null`.
    fn nested(depth: usize) -> Any {
        let mut value = Any::Null;
        for _ in 0..depth {
            value = Any::Array(Arc::from(vec![value]));
        }
        value
    }

    /// The update a real `yrs` document writes when `value` is pushed onto an array.
    fn written(value: Any) -> Vec<u8> {
        let doc = Doc::new();
        let array = doc.get_or_insert_array("values");
        {
            let mut txn = doc.transact_mut();
            array.push_back(&mut txn, value);
        }
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    }

    #[test]
    fn a_value_at_the_bound_decodes_and_applies_as_written() {
        let value = nested(MAX_ANY_DEPTH);
        let update = decode_update(&written(value.clone())).unwrap();
        let doc = Doc::new();
        let array: ArrayRef = doc.get_or_insert_array("values");
        doc.transact_mut().apply_update(update).unwrap();
        let txn = doc.transact();
        let Some(Out::Any(read)) = array.get(&txn, 0) else {
            panic!("the array holds the value");
        };
        assert_eq!(read, value);
    }

    #[test]
    fn a_value_one_past_the_bound_is_too_deep() {
        let bytes = written(nested(MAX_ANY_DEPTH + 1));
        assert_eq!(decode_update(&bytes).err(), Some(Unread::TooDeep));
    }

    #[test]
    fn a_map_counts_as_a_level_and_its_keys_are_read() {
        let mut value = Any::Null;
        for _ in 0..=MAX_ANY_DEPTH {
            value = Any::from(HashMap::from([("k".to_string(), value)]));
        }
        assert_eq!(decode_update(&written(value)).err(), Some(Unread::TooDeep));
    }

    /// An array of one value of every scalar tag, then `inner`: the check reads past each
    /// scalar to reach it, so a misread scalar would hide how deep `inner` goes.
    fn behind_every_scalar(inner: Any) -> Any {
        Any::Array(Arc::from(vec![
            Any::Undefined,
            Any::Null,
            Any::Bool(true),
            Any::Bool(false),
            Any::Number(-7.0),
            Any::Number(1.5),
            Any::Number(0.1),
            Any::BigInt(1 << 40),
            Any::String(Arc::from("uuuuvvvv")),
            Any::Buffer(Arc::from(vec![117u8, 117, 118, 118])),
            Any::from(HashMap::from([
                ("a".to_string(), Any::Bool(true)),
                ("b".to_string(), Any::String(Arc::from("u"))),
            ])),
            inner,
        ]))
    }

    #[test]
    fn scalars_of_every_tag_read_in_line_with_yrs() {
        let within = behind_every_scalar(nested(MAX_ANY_DEPTH - 1));
        assert!(decode_update(&written(within)).is_ok());
        let past = behind_every_scalar(nested(MAX_ANY_DEPTH));
        assert_eq!(decode_update(&written(past)).err(), Some(Unread::TooDeep));
    }

    #[test]
    fn a_subdocuments_options_are_held_to_the_bound_too() {
        // A `Doc` item (info 9) under the root `x`: its guid, then its options as an `Any`.
        let mut update = vec![1, 1, 1, 0, 9, 1, 1, b'x', 1, b'g'];
        for _ in 0..=MAX_ANY_DEPTH {
            update.extend([117, 1]);
        }
        update.extend([126, 0]);
        assert_eq!(decode_update(&update).err(), Some(Unread::TooDeep));
    }

    #[test]
    fn bytes_yrs_cannot_read_are_malformed_rather_than_too_deep() {
        assert_eq!(decode_update(&[1, 1, 1]).err(), Some(Unread::Malformed));
    }
}
