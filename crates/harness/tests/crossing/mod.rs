//! One document, and the anchor each library publishes for the same caret in it.
//!
//! `spec/vectors/anchors/relative-position.json` is the artifact and its `notes` member is
//! where it is explained. What matters here is that each suite rebuilds *its own* half of it
//! from the real library — this one from `yrs`, the TypeScript suite from `yjs` — so a fixture
//! that has drifted from either library fails a test instead of quietly ceasing to mean
//! anything, and then consumes the other half as bytes from the other side.

use std::env;
use std::error::Error as StdError;
use std::fs;
use std::path::{Path, PathBuf};
use std::str;

use serde::Deserialize;
use serde_json::Value;

/// Anything reading the fixture can fail with.
type Failure = Box<dyn StdError>;

/// The whole artifact.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Crossing {
    #[expect(
        dead_code,
        reason = "the loader must accept the members; the prose explains the artifact to a reader"
    )]
    pub id: String,
    #[expect(dead_code, reason = "the loader must accept the members")]
    pub title: String,
    #[expect(dead_code, reason = "the loader must accept the members")]
    pub notes: String,
    pub path: String,
    pub document: Document,
    pub yjs: Published,
    pub yrs: Published,
}

/// The document both anchors are taken from, as the library that wrote it encoded it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Document {
    /// The client id its writer was given, fixed so that its bytes are a constant.
    pub client: u64,
    pub text: String,
    /// The whole document as a v1 update, hex: what a peer sends.
    pub update: String,
}

/// One library's anchor for the caret, and the offset it denotes.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Published {
    pub anchor: Value,
    pub offset: u32,
}

impl Crossing {
    /// The document's bytes, as the writing library encoded them.
    pub fn update(&self) -> Result<Vec<u8>, Failure> {
        hex_bytes(&self.document.update)
    }
}

/// Reads the fixture.
pub fn load() -> Result<Crossing, Failure> {
    let path = root().join("anchors/relative-position.json");
    let bytes =
        fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(serde_json::from_str(&bytes)?)
}

/// Where the anchors fixture lives: beside the spec, which the sandbox is handed separately.
fn root() -> PathBuf {
    // `impl/crates/harness` -> the repository's `spec/vectors`.
    env::var_os("SELVAGE_VECTORS").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../spec/vectors"),
        PathBuf::from,
    )
}

fn hex_bytes(hex: &str) -> Result<Vec<u8>, Failure> {
    if !hex.len().is_multiple_of(2) {
        return Err(format!("`{hex}` is not a whole number of bytes").into());
    }
    hex.as_bytes()
        .chunks(2)
        .map(|pair| {
            let digits = str::from_utf8(pair)?;
            u8::from_str_radix(digits, 16)
                .map_err(|e| format!("`{digits}`: {e}").into())
        })
        .collect()
}
