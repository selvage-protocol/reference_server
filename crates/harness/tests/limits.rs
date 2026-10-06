//! The numeric bounds this workspace carries, compared with the specification's published
//! `schema/limits.json`.
//!
//! The bounds have one home: `specification/schema/limits.json` names each number, its unit
//! and the section that owns it. A constant here is a copy of one, and a copy drifts
//! silently — the number still looks right in the code it sits in. This test reads the
//! vendored copy (`scripts/sync-limits.sh` refreshes it) and fails when a constant and its
//! entry disagree.
//!
//! The file is vendored inside the Cargo workspace and handed in as `SELVAGE_LIMITS`, because
//! the Nix sandbox receives only the workspace and a test that reads outside it fails there
//! (`ai_notes/AGENTS.md` §2). A missing file or a missing entry is a failure, not a skip: a pin
//! that reaches nothing reports a clean tree (§4).

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use selvage_client::{host, sealed};
use selvage_protocol as proto;
use selvage_protocol::close;
use selvaged::{ServerConfig, budget, room};
use serde::Deserialize;

/// The scale of a `MiB` entry, in bytes.
const MIB: u64 = 1024 * 1024;

/// One published bound.
#[derive(Debug, Deserialize)]
struct Entry {
    name: String,
    value: u64,
    /// The unit the specification writes, so a comparison that fails says in which.
    unit: String,
}

/// The published file.
#[derive(Debug, Deserialize)]
struct Catalog {
    limits: Vec<Entry>,
}

/// The published bounds by name: each one's value and the unit it is written in.
type Bounds = BTreeMap<String, (u64, String)>;

/// The path of the vendored copy: `SELVAGE_LIMITS` when the build supplies it, otherwise this
/// crate's `tests/limits.json`.
fn path() -> PathBuf {
    env::var_os("SELVAGE_LIMITS").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/limits.json"),
        PathBuf::from,
    )
}

/// The published bounds by name.
///
/// A file that is missing, unreadable, empty, malformed or names a bound twice is an error
/// rather than a smaller run: a pin that reads nothing would otherwise report a clean tree
/// (`ai_notes/AGENTS.md` §4). The caller turns it into the test's failure.
fn published() -> Result<Bounds, String> {
    let path = path();
    let text = fs::read_to_string(&path).map_err(|error| {
        format!(
            "the published bounds are unreadable at {}: {error}",
            path.display()
        )
    })?;
    let catalog: Catalog = serde_json::from_str(&text).map_err(|error| {
        format!("{} is not a limits file: {error}", path.display())
    })?;
    if catalog.limits.is_empty() {
        return Err(format!("{} carries no bounds", path.display()));
    }
    let mut out = BTreeMap::new();
    for entry in catalog.limits {
        let previous =
            out.insert(entry.name.clone(), (entry.value, entry.unit));
        if previous.is_some() {
            return Err(format!(
                "{} names {} twice",
                path.display(),
                entry.name
            ));
        }
    }
    Ok(out)
}

/// A `usize` as the `u64` the published values are.
fn wide(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// A byte count in the `MiB` a bound publishes.
fn in_mib(bytes: usize) -> u64 {
    wide(bytes).checked_div(MIB).unwrap_or(u64::MAX)
}

/// A byte count in the `KiB` a bound publishes.
fn in_kib(bytes: usize) -> u64 {
    wide(bytes).checked_div(1024).unwrap_or(u64::MAX)
}

/// Every constant this workspace owns that a published bound names, in the unit that bound
/// publishes.
#[test]
fn the_workspace_bounds_are_the_published_ones() {
    let published = published().unwrap_or_else(|error| panic!("{error}"));
    let config = ServerConfig::default();

    let owned: [(&str, u64); 20] = [
        ("hello_timeout", config.hello_timeout.as_secs()),
        ("http_head_timeout", config.head_timeout.as_secs()),
        ("ws_ping_interval", config.ping_interval.as_secs()),
        ("connection_cap", wide(config.max_connections)),
        ("room_cap", wide(config.max_rooms)),
        ("peers_per_room", wide(config.max_peers_per_room)),
        ("outbound_queue_frames", wide(room::MAX_QUEUE_FRAMES)),
        ("outbound_queue_bytes", in_mib(config.max_queue_bytes)),
        ("inbound_text_envelope", in_mib(config.max_envelope_bytes)),
        ("inbound_rate", in_mib(config.inbound_bytes_per_sec)),
        ("inbound_burst", in_mib(config.inbound_burst_bytes)),
        (
            "inbound_frame_charge_floor",
            in_kib(budget::FRAME_COST_BYTES),
        ),
        (
            "inbound_small_frame_ceiling",
            wide(config.inbound_bytes_per_sec)
                .checked_div(wide(budget::FRAME_COST_BYTES))
                .unwrap_or(0),
        ),
        ("display_name_length", wide(proto::DISPLAY_NAME_MAX_UTF16)),
        ("close_code_min", u64::from(close::PROTOCOL_ERROR)),
        ("close_code_max", u64::from(close::ROOM_GONE)),
        ("nesting_depth", wide(proto::MAX_NESTING_DEPTH)),
        ("listing_path_bytes", wide(sealed::MAX_PATH_BYTES)),
        ("listing_paths", wide(host::MAX_LISTING_PATHS)),
        ("listing_path_bytes_total", in_mib(host::MAX_LISTING_BYTES)),
    ];

    let mut missing = Vec::new();
    let mut wrong = Vec::new();
    for (name, actual) in owned {
        match published.get(name) {
            None => missing.push(name),
            Some((value, _)) if *value == actual => {}
            Some((value, unit)) => wrong.push(format!(
                "{name}: the workspace carries {actual} {unit}, the specification publishes \
                 {value} {unit}"
            )),
        }
    }
    assert!(
        missing.is_empty(),
        "the published bounds carry no entry for {missing:?}"
    );
    assert!(
        wrong.is_empty(),
        "{} of {} bounds disagree with the specification:\n{}",
        wrong.len(),
        owned.len(),
        wrong.join("\n")
    );
}
