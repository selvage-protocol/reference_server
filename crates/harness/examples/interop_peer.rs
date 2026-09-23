//! A Selvage client another toolchain can drive, so a second implementation is put in
//! one room with this one and compared against it rather than against a mock.
//!
//! `cargo run -p selvage-harness --example interop_peer -- \
//!      --invite <session-url> --path <document-path> [--name <display-name>] [--version 1|2]`
//!
//! It joins through the invite URL it is given — the link a host shares — opens the
//! document and reports what its own replica holds. Commands arrive as one JSON object
//! per line on stdin and every reply is one JSON object per line on stdout, so the
//! caller drives the editing and does the waiting: this side never sleeps, and it
//! speaks first to nobody. A link this client cannot join with is an error, not a
//! fallback.
//!
//! **Which wire version it speaks is the link's decision.** `selvage/2`'s invite carries the
//! room key and the host key in its fragment (`PROTOCOL.md` §5.1), so a link that has one is
//! joined with the sealed relay and a link without one is joined with the version-1 engine. The
//! `--version` flag overrides that reading and never overrules it silently: `--version 2` on a
//! link with no fragment is an error, because there are no keys to read a room with, and
//! `--version 1` on a link that has one is an error too, because dropping those two keys is the
//! defect this reading exists to avoid.
//!
//! **The vocabulary is one vocabulary.** `insert`, `select`, `report` and `quit` are the same
//! commands, and `text`, `ok`, `report` and `unsupported` the same events, whichever version
//! the link chose; what differs is what a version can answer, and the differences are in the
//! reply rather than in the shape of it:
//!
//! - `session.role` is the role the handshake gave the connection in `selvage/1` and the role
//!   the applied room state gives this connection's own key in `selvage/2` — §13.4's rule, with
//!   the same `guest` fallback the shared engine uses for a key no state names yet.
//! - `session.documents` is the room's open-document set in `selvage/1` and §13.7's union of the
//!   live holds in `selvage/2`.
//! - `presence` is empty in `selvage/2`: this client's session applies no awareness and
//!   publishes none yet, so `select` is answered `unsupported` rather than `ok`.
//! - `insert`'s reply carries `published` in `selvage/2` — whether the frame went out, which is
//!   `false` until a state commits this connection's key (§13.1's step 4).
//!
//! `vscode_client/test/interop.test.ts` is the caller that matters: it hosts from the
//! TypeScript engine, spawns this as the guest, and asserts both sides converge on the
//! same text, the same room and the same state vectors.

use std::env;
use std::error::Error as StdError;
use std::io::{self, BufRead};

use serde_json::{Value, json};

use selvage_client::relay::{RelayJoinOptions, RelaySession};
use selvage_client::{
    ConnectOptions, PeerInfo, Presence, SelectionOffsets, SyncEngine,
};

/// Anything this driver can fail with.
type Failure = Box<dyn StdError>;

/// The wire version this driver speaks to the room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireVersion {
    One,
    Two,
}

/// The command line, and nothing the client itself does not need.
struct Options {
    invite: String,
    path: String,
    name: String,
    version: Option<WireVersion>,
}

fn options() -> Result<Options, Failure> {
    options_of(env::args().skip(1))
}

/// The command line, read from any sequence of arguments so that a test can hand it one.
fn options_of(args: impl Iterator<Item = String>) -> Result<Options, Failure> {
    let mut invite = None;
    let mut path = None;
    let mut name = None;
    let mut version = None;
    let mut args = args;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--invite" => invite = args.next(),
            "--path" => path = args.next(),
            "--name" => name = args.next(),
            "--version" => {
                // A flag with no value is an error like an unknown one: dropping it would
                // leave the version to the link and overrule it silently, which is the one
                // thing this reading is here to avoid.
                let value = args.next().ok_or(
                    "--version needs a value: `1` (or `selvage/1`) or `2` (or `selvage/2`)",
                )?;
                version = Some(parse_version(&value)?);
            }
            other => {
                return Err(format!("unknown argument {other:?}").into());
            }
        }
    }
    Ok(Options {
        invite: invite.ok_or("--invite <session-url> is required")?,
        path: path.ok_or("--path <document-path> is required")?,
        name: name.unwrap_or_else(|| "interop-peer".to_string()),
        version,
    })
}

fn parse_version(value: &str) -> Result<WireVersion, Failure> {
    match value {
        "1" | "selvage/1" => Ok(WireVersion::One),
        "2" | "selvage/2" => Ok(WireVersion::Two),
        other => Err(format!("unknown wire version {other:?}").into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let options = options()?;
    let peer = Peer::join(&options).await?;
    peer.open(&options.path).await?;
    serve(&peer, &options.path).await?;
    peer.disconnect().await?;
    Ok(())
}

/// The connected client, in whichever version the link named.
enum Peer {
    /// `selvage/2`: the sealed relay, whose frames a server reads nothing out of.
    Sealed(RelaySession),
    /// `selvage/1`: the sync engine, which is the server's peer.
    Plain(SyncEngine),
}

impl Peer {
    /// Reads the link, settles which wire version it names, and joins.
    async fn join(options: &Options) -> Result<Self, Failure> {
        let connect = ConnectOptions::read_invite_url(
            &options.invite,
            options.name.as_str(),
        )?;
        let sealed = connect.sealed_invite.is_some();
        let version = choose(options, sealed)?;
        match version {
            WireVersion::Two => Ok(Self::Sealed(
                RelaySession::join(RelayJoinOptions {
                    invite: options.invite.clone(),
                    display_name: options.name.clone(),
                    declared_role: None,
                    client: Some(format!(
                        "selvage-interop/{}",
                        env!("CARGO_PKG_VERSION")
                    )),
                    keepalive: None,
                })
                .await?,
            )),
            WireVersion::One => {
                Ok(Self::Plain(SyncEngine::connect(connect).await?))
            }
        }
    }

    async fn open(&self, path: &str) -> Result<(), Failure> {
        match self {
            Self::Sealed(relay) => Ok(relay.open(path)?),
            Self::Plain(engine) => Ok(engine.open(path).await?),
        }
    }

    /// One local insert, and the event this version reports it as.
    #[expect(
        clippy::too_many_arguments,
        reason = "the path, the offset and the text are the three members of one edit"
    )]
    async fn insert(
        &self,
        path: &str,
        index: u32,
        text: &str,
    ) -> Result<Value, Failure> {
        match self {
            Self::Sealed(relay) => {
                let published = relay.insert(path, index, text)?;
                Ok(json!({
                    "event": "text",
                    "text": relay.text(path),
                    "published": published,
                }))
            }
            Self::Plain(engine) => {
                engine.insert(path, index, text).await?;
                Ok(json!({"event": "text", "text": engine.text(path).await?}))
            }
        }
    }

    /// One selection, which `selvage/2`'s session cannot publish yet.
    async fn select(
        &self,
        path: &str,
        command: &Value,
    ) -> Result<Value, Failure> {
        let selection = SelectionOffsets {
            anchor: u32_member(command, "anchor")?,
            head: u32_member(command, "head")?,
        };
        match self {
            Self::Sealed(_) => Ok(json!({
                "event": "unsupported",
                "op": "select",
                "reason": "this client's `selvage/2` session publishes no awareness yet, so a cursor cannot be sent",
            })),
            Self::Plain(engine) => {
                engine.set_selection(path, selection).await?;
                Ok(json!({"event": "ok"}))
            }
        }
    }

    /// Everything the caller can check this side of the room against: the handshake's view,
    /// the live view, and what this replica itself holds.
    async fn report(&self, path: &str) -> Result<Value, Failure> {
        match self {
            Self::Sealed(relay) => Ok(report_sealed(relay, path)),
            Self::Plain(engine) => report_plain(engine, path).await,
        }
    }

    async fn disconnect(&self) -> Result<(), Failure> {
        match self {
            Self::Sealed(relay) => {
                relay.disconnect();
                Ok(())
            }
            Self::Plain(engine) => Ok(engine.disconnect().await?),
        }
    }
}

/// The version a link is joined with: what the caller named, or what the fragment says.
fn choose(options: &Options, sealed: bool) -> Result<WireVersion, Failure> {
    match (options.version, sealed) {
        (Some(WireVersion::Two), false) => Err(
            "this link carries no fragment, so it names no room key and no host key: a \
             `selvage/2` room is joined with the whole link, `#` and all"
                .into(),
        ),
        (Some(WireVersion::One), true) => Err(
            "this link carries §5.1's two keys, so it names a `selvage/2` room: joining it as \
             `selvage/1` would drop both keys"
                .into(),
        ),
        (Some(version), _) => Ok(version),
        (None, true) => Ok(WireVersion::Two),
        (None, false) => Ok(WireVersion::One),
    }
}

/// One command per line until stdin ends or a `quit` arrives.
///
/// Reading blocks the thread this runs on, which costs nothing here: the connection is
/// a task of its own, and this loop is the only thing the caller paces.
async fn serve(peer: &Peer, path: &str) -> Result<(), Failure> {
    let stdin = io::stdin();
    for next in stdin.lock().lines() {
        let line = next?;
        if line.trim().is_empty() {
            continue;
        }
        let command: Value = serde_json::from_str(&line)?;
        if !dispatch(peer, path, &command).await? {
            break;
        }
    }
    Ok(())
}

/// Returns `false` when the driver has been told to stop.
async fn dispatch(
    peer: &Peer,
    path: &str,
    command: &Value,
) -> Result<bool, Failure> {
    let op = command
        .get("op")
        .and_then(Value::as_str)
        .ok_or("a command needs an `op`")?;
    match op {
        "insert" => {
            let index = u32_member(command, "index")?;
            let text = command
                .get("text")
                .and_then(Value::as_str)
                .ok_or("an insert needs a `text`")?;
            let event = peer.insert(path, index, text).await?;
            emit(&event);
        }
        "select" => emit(&peer.select(path, command).await?),
        "report" => emit(&peer.report(path).await?),
        "quit" => return Ok(false),
        other => return Err(format!("unknown op {other:?}").into()),
    }
    Ok(true)
}

fn u32_member(command: &Value, member: &str) -> Result<u32, Failure> {
    let value = command
        .get(member)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("a command needs a `{member}`"))?;
    u32::try_from(value)
        .map_err(|_| format!("`{member}` is out of range").into())
}

/// What a `selvage/2` relay holds, in the one vocabulary this driver speaks.
fn report_sealed(relay: &RelaySession, path: &str) -> Value {
    let info = relay.session_info();
    let roles = relay.roles_by_seat();
    let peers: Vec<Value> = relay
        .peers()
        .into_iter()
        .map(|peer| {
            let role = roles.get(&peer.peer_id).map_or("guest", String::as_str);
            json!({
                "peer_id": peer.peer_id,
                "display_name": peer.display_name,
                "role": role,
                "awareness_client_id": peer.awareness_client_id,
            })
        })
        .collect();
    json!({
        "event": "report",
        "session": {
            "room": info.room_id,
            // §13.4: the role comes from the applied state and from nothing else, and a key no
            // state names yet is drawn the way the shared engine draws it.
            "role": relay.applied_role().unwrap_or_else(|| "guest".to_string()),
            "peer_id": info.seat,
            "documents": relay.open_documents(),
            "peers": peers,
        },
        "path": path,
        "text": relay.text(path),
        "documents": relay.documents(),
        "peers": peers,
        // No awareness is applied and none is published: §8's producer half is not in this
        // session yet.
        "presence": Vec::<Value>::new(),
        "state_vector": relay.state_vector(),
    })
}

/// Everything the caller can check this side of a `selvage/1` room against.
async fn report_plain(
    engine: &SyncEngine,
    path: &str,
) -> Result<Value, Failure> {
    let session = engine.session();
    Ok(json!({
        "event": "report",
        "session": {
            "room": session.room_id,
            "role": session.role.as_str(),
            "peer_id": session.peer.peer_id,
            "documents": session.documents,
            "peers": peers_of(&session.peers),
        },
        "path": path,
        "text": engine.text(path).await?,
        "documents": engine.documents().await?,
        "peers": peers_of(&engine.peers().await?),
        "presence": presence_of(&engine.presence().await?),
        "state_vector": vector_of(engine).await?,
    }))
}

fn peers_of(peers: &[PeerInfo]) -> Vec<Value> {
    peers
        .iter()
        .map(|peer| {
            json!({
                "peer_id": peer.peer_id,
                "display_name": peer.display_name,
                "role": peer.role.as_str(),
                "awareness_client_id": peer.awareness_client_id,
            })
        })
        .collect()
}

/// Each record twice over: the anchors as they arrived on the wire, and where this
/// replica resolves them. A caller that sees the same offsets it published, beside
/// anchor objects it did not write, has cross-implementation resolution and not a
/// number that happened to travel.
fn presence_of(records: &[Presence]) -> Vec<Value> {
    records
        .iter()
        .map(|record| {
            json!({
                "client_id": record.client_id,
                "display_name": record.display_name(),
                "role": record.peer.as_ref().map(|peer| peer.role.as_str()),
                "path": record.path(),
                "anchors": record.anchors(),
                "resolved": record.selection().map(|offsets| {
                    json!({"anchor": offsets.anchor, "head": offsets.head})
                }),
            })
        })
        .collect()
}

async fn vector_of(engine: &SyncEngine) -> Result<Vec<(u64, u32)>, Failure> {
    let mut vector = engine.state_vector().await?;
    vector.sort_unstable();
    Ok(vector)
}

/// `stdout` is line-buffered, so a reply is on the wire as soon as it is printed.
fn emit(value: &Value) {
    println!("{value}");
}

#[cfg(test)]
mod tests {
    use super::{WireVersion, options_of};

    /// Every flag the driver needs, in one place.
    fn base() -> Vec<String> {
        [
            "--invite",
            "ws://127.0.0.1:9/session?room=r-1&token=t-1",
            "--path",
            "notes.txt",
        ]
        .map(String::from)
        .to_vec()
    }

    /// `--version` at the end of a line names no value, and a flag that named none used to be
    /// dropped and leave the version to the link: the silent overrule this reading exists to
    /// avoid.
    #[test]
    fn a_version_flag_without_a_value_is_refused() {
        let mut args = base();
        args.push("--version".to_string());
        let Err(refused) = options_of(args.into_iter()) else {
            panic!("a bare --version is an error");
        };
        assert!(
            refused.to_string().contains("--version"),
            "the error names the flag: {refused}"
        );
    }

    /// The flag with a value settles the version, and the link is what settles one otherwise.
    #[test]
    fn a_version_flag_with_a_value_is_read() {
        let mut args = base();
        args.extend(["--version", "2"].map(String::from));
        let read = options_of(args.into_iter()).expect("the flag is read");
        assert_eq!(read.version, Some(WireVersion::Two));
        let read = options_of(base().into_iter()).expect("the link decides");
        assert_eq!(read.version, None);
    }
}
