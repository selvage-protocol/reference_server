//! A Selvage client another toolchain can drive, so a second implementation is put in
//! one room with this one and compared against it rather than against a mock.
//!
//! `cargo run -p selvage-harness --example interop_peer -- \
//!      --invite <session-url> --path <document-path> [--name <display-name>]`
//!
//! It joins through the invite URL it is given — the link a host shares — opens the
//! document and reports what its own replica holds. Commands arrive as one JSON object
//! per line on stdin and every reply is one JSON object per line on stdout, so the
//! caller drives the editing and does the waiting: this side never sleeps, and it
//! speaks first to nobody. A link this client cannot join with is an error, not a
//! fallback.
//!
//! `vscode_client/test/interop.test.ts` is the caller that matters: it hosts from the
//! TypeScript engine, spawns this as the guest, and asserts both sides converge on the
//! same text, the same room and the same state vectors.

use std::env;
use std::error::Error as StdError;
use std::io::{self, BufRead};

use serde_json::{Value, json};

use selvage_client::{
    ConnectOptions, PeerInfo, Presence, SelectionOffsets, SyncEngine,
};

/// Anything this driver can fail with.
type Failure = Box<dyn StdError>;

/// The command line, and nothing the client itself does not need.
struct Options {
    invite: String,
    path: String,
    name: String,
}

fn options() -> Result<Options, Failure> {
    let mut invite = None;
    let mut path = None;
    let mut name = None;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--invite" => invite = args.next(),
            "--path" => path = args.next(),
            "--name" => name = args.next(),
            other => {
                return Err(format!("unknown argument {other:?}").into());
            }
        }
    }
    Ok(Options {
        invite: invite.ok_or("--invite <session-url> is required")?,
        path: path.ok_or("--path <document-path> is required")?,
        name: name.unwrap_or_else(|| "interop-peer".to_string()),
    })
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let options = options()?;
    let engine = join(&options).await?;
    engine.open(&options.path).await?;
    serve(&engine, &options.path).await?;
    engine.disconnect().await?;
    Ok(())
}

async fn join(options: &Options) -> Result<SyncEngine, Failure> {
    let connect =
        ConnectOptions::from_invite_url(&options.invite, options.name.as_str())
            .ok_or("--invite is not a session URL this client can join with")?;
    Ok(SyncEngine::connect(connect).await?)
}

/// One command per line until stdin ends or a `quit` arrives.
///
/// Reading blocks the thread this runs on, which costs nothing here: the engine is
/// a task of its own, and this loop is the only thing the caller paces.
async fn serve(engine: &SyncEngine, path: &str) -> Result<(), Failure> {
    let stdin = io::stdin();
    for next in stdin.lock().lines() {
        let line = next?;
        if line.trim().is_empty() {
            continue;
        }
        let command: Value = serde_json::from_str(&line)?;
        if !dispatch(engine, path, &command).await? {
            break;
        }
    }
    Ok(())
}

/// Returns `false` when the driver has been told to stop.
async fn dispatch(
    engine: &SyncEngine,
    path: &str,
    command: &Value,
) -> Result<bool, Failure> {
    let op = command
        .get("op")
        .and_then(Value::as_str)
        .ok_or("a command needs an `op`")?;
    match op {
        "insert" => insert(engine, path, command).await?,
        "select" => select(engine, path, command).await?,
        "report" => emit(&report(engine, path).await?),
        "quit" => return Ok(false),
        other => return Err(format!("unknown op {other:?}").into()),
    }
    Ok(true)
}

/// Applies an insert locally and answers with this replica's own text afterwards, which
/// is what the caller compares against its own.
async fn insert(
    engine: &SyncEngine,
    path: &str,
    command: &Value,
) -> Result<(), Failure> {
    let index = u32_member(command, "index")?;
    let text = command
        .get("text")
        .and_then(Value::as_str)
        .ok_or("an insert needs a `text`")?;
    engine.insert(path, index, text).await?;
    emit(&json!({"event": "text", "text": engine.text(path).await?}));
    Ok(())
}

async fn select(
    engine: &SyncEngine,
    path: &str,
    command: &Value,
) -> Result<(), Failure> {
    let selection = SelectionOffsets {
        anchor: u32_member(command, "anchor")?,
        head: u32_member(command, "head")?,
    };
    engine.set_selection(path, selection).await?;
    emit(&json!({"event": "ok"}));
    Ok(())
}

fn u32_member(command: &Value, member: &str) -> Result<u32, Failure> {
    let value = command
        .get(member)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("a command needs a `{member}`"))?;
    u32::try_from(value)
        .map_err(|_| format!("`{member}` is out of range").into())
}

/// Everything the caller can check this side of the room against: the handshake's view,
/// the live view, and what this replica itself holds.
async fn report(engine: &SyncEngine, path: &str) -> Result<Value, Failure> {
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
