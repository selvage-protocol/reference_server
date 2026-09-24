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
//! **The link is the whole input.** A `selvage/2` invite carries the room key and the host
//! key in its fragment (`PROTOCOL.md` §5.1), and a link without one is refused rather than
//! joined: dropping those two keys is the defect the rule exists to avoid.
//!
//! **The vocabulary.** `insert`, `select`, `report` and `quit` are the commands, and `text`,
//! `ok`, `report` and `unsupported` the events, whichever implementation is on the other end.
//! What this session is:
//!
//! - `session.role` is the role the applied room state gives this connection's own key
//!   (§13.4), with the `guest` fallback for a key no state names yet.
//! - `session.documents` is §13.7's union of the live holds.
//! - `presence` is empty: this client's session publishes no awareness and reads none back,
//!   so `select` is answered `unsupported` rather than `ok` — and the hello advertises
//!   `y-protocols/1` alone for the same reason, because §10 defines `awareness` as "the peer
//!   publishes presence".
//! - `insert`'s reply carries `published` — whether the frame went out, which is `false`
//!   until a state commits this connection's key (§13.1's step 4).
//!
//! `vscode_client/test/interop.test.ts` is the caller that matters: it hosts from the
//! TypeScript engine, spawns this as the guest, and asserts both sides converge on the
//! same text, the same room and the same state vectors.

use std::env;
use std::error::Error as StdError;
use std::io::{self, BufRead};

use serde_json::{Value, json};

use selvage_client::relay::{RelayJoinOptions, RelaySession};

/// Anything this driver can fail with.
type Failure = Box<dyn StdError>;

/// The command line, and nothing the client itself does not need.
struct Options {
    invite: String,
    path: String,
    name: String,
}

fn options() -> Result<Options, Failure> {
    options_of(env::args().skip(1))
}

/// The command line, read from any sequence of arguments so that a test can hand it one.
fn options_of(args: impl Iterator<Item = String>) -> Result<Options, Failure> {
    let mut invite = None;
    let mut path = None;
    let mut name = None;
    let mut args = args;
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
    let peer = join(&options).await?;
    peer.open(&options.path)?;
    serve(&peer, &options.path)?;
    peer.disconnect();
    Ok(())
}

/// Joins the room the link names.
async fn join(options: &Options) -> Result<RelaySession, Failure> {
    Ok(RelaySession::join(RelayJoinOptions {
        invite: options.invite.clone(),
        display_name: options.name.clone(),
        declared_role: None,
        client: Some(format!("selvage-interop/{}", env!("CARGO_PKG_VERSION"))),
        keepalive: None,
    })
    .await?)
}

/// One command per line until stdin ends or a `quit` arrives.
///
/// Reading blocks the thread this runs on, which costs nothing here: the connection is
/// a task of its own, and this loop is the only thing the caller paces.
fn serve(peer: &RelaySession, path: &str) -> Result<(), Failure> {
    let stdin = io::stdin();
    for next in stdin.lock().lines() {
        let line = next?;
        if line.trim().is_empty() {
            continue;
        }
        let command: Value = serde_json::from_str(&line)?;
        if !dispatch(peer, path, &command)? {
            break;
        }
    }
    Ok(())
}

/// Returns `false` when the driver has been told to stop.
fn dispatch(
    peer: &RelaySession,
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
            let published = peer.insert(path, index, text)?;
            emit(&json!({
                "event": "text",
                "text": peer.text(path),
                "published": published,
            }));
        }
        "select" => emit(&json!({
            "event": "unsupported",
            "op": "select",
            "reason": "this session publishes no awareness, so a cursor cannot be sent",
        })),
        "report" => emit(&report(peer, path)),
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

/// Everything the caller can check this side of the room against: the handshake's view,
/// the live view, and what this replica itself holds.
fn report(relay: &RelaySession, path: &str) -> Value {
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
        // §8's producer half is not in this session: it publishes no awareness state and
        // reads none back, so this connection shows no cursor.
        "presence": Vec::<Value>::new(),
        "state_vector": relay.state_vector(),
    })
}

/// `stdout` is line-buffered, so a reply is on the wire as soon as it is printed.
fn emit(value: &Value) {
    println!("{value}");
}

#[cfg(test)]
mod tests {
    use super::options_of;

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

    #[test]
    fn the_required_flags_are_read_and_others_refused() {
        let read = options_of(base().into_iter()).expect("the flags are read");
        assert_eq!(read.name, "interop-peer");
        let mut named = base();
        named.extend(["--name", "peer"].map(String::from));
        assert_eq!(
            options_of(named.into_iter())
                .expect("the flags are read")
                .name,
            "peer"
        );
        let mut unknown = base();
        unknown.push("--version".to_string());
        let refused = options_of(unknown.into_iter())
            .expect_err("only the three flags are read");
        assert!(
            refused.to_string().contains("--version"),
            "the error names the argument: {refused}"
        );
    }
}
