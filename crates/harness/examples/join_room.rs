//! Joins a room on a server named on the command line, so a deployment — a
//! container, a remote host — is proved with the same engine a client uses.
//!
//! `cargo run -p selvage-harness --example join_room -- ws://127.0.0.1:18080`
//!
//! A host mints a room, a guest joins it with the invite the host was given, and
//! the guest receives an edit the host made after both opened the document. The
//! wait is bounded polling with a deadline, and a failure exits non-zero with the
//! state it observed.

use std::env;
use std::error::Error as StdError;

use selvage_client::ConnectOptions;
use selvage_harness::{Invite, SyncEngine, wait_for_convergence};

/// Anything this smoke can fail with.
type Failure = Box<dyn StdError>;

const PATH: &str = "smoke.md";
const EDIT: &str = "hello from a container\n";

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let base = env::args().nth(1).ok_or("usage: join_room <ws-base-url>")?;
    let host =
        SyncEngine::connect(ConnectOptions::host(&base, "container-host"))
            .await?;
    let session = host.session();
    let room = session.room_id.clone();
    let token = session
        .token
        .clone()
        .ok_or("the host that minted the room was told no token")?;
    println!("hosted     room {room} at {base}");

    let guest = SyncEngine::connect(ConnectOptions::guest(
        &base,
        "container-guest",
        Invite::new(room.clone(), token),
    ))
    .await?;
    println!("joined     guest as {:?} in {room}", guest.session().role);

    for engine in [&host, &guest] {
        engine.open(PATH).await?;
    }
    host.insert(PATH, 0, EDIT).await?;
    let text = wait_for_convergence(&host, &guest, PATH).await;
    if text != EDIT {
        return Err(
            format!("the guest converged on {text:?}, want {EDIT:?}").into()
        );
    }
    println!("converged  {text:?}");

    guest.disconnect().await?;
    host.disconnect().await?;
    Ok(())
}
