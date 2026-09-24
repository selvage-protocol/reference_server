//! Joins a room on a server named on the command line, so a deployment — a
//! container, a remote host — is proved with the same client a client uses.
//!
//! `cargo run -p selvage-harness --example join_room -- ws://127.0.0.1:18080`
//!
//! A host mints a room, a guest joins it with the invite the host was handed, and the
//! guest receives an edit the host made after both opened the document. The wait is
//! bounded polling with a deadline, and a failure exits non-zero with the state it
//! observed.

use std::env;
use std::error::Error as StdError;
use std::sync::Arc;

use selvage_client::relay::{RelayHostOptions, RelayJoinOptions, RelaySession};
use selvage_client::session::KeepaliveConfig;
use selvage_harness::wait_for_described;

/// Anything this smoke can fail with.
type Failure = Box<dyn StdError>;

const PATH: &str = "smoke.md";
const EDIT: &str = "hello from a container\n";

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let base = env::args().nth(1).ok_or("usage: join_room <ws-base-url>")?;
    let host = RelaySession::host(RelayHostOptions {
        base_url: base.clone(),
        display_name: "container-host".to_string(),
        listing: Arc::new(|| vec![PATH.to_string()]),
        room_key: None,
        host_seed: None,
        store: None,
        client: Some("selvage-harness/join-room".to_string()),
        keepalive: Some(KeepaliveConfig::default()),
    })
    .await?;
    let invite = host.invite().ok_or("a host is handed a link to send")?;
    let room = host.session_info().room_id.clone();
    println!("host minted {room}");
    println!("invite {invite}");

    let guest = RelaySession::join(RelayJoinOptions {
        invite,
        display_name: "container-guest".to_string(),
        declared_role: None,
        client: Some("selvage-harness/join-room".to_string()),
        keepalive: Some(KeepaliveConfig::default()),
    })
    .await?;
    println!("guest joined {}", guest.session_info().room_id);

    host.open(PATH)?;
    guest.open(PATH)?;
    host.insert(PATH, 0, EDIT)?;
    wait_for_described(
        "the guest to receive the host's edit",
        || async { format!("{:?}", guest.text(PATH)) },
        || async { guest.text(PATH).contains("container").then_some(()) },
    )
    .await;
    println!("guest holds {:?}", guest.text(PATH));

    host.disconnect();
    guest.disconnect();
    Ok(())
}
