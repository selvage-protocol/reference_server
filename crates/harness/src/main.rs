//! Runs the slice end to end against a real server and prints the transcript.
//!
//! `cargo run -p selvage-harness` — the same path the integration tests assert on,
//! but observable.

use std::error::Error as StdError;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use selvage_client::relay::{RelayHostOptions, RelayJoinOptions, RelaySession};
use selvage_client::session::KeepaliveConfig;
use selvage_harness::{Harness, wait_for, wait_for_described};

const PATH: &str = "notes.txt";

/// Anything this transcript can fail with.
type Failure = Box<dyn StdError>;

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(30)).await;
    println!("server     {}", harness.ws_base());

    let host = RelaySession::host(RelayHostOptions {
        base_url: harness.ws_base(),
        display_name: "Ada".to_string(),
        listing: Arc::new(|| vec![PATH.to_string()]),
        room_key: None,
        host_seed: None,
        store: None,
        client: Some("selvage-harness/transcript".to_string()),
        keepalive: Some(KeepaliveConfig::default()),
    })
    .await?;
    let invite = host.invite().ok_or("a host is handed a link to send")?;
    println!("room       {} (host Ada)", host.session_info().room_id);
    println!("invite     {invite}");
    println!(
        "meta       {}",
        http_get(&format!("{}/meta", harness.http_base())).await?
    );

    let guest = RelaySession::join(RelayJoinOptions {
        invite,
        display_name: "Bob".to_string(),
        declared_role: None,
        client: Some("selvage-harness/transcript".to_string()),
        keepalive: Some(KeepaliveConfig::default()),
    })
    .await?;
    println!(
        "guest      Bob joined as a peer of {}",
        guest.session_info().room_id
    );

    listing(&host, &guest).await?;
    sync(&host, &guest).await?;
    rename(&host, &guest).await?;
    guest_leaves(&host, &guest).await?;
    host.disconnect();
    Ok(())
}

/// The host's sealed listing reaches the guest, which is what a link's fragment is for.
async fn listing(
    host: &RelaySession,
    guest: &RelaySession,
) -> Result<(), Failure> {
    let host_listing = wait_for("the host's own state to commit", || async {
        let listing = host.listing();
        (!listing.is_empty()).then_some(listing)
    })
    .await;
    println!("listing    host {host_listing:?}");
    let guest_listing = wait_for_described(
        "the guest to apply the host's listing",
        || async { format!("{:?}", guest.listing()) },
        || async {
            let listing = guest.listing();
            (!listing.is_empty()).then_some(listing)
        },
    )
    .await;
    println!("listing    guest {guest_listing:?}");
    Ok(())
}

/// One document, seeded by the host and received by the guest.
async fn sync(
    host: &RelaySession,
    guest: &RelaySession,
) -> Result<(), Failure> {
    host.open(PATH)?;
    guest.open(PATH)?;
    host.insert(PATH, 0, "fn main() {\n    println!(\"hello\");\n}\n")?;
    let seeded = wait_for("the guest to see the seed", || async {
        let text = guest.text(PATH);
        text.contains("hello").then_some(text)
    })
    .await;
    println!("seeded     {seeded:?}");
    Ok(())
}

/// A rename, which the room is told about as `peer.renamed`.
async fn rename(
    host: &RelaySession,
    guest: &RelaySession,
) -> Result<(), Failure> {
    let bob = guest.session_info().seat.clone();
    guest.rename("Bob B.")?;
    let seen = wait_for_described(
        "the host to hear the rename",
        || async { format!("{:?}", host.peers()) },
        || async {
            host.peers()
                .into_iter()
                .find(|peer| peer.peer_id == bob)
                .filter(|peer| peer.display_name == "Bob B.")
        },
    )
    .await;
    println!("renamed    {} is now {}", seen.peer_id, seen.display_name);
    Ok(())
}

/// The guest disconnects; the host is told.
async fn guest_leaves(
    host: &RelaySession,
    guest: &RelaySession,
) -> Result<(), Failure> {
    let seat = guest.session_info().seat.clone();
    let left = wait_for("the host to notice the guest leaving", || async {
        (!host.peers().iter().any(|peer| peer.peer_id == seat)).then_some(())
    });
    guest.disconnect();
    left.await;
    println!("left       guest disconnected");
    Ok(())
}

/// Reads `GET /path` over a throwaway connection and returns the response body.
async fn http_get(url: &str) -> io::Result<String> {
    let address = url.trim_start_matches("http://");
    let (host, path) = address
        .split_once('/')
        .ok_or_else(|| io::Error::other("the URL has no path"))?;
    let mut stream = TcpStream::connect(host).await?;
    let request = format!(
        "GET /{path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| io::Error::other("the response has no body"))?;
    Ok(body.trim().to_string())
}
