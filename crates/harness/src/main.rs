//! Runs the whole slice end to end against a real server and prints the transcript.
//!
//! `cargo run -p selvage-harness` — the same path the integration tests assert on,
//! but observable.

use std::error::Error as StdError;
use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use selvage_harness::{
    Harness, Presence, Room, Selection, SyncEngine, wait_for,
};

const PATH: &str = "src/main.rs";

/// Anything this transcript can fail with.
type Failure = Box<dyn StdError>;

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let harness = Harness::start(Duration::from_secs(30)).await;
    println!("server     {}", harness.ws_base());

    let (host, room) = harness.host("Ada").await?;
    println!("room       {} (host Ada)", room.id);
    println!("invite     {}", room.invite_url);
    println!(
        "meta       {}",
        http_get(&format!("{}/meta", harness.http_base())).await?
    );

    let guest = harness.join(&room, "Bob").await?;
    println!("guest      Bob joined as {:?}", guest.session().role);

    seed(&host, &guest).await?;
    cursors(&host, &guest).await?;
    merge(&host, &guest).await?;
    guest_leaves(&host, &guest).await?;
    host_returns(&harness, &room, &host).await
}

/// One document, seeded by the host and received by the guest.
async fn seed(host: &SyncEngine, guest: &SyncEngine) -> Result<(), Failure> {
    host.open(PATH).await?;
    guest.open(PATH).await?;
    println!("documents  {:?}", host.documents().await?);

    host.insert(PATH, 0, "fn main() {\n    println!(\"hello\");\n}\n")
        .await?;
    let seeded = wait_for("the guest to see the seed", || async {
        let text = guest.text(PATH).await.ok()?;
        (text.contains("hello")).then_some(text)
    })
    .await;
    println!("seeded     {seeded:?}");
    Ok(())
}

/// Both cursors, each visible to the other side.
async fn cursors(host: &SyncEngine, guest: &SyncEngine) -> Result<(), Failure> {
    host.set_selection(PATH, Selection { anchor: 3, head: 7 })
        .await?;
    guest
        .set_selection(
            PATH,
            Selection {
                anchor: 11,
                head: 15,
            },
        )
        .await?;
    let presence = wait_for("both cursors", || async {
        let on_host = host.presence().await.ok()?;
        let on_guest = guest.presence().await.ok()?;
        (seen(&on_host, "Bob") && seen(&on_guest, "Ada"))
            .then_some((on_host, on_guest))
    })
    .await;
    print_presence(&presence.0);
    print_presence(&presence.1);
    Ok(())
}

/// Concurrent edits: both clients buffer their frames, so neither edit can be in the
/// other's causal history.
async fn merge(host: &SyncEngine, guest: &SyncEngine) -> Result<(), Failure> {
    for engine in [host, guest] {
        engine.set_outbound_paused(true).await?;
    }
    host.insert(PATH, 0, "// host edit\n").await?;
    guest.insert(PATH, 0, "// guest edit\n").await?;
    println!("paused     host={:?}", host.text(PATH).await?);
    println!("paused     guest={:?}", guest.text(PATH).await?);
    for engine in [host, guest] {
        engine.set_outbound_paused(false).await?;
    }

    let merged = wait_for("convergence", || async {
        let left = host.text(PATH).await.ok()?;
        let right = guest.text(PATH).await.ok()?;
        (left == right).then_some(left)
    })
    .await;
    println!("merged     {merged:?}");

    let vectors = wait_for("state vectors to match", || async {
        let left = host.state_vector().await.ok()?;
        let right = guest.state_vector().await.ok()?;
        (left == right).then_some(left)
    })
    .await;
    println!("vectors    {vectors:?}");
    Ok(())
}

/// The guest disconnects; the host is told.
async fn guest_leaves(
    host: &SyncEngine,
    guest: &SyncEngine,
) -> Result<(), Failure> {
    let noticed = wait_for("the host to notice the guest leaving", || async {
        let peers = host.peers().await.ok()?;
        peers.is_empty().then_some(())
    });
    guest.disconnect().await?;
    noticed.await;
    println!("left       guest disconnected");
    Ok(())
}

/// The host leaves and comes back into the same room, which kept its documents.
async fn host_returns(
    harness: &Harness,
    room: &Room,
    host: &SyncEngine,
) -> Result<(), Failure> {
    host.disconnect().await?;
    let reconnected = harness.reclaim(room, "Ada").await?;
    println!(
        "reconnect   host returned as {:?} to {} with documents {:?}",
        reconnected.session().role,
        reconnected.session().room_id,
        reconnected.documents().await?
    );
    reconnected.disconnect().await?;
    Ok(())
}

fn seen(list: &[Presence], name: &str) -> bool {
    list.iter().any(|p| {
        p.display_name() == Some(name)
            && p.selection().is_some()
            && p.path() == Some(PATH)
    })
}

fn print_presence(list: &[Presence]) {
    for entry in list {
        println!(
            "presence   {} @ {:?} {:?}",
            entry.display_name().unwrap_or("(unknown)"),
            entry.path(),
            entry.selection()
        );
    }
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
