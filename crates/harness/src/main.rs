//! Runs the whole slice end to end against a real server and prints the transcript.
//!
//! `cargo run -p selvage-harness` — the same path the integration tests assert on,
//! but observable.

use std::time::Duration;

use selvage_harness::{wait_for, Harness, Selection};

const PATH: &str = "src/main.rs";

#[tokio::main]
async fn main() {
    let harness = Harness::start(Duration::from_secs(30)).await;
    println!("server     {}", harness.ws_base());

    let (host, room) = harness.host("Ada").await.expect("host connects");
    println!("room       {} (host Ada)", room.id);
    println!("invite     {}", room.invite_url);
    println!(
        "meta       {}",
        http_get(&format!("{}/meta", harness.http_base())).await
    );

    let guest = harness.join(&room, "Bob").await.expect("guest joins");
    println!("guest      Bob joined as {:?}", guest.session().role);

    host.open(PATH).await.expect("host opens");
    guest.open(PATH).await.expect("guest opens");
    println!("documents  {:?}", host.documents().await.expect("documents"));

    host.insert(PATH, 0, "fn main() {\n    println!(\"hello\");\n}\n")
        .await
        .expect("host seeds");
    let seeded = wait_for("the guest to see the seed", || async {
        let text = guest.text(PATH).await.ok()?;
        (text.contains("hello")).then_some(text)
    })
    .await;
    println!("seeded     {:?}", seeded);

    host.set_selection(PATH, Selection { anchor: 3, head: 7 })
        .await
        .expect("host cursor");
    guest
        .set_selection(PATH, Selection { anchor: 11, head: 15 })
        .await
        .expect("guest cursor");
    let presence = wait_for("both cursors", || async {
        let on_host = host.presence().await.ok()?;
        let on_guest = guest.presence().await.ok()?;
        let seen = |list: &[selvage_harness::Presence], name: &str| {
            list.iter().any(|p| {
                p.display_name() == Some(name) && p.selection().is_some() && p.path() == Some(PATH)
            })
        };
        (seen(&on_host, "Bob") && seen(&on_guest, "Ada")).then_some((on_host, on_guest))
    })
    .await;
    for list in [&presence.0, &presence.1] {
        for entry in list {
            println!(
                "presence   {} @ {:?} {:?}",
                entry.display_name().unwrap_or("(unknown)"),
                entry.path(),
                entry.selection()
            );
        }
    }

    // Concurrent edits: both clients buffer their frames, so neither edit can be in
    // the other's causal history.
    for engine in [&host, &guest] {
        engine.set_outbound_paused(true).await.expect("pause");
    }
    host.insert(PATH, 0, "// host edit\n").await.expect("host edits");
    guest.insert(PATH, 0, "// guest edit\n").await.expect("guest edits");
    println!("paused     host={:?}", host.text(PATH).await.expect("host text"));
    println!("paused     guest={:?}", guest.text(PATH).await.expect("guest text"));
    for engine in [&host, &guest] {
        engine.set_outbound_paused(false).await.expect("resume");
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

    let guest_left = wait_for("the host to notice the guest leaving", || async {
        let peers = host.peers().await.ok()?;
        peers.is_empty().then_some(())
    });
    guest.disconnect().await.expect("guest leaves");
    guest_left.await;
    println!("left       guest disconnected");

    host.disconnect().await.expect("host leaves");
    let reconnected = harness.reclaim(&room, "Ada").await.expect("host returns");
    println!(
        "reconnect   host returned as {:?} to {} with documents {:?}",
        reconnected.session().role,
        reconnected.session().room_id,
        reconnected.documents().await.expect("documents")
    );
    reconnected.disconnect().await.expect("host leaves again");
}

async fn http_get(url: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let address = url.trim_start_matches("http://");
    let (host, path) = address.split_once('/').expect("host/path");
    let mut stream = tokio::net::TcpStream::connect(host).await.expect("connects");
    stream
        .write_all(
            format!("GET /{path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .expect("writes");
    let mut response = String::new();
    stream.read_to_string(&mut response).await.expect("reads");
    response
        .split_once("\r\n\r\n")
        .expect("body")
        .1
        .trim()
        .to_string()
}
