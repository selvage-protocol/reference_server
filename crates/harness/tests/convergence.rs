//! The gate: two clients, one room, one document, concurrent edits, convergence.
//!
//! Concurrency is created by pausing outbound frames on both clients, editing, and
//! resuming. Neither edit can be in the other's causal history, so the merge that
//! follows is a real CRDT merge and not a replay of a linear history.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use selvage_harness::{
    wait_for, wait_for_convergence, wait_for_peer, EditorAdapter, Harness, Presence, Role,
    Selection,
};

const PATH: &str = "src/main.rs";
const SEED: &str = "fn main() {\n    println!(\"hello\");\n}\n";

#[tokio::test]
async fn two_clients_converge_and_see_each_other() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");

    // --- session layer -----------------------------------------------------

    assert_eq!(host.session().role, Role::Host);
    assert_eq!(guest.session().role, Role::Guest);
    assert_eq!(host.session().room_id, guest.session().room_id);
    assert!(
        room.invite_url.contains(&room.token),
        "the invite URL carries the token: {}",
        room.invite_url
    );

    // --- one document, seeded by the host ----------------------------------

    host.open(PATH).await.expect("host opens the document");
    guest.open(PATH).await.expect("guest opens the document");
    host.insert(PATH, 0, SEED).await.expect("host writes the seed");

    let seeded = wait_for("the guest to receive the seeded document", || async {
        let text = guest.text(PATH).await.ok()?;
        (text == SEED).then_some(text)
    })
    .await;
    assert_eq!(seeded, SEED);

    // --- membership --------------------------------------------------------

    let bob = wait_for_peer(&host, "Bob").await;
    let ada = wait_for_peer(&guest, "Ada").await;
    assert_eq!(bob.role, Role::Guest);
    assert_eq!(ada.role, Role::Host);
    assert!(
        ada.awareness_client_id.is_some() && bob.awareness_client_id.is_some(),
        "the session layer attributes an awareness client id to each peer"
    );

    // --- presence ----------------------------------------------------------

    let ada_selection = Selection { anchor: 0, head: 2 };
    let bob_selection = Selection {
        anchor: 11,
        head: 13,
    };
    host.set_selection(PATH, ada_selection)
        .await
        .expect("host publishes its cursor");
    guest
        .set_selection(PATH, bob_selection)
        .await
        .expect("guest publishes its cursor");

    let bob_on_host = wait_for("Bob's cursor to reach the host", || async {
        host.presence()
            .await
            .ok()?
            .into_iter()
            .find(|p| presence_is(p, "Bob", PATH, bob_selection))
    })
    .await;
    assert_eq!(bob_on_host.client_id, bob.awareness_client_id.unwrap());

    let ada_on_guest = wait_for("Ada's cursor to reach the guest", || async {
        guest
            .presence()
            .await
            .ok()?
            .into_iter()
            .find(|p| presence_is(p, "Ada", PATH, ada_selection))
    })
    .await;
    assert_eq!(ada_on_guest.client_id, ada.awareness_client_id.unwrap());

    // --- concurrent edits --------------------------------------------------

    for engine in [&host, &guest] {
        engine
            .set_outbound_paused(true)
            .await
            .expect("outbound can be paused");
    }
    host.insert(PATH, 0, "AAA ").await.expect("host edit is local");
    guest.insert(PATH, 0, "BBB ").await.expect("guest edit is local");

    // Neither client has transmitted, so neither can have seen the other's edit.
    assert_eq!(host.text(PATH).await.unwrap(), format!("AAA {SEED}"));
    assert_eq!(guest.text(PATH).await.unwrap(), format!("BBB {SEED}"));

    for engine in [&host, &guest] {
        engine
            .set_outbound_paused(false)
            .await
            .expect("outbound can be resumed");
    }

    let merged = wait_for_convergence(&host, &guest, PATH).await;
    assert!(merged.contains("AAA"), "the merge kept the host's edit: {merged:?}");
    assert!(merged.contains("BBB"), "the merge kept the guest's edit: {merged:?}");
    assert!(merged.contains("hello"), "the merge kept the seed: {merged:?}");
    // Concurrent inserts at the same index are ordered, never interleaved.
    assert!(
        merged.starts_with("AAA BBB ") || merged.starts_with("BBB AAA "),
        "concurrent inserts stay contiguous: {merged:?}"
    );
    assert_eq!(merged.len(), SEED.len() + 8);

    // Convergence is not just text equality: the replicas must hold the same history.
    let vectors = wait_for("state vectors to match", || async {
        let (a, b) = (
            host.state_vector().await.ok()?,
            guest.state_vector().await.ok()?,
        );
        (a == b).then_some(a)
    })
    .await;
    assert_eq!(
        vectors.len(),
        2,
        "one entry per editing client, got {vectors:?}"
    );

    // A late joiner must be brought up to the merged state.
    let late = harness.join(&room, "Cleo").await.expect("third client joins");
    late.open(PATH).await.expect("third client opens");
    let on_late = wait_for("the late joiner to receive the merged document", || async {
        let text = late.text(PATH).await.ok()?;
        (text == merged).then_some(text)
    })
    .await;
    assert_eq!(on_late, merged);
}

fn presence_is(presence: &Presence, name: &str, path: &str, selection: Selection) -> bool {
    presence.display_name() == Some(name)
        && presence.path() == Some(path)
        && presence.selection() == Some(selection)
}

/// The editor-adapter seam: an adapter that mirrors documents into a buffer, driven by
/// the engine's event stream exactly as an editor plugin would be.
#[derive(Default)]
struct Mirror {
    documents: Mutex<HashMap<String, String>>,
    presence: Mutex<usize>,
}

impl EditorAdapter for Mirror {
    fn document_changed(&self, path: &str, text: &str) {
        self.documents
            .lock()
            .expect("mirror is not poisoned")
            .insert(path.to_string(), text.to_string());
    }

    fn presence_changed(&self, presence: &[selvage_harness::Presence]) {
        *self.presence.lock().expect("mirror is not poisoned") = presence.len();
    }
}

impl Mirror {
    fn text(&self, path: &str) -> Option<String> {
        self.documents.lock().unwrap().get(path).cloned()
    }
}

#[tokio::test]
async fn the_editor_adapter_seam_carries_remote_edits() {
    let harness = Harness::start(Duration::from_secs(5)).await;
    let (host, room) = harness.host("Ada").await.expect("host connects");
    let guest = harness.join(&room, "Bob").await.expect("guest joins");

    let mirror = Arc::new(Mirror::default());
    let driver = selvage_harness::drive_editor(&guest, mirror.clone());

    host.open(PATH).await.unwrap();
    guest.open(PATH).await.unwrap();
    host.insert(PATH, 0, "shipped\n").await.unwrap();
    host.insert(PATH, 0, "not ").await.unwrap();

    let mirrored = wait_for("the adapter to mirror the remote text", || async {
        let text = mirror.text(PATH)?;
        (text == "not shipped\n").then_some(text)
    })
    .await;
    assert_eq!(mirrored, "not shipped\n");
    driver.abort();
}
