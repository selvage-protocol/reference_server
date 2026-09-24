//! The conformance vector set: `vectors/*.json`, replayed against the reference server.
//!
//! Each vector is a transcript of real bytes, bound to a wire version and to the canonical
//! form of `CANONICAL.md`. The vector set is canonical in the specification repository
//! (`selvage-protocol/specification`) and vendored here by `scripts/sync-vectors.sh`; the
//! specification's own `README.md` says how to add one and `runner.rs` executes them. A vector
//! that stops holding is a protocol change, deliberate or not, and this test is where it is
//! noticed.

#[path = "vectors/provenance.rs"]
mod provenance;
#[path = "vectors/runner.rs"]
mod runner;

/// The wire layer's corpus: `vectors/*.json` and not `vectors/peer/`. A vector deleted from
/// the corpus replays green on less, so the count is asserted here as the specification's
/// `schema/validate.py` asserts it there.
const WIRE_VECTORS: usize = 24;

/// Vectors the corpus gets wrong rather than the server, and what is wrong with each.
///
/// A vector named here still runs; `a_named_corpus_defect_is_still_one` is what keeps the
/// entry honest, because it asserts the vector still fails and fails at a frame. The day the
/// corpus is fixed that test goes red, and the entry goes with it — so this is a pinned
/// defect and not a hole in the sweep.
const BROKEN_IN_CORPUS: [(&str, &str); 1] = [(
    "031",
    "the re-baseline onto `selvage/2` rewrote the frame inside the step's `text` and \
     collapsed the repeated member the vector exists to send: `{\"id\":1,\"id\":2,…}` \
     became `{\"id\":2,…}`, so the frame no longer repeats a member and a conforming \
     receiver seats it (specification `vectors/031-repeated-member-name.json`)",
)];

#[tokio::test]
async fn every_vector_holds() {
    let vectors = runner::load().expect("the vector directory is readable");
    assert_eq!(
        vectors.len(),
        WIRE_VECTORS,
        "the wire layer has {WIRE_VECTORS} vectors; this run found {}",
        vectors.len()
    );
    let failures = replay_all(&vectors).await;
    assert!(
        failures.is_empty(),
        "{} of {} vectors failed: {failures:?}",
        failures.len(),
        vectors.len()
    );
    println!(
        "{} vectors replayed from {}, {} of them named as corpus defects",
        vectors.len() - BROKEN_IN_CORPUS.len(),
        runner::root().display(),
        BROKEN_IN_CORPUS.len()
    );
}

/// Replays every vector but the named corpus defects, reporting the ones that did not hold
/// rather than stopping at the first: a protocol change usually moves more than one transcript.
async fn replay_all(vectors: &[runner::Vector]) -> Vec<String> {
    let mut failures = Vec::new();
    for vector in vectors {
        if is_a_named_defect(&vector.id) {
            continue;
        }
        let Err(error) = runner::replay(vector).await else {
            continue;
        };
        eprintln!("FAIL {error}");
        failures.push(vector.id.clone());
    }
    failures
}

fn is_a_named_defect(id: &str) -> bool {
    BROKEN_IN_CORPUS.iter().any(|(defect, _)| *defect == id)
}

#[tokio::test]
async fn a_named_corpus_defect_is_still_one() {
    let vectors = runner::load().expect("the vector directory is readable");
    for (id, why) in BROKEN_IN_CORPUS {
        let carried = vectors.iter().filter(|vector| vector.id == id).count();
        assert_eq!(
            carried, 1,
            "{id} is named as a corpus defect, and the corpus carries {carried} of it: {why}"
        );
        let vector = vectors
            .iter()
            .find(|vector| vector.id == id)
            .expect("the corpus carries the vector just counted");
        let report = how_it_fails(vector).await;
        assert!(
            report.contains("frame does not match"),
            "{id} is named as a corpus defect ({why}) and does not fail at a frame: {report}"
        );
    }
}

/// How a named vector fails, as the replay reports it.
///
/// A defect entry is only honest if the vector fails at a frame — a step whose expected frame
/// and received frame differ. A vector that holds reports that it no longer fails, and one the
/// runner refuses outright reports the refusal instead, so neither satisfies the entry's
/// assertion above.
async fn how_it_fails(vector: &runner::Vector) -> String {
    match runner::replay(vector).await {
        Ok(()) => "the vector holds now".to_string(),
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn a_transcript_that_stops_reading_early_does_not_hold() {
    // Vector 016 ends by reading the `session.error` its own id-less `session.rename`
    // is answered with; without that step the frame sits on the connection, which is
    // the omission the specification runner caught in four transcripts. The replay
    // must fail naming the connection and the frame it still holds.
    let vectors = runner::load().expect("the vector directory is readable");
    let vector = vectors
        .iter()
        .find(|vector| vector.id == "016")
        .expect("vector 016 is vendored");
    assert!(
        vector.steps.last().is_some_and(|step| step.op == "expect"),
        "vector 016 still ends in the `session.error` read this test cuts"
    );
    let mut truncated = vector.clone();
    truncated.steps.pop();
    let error = runner::replay(&truncated)
        .await
        .expect_err("a transcript that stops reading early must not hold");
    let report = error.to_string();
    assert!(
        report.contains("`host`") && report.contains("does not read"),
        "the failure names the connection and its unread frame: {report}"
    );
}
