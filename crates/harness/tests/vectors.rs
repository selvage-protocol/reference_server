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

#[tokio::test]
async fn every_vector_holds() {
    let vectors = runner::load().expect("the vector directory is readable");
    assert!(
        !vectors.is_empty(),
        "no vectors in {}: the conformance suite has nothing to run",
        runner::root().display()
    );
    let failures = replay_all(&vectors).await;
    assert!(
        failures.is_empty(),
        "{} of {} vectors failed: {failures:?}",
        failures.len(),
        vectors.len()
    );
    println!(
        "{} vectors replayed from {}",
        vectors.len(),
        runner::root().display()
    );
}

/// Replays every vector, reporting the ones that did not hold rather than stopping at the
/// first: a protocol change usually moves more than one transcript.
async fn replay_all(vectors: &[runner::Vector]) -> Vec<String> {
    let mut failures = Vec::new();
    for vector in vectors {
        let Err(error) = runner::replay(vector).await else {
            continue;
        };
        eprintln!("FAIL {error}");
        failures.push(vector.id.clone());
    }
    failures
}

#[tokio::test]
async fn a_transcript_that_stops_reading_early_does_not_hold() {
    // Vector 016 ends by reading the `doc.opened` its own `doc.open` announces;
    // without that step the frame sits on the connection, which is the omission
    // the specification runner caught in four transcripts. The replay must fail
    // naming the connection and the frame it still holds.
    let vectors = runner::load().expect("the vector directory is readable");
    let vector = vectors
        .iter()
        .find(|vector| vector.id == "016")
        .expect("vector 016 is vendored");
    assert!(
        vector.steps.last().is_some_and(|step| step.op == "expect"),
        "vector 016 still ends in the `doc.opened` read this test cuts"
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
