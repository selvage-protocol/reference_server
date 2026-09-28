# The vectors

The wire protocol is described by the specification repository,
[`selvage-protocol/specification`](https://github.com/selvage-protocol/specification):
`PROTOCOL.md` is the prose, `CANONICAL.md` fixes the bytes of a frame, and
`schema/` is the machine-readable model. This repository authors none of it.

Its wire vectors are vendored here as `vectors/`, next to the harness that
replays them, so that a plain `cargo test` and the Nix sandbox need no sibling
checkout. `scripts/sync-vectors.sh` copies them in from a specification checkout
when they change, and the specification remains the canonical source.

The replay reads `vectors/`, or `SELVAGE_VECTORS` when that is set. The Nix
build cannot see outside the Cargo workspace, so `flake.nix` hands the directory
in explicitly.

`crates/harness/tests/vectors.rs` is that replay: each of the corpus's 24 wire
transcripts is driven against a server the harness starts, and every frame it
answers with is compared to the bytes the vector writes. None of the 24 is
skipped: a vector this server cannot answer is a defect to fix in the corpus or
in the server, never a hole in the sweep. The specification's own runner
(`runner/run_vectors.py`, against a built `selvaged`) replays the same files.
