# The checks worth running

The harness is the quickest way to watch the protocol work:

```sh
nix develop . -c cargo run -p selvage-harness                  # the whole slice, printed step by step
nix develop . -c cargo test --all-targets                      # unit tests, every integration suite and the examples' own
```

`cargo run -p selvage-harness` runs a scripted demo transcript: it starts a
server, mints a room, prints the invite link, walks two clients through it, and
exits. `selvaged` waits for a client to connect instead, and a client's output
carries the invite link.

`scripts/ci-local.sh` runs the same commands as `.github/workflows/ci.yml` on
this machine, one flake check per step, and needs `nix`:

```sh
scripts/ci-local.sh all        # the whole gate, and what the `checks` job runs
scripts/ci-local.sh nightly    # coverage, the rest of cargo-deny and cargo-audit (slow)
scripts/ci-local.sh lint       # actionlint over the workflow files, on its own
scripts/ci-local.sh image      # the nix-built image smoke, no Docker needed
scripts/ci-local.sh container  # docker build, docker run and a room join (needs Docker)
```

`all` is what the `checks` job runs; `nightly` is opt-in because it is slow. The
`image` workflow's two buildx jobs have no step here, since this host has no
Docker, let alone buildx; a pull request's checks are where they run.

`checks`, `nightly` and `image` refuse to run when the working tree differs from
`HEAD`, because what they build is the tracked tree at its working-tree content
and CI checks out the committed ref: an untracked file — a new test, a new
vector — is invisible to the build, so its green run would be of a smaller suite
than CI's. Commit, or stash, before running them.

## What lives here

| Path              | What it is                                                                                                                                                                                                                                                  |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/protocol` | the `selvage/2` session envelope, method/event/error vocabulary, invite URLs. No I/O.                                                                                                                                                                       |
| `crates/selvaged` | the server: rooms, membership, payload-opaque relay, `GET /meta`                                                                                                                                                                                            |
| `crates/client`   | the `selvage/2` client: the sealed frame (`sealed.rs`), the peer session (`peer.rs`), the host's producer half (`host.rs`) and the relay that puts a session on a socket (`relay.rs`)                                                                       |
| `crates/harness`  | one server and the waits the integration tests share; the wire corpus's replay over `vectors/`, the runnable transcript (`cargo run -p selvage-harness`), the peer layer's two suites, and `selvage-subject`, the client the corpus's decision layer drives |
| `vectors/`        | the wire vectors and the peer corpus, vendored from the specification; `scripts/sync-vectors.sh` refreshes them                                                                                                                                             |
