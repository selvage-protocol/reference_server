# Publishing the server

This repository versions the server independently of the clients. `Cargo.toml`'s
workspace version owns the release number; `scripts/bump-version.sh` updates it,
and `scripts/release-tags.sh` asserts image/tag agreement. A server release does
not change the protocol's wire version.

## Dispatch and gates

[release.yml](../.github/workflows/release.yml) is dispatch-only at `main`:

```sh
gh workflow run release.yml -R selvage-protocol/reference_server --ref main \
  -f bump=patch -f dry_run=true
gh workflow run release.yml -R selvage-protocol/reference_server --ref main \
  -f bump=patch
```

Choose `patch`, `minor`, or `major` deliberately. The read-only plan requires an
exact remote tag for the current manifest version and no tag for the target.
The write job rechecks these preconditions before bumping; a retried write job
cannot rely on the plan's earlier remote read. Dry run prints the plan and stops
before modifying the tree, running the build gates, or publishing anything.

Merge with green [CI](../.github/workflows/ci.yml), including the public
workspace-contract job comparing vendored vectors and bounds with the
specification. The release runs its repository-local gate on the bumped tree
before an atomic push of its commit and lightweight tag. It never force-pushes
or moves a release tag. The workflow commits as the owner; it does not open a
bump PR whose checks would never start under its own `GITHUB_TOKEN`.

[image.yml](../.github/workflows/image.yml) builds and publishes the server-only
image for `linux/amd64` and `linux/arm64`. Its tag-gated publish uses
`GITHUB_TOKEN` with `packages: write`; no separate owner token or approval
environment is configured in that workflow. Tags are `<version>-<short-sha>`,
`<version>`, and, for non-prereleases, `latest`. The release explicitly
dispatches the image workflow because a tag push using `GITHUB_TOKEN` does not
start it automatically.

## Deployment and recovery

After the image becomes available, release dispatches `deploy-prod.yml` at
`main` for its own server version. It sets `install_shape=true` when `deploy/`
changed, waits for the resulting deploy run, and fails unless that run succeeds.
A published tag or image can therefore remain after a red release. A green run
means the deploy's assertions passed, not that human acceptance or a live
rollback was performed. The host privilege boundary, credentials by name,
verification limits, and rollback procedure are in
[Operating the demo](operating-the-demo.md).

Recover the failed stage rather than dispatching a new bump blindly:

- **Tag exists, image missing:** inspect the original image run and rerun the
  workflow at the existing tag:

  ```sh
  gh workflow run image.yml -R selvage-protocol/reference_server --ref 'v<version>'
  ```

  The image workflow reasserts tag/manifest agreement. Do not move the tag to
  include a packaging fix; fix forward with a new version instead.
- **Image exists, demo deploy failed:** inspect the deploy's origin/read-back
  failure, then retry the bounded deploy at `main` with the intended version
  and shape input. Another release dispatch would compute a new version.
- **Image exists, GitHub Release missing:** the image workflow creates the
  GitHub Release in a separate job; the image and even the deployed demo can
  exist while that job is red. Inspect its failure, then rerun `image.yml` at
  the existing `v<version>` tag as above to create the missing release. Do not
  dispatch another version bump merely to recover the release record.
- **Atomic push refused:** reconcile with current `main` and tags before
  planning again. Do not force the old candidate over concurrent work.

Verify the artifact and deployed version, not just a tag's existence:

```sh
gh release view 'v<version>' -R selvage-protocol/reference_server --json tagName,assets
docker run --rm ghcr.io/selvage-protocol/selvaged:<version> --version
```

Inspect the published multi-platform manifest and its per-platform layers when
checking architecture claims. A two-platform manifest alone does not prove the
binaries inside differ. The image workflow's smoke and layer checks cover that
artifact contract; origin `/meta` checks cover the running server version.
