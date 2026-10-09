# Licence

The server is licensed differently from everything beside it.

| Path                                                 | Licence                                                                                                         |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `crates/protocol`, `crates/client`, `crates/harness` | `MIT OR Apache-2.0`, the workspace default: [`LICENSE-MIT`](../LICENSE-MIT) and [`LICENSE-APACHE`](../LICENSE-APACHE) |
| `crates/selvaged`                                    | `FSL-1.1-MIT`: [source-available, _not_ open source](../crates/selvaged/LICENSE)                                   |
| `vectors/`                                           | vendored from the specification repository, whose material is `CC-BY-4.0`                                       |

The Functional Source License 1.1 is free for any non-competing purpose: a
company self-hosting it internally is free, as are non-commercial education and
research. It forbids making the software available to others in a commercial
product or service that substitutes for it. The published image carries the
licence: `crates/selvaged/LICENSE` travels inside it and its
`org.opencontainers.image.licenses` label names the licence. Each release
converts to MIT on the second anniversary of the date it was made available,
irrevocably.

The harness links `selvaged`, so its own `MIT OR Apache-2.0` covers the crate
while a redistributed `selvage-harness` binary carries FSL code with it. That
redistribution must include the FSL terms or a link to them and retain the
copyright notices; the harness's licence does not replace its dependency's FSL
terms.

The vendored vectors are `CC-BY-4.0`
([`selvage-protocol/specification`](https://github.com/selvage-protocol/specification)),
which this repository does not author and redistributes with that repository as
the source of the attribution.
