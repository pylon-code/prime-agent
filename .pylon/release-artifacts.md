# Pylon Prime release artifacts

Pylon builds Prime Agent from the protected `pylon` branch without changing the installed package or command identity. This document defines the deterministic candidate artifact boundary. Publication, attestation, Pylon-side verification, and installation are separate changes.

## Frozen recipe

`scripts/pylon-prime-release-v1.json` is the recipe authority. A recipe revision pins:

- source repository `pylon-code/prime-agent`;
- exact Node and npm versions;
- minimum supported Node version;
- the immutable `pylon-build-g<commit12>-r<recipe>` GitHub release tag pattern; and
- manifest schema version 1.

The full source commit and tree remain authoritative. The abbreviated commit in the build tag is only a readable locator. Publication must reject a tag collision unless every byte and the full manifest identity match.

The build requires the canonical `pylon-code/prime-agent` origin, a clean committed source tree, and a complete lockfile v3. Every root `node_modules` registry entry used to synthesize release archives must contain an HTTPS registry tarball URL and canonical 64-byte SHA-512 integrity. All workspace package versions must match. Build output goes only below `.npm/pylon-release` unless an empty output directory is supplied explicitly.

If npm leaves exact registry metadata out of a lock record, make a clean dependency-update checkpoint, run `npm run release:pylon:hydrate-lock` with the pinned toolchain, and amend that checkpoint. The hydrator intentionally starts from a clean tree. It queries `npm view <name>@<exact-version> dist --json`, adds only missing `resolved` and `integrity` fields, and rejects any package-graph change. Never use `npm update` to hydrate release metadata.

The release path uses committed `packages/ai/src/models.generated.ts`. `PYLON_RELEASE_OFFLINE=1` makes the live generator fail, and the CI pack executes after dependency installation in a network namespace. Normal development builds retain live catalog generation. A private ignored build receipt binds every copied package input, including type and mode, to the exact source, toolchain, lock, and build ID before packing; the standalone packer rejects missing or stale receipts.

## Output contract

The artifacts directory contains exactly:

```text
pylon-prime-agent-<version>.tgz
pylon-prime-agent-ai-<version>.tgz
pylon-prime-agent-core-<version>.tgz
pylon-prime-agent-tui-<version>.tgz
pylon-prime-agent-release-v1.json
```

The root archive keeps package name `prime-agent`, command `prime-agent`, and the public SDK exports. Internal archives keep their existing scoped workspace package names. Every archive rewrites the repository to `pylon-code/prime-agent` and carries the same immutable-only `pylonDistribution` object:

- schema and recipe revisions;
- fork repository;
- full source commit and tree;
- build ID;
- exact Node and npm versions; and
- committed lockfile SHA-256.

No channel, timestamp, actor, workflow run, branch, mutable URL, or feature flag is allowed in package metadata.

Every internal dependency must resolve by matching package name to its exact URL below the immutable build release. Missing semver rewrites and cross-wired archives fail packing and verification. Archive SHA-512 values are repeated in `npm-shrinkwrap.json`. These shrinkwrap entries are auditable receipts; npm does not reliably enforce a dependency archive's nested shrinkwrap during installation. The Pylon verifier and installer must enforce manifest and attestation digests before installation.

The release manifest records the exact source, recipe, toolchain, lock digest, minimum Node version, package/command identity, sorted archive names, sizes, SHA-256, SHA-512, and external attestation subjects. It does not contain its own digest because that would be self-referential.

## Local verification

Use a clean committed checkout with the exact recipe toolchain:

```sh
npm ci
npm run test:pylon-release
npm run release:pylon:pack
npm run release:pylon:verify
npm run release:pylon:smoke
```

The pack command cleans and rebuilds only package `dist` outputs, compiles committed model data, records their exact receipt, and refuses dirty source inputs. The smoke performs a local-asset consumer install with lifecycle scripts disabled. It imports the public `prime-agent` package specifier, checks the frozen SDK tokens, invokes the installed command through pinned npm, and proves the Pylon build refuses the stock R2 self-updater without invoking even a fenced fake npm command.

Trusted CI performs two isolated Ubuntu packs and compares all five files byte-for-byte. It then repeats temporary-prefix checks on Ubuntu, macOS, and Windows. POSIX additionally requires the manifest build ID in the daemon hello, proves exact post-attach capability negotiation and authoritative owned cleanup, and retains exact worker identities for bounded failure cleanup. Windows explicitly verifies in-process ACP compatibility mode while native named-pipe authentication remains unavailable.

Matching CI packs are evidence for the pinned source, recipe, toolchain, and runner inputs. They are not a timeless or mathematically hermetic reproducibility claim.

## Trust and publication boundary

`pylonDistribution` is unsigned local metadata. It controls only local update policy and never enables daemon behavior. Runtime behavior still requires frozen public SDK tokens and exact post-attach capability negotiation.

Issue #28 creates no tag or GitHub Release and needs only `contents: read`. Its artifact jobs receive no repository secrets, and their Actions uploads are short-lived CI transport. They must not use npm publish, R2, `contents: write`, OIDC, or attestations.

Issue #29 owns protected preview publication, keyless attestations, stable promotion, rollback, and yanking. Pylon issues #193 and #194 own signed receipt verification and opt-in side-by-side install/update/rollback/switch-back. Until those land, the artifacts are build candidates, not a managed Pylon installation channel.
