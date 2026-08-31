# Protected Pylon publication

Pylon publishes Prime Agent in two steps. A protected `pylon` push can create one immutable preview build. A maintainer can later promote those exact bytes to the append-only stable channel or publish a later withdrawal. No workflow publishes npm packages or rebuilds during promotion.

## Administrative prerequisites

Publication fails closed unless all of these controls exist:

- the canonical repository is `pylon-code/prime-agent`, with immutable GitHub Releases enabled;
- `refs/heads/pylon` requires strict, exact-SHA `build-check-test` and `Check changelog fragment` checks from GitHub Actions app `15368`;
- the `pylon-preview` environment uses custom branch policies with exactly the protected `pylon` branch and requires reviewer `rynfar` (user id `11325514`) before its publisher job;
- the `pylon-stable` environment uses the same exact custom branch policy and reviewer;
- the solo-maintainer exception keeps `prevent_self_review: false`; this permits, but never skips, an explicit environment approval;
- the stable workflow's `pylon-stable-publication` concurrency group remains serialized with `cancel-in-progress: false`;
- active repository ruleset `21950766`, **Pylon immutable publication tags**, targets `refs/tags/pylon-build-*` and `refs/tags/pylon-stable-*`, permits new-tag creation, forbids update and deletion after creation, and has no bypass actors; and
- every action remains pinned to a full commit SHA.

Environment reviewers must inspect the tag, full source SHA and tree, exact current required checks, attestation job, and intended stable operation before approval. The tag ruleset deliberately protects permanence rather than restricting creation: GitHub rejected the global Actions app as a repository ruleset bypass actor, and a maintainer/owner bypass would weaken the boundary. Do not add repository secrets. The jobs use only the built-in `GITHUB_TOKEN`.

## Preview publication

`.github/workflows/pylon-preview-release.yml` runs only for an exact push to canonical `refs/heads/pylon`. Admission and final publication both require the live branch tip to equal the event SHA, so a stale rerun cannot publish.

The build uses Node `22.23.2` and npm `11.10.1`. It packs twice with dependency networking disabled and compares the results byte for byte. Linux, macOS, and Windows install the exact first pack with lifecycle scripts disabled. The preview identity is:

```text
pylon-build-g<source-sha-12>-r<recipe-revision>
```

The immutable prerelease contains exactly four tarballs plus:

```text
pylon-prime-agent-release-v1.json
pylon-preview-channel-v1.json
```

The channel manifest binds the full source commit and tree, recipe, build manifest digest, and all archive digests. All six files receive GitHub keyless SLSA provenance. A rerun is idempotent only when the existing tag, release metadata, immutable state, target, asset names, sizes, and SHA-256 digests are identical. A changed collision stops. An exact partial draft can resume; it never deletes or overwrites an asset. A `422` reservation race stops instead of choosing another tag.

The publisher checks out nothing and executes no repository or downloaded code. Only the attestation job receives `id-token: write` and `attestations: write`. Only the publisher receives `contents: write`.

## Stable promotion

Run **Actions → Pylon stable promotion → Run workflow** on `pylon` with:

- `operation=promote`;
- the immutable preview `preview_tag`; and
- empty withdrawal fields.

Promotion downloads the preview release by exact tag, rejects unexpected files and non-regular files, verifies every digest and canonical manifest byte, verifies all six attestations against the exact preview workflow identity, protected ref, source SHA, GitHub OIDC issuer, SLSA provenance type, and Rekor inclusion, then installs the same tarballs on Linux, macOS, and Windows. It never rebuilds.

Stable tags are monotonic:

```text
pylon-stable-<six-digit-sequence>-g<source-sha-12>-r<recipe-revision>
```

Each stable release contains only `pylon-stable-channel-v1.json`. The signed manifest binds the immutable preview, full artifact digests, protected promotion-policy commit and tree, previous stable tag and canonical manifest SHA-256, high-water mark, and cumulative sorted revocations. The next sequence must be contiguous. Before creating the release, the publisher atomically creates permanent N-only `pylon-stable-sequence-<six-digits>` reservation ref. The ref targets an annotated tag that binds the exact policy commit/tree, selected preview and stable tags, and proposed manifest SHA-256; the annotation targets the protected promotion-policy commit. Creating this N-only ref is the global compare-and-set: a missing, changed, duplicate, gapped, or raced reservation stops without selecting another sequence. Reservation refs are never releases and are excluded from stable release parsing. Reservation races, gaps, stale high-water state, changed previous digests, and duplicate promotion stop. The stable publisher rechecks the live protected tip, source reachability and tree, current exact-SHA required checks and their canonical workflow-run provenance, immutable preview identity, history, and sequence immediately before it creates the draft.

The stable publisher uses the same no-clobber draft, resume, publish-once, immutable-postcondition, and `422` stop rules as preview publication. A stable release tag targets the current protected promotion-policy commit, not the selected build commit. The manifest and readable `g<sha12>` tag segment bind the older preview source independently. This lets a current protected policy promote or withdraw an older verified build without a PAT or `workflows:write` permission. Stable releases do not become GitHub's mutable “latest” pointer.

## Withdrawal and revocation

Published assets are never deleted, replaced, or retagged. To withdraw a stable build, dispatch the stable workflow with:

- `operation=withdraw`;
- the immutable preview `preview_tag` for the new sequence;
- `revoke_stable_tag` set to the prior stable tag; and
- a concise non-empty `reason`.

This creates a later immutable stable sequence. Its signed manifest keeps every earlier revocation and appends the exact withdrawn stable/build tag pair, reason, and revoking sequence. Repeating a revocation fails. Consumers must reject any stable tag listed in the newest verified manifest and keep the historical release for audit and rollback decisions.

## Independent verification

Use a recent GitHub CLI with `gh release verify`, `gh release verify-asset`, and `gh attestation verify` support. Download into a new empty directory.

For a preview:

```sh
tag=pylon-build-g0123456789ab-r1
gh release download "$tag" --repo pylon-code/prime-agent --dir publication
npm run release:pylon:verify-preview -- --artifact-dir publication
gh release verify "$tag" --repo pylon-code/prime-agent
for asset in publication/*; do
  gh release verify-asset "$tag" "$asset" --repo pylon-code/prime-agent
done
source_sha="$(node -e "console.log(JSON.parse(require('node:fs').readFileSync('publication/pylon-preview-channel-v1.json')).build.source.commit)")"
source_tree="$(node -e "console.log(JSON.parse(require('node:fs').readFileSync('publication/pylon-preview-channel-v1.json')).build.source.tree)")"
npm run release:pylon:verify-attestations -- \
  --artifact-dir publication --source-sha "$source_sha" --source-tree "$source_tree"
```

`release:pylon:verify-attestations` requires the exact certificate identity
`https://github.com/pylon-code/prime-agent/.github/workflows/pylon-preview-release.yml@refs/heads/pylon`, exact signer/source digest, GitHub OIDC issuer, SLSA provenance predicate, non-self-hosted runner, one exact subject digest, and a Rekor timestamp for each of the six files.

For a stable sequence:

```sh
tag=pylon-stable-000001-g0123456789ab-r1
gh release download "$tag" --repo pylon-code/prime-agent --dir stable
gh release verify "$tag" --repo pylon-code/prime-agent
gh release verify-asset "$tag" stable/pylon-stable-channel-v1.json --repo pylon-code/prime-agent
policy_sha="$(node -e "console.log(JSON.parse(require('node:fs').readFileSync('stable/pylon-stable-channel-v1.json')).promotion.policyCommit)")"
policy_tree="$(node -e "console.log(JSON.parse(require('node:fs').readFileSync('stable/pylon-stable-channel-v1.json')).promotion.policyTree)")"
npm run release:pylon:verify-stable-attestation -- \
  --manifest stable/pylon-stable-channel-v1.json \
  --promotion-sha "$policy_sha" \
  --promotion-tree "$policy_tree"
```

For the full channel history, download every stable release into its own tag-named directory, verify each immutable release/asset and exact stable signer, then verify the canonical digest chain and append-only revocations:

```sh
rm -rf stable-history
mkdir stable-history
gh api --paginate repos/pylon-code/prime-agent/releases \
  --jq '.[] | select(.draft == false and (.tag_name | startswith("pylon-stable-"))) | .tag_name' | sort >stable-tags
while IFS= read -r tag; do
  test -n "$tag"
  printf '%s\n' "$tag" | grep -Eq '^pylon-stable-[0-9]{6}-g[0-9a-f]{12}-r[1-9][0-9]*$'
  dir="stable-history/$tag"
  mkdir "$dir"
  gh release download "$tag" --repo pylon-code/prime-agent --dir "$dir"
  gh release verify "$tag" --repo pylon-code/prime-agent
  gh release verify-asset "$tag" "$dir/pylon-stable-channel-v1.json" --repo pylon-code/prime-agent
  policy_sha="$(node -e "console.log(JSON.parse(require('node:fs').readFileSync(process.argv[1])).promotion.policyCommit)" "$dir/pylon-stable-channel-v1.json")"
  policy_tree="$(node -e "console.log(JSON.parse(require('node:fs').readFileSync(process.argv[1])).promotion.policyTree)" "$dir/pylon-stable-channel-v1.json")"
  npm run release:pylon:verify-stable-attestation -- \
    --manifest "$dir/pylon-stable-channel-v1.json" \
    --promotion-sha "$policy_sha" \
    --promotion-tree "$policy_tree"
done <stable-tags
npm run release:pylon:verify-stable-history -- \
  stable-history/pylon-stable-*/pylon-stable-channel-v1.json
```

The history verifier rejects noncanonical bytes, skipped/duplicate sequences, wrong previous `{tag, sha256}` linkage, nested schema changes, a revocation bound to the wrong build, removed history, and more than one newly declared withdrawal. Never select a build through GitHub's “latest” release pointer.

## Failure handling

- **Environment is absent or approval is not expected:** stop. Configure governance outside the workflow; do not remove `environment:`.
- **Required check or merged changelog proof is missing:** fix the protected PR/check path and create a new protected merge. Never synthesize a success status.
- **Preview or stable tag collides:** compare the full immutable release and asset digests. Exact state is a successful replay; anything else requires investigation, not deletion.
- **Partial draft exists:** rerun only after confirming its metadata and uploaded subset are exact. The workflow resumes only an identical draft.
- **Sequence race or `422`:** inspect the permanent N-only reservation and the run that owns its exact policy commit. Let that operation finish, then dispatch again. Never delete, move, skip, or reuse a reservation or release sequence.
- **Attestation or Rekor verification fails:** do not promote, install, or approve publication.
- **Withdrawal is needed:** publish a later revocation sequence. Never delete the prior release, tag, or attestation.

Run offline policy tests with `npm run test:pylon-publication`. They require no network credentials and cover canonicalization, tag grammars, exact source/check/workflow identity, immutable replay, provenance tamper cases, artifact transport provenance, append-only history, workflow permissions, action pins, and publisher no-source-execution policy.
