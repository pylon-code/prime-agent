# Protected Pylon publication

Pylon publishes Prime Agent in two steps. A protected `pylon` push can create one immutable preview. A maintainer can later promote those exact bytes to the append-only stable channel or publish a later withdrawal. Promotion never rebuilds or executes old repository source.

## Administrative prerequisites

Publication fails closed unless all of these controls exist:

- the canonical repository is `pylon-code/prime-agent`, with immutable GitHub Releases enabled;
- `refs/heads/pylon` requires strict exact-SHA `build-check-test` and `Check changelog fragment` checks from GitHub Actions app `15368`;
- `pylon-preview` and `pylon-stable` use custom deployment branches with only `pylon`, require reviewer `rynfar` (user id `11325514`), and keep the documented solo-maintainer `prevent_self_review: false` exception;
- `pylon-upstream-sync` has the same reviewer and branch restriction before the scheduled sync workflow is enabled;
- the stable workflow keeps `pylon-stable-publication` serialized with `cancel-in-progress: false`;
- active no-bypass repository ruleset `21950766`, **Pylon immutable publication tags**, targets `refs/tags/pylon-build-*` and `refs/tags/pylon-stable-*`, permits creation, and forbids every update and deletion; and
- repository action policy requires full commit-SHA pins.

The normal preview and stable attester jobs carry `pylon-preview` and `pylon-stable` directly. Approval therefore occurs before OIDC signing. Read-only verification follows. Every contents writer is downstream of that verified attestation. An explicit stable recovery creates no new attestation, so its mutually exclusive zero-write `authorize-stable-resume` job carries `pylon-stable` instead. The upstream-sync contents writer carries `pylon-upstream-sync` directly. Each path asks for one approval.

The jobs use only `GITHUB_TOKEN`. Do not add npm, R2, PAT, app, or repository secrets.

## Preview publication

`.github/workflows/pylon-preview-release.yml` runs only for an exact canonical push to `refs/heads/pylon`. It uses Node `22.23.2` and npm `11.10.1`, packs twice with build networking disabled, compares all subjects byte for byte, and installs the first pack on Linux, macOS, and Windows.

The preview identity is:

```text
pylon-build-g<source-sha-12>-r<recipe-revision>
```

Its immutable prerelease contains four tarballs plus:

```text
pylon-prime-agent-release-v1.json
pylon-preview-channel-v1.json
```

The canonical preview manifest binds the full source commit/tree, recipe, build-manifest digest, archive digests, and this monotonic channel identity:

```json
{
  "sequenceEpoch": 1,
  "sequence": 123,
  "workflowRunId": "33428882721"
}
```

`sequence` is the positive safe integer `github.run_number` for the one preview workflow. `workflowRunId` is its exact positive decimal run id. Failed runs create gaps, so consumers allow a higher non-adjacent sequence. A workflow sequence reset requires a new signed epoch/schema and consumer migration; it must never silently reuse epoch 1. Ordering never comes from a commit abbreviation, SemVer, a timestamp, the GitHub “latest” pointer, or a tag sort.

`runAttempt` is deliberately not in manifest bytes. A rerun keeps the same run id, run number, manifest, and build-tag identity. The verified SLSA workflow/v1 predicate supplies the actual `/runs/<id>/attempts/<attempt>` invocation. Verification requires its signed run id to equal `workflowRunId`, then reads that immutable Actions run and proves the exact run number, repository id, workflow path/ref, push event, source SHA/branch, GitHub Actions check-suite app, and successful directly environment-gated attester job for the signed attempt.

The approved attester signs exactly six subjects with pinned `actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8`, whose reviewed pinned chain delegates to `actions/attest@508db95dd578ae2727ebd6217d5ba78e4fbda05d`. A read-only job verifies the exact subject set, SLSA v1 workflow predicate, GitHub OIDC issuer, signer digest/ref, public Rekor entry, and run invocation. Only then can checkout-free contents jobs fully stage and publish the exact draft.

A publisher re-reads live `pylon` immediately before its first release mutation and again immediately before publication. GitHub has no conditional transaction across a branch and release. These reads give point-in-time authorization: a later push does not revoke the exact already-authorized draft.

## Preview consumer high-water

Download one preview into a new directory. The integrated verifier checks all bytes, six attestations, signer workflow at the signer digest, signed run invocation, Actions run number, and then atomically advances explicit consumer-local state:

```sh
tag=pylon-build-g0123456789ab-r1
mkdir publication
gh release download "$tag" --repo pylon-code/prime-agent --dir publication
GH_TOKEN="$(gh auth token)" npm run release:pylon:verify-preview-history -- \
  --historical \
  --artifact-dir publication \
  --state "$HOME/.local/state/pylon-prime/preview-high-water.json" \
  --initialize
```

Use `--initialize` only after manually inspecting the first full verified receipt. Omit it thereafter. The state and adjacent exclusive lock must be local regular non-symlink entries. The write is file-fsync, atomic rename, then directory-fsync. Lower sequences and the same sequence with a different tag, run id, or manifest digest fail as rollback/equivocation. Higher gaps are valid.

## Stable promotion

Run **Actions → Pylon stable promotion → Run workflow** on `pylon` with `operation=promote`, an immutable `preview_tag`, and no recovery or withdrawal identity.

Current policy can promote an older recipe only when `scripts/pylon-prime-supported-release-recipes-v1.json` lists its exact closed manifest schema and Node/npm/minimum-Node tuple. The current verifier validates that historical receipt and its exact old preview attestation/workflow. The three-OS install uses current protected verifier source; it never checks out or executes the older source. The preview tag recipe must equal the build recipe copied into stable.

Normal stable transaction order is strict:

1. Download the immutable preview. Verify six exact bytes, its signed workflow/run sequence, public Rekor evidence, source/tree, old preview workflow policy, current ancestry, and original/current exact-SHA checks.
2. Install those same bytes on Linux, macOS, and Windows.
3. Read and validate the complete immutable stable release/tag digest chain. Before extending a nonempty chain, verify the latest singleton stable manifest against the exact stable workflow/ref, signer policy commit/tree, SLSA v1, public Rekor, and the workflow's directly gated static policy at that signer digest.
4. Prepare one canonical next manifest. The directly `pylon-stable`-gated attester signs that singleton. A separate read-only job verifies it.
5. A checkout-free contents writer creates or resumes one exact draft, fully uploads it, and re-downloads/re-hashes the draft asset before CAS.
6. The final checkout-free publisher re-downloads the draft from GitHub Releases, not an Actions artifact. It rechecks the live current tip/checks, old policy tree/ancestry/checks, immutable preview, recipe, N-1 history, operation fields, and draft id/digest.
7. It creates annotated `pylon-stable-sequence-NNNNNN` as the sole sequence compare-and-set, then only publishes that exact draft and checks immutable postconditions.

The reservation annotation binds sequence, policy commit/tree, promote/withdraw fields, stable and preview tags, stable-manifest SHA-256, and draft release id. A `422` refetches state and stops. No path selects N+1, moves, deletes, or reuses a ref. The live `pylon` read immediately before `createRef` has no fallible build/upload work between it and CAS. The reservation freezes the approved old policy tuple if `pylon` advances later.

Stable tags remain:

```text
pylon-stable-<six-digit-sequence>-g<source-sha-12>-r<recipe-revision>
```

The signed stable manifest copies the preview sequence epoch/number/run id, full preview identity/digests, current policy commit/tree, exact previous stable tag/digest, high-water, operation, and cumulative sorted revocations.

## Explicit stable recovery

A crash can leave either:

- a complete approved draft before CAS; or
- the exact complete draft plus its permanent CAS reservation before release publication.

Start a fresh run on current `pylon` with `operation=resume-promote` or `resume-withdraw`, the original `preview_tag`, withdrawal fields, and `resume_identity` set to the numeric draft release id, stable draft tag, or exact reservation tag. The run discovers the manifest bytes from that exact GitHub draft/release. It never relies on an old Actions artifact.

Recovery requires the exact operator tuple; complete draft asset; old stable attestation and static approval policy; exact draft/annotation/digest; every signed N-1 history receipt; immutable old preview and preview attestation/run sequence; original source/policy checks; fresh current checks; old policy exact tree and ancestry; current three-OS install; and one fresh `pylon-stable` approval. It does not reprepare or reattest.

Draft-only recovery can create the still-free N reservation. Reservation recovery can finalize only the exact already-reserved tuple. An unexpected reservation, draft, tag, asset, sequence, annotation, signer, or digest fails closed.

## Withdrawal

Use `operation=withdraw`, a preview for the new sequence, the exact prior `revoke_stable_tag`, and a lowercase reason code. This appends one signed revocation bound to the old stable/build tags. Repeat withdrawal fails. Never delete, replace, or retag withdrawn history.

## Stable consumer high-water

Verify each immutable release and stable attestation first. Then give the verifier every canonical stable manifest from sequence 1 through current and explicit local state:

```sh
npm run release:pylon:verify-stable-history -- \
  --state "$HOME/.local/state/pylon-prime/stable-high-water.json" \
  --initialize \
  stable-history/pylon-stable-*/pylon-stable-channel-v1.json
```

Use `--initialize` once, then omit it. The CLI requires the complete contiguous canonical chain, an adjacent exclusive lock, regular non-symlink manifests/state, and explicit local state. It rejects malformed state, a lower valid prefix, and any rewrite at or below the witnessed sequence. It atomically file-fsyncs, renames, and directory-fsyncs only a monotonic advance.

## Failure and incident handling

- **Approval is absent:** configure the exact environment. Never remove or bypass `environment:`.
- **Required proof is missing:** fix branch protection/check provenance and create a new protected merge. Never synthesize status.
- **Draft differs:** stop. Do not delete an asset or rebuild/reprepare around it. Use exact recovery only for a complete matching transaction.
- **Reservation race or `422`:** inspect the ref and owning run. Resume only the exact tuple. Never choose N+1, move, or delete.
- **Attestation/Rekor/workflow policy fails:** do not approve, promote, install, or advance consumer state.
- **Release is immutable:** workflows never delete it. A withdrawal is a later sequence.
- **Invalid tag squat:** publication stays blocked. Record an incident and export the active ruleset plus tag/release/Actions audit evidence. A repository administrator must make one reviewed temporary ruleset change that permits deleting only the named invalid ref, delete it by exact ref/object identity, and immediately restore/read back ruleset `21950766` with the original targets, no bypass actors, update/deletion blocks, and `current_user_can_bypass: never`. Never let publication automation perform this recovery.
- **Invalid immutable release:** preserve evidence first. GitHub may require an administrator to temporarily disable immutable releases before exact-id deletion. Delete only the proven invalid release, restore/read back immutable releases immediately, and link every API response in the incident. Never alter a valid published sequence.

Run offline policy tests with `npm run test:pylon-publication`. They cover closed current/historical recipes, run-sequence and rerun identity, signed invocation evidence, preview/stable rollback state, exact approval DAGs, every contents writer, pinned actions, draft-before-CAS order, reservation recovery, and no source/download execution in publication writers.
