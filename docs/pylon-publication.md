# Protected Pylon publication

Pylon publishes Prime Agent in two steps. A protected `pylon` push can create one immutable preview. A maintainer can later promote those exact bytes to the append-only stable channel or publish a later withdrawal. Promotion never rebuilds or executes old repository source.

## Administrative prerequisites

Publication fails closed unless all of these controls exist:

- the canonical repository is `pylon-code/prime-agent`, with immutable GitHub Releases enabled;
- `refs/heads/pylon` requires strict exact-SHA `build-check-test` and `Check changelog fragment` checks from GitHub Actions app `15368`;
- `pylon-preview` and `pylon-stable` use custom deployment branches with only `pylon`, require reviewer `rynfar` (user id `11325514`), set `prevent_self_review: false`, and set `can_admins_bypass: false`;
- `pylon-upstream-sync` has the same sole custom `pylon` branch, reviewer, `prevent_self_review: false`, and `can_admins_bypass: false` policy before the scheduled sync workflow is enabled;
- the stable workflow keeps `pylon-stable-publication` serialized with `cancel-in-progress: false`;
- active no-bypass repository ruleset `21950766`, **Pylon immutable publication tags**, targets `refs/tags/pylon-build-*` and `refs/tags/pylon-stable-*`, permits creation, and forbids every update and deletion; and
- repository action policy requires full commit-SHA pins.

Before enabling any writer, read back all three environment protection-rule responses. Each must show `can_admins_bypass: false`, reviewer `rynfar`, `prevent_self_review: false`, and exactly one custom deployment branch named `pylon`. Treat a missing, extra, or different value as a publication blocker.

The normal preview and stable attester jobs carry `pylon-preview` and `pylon-stable` directly. Approval therefore occurs before OIDC signing. Read-only verification follows. Every contents writer is downstream of that verified attestation. An explicit stable recovery creates no new attestation, so its mutually exclusive zero-write `authorize-stable-resume` job carries `pylon-stable` instead. The upstream-sync contents writer carries `pylon-upstream-sync` directly. Each path asks for one approval.

The jobs use only `GITHUB_TOKEN`. Do not add npm, R2, PAT, app, or repository secrets. Upstream sync checks out exactly `${{ github.sha }}` and, in the same shell that executes repository code, proves the canonical repository/event/ref, exact `HEAD`, workspace, and immediate live `pylon` SHA; a stale approved run stops before the sync script.

## Preview publication

`.github/workflows/pylon-preview-release.yml` runs only for an exact canonical push to `refs/heads/pylon`. It uses Node `22.23.2` and npm `11.10.1`, packs twice with build networking disabled, compares all subjects byte for byte, and installs the first pack on Ubuntu Linux and macOS. Ubuntu is the supported gate for Linux and WSL2; native Windows publication support is deferred.

The preview identity is:

```text
pylon-build-g<source-sha-12>-r<recipe-revision>
```

Its immutable prerelease contains four tarballs plus:

```text
pylon-prime-agent-release-v1.json
pylon-preview-channel-v1.json
```

The canonical preview manifest binds the full source commit/tree, artifact recipe, build-manifest digest, archive digests, exact preview signer policy, and this monotonic channel identity:

```json
{
  "publicationPolicyRevision": 1,
  "sequenceEpoch": 1,
  "sequence": 123,
  "workflowRunId": "33428882721"
}
```

`sequence` is the positive safe integer `github.run_number` for the one preview workflow. `workflowRunId` is its exact positive decimal run id. Failed runs create gaps, so consumers allow a higher non-adjacent sequence. A workflow sequence reset requires a new signed epoch/schema and consumer migration; it must never silently reuse epoch 1. Ordering never comes from a commit abbreviation, SemVer, a timestamp, the GitHub “latest” pointer, or a tag sort.

`runAttempt` is deliberately not in manifest bytes. A rerun keeps the same run id, run number, manifest, and build-tag identity. The verified SLSA workflow/v1 predicate supplies the actual `/runs/<id>/attempts/<attempt>` invocation. Verification requires its signed run id to equal `workflowRunId`, then reads that exact immutable attempt endpoint and its attempt-specific jobs. It proves the run number, repository id, workflow path/ref, push event, source SHA/branch, GitHub Actions check-suite app, and successful directly environment-gated attester job. The aggregate `/runs/<id>` view is mutable across reruns and is not an attestation trust root; a later failed rerun cannot invalidate an earlier exact signed and published attempt.

The approved attester signs exactly six subjects with pinned `actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8`, whose reviewed pinned chain delegates to `actions/attest@508db95dd578ae2727ebd6217d5ba78e4fbda05d`. A read-only job verifies the exact subject set, SLSA v1 workflow predicate, GitHub OIDC issuer, signer digest/ref, public Rekor entry, and run invocation. Only then can checkout-free contents jobs fully stage and publish the exact draft.

A publisher re-reads live `pylon` before its first mutation and again at the final tag/publication boundary. It creates or refetches the exact lightweight preview tag and requires the full source commit before making the release immutable. GitHub does not offer an atomic transaction across branch reads, tag creation, and release publication. Each read and compare-and-set is a separate fail-closed point-in-time check; this design does not claim cross-resource atomicity.

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

Use `--initialize` only after manually inspecting the first full verified receipt. Omit it thereafter. The canonical JSON at `--state` remains the CLI-compatible projection. The adjacent private `.journal` directory is the concurrency authority. Its authenticated checkpoint names one current epoch, anchors the exact prior immutable tip, and carries that tip's bounded canonical state bytes. Within the epoch, base-digest transition links and random-token claims are immutable no-replace records. Token-specific 10-second heartbeats yield to one permanent `released`, `retired`, `commit`, or `rotate` decision. A stale 30-second claim is retired; a complete commit or rotation is helpable after every crash point. The verifier rejects gaps, cycles, unreachable records, orphan markers, unexpected hidden entries, and excess record, depth, or byte work. It repairs a missing or stale JSON projection from the journal tip.

`${state}.lock` is not the current journal namespace. It is a permanent exact regular-file downgrade guard for clients that used `proper-lockfile`. Current tooling publishes it as a complete `0600` file by fsyncing a named owned temporary, hard-linking it no-replace, and fsyncing the parent. An old client's atomic lock-directory `mkdir` and this link cannot both win. Once the guard wins, old clients remain blocked. Any observed directory at that path is treated as a live or ambiguous legacy lease and fails closed. Stop all old clients, confirm no owner remains, and remove that directory manually before retrying; current tooling never enters, steals, or reuses it.

Rotate before an epoch reaches 3,800 transitions or 60,000 claims:

```sh
npm run release:pylon:rotate-consumer-journal -- \
  --state "$HOME/.local/state/pylon-prime/preview-high-water.json"
```

Rotation takes the exact current claim authority, commits an immutable helpable `rotate` decision, anchors the exact old tip in a new checkpoint epoch, and fences paused old writers. The current projection and high-water JSON schema do not change. The final claim capacity remains reserved for this operation, and rotation remains available at the transaction-depth limit. After the new epoch is durable, a new fenced owner removes only the authenticated retired epoch and predecessor checkpoint, so active fencing data, directory entries, scan depth, and bytes remain bounded.

These pathname checks are not a portable `openat` security sandbox. The verifier rejects observed symlinks and non-directories, pins every read to a no-follow file descriptor where Node exposes it, bounds bytes before allocation, and re-stats after an exact read. On POSIX, every relied-on state, guard, journal, claim, marker, and transition entry must have the current uid and no group/world write bit; current-owner entries are safely tightened before use, while foreign-owner entries fail. Created directories are `0700` and files are `0600`. Windows enforces the regular-file, no-follow-where-available, and bounded-read contract without POSIX uid/mode checks. The state parent remains a trusted user-owned local directory with no hostile mutation by the same OS user. Every immutable link, projection rename, journal handoff, and relied-on parent entry is fsynced before success.

## Stable promotion

Run **Actions → Pylon stable promotion → Run workflow** on `pylon` with `operation=promote`, an immutable `preview_tag`, and no recovery or withdrawal identity.

Current policy can promote older artifacts only when `scripts/pylon-prime-supported-release-recipes-v1.json` closes two independent immutable identity sets. A `recipeRevision` entry contains only the build manifest schema and Node/npm/minimum-Node tuple. A `publicationPolicyRevision` entry contains the exact preview/stable workflow paths and SHA-256 of both workflow byte strings. Preview manifests bind the preview policy revision that signed them. Stable manifests preserve that preview policy revision beside the build recipe and record the current stable policy revision under `promotion`.

The verifier selects preview workflow bytes by the preview manifest's policy revision and stable workflow bytes by `promotion.publicationPolicyRevision`. A future stable policy revision can therefore promote historical recipe/policy-r1 preview bytes without rewriting r1. Unknown, duplicate, nonpositive, or extra registry identities fail closed. A publication workflow edit requires a new immutable publication policy revision and reviewed digests; it does not by itself require an artifact recipe revision. Bump the recipe only when the artifact recipe identity changes. Never rewrite either historical entry. The Ubuntu Linux/macOS install uses current protected verifier source; it never checks out or executes the older source. The preview tag recipe must equal the build recipe copied into stable.

Normal stable transaction order is strict:

1. Download the immutable preview. Verify six exact bytes, its signed workflow/run sequence, public Rekor evidence, source/tree, old preview workflow policy, current ancestry, and original/current exact-SHA checks.
2. Install those same bytes on Ubuntu Linux and macOS.
3. Read and validate the complete immutable stable release/tag digest chain. Before extending a nonempty chain, verify the latest singleton stable manifest against the exact stable workflow/ref, signer policy commit/tree, SLSA v1, public Rekor, and the workflow's directly gated static policy at that signer digest.
4. Prepare one canonical next manifest. The directly `pylon-stable`-gated attester signs that singleton. A separate read-only job verifies it.
5. A checkout-free contents writer creates or resumes one exact draft. Creation durably places the exact canonical manifest bytes, byte count, and SHA-256 in the bounded release-body recovery envelope before asset upload. It uploads and re-downloads/re-hashes the singleton.
6. The final checkout-free publisher re-downloads the draft from GitHub Releases, not an old Actions artifact. For a zero-asset crash draft, it recovers only the exact body-carried attested bytes, uploads the missing singleton once, and re-downloads/re-hashes it before any CAS. It rechecks the live current tip/checks, old policy tree/ancestry/checks, immutable preview, recipe, N-1 history, operation fields, and draft id/digest.
7. It first creates annotated `pylon-stable-sequence-NNNNNN` as the sequence compare-and-set. It then creates or refetches the exact lightweight stable tag at the full policy commit. Only after both exact refs exist does it make that draft immutable and check postconditions.

The reservation annotation binds sequence, policy commit/tree, the exact promote/withdraw tuple and reason, stable and preview tags, stable-manifest SHA-256, and draft release id. A reservation `422` refetches and stops for explicit recovery. Final tag `422` handling refetches and accepts only the exact lightweight full-commit target; a wrong or annotated object fails before immutable publication. No path selects N+1, moves, deletes, or reuses a ref. The reservation freezes the approved old policy tuple if `pylon` advances later. Reservation CAS, final tag CAS, and release publication are ordered GitHub operations, not one atomic GitHub transaction.

Stable tags remain:

```text
pylon-stable-<six-digit-sequence>-g<source-sha-12>-r<recipe-revision>
```

The signed stable manifest copies the preview sequence epoch/number/run id, full preview identity/digests, artifact recipe and preview publication policy revision. Its `promotion` record adds the current stable publication policy revision with the policy commit/tree, exact previous stable tag/digest, high-water, operation, and cumulative sorted revocations.

## Explicit stable recovery

A crash can leave any of these exact recoverable states:

- a zero-asset draft whose bounded body already carries the attested canonical manifest;
- a complete approved draft before CAS;
- the complete draft plus its permanent sequence reservation; or
- both the reservation and exact final lightweight tag before release publication.

Start a fresh run on current `pylon` with `operation=resume-promote` or `resume-withdraw`, the original `preview_tag`, withdrawal fields, and `resume_identity` set to the numeric draft release id, stable draft tag, or exact reservation tag. The run recovers manifest bytes from the exact draft body and requires any existing singleton to match byte for byte. The canonical stable manifest is limited to 48 KiB and the complete encoded release body to 80 KiB. Altered, truncated, oversized, or ambiguous envelopes/assets fail. It never relies on an old Actions artifact.

Recovery requires the exact operator tuple; exact body-carried bytes and any present draft asset; old stable attestation and exact workflow-byte approval policy; exact draft/annotation/digest; every signed N-1 history receipt; immutable old preview and preview attestation/run sequence; original source/policy checks; fresh current checks; old policy exact tree and ancestry; current Ubuntu Linux/macOS install; and one fresh `pylon-stable` approval. History reading excludes exactly the selected recovery draft id and rejects every other draft. Recovery uploads and re-hashes a missing singleton before reservation/final-tag CAS. It does not reprepare or reattest.

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

Use `--initialize` once, then omit it. The CLI requires the complete contiguous canonical manifest chain, regular non-symlink inputs, and explicit local state. It parses and hashes all manifests before acquiring the same tokenized lock and immutable base-digest transaction journal described for preview state. The canonical JSON state path is a repairable projection of that journal tip. The CLI rejects malformed authoritative state, a lower valid prefix, and any rewrite at or below the witnessed sequence. It commits only a monotonic journal advance and repairs the projection before success.

## Failure and incident handling

- **Approval is absent:** configure the exact environment. Never remove or bypass `environment:`.
- **Required proof is missing:** fix branch protection/check provenance and create a new protected merge. Never synthesize status.
- **Draft differs:** stop. Do not delete an asset or rebuild/reprepare around it. Exact recovery accepts only the bounded body-carried manifest and an absent or identical singleton; every other difference fails.
- **Reservation race or `422`:** inspect the ref and owning run. Resume only the exact tuple. Never choose N+1, move, or delete.
- **Attestation/Rekor/workflow policy fails:** do not approve, promote, install, or advance consumer state.
- **Release is immutable:** workflows never delete it. A withdrawal is a later sequence.
- **Invalid tag squat:** publication stays blocked. Record an incident and export the active ruleset plus tag/release/Actions audit evidence. A repository administrator must make one reviewed temporary ruleset change that permits deleting only the named invalid ref, delete it by exact ref/object identity, and immediately restore/read back ruleset `21950766` with the original targets, no bypass actors, update/deletion blocks, and `current_user_can_bypass: never`. Never let publication automation perform this recovery.
- **Invalid immutable release:** preserve evidence first. GitHub may require an administrator to temporarily disable immutable releases before exact-id deletion. Delete only the proven invalid release, restore/read back immutable releases immediately, and link every API response in the incident. Never alter a valid published sequence.

Run offline policy tests with `npm run test:pylon-publication`. They cover exact current/historical workflow digests and registry closure, immutable signed attempt evidence, zero-asset crash recovery, deterministic stale recovery, active heartbeats, transaction crash convergence, path-boundary checks, exact required-check paths/apps, preview/stable tag squats and CAS order, withdrawal tuples, rollback state, approval DAGs, every contents writer, pinned actions, and no source/download execution in publication writers.
