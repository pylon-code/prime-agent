# Protected Pylon publication

Pylon publishes Prime Agent in two steps. A protected `pylon` push can create one immutable preview. A maintainer can later promote those exact bytes to the append-only stable channel or publish a later withdrawal. Promotion never rebuilds or executes old repository source.

## Administrative prerequisites

Publication fails closed unless all of these controls exist:

- the canonical repository is `pylon-code/prime-agent`, with immutable GitHub Releases enabled;
- `refs/heads/pylon` requires strict exact-SHA `build-check-test` and `Check changelog fragment` checks from GitHub Actions app `15368`;
- `pylon-preview` and `pylon-stable` use custom deployment branches with only `pylon`, require reviewer `rynfar` (user id `11325514`), set `prevent_self_review: false`, and set `can_admins_bypass: false`;
- `pylon-upstream-sync` has the same sole custom `pylon` branch, reviewer, `prevent_self_review: false`, and `can_admins_bypass: false` policy before the scheduled sync workflow is enabled;
- the stable workflow keeps `pylon-stable-publication` serialized with `cancel-in-progress: false`;
- active no-bypass repository ruleset `21950766`, **Pylon immutable publication tags**, targets exactly `refs/tags/pylon-build-*` and `refs/tags/pylon-stable-*` with no excludes, has GraphQL `bypassActors.totalCount: 0`, permits creation, and forbids every update and deletion; and
- repository action policy requires full commit-SHA pins.

Before enabling any writer, read back all three environment protection-rule responses. Each must show `can_admins_bypass: false`, reviewer `rynfar`, `prevent_self_review: false`, and exactly one custom deployment branch named `pylon`. Treat a missing, extra, or different value as a publication blocker.

The normal preview and stable attester jobs carry `pylon-preview` and `pylon-stable` directly. Approval therefore occurs before OIDC signing. Read-only verification follows. Every contents writer remains directly downstream of verified attestation or of the mutually exclusive approved recovery path. Every job that creates a protected publication ref or makes a release immutable also carries its channel environment directly. An explicit stable recovery creates no new attestation, so its zero-write `authorize-stable-resume` job carries `pylon-stable`, and the final publisher carries it again. The upstream-sync contents writer carries `pylon-upstream-sync` directly.

Environment approval applies per deployment job, not once per workflow. A preview run can therefore ask for approval for attestation, preview-tag staging, and final immutable publication. A normal stable run can ask at attestation and final publication. A recovery run can ask at recovery authorization and final publication. Do not remove a later gate because an earlier job used the same environment. GitHub can group pending deployments in one approval screen, but operators must review every named job before approving it.

Publication uses `GITHUB_TOKEN` for the existing minimum contents/checks/actions operations. It additionally uses one read-only GitHub App installation token only in dedicated inline ruleset-audit steps. Each audit performs a REST shape check and then a final authoritative GraphQL read with that same token. The App token never enters a contents mutation, checkout, downloaded artifact, shell, repository script, environment variable, or workflow output. The pinned mint action masks the token and revokes it in its post step. Do not set `skip-token-revoke`. Do not add npm, R2, PAT, or other repository secrets. The checkout-free publishers execute only frozen inline code; they never execute source from the repository or a downloaded artifact. Upstream sync does not use the auditor App. It checks out exactly `${{ github.sha }}` and, in the same shell that executes repository code, proves the canonical repository/event/ref, exact `HEAD`, workspace, and immediate live `pylon` SHA; a stale approved run stops before the sync script.

### Ruleset-auditor GitHub App

Create a dedicated GitHub App for publication ruleset readback:

1. Grant only repository **Administration: read**. GitHub adds unavoidable **Metadata: read**. Grant no other permission and no write permission.
2. Install it on `pylon-code` for **Only select repositories**, with only repository id `1349002285`, `pylon-code/prime-agent`, selected. Do not install it organization-wide. The installation must not be suspended.
3. Generate a private key. Store the App id only as protected-environment variable `PYLON_RULESET_AUDITOR_APP_ID` and the PEM only as protected-environment secret `PYLON_RULESET_AUDITOR_PRIVATE_KEY` in both `pylon-preview` and `pylon-stable`. Do not create repository-level, organization-level, file-based, output-based, or job-environment fallbacks.
4. Keep the pinned `actions/create-github-app-token@fee1f7d63c2ff003460e3d139729b119787bc349` inputs closed to `owner: pylon-code`, `repositories: prime-agent`, and `permission-administration: read`. Its inspected v2 bundle calls `core.setSecret` before exposing the token and revokes it with `DELETE /installation/token` in the post action. Do not add `github-api-url`, another permission, or another input.

An Administration-read App can receive a REST ruleset response that omits `bypass_actors` and `current_user_can_bypass`. That omission is expected redaction, not evidence of an empty bypass list. The combined validator still requires REST status 200; exact numeric and node ids; name, repository source and type; active tag target; exact include and exclude conditions; and exactly one update rule with `update_allows_fetch_and_merge: false` plus one deletion rule. It does not require the two REST bypass fields. If either field is present, only `bypass_actors: []` and `current_user_can_bypass: never` are safe; any other present value fails.

The last authoritative read before each separate `GITHUB_TOKEN` mutation is the exact GraphQL `repository.ruleset(databaseId: 21950766, includeParents: false)` query with the same downscoped App token. It binds outer repository id `R_kgDOUGgkLQ`, database id `1349002285`, and `pylon-code/prime-agent`; ruleset node id `RRS_lACqUmVwb3NpdG9yec5QaCQtzgFO8S4`, database id, name, active tag target, and repository source; exact ref conditions with every other condition target null; `bypassActors.totalCount` as integer zero; and exactly the `UPDATE`/`UpdateParameters(updateAllowsFetchAndMerge: false)` and `DELETION`/null-parameters nodes. A GraphQL error, null or partial object, redacted bypass connection or count, nonzero count, unexpected id, condition, or rule blocks publication. Octokit turns GraphQL `errors` into a thrown audit failure.

### Mandatory live App acceptance

Before enabling either publication writer, and after any App key, installation, permission, repository selection, or ruleset change, run the maintainer-only acceptance CLI from a trusted checkout. This is mandatory. It never changes repository or ruleset state, and it never runs inside a publisher. It accepts a private-key **path**, reads bounded key bytes only to sign a bounded local JWT, never accepts the PEM value as an argument, and never prints the JWT, installation tokens, or key. Keep the key file outside the repository. Do not use `cat`, command substitution, `gh auth token`, a PAT, or a user token.

Configure a different known public ruleset whose GraphQL bypass aggregate is known to be nonzero. The canary proves that this App token does not turn a visible nonzero aggregate into zero. The command below prints only a non-secret acceptance summary:

```sh
export PYLON_RULESET_AUDITOR_APP_ID='<app-id>'
export PYLON_RULESET_AUDITOR_PRIVATE_KEY_PATH='/secure/path/to/app-private-key.pem'
export PYLON_RULESET_AUDITOR_APP_SLUG='<exact-app-slug>'
export PYLON_RULESET_CANARY_OWNER='<public-owner>'
export PYLON_RULESET_CANARY_REPO='<public-repository>'
export PYLON_RULESET_CANARY_RULESET_ID='<known-nonzero-ruleset-id>'

node scripts/accept-pylon-ruleset-auditor-app.mjs \
  --app-id "$PYLON_RULESET_AUDITOR_APP_ID" \
  --private-key-path "$PYLON_RULESET_AUDITOR_PRIVATE_KEY_PATH" \
  --app-slug "$PYLON_RULESET_AUDITOR_APP_SLUG" \
  --canary-owner "$PYLON_RULESET_CANARY_OWNER" \
  --canary-repo "$PYLON_RULESET_CANARY_REPO" \
  --canary-ruleset-id "$PYLON_RULESET_CANARY_RULESET_ID"

unset PYLON_RULESET_AUDITOR_APP_ID PYLON_RULESET_AUDITOR_PRIVATE_KEY_PATH PYLON_RULESET_AUDITOR_APP_SLUG
unset PYLON_RULESET_CANARY_OWNER PYLON_RULESET_CANARY_REPO PYLON_RULESET_CANARY_RULESET_ID
```

The CLI verifies `GET /app` id, slug, and exact read-only permissions; the exact unsuspended selected-repository `pylon-code` installation; and the exact singleton installation repository. It mints and revokes a full-installation Administration-read token to paginate that singleton, then mints an exact `prime-agent` runtime token and inspects its returned repository and permission scope. With the runtime token it runs the combined target validator and then the nonzero canary. It revokes each minted token even when later validation fails. Any endpoint, scope, identity, pagination, revocation, GraphQL, target, or canary mismatch fails closed. In particular, a GraphQL null/error or canary count zero is not acceptance.

A missing protected-environment variable, omitted secret, unavailable installation, token-mint failure, endpoint/auth failure, target or canary redaction, or token-revocation setup change blocks mutation. Do not run this acceptance CLI from a publisher. Publishers retain the pinned no-checkout, no-source-execution design.

## Preview publication

`.github/workflows/pylon-preview-release.yml` runs only for an exact canonical push to `refs/heads/pylon`. It uses Node `22.23.2` and npm `11.10.1`, packs twice with build networking disabled, compares all subjects byte for byte, and installs the first pack on Ubuntu Linux and macOS. Ubuntu is the supported gate for Linux and WSL2; native Windows publication support is deferred.

Each Linux pack job invokes `scripts/run-pylon-release-sandbox.mjs` once from the untouched checkout under an empty host environment. The orchestrator validates the original HEAD, tree, and clean state with optional Git locks, hooks, and filesystem monitoring disabled. Before repository execution it creates a standalone immutable source tree with a real `.git` directory, a separate Git-archive build tree with no writable metadata, and private control storage. It downloads npm `11.10.1` from the reviewed registry URL with a four-MiB transfer bound and requires the recorded SHA-256, SHA-512, and SRI.

Dependency installation, offline packing, contract checks, preview preparation, final verification, cache preparation, and Linux smoke use separate containers from the exact Linux/amd64 Node `22.23.2` image manifest. The orchestrator pins both the manifest digest and its independently checked image-config digest. It accepts the repository digest only as `node@…` or Docker's equivalent `docker.io/library/node@…` presentation, and explicitly requires the expected environment, entrypoint, and command while rejecting semantic volumes, healthchecks, users, workdirs, exposed ports, build hooks, shells, stop settings, and related runtime configuration. Docker versions may omit safe fields or serialize them as null or empty without weakening those semantic checks. Every run overrides the image entrypoint with exact `/usr/bin/env`, disables healthchecks, selects the exact user, workdir, command, and environment, drops every capability, sets `no-new-privileges`, and bounds PIDs, CPU, and memory. The root filesystem is read-only and temporary filesystems are private. Immutable source, Git metadata, npm, and artifact inputs are read-only. The original checkout, host home, runner control files, tokens, npmrc files, and Docker socket are never mounted or passed. Signal and normal cleanup own only exact captured cidfile identities and the one active macOS process-group identity; handlers remain installed through terminal absence proofs. Cleanup requires container removal plus a terminal inspection result and, where applicable, process-group drain. A failure prints only one enumerated public stage code, never the rejected path, value, or environment content.

The writable build and npm-cache mounts are backed by runner storage. Docker's CPU, memory, PID, and tmpfs limits do not impose a disk-byte or inode quota on those bind mounts. Traversals that affect admission and freezing have explicit entry, depth, subject-count, or byte bounds where practical, but cleanup still has to traverse whatever private tree a failed phase left behind. Disk or inode exhaustion, a traversal-limit failure, a timeout, or incomplete cleanup fails the job before it can return or upload a publication output; this is availability protection, not a claim that bind-backed storage consumption is contained.

Dependency-backed release and publication contract tests, in both CI and preview modes, receive only a disposable candidate copy. That copy is discarded after the container exits. A separate networkless, dependency-free preparation container is the only process allowed to write the real candidate. The candidate is then frozen. A fresh networkless, dependency-free verifier mounts it read-only and writes a canonical five- or six-file size/SHA-256 receipt to a separate control mount. After every container and process group is proven absent, the host validates that receipt and performs bounded chunked `O_NOFOLLOW` reads and writes into a never-mounted output directory. It rejects sparse files and any owner, mode, link, inode, size, timestamp, pathname, or digest change; freezes files through their descriptors to `0444` and the directory to `0555`; and repeats the validation. This copy helper is a workflow-private bridge whose preconditions are exact process absence and caller-owned private directories. It is not a general primitive for a source tree that still has a hostile concurrent writer. No repository command runs between this final freeze and upload.

The Linux install gate first fills a disposable npm cache without lifecycle scripts, then runs the exact statically verified artifact in a fresh `--network none` container as the job's last meaningful step. The macOS gate cannot provide the Linux PID and network namespace boundary. It therefore captures the trusted setup-node executable with `fs.realpathSync.native(process.execPath)` before artifact execution, uses immutable verifier source, a fresh HOME and npm runtime, a minimal tokenless environment, and an exact process group. It sends `SIGKILL` when that group does not drain and verifies that the original group is gone. Repeated or mixed termination signals coalesce on that one captured group and the captured container set until absence proofs finish. This numeric-PGID control assumes the operating system does not reuse the PGID during the bounded interval from spawn through drain; Node does not expose a stronger macOS process-group handle. A child can deliberately escape with a new session, so this is not a claim of complete descendant containment. The statically verified artifact instead runs only in a read-only, ephemeral, terminal install job with no later publication mutation or repository validation. These gates never use mutable global npm installation.

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
  "publicationPolicyRevision": 3,
  "sequenceEpoch": 1,
  "sequence": 123,
  "workflowRunId": "33428882721"
}
```

`sequence` is the positive safe integer `github.run_number` for the one preview workflow. `workflowRunId` is its exact positive decimal run id. Failed runs create gaps, so consumers allow a higher non-adjacent sequence. A workflow sequence reset requires a new signed epoch/schema and consumer migration; it must never silently reuse epoch 1. Ordering never comes from a commit abbreviation, SemVer, a timestamp, the GitHub “latest” pointer, or a tag sort.

`runAttempt` is deliberately not in manifest bytes. A rerun keeps the same run id, run number, manifest, and build-tag identity. The verified SLSA workflow/v1 predicate supplies the actual `/runs/<id>/attempts/<attempt>` invocation. Verification requires its signed run id to equal `workflowRunId`, then reads that exact immutable attempt endpoint and its attempt-specific jobs. It proves the run number, repository id, workflow path/ref, push event, source SHA/branch, GitHub Actions check-suite app, and successful directly environment-gated attester job. The aggregate `/runs/<id>` view is mutable across reruns and is not an attestation trust root; a later failed rerun cannot invalidate an earlier exact signed and published attempt.

The approved attester signs exactly six subjects with pinned `actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8`, whose reviewed pinned chain delegates to `actions/attest@508db95dd578ae2727ebd6217d5ba78e4fbda05d`. A read-only job verifies the exact subject set, SLSA v1 workflow predicate, GitHub OIDC issuer, signer digest/ref, public Rekor entry, and run invocation. Only then can checkout-free contents jobs fully stage and publish the exact draft.

The directly `pylon-preview`-gated staging job re-reads live `pylon`, then a dedicated App-authenticated step performs the exact combined REST/GraphQL audit immediately before the separate `GITHUB_TOKEN` preview-tag CAS step. The directly gated publisher repeats live branch/tag checks and a fresh combined audit immediately before the separate immutable-release update. GraphQL is the final authoritative read in each audit. A stale admission or earlier audit is irrelevant. GitHub does not offer an atomic transaction across branch reads, tag creation, and release publication. Each read and compare-and-set is a separate fail-closed point-in-time check; this design does not claim cross-resource atomicity.

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

Use `--initialize` only after manually inspecting the first full verified receipt. Omit it thereafter. The canonical JSON at `--state` remains the CLI-compatible projection. The adjacent private `.journal` directory is the concurrency authority. Its authenticated checkpoint names one current epoch, anchors the exact prior immutable tip, and carries that tip's bounded canonical state bytes. Within the epoch, base-digest transition links and one contiguous operation-slot namespace are immutable no-replace records. Each slot is bound to its exact checkpoint epoch and generation and carries either a random-token normal operation or a deterministic rotation operation. Normal-operation 10-second heartbeats yield to one permanent `released`, `retired`, or `commit` decision. `transaction.commitState(candidate)` only validates and privately stages one copied candidate for the current callback. It does not publish a commit terminal, a transition, or the projection while callback code is still running. Only after the callback returns successfully does the wrapper publish the immutable commit decision and finish its transitions, projection, and applied marker before the overall call returns. A throw, process exit, or stale-owner retirement after staging but before callback success leaves no state commit. The active heartbeat and unresolved shared operation slot keep every later normal operation and rotation out for the full callback. A stale 30-second claim is retired, and a complete post-callback commit is helpable after every crash point. Owned write temporaries live in the separate bounded `.owned-temporaries-v2` namespace, so authenticated logical-entry caps never make orphan cleanup unreachable. The verifier preserves live-writer fencing, rejects gaps, cycles, unreachable records, orphan markers, symlinks, unexpected entries, and excess record, temporary, depth, or byte work, and repairs a missing or stale JSON projection from the journal tip.

`${state}.lock` is not the current journal namespace. For a fresh v2 journal, it is a permanent exact regular-file downgrade guard for clients that used `proper-lockfile`. Current tooling publishes that file by fsyncing a named owned temporary, hard-linking it no-replace, and fsyncing the parent. For migrated v1 authority, the original `${state}.lock` directory stays in place and contains an immutable `.pylon-consumer-v1-retired.json` marker. The marker binds the exact complete pre-marker authority digest and tip digest, and only that exact marker is excluded from the v1 authority digest. Its file, lock directory, and parent are fsynced before migration continues. The nonempty directory permanently blocks an old client's `rmdir` and subsequent atomic lock-directory `mkdir`. A directory without that exact marker is treated as a live or ambiguous legacy lease and fails closed. If no `${state}.transactions` authority exists, stop all old clients, confirm no owner remains, and remove that lease directory manually before retrying; current verification never enters or steals it. If the transaction namespace exists, preserve the directory and use the migration command below.

Versions before the checkpoint journal used `${state}.transactions` plus claim, terminal, and applied records in a `${state}.lock` directory. Legacy detection does not depend on that transaction namespace alone. Before guard or journal initialization, current verification independently inspects `${state}.transactions`, `${state}.lock.v1-retired`, an in-place `${state}.lock` directory and retirement marker, and any existing v2 head whose `sourceAuthoritySha256` is non-genesis. Any one signal requires the complete exact legacy source. A missing or deleted companion namespace, malformed entry, wrong mode, or symlink fails closed. The presence of the transaction namespace is always prior authority; current verification refuses to seed or trust a v2 projection around it. After stopping every old client and confirming that every old claim is terminal, migrate once:

```sh
npm run release:pylon:migrate-consumer-journal -- \
  --state "$HOME/.local/state/pylon-prime/preview-high-water.json"
```

This explicit quiescent command pins and bounds every v1 file, authenticates the complete transition chain and every relevant commit/help record, and accepts a projection only when it is the exact tip or an authenticated stale prefix. A commit is complete only when its exact applied marker and every decided transition are durable. An incomplete commit is recoverable only when `kill(pid, 0)` proves its recorded owner is gone with `ESRCH`; a live PID, PID reuse, `EPERM`, or any uncertain liveness blocks without retiring the authority. After all permitted dead-owner help, the command re-reads the source and atomically publishes the immutable retirement marker inside the existing `${state}.lock` directory. It never renames that directory and never creates or replaces `${state}.lock.v1-retired`. A prior `${state}.lock.v1-retired` layout from an interrupted local migration is accepted only as read-only source evidence and must use the exact regular downgrade guard. The deterministic v2 checkpoint binds the digest and tip of the complete old authority. Concurrent migrators re-read and join an exact marker or checkpoint that appeared after their initial read; conflicting marker, authority, directory, or checkpoint data fails closed. The command re-authenticates the full source immediately before marker publication, immediately before checkpoint publication, before projection repair, and before success. Every step is fsynced, deterministic, concurrently joinable, and retryable after a crash. Corrupt, active, missing, unreachable, extra, symlinked, over-limit, or permission-unsafe old authority fails closed.

Rotate before an epoch reaches 3,800 transitions or 60,000 claims:

```sh
npm run release:pylon:rotate-consumer-journal -- \
  --state "$HOME/.local/state/pylon-prime/preview-high-water.json"
```

Normal updates and rotation allocate from one immutable next-operation slot namespace. Every allocator scans and resolves the latest slot, rescans the same epoch and intent, and publishes only that exact next generation with no replacement; a lost publication loops from the new authority. No allocator may publish generation `N+1` while `N` is active or unresolved. A normal slot uses a random token and the configured finite normal-claim cap. A rotation slot is cap-exempt and carries the deterministic intent derived only from the exact current checkpoint and immutable tip, so rotators with different caller caps join the same generation, epoch id, directory, and checkpoint. Once a rotation wins its slot it is never released or retired: normal callers and later rotators help it through prior-writer quiescence, and no normal operation can cross it. If a normal operation wins the shared next slot first, rotation re-reads its committed tip and derives a new slot. Immediately before checkpoint linking and before success, rotation scans the complete bounded root set and rejects every competing same-epoch directory or checkpoint. Dead or already-retired normal-operation temporaries are removed; live prior temporaries keep rotation pending until they quiesce, while live helpers for the same deterministic rotation may join the same no-replace checkpoint link. Projection repair and commit help use bounded internal retries only for authenticated replacement or ctime races. Each retry re-walks the immutable journal tip before rereading or repairing the projection; malformed metadata, symlinks, and transaction digest corruption remain terminal errors. The current projection and high-water JSON schema do not change. After the new epoch is durable, a new fenced owner removes only the authenticated retired epoch and predecessor checkpoint, so active fencing data, directory entries, scan depth, and bytes remain bounded. Exact concurrent helpers treat a peer's already-removed retired epoch, predecessor checkpoint, or owned temporary as the same completed cleanup, re-scan an advanced authenticated head, and join the exact deterministic rotation instead of surfacing a transient path error.

These pathname checks are not a portable `openat` security sandbox. The verifier rejects observed symlinks and non-directories, pins every read to a no-follow file descriptor where Node exposes it, bounds bytes before allocation, and re-stats after an exact read. Every operation requires a numeric current uid. Every relied-on state, guard, journal, temporary namespace, epoch, claim, marker, transition, and migration-authority entry must already be owned by that uid and have exact `0600` file or `0700` directory mode. Group/world-writable entries are rejected before parsing or use and are never chmod-and-trusted, because another process may retain a writable file descriptor. Newly created directories and files use exact `0700` and `0600`; their contents and directory entries are fsynced before success. For old private state with other modes, stop every process that may hold a descriptor, preserve an offline backup, correct the modes while fully quiescent, and retry. Tooling never performs that migration implicitly. The state parent remains a trusted user-owned local directory with no hostile mutation by the same OS user. Platforms without a numeric current uid fail closed.

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
7. The directly `pylon-stable`-gated publisher mints one repository-scoped auditor token. Immediately before creating annotated `pylon-stable-sequence-NNNNNN`, before creating or refetching the exact lightweight stable tag, and before making the draft immutable, a separate read-only step uses that same token for the exact REST validation followed by the authoritative GraphQL zero-bypass proof. Each following mutation step uses only `GITHUB_TOKEN`. Only after both exact refs exist does it make that draft immutable and check postconditions.

The reservation annotation binds sequence, policy commit/tree, the exact promote/withdraw tuple and reason, stable and preview tags, stable-manifest SHA-256, and draft release id. A reservation `422` refetches and stops for explicit recovery. Final tag `422` handling refetches and accepts only the exact lightweight full-commit target; a wrong or annotated object fails before immutable publication. No path selects N+1, moves, deletes, or reuses a ref. The reservation freezes the approved old policy tuple if `pylon` advances later. Reservation CAS, final tag CAS, and release publication are ordered GitHub operations, not one atomic GitHub transaction. Each protected mutation has a new combined audit whose last authoritative read is GraphQL; earlier admission and the prior mutation's audit do not authorize it.

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

Run offline policy tests with `npm run test:pylon-publication` and App-acceptance unit tests with `npm run test:pylon-ruleset-auditor-app`. They cover exact current/historical workflow digests and registry closure, immutable signed attempt evidence, zero-asset crash recovery, deterministic stale recovery, active heartbeats, transaction crash convergence, path-boundary checks, exact required-check paths/apps, preview/stable tag squats and CAS order, withdrawal tuples, rollback state, approval DAGs, every contents writer, pinned actions, no source/download execution in publication writers, mocked App scopes and endpoints, REST and GraphQL redaction, the nonzero canary, and token revocation.
