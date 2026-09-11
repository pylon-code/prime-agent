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

The offline build enters its network namespace through `sudo unshare`. Before writing its receipt or spawning build commands, the release entrypoint restores the invoking non-root `SUDO_UID` and `SUDO_GID`, clearing supplementary groups first. It verifies the resulting real and effective identities; malformed sudo identities or failed credential changes stop the build. The build and its children keep the isolated network namespace, while output directories and artifacts belong to the runner that prepares the preview manifest afterward. Ordinary non-sudo builds retain their caller identity.

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

All preview and stable asset uploads use the pinned GitHub client's `repos.uploadReleaseAsset` method. It selects `uploads.github.com`, encodes the required `name` query parameter, and sends the exact binary body and content length. A generic API-host POST does not supply that transport contract. Preview staging checks every returned asset receipt; stable staging and zero-asset recovery also download and hash the singleton before reservation or publication.

Policy revision 3 binds these upload workflow bytes. Revisions 1 and 2 retain their original digests for historical verification and explicit stable recovery. A failed preview's valid source tag and empty draft remain attached to that source; a correction on a new source commit uses its own source-derived preview tag. Do not retarget the old tag or attach new-source bytes to its draft.

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

Use `--initialize` only after manually inspecting the first full verified receipt. Omit it thereafter. The canonical JSON at `--state` is a repairable projection. The private `${state}.journal-v3` sidecar selects one inode-bound `journal-<intent-digest>-<uuid>` root through a receipted `root.json`. Its immutable `intent.json` retains the actual initial state, and migration additionally retains the full historical source commitment. A directory's existence or a matching checkpoint digest alone never establishes authority.

Each v3 generation contains a checkpoint, an epoch and exact receipt links. A complete hidden builder is fsynced before one same-parent directory rename publishes it. Normal claims, their winning index CAS, heartbeats, terminal decisions, transitions and applied records are immutable. Every record is published in this order: write/fsync an owned temporary, fsync its receipt directory, hardlink the canonical target without replacement, fsync the target parent, rename the temporary to its fixed receipt name, then fsync the receipt directory. Recovery authenticates the exact remaining inode and bytes; it never removes the only durable publication proof.

`transaction.commitState(candidate)` privately stages one copied candidate. Only successful callback return permits the commit terminal. Callback errors, process death or stale-owner retirement before that return cannot commit staged bytes or replay the callback. A durable commit decision can be completed by another owner. Active claims exclude normal operations and rotations; 10-second heartbeats refresh the ordinary 30-second lease. Expiry permits a permanent retired decision, not callback replay. Projection repair rescans the authenticated tip after every rename and repairs forward if a delayed writer installed an older projection.

### Explicit historical migration

Stop every legacy verifier process before migration. This external quiescence is required even if PID probes or heartbeat age suggest inactivity: a previously admitted old process may already be past its projection revalidation. The acknowledgement is mandatory before any migration filesystem mutation:

```sh
npm run release:pylon:migrate-consumer-journal -- \
  --state "$HOME/.local/state/pylon-prime/preview-high-water.json" \
  --acknowledge-legacy-processes-stopped
```

The equivalent API is `migrateConsumerStateJournal(statePath, { acknowledgeLegacyProcessesStopped: true })`. Normal operations never implicitly migrate historical authority. Migration supports original in-place v1 lock/transaction directories, prior-retired v1 plus its exact regular guard, native v2, and v2 descended from either v1 layout. The reader authenticates every retained/current epoch, winning claim/index, valid loser, complete or helpable decision, exact source inode, bounded permitted temporary and allowed stale-prefix projection. Missing companions, active unresolved writers, ambiguous recovery, unsafe modes and unknown entries fail closed.

Migration installs a receipted impossible-generation blocker, `claim-9999999999999999.json`, into each applicable original v1 lock and retained/current v2 epoch before publishing the v3 guard. The blocker binds the original source identity, authority, tip, retirement marker and immutable intent. Both original v1 directories are preserved in place, opened and validated without following symlinks, frozen through their pinned handles to exact `0500`, then fsynced with their parents. Partial freezes are accepted only with their exact prior proof. Native or prior-retired v2 receives its durable regular v3 guard before the exact `.journal` inode moves to `.journal.v2-retired`. Original in-place v1 keeps its `.lock` directory. A new old-client bootstrap `.journal` in this retirement gap is a foreign inode conflict, never overwritten or silently adopted.

Provenance is reconstructed from that final fenced source, including every retained/current record and the immutable projection snapshot. The selected construction root has its own durable inode receipt before any generation publication. A crash before root selection may leave an inert empty construction directory; resume allocates a new exclusive root instead of claiming the unknown inode. At most 64 such directories are permitted. The selected generation is built and published atomically, the projection is repaired through an owned claim, and canonical completion is separately receipted. Re-running the acknowledged migration revalidates the retained source and exact selected root. Concurrent helpers can join an independently authenticated winner; callers that observe conflicting bytes, inode replacement or an unsafe intermediate read fail closed. Already-running legacy processes are never supported concurrently.

Recovery and concurrent helpers complete the same durability sequence as the original publisher: canonical-parent fsync, fixed receipt rename, then receipt-directory fsync. An already fixed receipt still requires the joining caller to synchronize and revalidate its canonical and receipt parents. An already installed guard requires both its staging and canonical parents to be synchronized and its proof-backed inode revalidated before v2 retirement.

Preserve the original frozen v1 namespaces and `.journal.v2-retired`: they remain required provenance, not disposable generation history. Never remove a guard, reset the authority or copy a replacement journal over a failed migration. Preserve an offline backup and diagnose the exact reported conflict before retrying the same acknowledged command.

### Rotation, cleanup and resource limits

Rotate explicitly before the epoch reaches 3,800 transitions or 60,000 claims; normal operations also reserve capacity for rotation:

```sh
npm run release:pylon:rotate-consumer-journal -- \
  --state "$HOME/.local/state/pylon-prime/preview-high-water.json"
```

Normal operations and rotation share one next-slot CAS. A rotation binds the exact latest winning claim/index, immutable tip, complete predecessor grammar and successor intent. Preparation converges a durable two-final cut before callback entry; successful rotation leaves one current final. Retirement and deletion preserve the observed predecessor inode. Cleanup removes only entries authenticated by the successor's committed retirement certificate; both certificate links survive until all ordinary authority is gone. After the last proof link, the successor permits cleanup only of that exact same-inode empty container. Byte-identical replacement directories and unknown extra entries remain conflicts. Dead temporaries and exact decided losers can be removed despite PID reuse; live unresolved writers block cleanup.

The supported state size is 16 MiB per field. Checkpoint bounds account for all three base64 fields (`4 * ceil(bytes / 3)` each) and the complete envelope. Separate explicit budgets are 256 MiB for each historical v1/v2 inventory, 512 MiB for migration metadata/receipts, and 512 MiB for the v3 generation journal; these are not one combined memory or disk cap. Root, epoch, receipt and aggregate bounds are checked before nested allocations. Receipt lookup indexes canonical inode identities once. If claim/index publication or preparation heartbeats consume the remaining admission margin, the exact owned claim is released and rotation finishes before heartbeat scheduling or callback entry. Preparation errors remain terminal with their original identity; capacity reacquisition never replays a callback. A capacity refusal occurs before commitment; do not reduce the real maximum fixture to make a verification run pass.

Every relied-on file is owned by the current numeric uid with exact `0600`; directories are exact `0700`, except proven original v1 directories frozen to `0500`. Reads are bounded, no-follow where Node supports it, exact to EOF, and checked against pinned inode/size/mtime/ctime observations. Only direct native unpinned discovery loss may restart bounded discovery. Hook or injected filesystem errors retain their identity, including `ENOENT`, `EIO`, `EPERM`, `ELOOP` and `EISDIR`; a later successful rename does not erase an earlier terminal error. Native unsafe-file errors remain terminal path refusals. These checks assume a trusted user-owned local parent and are not a portable `openat` sandbox. Unsupported numeric-uid platforms fail closed.

### Required publication verification

`npm run test:pylon-publication` retains the protected v2 regression oracle and exercises current public v3 preview/stable verification, migration and generation grammar. `npm run test:pylon-publication-crash` adds the exhaustive real child-process crash matrix; `npm run test:pylon-publication-stress` adds repeated four-process competition and read handoffs. These are three mandatory suites. The protected preview pack runs the retained/current-public contract suite within its existing job budget; exact-source admission separately requires the full CI aggregate, including every crash/stress/maximum gate. The crash inventory fixes each scenario's complete ordered hook/path/occurrence trace. Every listed cut must be reached through an IPC barrier; the parent kills only its captured child with `SIGKILL` and requires a fresh process to recover. Trace changes and missing cuts fail the gate. TAP diagnostics record the scenario, exact boundary, PID/signal, recovered root inode, projection outcome and elapsed time. These are process-crash tests; ordered fsync assertions support durability sequencing but do not simulate physical power loss.

CI requires the complete retained/current-public suite, repeated process stress suite, exhaustive crash suite and `npm run test:pylon-publication-maximum` on Ubuntu 24.04 and macOS 15 with Node 22.23.2, in that sequential order. The actual 16 MiB maximum starts only after all three preceding suites pass. Both platform results feed `build-check-test`; skipped, cancelled or failed publication jobs cannot make that aggregate succeed. Evidence artifacts bind logs to the tested commit and tree. For a complete local non-maximum proof, run `test:pylon-publication`, `test:pylon-publication-stress` and `test:pylon-publication-crash` sequentially on the same tree. Running only one is a component proof, never the full gate.

The crash matrix includes both an existing state parent and two missing nested parents, with actual file/ancestor-directory fsync instrumentation. Repeated normal/rotation stress requires each earlier recovered history to remain an exact prefix, every cumulatively acknowledged value to survive exactly once, and every recorded value to have a returned callback marker. A callback may have committed durably even when its operation subsequently failed closed; such values also remain in the preserved prefix.

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

## Self-contained managed artifacts (recipe 2)

Recipe 2 bundles the full production runtime into the root tarball. Before entering the offline build namespace, run `npm run release:pylon:hydrate-runtime` with the pinned toolchain. It acquires only URLs and SHA-512 identities already committed in `package-lock.json`, including every optional platform package. The offline pack rechecks these inputs, retains nested and peer resolution, includes the exact built internal workspaces, and never runs dependency lifecycle scripts. Ambient `node_modules` is not a runtime packaging input.

The final root tarball is checked for every expected runtime package. Both installed-artifact jobs extract that same root directly, with no npm installation, and prove CLI, SDK, native/WASM loading and caller-owned daemon behavior. Historical recipe 1 smoke remains available only for historical publication verification. Pylon also verifies the final archive through its bounded managed installer; signed provenance alone does not prove a runnable package.

The root retains SDK declarations and source maps. npm emits per-file PAX headers for a few dependency basenames longer than 100 bytes; the matching Pylon consumer accepts only bounded path/size/mtime metadata immediately followed by a regular file, preserving path, collision, link and size restrictions. Stable approval still requires successful protected artifact graduation for the exact preview.
