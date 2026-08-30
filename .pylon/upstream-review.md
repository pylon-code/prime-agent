# Pylon upstream review ledger

This ledger records Prime upstream evidence and the decision taken for each overlapping Pylon feature. `.pylon/features.yaml` is the current machine-readable state; this file is the chronological review record.

## Entry template

- Date and upstream commit or release
- Prime issues, pull requests, releases, and source reviewed
- Affected feature keys from `.pylon/features.yaml`
- Decision: `adopt`, `hybridize`, `retain`, or `redesign`
- Reason and Pylon constraints
- Validation performed
- Follow-up or revisit trigger

## 2026-08-27 — fork baseline

- Upstream baseline: `PrimeIntellect-ai/prime-agent@bc0fa7606abb3b7af0f765319518d255e6ae553d`
- Latest audited release: `v0.8.1`
- Reviewed Prime's extension todo example, daemon plan and heartbeat APIs, ACP event surface, background execution work, releases, and related public issues and pull requests.
- `managed-plan-daemon`: **retain** Pylon's allowlisted full-snapshot managed extension. Prime has an extension example, not an equivalent reconnect-safe managed daemon plan contract.
- `managed-plan-acp`: **redesign** as a small capability-gated fork bridge. Do not expose arbitrary extension tool details.
- `delegated-work-progress`: **redesign** around phase-level root progress plus the separate Agents surface. Do not create one plan item per subagent.
- `heartbeat-projection` and `background-process-lifecycle`: **hybridize** after Pylon defines provider-neutral autonomous occurrence and checkpoint ownership.
- `scheduled-prompts`: **redesign** after the same ownership foundation. Prime's native scheduling is useful but is not itself Pylon's durable cross-provider scheduler model.
- No fork source behavior was added at baseline. The first fork change should target a measured Pylon parity gap and include stock-Prime degradation tests.

## 2026-08-29 — upstream integration through `d60fab8a`

- Upstream range: `PrimeIntellect-ai/prime-agent@bc0fa7606abb3b7af0f765319518d255e6ae553d..d60fab8a76d9c169f945341f0ee3bde21903bb55`; latest audited release remains `v0.8.1`.
- Reviewed Prime PRs #1878, #1876, #1877, and #1633 plus `generate-models.ts`, the generated model catalog, and the affected model tests. PR #1878 deliberately kept live catalog generation and refreshed Kimi K3 Prime Inference pricing. Prime closed the deterministic-CI and fixture-test proposals in #1876 and #1877; scheduled catalog refresh PR #1633 remains open.
- `prime-inference-catalog-capabilities`: **hybridize**. Adopt Prime's current catalog and generator changes, but retain Pylon's route-scoped live-pricing invariants instead of pinning vendor-owned numeric prices that can drift independently before tests.
- Reviewed the remaining upstream PRs in the range (#1841, #1846, #1849, #1852, #1853, #1835, #1838, #1861, #1836, and #1839). They apply without conflict and do not supersede landed Pylon fork behavior. Existing lifecycle decisions remain unchanged.
- Post-merge CI exposed an upstream-only regression: Prime main run `33186265648` and Pylon PR #5 both failed 23 kernel tests because the #1839 merge commit retained protocol 2 in fake runtimes while production requires protocol 3.
- `kernel-protocol-test-fixtures`: **adopt** the exact three-line repair from open Prime PR #1886 at `a97f6995d39f2cccbd33123ab246f9786fc8ac13`. Preserve that upstream commit as a merge parent rather than creating Pylon-specific behavior.
- Validation: the focused Prime Inference catalog test passed all 12 cases; the two protocol fixture suites passed all 39 cases; `npm run check` passed Biome over 937 files, the TypeScript check, installer rendering, and the browser smoke check; `git diff --check` passed.
- Revisit when Prime adopts deterministic catalog fixtures or repo-owned structural invariants, or stops regenerating vendor pricing before tests and releases.

## 2026-08-29 — correlated prompt lifecycle candidate

- Upstream baseline: `PrimeIntellect-ai/prime-agent@d60fab8a76d9c169f945341f0ee3bde21903bb55`; latest audited release remains `v0.8.1`.
- Reviewed Prime PRs #800, #1239, #1859, and #1861 plus the current daemon prompt, queue, cancellation, compact stream, worker recovery, update restart, and agent-connection paths. These changes provide useful primitives but no single optional contract proves exact per-prompt ownership, delivery, provenance, scoped cancellation, reconnect/recovery, generation fencing, request identity, and terminal usage.
- `correlated-prompt-lifecycle`: **hybridize**. Retain Prime's existing session and queue machinery behind Pylon's optional `correlated_prompt_lifecycle_v1` daemon/client capability. Keep protocol version 7, use additive schema revision 24, keep the capability out of default and required client capability sets, and shape lifecycle/provenance only for clients that explicitly negotiate it.
- The fork candidate adds dedicated correlated submit, cancel, and read-only lifecycle commands; exact session-generation fencing; canonical request fingerprints and conflicting retry rejection; durable command and worker recovery; delivery-aware cancellation; bounded lifecycle records/tombstones; prompt or session event provenance; per-prompt terminal usage; and fail-closed reconnect, replacement, and recovery validation. Stock clients and daemons retain their legacy prompt behavior.
- Replacement and reload are fenced at the session runtime boundary while any correlated lifecycle is nonterminal. Worker recovery replays only queued or selected actions; preparation that may already have run arbitrary extension hooks is terminalized as failed instead of being replayed.
- Fork candidate: [pylon-code/prime-agent#9](https://github.com/pylon-code/prime-agent/pull/9).
- Validation: the post-rebase lifecycle, connection, supervisor, runtime, recovery, and ledger checkpoint passed 564 tests across 17 files. The final changed-boundary checkpoint passed 202 connection and supervisor-monitor tests; the clean supervisor process checkpoint passed 10 tests with 8 fixture-gated skips. `npm run check` passed Biome over 940 files, TypeScript, installer rendering, and browser smoke; `git diff --check` passed. Independent final and follow-up audits found no remaining P0 or P1 findings.
- Revisit when Prime upstream ships an explicitly negotiated equivalent with the same delivery, provenance, scoped cancellation, recovery, generation, retry-integrity, privacy, and per-prompt usage guarantees.

## 2026-08-29 — snapshot recovery integrity candidate

- Audited baseline remains `PrimeIntellect-ai/prime-agent@d60fab8a76d9c169f945341f0ee3bde21903bb55`; latest audited release remains `v0.8.1`. The selected evidence commits are later than that baseline, but the intervening range was not fully audited and therefore does not advance `reviewed_upstream_commit`.
- Reviewed closed Prime issue #1229, merged Prime PRs #1845, #1756, and #1864, and their commits `c0334a176fb8f78b8c37327a6c76865792b03b52`, `ee8fd699617e8b66cb4bf36c97872c8fe52192e3`, and `5b6c0e94e11a97fcfdd7a9fc9dc4f7acbda9c853`. The pull requests solve child-snapshot projection fan-in, recovery-aware worker reuse, and in-flight open ownership. They do not fix the snapshot identity and failure-containment invariant recorded in #1229.
- `snapshot-recovery-integrity`: **hybridize**. Adopt and adapt those three upstream changes, while retaining Pylon's correlated prompt lifecycle and adding the missing transfer boundary: each attach, replacement, or catch-up gets an opaque unique snapshot ID; selected messages are encoded into immutable memory/file-backed chunks before deferred streaming; and a transfer-local mismatch retires only that generation and permits one bounded fresh-generation retry without closing the healthy resident worker.
- Protocol classification: **backward-compatible, capability-gated**. Schema revision 25 adds optional `immutable_snapshot_transfer_v1` negotiation without changing protocol version 7 or any existing command/event shape. A new supervisor requests worker chunk streams only when the worker advertises immutable identities. With an older worker it requests the full snapshot and re-encodes it under a supervisor-owned UUID, while old supervisors and stock clients retain their existing behavior.
- Resource boundary: transcript preparation checks cancellation before allocation, yields during large encodes, serializes per-session preparation, cleans partial spill files on every constructor/factory failure, uses private pid/start-identity/UUID cache roots, and sweeps abandoned roots only after ownership can be disproved. A single very large JSON message still incurs one synchronous `JSON.stringify` interval; realistic transcript size is not capped.
- The local `refs/review/current-candidate` at `a37efe92f88ae7234dcea1d065c0d3d76ac048cc` predates these corrections and includes the unaudited intervening range. Do not merge it. The upstream-review workflow must discard or regenerate it after this issue lands; this change does not mutate the protected review ref.
- Validation: the touched snapshot, protocol, connection, supervisor, recursion, daemon-mode, session-list, and lazy-subagent suites passed 616 tests across 11 files. The real-process supervisor control-channel regression for root isolation, chunked snapshot streaming, and worker adoption passed. `npm run check` passed Biome over 940 files, TypeScript, installer rendering, and browser smoke; `git diff --check` passed.
- Revisit when Prime gives every snapshot attempt immutable bytes and unique transfer identity, isolates bad generations from worker recovery, and preserves the same recovery and ownership guarantees without weakening Pylon's correlated lifecycle contract.

## 2026-08-30 — snapshot, worker-authority, and authoritative owned-session cleanup candidate

- Pylon base: `pylon@f728316dabbaa85aa561e6d6b08550ed337574be`, which includes merged correlated-lifecycle PR [#12](https://github.com/pylon-code/prime-agent/pull/12). Upstream evidence remains `PrimeIntellect-ai/prime-agent@a903d4b6768f484bd6d459b7b0aa7dee38e461e2`; latest compatibility release remains stock `v0.8.1`.
- The recovered [#11](https://github.com/pylon-code/prime-agent/issues/11) candidate keeps protocol version 7. Schema revision 26 adds the optional pre-begin `session_snapshot_failed.purpose` correlation used by immutable snapshot transfer. Schema revision 27 adds the supervisor-only `authoritative_owned_session_cleanup_v1` offer and read-only `get_owned_session_cleanup` query. Neither capability is inferred from package version, schema alone, method presence, or the legacy `client_owned_sessions` offer.
- `snapshot-recovery-integrity`: **hybridize**. In addition to immutable transfer identities and isolated generation failure, the candidate preserves terminal child errors through passive hydration, stabilizes snapshot cursors across asynchronous roster selection and the exact attach-registration cut, invalidates stale supervisor selection/materialization, closes a private channel if an announced frame is interrupted, and binds worker frames to the current authenticated channel and latest authoritative roster.
- `authoritative-owned-session-cleanup`: **hybridize**. `complete_owned_session` cannot succeed until the exact process generation is absent, ancillary journals are absent, the durable descriptor is removed and verified last, and only then the in-memory registration is removed. Tombstone, archive, and descriptor failures retain a stopping registration and arm a single exact-generation retry finalizer. A different client that already knows the exact host-private active-session ID may poll only `active | stopping | settled`; no PID, path, owner identity, descriptor, token, or diagnostics cross the public boundary.
- Compatibility classification: **backward-compatible, additive, capability-gated**. Private session workers continue to advertise the shared default server capabilities and do not falsely offer the supervisor-only cleanup query. Current clients reject the query locally against stock `v0.8.1` or schema 26 without sending an unknown command. The proof applies only to registrations created or durably adopted by a capable supervisor; it cannot retroactively certify a descriptorless historical orphan.
- Validation: 695 focused protocol/client/connection/session/snapshot/supervisor tests passed across 13 files; 40 correlated-lifecycle/queue/continuation tests passed; 13 real-process supervisor tests passed with 8 fixture-gated skips, including direct completion, owner loss before and after worker registration, visible `stopping`, replacement-client `settled` proof, supervisor replacement, exact worker exit, and zero descriptors. Final exact-head review also forced recovery-join, published-replacement rollback retention, reentrant shutdown single-flight, concurrent-completion, and generation-keyed-finalizer repairs before renewed exact-head review. Both stock/current `v0.8.1` adoption directions passed with local cleanup-query rejection on the stock supervisor. `npm run check` passed Biome over 941 files, TypeScript, installer rendering, and browser smoke. The root build passed; the live generated model catalog was restored afterward. `git diff --check` passed.
- Revisit when Prime upstream provides equivalent immutable snapshot, attach admission, worker-channel authority, and descriptor-last crash cleanup contracts without weakening the stock fallback or exposing host-private process/session details.
