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
