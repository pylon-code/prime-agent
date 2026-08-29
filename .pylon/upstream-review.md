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
- Validation: the focused Prime Inference catalog test passed all 12 cases; `npm run check` passed Biome over 937 files, the TypeScript check, installer rendering, and the browser smoke check; `git diff --check` passed.
- Revisit when Prime adopts deterministic catalog fixtures or repo-owned structural invariants, or stops regenerating vendor pricing before tests and releases.
