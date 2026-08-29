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
