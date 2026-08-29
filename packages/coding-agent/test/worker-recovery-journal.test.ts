import { appendFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { WorkerRecoveryJournal } from "../src/modes/daemon/worker-recovery-journal.js";

describe("WorkerRecoveryJournal", () => {
	const roots: string[] = [];

	afterEach(() => {
		for (const root of roots.splice(0)) {
			rmSync(root, { recursive: true, force: true });
		}
	});

	function createPath(): string {
		const root = mkdtempSync(join(tmpdir(), "prime-agent-worker-recovery-"));
		roots.push(root);
		return join(root, "worker.recovery.jsonl");
	}

	it("restores the latest operation state per session", () => {
		const path = createPath();
		const journal = new WorkerRecoveryJournal(path);
		journal.record({
			activeSessionId: "active-1",
			sessionId: "session-1",
			sessionFile: "/tmp/session-1.jsonl",
			busy: true,
			operation: "prompt_accepted",
			promptLifecycles: {
				records: [
					{
						correlationId: "correlation-1",
						phase: "delivered",
						kind: "model_prompt",
						revision: 3,
						deliveryCrossed: true,
					},
				],
				expired: [],
				pendingUsage: [],
			},
		});
		journal.record({
			activeSessionId: "active-2",
			sessionId: "session-2",
			busy: false,
			operation: "ready",
		});

		expect(WorkerRecoveryJournal.readLatest(path)).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					activeSessionId: "active-1",
					busy: true,
					operation: "prompt_accepted",
					promptLifecycles: {
						records: [expect.objectContaining({ correlationId: "correlation-1", deliveryCrossed: true })],
						expired: [],
						pendingUsage: [],
					},
				}),
				expect.objectContaining({ activeSessionId: "active-2", busy: false, operation: "ready" }),
			]),
		);
	});

	it("accepts legacy records and ignores malformed lifecycle recovery state", () => {
		const path = createPath();
		appendFileSync(
			path,
			`${JSON.stringify({
				version: 1,
				activeSessionId: "legacy-active",
				sessionId: "legacy-session",
				busy: true,
				operation: "prompt_accepted",
				recordedAt: "2026-01-01T00:00:00.000Z",
			})}\n`,
		);
		appendFileSync(
			path,
			`${JSON.stringify({
				version: 2,
				activeSessionId: "invalid-active",
				sessionId: "invalid-session",
				busy: true,
				operation: "prompt_accepted",
				promptLifecycles: {
					records: [{ correlationId: "bad", phase: "delivered", revision: 3 }],
					expired: [],
					pendingUsage: [],
				},
				recordedAt: "2026-01-01T00:00:00.000Z",
			})}\n`,
		);

		expect(WorkerRecoveryJournal.readLatest(path)).toEqual([
			expect.objectContaining({ version: 1, activeSessionId: "legacy-active" }),
		]);
	});

	it("compacts stable checkpoints and ignores a truncated final record", () => {
		const path = createPath();
		const journal = new WorkerRecoveryJournal(path);
		journal.record({
			activeSessionId: "active-1",
			sessionId: "session-1",
			busy: true,
			operation: "bash_start",
		});
		journal.record({
			activeSessionId: "active-1",
			sessionId: "session-1",
			busy: false,
			operation: "bash_end",
		});
		appendFileSync(path, "{truncated");

		expect(WorkerRecoveryJournal.readLatest(path)).toEqual([
			expect.objectContaining({ activeSessionId: "active-1", busy: false, operation: "bash_end" }),
		]);
	});
});
