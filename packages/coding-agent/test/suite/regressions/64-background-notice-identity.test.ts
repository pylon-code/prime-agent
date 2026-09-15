import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it } from "vitest";
import { createDeferred, type HostRequestHandlers } from "../../../src/core/kernel/index.js";
import { createHarness, type Harness } from "../harness.js";

describe("#64 background completion identity", () => {
	let harness: Harness | undefined;
	afterEach(() => harness?.cleanup());

	it("withdraws the newer read handle without losing an older unread notice after PID reuse", async () => {
		const started = createDeferred<void>();
		const release = createDeferred<void>();
		harness = await createHarness({
			tools: [
				{
					name: "wait",
					label: "Wait",
					description: "Hold the current tool",
					parameters: Type.Object({}),
					execute: async () => {
						started.resolve();
						await release.promise;
						return { content: [{ type: "text", text: "released" }], details: {} };
					},
				},
			],
		});
		const session = harness.session;
		harness.setResponses([
			fauxAssistantMessage(fauxToolCall("wait", {}), { stopReason: "toolUse" }),
			fauxAssistantMessage("Finished."),
		]);
		const original = session.prompt("Continue working.");
		const handlers = (
			session as unknown as { _createKernelHostHandlers(): HostRequestHandlers }
		)._createKernelHostHandlers();
		const older = { pid: 42, command: "same command", exitCode: 0, completionId: "a".repeat(32) };
		const newer = { ...older, exitCode: 7, completionId: "b".repeat(32) };
		try {
			await started.promise;
			await expect(handlers["bash.completed"]!(older)).resolves.toEqual({ completionId: older.completionId });
			const unread = session.getSteeringMessages();
			expect(unread).toHaveLength(1);
			await handlers["bash.completed"]!(newer);
			expect(session.getSteeringMessages()).toHaveLength(2);
			await handlers["bash.consumed"]!(newer);
			expect(session.getSteeringMessages()).toEqual(unread);
			await handlers["bash.consumed"]!(newer);
			await handlers["bash.consumed"]!({ pid: older.pid, command: older.command });
			await handlers["bash.consumed"]!({ ...older, command: "foreign command" });
			await handlers["bash.consumed"]!({ ...older, pid: 43 });
			expect(session.getSteeringMessages()).toEqual(unread);
			await handlers["bash.consumed"]!(older);
			expect(session.getSteeringMessages()).toEqual([]);
			await expect(handlers["bash.completed"]!({ ...older, completionId: "invalid" })).rejects.toThrow(
				"completionId",
			);
			await expect(handlers["bash.consumed"]!({ ...older, completionId: "invalid" })).rejects.toThrow(
				"completionId",
			);
		} finally {
			release.resolve();
			await original;
		}
		await session.waitForIdle();
		expect(harness.eventsOfType("agent_start")).toHaveLength(1);
	});
});
