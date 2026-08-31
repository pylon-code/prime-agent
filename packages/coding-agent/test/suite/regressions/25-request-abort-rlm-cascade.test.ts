import { type FauxResponseStep, fauxAssistantMessage } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it } from "vitest";
import type { CustomMessage } from "../../../src/core/messages.js";
import { createHarness, getAssistantTexts, type Harness } from "../harness.js";

interface Gate {
	promise: Promise<void>;
	open: () => void;
}

function gate(): Gate {
	let open = () => {};
	const promise = new Promise<void>((resolve) => {
		open = resolve;
	});
	return { promise, open };
}

/** Faux step that holds the provider stream open until the gate opens or the request aborts. */
function heldResponse(options: { record: () => void; release: Promise<void>; text: string }): FauxResponseStep {
	return async (_context, streamOptions) => {
		options.record();
		const signal = streamOptions?.signal;
		await new Promise<void>((resolve) => {
			if (signal?.aborted) {
				resolve();
				return;
			}
			signal?.addEventListener("abort", () => resolve(), { once: true });
			void options.release.then(() => resolve());
		});
		return fauxAssistantMessage(options.text);
	};
}

function terminalNotices(messages: readonly unknown[]): CustomMessage[] {
	return messages.filter(
		(message): message is CustomMessage =>
			typeof message === "object" &&
			message !== null &&
			(message as { role?: unknown }).role === "custom" &&
			((message as { customType?: unknown }).customType === "rlm_child_terminal_notice" ||
				(message as { customType?: unknown }).customType === "rlm_child_failure"),
	);
}

describe("issue #25 requestAbort cascades into active RLM child runs", () => {
	const harnesses: Harness[] = [];
	const gates: Gate[] = [];

	afterEach(() => {
		while (gates.length > 0) {
			gates.pop()?.open();
		}
		while (harnesses.length > 0) {
			harnesses.pop()?.cleanup();
		}
	});

	async function createParent(child: Harness): Promise<Harness> {
		const parent = await createHarness({
			rlmDepth: 0,
			rlmMaxDepth: 1,
			subagentRuntimeHost: {
				createRlmSubagentRuntime: async () => ({ session: child.session }),
				deleteRlmSubagentRuntime: async () => {},
			},
		});
		harnesses.push(parent);
		return parent;
	}

	it("terminates an in-flight child provider stream and settles the parent", async () => {
		const child = await createHarness();
		harnesses.push(child);
		const release = gate();
		gates.push(release);
		let childRequests = 0;
		child.setResponses([
			heldResponse({
				record: () => {
					childRequests++;
				},
				release: release.promise,
				text: "child finished after the abort",
			}),
			fauxAssistantMessage("child must not start a second turn"),
		]);
		const parent = await createParent(child);
		parent.setResponses([fauxAssistantMessage("parent recovered")]);

		const spawned = await parent.session.runRlmChild("long shard", { name: "cascade-worker" });
		await expect.poll(() => childRequests).toBe(1);
		expect(parent.session.getRlmChildRunStatus(spawned.rlm_child_id)).toBe("running");

		parent.session.requestAbort();

		expect(parent.session.getRlmChildRunStatus(spawned.rlm_child_id)).toBe("cancelled");
		// The run unwinds on its own once the child's provider stream is cut.
		await expect.poll(() => parent.session.getRlmChildRunStatus(spawned.rlm_child_id)).toBeUndefined();
		expect(parent.session.hasRunningRlmChildren()).toBe(false);
		expect(child.session.isStreaming).toBe(false);
		// No further child request reached the provider.
		expect(child.faux.state.callCount).toBe(1);
		expect(child.getPendingResponseCount()).toBe(1);
		// A cancelled child does not report an outcome to the parent.
		expect(terminalNotices(parent.session.messages)).toEqual([]);
		expect(parent.session.getPendingNextTurnMessageSnapshots()).toEqual([]);

		// The cut leaves no quiescence waiter behind, and the next turn runs.
		await expect(parent.session.waitForRlmQuiescence()).resolves.toBeUndefined();
		parent.session.resumeQueuedWork();
		await parent.session.prompt("what happened?");
		await expect(parent.session.waitForRlmQuiescence()).resolves.toBeUndefined();
		expect(getAssistantTexts(parent)).toEqual(["parent recovered"]);
	});

	it("leaves a retained background subagent running", async () => {
		const child = await createHarness();
		harnesses.push(child);
		const release = gate();
		gates.push(release);
		let childRequests = 0;
		child.setResponses([
			fauxAssistantMessage("first shard done"),
			heldResponse({
				record: () => {
					childRequests++;
				},
				release: release.promise,
				text: "background work finished",
			}),
		]);
		const parent = await createParent(child);
		parent.setResponses([fauxAssistantMessage("parent consumed the child result")]);

		const spawned = await parent.session.runRlmChild("first shard", { name: "retained-worker" });
		await expect.poll(() => terminalNotices(parent.session.messages)).toHaveLength(1);
		const retained = parent.session.getRlmChildSession(spawned.rlm_child_id);
		expect(retained).toBe(child.session);

		// Retained children stay addressable, so their own work is background work the
		// parent turn does not own.
		const background = child.session.prompt("keep working in the background");
		await expect.poll(() => childRequests).toBe(1);

		parent.session.requestAbort();

		expect(child.session.isStreaming).toBe(true);
		release.open();
		await background;
		expect(child.session.getLastAssistantText()).toBe("background work finished");
		expect(child.faux.state.callCount).toBe(2);
		expect(parent.session.getRlmChildSession(spawned.rlm_child_id)).toBe(child.session);
	});
});
