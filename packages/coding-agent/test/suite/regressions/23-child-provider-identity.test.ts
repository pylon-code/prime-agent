import { type FauxRequestPayload, fauxAssistantMessage } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import { startSideQuestion } from "../../../src/core/side-question.js";
import type { ExtensionAPI } from "../../../src/index.js";
import { createHarness, type Harness } from "../harness.js";

/**
 * Stands in for the Meridian extension: `metadata.user_id` is the only session
 * identity channel Anthropic-protocol providers have, and extensions fill it
 * from `ctx.sessionManager.getSessionId()`.
 */
function stampIdentity(observed: string[]) {
	return (pi: ExtensionAPI) => {
		pi.on("before_provider_request", (event, ctx) => {
			const sessionId = ctx.sessionManager.getSessionId();
			observed.push(sessionId);
			return {
				...(event.payload as FauxRequestPayload),
				metadata: { user_id: JSON.stringify({ session_id: sessionId }) },
			};
		});
	};
}

function sentUserIds(harness: Harness): string[] {
	return harness.faux.getSentPayloads().map((payload) => {
		const userId = (payload as FauxRequestPayload).metadata?.user_id;
		return typeof userId === "string" ? (JSON.parse(userId) as { session_id: string }).session_id : "";
	});
}

describe("#23 child-scoped provider identity", () => {
	const harnesses: Harness[] = [];

	afterEach(() => {
		while (harnesses.length > 0) {
			harnesses.pop()?.cleanup();
		}
	});

	it("stamps each inline RLM child with its own session id, never the parent's", async () => {
		const observed: string[] = [];
		const harness = await createHarness({
			persistSession: true,
			extensionFactories: [stampIdentity(observed)],
		});
		harnesses.push(harness);

		harness.setResponses([fauxAssistantMessage("parent answer")]);
		await harness.session.prompt("parent turn");
		const parentSessionId = harness.session.sessionId;
		expect(observed).toEqual([parentSessionId]);

		// Two child turns plus the parent turns its own terminal notices trigger.
		harness.setResponses([
			fauxAssistantMessage("child answer"),
			fauxAssistantMessage("child answer"),
			fauxAssistantMessage("parent noted"),
			fauxAssistantMessage("parent noted"),
		]);
		const first = await harness.session.runRlmChild("first child task");
		const second = await harness.session.runRlmChild("second child task");

		await vi.waitFor(() => {
			expect(harness.session.getRlmChildSession(first.rlm_child_id)?.getLastAssistantText()).toBe("child answer");
			expect(harness.session.getRlmChildSession(second.rlm_child_id)?.getLastAssistantText()).toBe("child answer");
		});

		const childSessionIds = [first, second].map(
			(child) => harness.session.getRlmChildSession(child.rlm_child_id)?.sessionId,
		);
		expect(childSessionIds.every((id) => typeof id === "string" && id.length > 0)).toBe(true);
		expect(new Set(childSessionIds).size).toBe(2);
		expect(childSessionIds).not.toContain(parentSessionId);

		// Exactly the two child requests carried a child identity; before the fix
		// every request in this run interleaved on the parent's key.
		const nonParentIdentities = observed.filter((identity) => identity !== parentSessionId);
		expect(nonParentIdentities).toHaveLength(2);
		expect(new Set(nonParentIdentities)).toEqual(new Set(childSessionIds));
		expect(sentUserIds(harness)).toEqual(observed);
	});

	it("gives a side question an identity derived from its parent instead of the parent's own", async () => {
		const observed: string[] = [];
		const harness = await createHarness({ extensionFactories: [stampIdentity(observed)] });
		harnesses.push(harness);

		harness.setResponses([fauxAssistantMessage("main answer")]);
		await harness.session.prompt("main turn");
		const parentSessionId = harness.session.sessionId;

		harness.setResponses([fauxAssistantMessage("side answer")]);
		const run = startSideQuestion(harness.session, "question-1", "A side question?", () => {});
		await run.done;

		expect(observed).toEqual([parentSessionId, `${parentSessionId}/side:question-1`]);
		expect(sentUserIds(harness)).toEqual(observed);
	});

	it("keys compaction summarization instead of reaching the provider anonymously", async () => {
		const observed: string[] = [];
		const harness = await createHarness({
			settings: { compaction: { keepRecentTokens: 1 } },
			persistSession: true,
			extensionFactories: [stampIdentity(observed)],
		});
		harnesses.push(harness);

		harness.setResponses([
			fauxAssistantMessage("one response"),
			fauxAssistantMessage("two response"),
			fauxAssistantMessage("model-generated summary"),
			fauxAssistantMessage("model-generated turn summary"),
		]);
		await harness.session.prompt("one");
		await harness.session.prompt("two");
		const parentSessionId = harness.session.sessionId;
		const beforeCompaction = observed.length;

		await harness.session.compact();

		const summarizationIdentities = observed.slice(beforeCompaction);
		expect(summarizationIdentities.length).toBeGreaterThan(0);
		for (const identity of summarizationIdentities) {
			expect(identity).toBe(`${parentSessionId}/compaction`);
		}
		expect(sentUserIds(harness)).toEqual(observed);
	});
});
