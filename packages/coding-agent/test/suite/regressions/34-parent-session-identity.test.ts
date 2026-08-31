import { type FauxRequestPayload, fauxAssistantMessage } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AgentSession } from "../../../src/core/agent-session.js";
import { SessionManager } from "../../../src/core/session-manager.js";
import { startSideQuestion } from "../../../src/core/side-question.js";
import type { ExtensionAPI } from "../../../src/index.js";
import { createHarness, type Harness } from "../harness.js";

interface Lineage {
	sessionId: string;
	parentSessionId?: string;
}

/**
 * Stands in for the Meridian extension: `metadata.user_id` is the only session
 * identity channel Anthropic-protocol providers have, and the proxy-side
 * cancellation registry needs the parent edge in the same envelope.
 */
function stampLineage(observed: Lineage[]) {
	return (pi: ExtensionAPI) => {
		pi.on("before_provider_request", (event, ctx) => {
			const lineage: Lineage = {
				sessionId: ctx.sessionManager.getSessionId(),
				parentSessionId: ctx.sessionManager.getParentSessionId(),
			};
			observed.push(lineage);
			return {
				...(event.payload as FauxRequestPayload),
				metadata: {
					user_id: JSON.stringify({
						session_id: lineage.sessionId,
						parent_session_id: lineage.parentSessionId,
					}),
				},
			};
		});
	};
}

function sentLineages(harness: Harness): Lineage[] {
	return harness.faux.getSentPayloads().map((payload) => {
		const userId = (payload as FauxRequestPayload).metadata?.user_id;
		if (typeof userId !== "string") {
			return { sessionId: "" };
		}
		const envelope = JSON.parse(userId) as { session_id: string; parent_session_id?: string };
		return { sessionId: envelope.session_id, parentSessionId: envelope.parent_session_id };
	});
}

function requireChildSession(session: AgentSession, childId: string): AgentSession {
	const child = session.getRlmChildSession(childId);
	if (!child) {
		throw new Error(`RLM child session ${childId} was never published`);
	}
	return child;
}

async function spawnAnsweredChild(parent: AgentSession, prompt: string): Promise<AgentSession> {
	const handle = await parent.runRlmChild(prompt);
	await vi.waitFor(() => {
		expect(requireChildSession(parent, handle.rlm_child_id).getLastAssistantText()).toBe("child answer");
	});
	return requireChildSession(parent, handle.rlm_child_id);
}

function lineageOf(observed: Lineage[], sessionId: string): Lineage | undefined {
	return observed.find((lineage) => lineage.sessionId === sessionId);
}

describe("#34 parent session identity in extension contexts", () => {
	const harnesses: Harness[] = [];

	afterEach(() => {
		while (harnesses.length > 0) {
			harnesses.pop()?.cleanup();
		}
	});

	it("reports no parent for a root session and the spawning session for an inline RLM child", async () => {
		const observed: Lineage[] = [];
		const harness = await createHarness({
			persistSession: true,
			extensionFactories: [stampLineage(observed)],
		});
		harnesses.push(harness);

		harness.setResponses([fauxAssistantMessage("parent answer")]);
		await harness.session.prompt("parent turn");
		const rootSessionId = harness.session.sessionId;
		expect(observed).toEqual([{ sessionId: rootSessionId, parentSessionId: undefined }]);

		harness.setResponses([fauxAssistantMessage("child answer"), fauxAssistantMessage("parent noted")]);
		const child = await spawnAnsweredChild(harness.session, "child task");

		expect(child.sessionId).not.toBe(rootSessionId);
		expect(lineageOf(observed, child.sessionId)).toEqual({
			sessionId: child.sessionId,
			parentSessionId: rootSessionId,
		});
		// The root keeps reporting no parent, including on its terminal-notice turn.
		for (const lineage of observed.filter((entry) => entry.sessionId === rootSessionId)) {
			expect(lineage.parentSessionId).toBeUndefined();
		}
		// Both fields reach the provider payload inside the envelope the extension
		// stamped, which is the contract the proxy registry reads.
		expect(sentLineages(harness)).toEqual(observed);
	});

	it("names the immediate parent, not the root, for a grandchild", async () => {
		const observed: Lineage[] = [];
		const harness = await createHarness({
			persistSession: true,
			extensionFactories: [stampLineage(observed)],
		});
		harnesses.push(harness);

		// Uniform answers: the child, the grandchild, and the terminal-notice turns
		// they trigger on their parents all draw from this one queue.
		harness.setResponses(Array.from({ length: 6 }, () => fauxAssistantMessage("child answer")));
		const rootSessionId = harness.session.sessionId;
		const child = await spawnAnsweredChild(harness.session, "child task");
		const grandchild = await spawnAnsweredChild(child, "grandchild task");

		expect(new Set([rootSessionId, child.sessionId, grandchild.sessionId]).size).toBe(3);
		expect(lineageOf(observed, grandchild.sessionId)).toEqual({
			sessionId: grandchild.sessionId,
			parentSessionId: child.sessionId,
		});
		expect(sentLineages(harness)).toEqual(observed);
	});

	it("reports the owning session's parent for a scoped auxiliary request", async () => {
		const observed: Lineage[] = [];
		const harness = await createHarness({
			persistSession: true,
			extensionFactories: [stampLineage(observed)],
		});
		harnesses.push(harness);

		harness.setResponses([fauxAssistantMessage("child answer"), fauxAssistantMessage("parent noted")]);
		const rootSessionId = harness.session.sessionId;
		const child = await spawnAnsweredChild(harness.session, "child task");

		harness.setResponses([fauxAssistantMessage("side answer")]);
		const run = startSideQuestion(child, "question-1", "A side question?", () => {});
		await run.done;

		// The scope changes only the id; the parent edge still describes the owner.
		expect(lineageOf(observed, `${child.sessionId}/side:question-1`)).toEqual({
			sessionId: `${child.sessionId}/side:question-1`,
			parentSessionId: rootSessionId,
		});
		expect(sentLineages(harness)).toEqual(observed);
	});

	it("reports no parent for a root session's compaction scope", async () => {
		const observed: Lineage[] = [];
		const harness = await createHarness({
			settings: { compaction: { keepRecentTokens: 1 } },
			persistSession: true,
			extensionFactories: [stampLineage(observed)],
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
		const rootSessionId = harness.session.sessionId;
		const beforeCompaction = observed.length;

		await harness.session.compact();

		const summarization = observed.slice(beforeCompaction);
		expect(summarization.length).toBeGreaterThan(0);
		for (const lineage of summarization) {
			expect(lineage).toEqual({ sessionId: `${rootSessionId}/compaction`, parentSessionId: undefined });
		}
		expect(sentLineages(harness)).toEqual(observed);
	});

	it("keeps the parent id when a child session is reopened from disk", async () => {
		const observed: Lineage[] = [];
		const harness = await createHarness({
			persistSession: true,
			extensionFactories: [stampLineage(observed)],
		});
		harnesses.push(harness);

		harness.setResponses([fauxAssistantMessage("child answer"), fauxAssistantMessage("parent noted")]);
		const rootSessionId = harness.session.sessionId;
		const child = await spawnAnsweredChild(harness.session, "child task");
		const childSessionFile = child.sessionFile;
		if (childSessionFile === undefined) {
			throw new Error("Persisted RLM child is missing a session file");
		}

		const reopened = SessionManager.open(childSessionFile);
		expect(reopened.getSessionId()).toBe(child.sessionId);
		expect(reopened.getParentSessionId()).toBe(rootSessionId);
	});
});
