/**
 * Regression tests: the agent must keep working after an auto-compaction that interrupted
 * unfinished work. BUG A: a skipped/failed threshold compaction that stopped a tool loop must
 * resume it. BUG B: an assistant-text-turn threshold stop reads as "task finished", so an
 * active goal queues its continuation as a session input before compaction.
 */
import type { AgentMessage, ShouldStopAfterTurnContext } from "@earendil-works/pi-agent-core";
import {
	type AssistantMessage,
	fauxAssistantMessage,
	fauxToolCall,
	type ToolResultMessage,
	type Usage,
} from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AgentSession } from "../../src/core/agent-session.js";
import { createHarness, type Harness } from "./harness.js";
import { gatedHook } from "./scheduling.js";

type SessionInternals = {
	_shouldStopAfterTurn: (context: ShouldStopAfterTurnContext) => boolean | Promise<boolean>;
	_runAutoCompaction: (reason: "overflow" | "threshold" | "requested", willRetry: boolean) => Promise<boolean>;
	_performCompaction: (options: {
		model: unknown;
		apiKey: string;
		headers?: Record<string, string>;
		customInstructions?: string;
		signal: AbortSignal;
	}) => Promise<unknown>;
	_continueAfterThresholdCompaction: boolean;
};

function createUsage(totalTokens: number): Usage {
	return {
		input: totalTokens,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

function sumUsage(usages: Usage[]): Usage {
	const total = createUsage(0);
	for (const usage of usages) {
		total.input += usage.input;
		total.output += usage.output;
		total.cacheRead += usage.cacheRead;
		total.cacheWrite += usage.cacheWrite;
		total.totalTokens += usage.totalTokens;
		total.cost.input += usage.cost.input;
		total.cost.output += usage.cost.output;
		total.cost.cacheRead += usage.cost.cacheRead;
		total.cost.cacheWrite += usage.cost.cacheWrite;
		total.cost.total += usage.cost.total;
	}
	return total;
}

function createAssistant(
	harness: Harness,
	options: { stopReason?: AssistantMessage["stopReason"]; totalTokens?: number; timestamp?: number },
): AssistantMessage {
	const model = harness.getModel();
	return {
		...fauxAssistantMessage("", { stopReason: options.stopReason, timestamp: options.timestamp }),
		api: model.api,
		provider: model.provider,
		model: model.id,
		usage: createUsage(options.totalTokens ?? 0),
	};
}

/** Faux ipython tool that services goal.* host requests like the real kernel bridge. */
function createFauxIpythonTool(sessionRef: { current?: AgentSession }) {
	return {
		name: "ipython",
		label: "ipython",
		description: "Execute Python code in the agent kernel.",
		parameters: Type.Object({ code: Type.String() }),
		execute: async (_toolCallId: string, params: unknown) => {
			const session = sessionRef.current;
			if (!session) throw new Error("test session is not initialized");
			const code = (params as { code: string }).code.trim();
			let text = "";
			if (code.startsWith("goal.")) {
				const spaceIndex = code.indexOf(" ");
				const type = spaceIndex < 0 ? code : code.slice(0, spaceIndex);
				const payload = spaceIndex < 0 ? {} : JSON.parse(code.slice(spaceIndex + 1));
				text = JSON.stringify(session.handleGoalHostRequest(type, payload));
			}
			return { content: [{ type: "text" as const, text }], details: {} };
		},
	};
}

function createBigTool() {
	return {
		name: "big",
		label: "big",
		description: "returns big text",
		parameters: Type.Object({}),
		execute: async () => ({
			content: [{ type: "text" as const, text: "x".repeat(40_000) }],
			details: {},
		}),
	};
}

async function createCompactingHarness() {
	return createHarness({
		tools: [createBigTool()],
		settings: { compaction: { enabled: true, reserveTokens: 500, keepRecentTokens: 1 } },
		models: [{ id: "faux-1", contextWindow: 6_000 }],
		persistSession: true,
		extensionFactories: [
			(pi) => {
				pi.on("session_before_compact", async (event) => ({
					compaction: {
						summary: "auto compacted",
						firstKeptEntryId: event.preparation.firstKeptEntryId,
						tokensBefore: event.preparation.tokensBefore,
						details: {},
					},
				}));
			},
		],
	});
}

describe("compaction continuation", () => {
	const harnesses: Harness[] = [];

	afterEach(() => {
		vi.useRealTimers();
		vi.restoreAllMocks();
		while (harnesses.length > 0) harnesses.pop()?.cleanup();
	});

	function midToolLoopContext(harness: Harness): ShouldStopAfterTurnContext {
		const assistant = createAssistant(harness, {
			stopReason: "toolUse",
			totalTokens: 250_000,
			timestamp: Date.now(),
		});
		const toolResult: ToolResultMessage<unknown> = {
			role: "toolResult",
			toolCallId: "call-1",
			toolName: "big",
			content: [{ type: "text", text: "result" }],
			isError: false,
			timestamp: Date.now() + 500,
		};
		const messages: AgentMessage[] = [
			{ role: "user", content: [{ type: "text", text: "hello" }], timestamp: Date.now() - 1000 },
			assistant,
			toolResult,
		];
		harness.session.agent.state.messages = messages;
		return {
			message: assistant,
			toolResults: [toolResult],
			context: { systemPrompt: harness.session.systemPrompt, messages, tools: [] },
			newMessages: [assistant, toolResult],
		};
	}

	it("resumes the interrupted tool loop when a threshold compaction is skipped", async () => {
		vi.useFakeTimers();
		const harness = await createHarness({
			settings: { compaction: { enabled: true, reserveTokens: 1000 } },
			models: [{ id: "faux-1", contextWindow: 200_000 }],
		});
		harnesses.push(harness);
		const internals = harness.session as unknown as SessionInternals;
		const context = midToolLoopContext(harness);

		// toolResult-last makes the session stop the loop for compaction AND continue afterwards.
		const shouldStop = await internals._shouldStopAfterTurn(context);
		expect(shouldStop).toBe(true);
		expect(internals._continueAfterThresholdCompaction).toBe(true);

		const continueSpy = vi.spyOn(harness.session.agent, "continue").mockResolvedValue();

		// The in-memory session has no persisted entries, so _performCompaction throws CompactionSkippedError.
		await internals._runAutoCompaction("threshold", false);
		await vi.advanceTimersByTimeAsync(500);

		const endEvents = harness.eventsOfType("compaction_end");
		expect(endEvents).toHaveLength(1);
		expect(endEvents[0].errorMessage).toContain("skipped");

		expect(continueSpy).toHaveBeenCalledTimes(1);
	});

	it("control: a skipped requested compaction mid tool loop does resume", async () => {
		vi.useFakeTimers();
		const harness = await createHarness({
			settings: { compaction: { enabled: true, reserveTokens: 1000 } },
			models: [{ id: "faux-1", contextWindow: 200_000 }],
		});
		harnesses.push(harness);
		const internals = harness.session as unknown as SessionInternals;
		midToolLoopContext(harness);
		internals._continueAfterThresholdCompaction = true;

		const continueSpy = vi.spyOn(harness.session.agent, "continue").mockResolvedValue();

		await internals._runAutoCompaction("requested", false);
		await vi.advanceTimersByTimeAsync(500);

		expect(continueSpy).toHaveBeenCalledTimes(1);
	});

	it("e2e: tool loop interrupted by a skipped threshold compaction resumes", async () => {
		const bigTool = {
			name: "big",
			label: "big",
			description: "returns big text",
			parameters: Type.Object({}),
			execute: async () => ({
				content: [{ type: "text" as const, text: "x".repeat(40_000) }],
				details: {},
			}),
		};
		const harness = await createHarness({
			tools: [bigTool],
			// Huge keepRecentTokens: prepareCompaction finds nothing to summarize and throws CompactionSkippedError.
			settings: { compaction: { enabled: true, reserveTokens: 500, keepRecentTokens: 1_000_000 } },
			models: [{ id: "faux-1", contextWindow: 6_000 }],
			persistSession: true,
		});
		harnesses.push(harness);
		harness.setResponses([
			fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" }),
			fauxAssistantMessage("final answer after the tool call"),
		]);

		await harness.session.prompt("run the tool then summarize");
		await new Promise((resolve) => setTimeout(resolve, 300));
		await harness.session.waitForIdle();
		await new Promise((resolve) => setTimeout(resolve, 300));

		expect(harness.eventsOfType("compaction_start").map((event) => event.reason)).toContain("threshold");
		expect(harness.eventsOfType("compaction_end")[0]?.errorMessage).toContain("skipped");
		expect(harness.getPendingResponseCount()).toBe(0);
	});

	it("keeps correlated lifecycle ownership through a successful post-compaction continuation", async () => {
		const harness = await createCompactingHarness();
		harnesses.push(harness);
		const beforeCompact = fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" });
		const afterCompact = fauxAssistantMessage("final answer after successful compaction");
		harness.setResponses([beforeCompact, afterCompact]);
		const correlationId = "post-compaction-1";

		await harness.session.prompt("run the tool then summarize", { promptCorrelationId: correlationId });
		await harness.session.waitForHeadlessIdle();

		expect(harness.eventsOfType("compaction_end").find((event) => event.result)?.result).toBeDefined();
		const assistantEnds = harness.events.filter(
			(event) => event.type === "message_end" && event.message.role === "assistant",
		);
		expect(assistantEnds).toHaveLength(2);
		expect(assistantEnds.every((event) => event.promptCorrelationId === correlationId)).toBe(true);
		const expectedUsage = sumUsage(
			harness.events.flatMap((event) =>
				event.type === "message_end" && event.message.role === "assistant" ? [event.message.usage] : [],
			),
		);
		const lifecycle = harness
			.eventsOfType("prompt_lifecycle")
			.filter((event) => event.correlationId === correlationId);
		expect(lifecycle.map((event) => event.phase)).toEqual(["owned", "delivered", "completed"]);
		expect(lifecycle.filter((event) => ["completed", "cancelled", "failed"].includes(event.phase))).toEqual([
			expect.objectContaining({ phase: "completed", usage: expectedUsage }),
		]);
		expect(harness.session.getPromptLifecycle(correlationId)).toMatchObject({
			phase: "completed",
			usage: expectedUsage,
		});
		expect(harness.getPendingResponseCount()).toBe(0);
		expect(harness.session.getLastAssistantText()).toBe("final answer after successful compaction");
	});

	it("does not correlate unrelated session events while a continuation streams", async () => {
		let markContinuationStarted = () => {};
		const continuationStarted = new Promise<void>((resolve) => {
			markContinuationStarted = resolve;
		});
		let releaseContinuation = () => {};
		const continuationGate = new Promise<void>((resolve) => {
			releaseContinuation = resolve;
		});
		const harness = await createCompactingHarness();
		harnesses.push(harness);
		harness.setResponses([
			fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" }),
			async () => {
				markContinuationStarted();
				await continuationGate;
				return fauxAssistantMessage("final answer after successful compaction");
			},
		]);

		await harness.session.prompt("foreground", { promptCorrelationId: "streaming-continuation" });
		await continuationStarted;
		harness.session.handleGoalHostRequest("goal.create", { objective: "unrelated background goal" });

		expect(harness.eventsOfType("goal_update").at(-1)?.promptCorrelationId).toBeNull();
		releaseContinuation();
		await harness.session.waitForHeadlessIdle();
		expect(
			harness.events.filter((event) => event.type === "message_end" && event.message.role === "assistant").at(-1)
				?.promptCorrelationId,
		).toBe("streaming-continuation");
	});

	it("retains correlation when a turn reuses an older parked continuation settlement", async () => {
		let markBackgroundStarted = () => {};
		const backgroundStarted = new Promise<void>((resolve) => {
			markBackgroundStarted = resolve;
		});
		let releaseBackground = () => {};
		const backgroundGate = new Promise<void>((resolve) => {
			releaseBackground = resolve;
		});
		const harness = await createCompactingHarness();
		harnesses.push(harness);
		harness.setResponses([
			async () => {
				markBackgroundStarted();
				await backgroundGate;
				return fauxAssistantMessage("background complete");
			},
			fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" }),
			fauxAssistantMessage("final answer after compaction"),
		]);

		const background = harness.session.prompt("background");
		await backgroundStarted;
		const internals = harness.session as unknown as { _schedulePostCompactionContinue(): void };
		internals._schedulePostCompactionContinue();
		await new Promise<void>(setImmediate);
		await harness.session.prompt("foreground", {
			streamingBehavior: "followUp",
			queueIfBusy: true,
			promptCorrelationId: "shared-settlement",
		});
		releaseBackground();
		await background;
		await harness.session.waitForHeadlessIdle();

		const foregroundAssistantEnds = harness.events
			.flatMap((event) => (event.type === "message_end" && event.message.role === "assistant" ? [event] : []))
			.slice(-2);
		const foregroundUsage = harness.events
			.flatMap((event) =>
				event.type === "message_end" && event.message.role === "assistant" ? [event.message.usage] : [],
			)
			.slice(-2);
		expect(foregroundAssistantEnds.map((event) => event.promptCorrelationId)).toEqual([
			"shared-settlement",
			"shared-settlement",
		]);
		expect(harness.session.getPromptLifecycle("shared-settlement")).toMatchObject({
			phase: "completed",
			usage: sumUsage(foregroundUsage),
		});
	});

	it("does not let an unrelated compaction claim a parked settlement", async () => {
		const beforeStart = gatedHook({ prompt: "foreground" });
		const harness = await createHarness({
			persistSession: true,
			extensionFactories: [beforeStart.factory],
		});
		harnesses.push(harness);
		harness.setResponses([fauxAssistantMessage("background"), fauxAssistantMessage("foreground done")]);

		await harness.session.prompt("background");
		const foreground = harness.session.prompt("foreground", { promptCorrelationId: "unrelated-compaction" });
		await beforeStart.reached;
		const internals = harness.session as unknown as {
			_schedulePostCompactionContinue(): void;
			_postCompactionContinuationSettlement?: object;
			_postCompactionPromptOwners: WeakMap<object, string>;
		};
		internals._schedulePostCompactionContinue();
		await harness.session.compact(undefined, { skipAbort: true }).catch(() => undefined);
		const settlement = internals._postCompactionContinuationSettlement;
		expect(settlement).toBeDefined();

		beforeStart.release();
		await foreground;
		expect(internals._postCompactionPromptOwners.get(settlement!)).toBeUndefined();
		await harness.session.waitForHeadlessIdle();
	});

	it("adopts an older settlement when a skipped compaction schedules recovery", async () => {
		let markBackgroundStarted = () => {};
		const backgroundStarted = new Promise<void>((resolve) => {
			markBackgroundStarted = resolve;
		});
		let releaseBackground = () => {};
		const backgroundGate = new Promise<void>((resolve) => {
			releaseBackground = resolve;
		});
		const harness = await createHarness({
			tools: [createBigTool()],
			settings: { compaction: { enabled: true, reserveTokens: 500, keepRecentTokens: 1 } },
			models: [{ id: "faux-1", contextWindow: 6_000 }],
			persistSession: true,
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async () => ({ cancel: true }));
				},
			],
		});
		harnesses.push(harness);
		harness.setResponses([
			async () => {
				markBackgroundStarted();
				await backgroundGate;
				return fauxAssistantMessage("background complete");
			},
			fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" }),
		]);

		const background = harness.session.prompt("background");
		await backgroundStarted;
		const internals = harness.session as unknown as {
			_schedulePostCompactionContinue(): void;
			_postCompactionContinuationSettlement?: object;
			_postCompactionPromptOwners: WeakMap<object, string>;
		};
		internals._schedulePostCompactionContinue();
		const olderSettlement = internals._postCompactionContinuationSettlement;
		expect(olderSettlement).toBeDefined();
		let releaseOlderContinuation = () => {};
		const olderContinuation = new Promise<void>((resolve) => {
			releaseOlderContinuation = resolve;
		});
		vi.spyOn(harness.session.agent, "continue").mockReturnValue(olderContinuation);
		await harness.session.prompt("foreground", {
			streamingBehavior: "followUp",
			queueIfBusy: true,
			promptCorrelationId: "cancelled-compaction",
		});
		releaseBackground();
		await background;
		await harness.session.waitForIdle();

		expect(internals._postCompactionPromptOwners.get(olderSettlement!)).toBe("cancelled-compaction");
		expect(harness.session.getPromptLifecycle("cancelled-compaction")).toMatchObject({ phase: "delivered" });
		releaseOlderContinuation();
		await harness.session.waitForHeadlessIdle();
		expect(harness.session.getPromptLifecycle("cancelled-compaction")).toMatchObject({ phase: "completed" });
	});

	it("transfers correlated ownership across queued manual compaction", async () => {
		let markProviderStarted = () => {};
		const providerStarted = new Promise<void>((resolve) => {
			markProviderStarted = resolve;
		});
		let releaseProvider = () => {};
		const providerGate = new Promise<void>((resolve) => {
			releaseProvider = resolve;
		});
		const harness = await createCompactingHarness();
		harnesses.push(harness);
		harness.setResponses([
			async () => {
				markProviderStarted();
				await providerGate;
				return fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" });
			},
			fauxAssistantMessage("final answer after manual compaction"),
		]);

		const foreground = harness.session.prompt("foreground", { promptCorrelationId: "manual-transfer" });
		await providerStarted;
		await harness.session.prompt("/compact", {
			streamingBehavior: "followUp",
			queueIfBusy: true,
		});
		releaseProvider();
		await foreground;
		await harness.session.waitForHeadlessIdle();

		expect(harness.eventsOfType("compaction_start").map((event) => event.reason)).toEqual(["threshold", "manual"]);
		const assistantCorrelations = harness.events.flatMap((event) =>
			event.type === "message_end" && event.message.role === "assistant" ? [event.promptCorrelationId] : [],
		);
		const assistantUsage = harness.events.flatMap((event) =>
			event.type === "message_end" && event.message.role === "assistant" ? [event.message.usage] : [],
		);
		expect(assistantCorrelations).toEqual(["manual-transfer", "manual-transfer"]);
		expect(harness.session.getPromptLifecycle("manual-transfer")).toMatchObject({
			phase: "completed",
			usage: sumUsage(assistantUsage),
		});
	});

	it("fails a delivered lifecycle when disposal cancels its continuation", async () => {
		const harness = await createCompactingHarness();
		harnesses.push(harness);
		harness.setResponses([fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" })]);
		let releaseContinue = () => {};
		const pendingContinue = new Promise<void>((resolve) => {
			releaseContinue = resolve;
		});
		vi.spyOn(harness.session.agent, "continue").mockReturnValue(pendingContinue);

		await harness.session.prompt("foreground", { promptCorrelationId: "disposed-continuation" });
		expect(harness.session.getPromptLifecycle("disposed-continuation")).toMatchObject({ phase: "delivered" });
		harness.session.dispose();

		expect(harness.session.getPromptLifecycle("disposed-continuation")).toMatchObject({ phase: "failed" });
		releaseContinue();
	});

	it("fails rather than completing when continuation message persistence rejects", async () => {
		vi.useFakeTimers();
		const harness = await createCompactingHarness();
		harnesses.push(harness);
		harness.setResponses([
			fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" }),
			fauxAssistantMessage("final continuation answer"),
		]);
		const appendMessage = harness.session.sessionManager.appendMessage.bind(harness.session.sessionManager);
		let persistenceFailed = false;
		vi.spyOn(harness.session.sessionManager, "appendMessage").mockImplementation((message) => {
			const text =
				message.role === "assistant"
					? message.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("")
					: "";
			if (!persistenceFailed && text.includes("final continuation answer")) {
				persistenceFailed = true;
				throw new Error("synthetic assistant persistence failure");
			}
			return appendMessage(message);
		});

		await harness.session.prompt("foreground", { promptCorrelationId: "failed-event-persistence" });
		const headlessIdle = harness.session.waitForHeadlessIdle();
		const rejectedIdle = expect(headlessIdle).rejects.toThrow("Correlated prompt event processing failed");
		await vi.advanceTimersByTimeAsync(100);
		await rejectedIdle;

		expect(persistenceFailed).toBe(true);
		expect(harness.session.getPromptLifecycle("failed-event-persistence")).toMatchObject({ phase: "failed" });
		expect(
			harness
				.eventsOfType("prompt_lifecycle")
				.flatMap((event) =>
					event.promptCorrelationId === "failed-event-persistence" &&
					(event.phase === "completed" || event.phase === "failed")
						? [event.phase]
						: [],
				),
		).toEqual(["failed"]);
		expect(harness.session.hasNonterminalPromptLifecycle).toBe(false);
	});

	it("fails a deferred lifecycle when the current event queue tail rejects", async () => {
		vi.useFakeTimers();
		const harness = await createCompactingHarness();
		harnesses.push(harness);
		harness.setResponses([fauxAssistantMessage(fauxToolCall("big", {}), { stopReason: "toolUse" })]);
		let releaseContinue = () => {};
		const pendingContinue = new Promise<void>((resolve) => {
			releaseContinue = resolve;
		});
		const continueSpy = vi.spyOn(harness.session.agent, "continue").mockReturnValue(pendingContinue);

		await harness.session.prompt("foreground", { promptCorrelationId: "failed-event-queue-tail" });
		await vi.advanceTimersByTimeAsync(100);
		expect(continueSpy).toHaveBeenCalledTimes(1);
		const internals = harness.session as unknown as { _agentEventQueue: Promise<void> };
		const failedQueue = Promise.reject(new Error("event persistence failed"));
		void failedQueue.catch(() => undefined);
		internals._agentEventQueue = failedQueue;
		releaseContinue();
		await vi.waitFor(() => {
			expect(harness.session.getPromptLifecycle("failed-event-queue-tail")).toMatchObject({ phase: "failed" });
		});
		expect(harness.session.hasNonterminalPromptLifecycle).toBe(false);
	});

	it("rejects headless idle waiters when a continuation cannot start", async () => {
		vi.useFakeTimers();
		const harness = await createHarness();
		harnesses.push(harness);
		const sessionInternals = harness.session as unknown as {
			_schedulePostCompactionContinue(): void;
		};
		vi.spyOn(harness.session.agent, "continue").mockRejectedValueOnce(new Error("continuation failed"));

		sessionInternals._schedulePostCompactionContinue();
		const idle = harness.session.waitForHeadlessIdle();
		const rejectedIdle = expect(idle).rejects.toThrow("continuation failed");
		await vi.advanceTimersByTimeAsync(100);

		await rejectedIdle;
	});

	it("does not expose a failed continuation to later headless idle waiters", async () => {
		vi.useFakeTimers();
		const harness = await createHarness();
		harnesses.push(harness);
		const sessionInternals = harness.session as unknown as {
			_schedulePostCompactionContinue(): void;
		};
		vi.spyOn(harness.session.agent, "continue").mockRejectedValueOnce(new Error("continuation failed"));

		sessionInternals._schedulePostCompactionContinue();
		await vi.advanceTimersByTimeAsync(100);

		await expect(harness.session.waitForHeadlessIdle()).resolves.toBeUndefined();
	});

	// BUG B (end-to-end): unlike the tests above, the threshold compaction here SUCCEEDS.
	it("e2e: an active goal keeps continuing after a successful threshold compaction", async () => {
		const sessionRef: { current?: AgentSession } = {};
		const harness = await createHarness({
			tools: [createFauxIpythonTool(sessionRef)],
			// Let a running goal continuation cross the threshold while remaining well below overflow.
			settings: { compaction: { enabled: true, reserveTokens: 8_000, keepRecentTokens: 1 } },
			models: [{ id: "faux-1", contextWindow: 10_000 }],
			persistSession: true,
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => ({
						compaction: {
							summary: "auto compacted",
							firstKeptEntryId: event.preparation.firstKeptEntryId,
							tokensBefore: event.preparation.tokensBefore,
							details: {},
						},
					}));
				},
			],
		});
		harnesses.push(harness);
		sessionRef.current = harness.session;
		const largeStep = "x".repeat(3_500);
		harness.setResponses([
			fauxAssistantMessage(`step one done, more to do ${largeStep}`),
			fauxAssistantMessage(`step two done, still more to do ${largeStep}`),
			fauxAssistantMessage(`step three done, still not finished ${largeStep}`),
			fauxAssistantMessage(fauxToolCall("ipython", { code: "goal.complete" }), { stopReason: "toolUse" }),
			fauxAssistantMessage("Goal complete."),
		]);

		await harness.session.prompt("/goal finish the task");
		await vi.waitFor(
			() => {
				const compactionReasons = harness.eventsOfType("compaction_start").map((event) => event.reason);
				expect(compactionReasons).toContain("threshold");
				expect(compactionReasons).not.toContain("overflow");
				expect(harness.eventsOfType("compaction_end").find((event) => event.result)?.result).toBeDefined();
				expect(harness.getPendingResponseCount()).toBe(0);
				expect(harness.session.goalState.status).toBe("complete");
			},
			{ timeout: 5_000 },
		);
	});

	// With both drivers active the goal continuation takes exclusive priority, matching _getContinuationMessages.
	it("queues only the goal continuation when a goal and autonomous mode are both active", async () => {
		const sessionRef: { current?: AgentSession } = {};
		const harness = await createHarness({
			tools: [createFauxIpythonTool(sessionRef)],
			autonomous: { enabled: true, maxContinuations: 5 },
			settings: { compaction: { enabled: true, reserveTokens: 1000 } },
			models: [{ id: "faux-1", contextWindow: 200_000 }],
		});
		harnesses.push(harness);
		sessionRef.current = harness.session;
		harness.session.handleGoalHostRequest("goal.create", { objective: "finish the task" });
		const internals = harness.session as unknown as SessionInternals;
		const context = midToolLoopContext(harness);

		const shouldStop = await internals._shouldStopAfterTurn(context);
		expect(shouldStop).toBe(true);
		expect(internals._continueAfterThresholdCompaction).toBe(true);

		expect(harness.session.queuedActionCount).toBe(1);
		expect(harness.session.goalState.continuationsUsed).toBe(1);
		expect(harness.session.getAutonomousStatus().continuationsUsed).toBe(0);
	});

	// A user-cancelled compaction must withdraw the goal continuation queued for it.
	it("withdraws the queued goal continuation when the threshold compaction is cancelled", async () => {
		const sessionRef: { current?: AgentSession } = {};
		const harness = await createHarness({
			tools: [createFauxIpythonTool(sessionRef)],
			settings: { compaction: { enabled: true, reserveTokens: 1000 } },
			models: [{ id: "faux-1", contextWindow: 200_000 }],
		});
		harnesses.push(harness);
		sessionRef.current = harness.session;
		harness.session.handleGoalHostRequest("goal.create", { objective: "finish the task" });
		const internals = harness.session as unknown as SessionInternals;
		const context = midToolLoopContext(harness);

		const shouldStop = await internals._shouldStopAfterTurn(context);
		expect(shouldStop).toBe(true);
		expect(harness.session.queuedActionCount).toBe(1);
		expect(harness.session.goalState.continuationsUsed).toBe(1);

		vi.spyOn(internals, "_performCompaction").mockRejectedValue(new Error("Compaction cancelled"));
		await internals._runAutoCompaction("threshold", false);

		const endEvents = harness.eventsOfType("compaction_end");
		expect(endEvents).toHaveLength(1);
		expect(endEvents[0].aborted).toBe(true);
		expect(harness.session.queuedActionCount).toBe(0);
		expect(harness.session.goalState.continuationsUsed).toBe(0);

		// The cancellation must not consume the continuation: the next natural threshold stop re-queues it.
		const shouldStopAgain = await internals._shouldStopAfterTurn(context);
		expect(shouldStopAgain).toBe(true);
		expect(harness.session.queuedActionCount).toBe(1);
		expect(harness.session.goalState.continuationsUsed).toBe(1);
	});

	// A stale marker (continuation already consumed, goal completed) must not be rolled back.
	it("keeps completed-goal bookkeeping when a later threshold compaction is cancelled", async () => {
		const sessionRef: { current?: AgentSession } = {};
		const harness = await createHarness({
			tools: [createFauxIpythonTool(sessionRef)],
			settings: { compaction: { enabled: true, reserveTokens: 1000 } },
			models: [{ id: "faux-1", contextWindow: 200_000 }],
		});
		harnesses.push(harness);
		sessionRef.current = harness.session;
		harness.session.handleGoalHostRequest("goal.create", { objective: "finish the task" });
		const internals = harness.session as unknown as SessionInternals;
		const context = midToolLoopContext(harness);

		await internals._shouldStopAfterTurn(context);
		expect(harness.session.goalState.continuationsUsed).toBe(1);

		// Completing the goal clears the queued continuation but leaves the marker stale.
		harness.session.handleGoalHostRequest("goal.complete");
		expect(harness.session.queuedActionCount).toBe(0);

		const shouldStop = await internals._shouldStopAfterTurn(context);
		expect(shouldStop).toBe(true);
		vi.spyOn(internals, "_performCompaction").mockRejectedValue(new Error("Compaction cancelled"));
		await internals._runAutoCompaction("threshold", false);

		expect(harness.session.goalState.status).toBe("complete");
		expect(harness.session.goalState.continuationsUsed).toBe(1);
	});
});
