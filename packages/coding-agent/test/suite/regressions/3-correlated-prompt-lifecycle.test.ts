import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AgentSessionEvent } from "../../../src/core/agent-session.js";
import { createHarness, type Harness } from "../harness.js";
import { gatedHook } from "../scheduling.js";

describe("issue #3 correlated prompt lifecycle", () => {
	const harnesses: Harness[] = [];

	afterEach(() => {
		while (harnesses.length > 0) harnesses.pop()?.cleanup();
	});

	it("keeps predecessor events background-scoped until a queued prompt is delivered", async () => {
		let markBackgroundStarted = () => {};
		const backgroundStarted = new Promise<void>((resolve) => {
			markBackgroundStarted = resolve;
		});
		let releaseBackground = () => {};
		const backgroundGate = new Promise<void>((resolve) => {
			releaseBackground = resolve;
		});
		const harness = await createHarness();
		harnesses.push(harness);
		harness.setResponses([
			async () => {
				markBackgroundStarted();
				await backgroundGate;
				return fauxAssistantMessage("background complete");
			},
			fauxAssistantMessage("foreground complete"),
		]);
		const events: AgentSessionEvent[] = [];
		harness.session.subscribe((event) => events.push(event));

		const background = harness.session.prompt("background");
		await backgroundStarted;
		await harness.session.prompt("foreground", {
			streamingBehavior: "followUp",
			queueIfBusy: true,
			promptCorrelationId: "foreground-1",
		});

		expect(events.filter((event) => event.type === "prompt_lifecycle").map((event) => event.phase)).toEqual([
			"owned",
			"queued",
		]);
		expect(events.filter((event) => event.promptCorrelationId === null).length).toBeGreaterThan(0);

		releaseBackground();
		await background;
		await harness.session.waitForIdle();

		const lifecyclePhases = events.flatMap((event) =>
			event.type === "prompt_lifecycle" && event.promptCorrelationId === "foreground-1" ? [event.phase] : [],
		);
		expect(lifecyclePhases).toEqual(["owned", "queued", "delivered", "completed"]);
		const deliveredIndex = events.findIndex(
			(event) => event.type === "prompt_lifecycle" && event.phase === "delivered",
		);
		const firstOwnedPayloadIndex = events.findIndex(
			(event) => event.type !== "prompt_lifecycle" && event.promptCorrelationId === "foreground-1",
		);
		expect(deliveredIndex).toBeGreaterThanOrEqual(0);
		expect(firstOwnedPayloadIndex).toBeGreaterThan(deliveredIndex);
	});

	it("fails an ordinary correlated turn when a recovered event persistence step rejects", async () => {
		const harness = await createHarness({ persistSession: true });
		harnesses.push(harness);
		harness.setResponses([
			fauxAssistantMessage("ordinary persistence failure"),
			fauxAssistantMessage("subsequent prompt succeeds"),
		]);
		const appendMessage = harness.session.sessionManager.appendMessage.bind(harness.session.sessionManager);
		let persistenceFailed = false;
		vi.spyOn(harness.session.sessionManager, "appendMessage").mockImplementation((message) => {
			if (!persistenceFailed && message.role === "assistant") {
				persistenceFailed = true;
				throw new Error("synthetic ordinary persistence failure");
			}
			return appendMessage(message);
		});

		await expect(
			harness.session.prompt("foreground", { promptCorrelationId: "ordinary-persistence-failure" }),
		).rejects.toThrow("Correlated prompt event processing failed");

		expect(persistenceFailed).toBe(true);
		expect(harness.session.getPromptLifecycle("ordinary-persistence-failure")).toMatchObject({
			phase: "failed",
			deliveryCrossed: true,
		});
		expect(
			harness
				.eventsOfType("prompt_lifecycle")
				.flatMap((event) =>
					event.promptCorrelationId === "ordinary-persistence-failure" &&
					(event.phase === "completed" || event.phase === "failed")
						? [event.phase]
						: [],
				),
		).toEqual(["failed"]);

		await harness.session.prompt("subsequent", { promptCorrelationId: "ordinary-persistence-recovery" });
		expect(harness.session.getPromptLifecycle("ordinary-persistence-recovery")).toMatchObject({
			phase: "completed",
			deliveryCrossed: true,
		});
	});

	it("does not admit a correlated prompt after session replacement is fenced", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		const release = harness.session.acquireSessionReplacementFence();
		try {
			await expect(
				harness.session.prompt("foreground", { promptCorrelationId: "replacement-race" }),
			).rejects.toThrow("A session replacement or reload is already in progress");
			expect(harness.session.getPromptLifecycle("replacement-race")).toBeUndefined();
		} finally {
			release();
		}
	});

	it("cancels an owned prompt before an action can be inserted", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		const session = harness.session as unknown as {
			_acquireDirectTurnAdmissionFence: (signal?: AbortSignal) => Promise<{ owner: symbol; release: () => void }>;
		};
		session._acquireDirectTurnAdmissionFence = (signal) =>
			new Promise((_, reject) => {
				signal?.addEventListener("abort", () => reject(new Error("cancelled before action insertion")), {
					once: true,
				});
			});

		const prompt = harness.session.prompt("foreground", { promptCorrelationId: "owned-1" });
		expect(harness.session.getPromptLifecycle("owned-1")).toMatchObject({
			phase: "owned",
			deliveryCrossed: false,
		});
		expect(harness.session.cancelPromptLifecycle("owned-1")).toMatchObject({
			status: "cancelled",
			deliveryCrossed: false,
		});
		await expect(prompt).rejects.toThrow();
		expect(harness.session.getPromptLifecycle("owned-1")).toMatchObject({ phase: "cancelled" });
	});

	it("keeps preparation cancellable before model delivery", async () => {
		const beforeStart = gatedHook({ prompt: "foreground" });
		const harness = await createHarness({ extensionFactories: [beforeStart.factory] });
		harnesses.push(harness);
		harness.setResponses([fauxAssistantMessage("must not run")]);

		const prompt = harness.session.prompt("foreground", { promptCorrelationId: "preparing-1" });
		await beforeStart.reached;
		expect(harness.session.getPromptLifecycle("preparing-1")).toMatchObject({
			phase: "owned",
			deliveryCrossed: false,
		});
		expect(harness.session.cancelPromptLifecycle("preparing-1")).toMatchObject({
			status: "cancelled",
			deliveryCrossed: false,
		});
		beforeStart.release();
		await expect(prompt).rejects.toThrow("Correlated prompt was cancelled before delivery");
		expect(harness.session.getPromptLifecycle("preparing-1")).toMatchObject({
			phase: "cancelled",
			deliveryCrossed: false,
		});
	});

	it("fails a preparing lifecycle on recovery instead of replaying its hook", async () => {
		const beforeStart = gatedHook({ prompt: "foreground" });
		const source = await createHarness({ extensionFactories: [beforeStart.factory] });
		harnesses.push(source);
		source.setResponses([fauxAssistantMessage("must not run")]);
		const prompt = source.session.prompt("foreground", { promptCorrelationId: "preparing-recovery" });
		await beforeStart.reached;

		const recovery = source.session.getSessionActionRecoverySnapshot();
		expect(recovery.actions).toEqual([]);
		expect(recovery.promptLifecycles?.records).toContainEqual(
			expect.objectContaining({ correlationId: "preparing-recovery", deliveryCrossed: false }),
		);

		const replacement = await createHarness();
		harnesses.push(replacement);
		await replacement.session.restoreSessionActions(recovery);
		expect(replacement.session.getPromptLifecycle("preparing-recovery")).toMatchObject({
			phase: "failed",
			deliveryCrossed: false,
		});

		expect(source.session.cancelPromptLifecycle("preparing-recovery")).toMatchObject({ status: "cancelled" });
		beforeStart.release();
		await expect(prompt).rejects.toThrow("Correlated prompt was cancelled before delivery");
	});

	it("cancels a queued correlated prompt while post-compaction continuation is parked", async () => {
		let markBackgroundStarted = () => {};
		const backgroundStarted = new Promise<void>((resolve) => {
			markBackgroundStarted = resolve;
		});
		let releaseBackground = () => {};
		const backgroundGate = new Promise<void>((resolve) => {
			releaseBackground = resolve;
		});
		const foregroundProvider = vi.fn(() => fauxAssistantMessage("must not run"));
		const harness = await createHarness();
		harnesses.push(harness);
		harness.setResponses([
			async () => {
				markBackgroundStarted();
				await backgroundGate;
				return fauxAssistantMessage("background complete");
			},
			foregroundProvider,
		]);

		const background = harness.session.prompt("background");
		await backgroundStarted;
		const internals = harness.session as unknown as { _schedulePostCompactionContinue(): void };
		const continueSpy = vi.spyOn(harness.session.agent, "continue");
		internals._schedulePostCompactionContinue();
		await new Promise<void>(setImmediate);
		expect(continueSpy).not.toHaveBeenCalled();

		await harness.session.prompt("foreground", {
			streamingBehavior: "followUp",
			queueIfBusy: true,
			promptCorrelationId: "foreground-2",
		});
		expect(harness.session.getPromptLifecycle("foreground-2")).toMatchObject({
			phase: "queued",
			deliveryCrossed: false,
		});
		expect(harness.session.cancelPromptLifecycle("foreground-2")).toMatchObject({
			status: "cancelled",
			deliveryCrossed: false,
		});
		expect(harness.session.getPromptLifecycles().records).toContainEqual(
			expect.objectContaining({ correlationId: "foreground-2", phase: "cancelled", kind: "model_prompt" }),
		);

		releaseBackground();
		await background;
		await harness.session.waitForHeadlessIdle();
		expect(foregroundProvider).not.toHaveBeenCalled();
	});
	it("does not abort an async input handler after its delivery boundary", async () => {
		let markHandlerStarted = () => {};
		const handlerStarted = new Promise<void>((resolve) => {
			markHandlerStarted = resolve;
		});
		let releaseHandler = () => {};
		const handlerGate = new Promise<void>((resolve) => {
			releaseHandler = resolve;
		});
		const harness = await createHarness({
			extensionFactories: [
				(pi) => {
					pi.on("input", async (event) => {
						if (event.text !== "handled input") return;
						markHandlerStarted();
						await handlerGate;
					});
				},
			],
		});
		harnesses.push(harness);
		harness.setResponses([fauxAssistantMessage("input complete")]);

		const prompt = harness.session.prompt("handled input", { promptCorrelationId: "input-cancel-1" });
		await handlerStarted;
		expect(harness.session.cancelPromptLifecycle("input-cancel-1")).toMatchObject({
			status: "too_late",
			deliveryCrossed: true,
		});
		releaseHandler();
		await prompt;
		expect(harness.session.getPromptLifecycle("input-cancel-1")).toMatchObject({
			phase: "completed",
			deliveryCrossed: true,
		});
	});

	it("delivers async input and extension handlers before attributing their events", async () => {
		const harness = await createHarness({
			extensionFactories: [
				(pi) => {
					pi.on("input", async (event) => {
						if (event.text !== "handled input") return;
						await pi.sendMessage(
							{ customType: "input-context", content: "input context", display: false },
							{ deliverAs: "nextTurn" },
						);
					});
					pi.registerCommand("owned", {
						description: "Emit correlated command context",
						handler: async () => {
							await pi.sendMessage(
								{ customType: "command-context", content: "command context", display: false },
								{ deliverAs: "nextTurn" },
							);
						},
					});
				},
			],
		});
		harnesses.push(harness);
		harness.setResponses([fauxAssistantMessage("input complete")]);
		const events: AgentSessionEvent[] = [];
		harness.session.subscribe((event) => events.push(event));

		await harness.session.prompt("handled input", { promptCorrelationId: "input-1" });
		await harness.session.prompt("/owned", { promptCorrelationId: "extension-1" });

		for (const [correlationId, kind] of [
			["input-1", "input_handler"],
			["extension-1", "extension_command"],
		] as const) {
			const deliveredIndex = events.findIndex(
				(event) =>
					event.type === "prompt_lifecycle" &&
					event.correlationId === correlationId &&
					event.phase === "delivered",
			);
			const firstPayloadIndex = events.findIndex(
				(event) => event.type !== "prompt_lifecycle" && event.promptCorrelationId === correlationId,
			);
			expect(deliveredIndex).toBeGreaterThanOrEqual(0);
			expect(firstPayloadIndex === -1 || firstPayloadIndex > deliveredIndex).toBe(true);
			expect(harness.session.getPromptLifecycle(correlationId)).toMatchObject({ phase: "completed", kind });
		}
	});

	it("restores a delivered lifecycle without replaying owned or queued phases", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		const events: AgentSessionEvent[] = [];
		harness.session.subscribe((event) => events.push(event));

		await harness.session.restoreSessionActions({
			formatVersion: 1,
			actions: [],
			promptLifecycles: {
				records: [
					{
						correlationId: "restored-delivered",
						phase: "delivered",
						kind: "model_prompt",
						revision: 5,
						deliveryCrossed: true,
					},
				],
				expired: [],
				pendingUsage: [],
			},
		});

		expect(harness.session.getPromptLifecycle("restored-delivered")).toMatchObject({
			phase: "failed",
			revision: 6,
			deliveryCrossed: true,
		});
		expect(
			events.flatMap((event) =>
				event.type === "prompt_lifecycle" && event.correlationId === "restored-delivered" ? [event.phase] : [],
			),
		).toEqual(["failed"]);
	});

	it("reports only prompt-scoped assistant usage at terminal state", async () => {
		const backgroundMessage = fauxAssistantMessage("background complete");
		backgroundMessage.usage.input = 100;
		backgroundMessage.usage.output = 50;
		backgroundMessage.usage.totalTokens = 150;
		const foregroundMessage = fauxAssistantMessage("foreground complete");
		foregroundMessage.usage.input = 7;
		foregroundMessage.usage.output = 3;
		foregroundMessage.usage.totalTokens = 10;
		const harness = await createHarness();
		harnesses.push(harness);
		harness.setResponses([backgroundMessage, foregroundMessage]);
		const events: AgentSessionEvent[] = [];
		harness.session.subscribe((event) => events.push(event));

		await harness.session.prompt("background");
		await harness.session.prompt("foreground", { promptCorrelationId: "usage-1" });

		const foregroundEnd = events.find(
			(event) =>
				event.type === "message_end" &&
				event.message.role === "assistant" &&
				event.promptCorrelationId === "usage-1",
		);
		if (foregroundEnd?.type !== "message_end" || foregroundEnd.message.role !== "assistant") {
			throw new Error("Missing correlated assistant terminal event");
		}
		expect(harness.session.getPromptLifecycle("usage-1")).toMatchObject({
			phase: "completed",
			usage: foregroundEnd.message.usage,
		});
		expect(
			events.filter(
				(event) =>
					event.type === "message_end" && event.message.role === "assistant" && event.promptCorrelationId === null,
			).length,
		).toBeGreaterThan(0);
	});
	it("returns too_late without aborting a delivered correlated prompt", async () => {
		let markStarted = () => {};
		const started = new Promise<void>((resolve) => {
			markStarted = resolve;
		});
		let release = () => {};
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const harness = await createHarness();
		harnesses.push(harness);
		harness.setResponses([
			async () => {
				markStarted();
				await gate;
				return fauxAssistantMessage("completed normally");
			},
		]);

		const prompt = harness.session.prompt("foreground", { promptCorrelationId: "delivered-1" });
		await started;
		expect(harness.session.cancelPromptLifecycle("delivered-1")).toMatchObject({
			status: "too_late",
			deliveryCrossed: true,
		});
		release();
		await prompt;
		expect(harness.session.getPromptLifecycle("delivered-1")).toMatchObject({ phase: "completed" });
	});
});
