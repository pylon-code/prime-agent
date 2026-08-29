import { describe, expect, it } from "vitest";
import { createPromptRequestFingerprint, PromptLifecycleStore } from "../src/core/prompt-lifecycle.js";

const usage = {
	input: 10,
	output: 5,
	cacheRead: 2,
	cacheWrite: 1,
	totalTokens: 18,
	cost: { input: 1, output: 2, cacheRead: 3, cacheWrite: 4, total: 10 },
};

describe("PromptLifecycleStore", () => {
	it("canonicalizes equivalent prompt request payloads", () => {
		const left = createPromptRequestFingerprint({
			message: "hello",
			images: [{ type: "image", data: "bytes", mimeType: "image/png" }],
		});
		const right = createPromptRequestFingerprint({
			message: "hello",
			images: [{ mimeType: "image/png", data: "bytes", type: "image" }],
			queueIfBusy: false,
		});
		expect(left).toBe(right);
		expect(createPromptRequestFingerprint({ message: "hello", images: [] })).toBe(
			createPromptRequestFingerprint({ message: "hello" }),
		);
		expect(createPromptRequestFingerprint({ message: "hello", queueIfBusy: true })).not.toBe(left);
	});
	it("records ordered lifecycle snapshots without prompt content", () => {
		const store = new PromptLifecycleStore();

		expect(store.begin("prompt-1", "model_prompt")).toEqual({
			correlationId: "prompt-1",
			phase: "owned",
			kind: "model_prompt",
			revision: 1,
			deliveryCrossed: false,
		});
		expect(store.transition("prompt-1", "queued")).toMatchObject({ phase: "queued", revision: 2 });
		expect(store.transition("prompt-1", "delivered")).toMatchObject({ phase: "delivered", revision: 3 });
		expect(store.transition("prompt-1", "completed")).toMatchObject({ phase: "completed", revision: 4 });
		expect(store.snapshot()).toEqual({
			records: [
				{
					correlationId: "prompt-1",
					phase: "completed",
					kind: "model_prompt",
					revision: 4,
					deliveryCrossed: true,
				},
			],
			expired: [],
		});
	});

	it("rejects duplicate ids, oversized ids, and lifecycle regression", () => {
		const store = new PromptLifecycleStore();
		store.begin("prompt-1", "model_prompt");
		store.transition("prompt-1", "queued");

		expect(() => store.begin("prompt-1", "model_prompt")).toThrow("already in use");
		expect(() => store.begin("x".repeat(129), "model_prompt")).toThrow("between 1 and 128");
		store.transition("prompt-1", "cancelled");
		expect(() => store.transition("prompt-1", "delivered")).toThrow("cancelled -> delivered");
	});

	it("retains every active lifecycle and turns evicted terminals into bounded tombstones", () => {
		const store = new PromptLifecycleStore(2, 2);
		store.begin("active", "model_prompt");
		for (const id of ["first", "second", "third"]) {
			store.begin(id, "model_prompt");
			store.transition(id, "failed");
		}

		expect(store.snapshot().records.map(({ correlationId }) => correlationId)).toEqual(["active", "second", "third"]);
		expect(store.snapshot().expired).toEqual([{ correlationId: "first", deliveryCrossed: false }]);
		expect(() => store.begin("first", "model_prompt")).toThrow("already in use");
	});

	it("retains the exact delivery boundary for failures and tombstones", () => {
		const store = new PromptLifecycleStore(1, 2);
		store.begin("before", "model_prompt");
		const before = store.transition("before", "failed");
		expect(before).toMatchObject({ phase: "failed", deliveryCrossed: false });

		store.begin("after", "model_prompt");
		store.transition("after", "delivered");
		const after = store.transition("after", "failed");
		expect(after).toMatchObject({ phase: "failed", deliveryCrossed: true });
		expect(store.snapshot().expired).toContainEqual({ correlationId: "before", deliveryCrossed: false });
	});

	it("round-trips recovery state without regressing revisions or pending usage", () => {
		const source = new PromptLifecycleStore(2, 2);
		source.begin("queued", "model_prompt", "a".repeat(64));
		source.transition("queued", "queued");
		source.addUsage("queued", usage);
		source.begin("delivered", "model_prompt", "b".repeat(64));
		source.transition("delivered", "delivered");
		source.transition("delivered", "failed");

		const restored = new PromptLifecycleStore(2, 2);
		restored.restore(source.recoverySnapshot());
		expect(restored.recoverySnapshot()).toEqual(source.recoverySnapshot());
		const terminal = restored.transition("queued", "failed");
		expect(terminal).toMatchObject({ deliveryCrossed: false });
		expect(terminal.revision).toBeGreaterThan(4);
		expect(terminal.usage).toEqual(usage);
	});

	it("clones and exposes summed correlated usage only at terminal state", () => {
		const store = new PromptLifecycleStore();
		store.begin("prompt-1", "model_prompt");
		store.transition("prompt-1", "delivered");
		store.addUsage("prompt-1", usage);
		store.addUsage("prompt-1", usage);
		expect(store.get("prompt-1")?.usage).toBeUndefined();
		const terminal = store.transition("prompt-1", "completed");
		expect(terminal.usage).toEqual({
			input: 20,
			output: 10,
			cacheRead: 4,
			cacheWrite: 2,
			totalTokens: 36,
			cost: { input: 2, output: 4, cacheRead: 6, cacheWrite: 8, total: 20 },
		});
		usage.cost.total = 99;
		expect(terminal.usage?.cost.total).toBe(20);
	});
});
