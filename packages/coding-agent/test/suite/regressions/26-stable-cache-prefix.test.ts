import { join } from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { Context, Model, StreamOptions } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { applyRefinementProposal, loadHarnessState, saveHarnessState } from "../../../src/core/refinement/index.js";
import { createTestResourceLoader } from "../../utilities.js";
import { createHarness, type Harness, type HarnessOptions } from "../harness.js";

/**
 * Provider-visible request shape, captured without the non-serializable tool
 * executors. `prefix` is everything a provider caches; `volatile` is the block
 * that travels after the last cache breakpoint.
 */
interface RequestSnapshot {
	prefix: { systemPrompt: string; tools: string; history: string };
	volatile: string;
}

function serializeTools(tools: Context["tools"]): string {
	return JSON.stringify(
		(tools ?? []).map((tool) => ({
			name: tool.name,
			description: tool.description,
			parameters: tool.parameters,
		})),
	);
}

function snapshotRequest(context: Context): RequestSnapshot {
	return {
		prefix: {
			systemPrompt: context.systemPrompt ?? "",
			tools: serializeTools(context.tools),
			history: JSON.stringify(context.messages),
		},
		volatile: context.volatileContext ?? "",
	};
}

function tool(name: string): AgentTool {
	return {
		name,
		label: name,
		description: `${name} description`,
		parameters: Type.Object({ input: Type.String() }),
		execute: async () => ({ content: [{ type: "text", text: "ok" }], details: undefined }),
	};
}

async function createRecordingHarness(options: HarnessOptions = {}): Promise<{
	harness: Harness;
	requests: RequestSnapshot[];
}> {
	const harness = await createHarness({
		persistSession: true,
		tools: [tool("alpha"), tool("beta"), tool("gamma")],
		...options,
	});
	const requests: RequestSnapshot[] = [];
	const record = (context: Context, _options: StreamOptions | undefined, _state: unknown, model: Model<string>) => {
		requests.push(snapshotRequest(context));
		return {
			role: "assistant" as const,
			content: [{ type: "text" as const, text: "ok" }],
			api: harness.faux.api,
			provider: "faux",
			model: model.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop" as const,
			timestamp: Date.now(),
		};
	};
	harness.setResponses([record, record, record]);
	return { harness, requests };
}

/** Local-day stamp in the same format the prompt builder emits. */
function localDate(): string {
	const now = new Date();
	const month = String(now.getMonth() + 1).padStart(2, "0");
	const day = String(now.getDate()).padStart(2, "0");
	return `${now.getFullYear()}-${month}-${day}`;
}

/** Persist a memory the way a kernel-side `rlm.harness.create_memory` call would. */
function seedLocalMemory(harness: Harness, id: string, content: string): void {
	const localDir = join(harness.sessionManager.getSessionArtifactDir() ?? harness.tempDir, "harness");
	const state = loadHarnessState(localDir, "local");
	applyRefinementProposal(
		state,
		{
			summary: `Seed ${id}`,
			rationale: "seed",
			expectedOutcome: "seeded",
			edits: [{ action: "create", kind: "memory", id, title: id, content }],
		},
		{ id: `refine_${id}`, scope: "local" },
	);
	saveHarnessState(localDir, state);
}

describe("issue 26: stable prompt-cache prefix", () => {
	const harnesses: Harness[] = [];
	let previousAgentDir: string | undefined;
	let agentDirWasSet = false;

	function isolateGlobalHarnessState(harness: Harness): void {
		if (!agentDirWasSet) {
			previousAgentDir = process.env.PRIME_AGENT_CODING_AGENT_DIR;
			agentDirWasSet = true;
		}
		process.env.PRIME_AGENT_CODING_AGENT_DIR = join(harness.tempDir, "agent");
	}

	afterEach(() => {
		vi.useRealTimers();
		if (agentDirWasSet) {
			if (previousAgentDir === undefined) {
				delete process.env.PRIME_AGENT_CODING_AGENT_DIR;
			} else {
				process.env.PRIME_AGENT_CODING_AGENT_DIR = previousAgentDir;
			}
			agentDirWasSet = false;
			previousAgentDir = undefined;
		}
		while (harnesses.length > 0) {
			harnesses.pop()?.cleanup();
		}
	});

	it("keeps the cached prefix byte-identical across a mid-session harness update", async () => {
		const { harness, requests } = await createRecordingHarness();
		harnesses.push(harness);
		isolateGlobalHarnessState(harness);

		await harness.session.prompt("first");
		seedLocalMemory(harness, "cache_note", "Prompt caching needs a stable prefix.");
		// The same rebuild a harness write triggers through the tool-registry refresh.
		harness.session.setActiveToolsByName(harness.session.getActiveToolNames());
		await harness.session.prompt("second");

		expect(requests).toHaveLength(2);
		const [first, second] = requests;

		expect(second.prefix.systemPrompt).toBe(first.prefix.systemPrompt);
		expect(second.prefix.tools).toBe(first.prefix.tools);
		// The second request only appends to the first request's history.
		expect(second.prefix.history.startsWith(first.prefix.history.slice(0, -1))).toBe(true);

		expect(first.prefix.systemPrompt).not.toContain("cache_note");
		expect(second.prefix.systemPrompt).not.toContain("cache_note");
		expect(second.prefix.history).not.toContain("cache_note");
		expect(second.volatile).toContain("cache_note");
		expect(second.volatile).toContain("Prompt caching needs a stable prefix.");
		expect(second.volatile).not.toBe(first.volatile);
	});

	it("keeps the cached prefix byte-identical across a date flip", async () => {
		vi.useFakeTimers({ toFake: ["Date"], now: new Date("2026-08-31T12:00:00Z") });
		const { harness, requests } = await createRecordingHarness({
			resourceLoader: {
				...createTestResourceLoader(),
				getSystemPrompt: () => "You are a test assistant.",
			},
		});
		harnesses.push(harness);
		isolateGlobalHarnessState(harness);

		await harness.session.prompt("first");
		const firstDate = localDate();
		vi.setSystemTime(new Date("2026-09-02T12:00:00Z"));
		const secondDate = localDate();
		harness.session.setActiveToolsByName(harness.session.getActiveToolNames());
		await harness.session.prompt("second");

		expect(secondDate).not.toBe(firstDate);
		expect(requests).toHaveLength(2);
		const [first, second] = requests;

		expect(second.prefix.systemPrompt).toBe(first.prefix.systemPrompt);
		expect(second.prefix.tools).toBe(first.prefix.tools);
		expect(first.prefix.systemPrompt).not.toContain("Current date");
		expect(second.prefix.systemPrompt).not.toContain("Current date");
		expect(first.volatile).toContain(`Current date: ${firstDate}`);
		expect(second.volatile).toContain(`Current date: ${secondDate}`);
	});

	it("serializes an unchanged tool set to identical bytes across registry refreshes", async () => {
		const { harness, requests } = await createRecordingHarness();
		harnesses.push(harness);
		isolateGlobalHarnessState(harness);

		await harness.session.prompt("first");
		const session = harness.session as unknown as { _refreshToolRegistry: () => void };
		session._refreshToolRegistry();
		session._refreshToolRegistry();
		await harness.session.prompt("second");

		expect(requests).toHaveLength(2);
		expect(requests[1].prefix.tools).toBe(requests[0].prefix.tools);
	});

	it("keeps the history append-only and moves volatile content into the system prompt for an appendOnlyHistory model", async () => {
		const { harness, requests } = await createRecordingHarness();
		harnesses.push(harness);
		isolateGlobalHarnessState(harness);

		const fauxModel = harness.getModel();
		harness.session.modelRegistry.registerProvider("append-only-proxy", {
			baseUrl: fauxModel.baseUrl,
			apiKey: "faux-key",
			api: harness.faux.api,
			models: [
				{
					id: "append-only-model",
					name: "Append Only Proxy",
					reasoning: false,
					input: ["text"],
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
					contextWindow: 128000,
					maxTokens: 16384,
					appendOnlyHistory: true,
				},
			],
		});
		const registered = harness.session.modelRegistry.getAll().find((model) => model.id === "append-only-model");
		// The flag has to survive registerProvider parsing to reach the provider.
		expect(registered?.appendOnlyHistory).toBe(true);
		await harness.session.setModel(registered!);

		await harness.session.prompt("first");
		seedLocalMemory(harness, "proxy_note", "Session-cached backends need an append-only history.");
		harness.session.setActiveToolsByName(harness.session.getActiveToolNames());
		await harness.session.prompt("second");

		expect(requests).toHaveLength(2);
		const [first, second] = requests;

		// No payload-only block anywhere: each request's history only appends to the last.
		expect(first.volatile).toBe("");
		expect(second.volatile).toBe("");
		expect(second.prefix.history.startsWith(first.prefix.history.slice(0, -1))).toBe(true);
		expect(first.prefix.history).not.toContain("Continual Harness State");
		expect(second.prefix.history).not.toContain("Continual Harness State");

		// The content still reaches the model, inside the system prompt.
		expect(first.prefix.systemPrompt).toContain("# Continual Harness State");
		expect(second.prefix.systemPrompt).toContain("proxy_note");
		expect(second.prefix.systemPrompt).toContain("Session-cached backends need an append-only history.");
	});

	it("pins tool order to first activation so reordering cannot move the cache marker", async () => {
		const { harness } = await createRecordingHarness();
		harnesses.push(harness);
		isolateGlobalHarnessState(harness);

		const initialOrder = harness.session.getActiveToolNames();
		expect(initialOrder).toEqual(["alpha", "beta", "gamma"]);

		harness.session.setActiveToolsByName(["gamma", "beta", "alpha"]);
		expect(harness.session.getActiveToolNames()).toEqual(["alpha", "beta", "gamma"]);

		// A tool that leaves and comes back returns to its original slot.
		harness.session.setActiveToolsByName(["alpha", "gamma"]);
		expect(harness.session.getActiveToolNames()).toEqual(["alpha", "gamma"]);
		harness.session.setActiveToolsByName(["gamma", "alpha", "beta"]);
		expect(harness.session.getActiveToolNames()).toEqual(["alpha", "beta", "gamma"]);
	});
});
