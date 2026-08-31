import { Type } from "typebox";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { registerApiProvider, unregisterApiProviders } from "../src/api-registry.js";
import { getModel } from "../src/models.js";
import { streamAnthropic } from "../src/providers/anthropic.js";
import { fauxAssistantMessage, registerFauxProvider } from "../src/providers/faux.js";
import { streamOpenAICompletions } from "../src/providers/openai-completions.js";
import { streamSimple } from "../src/stream.js";
import type { Context, Model, StreamFunction, StreamOptions } from "../src/types.js";
import { createAssistantMessageEventStream } from "../src/utils/event-stream.js";

interface CacheControl {
	type: "ephemeral";
	ttl?: string;
}

interface TextPart {
	type: string;
	text?: string;
	cache_control?: CacheControl;
}

interface CapturedMessage {
	role: string;
	content: string | TextPart[] | null;
}

interface CapturedPayload {
	system?: unknown;
	tools?: unknown[];
	messages: CapturedMessage[];
}

const TOOLS = [
	{
		name: "read",
		description: "Read a file",
		parameters: Type.Object({ path: Type.String() }),
	},
	{
		name: "write",
		description: "Write a file",
		parameters: Type.Object({ path: Type.String() }),
	},
];

function baseContext(volatileContext?: string): Context {
	return {
		systemPrompt: "You are a general purpose agent.",
		messages: [
			{ role: "user", content: "first", timestamp: 1 },
			{
				role: "assistant",
				content: [{ type: "text", text: "reply" }],
				api: "anthropic-messages",
				provider: "anthropic",
				model: "claude-sonnet-4-5",
				usage: {
					input: 0,
					output: 0,
					cacheRead: 0,
					cacheWrite: 0,
					totalTokens: 0,
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
				},
				stopReason: "stop",
				timestamp: 2,
			},
			{ role: "user", content: "second", timestamp: 3 },
		],
		tools: TOOLS,
		volatileContext,
	};
}

function flattenParts(message: CapturedMessage): TextPart[] {
	const content = message.content;
	if (content === null) return [];
	if (typeof content === "string") return [{ type: "text", text: content }];
	return content;
}

/**
 * Serialize everything an Anthropic-style request caches: the tool definitions,
 * the system blocks, and the history up to and including the last cache marker.
 */
function cachedPrefix(payload: CapturedPayload): string {
	const history: string[] = [];
	let lastMarker = -1;
	for (const message of payload.messages) {
		for (const part of flattenParts(message)) {
			if (part.cache_control) {
				lastMarker = history.length;
			}
			history.push(JSON.stringify({ role: message.role, part }));
		}
	}
	return JSON.stringify({
		tools: payload.tools,
		system: payload.system,
		history: history.slice(0, lastMarker + 1),
	});
}

function lastPart(payload: CapturedPayload): TextPart | undefined {
	const parts = flattenParts(payload.messages[payload.messages.length - 1]);
	return parts[parts.length - 1];
}

async function captureAnthropicPayload(volatileContext?: string): Promise<CapturedPayload> {
	const model: Model<"anthropic-messages"> = {
		...getModel("anthropic", "claude-sonnet-4-5"),
		baseUrl: "http://127.0.0.1:9",
	};
	let captured: CapturedPayload | undefined;
	await streamAnthropic(model, baseContext(volatileContext), {
		apiKey: "fake-key",
		maxRetries: 0,
		onPayload: (payload) => {
			captured = payload as CapturedPayload;
			return payload;
		},
	}).result();

	if (!captured) {
		throw new Error("Expected the Anthropic payload to be captured before the request failed");
	}
	return captured;
}

const openAIMockState = vi.hoisted(() => ({
	lastParams: undefined as CapturedPayload | undefined,
}));

vi.mock("openai", () => {
	class FakeOpenAI {
		chat = {
			completions: {
				create: (params: CapturedPayload) => {
					openAIMockState.lastParams = params;
					const stream = {
						async *[Symbol.asyncIterator]() {
							yield {
								id: "chatcmpl-test",
								choices: [{ delta: {}, finish_reason: "stop" }],
							};
						},
					};
					const promise = Promise.resolve(stream) as Promise<typeof stream> & {
						withResponse: () => Promise<{
							data: typeof stream;
							response: { status: number; headers: Headers };
						}>;
					};
					promise.withResponse = async () => ({
						data: stream,
						response: { status: 200, headers: new Headers() },
					});
					return promise;
				},
			},
		};
	}

	return { default: FakeOpenAI };
});

const anthropicCompatModel: Model<"openai-completions"> = {
	id: "anthropic-proxy",
	name: "Anthropic Proxy",
	api: "openai-completions",
	provider: "openrouter",
	baseUrl: "https://example.com/v1",
	reasoning: false,
	input: ["text"],
	cost: { input: 1, output: 1, cacheRead: 0, cacheWrite: 1 },
	contextWindow: 128000,
	maxTokens: 32000,
	compat: { cacheControlFormat: "anthropic" },
};

const plainOpenAIModel: Model<"openai-completions"> = {
	...anthropicCompatModel,
	id: "plain-completions",
	name: "Plain Completions",
	compat: undefined,
};

async function captureOpenAIPayload(
	model: Model<"openai-completions">,
	volatileContext?: string,
): Promise<CapturedPayload> {
	openAIMockState.lastParams = undefined;
	await streamOpenAICompletions(model, baseContext(volatileContext), { apiKey: "test-key" }).result();
	if (!openAIMockState.lastParams) {
		throw new Error("Expected the OpenAI payload to be captured");
	}
	return openAIMockState.lastParams;
}

describe("Anthropic volatile context placement", () => {
	it("keeps the cached prefix byte-identical when volatile context changes", async () => {
		const first = await captureAnthropicPayload("# Continual Harness State\n\nmemory: 0");
		const second = await captureAnthropicPayload("# Continual Harness State\n\nmemory: 1\n- [local:m1] note");

		expect(cachedPrefix(second)).toBe(cachedPrefix(first));
		expect(JSON.stringify(second.messages)).not.toBe(JSON.stringify(first.messages));
	});

	it("keeps the cached prefix byte-identical against a request with no volatile context", async () => {
		const withoutVolatile = await captureAnthropicPayload();
		const withVolatile = await captureAnthropicPayload("Current date: 2026-09-01");

		expect(cachedPrefix(withVolatile)).toBe(cachedPrefix(withoutVolatile));
	});

	it("places volatile context after the last cache marker", async () => {
		const payload = await captureAnthropicPayload("Current date: 2026-09-01");
		const parts = flattenParts(payload.messages[payload.messages.length - 1]);

		expect(parts).toHaveLength(2);
		expect(parts[0]?.text).toBe("second");
		expect(parts[0]?.cache_control).toEqual({ type: "ephemeral" });
		expect(parts[1]?.text).toBe("Current date: 2026-09-01");
		expect(parts[1]?.cache_control).toBeUndefined();
	});

	it("marks the last tool definition and leaves the system prompt free of volatile content", async () => {
		const payload = await captureAnthropicPayload("# Continual Harness State\n\nmemory: 1");
		const tools = payload.tools as Array<{ name: string; cache_control?: CacheControl }>;

		expect(tools.map((tool) => tool.name)).toEqual(["read", "write"]);
		expect(tools[0]?.cache_control).toBeUndefined();
		expect(tools[1]?.cache_control).toEqual({ type: "ephemeral" });
		expect(JSON.stringify(payload.system)).not.toContain("Continual Harness State");
	});

	it("appends a trailing user message when the request ends with an assistant turn", async () => {
		const model: Model<"anthropic-messages"> = {
			...getModel("anthropic", "claude-sonnet-4-5"),
			baseUrl: "http://127.0.0.1:9",
		};
		const context = baseContext("volatile tail");
		context.messages = context.messages.slice(0, 2);
		let captured: CapturedPayload | undefined;
		await streamAnthropic(model, context, {
			apiKey: "fake-key",
			maxRetries: 0,
			onPayload: (payload) => {
				captured = payload as CapturedPayload;
				return payload;
			},
		}).result();

		if (!captured) throw new Error("Expected the Anthropic payload to be captured");
		const last = captured.messages[captured.messages.length - 1];
		expect(last.role).toBe("user");
		expect(lastPart(captured)?.text).toBe("volatile tail");
		expect(lastPart(captured)?.cache_control).toBeUndefined();
	});

	it("ignores blank volatile context", async () => {
		const payload = await captureAnthropicPayload("   \n  ");
		expect(flattenParts(payload.messages[payload.messages.length - 1])).toHaveLength(1);
	});

	it("sends volatile context in the system prompt for an appendOnlyHistory model", async () => {
		const model: Model<"anthropic-messages"> = {
			...getModel("anthropic", "claude-sonnet-4-5"),
			baseUrl: "http://127.0.0.1:9",
			appendOnlyHistory: true,
		};
		const source = baseContext("# Continual Harness State\n\nmemory: 1");
		let captured: CapturedPayload | undefined;
		// Routed through the registry, which owns the placement decision.
		await streamSimple(model, source, {
			apiKey: "fake-key",
			maxRetries: 0,
			onPayload: (payload) => {
				captured = payload as CapturedPayload;
				return payload;
			},
		}).result();

		if (!captured) throw new Error("Expected the Anthropic payload to be captured");
		expect(JSON.stringify(captured.system)).toContain("Continual Harness State");

		// No payload-only block: the message array still matches the caller's history.
		const parts = flattenParts(captured.messages[captured.messages.length - 1]);
		expect(parts).toHaveLength(1);
		expect(parts[0]?.text).toBe("second");
		expect(parts[0]?.cache_control).toEqual({ type: "ephemeral" });
		expect(captured.messages).toHaveLength(source.messages.length);
	});
});

describe("OpenAI-completions volatile context placement", () => {
	beforeEach(() => {
		openAIMockState.lastParams = undefined;
	});

	it("keeps the cached prefix byte-identical when volatile context changes", async () => {
		const first = await captureOpenAIPayload(anthropicCompatModel, "memory: 0");
		const second = await captureOpenAIPayload(anthropicCompatModel, "memory: 1\n- [local:m1] note");

		expect(cachedPrefix(second)).toBe(cachedPrefix(first));
		expect(JSON.stringify(second.messages)).not.toBe(JSON.stringify(first.messages));
	});

	it("places volatile context after the Anthropic-style cache markers", async () => {
		const payload = await captureOpenAIPayload(anthropicCompatModel, "memory: 1");
		const parts = flattenParts(payload.messages[payload.messages.length - 1]);

		expect(parts).toHaveLength(2);
		expect(parts[0]?.cache_control).toEqual({ type: "ephemeral" });
		expect(parts[1]?.text).toBe("memory: 1");
		expect(parts[1]?.cache_control).toBeUndefined();
	});

	it("appends volatile context last for providers with automatic prefix caching", async () => {
		const payload = await captureOpenAIPayload(plainOpenAIModel, "memory: 1");
		const parts = flattenParts(payload.messages[payload.messages.length - 1]);

		expect(parts.map((part) => part.text)).toEqual(["second", "memory: 1"]);
		const instruction = payload.messages.find((message) => message.role === "system");
		expect(JSON.stringify(instruction)).not.toContain("memory: 1");
	});
});

describe("volatile context handling at the provider registry", () => {
	function recordingProvider(
		api: string,
		handlesVolatileContext: boolean,
	): { contexts: Context[]; model: Model<string> } {
		const contexts: Context[] = [];
		const record: StreamFunction<string, StreamOptions> = (_model, context) => {
			contexts.push(context);
			return createAssistantMessageEventStream();
		};
		registerApiProvider({ api, stream: record, streamSimple: record, handlesVolatileContext }, api);
		return {
			contexts,
			model: {
				id: `${api}-model`,
				name: api,
				api,
				provider: api,
				baseUrl: "http://localhost:0",
				reasoning: false,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 1000,
				maxTokens: 100,
			},
		};
	}

	afterEach(() => {
		unregisterApiProviders("fallback-api");
		unregisterApiProviders("native-api");
	});

	it("folds volatile context into the end of the message list for providers without cache markers", () => {
		const { contexts, model } = recordingProvider("fallback-api", false);

		streamSimple(model, baseContext("# Continual Harness State\n\nmemory: 1"));

		expect(contexts).toHaveLength(1);
		const received = contexts[0];
		expect(received.volatileContext).toBeUndefined();
		expect(received.systemPrompt).toBe("You are a general purpose agent.");
		expect(received.messages).toHaveLength(baseContext().messages.length + 1);
		const last = received.messages[received.messages.length - 1];
		expect(last.role).toBe("user");
		expect(JSON.stringify(last)).toContain("Continual Harness State");
	});

	it("leaves volatile context untouched for providers that place it themselves", () => {
		const { contexts, model } = recordingProvider("native-api", true);

		streamSimple(model, baseContext("volatile tail"));

		expect(contexts).toHaveLength(1);
		expect(contexts[0].volatileContext).toBe("volatile tail");
		expect(contexts[0].messages).toHaveLength(baseContext().messages.length);
	});

	it("routes volatile context into the system prompt for appendOnlyHistory models", () => {
		for (const handlesVolatileContext of [true, false]) {
			const api = handlesVolatileContext ? "native-api" : "fallback-api";
			const { contexts, model } = recordingProvider(api, handlesVolatileContext);
			const source = baseContext("# Continual Harness State\n\nmemory: 1");

			streamSimple({ ...model, appendOnlyHistory: true }, source);

			expect(contexts).toHaveLength(1);
			const received = contexts[0];
			expect(received.volatileContext).toBeUndefined();
			// A session-cached backend matches on the message array, so it must be
			// byte-identical to the caller's persisted history.
			expect(JSON.stringify(received.messages)).toBe(JSON.stringify(source.messages));
			expect(received.systemPrompt).toBe(
				"You are a general purpose agent.\n\n# Continual Harness State\n\nmemory: 1",
			);
			unregisterApiProviders(api);
		}
	});

	it("uses the volatile content as the system prompt when an appendOnlyHistory model has none", () => {
		const { contexts, model } = recordingProvider("native-api", true);

		streamSimple({ ...model, appendOnlyHistory: true }, { messages: [], volatileContext: "memory: 1" });

		expect(contexts[0].systemPrompt).toBe("memory: 1");
		expect(contexts[0].messages).toHaveLength(0);
	});

	it("keeps volatile content out of the faux provider's simulated cache prefix", async () => {
		const faux = registerFauxProvider();
		try {
			const context = baseContext("memory: 0");
			faux.setResponses([fauxAssistantMessage("first"), fauxAssistantMessage("second")]);
			const options = { sessionId: "cache-session" };

			await streamSimple(faux.getModel(), context, options).result();
			const second = await streamSimple(
				faux.getModel(),
				{ ...context, volatileContext: "memory: 1\n- [local:m1] note" },
				options,
			).result();

			expect(second.usage.cacheRead).toBeGreaterThan(0);
			expect(second.usage.cacheWrite).toBeLessThan(second.usage.cacheRead);
		} finally {
			faux.unregister();
		}
	});
});
