import { existsSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type AssistantMessage, fauxAssistantMessage, registerFauxProvider } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it } from "vitest";
import { AuthStorage } from "../../../src/core/auth-storage.js";
import { ModelRegistry } from "../../../src/core/model-registry.js";
import { createAgentSession } from "../../../src/core/sdk.js";
import { SessionManager } from "../../../src/core/session-manager.js";
import { type Settings, SettingsManager } from "../../../src/core/settings-manager.js";
import { createTestResourceLoader } from "../../utilities.js";
import { createHarness, type Harness } from "../harness.js";

function providerFailure(details: Record<string, unknown>, errorMessage: string): AssistantMessage {
	return {
		...fauxAssistantMessage("", { stopReason: "error", errorMessage }),
		diagnostics: [{ type: "provider_stream_failure", timestamp: Date.now(), details }],
	};
}

function rateLimited(retryAfterMs?: number): AssistantMessage {
	return providerFailure(
		{ kind: "rate_limit", status: 429, retryAfterMs },
		"Provider rate limit exceeded (rate_limit_error, 429)",
	);
}

/** Capture the scheduled backoff without serving it: the sleep is abortable. */
async function captureRetryDelayMs(harness: Harness): Promise<number | undefined> {
	let delayMs: number | undefined;
	const sawRetryStart = new Promise<void>((resolve) => {
		const unsubscribe = harness.session.subscribe((event) => {
			if (event.type === "auto_retry_start") {
				delayMs = event.delayMs;
				unsubscribe();
				resolve();
			}
		});
	});

	const promptPromise = harness.session.prompt("test");
	await sawRetryStart;
	harness.session.abortRetry();
	await promptPromise;
	return delayMs;
}

describe("issue #24 kind-aware retry backoff", () => {
	const harnesses: Harness[] = [];
	const cleanups: Array<() => void> = [];

	afterEach(() => {
		while (harnesses.length > 0) {
			harnesses.pop()?.cleanup();
		}
		while (cleanups.length > 0) {
			cleanups.pop()?.();
		}
	});

	it("honors an upstream Retry-After instead of the 2s ladder", async () => {
		const harness = await createHarness({
			settings: { retry: { enabled: true, maxRetries: 3, baseDelayMs: 2000 } },
		});
		harnesses.push(harness);
		harness.setResponses([rateLimited(30_000), rateLimited(30_000), rateLimited(30_000), rateLimited(30_000)]);

		const delayMs = await captureRetryDelayMs(harness);

		expect(harness.faux.state.callCount).toBe(1);
		expect(delayMs).toBeGreaterThanOrEqual(30_000);
		expect(delayMs).toBeLessThanOrEqual(31_000);
	});

	it("fails fast when Retry-After exceeds retry.provider.maxRetryDelayMs", async () => {
		const harness = await createHarness({
			settings: {
				retry: { enabled: true, maxRetries: 3, baseDelayMs: 1, provider: { maxRetryDelayMs: 60_000 } },
			},
		});
		harnesses.push(harness);
		harness.setResponses([rateLimited(3_600_000), rateLimited(3_600_000)]);

		await harness.session.prompt("test");

		expect(harness.faux.state.callCount).toBe(1);
		expect(harness.eventsOfType("auto_retry_start")).toEqual([]);
		expect(harness.eventsOfType("auto_retry_end").map((event) => event.success)).toEqual([false]);
		expect(harness.eventsOfType("auto_retry_end")[0]?.finalError).toContain(
			"Provider asked to wait 3600s, longer than the 60s retry delay cap",
		);
	});

	it("aborts the ladder on auth failures instead of burning every attempt", async () => {
		const harness = await createHarness({
			settings: { retry: { enabled: true, maxRetries: 3, baseDelayMs: 1 } },
		});
		harnesses.push(harness);
		harness.setResponses([
			providerFailure({ kind: "auth", status: 401 }, "401 Unauthorized: token expired"),
			fauxAssistantMessage("must not be reached"),
		]);

		await harness.session.prompt("test");

		expect(harness.faux.state.callCount).toBe(1);
		expect(harness.eventsOfType("auto_retry_start")).toEqual([]);
		expect(harness.eventsOfType("auto_retry_end").map((event) => event.success)).toEqual([false]);
		expect(harness.eventsOfType("auto_retry_end")[0]?.finalError).toContain(
			"Provider authentication failed; not retrying",
		);
		expect(harness.session.isRetrying).toBe(false);
	});

	it("jitters rate-limit backoff well above the plain ladder", async () => {
		const controlHarness = await createHarness({
			settings: { retry: { enabled: true, maxRetries: 3, baseDelayMs: 100 } },
		});
		harnesses.push(controlHarness);
		controlHarness.setResponses([
			providerFailure({ kind: "server_error", status: 500 }, "500 Internal Server Error"),
			fauxAssistantMessage("unused"),
		]);
		expect(await captureRetryDelayMs(controlHarness)).toBe(100);

		const delays: number[] = [];
		for (let run = 0; run < 4; run++) {
			const harness = await createHarness({
				settings: { retry: { enabled: true, maxRetries: 3, baseDelayMs: 100 } },
			});
			harnesses.push(harness);
			harness.setResponses([rateLimited(), rateLimited()]);
			const delayMs = await captureRetryDelayMs(harness);
			expect(delayMs).toBeDefined();
			delays.push(delayMs as number);
		}

		for (const delayMs of delays) {
			expect(delayMs).toBeGreaterThanOrEqual(100);
			expect(delayMs).toBeLessThanOrEqual(15_000);
		}
		expect(new Set(delays).size).toBeGreaterThan(1);
	});
});

describe("issue #24 single retry layer", () => {
	const cleanups: Array<() => void> = [];

	afterEach(() => {
		while (cleanups.length > 0) {
			cleanups.pop()?.();
		}
	});

	async function captureProviderMaxRetries(settings: Partial<Settings>): Promise<number | undefined> {
		const tempDir = join(tmpdir(), `pi-issue24-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		const agentDir = join(tempDir, "agent");
		mkdirSync(agentDir, { recursive: true });
		const faux = registerFauxProvider();
		const model = faux.getModel();
		const authStorage = AuthStorage.inMemory();
		authStorage.setRuntimeApiKey(model.provider, "faux-key");
		const modelRegistry = ModelRegistry.inMemory(authStorage);
		modelRegistry.registerProvider(model.provider, {
			baseUrl: model.baseUrl,
			apiKey: "faux-key",
			api: faux.api,
			models: faux.models.map((registered) => ({
				id: registered.id,
				name: registered.name,
				api: registered.api,
				reasoning: registered.reasoning,
				input: registered.input,
				cost: registered.cost,
				contextWindow: registered.contextWindow,
				maxTokens: registered.maxTokens,
				baseUrl: registered.baseUrl,
			})),
		});

		const { session } = await createAgentSession({
			cwd: tempDir,
			agentDir,
			model,
			authStorage,
			modelRegistry,
			settingsManager: SettingsManager.inMemory(settings),
			sessionManager: SessionManager.inMemory(),
			resourceLoader: createTestResourceLoader(),
		});
		cleanups.push(() => {
			session.dispose();
			faux.unregister();
			if (existsSync(tempDir)) rmSync(tempDir, { recursive: true, force: true });
		});

		let observed: number | undefined;
		faux.setResponses([
			(_context, streamOptions) => {
				observed = streamOptions?.maxRetries;
				return fauxAssistantMessage("ok");
			},
		]);
		await session.prompt("hello");
		return observed;
	}

	it("disables provider SDK retries while the session retry loop owns policy", async () => {
		await expect(captureProviderMaxRetries({ retry: { enabled: true } })).resolves.toBe(0);
	});

	it("keeps an explicit provider retry override", async () => {
		await expect(captureProviderMaxRetries({ retry: { enabled: true, provider: { maxRetries: 2 } } })).resolves.toBe(
			2,
		);
	});

	it("leaves provider retries at the SDK default when session retry is disabled", async () => {
		await expect(captureProviderMaxRetries({ retry: { enabled: false } })).resolves.toBeUndefined();
	});
});
