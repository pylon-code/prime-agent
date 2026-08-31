import { describe, expect, it } from "vitest";
import { computeRetryBackoff, isNonRetryableFailureKind } from "../src/core/retry-backoff.js";

const BASE = { baseDelayMs: 2000, maxRetryDelayMs: 60_000 };

describe("computeRetryBackoff", () => {
	it("keeps the plain exponential ladder for kinds it does not classify", () => {
		for (const kind of [undefined, "server_error", "unknown", "malformed_response"]) {
			expect([1, 2, 3].map((attempt) => computeRetryBackoff({ ...BASE, kind, attempt }))).toEqual([
				{ type: "wait", delayMs: 2000, honoredRetryAfter: false },
				{ type: "wait", delayMs: 4000, honoredRetryAfter: false },
				{ type: "wait", delayMs: 8000, honoredRetryAfter: false },
			]);
		}
	});

	it("aborts immediately on auth failures", () => {
		expect(computeRetryBackoff({ ...BASE, kind: "auth", attempt: 1 })).toEqual({
			type: "abort",
			reason: "Provider authentication failed; not retrying",
		});
		expect(isNonRetryableFailureKind("auth")).toBe(true);
		expect(isNonRetryableFailureKind("server_error")).toBe(false);
	});

	it("backs off rate limits from a longer base with full jitter", () => {
		expect(computeRetryBackoff({ ...BASE, kind: "rate_limit", attempt: 1, random: () => 1 })).toEqual({
			type: "wait",
			delayMs: 15_000,
			honoredRetryAfter: false,
		});
		expect(computeRetryBackoff({ ...BASE, kind: "rate_limit", attempt: 1, random: () => 0.5 })).toEqual({
			type: "wait",
			delayMs: 7500,
			honoredRetryAfter: false,
		});
	});

	it("floors a jittered wait at the configured base delay", () => {
		expect(computeRetryBackoff({ ...BASE, kind: "rate_limit", attempt: 1, random: () => 0 })).toEqual({
			type: "wait",
			delayMs: 2000,
			honoredRetryAfter: false,
		});
	});

	it("backs off overloads from their own base and grows exponentially", () => {
		expect(computeRetryBackoff({ ...BASE, kind: "overloaded", attempt: 1, random: () => 1 }).type).toBe("wait");
		expect(computeRetryBackoff({ ...BASE, kind: "overloaded", attempt: 1, random: () => 1 })).toMatchObject({
			delayMs: 10_000,
		});
		expect(computeRetryBackoff({ ...BASE, kind: "overloaded", attempt: 2, random: () => 1 })).toMatchObject({
			delayMs: 20_000,
		});
	});

	it("caps the jittered window at maxRetryDelayMs", () => {
		expect(
			computeRetryBackoff({ ...BASE, kind: "overloaded", attempt: 4, maxRetryDelayMs: 30_000, random: () => 1 }),
		).toMatchObject({ delayMs: 30_000 });
	});

	it("honors Retry-After and spreads concurrent agents apart", () => {
		expect(
			computeRetryBackoff({ ...BASE, kind: "rate_limit", attempt: 1, retryAfterMs: 30_000, random: () => 0 }),
		).toEqual({ type: "wait", delayMs: 30_000, honoredRetryAfter: true });
		expect(
			computeRetryBackoff({ ...BASE, kind: "rate_limit", attempt: 1, retryAfterMs: 30_000, random: () => 1 }),
		).toEqual({ type: "wait", delayMs: 31_000, honoredRetryAfter: true });
	});

	it("fails fast when Retry-After exceeds the cap instead of waiting silently", () => {
		expect(computeRetryBackoff({ ...BASE, kind: "rate_limit", attempt: 1, retryAfterMs: 3_600_000 })).toEqual({
			type: "abort",
			reason: "Provider asked to wait 3600s, longer than the 60s retry delay cap (retry.provider.maxRetryDelayMs)",
		});
	});

	it("treats maxRetryDelayMs 0 as no cap", () => {
		expect(
			computeRetryBackoff({
				...BASE,
				kind: "rate_limit",
				attempt: 1,
				maxRetryDelayMs: 0,
				retryAfterMs: 3_600_000,
				random: () => 0,
			}),
		).toEqual({ type: "wait", delayMs: 3_600_000, honoredRetryAfter: true });
	});

	it("ignores Retry-After for kinds that keep the plain ladder", () => {
		expect(computeRetryBackoff({ ...BASE, kind: "server_error", attempt: 1, retryAfterMs: 3_600_000 })).toEqual({
			type: "wait",
			delayMs: 2000,
			honoredRetryAfter: false,
		});
	});
});
