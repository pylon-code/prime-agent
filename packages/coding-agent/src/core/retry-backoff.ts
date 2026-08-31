/**
 * Session-level retry policy.
 *
 * The session loop is the only layer that retries a failed turn (provider SDK
 * retries are disabled while it is enabled, see `sdk.ts`), so this module owns
 * the whole decision: how long to wait, when to honor an upstream `Retry-After`,
 * and when to stop laddering entirely.
 *
 * Overload and rate-limit failures back off far harder than the generic ladder
 * and spread concurrent agents apart with jitter, because N subagents sharing
 * one upstream account otherwise retry in lockstep and re-create the burst that
 * caused the failure. Kinds this module does not classify keep the historical
 * fixed exponential ladder.
 */

/** Base backoff for 429s. Rate-limit windows are long; a 2s ladder just re-burns quota. */
const RATE_LIMIT_BASE_DELAY_MS = 15_000;
/** Base backoff for 529/overloaded. Shorter than a rate limit, still far above the generic ladder. */
const OVERLOADED_BASE_DELAY_MS = 10_000;
/** Spread added on top of an honored `Retry-After` so concurrent agents do not resume together. */
const RETRY_AFTER_JITTER_MS = 1_000;

export interface RetryBackoffInput {
	/** `provider_stream_failure` diagnostic kind, when the provider classified the failure. */
	kind?: string;
	/** 1-based retry attempt number. */
	attempt: number;
	/** Configured `retry.baseDelayMs`; also the floor for jittered waits. */
	baseDelayMs: number;
	/** Configured `retry.provider.maxRetryDelayMs`; 0 disables the cap. */
	maxRetryDelayMs: number;
	/** Upstream `Retry-After` in milliseconds, when the provider sent one. */
	retryAfterMs?: number;
	/** Injectable for deterministic tests. */
	random?: () => number;
}

export type RetryBackoffDecision =
	| { type: "wait"; delayMs: number; honoredRetryAfter: boolean }
	| { type: "abort"; reason: string };

function kindBaseDelayMs(kind: string | undefined): number | undefined {
	if (kind === "rate_limit") return RATE_LIMIT_BASE_DELAY_MS;
	if (kind === "overloaded") return OVERLOADED_BASE_DELAY_MS;
	return undefined;
}

function formatSeconds(ms: number): string {
	return `${Math.round(ms / 1000)}s`;
}

/**
 * Auth failures never recover by waiting: the credential is expired, revoked, or
 * wrong, and every extra attempt is another rejected upstream request.
 */
export function isNonRetryableFailureKind(kind: string | undefined): boolean {
	return kind === "auth";
}

export function computeRetryBackoff(input: RetryBackoffInput): RetryBackoffDecision {
	if (isNonRetryableFailureKind(input.kind)) {
		return { type: "abort", reason: "Provider authentication failed; not retrying" };
	}

	const base = kindBaseDelayMs(input.kind);
	if (base === undefined) {
		return {
			type: "wait",
			delayMs: input.baseDelayMs * 2 ** (input.attempt - 1),
			honoredRetryAfter: false,
		};
	}

	const random = input.random ?? Math.random;
	const cap = input.maxRetryDelayMs > 0 ? input.maxRetryDelayMs : Number.POSITIVE_INFINITY;

	if (input.retryAfterMs !== undefined) {
		if (input.retryAfterMs > cap) {
			return {
				type: "abort",
				reason: `Provider asked to wait ${formatSeconds(input.retryAfterMs)}, longer than the ${formatSeconds(cap)} retry delay cap (retry.provider.maxRetryDelayMs)`,
			};
		}
		const spread = Math.round(random() * RETRY_AFTER_JITTER_MS);
		return { type: "wait", delayMs: Math.min(cap, input.retryAfterMs + spread), honoredRetryAfter: true };
	}

	// Full jitter over the exponential window, floored at the configured base
	// delay so a lucky draw never turns a rate limit into an immediate retry.
	const window = Math.min(cap, base * 2 ** (input.attempt - 1));
	const floor = Math.min(input.baseDelayMs, window);
	return { type: "wait", delayMs: Math.max(floor, Math.round(random() * window)), honoredRetryAfter: false };
}
