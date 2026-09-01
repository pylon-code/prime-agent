import { describe, expect, it, vi } from "vitest";
import {
	RECOVERABLE_OWNED_CONFIRMATION_RETENTION_MS,
	RECOVERABLE_OWNED_DISCONNECTED_RETENTION_MS,
	RECOVERABLE_OWNED_NO_LIFECYCLE_RETENTION_MS,
	RECOVERABLE_OWNED_PREPARE_TIMEOUT_MS,
	RECOVERABLE_OWNED_TERMINAL_RETENTION_MS,
	recoverableOwnedRetention,
} from "../src/modes/daemon/owned-session-recovery-retention.js";

describe("recoverable owned-session retention", () => {
	it("uses the fixed no-lifecycle, prepared, disconnected, and terminal bounds", () => {
		vi.useFakeTimers();
		vi.setSystemTime(1_000);
		expect(RECOVERABLE_OWNED_PREPARE_TIMEOUT_MS).toBe(15_000);
		expect(RECOVERABLE_OWNED_CONFIRMATION_RETENTION_MS).toBe(15 * 60_000);
		expect(recoverableOwnedRetention({ now: Date.now(), busy: false, hasLifecycle: false, terminal: false })).toEqual(
			{
				retentionMs: RECOVERABLE_OWNED_NO_LIFECYCLE_RETENTION_MS,
			},
		);
		const active = recoverableOwnedRetention({ now: Date.now(), busy: true, hasLifecycle: true, terminal: false });
		expect(active).toEqual({
			retentionMs: RECOVERABLE_OWNED_DISCONNECTED_RETENTION_MS,
			activeDeadline: Date.now() + RECOVERABLE_OWNED_DISCONNECTED_RETENTION_MS,
		});
		vi.advanceTimersByTime(RECOVERABLE_OWNED_DISCONNECTED_RETENTION_MS - 20_000);
		expect(
			recoverableOwnedRetention({
				now: Date.now(),
				busy: false,
				hasLifecycle: true,
				terminal: true,
				activeDeadline: active.activeDeadline,
			}),
		).toEqual({ retentionMs: 20_000, activeDeadline: active.activeDeadline });
		expect(recoverableOwnedRetention({ now: Date.now(), busy: false, hasLifecycle: true, terminal: true })).toEqual({
			retentionMs: RECOVERABLE_OWNED_TERMINAL_RETENTION_MS,
		});
		vi.useRealTimers();
	});
});
