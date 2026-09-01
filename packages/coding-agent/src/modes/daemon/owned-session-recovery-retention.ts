export const RECOVERABLE_OWNED_NO_LIFECYCLE_RETENTION_MS = 30_000;
export const RECOVERABLE_OWNED_DISCONNECTED_RETENTION_MS = 15 * 60_000;
export const RECOVERABLE_OWNED_CONFIRMATION_RETENTION_MS = 15 * 60_000;
export const RECOVERABLE_OWNED_TERMINAL_RETENTION_MS = 60_000;
export const RECOVERABLE_OWNED_PREPARE_TIMEOUT_MS = 15_000;

export interface RecoverableOwnedRetentionInput {
	now: number;
	busy: boolean;
	hasLifecycle: boolean;
	terminal: boolean;
	activeDeadline?: number;
}

export interface RecoverableOwnedRetention {
	retentionMs: number;
	activeDeadline?: number;
}

export function recoverableOwnedRetention(input: RecoverableOwnedRetentionInput): RecoverableOwnedRetention {
	if (input.busy || (input.hasLifecycle && !input.terminal)) {
		const activeDeadline = input.activeDeadline ?? input.now + RECOVERABLE_OWNED_DISCONNECTED_RETENTION_MS;
		return { retentionMs: Math.max(0, activeDeadline - input.now), activeDeadline };
	}
	if (input.terminal) {
		return {
			retentionMs: Math.max(
				0,
				Math.min(
					RECOVERABLE_OWNED_TERMINAL_RETENTION_MS,
					input.activeDeadline ? input.activeDeadline - input.now : RECOVERABLE_OWNED_TERMINAL_RETENTION_MS,
				),
			),
			...(input.activeDeadline !== undefined ? { activeDeadline: input.activeDeadline } : {}),
		};
	}
	return { retentionMs: RECOVERABLE_OWNED_NO_LIFECYCLE_RETENTION_MS };
}
