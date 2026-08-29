import { createHash } from "node:crypto";
import type { Usage } from "@earendil-works/pi-ai";

export type PromptLifecycleKind =
	| "model_prompt"
	| "session_command"
	| "extension_command"
	| "input_handler"
	| "injected_prompt";
export type PromptLifecyclePhase = "owned" | "queued" | "delivered" | "completed" | "cancelled" | "failed";

export interface PromptLifecycleSnapshot {
	correlationId: string;
	phase: PromptLifecyclePhase;
	kind: PromptLifecycleKind;
	revision: number;
	deliveryCrossed: boolean;
	usage?: Usage;
}

export interface ExpiredPromptLifecycle {
	correlationId: string;
	deliveryCrossed: boolean;
}

export interface PromptLifecycleStateSnapshot {
	records: PromptLifecycleSnapshot[];
	expired: ExpiredPromptLifecycle[];
}

export interface PromptLifecycleRecoveryStateSnapshot extends PromptLifecycleStateSnapshot {
	pendingUsage: Array<{ correlationId: string; usage: Usage }>;
	requestFingerprints?: Array<{ correlationId: string; fingerprint: string }>;
}

export type PromptLifecycleCancellationResult =
	| {
			status: "cancelled";
			ownershipCrossed: true;
			deliveryCrossed: false;
			lifecycle: PromptLifecycleSnapshot;
	  }
	| {
			status: "too_late";
			ownershipCrossed: true;
			deliveryCrossed: boolean;
			lifecycle: PromptLifecycleSnapshot;
	  }
	| { status: "expired"; ownershipCrossed: true; deliveryCrossed: boolean }
	| { status: "unknown"; ownershipCrossed: "unknown"; deliveryCrossed: "unknown" };

export type PromptEventAttribution = { scope: "prompt"; correlationId: string } | { scope: "session" };

export function isPromptEventAttribution(value: unknown): value is PromptEventAttribution {
	if (typeof value !== "object" || value === null || !("scope" in value)) return false;
	if (value.scope === "session") return true;
	return (
		value.scope === "prompt" &&
		"correlationId" in value &&
		typeof value.correlationId === "string" &&
		value.correlationId.length > 0 &&
		value.correlationId.length <= 128
	);
}

export interface PromptLifecycleEvent extends PromptLifecycleSnapshot {
	type: "prompt_lifecycle";
	promptCorrelationId: string;
}

const TERMINAL_PHASES = new Set<PromptLifecyclePhase>(["completed", "cancelled", "failed"]);
const LEGAL_TRANSITIONS: Readonly<Record<PromptLifecyclePhase, ReadonlySet<PromptLifecyclePhase>>> = {
	owned: new Set(["queued", "delivered", "cancelled", "failed"]),
	queued: new Set(["delivered", "cancelled", "failed"]),
	delivered: new Set(["completed", "failed"]),
	completed: new Set(),
	cancelled: new Set(),
	failed: new Set(),
};
const PROMPT_LIFECYCLE_KINDS = new Set<PromptLifecycleKind>([
	"model_prompt",
	"session_command",
	"extension_command",
	"input_handler",
	"injected_prompt",
]);
const PROMPT_LIFECYCLE_PHASES = new Set<PromptLifecyclePhase>([
	"owned",
	"queued",
	"delivered",
	"completed",
	"cancelled",
	"failed",
]);

function canonicalizePromptRequestValue(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(canonicalizePromptRequestValue);
	if (typeof value !== "object" || value === null) return value;
	return Object.fromEntries(
		Object.entries(value)
			.filter(([, entry]) => entry !== undefined)
			.sort(([left], [right]) => left.localeCompare(right))
			.map(([key, entry]) => [key, canonicalizePromptRequestValue(entry)]),
	);
}

export function createPromptRequestFingerprint(input: {
	message: string;
	images?: readonly unknown[];
	queueIfBusy?: boolean;
}): string {
	return createHash("sha256")
		.update(
			JSON.stringify(
				canonicalizePromptRequestValue([
					input.message,
					input.images && input.images.length > 0 ? input.images : null,
					input.queueIfBusy === true,
				]),
			),
		)
		.digest("hex");
}

function validateCorrelationId(correlationId: string): void {
	if (correlationId.length === 0 || correlationId.length > 128) {
		throw new Error("Prompt correlation id must contain between 1 and 128 characters");
	}
}

function validUsage(usage: Usage): boolean {
	return [
		usage.input,
		usage.output,
		usage.cacheRead,
		usage.cacheWrite,
		usage.totalTokens,
		usage.cost.input,
		usage.cost.output,
		usage.cost.cacheRead,
		usage.cost.cacheWrite,
		usage.cost.total,
	].every((value) => Number.isFinite(value) && value >= 0);
}

function cloneUsage(usage: Usage): Usage {
	return { ...usage, cost: { ...usage.cost } };
}

function addUsage(target: Usage, usage: Usage): void {
	target.input += usage.input;
	target.output += usage.output;
	target.cacheRead += usage.cacheRead;
	target.cacheWrite += usage.cacheWrite;
	target.totalTokens += usage.totalTokens;
	target.cost.input += usage.cost.input;
	target.cost.output += usage.cost.output;
	target.cost.cacheRead += usage.cost.cacheRead;
	target.cost.cacheWrite += usage.cost.cacheWrite;
	target.cost.total += usage.cost.total;
}

export function isPromptLifecycleTerminal(phase: PromptLifecyclePhase): boolean {
	return TERMINAL_PHASES.has(phase);
}

export function isPromptLifecycleSnapshot(value: unknown): value is PromptLifecycleSnapshot {
	if (
		!isRecord(value) ||
		typeof value.correlationId !== "string" ||
		value.correlationId.length === 0 ||
		value.correlationId.length > 128 ||
		typeof value.phase !== "string" ||
		!PROMPT_LIFECYCLE_PHASES.has(value.phase as PromptLifecyclePhase) ||
		typeof value.kind !== "string" ||
		!PROMPT_LIFECYCLE_KINDS.has(value.kind as PromptLifecycleKind) ||
		typeof value.revision !== "number" ||
		!Number.isInteger(value.revision) ||
		value.revision <= 0 ||
		typeof value.deliveryCrossed !== "boolean"
	) {
		return false;
	}
	const snapshot = value as unknown as PromptLifecycleSnapshot;
	if (
		(snapshot.phase === "owned" || snapshot.phase === "queued" || snapshot.phase === "cancelled") &&
		snapshot.deliveryCrossed
	) {
		return false;
	}
	if ((snapshot.phase === "delivered" || snapshot.phase === "completed") && !snapshot.deliveryCrossed) return false;
	if (
		snapshot.usage !== undefined &&
		(!isPromptLifecycleTerminal(snapshot.phase) || !isUsageSnapshot(snapshot.usage))
	) {
		return false;
	}
	return true;
}

export function isPromptLifecycleSuccessor(current: PromptLifecycleSnapshot, next: PromptLifecycleSnapshot): boolean {
	if (
		current.correlationId !== next.correlationId ||
		current.kind !== next.kind ||
		next.revision <= current.revision ||
		!LEGAL_TRANSITIONS[current.phase].has(next.phase) ||
		next.deliveryCrossed !== (current.deliveryCrossed || next.phase === "delivered")
	) {
		return false;
	}
	if ((next.phase === "owned" || next.phase === "queued" || next.phase === "cancelled") && next.deliveryCrossed) {
		return false;
	}
	if ((next.phase === "delivered" || next.phase === "completed") && !next.deliveryCrossed) return false;
	return true;
}

export class PromptLifecycleStore {
	private readonly entries = new Map<string, PromptLifecycleSnapshot>();
	private readonly usage = new Map<string, Usage>();
	private readonly requestFingerprints = new Map<string, string>();
	private readonly terminalOrder: string[] = [];
	private readonly expired = new Map<string, ExpiredPromptLifecycle>();
	private readonly expiredOrder: string[] = [];
	private revision = 0;

	constructor(
		private readonly terminalRetention = 256,
		private readonly expiredRetention = 256,
	) {
		if (!Number.isInteger(terminalRetention) || terminalRetention < 0) {
			throw new Error("Prompt lifecycle terminal retention must be a non-negative integer");
		}
		if (!Number.isInteger(expiredRetention) || expiredRetention < 0) {
			throw new Error("Prompt lifecycle tombstone retention must be a non-negative integer");
		}
	}

	begin(correlationId: string, kind: PromptLifecycleKind, requestFingerprint?: string): PromptLifecycleSnapshot {
		validateCorrelationId(correlationId);
		if (requestFingerprint !== undefined && !/^[0-9a-f]{64}$/.test(requestFingerprint)) {
			throw new Error("Prompt lifecycle request fingerprint must be a SHA-256 digest");
		}
		if (this.entries.has(correlationId) || this.expired.has(correlationId)) {
			throw new Error(`Prompt correlation id is already in use: ${correlationId}`);
		}
		const snapshot = this.createSnapshot(correlationId, "owned", kind, false);
		this.entries.set(correlationId, snapshot);
		if (requestFingerprint !== undefined) this.requestFingerprints.set(correlationId, requestFingerprint);
		return snapshot;
	}

	transition(correlationId: string, phase: Exclude<PromptLifecyclePhase, "owned">): PromptLifecycleSnapshot {
		const current = this.entries.get(correlationId);
		if (!current) throw new Error(`Unknown prompt correlation id: ${correlationId}`);
		if (current.phase === phase) return current;
		if (!LEGAL_TRANSITIONS[current.phase].has(phase)) {
			throw new Error(`Illegal prompt lifecycle transition: ${current.phase} -> ${phase}`);
		}
		const snapshot = this.createSnapshot(
			correlationId,
			phase,
			current.kind,
			current.deliveryCrossed || phase === "delivered",
			this.usage.get(correlationId),
		);
		this.entries.set(correlationId, snapshot);
		if (isPromptLifecycleTerminal(phase)) {
			this.usage.delete(correlationId);
			this.terminalOrder.push(correlationId);
			this.pruneTerminals();
		}
		return snapshot;
	}

	addUsage(correlationId: string, usage: Usage): void {
		const lifecycle = this.entries.get(correlationId);
		if (!lifecycle || isPromptLifecycleTerminal(lifecycle.phase)) return;
		const current = this.usage.get(correlationId);
		if (current) addUsage(current, usage);
		else this.usage.set(correlationId, cloneUsage(usage));
	}

	get(correlationId: string): PromptLifecycleSnapshot | undefined {
		return this.entries.get(correlationId);
	}

	getExpired(correlationId: string): ExpiredPromptLifecycle | undefined {
		return this.expired.get(correlationId);
	}

	getRequestFingerprint(correlationId: string): string | undefined {
		return this.requestFingerprints.get(correlationId);
	}

	snapshot(): PromptLifecycleStateSnapshot {
		return {
			records: [...this.entries.values()].sort((left, right) => left.revision - right.revision),
			expired: [...this.expired.values()],
		};
	}

	recoverySnapshot(): PromptLifecycleRecoveryStateSnapshot {
		return {
			...this.snapshot(),
			pendingUsage: [...this.usage].map(([correlationId, usage]) => ({
				correlationId,
				usage: cloneUsage(usage),
			})),
			requestFingerprints: [...this.requestFingerprints].map(([correlationId, fingerprint]) => ({
				correlationId,
				fingerprint,
			})),
		};
	}

	restore(snapshot: PromptLifecycleRecoveryStateSnapshot): void {
		if (this.entries.size > 0 || this.expired.size > 0 || this.usage.size > 0 || this.requestFingerprints.size > 0) {
			throw new Error("Prompt lifecycle state can only be restored into an empty store");
		}
		const seen = new Set<string>();
		let maxRevision = 0;
		for (const lifecycle of snapshot.records) {
			validateCorrelationId(lifecycle.correlationId);
			if (seen.has(lifecycle.correlationId)) {
				throw new Error(`Duplicate prompt lifecycle recovery id: ${lifecycle.correlationId}`);
			}
			if (!PROMPT_LIFECYCLE_KINDS.has(lifecycle.kind) || !PROMPT_LIFECYCLE_PHASES.has(lifecycle.phase)) {
				throw new Error(`Invalid prompt lifecycle recovery record: ${lifecycle.correlationId}`);
			}
			if (!Number.isInteger(lifecycle.revision) || lifecycle.revision <= 0) {
				throw new Error(`Invalid prompt lifecycle revision: ${lifecycle.correlationId}`);
			}
			if (
				(lifecycle.phase === "owned" || lifecycle.phase === "queued" || lifecycle.phase === "cancelled") &&
				lifecycle.deliveryCrossed
			) {
				throw new Error(`Invalid prompt lifecycle delivery boundary: ${lifecycle.correlationId}`);
			}
			if ((lifecycle.phase === "delivered" || lifecycle.phase === "completed") && !lifecycle.deliveryCrossed) {
				throw new Error(`Invalid prompt lifecycle delivery boundary: ${lifecycle.correlationId}`);
			}
			if (
				lifecycle.usage !== undefined &&
				(!isPromptLifecycleTerminal(lifecycle.phase) || !validUsage(lifecycle.usage))
			) {
				throw new Error(`Invalid prompt lifecycle usage: ${lifecycle.correlationId}`);
			}
			seen.add(lifecycle.correlationId);
			maxRevision = Math.max(maxRevision, lifecycle.revision);
			this.entries.set(lifecycle.correlationId, {
				...lifecycle,
				...(lifecycle.usage === undefined ? {} : { usage: cloneUsage(lifecycle.usage) }),
			});
			if (isPromptLifecycleTerminal(lifecycle.phase)) this.terminalOrder.push(lifecycle.correlationId);
		}
		this.terminalOrder.sort(
			(left, right) => (this.entries.get(left)?.revision ?? 0) - (this.entries.get(right)?.revision ?? 0),
		);
		for (const tombstone of snapshot.expired) {
			validateCorrelationId(tombstone.correlationId);
			if (seen.has(tombstone.correlationId)) {
				throw new Error(`Duplicate prompt lifecycle recovery id: ${tombstone.correlationId}`);
			}
			seen.add(tombstone.correlationId);
			this.expired.set(tombstone.correlationId, { ...tombstone });
			this.expiredOrder.push(tombstone.correlationId);
		}
		for (const pending of snapshot.pendingUsage) {
			const lifecycle = this.entries.get(pending.correlationId);
			if (!lifecycle || isPromptLifecycleTerminal(lifecycle.phase) || !validUsage(pending.usage)) {
				throw new Error(`Invalid pending prompt lifecycle usage: ${pending.correlationId}`);
			}
			if (this.usage.has(pending.correlationId)) {
				throw new Error(`Duplicate pending prompt lifecycle usage: ${pending.correlationId}`);
			}
			this.usage.set(pending.correlationId, cloneUsage(pending.usage));
		}
		for (const entry of snapshot.requestFingerprints ?? []) {
			if (
				!seen.has(entry.correlationId) ||
				this.requestFingerprints.has(entry.correlationId) ||
				!/^[0-9a-f]{64}$/.test(entry.fingerprint)
			) {
				throw new Error(`Invalid prompt lifecycle request fingerprint: ${entry.correlationId}`);
			}
			this.requestFingerprints.set(entry.correlationId, entry.fingerprint);
		}
		this.revision = maxRevision;
		this.pruneTerminals();
	}

	private createSnapshot(
		correlationId: string,
		phase: PromptLifecyclePhase,
		kind: PromptLifecycleKind,
		deliveryCrossed: boolean,
		usage?: Usage,
	): PromptLifecycleSnapshot {
		this.revision += 1;
		return {
			correlationId,
			phase,
			kind,
			revision: this.revision,
			deliveryCrossed,
			...(usage === undefined ? {} : { usage: cloneUsage(usage) }),
		};
	}

	private pruneTerminals(): void {
		while (this.terminalOrder.length > this.terminalRetention) {
			const correlationId = this.terminalOrder.shift();
			if (correlationId === undefined) continue;
			const lifecycle = this.entries.get(correlationId);
			if (!lifecycle) continue;
			this.entries.delete(correlationId);
			this.expired.set(correlationId, {
				correlationId,
				deliveryCrossed: lifecycle.deliveryCrossed,
			});
			this.expiredOrder.push(correlationId);
		}
		while (this.expiredOrder.length > this.expiredRetention) {
			const correlationId = this.expiredOrder.shift();
			if (correlationId !== undefined) {
				this.expired.delete(correlationId);
				this.requestFingerprints.delete(correlationId);
			}
		}
	}
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isUsageSnapshot(value: unknown): value is Usage {
	if (!isRecord(value) || !isRecord(value.cost)) return false;
	return [
		value.input,
		value.output,
		value.cacheRead,
		value.cacheWrite,
		value.totalTokens,
		value.cost.input,
		value.cost.output,
		value.cost.cacheRead,
		value.cost.cacheWrite,
		value.cost.total,
	].every((entry) => typeof entry === "number" && Number.isFinite(entry) && entry >= 0);
}

export function isPromptLifecycleStateSnapshot(value: unknown): value is PromptLifecycleStateSnapshot {
	if (!isRecord(value) || !Array.isArray(value.records) || !Array.isArray(value.expired)) return false;
	return isPromptLifecycleRecoveryStateSnapshot({ ...value, pendingUsage: [] });
}

export function isPromptLifecycleRecoveryStateSnapshot(value: unknown): value is PromptLifecycleRecoveryStateSnapshot {
	if (!isRecord(value) || !Array.isArray(value.records) || !Array.isArray(value.expired)) return false;
	if (!Array.isArray(value.pendingUsage)) return false;
	if (value.requestFingerprints !== undefined && !Array.isArray(value.requestFingerprints)) return false;
	if (
		!value.records.every(
			(record) =>
				isRecord(record) &&
				typeof record.correlationId === "string" &&
				typeof record.phase === "string" &&
				typeof record.kind === "string" &&
				typeof record.revision === "number" &&
				typeof record.deliveryCrossed === "boolean" &&
				(record.usage === undefined || isUsageSnapshot(record.usage)),
		) ||
		!value.expired.every(
			(tombstone) =>
				isRecord(tombstone) &&
				typeof tombstone.correlationId === "string" &&
				typeof tombstone.deliveryCrossed === "boolean",
		) ||
		!value.pendingUsage.every(
			(pending) => isRecord(pending) && typeof pending.correlationId === "string" && isUsageSnapshot(pending.usage),
		) ||
		!(value.requestFingerprints ?? []).every(
			(entry) => isRecord(entry) && typeof entry.correlationId === "string" && typeof entry.fingerprint === "string",
		)
	) {
		return false;
	}
	try {
		const validator = new PromptLifecycleStore(Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER);
		validator.restore(value as unknown as PromptLifecycleRecoveryStateSnapshot);
		return true;
	} catch {
		return false;
	}
}
