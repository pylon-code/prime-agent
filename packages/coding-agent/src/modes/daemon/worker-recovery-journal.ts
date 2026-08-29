import { createHash } from "node:crypto";
import {
	chmodSync,
	closeSync,
	fsyncSync,
	mkdirSync,
	openSync,
	readFileSync,
	renameSync,
	writeFileSync,
	writeSync,
} from "node:fs";
import { dirname } from "node:path";
import type { SessionActionRecoverySnapshot } from "../../core/agent-session.js";
import {
	isPromptLifecycleRecoveryStateSnapshot,
	type PromptLifecycleRecoveryStateSnapshot,
} from "../../core/prompt-lifecycle.js";

export interface WorkerRecoveryRecord {
	version: 1 | 2 | 3;
	activeSessionId: string;
	sessionId: string;
	sessionFile?: string;
	busy: boolean;
	operation: string;
	promptLifecycles?: PromptLifecycleRecoveryStateSnapshot;
	sessionActions?: SessionActionRecoverySnapshot;
	sessionActionsHash?: string;
	recordedAt: string;
}

function sessionActionsHash(snapshot: SessionActionRecoverySnapshot): string {
	return createHash("sha256").update(JSON.stringify(snapshot)).digest("hex");
}

function isSessionActionRecoverySnapshot(value: unknown): value is SessionActionRecoverySnapshot {
	if (typeof value !== "object" || value === null) return false;
	const snapshot = value as Partial<SessionActionRecoverySnapshot>;
	return (
		snapshot.formatVersion === 1 &&
		Array.isArray(snapshot.actions) &&
		snapshot.actions.every(
			(action) =>
				typeof action === "object" &&
				action !== null &&
				typeof action.id === "string" &&
				(typeof action.promptCorrelationId === "undefined" || typeof action.promptCorrelationId === "string"),
		) &&
		(snapshot.promptLifecycles === undefined || isPromptLifecycleRecoveryStateSnapshot(snapshot.promptLifecycles))
	);
}

function parseRecords(path: string): Map<string, WorkerRecoveryRecord> {
	const latest = new Map<string, WorkerRecoveryRecord>();
	let contents: string;
	try {
		contents = readFileSync(path, "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") {
			return latest;
		}
		throw error;
	}
	for (const line of contents.split("\n")) {
		if (!line) {
			continue;
		}
		let record: WorkerRecoveryRecord;
		try {
			record = JSON.parse(line) as WorkerRecoveryRecord;
		} catch {
			continue;
		}
		if (
			(record.version === 1 || record.version === 2 || record.version === 3) &&
			(record.promptLifecycles === undefined || isPromptLifecycleRecoveryStateSnapshot(record.promptLifecycles)) &&
			(record.sessionActions === undefined ||
				(isSessionActionRecoverySnapshot(record.sessionActions) &&
					typeof record.sessionActionsHash === "string" &&
					record.sessionActionsHash === sessionActionsHash(record.sessionActions))) &&
			typeof record.activeSessionId === "string" &&
			typeof record.sessionId === "string" &&
			typeof record.busy === "boolean" &&
			typeof record.operation === "string"
		) {
			latest.set(record.activeSessionId, record);
		}
	}
	return latest;
}

export class WorkerRecoveryJournal {
	private readonly latest: Map<string, WorkerRecoveryRecord>;

	constructor(private readonly path: string) {
		mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
		this.latest = parseRecords(path);
	}

	record(input: Omit<WorkerRecoveryRecord, "version" | "recordedAt" | "sessionActionsHash">): void {
		const previous = this.latest.get(input.activeSessionId);
		if (
			previous?.busy === input.busy &&
			previous.operation === input.operation &&
			previous.sessionFile === input.sessionFile &&
			JSON.stringify(previous.promptLifecycles) === JSON.stringify(input.promptLifecycles) &&
			JSON.stringify(previous.sessionActions) === JSON.stringify(input.sessionActions)
		) {
			return;
		}
		const record: WorkerRecoveryRecord = {
			version: 3,
			...input,
			...(input.sessionActions ? { sessionActionsHash: sessionActionsHash(input.sessionActions) } : {}),
			recordedAt: new Date().toISOString(),
		};
		this.append(record);
		this.latest.set(record.activeSessionId, record);
		if ([...this.latest.values()].every((entry) => !entry.busy)) {
			this.compact();
		}
	}

	getLatest(): WorkerRecoveryRecord[] {
		return [...this.latest.values()];
	}

	static readLatest(path: string): WorkerRecoveryRecord[] {
		return [...parseRecords(path).values()];
	}

	private append(record: WorkerRecoveryRecord): void {
		const descriptor = openSync(this.path, "a", 0o600);
		try {
			writeSync(descriptor, `${JSON.stringify(record)}\n`);
			fsyncSync(descriptor);
		} finally {
			closeSync(descriptor);
		}
		chmodSync(this.path, 0o600);
	}

	private compact(): void {
		const tempPath = `${this.path}.${process.pid}.tmp`;
		writeFileSync(tempPath, `${[...this.latest.values()].map((record) => JSON.stringify(record)).join("\n")}\n`, {
			mode: 0o600,
		});
		chmodSync(tempPath, 0o600);
		renameSync(tempPath, this.path);
	}
}
