import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import {
	chmodSync,
	closeSync,
	fsyncSync,
	lstatSync,
	mkdirSync,
	openSync,
	readdirSync,
	readFileSync,
	renameSync,
	unlinkSync,
	writeSync,
} from "node:fs";
import { join } from "node:path";

export const OWNED_SESSION_RECOVERY_HANDLE_BYTES = 32;
export const OWNED_SESSION_ADOPTION_UNAVAILABLE = "Recoverable owned session adoption is unavailable";

const UUID_SOURCE = "[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
const OWNED_RECORD_NAME_PATTERN = new RegExp(`^(${UUID_SOURCE})\\.json$`, "i");
const OWNED_TEMP_NAME_PATTERN = new RegExp(`^${UUID_SOURCE}\\.json\\.[1-9][0-9]*\\.${UUID_SOURCE}\\.tmp$`, "i");

export type OwnedSessionRecoveryPhase = "connected" | "disconnected" | "prepared" | "committing" | "final";

interface PersistedOwnedSessionRecoveryRecord {
	version: 1;
	recordId: string;
	supervisorGeneration: string;
	phase: OwnedSessionRecoveryPhase;
	ownershipGeneration: number;
	createRequestIdDigest: string;
	createRequestDigest: string;
	createRequestReplayRetired: boolean;
	currentVerifier: string;
	previousVerifier?: string;
	adoptionRequestIdDigest?: string;
	adoptionRequestDigest?: string;
	previousAuthorityDigest?: string;
	authorityDigest: string;
	expiresAt: number;
}

interface MemoryOwnedSessionRecoveryRecord<TAuthority> {
	persisted: PersistedOwnedSessionRecoveryRecord;
	authority: TAuthority;
	currentHandle: string;
	previousHandle?: string;
}

export interface OwnedSessionRecoveryReceipt<TAuthority> {
	readonly recordId: string;
	readonly authority: TAuthority;
	readonly recoveryHandle: string;
	readonly ownershipGeneration: number;
	readonly phase: OwnedSessionRecoveryPhase;
	readonly repeated: boolean;
}

export class OwnedSessionAdoptionUnavailableError extends Error {
	constructor() {
		super(OWNED_SESSION_ADOPTION_UNAVAILABLE);
		this.name = "OwnedSessionAdoptionUnavailableError";
	}
}

function canonicalJson(value: unknown): string {
	const seen = new WeakSet<object>();
	const normalize = (current: unknown): unknown => {
		if (current === null || typeof current === "string" || typeof current === "boolean") return current;
		if (typeof current === "number") {
			if (!Number.isFinite(current)) throw new TypeError("Recovery requests must contain finite numbers");
			return current;
		}
		if (current === undefined) return null;
		if (typeof current !== "object") throw new TypeError("Recovery requests must be JSON serializable");
		if (seen.has(current)) throw new TypeError("Recovery requests must not contain cycles");
		seen.add(current);
		if (Array.isArray(current)) return current.map(normalize);
		const output: Record<string, unknown> = {};
		for (const key of Object.keys(current as Record<string, unknown>).sort()) {
			const entry = (current as Record<string, unknown>)[key];
			if (entry !== undefined) output[key] = normalize(entry);
		}
		return output;
	};
	return JSON.stringify(normalize(value));
}

function isPersistedRecord(value: unknown): value is PersistedOwnedSessionRecoveryRecord {
	if (!value || typeof value !== "object") return false;
	const record = value as Partial<PersistedOwnedSessionRecoveryRecord>;
	return (
		record.version === 1 &&
		typeof record.recordId === "string" &&
		typeof record.supervisorGeneration === "string" &&
		(record.phase === "connected" ||
			record.phase === "disconnected" ||
			record.phase === "prepared" ||
			record.phase === "committing" ||
			record.phase === "final") &&
		Number.isSafeInteger(record.ownershipGeneration) &&
		(record.ownershipGeneration ?? -1) >= 0 &&
		typeof record.createRequestIdDigest === "string" &&
		typeof record.createRequestDigest === "string" &&
		typeof record.createRequestReplayRetired === "boolean" &&
		typeof record.currentVerifier === "string" &&
		(record.previousVerifier === undefined || typeof record.previousVerifier === "string") &&
		(record.adoptionRequestIdDigest === undefined || typeof record.adoptionRequestIdDigest === "string") &&
		(record.adoptionRequestDigest === undefined || typeof record.adoptionRequestDigest === "string") &&
		(record.previousAuthorityDigest === undefined || typeof record.previousAuthorityDigest === "string") &&
		typeof record.authorityDigest === "string" &&
		Number.isSafeInteger(record.expiresAt) &&
		(record.expiresAt ?? 0) > 0
	);
}

/**
 * Private bearer authority for recoverable owned workers. It deliberately has
 * no public descriptor or generic command-journal representation.
 */
export class OwnedSessionRecoveryStore<TAuthority> {
	private readonly secret: Buffer;
	private readonly records = new Map<string, MemoryOwnedSessionRecoveryRecord<TAuthority>>();
	private readonly createRequests = new Map<string, MemoryOwnedSessionRecoveryRecord<TAuthority>>();
	private readonly retiredCreateRequests = new Set<string>();

	constructor(
		private readonly directory: string,
		private readonly supervisorGeneration: string,
		options: { secret?: Buffer; now?: () => number } = {},
	) {
		this.secret = options.secret ? Buffer.from(options.secret) : randomBytes(OWNED_SESSION_RECOVERY_HANDLE_BYTES);
		if (this.secret.length !== OWNED_SESSION_RECOVERY_HANDLE_BYTES) {
			throw new Error("Owned-session recovery secret must be 256 bits");
		}
		this.now = options.now ?? Date.now;
		this.prepareDirectory();
		this.removeUnusableRecords();
	}

	private readonly now: () => number;

	private prepareDirectory(): void {
		let entry: ReturnType<typeof lstatSync>;
		try {
			entry = lstatSync(this.directory);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			mkdirSync(this.directory, { recursive: true, mode: 0o700 });
			entry = lstatSync(this.directory);
		}
		if (entry.isSymbolicLink() || !entry.isDirectory()) {
			throw new Error("Owned-session recovery path must be a private directory");
		}
		chmodSync(this.directory, 0o700);
		const verified = lstatSync(this.directory);
		if (verified.isSymbolicLink() || !verified.isDirectory()) {
			throw new Error("Owned-session recovery path changed during startup");
		}
	}

	digestRequest(value: unknown): string {
		return this.mac("request", canonicalJson(value));
	}

	digestAuthority(value: unknown): string {
		return this.mac("authority", canonicalJson(value));
	}

	create(input: {
		requestIdDigest: string;
		requestDigest: string;
		authorityDigest: string;
		authority: TAuthority;
		expiresAt: number;
	}): OwnedSessionRecoveryReceipt<TAuthority> {
		if (this.retiredCreateRequests.has(input.requestIdDigest)) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		const previous = this.createRequests.get(input.requestIdDigest);
		if (previous) {
			this.assertUsable(previous);
			if (
				previous.persisted.createRequestDigest !== input.requestDigest ||
				previous.persisted.authorityDigest !== input.authorityDigest
			) {
				throw new OwnedSessionAdoptionUnavailableError();
			}
			return this.receipt(previous, false);
		}
		if (!Number.isSafeInteger(input.expiresAt) || input.expiresAt <= this.now()) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		const recordId = randomUUID();
		const currentHandle = randomBytes(OWNED_SESSION_RECOVERY_HANDLE_BYTES).toString("base64url");
		const record: MemoryOwnedSessionRecoveryRecord<TAuthority> = {
			persisted: {
				version: 1,
				recordId,
				supervisorGeneration: this.supervisorGeneration,
				phase: "connected",
				ownershipGeneration: 0,
				createRequestIdDigest: input.requestIdDigest,
				createRequestDigest: input.requestDigest,
				createRequestReplayRetired: false,
				currentVerifier: this.handleVerifier(currentHandle),
				authorityDigest: input.authorityDigest,
				expiresAt: input.expiresAt,
			},
			authority: input.authority,
			currentHandle,
		};
		try {
			this.persist(record);
		} catch (error) {
			try {
				this.unlinkOwnedRegularFile(this.pathFor(recordId));
				this.fsyncDirectory();
			} catch {
				// The supervisor contains the exact new worker; preserve the persistence failure.
			}
			throw error;
		}
		this.records.set(recordId, record);
		this.createRequests.set(input.requestIdDigest, record);
		return this.receipt(record, false);
	}

	getByHandle(recoveryHandle: string): OwnedSessionRecoveryReceipt<TAuthority> {
		const match = this.findHandle(recoveryHandle);
		if (!match) throw new OwnedSessionAdoptionUnavailableError();
		this.assertUsable(match.record);
		return this.receipt(match.record, true);
	}

	getByCreateRequest(
		requestIdDigest: string,
		requestDigest: string,
	): OwnedSessionRecoveryReceipt<TAuthority> | undefined {
		const record = this.createRequests.get(requestIdDigest);
		if (!record) {
			if (this.retiredCreateRequests.has(requestIdDigest)) throw new OwnedSessionAdoptionUnavailableError();
			return undefined;
		}
		this.assertUsable(record);
		if (
			record.persisted.createRequestReplayRetired ||
			record.persisted.ownershipGeneration !== 0 ||
			record.persisted.createRequestDigest !== requestDigest
		) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		return this.receipt(record, true);
	}

	beginAdoption(input: {
		recoveryHandle: string;
		requestIdDigest: string;
		requestDigest: string;
		authorityDigest: string;
		expectedSupervisorGeneration: string;
		expiresAt: number;
	}): OwnedSessionRecoveryReceipt<TAuthority> {
		if (input.expectedSupervisorGeneration !== this.supervisorGeneration) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		const match = this.findHandle(input.recoveryHandle);
		if (!match) throw new OwnedSessionAdoptionUnavailableError();
		const { record, kind } = match;
		this.assertUsable(record);
		const repeated =
			record.persisted.adoptionRequestIdDigest === input.requestIdDigest &&
			record.persisted.adoptionRequestDigest === input.requestDigest &&
			(kind === "previous" || kind === "current");
		const authorityMatches =
			record.persisted.authorityDigest === input.authorityDigest ||
			(repeated && record.persisted.previousAuthorityDigest === input.authorityDigest);
		if (!authorityMatches || (kind === "previous" && !repeated)) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		if (!repeated) {
			record.persisted.createRequestReplayRetired = true;
			this.retiredCreateRequests.add(record.persisted.createRequestIdDigest);
			const previousHandle = record.currentHandle;
			const nextHandle = this.nextHandle(record, input.requestDigest);
			record.previousHandle = previousHandle;
			record.currentHandle = nextHandle;
			record.persisted.previousVerifier = record.persisted.currentVerifier;
			record.persisted.currentVerifier = this.handleVerifier(nextHandle);
			record.persisted.adoptionRequestIdDigest = input.requestIdDigest;
			record.persisted.adoptionRequestDigest = input.requestDigest;
			record.persisted.ownershipGeneration++;
		}
		if (!(repeated && record.persisted.phase === "final")) {
			record.persisted.phase = "prepared";
			record.persisted.expiresAt = input.expiresAt;
			this.persist(record);
		}
		if (record.persisted.createRequestReplayRetired) {
			this.retiredCreateRequests.add(record.persisted.createRequestIdDigest);
			if (this.createRequests.get(record.persisted.createRequestIdDigest) === record) {
				this.createRequests.delete(record.persisted.createRequestIdDigest);
			}
		}
		return this.receipt(record, repeated);
	}

	replaceAuthority(recordId: string, authorityDigest: string, authority: TAuthority): void {
		const record = this.requireRecord(recordId);
		record.persisted.previousAuthorityDigest = record.persisted.authorityDigest;
		record.persisted.authorityDigest = authorityDigest;
		record.authority = authority;
		this.persist(record);
	}

	markConnected(
		recordId: string,
		authorityDigest: string,
		expiresAt: number,
	): OwnedSessionRecoveryReceipt<TAuthority> {
		const record = this.requireRecord(recordId);
		if (record.persisted.authorityDigest !== authorityDigest || record.persisted.phase === "final") {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		this.restorePreviousHandle(record);
		record.persisted.phase = "connected";
		record.persisted.expiresAt = expiresAt;
		this.persist(record);
		return this.receipt(record, true);
	}

	rollbackAdoption(
		recordId: string,
		authorityDigest: string,
		expiresAt: number,
	): OwnedSessionRecoveryReceipt<TAuthority> {
		const record = this.requireRecord(recordId);
		if (
			record.persisted.authorityDigest !== authorityDigest ||
			record.persisted.phase === "final" ||
			record.persisted.phase === "connected"
		) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		this.restorePreviousHandle(record);
		record.persisted.phase = "disconnected";
		record.persisted.expiresAt = expiresAt;
		this.persist(record);
		return this.receipt(record, true);
	}

	markDisconnected(recordId: string, expiresAt: number): void {
		const record = this.requireRecord(recordId);
		if (record.persisted.phase !== "final") record.persisted.phase = "disconnected";
		record.persisted.expiresAt = expiresAt;
		this.persist(record);
	}

	markCommitting(recordId: string): void {
		const record = this.requireRecord(recordId);
		if (record.persisted.phase !== "prepared") throw new OwnedSessionAdoptionUnavailableError();
		record.persisted.phase = "committing";
		this.persist(record);
	}

	markFinal(recordId: string, expiresAt: number): OwnedSessionRecoveryReceipt<TAuthority> {
		const record = this.requireRecord(recordId);
		if (record.persisted.phase !== "committing" && record.persisted.phase !== "final") {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		record.persisted.phase = "final";
		record.persisted.expiresAt = expiresAt;
		this.persist(record);
		return this.receipt(record, true);
	}

	getForCommit(
		recordId: string,
		recoveryHandle: string,
		requestIdDigest: string,
	): OwnedSessionRecoveryReceipt<TAuthority> {
		const record = this.requireRecord(recordId);
		const match = this.findHandle(recoveryHandle);
		if (
			!match ||
			match.kind !== "current" ||
			match.record !== record ||
			record.persisted.phase !== "prepared" ||
			record.persisted.adoptionRequestIdDigest !== requestIdDigest
		) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		return this.receipt(record, true);
	}

	getForConfirmation(recoveryHandle: string, requestIdDigest: string): OwnedSessionRecoveryReceipt<TAuthority> {
		const match = this.findHandle(recoveryHandle);
		if (
			!match ||
			match.kind !== "current" ||
			(match.record.persisted.phase !== "final" && match.record.persisted.phase !== "connected") ||
			match.record.persisted.adoptionRequestIdDigest !== requestIdDigest
		) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		this.assertUsable(match.record);
		return this.receipt(match.record, true);
	}

	confirm(input: {
		recoveryHandle: string;
		requestIdDigest: string;
		authorityDigest: string;
		expiresAt: number;
	}): OwnedSessionRecoveryReceipt<TAuthority> {
		const match = this.findHandle(input.recoveryHandle);
		if (!match || match.kind !== "current") throw new OwnedSessionAdoptionUnavailableError();
		const record = match.record;
		this.assertUsable(record);
		if (
			(record.persisted.phase !== "final" && record.persisted.phase !== "connected") ||
			record.persisted.adoptionRequestIdDigest !== input.requestIdDigest ||
			record.persisted.authorityDigest !== input.authorityDigest
		) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		if (!Number.isSafeInteger(input.expiresAt) || input.expiresAt <= this.now()) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		record.previousHandle = undefined;
		record.persisted.previousVerifier = undefined;
		record.persisted.previousAuthorityDigest = undefined;
		record.persisted.phase = "connected";
		record.persisted.expiresAt = input.expiresAt;
		this.persist(record);
		return this.receipt(record, true);
	}

	get(recordId: string): OwnedSessionRecoveryReceipt<TAuthority> | undefined {
		const record = this.records.get(recordId);
		if (!record) return undefined;
		this.assertUsable(record);
		return this.receipt(record, true);
	}

	remove(recordId: string): void {
		const record = this.records.get(recordId);
		if (!record) return;
		this.unlinkOwnedRegularFile(this.pathFor(recordId));
		this.fsyncDirectory();
		this.records.delete(recordId);
		if (this.createRequests.get(record.persisted.createRequestIdDigest) === record) {
			this.createRequests.delete(record.persisted.createRequestIdDigest);
		}
	}

	sweep(): string[] {
		const removed: string[] = [];
		for (const [recordId, record] of this.records) {
			if (record.persisted.expiresAt > this.now()) continue;
			removed.push(recordId);
			this.remove(recordId);
		}
		return removed;
	}

	private receipt(
		record: MemoryOwnedSessionRecoveryRecord<TAuthority>,
		repeated: boolean,
	): OwnedSessionRecoveryReceipt<TAuthority> {
		return {
			recordId: record.persisted.recordId,
			authority: record.authority,
			recoveryHandle: record.currentHandle,
			ownershipGeneration: record.persisted.ownershipGeneration,
			phase: record.persisted.phase,
			repeated,
		};
	}

	private restorePreviousHandle(record: MemoryOwnedSessionRecoveryRecord<TAuthority>): void {
		if (record.previousHandle && record.persisted.previousVerifier) {
			record.currentHandle = record.previousHandle;
			record.persisted.currentVerifier = record.persisted.previousVerifier;
			record.persisted.ownershipGeneration = Math.max(0, record.persisted.ownershipGeneration - 1);
		}
		record.previousHandle = undefined;
		record.persisted.previousVerifier = undefined;
		record.persisted.previousAuthorityDigest = undefined;
		record.persisted.adoptionRequestIdDigest = undefined;
		record.persisted.adoptionRequestDigest = undefined;
	}

	private requireRecord(recordId: string): MemoryOwnedSessionRecoveryRecord<TAuthority> {
		const record = this.records.get(recordId);
		if (!record) throw new OwnedSessionAdoptionUnavailableError();
		this.assertUsable(record);
		return record;
	}

	private assertUsable(record: MemoryOwnedSessionRecoveryRecord<TAuthority>): void {
		if (
			record.persisted.supervisorGeneration !== this.supervisorGeneration ||
			record.persisted.expiresAt <= this.now()
		) {
			this.remove(record.persisted.recordId);
			throw new OwnedSessionAdoptionUnavailableError();
		}
	}

	private findHandle(
		handle: string,
	): { record: MemoryOwnedSessionRecoveryRecord<TAuthority>; kind: "current" | "previous" } | undefined {
		if (!/^[A-Za-z0-9_-]{43}$/.test(handle)) return undefined;
		const verifier = this.handleVerifier(handle);
		let match: { record: MemoryOwnedSessionRecoveryRecord<TAuthority>; kind: "current" | "previous" } | undefined;
		for (const record of this.records.values()) {
			if (this.equalVerifier(verifier, record.persisted.currentVerifier)) match ??= { record, kind: "current" };
			if (record.persisted.previousVerifier && this.equalVerifier(verifier, record.persisted.previousVerifier)) {
				match ??= { record, kind: "previous" };
			}
		}
		return match;
	}

	private nextHandle(record: MemoryOwnedSessionRecoveryRecord<TAuthority>, requestDigest: string): string {
		return createHmac("sha256", this.secret)
			.update("next-handle\0")
			.update(record.persisted.recordId)
			.update("\0")
			.update(record.persisted.currentVerifier)
			.update("\0")
			.update(requestDigest)
			.digest("base64url");
	}

	private handleVerifier(handle: string): string {
		return this.mac("handle", handle);
	}

	private mac(domain: string, value: string): string {
		return createHmac("sha256", this.secret).update(domain).update("\0").update(value).digest("base64url");
	}

	private equalVerifier(left: string, right: string): boolean {
		const leftBytes = Buffer.from(left);
		const rightBytes = Buffer.from(right);
		return leftBytes.length === rightBytes.length && timingSafeEqual(leftBytes, rightBytes);
	}

	private persist(record: MemoryOwnedSessionRecoveryRecord<TAuthority>): void {
		const path = this.pathFor(record.persisted.recordId);
		const tempPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
		let descriptor: number | undefined;
		let tempExists = false;
		try {
			descriptor = openSync(tempPath, "wx", 0o600);
			tempExists = true;
			writeSync(descriptor, `${JSON.stringify(record.persisted)}\n`);
			fsyncSync(descriptor);
			closeSync(descriptor);
			descriptor = undefined;
			chmodSync(tempPath, 0o600);
			this.assertReplaceableRecordPath(path);
			renameSync(tempPath, path);
			tempExists = false;
			this.fsyncDirectory();
		} finally {
			if (descriptor !== undefined) {
				try {
					closeSync(descriptor);
				} catch {
					// Preserve the persistence failure that triggered this best-effort cleanup.
				}
			}
			if (tempExists) {
				try {
					this.unlinkOwnedRegularFile(tempPath);
				} catch {
					// Preserve the persistence failure that triggered this best-effort cleanup.
				}
			}
		}
	}

	private removeUnusableRecords(): void {
		let changed = false;
		for (const name of readdirSync(this.directory)) {
			const path = join(this.directory, name);
			let entry: ReturnType<typeof lstatSync>;
			try {
				entry = lstatSync(path);
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
				throw error;
			}
			if (entry.isSymbolicLink() || !entry.isFile()) continue;
			if (OWNED_TEMP_NAME_PATTERN.test(name)) {
				unlinkSync(path);
				changed = true;
				continue;
			}
			const recordName = OWNED_RECORD_NAME_PATTERN.exec(name);
			if (!recordName) continue;
			let parsed: unknown;
			try {
				parsed = JSON.parse(readFileSync(path, "utf8"));
			} catch {
				unlinkSync(path);
				changed = true;
				continue;
			}
			if (
				!isPersistedRecord(parsed) ||
				parsed.recordId !== recordName[1] ||
				parsed.supervisorGeneration !== this.supervisorGeneration
			) {
				unlinkSync(path);
				changed = true;
			}
		}
		if (changed) this.fsyncDirectory();
	}

	private assertReplaceableRecordPath(path: string): void {
		try {
			const entry = lstatSync(path);
			if (entry.isSymbolicLink() || !entry.isFile()) {
				throw new Error("Owned-session recovery record path is not a regular file");
			}
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
			throw error;
		}
	}

	private unlinkOwnedRegularFile(path: string): void {
		let entry: ReturnType<typeof lstatSync>;
		try {
			entry = lstatSync(path);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
			throw error;
		}
		if (entry.isSymbolicLink() || !entry.isFile()) {
			throw new Error("Owned-session recovery entry is not a regular file");
		}
		unlinkSync(path);
	}

	private pathFor(recordId: string): string {
		return join(this.directory, `${recordId}.json`);
	}

	private fsyncDirectory(): void {
		const descriptor = openSync(this.directory, "r");
		try {
			fsyncSync(descriptor);
		} finally {
			closeSync(descriptor);
		}
	}
}
