import { randomBytes, randomUUID } from "node:crypto";
import {
	chmodSync,
	lstatSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	statSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
	OWNED_SESSION_ADOPTION_UNAVAILABLE,
	OwnedSessionRecoveryStore,
} from "../src/modes/daemon/owned-session-recovery-store.js";

const directories: string[] = [];

afterEach(() => {
	for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

function fixture(now = 1_000) {
	const directory = mkdtempSync(join(tmpdir(), "owned-recovery-store-"));
	directories.push(directory);
	let clock = now;
	const store = new OwnedSessionRecoveryStore<{ worker: string }>(directory, randomUUID(), {
		secret: randomBytes(32),
		now: () => clock,
	});
	const createRequest = "create-canary-request";
	const authority = {
		activeSessionId: "active-canary",
		sessionId: "session-canary",
		correlationId: "correlation-canary",
		mcpOwnerId: "mcp-canary",
	};
	const created = store.create({
		requestIdDigest: store.digestRequest({ requestId: createRequest }),
		requestDigest: store.digestRequest({ requestId: createRequest, secret: "prompt-canary" }),
		authorityDigest: store.digestAuthority(authority),
		authority: { worker: "private-worker" },
		expiresAt: 10_000,
	});
	return {
		directory,
		store,
		created,
		authority,
		setNow(value: number) {
			clock = value;
		},
	};
}

function bytes(directory: string): string {
	return readdirSync(directory)
		.map((name) => readFileSync(join(directory, name), "utf8"))
		.join("\n");
}

describe("OwnedSessionRecoveryStore", () => {
	it("writes atomic private records without bearer or authority canaries", () => {
		const { directory, created } = fixture();
		expect(statSync(directory).mode & 0o777).toBe(0o700);
		const names = readdirSync(directory);
		expect(names).toHaveLength(1);
		expect(statSync(join(directory, names[0]!)).mode & 0o777).toBe(0o600);
		const persisted = bytes(directory);
		for (const canary of [
			created.recoveryHandle,
			"create-canary-request",
			"prompt-canary",
			"active-canary",
			"session-canary",
			"correlation-canary",
			"mcp-canary",
			"private-worker",
		]) {
			expect(persisted).not.toContain(canary);
		}
		expect(readdirSync(directory).some((name) => name.endsWith(".tmp"))).toBe(false);
	});

	it("converges retries on one deterministic next handle until confirmation", () => {
		const { store, created, authority } = fixture();
		const requestIdDigest = store.digestRequest({ requestId: "adopt-request-one" });
		const requestDigest = store.digestRequest({ requestId: "adopt-request-one", cursor: 1 });
		const authorityDigest = store.digestAuthority(authority);
		const first = store.beginAdoption({
			recoveryHandle: created.recoveryHandle,
			requestIdDigest,
			requestDigest,
			authorityDigest,
			expectedSupervisorGeneration: (store as unknown as { supervisorGeneration: string }).supervisorGeneration,
			expiresAt: 9_000,
		});
		const retry = store.beginAdoption({
			recoveryHandle: created.recoveryHandle,
			requestIdDigest,
			requestDigest,
			authorityDigest,
			expectedSupervisorGeneration: (store as unknown as { supervisorGeneration: string }).supervisorGeneration,
			expiresAt: 9_000,
		});
		expect(retry.recoveryHandle).toBe(first.recoveryHandle);
		expect(retry.ownershipGeneration).toBe(first.ownershipGeneration);
		store.markCommitting(first.recordId);
		store.markFinal(first.recordId, 9_000);
		const finalRetry = store.beginAdoption({
			recoveryHandle: created.recoveryHandle,
			requestIdDigest,
			requestDigest,
			authorityDigest,
			expectedSupervisorGeneration: (store as unknown as { supervisorGeneration: string }).supervisorGeneration,
			expiresAt: 9_000,
		});
		expect(finalRetry).toMatchObject({ phase: "final", recoveryHandle: first.recoveryHandle });
		store.confirm({ recoveryHandle: first.recoveryHandle, requestIdDigest, authorityDigest, expiresAt: 9_000 });
		expect(() =>
			store.beginAdoption({
				recoveryHandle: created.recoveryHandle,
				requestIdDigest,
				requestDigest,
				authorityDigest,
				expectedSupervisorGeneration: (store as unknown as { supervisorGeneration: string }).supervisorGeneration,
				expiresAt: 9_000,
			}),
		).toThrow(OWNED_SESSION_ADOPTION_UNAVAILABLE);
	});

	it("uses one non-enumerating error for wrong, stale, mismatched, and expired authority", () => {
		const { store, created, authority, setNow } = fixture();
		const authorityDigest = store.digestAuthority(authority);
		const requestIdDigest = store.digestRequest({ requestId: "adopt-request-two" });
		const requestDigest = store.digestRequest({ requestId: "adopt-request-two" });
		const attempts = [
			() =>
				store.beginAdoption({
					recoveryHandle: randomBytes(32).toString("base64url"),
					requestIdDigest,
					requestDigest,
					authorityDigest,
					expectedSupervisorGeneration: "stale",
					expiresAt: 9_000,
				}),
			() =>
				store.beginAdoption({
					recoveryHandle: created.recoveryHandle,
					requestIdDigest,
					requestDigest,
					authorityDigest: store.digestAuthority({ other: true }),
					expectedSupervisorGeneration: "stale",
					expiresAt: 9_000,
				}),
		];
		for (const attempt of attempts) {
			let message = "";
			try {
				attempt();
			} catch (error) {
				message = error instanceof Error ? error.message : String(error);
			}
			expect(message).toBe(OWNED_SESSION_ADOPTION_UNAVAILABLE);
		}
		setNow(10_001);
		expect(() =>
			store.beginAdoption({
				recoveryHandle: created.recoveryHandle,
				requestIdDigest,
				requestDigest,
				authorityDigest,
				expectedSupervisorGeneration: "stale",
				expiresAt: 11_000,
			}),
		).toThrow(OWNED_SESSION_ADOPTION_UNAVAILABLE);
	});

	it("fails closed across replacement secrets and removes corrupt records", () => {
		const { directory, created } = fixture();
		writeFileSync(join(directory, `${randomUUID()}.json`), "{not-json", { mode: 0o666 });
		chmodSync(directory, 0o777);
		const replacement = new OwnedSessionRecoveryStore<{ worker: string }>(directory, randomUUID());
		expect(statSync(directory).mode & 0o777).toBe(0o700);
		expect(readdirSync(directory)).toEqual([]);
		expect(() =>
			replacement.beginAdoption({
				recoveryHandle: created.recoveryHandle,
				requestIdDigest: replacement.digestRequest({ requestId: "retry" }),
				requestDigest: replacement.digestRequest({ requestId: "retry" }),
				authorityDigest: replacement.digestAuthority({}),
				expectedSupervisorGeneration: "stale",
				expiresAt: 20_000,
			}),
		).toThrow(OWNED_SESSION_ADOPTION_UNAVAILABLE);
	});

	it.skipIf(process.platform === "win32")("rejects a recovery-directory symlink without touching its victim", () => {
		const root = mkdtempSync(join(tmpdir(), "owned-recovery-symlink-"));
		directories.push(root);
		const victim = join(root, "victim");
		const recoveryPath = join(root, "recovery");
		mkdirSync(victim, { mode: 0o755 });
		const canaryPath = join(victim, "canary.txt");
		writeFileSync(canaryPath, "victim-canary", { mode: 0o644 });
		symlinkSync(victim, recoveryPath);

		expect(() => new OwnedSessionRecoveryStore(recoveryPath, randomUUID())).toThrow("must be a private directory");
		expect(readFileSync(canaryPath, "utf8")).toBe("victim-canary");
		expect(statSync(victim).mode & 0o777).toBe(0o755);
	});

	it.skipIf(process.platform === "win32")("unlinks its regular temp file when an atomic replace fails", () => {
		const root = mkdtempSync(join(tmpdir(), "owned-recovery-persist-error-"));
		directories.push(root);
		const directory = join(root, "recovery");
		const store = new OwnedSessionRecoveryStore<{ worker: string }>(directory, randomUUID());
		const victim = join(root, "victim.txt");
		writeFileSync(victim, "victim-canary");
		const recordPath = join(directory, `${randomUUID()}.json`);
		symlinkSync(victim, recordPath);
		(store as unknown as { pathFor(recordId: string): string }).pathFor = () => recordPath;

		expect(() =>
			store.create({
				requestIdDigest: store.digestRequest({ requestId: "persist-failure" }),
				requestDigest: store.digestRequest({ requestId: "persist-failure", cursor: 1 }),
				authorityDigest: store.digestAuthority({ worker: "worker" }),
				authority: { worker: "worker" },
				expiresAt: Date.now() + 60_000,
			}),
		).toThrow("not a regular file");
		expect(readdirSync(directory)).toEqual([recordPath.split("/").at(-1)]);
		expect(lstatSync(recordPath).isSymbolicLink()).toBe(true);
		expect(readFileSync(victim, "utf8")).toBe("victim-canary");
	});

	it.skipIf(process.platform === "win32")(
		"reclaims only owned regular stale temps and never reads or removes symlink entries",
		() => {
			const root = mkdtempSync(join(tmpdir(), "owned-recovery-startup-"));
			directories.push(root);
			const directory = join(root, "recovery");
			mkdirSync(directory, { mode: 0o700 });
			const victim = join(root, "victim.txt");
			writeFileSync(victim, "victim-canary");
			const recordId = randomUUID();
			const staleTemp = `${recordId}.json.${process.pid}.${randomUUID()}.tmp`;
			const foreignTemp = "foreign.tmp";
			const recordSymlink = `${randomUUID()}.json`;
			const tempSymlink = `${randomUUID()}.json.${process.pid}.${randomUUID()}.tmp`;
			writeFileSync(join(directory, staleTemp), "stale");
			writeFileSync(join(directory, foreignTemp), "foreign");
			symlinkSync(victim, join(directory, recordSymlink));
			symlinkSync(victim, join(directory, tempSymlink));

			new OwnedSessionRecoveryStore(directory, randomUUID());

			expect(readdirSync(directory).sort()).toEqual([foreignTemp, recordSymlink, tempSymlink].sort());
			expect(lstatSync(join(directory, recordSymlink)).isSymbolicLink()).toBe(true);
			expect(lstatSync(join(directory, tempSymlink)).isSymbolicLink()).toBe(true);
			expect(readFileSync(victim, "utf8")).toBe("victim-canary");
			expect(readFileSync(join(directory, foreignTemp), "utf8")).toBe("foreign");
		},
	);

	it("permanently retires create replay when adoption first begins", () => {
		const { store, created, authority } = fixture();
		const createRequestIdDigest = store.digestRequest({ requestId: "create-canary-request" });
		const createRequestDigest = store.digestRequest({
			requestId: "create-canary-request",
			secret: "prompt-canary",
		});
		expect(store.getByCreateRequest(createRequestIdDigest, createRequestDigest)?.recoveryHandle).toBe(
			created.recoveryHandle,
		);
		const authorityDigest = store.digestAuthority(authority);
		store.beginAdoption({
			recoveryHandle: created.recoveryHandle,
			requestIdDigest: store.digestRequest({ requestId: "adopt-retire-create" }),
			requestDigest: store.digestRequest({ requestId: "adopt-retire-create", cursor: 1 }),
			authorityDigest,
			expectedSupervisorGeneration: (store as unknown as { supervisorGeneration: string }).supervisorGeneration,
			expiresAt: 9_000,
		});
		expect(() => store.getByCreateRequest(createRequestIdDigest, createRequestDigest)).toThrow(
			OWNED_SESSION_ADOPTION_UNAVAILABLE,
		);
		store.rollbackAdoption(created.recordId, authorityDigest, 9_000);
		expect(() => store.getByCreateRequest(createRequestIdDigest, createRequestDigest)).toThrow(
			OWNED_SESSION_ADOPTION_UNAVAILABLE,
		);
		expect(() =>
			store.create({
				requestIdDigest: createRequestIdDigest,
				requestDigest: createRequestDigest,
				authorityDigest,
				authority: { worker: "private-worker" },
				expiresAt: 9_000,
			}),
		).toThrow(OWNED_SESSION_ADOPTION_UNAVAILABLE);
		const persisted = JSON.parse(bytes((store as unknown as { directory: string }).directory)) as {
			createRequestReplayRetired?: boolean;
		};
		expect(persisted.createRequestReplayRetired).toBe(true);
	});

	it("makes removed handles unavailable for adoption and confirmation", () => {
		const { store, created } = fixture();
		store.remove(created.recordId);
		expect(() => store.getByHandle(created.recoveryHandle)).toThrow(OWNED_SESSION_ADOPTION_UNAVAILABLE);
		expect(() =>
			store.getForConfirmation(created.recoveryHandle, store.digestRequest({ requestId: randomUUID() })),
		).toThrow(OWNED_SESSION_ADOPTION_UNAVAILABLE);
	});

	it("expires records with a fake clock", () => {
		const { directory, store, created, setNow } = fixture();
		setNow(10_001);
		expect(store.sweep()).toEqual([created.recordId]);
		expect(readdirSync(directory)).toEqual([]);
	});

	it("rolls back a prepared claimant and resets the old owner's disconnect deadline on reconnect", () => {
		const { store, created, authority, setNow } = fixture();
		const authorityDigest = store.digestAuthority(authority);
		const requestIdDigest = store.digestRequest({ requestId: "adopt-reconnect" });
		const requestDigest = store.digestRequest({ requestId: "adopt-reconnect", cursor: 1 });
		const prepared = store.beginAdoption({
			recoveryHandle: created.recoveryHandle,
			requestIdDigest,
			requestDigest,
			authorityDigest,
			expectedSupervisorGeneration: (store as unknown as { supervisorGeneration: string }).supervisorGeneration,
			expiresAt: 1_500,
		});
		expect(prepared.ownershipGeneration).toBe(1);
		const rolledBack = store.rollbackAdoption(created.recordId, authorityDigest, 4_000);
		expect(rolledBack).toMatchObject({ phase: "disconnected", ownershipGeneration: 0 });
		expect(rolledBack.recoveryHandle).toBe(created.recoveryHandle);

		setNow(2_000);
		const connected = store.markConnected(created.recordId, authorityDigest, Number.MAX_SAFE_INTEGER);
		expect(connected.phase).toBe("connected");
		setNow(5_000);
		expect(store.get(created.recordId)?.phase).toBe("connected");
		store.markDisconnected(created.recordId, 8_000);
		setNow(7_000);
		expect(store.get(created.recordId)?.phase).toBe("disconnected");
	});
});
